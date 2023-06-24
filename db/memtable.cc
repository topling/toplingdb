//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"

#include <algorithm>
#include <array>
#include <limits>
#include <memory>

#include "db/dbformat.h"
#include "db/kv_checksum.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/read_callback.h"
#include "db/wide/wide_column_serialization.h"
#include "logging/logging.h"
#include "memory/arena.h"
#include "memory/memory_usage.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "port/lang.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/types.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/merging_iterator.h"
#include "util/autovector.h"
#include "util/coding.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

ImmutableMemTableOptions::ImmutableMemTableOptions(
    const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options)
    : arena_block_size(mutable_cf_options.arena_block_size),
      memtable_prefix_bloom_bits(
          static_cast<uint32_t>(
              static_cast<double>(mutable_cf_options.write_buffer_size) *
              mutable_cf_options.memtable_prefix_bloom_size_ratio) *
          8u),
      memtable_huge_page_size(mutable_cf_options.memtable_huge_page_size),
      memtable_whole_key_filtering(
          mutable_cf_options.memtable_whole_key_filtering),
      inplace_update_support(ioptions.inplace_update_support),
      inplace_update_num_locks(mutable_cf_options.inplace_update_num_locks),
      inplace_callback(ioptions.inplace_callback),
      max_successive_merges(mutable_cf_options.max_successive_merges),
      statistics(ioptions.stats),
      merge_operator(ioptions.merge_operator.get()),
      info_log(ioptions.logger),
      allow_data_in_errors(ioptions.allow_data_in_errors) {}

MemTable::MemTable(const InternalKeyComparator& cmp,
                   const ImmutableOptions& ioptions,
                   const MutableCFOptions& mutable_cf_options,
                   WriteBufferManager* write_buffer_manager,
                   SequenceNumber latest_seq, uint32_t column_family_id)
    : comparator_(cmp),
      moptions_(ioptions, mutable_cf_options),
      refs_(0),
      kArenaBlockSize(Arena::OptimizeBlockSize(moptions_.arena_block_size)),
      mem_tracker_(write_buffer_manager),
      arena_(moptions_.arena_block_size,
             (write_buffer_manager != nullptr &&
              (write_buffer_manager->enabled() ||
               write_buffer_manager->cost_to_cache()))
                 ? &mem_tracker_
                 : nullptr,
             mutable_cf_options.memtable_huge_page_size),
      table_(ioptions.memtable_factory->CreateMemTableRep(
          comparator_, &arena_, mutable_cf_options.prefix_extractor.get(),
          ioptions.logger, column_family_id)),
      range_del_table_(SkipListFactory().CreateMemTableRep(
          comparator_, &arena_, nullptr /* transform */, ioptions.logger,
          column_family_id)),
      is_range_del_table_empty_(true),
      data_size_(0),
      num_entries_(0),
      num_deletes_(0),
      write_buffer_size_(mutable_cf_options.write_buffer_size),
      flush_in_progress_(false),
      flush_completed_(false),
      file_number_(0),
      first_seqno_(0),
      earliest_seqno_(latest_seq),
      creation_seq_(latest_seq),
      mem_next_logfile_number_(0),
      min_prep_log_referenced_(0),
      locks_(moptions_.inplace_update_support
                 ? moptions_.inplace_update_num_locks
                 : 0),
      prefix_extractor_(mutable_cf_options.prefix_extractor.get()),
      flush_state_(FLUSH_NOT_REQUESTED),
      clock_(ioptions.clock),
      insert_with_hint_prefix_extractor_(
          ioptions.memtable_insert_with_hint_prefix_extractor.get()),
      oldest_key_time_(std::numeric_limits<uint64_t>::max()),
      atomic_flush_seqno_(kMaxSequenceNumber),
      approximate_memory_usage_(0) {
  needs_user_key_cmp_in_get_ = table_->NeedsUserKeyCompareInGet();
  UpdateFlushState();
  // something went wrong if we need to flush before inserting anything
  assert(!ShouldScheduleFlush());

  // use bloom_filter_ for both whole key and prefix bloom filter
  if ((prefix_extractor_ || moptions_.memtable_whole_key_filtering) &&
      moptions_.memtable_prefix_bloom_bits > 0) {
    bloom_filter_.reset(
        new DynamicBloom(&arena_, moptions_.memtable_prefix_bloom_bits,
                         6 /* hard coded 6 probes */,
                         moptions_.memtable_huge_page_size, ioptions.logger));
  }
  // Initialize cached_range_tombstone_ here since it could
  // be read before it is constructed in MemTable::Add(), which could also lead
  // to a data race on the global mutex table backing atomic shared_ptr.
  auto new_cache = std::make_shared<FragmentedRangeTombstoneListCache>();
  size_t size = cached_range_tombstone_.Size();
  for (size_t i = 0; i < size; ++i) {
    std::shared_ptr<FragmentedRangeTombstoneListCache>* local_cache_ref_ptr =
        cached_range_tombstone_.AccessAtCore(i);
    auto new_local_cache_ref = std::make_shared<
        const std::shared_ptr<FragmentedRangeTombstoneListCache>>(new_cache);
    std::atomic_store_explicit(
        local_cache_ref_ptr,
        std::shared_ptr<FragmentedRangeTombstoneListCache>(new_local_cache_ref,
                                                           new_cache.get()),
        std::memory_order_relaxed);
  }
}

MemTable::~MemTable() {
  mem_tracker_.FreeMem();
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() {
  size_t usages[] = {
      arena_.ApproximateMemoryUsage(), table_->ApproximateMemoryUsage(),
      range_del_table_->ApproximateMemoryUsage(),
      ROCKSDB_NAMESPACE::ApproximateMemoryUsage(insert_hints_)};
  size_t total_usage = 0;
  for (size_t usage : usages) {
    // If usage + total_usage >= kMaxSizet, return kMaxSizet.
    // the following variation is to avoid numeric overflow.
    if (usage >= std::numeric_limits<size_t>::max() - total_usage) {
      return std::numeric_limits<size_t>::max();
    }
    total_usage += usage;
  }
  approximate_memory_usage_.store(total_usage, std::memory_order_relaxed);
  // otherwise, return the actual usage
  return total_usage;
}

bool MemTable::ShouldFlushNow() {
  size_t write_buffer_size = write_buffer_size_.load(std::memory_order_relaxed);
  // In a lot of times, we cannot allocate arena blocks that exactly matches the
  // buffer size. Thus we have to decide if we should over-allocate or
  // under-allocate.
  // This constant variable can be interpreted as: if we still have more than
  // "kAllowOverAllocationRatio * kArenaBlockSize" space left, we'd try to over
  // allocate one more block.
  const double kAllowOverAllocationRatio = 0.6;

  // If arena still have room for new block allocation, we can safely say it
  // shouldn't flush.
  auto allocated_memory = table_->ApproximateMemoryUsage() +
                          range_del_table_->ApproximateMemoryUsage() +
                          arena_.MemoryAllocatedBytes();

  approximate_memory_usage_.store(allocated_memory, std::memory_order_relaxed);

  // if we can still allocate one more block without exceeding the
  // over-allocation ratio, then we should not flush.
  if (allocated_memory + kArenaBlockSize <
      write_buffer_size + kArenaBlockSize * kAllowOverAllocationRatio) {
    return false;
  }

  // if user keeps adding entries that exceeds write_buffer_size, we need to
  // flush earlier even though we still have much available memory left.
  if (allocated_memory >
      write_buffer_size + kArenaBlockSize * kAllowOverAllocationRatio) {
    return true;
  }

  // In this code path, Arena has already allocated its "last block", which
  // means the total allocatedmemory size is either:
  //  (1) "moderately" over allocated the memory (no more than `0.6 * arena
  // block size`. Or,
  //  (2) the allocated memory is less than write buffer size, but we'll stop
  // here since if we allocate a new arena block, we'll over allocate too much
  // more (half of the arena block size) memory.
  //
  // In either case, to avoid over-allocate, the last block will stop allocation
  // when its usage reaches a certain ratio, which we carefully choose "0.75
  // full" as the stop condition because it addresses the following issue with
  // great simplicity: What if the next inserted entry's size is
  // bigger than AllocatedAndUnused()?
  //
  // The answer is: if the entry size is also bigger than 0.25 *
  // kArenaBlockSize, a dedicated block will be allocated for it; otherwise
  // arena will anyway skip the AllocatedAndUnused() and allocate a new, empty
  // and regular block. In either case, we *overly* over-allocated.
  //
  // Therefore, setting the last block to be at most "0.75 full" avoids both
  // cases.
  //
  // NOTE: the average percentage of waste space of this approach can be counted
  // as: "arena block size * 0.25 / write buffer size". User who specify a small
  // write buffer size and/or big arena block size may suffer.
  return arena_.AllocatedAndUnused() < kArenaBlockSize / 4;
}

void MemTable::UpdateFlushState() {
  auto state = flush_state_.load(std::memory_order_relaxed);
  if (state == FLUSH_NOT_REQUESTED && ShouldFlushNow()) {
    // ignore CAS failure, because that means somebody else requested
    // a flush
    flush_state_.compare_exchange_strong(state, FLUSH_REQUESTED,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed);
  }
}

void MemTable::UpdateOldestKeyTime() {
  uint64_t oldest_key_time = oldest_key_time_.load(std::memory_order_relaxed);
  if (UNLIKELY(oldest_key_time == std::numeric_limits<uint64_t>::max())) {
    int64_t current_time = 0;
    auto s = clock_->GetCurrentTime(&current_time);
    if (s.ok()) {
      assert(current_time >= 0);
      // If fail, the timestamp is already set.
      oldest_key_time_.compare_exchange_strong(
          oldest_key_time, static_cast<uint64_t>(current_time),
          std::memory_order_relaxed, std::memory_order_relaxed);
    }
  }
}

int MemTable::KeyComparator::operator()(const char* prefix_len_key1,
                                        const char* prefix_len_key2) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice k1 = GetLengthPrefixedSlice(prefix_len_key1);
  Slice k2 = GetLengthPrefixedSlice(prefix_len_key2);
  return comparator.CompareKeySeq(k1, k2);
}

int MemTable::KeyComparator::operator()(
    const char* prefix_len_key, const KeyComparator::DecodedType& key) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(prefix_len_key);
  return comparator.CompareKeySeq(a, key);
}

void MemTableRep::InsertConcurrently(KeyHandle /*handle*/) {
  throw std::runtime_error("concurrent insert not supported");
}

const InternalKeyComparator* MemTable::KeyComparator::icomparator() const {
  return &comparator;
}

Slice MemTableRep::UserKey(const char* key) const {
  Slice slice = GetLengthPrefixedSlice(key);
  return Slice(slice.data(), slice.size() - 8);
}

size_t MemTableRep::EncodeKeyValueSize(const Slice& key, const Slice& value) {
  size_t buf_size = 0;
  buf_size += VarintLength(key.size()) + key.size();
  buf_size += VarintLength(value.size()) + value.size();
  return buf_size;
}

KeyHandle MemTableRep::EncodeKeyValue(const Slice& key, const Slice& value) {
  size_t buf_size = EncodeKeyValueSize(key, value);
  char* buf = nullptr;
  KeyHandle handle = Allocate(buf_size, &buf);
  assert(nullptr != handle);
  assert(nullptr != buf);
  char* p = EncodeVarint32(buf, (uint32_t)key.size());
  memcpy(p, key.data(), key.size());
  p = EncodeVarint32(p + key.size(), (uint32_t)value.size());
  memcpy(p, value.data(), value.size());
  return handle;
}

bool MemTableRep::InsertKeyValue(const Slice& internal_key,
                                 const Slice& value) {
  KeyHandle handle = EncodeKeyValue(internal_key, value);
  return InsertKey(handle);
}

bool MemTableRep::InsertKeyValueWithHint(const Slice& internal_key,
                                         const Slice& value, void** hint) {
  KeyHandle handle = EncodeKeyValue(internal_key, value);
  return InsertKeyWithHint(handle, hint);
}

bool MemTableRep::InsertKeyValueConcurrently(const Slice& internal_key,
                                             const Slice& value) {
  KeyHandle handle = EncodeKeyValue(internal_key, value);
  return InsertKeyConcurrently(handle);
}

bool MemTableRep::InsertKeyValueWithHintConcurrently(const Slice& internal_key,
                                                     const Slice& value,
                                                     void** hint) {
  KeyHandle handle = EncodeKeyValue(internal_key, value);
  return InsertKeyWithHintConcurrently(handle, hint);
}

KeyHandle MemTableRep::Allocate(const size_t len, char** buf) {
  *buf = allocator_->Allocate(len);
  return static_cast<KeyHandle>(*buf);
}

bool MemTableRep::Iterator::NextAndGetResult(IterateResult* result) {
  if (LIKELY(NextAndCheckValid())) {
    result->SetKey(this->GetKey());
    result->bound_check_result = IterBoundCheck::kUnknown;
    result->value_prepared = true;
    result->is_valid = true;
    return true;
  } else {
    result->is_valid = false;
    return false;
  }
}
bool MemTableRep::Iterator::NextAndCheckValid() { Next(); return Valid(); }
bool MemTableRep::Iterator::PrevAndCheckValid() { Prev(); return Valid(); }

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, static_cast<uint32_t>(target.size()));
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public InternalIterator {
 public:
  MemTableIterator(const MemTable& mem, const ReadOptions& read_options,
                   Arena* arena, bool use_range_del_table = false)
      : bloom_(nullptr),
        prefix_extractor_(mem.prefix_extractor_),
        comparator_(mem.comparator_),
        valid_(false),
        arena_mode_(arena != nullptr),
        value_pinned_(
            !mem.GetImmutableMemTableOptions()->inplace_update_support) {
    if (use_range_del_table) {
      iter_ = mem.range_del_table_->GetIterator(arena);
    } else if (prefix_extractor_ != nullptr && !read_options.total_order_seek &&
               !read_options.auto_prefix_mode) {
      // Auto prefix mode is not implemented in memtable yet.
      bloom_ = mem.bloom_filter_.get();
      iter_ = mem.table_->GetDynamicPrefixIterator(arena);
    } else {
      iter_ = mem.table_->GetIterator(arena);
    }
  }
  // No copying allowed
  MemTableIterator(const MemTableIterator&) = delete;
  void operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override {
#ifndef NDEBUG
    // Assert that the MemTableIterator is never deleted while
    // Pinning is Enabled.
    assert(!pinned_iters_mgr_ || !pinned_iters_mgr_->PinningEnabled());
#endif
    if (arena_mode_) {
      iter_->~Iterator();
    } else {
      delete iter_;
    }
  }

#ifndef NDEBUG
  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;
#endif

  bool Valid() const override { return valid_; }
  void Seek(const Slice& k) override {
    PERF_TIMER_GUARD(seek_on_memtable_time);
    PERF_COUNTER_ADD(seek_on_memtable_count, 1);
    if (bloom_) {
      // iterator should only use prefix bloom filter
      auto ts_sz = comparator_.comparator.user_comparator()->timestamp_size();
      Slice user_k_without_ts(ExtractUserKeyAndStripTimestamp(k, ts_sz));
      if (prefix_extractor_->InDomain(user_k_without_ts)) {
        if (!bloom_->MayContain(
                prefix_extractor_->Transform(user_k_without_ts))) {
          PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
          valid_ = false;
          return;
        } else {
          PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
        }
      }
    }
    iter_->Seek(k, nullptr);
    valid_ = iter_->Valid();
  }
  void SeekForPrev(const Slice& k) override {
    PERF_TIMER_GUARD(seek_on_memtable_time);
    PERF_COUNTER_ADD(seek_on_memtable_count, 1);
    if (bloom_) {
      auto ts_sz = comparator_.comparator.user_comparator()->timestamp_size();
      Slice user_k_without_ts(ExtractUserKeyAndStripTimestamp(k, ts_sz));
      if (prefix_extractor_->InDomain(user_k_without_ts)) {
        if (!bloom_->MayContain(
                prefix_extractor_->Transform(user_k_without_ts))) {
          PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
          valid_ = false;
          return;
        } else {
          PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
        }
      }
    }
    iter_->Seek(k, nullptr);
    valid_ = iter_->Valid();
    if (!Valid()) {
      SeekToLast();
    }
    while (Valid() && comparator_.comparator.Compare(k, key()) < 0) {
      Prev();
    }
  }
  void SeekToFirst() override {
    iter_->SeekToFirst();
    valid_ = iter_->Valid();
  }
  void SeekToLast() override {
    iter_->SeekToLast();
    valid_ = iter_->Valid();
  }
  ROCKSDB_FLATTEN
  void Next() override {
    NextAndCheckValid(); // ignore return value
  }
  bool NextAndCheckValid() final {
    PERF_COUNTER_ADD(next_on_memtable_count, 1);
    assert(Valid());
    bool is_valid = iter_->NextAndCheckValid();
    TEST_SYNC_POINT_CALLBACK("MemTableIterator::Next:0", iter_);
    valid_ = is_valid;
    return is_valid;
  }
  bool NextAndGetResult(IterateResult* result) override {
    return iter_->NextAndGetResult(result);
  }
  ROCKSDB_FLATTEN
  void Prev() override {
    PrevAndCheckValid(); // ignore return value
  }
  bool PrevAndCheckValid() final {
    PERF_COUNTER_ADD(prev_on_memtable_count, 1);
    assert(Valid());
    valid_ = iter_->PrevAndCheckValid();
    return valid_;
  }
  Slice key() const override {
    assert(Valid());
    return iter_->GetKey();
  }
  Slice value() const override {
    assert(Valid());
    return iter_->GetValue();
  }

  Status status() const override { return Status::OK(); }

  bool IsKeyPinned() const override {
    // some memtable key may not pinned, such as a patricia trie
    // which reconstruct key during search/iterate
    return iter_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    // memtable value is always pinned, except if we allow inplace update.
    return value_pinned_;
  }

 private:
  DynamicBloom* bloom_;
  const SliceTransform* const prefix_extractor_;
  const MemTable::KeyComparator comparator_;
  MemTableRep::Iterator* iter_;
  bool valid_;
  bool arena_mode_;
  bool value_pinned_;
};

InternalIterator* MemTable::NewIterator(const ReadOptions& read_options,
                                        Arena* arena) {
  assert(arena != nullptr);
  auto mem = arena->AllocateAligned(sizeof(MemTableIterator));
  return new (mem) MemTableIterator(*this, read_options, arena);
}

FragmentedRangeTombstoneIterator* MemTable::NewRangeTombstoneIterator(
    const ReadOptions& read_options, SequenceNumber read_seq,
    bool immutable_memtable) {
  if (read_options.ignore_range_deletions ||
      is_range_del_table_empty_.load(std::memory_order_relaxed)) {
    return nullptr;
  }
  return NewRangeTombstoneIteratorInternal(read_options, read_seq,
                                           immutable_memtable);
}

FragmentedRangeTombstoneIterator* MemTable::NewRangeTombstoneIteratorInternal(
    const ReadOptions& read_options, SequenceNumber read_seq,
    bool immutable_memtable) {
  if (immutable_memtable) {
    // Note that caller should already have verified that
    // !is_range_del_table_empty_
    assert(IsFragmentedRangeTombstonesConstructed());
    return new FragmentedRangeTombstoneIterator(
        fragmented_range_tombstone_list_.get(), comparator_.comparator,
        read_seq, read_options.timestamp);
  }

  // takes current cache
  std::shared_ptr<FragmentedRangeTombstoneListCache> cache =
      std::atomic_load_explicit(cached_range_tombstone_.Access(),
                                std::memory_order_relaxed);
  // construct fragmented tombstone list if necessary
  if (!cache->initialized.load(std::memory_order_acquire)) {
    cache->reader_mutex.lock();
    if (!cache->tombstones) {
      auto* unfragmented_iter =
          new MemTableIterator(*this, read_options, nullptr /* arena */,
                               true /* use_range_del_table */);
      cache->tombstones.reset(new FragmentedRangeTombstoneList(
          std::unique_ptr<InternalIterator>(unfragmented_iter),
          comparator_.comparator));
      cache->initialized.store(true, std::memory_order_release);
    }
    cache->reader_mutex.unlock();
  }

  auto* fragmented_iter = new FragmentedRangeTombstoneIterator(
      cache, comparator_.comparator, read_seq, read_options.timestamp);
  return fragmented_iter;
}

void MemTable::ConstructFragmentedRangeTombstones() {
  assert(!IsFragmentedRangeTombstonesConstructed(false));
  // There should be no concurrent Construction
  if (!is_range_del_table_empty_.load(std::memory_order_relaxed)) {
    // TODO: plumb Env::IOActivity
    auto* unfragmented_iter =
        new MemTableIterator(*this, ReadOptions(), nullptr /* arena */,
                             true /* use_range_del_table */);

    fragmented_range_tombstone_list_ =
        std::make_unique<FragmentedRangeTombstoneList>(
            std::unique_ptr<InternalIterator>(unfragmented_iter),
            comparator_.comparator);
  }
}

port::RWMutex* MemTable::GetLock(const Slice& key) {
  return &locks_[GetSliceRangedNPHash(key, locks_.size())];
}

MemTable::MemTableStats MemTable::ApproximateStats(const Slice& start_ikey,
                                                   const Slice& end_ikey) {
  uint64_t entry_count = table_->ApproximateNumEntries(start_ikey, end_ikey);
  entry_count += range_del_table_->ApproximateNumEntries(start_ikey, end_ikey);
  if (entry_count == 0) {
    return {0, 0};
  }
  uint64_t n = num_entries_.load(std::memory_order_relaxed);
  if (n == 0) {
    return {0, 0};
  }
  if (entry_count > n) {
    // (range_del_)table_->ApproximateNumEntries() is just an estimate so it can
    // be larger than actual entries we have. Cap it to entries we have to limit
    // the inaccuracy.
    entry_count = n;
  }
  uint64_t data_size = data_size_.load(std::memory_order_relaxed);
  return {entry_count * (data_size / n), entry_count};
}

// encoded just contains key
Status MemTable::VerifyEncodedEntry(Slice ikey, Slice value,
                                    const ProtectionInfoKVOS64& kv_prot_info) {
  size_t ikey_len = ikey.size();
  size_t ts_sz = GetInternalKeyComparator().user_comparator()->timestamp_size();
  if (ikey_len < 8 + ts_sz) {
    return Status::Corruption("Internal key length too short");
  }
  if (ikey_len > ikey.size()) {
    return Status::Corruption("Internal key length too long");
  }
  const size_t user_key_len = ikey_len - 8;
  Slice key(ikey.data(), user_key_len);

  uint64_t packed = DecodeFixed64(key.end());
  ValueType value_type = kMaxValue;
  SequenceNumber sequence_number = kMaxSequenceNumber;
  UnPackSequenceAndType(packed, &sequence_number, &value_type);

  return kv_prot_info.StripS(sequence_number)
      .StripKVO(key, value, value_type)
      .GetStatus();
}

ROCKSDB_FLATTEN
Status MemTable::Add(SequenceNumber s, ValueType type,
                     const Slice& key, /* user key */
                     const Slice& value,
                     const ProtectionInfoKVOS64* kv_prot_info,
                     bool allow_concurrent,
                     MemTablePostProcessInfo* post_process_info, void** hint) {
  std::unique_ptr<MemTableRep>& table =
      type == kTypeRangeDeletion ? range_del_table_ : table_;
  Slice key_slice((char*)memcpy(alloca(key.size_ + 8), key.data_, key.size_),
                  key.size_ + 8);
  PutUnaligned((uint64_t*)(key_slice.data_ + key.size_), PackSequenceAndType(s, type));
  if (kv_prot_info != nullptr) {
    TEST_SYNC_POINT_CALLBACK("MemTable::Add:Encoded", &key_slice);
    Status status = VerifyEncodedEntry(key_slice, value, *kv_prot_info);
    if (!status.ok()) {
      return status;
    }
  }

  size_t encoded_len = MemTableRep::EncodeKeyValueSize(key_slice, value);
  if (!allow_concurrent) {
    // Extract prefix for insert with hint.
    if (insert_with_hint_prefix_extractor_ != nullptr &&
        insert_with_hint_prefix_extractor_->InDomain(key_slice)) {
      Slice prefix = insert_with_hint_prefix_extractor_->Transform(key_slice);
      hint = &insert_hints_[prefix];  // overwrite hint?
      bool res = table->InsertKeyValueWithHint(key_slice, value, hint);
      if (UNLIKELY(!res)) {
        return Status::TryAgain("key+seq exists");
      }
    } else {
      bool res = table->InsertKeyValue(key_slice, value);
      if (UNLIKELY(!res)) {
        return Status::TryAgain("key+seq exists");
      }
    }

    // this is a bit ugly, but is the way to avoid locked instructions
    // when incrementing an atomic
    num_entries_.store(num_entries_.load(std::memory_order_relaxed) + 1,
                       std::memory_order_relaxed);
    data_size_.store(data_size_.load(std::memory_order_relaxed) + encoded_len,
                     std::memory_order_relaxed);
    if (type == kTypeDeletion || type == kTypeSingleDeletion ||
        type == kTypeDeletionWithTimestamp) {
      num_deletes_.store(num_deletes_.load(std::memory_order_relaxed) + 1,
                         std::memory_order_relaxed);
    }

    if (bloom_filter_) {
    #if defined(TOPLINGDB_WITH_TIMESTAMP)
      size_t ts_sz = GetInternalKeyComparator().user_comparator()->timestamp_size();
      Slice key_without_ts = StripTimestampFromUserKey(key, ts_sz);
    #else
      const Slice& key_without_ts = key;
    #endif
      if (prefix_extractor_ && prefix_extractor_->InDomain(key_without_ts)) {
        bloom_filter_->Add(prefix_extractor_->Transform(key_without_ts));
      }
      if (moptions_.memtable_whole_key_filtering) {
        bloom_filter_->Add(key_without_ts);
      }
    }

    // The first sequence number inserted into the memtable
    assert(first_seqno_ == 0 || s >= first_seqno_);
    if (first_seqno_ == 0) {
      first_seqno_.store(s, std::memory_order_relaxed);

      if (earliest_seqno_ == kMaxSequenceNumber) {
        earliest_seqno_.store(GetFirstSequenceNumber(),
                              std::memory_order_relaxed);
      }
      assert(first_seqno_.load() >= earliest_seqno_.load());
    }
    assert(post_process_info == nullptr);
    UpdateFlushState();
  } else {
    bool res =
        (hint == nullptr)
            ? table->InsertKeyValueConcurrently(key_slice, value)
            : table->InsertKeyValueWithHintConcurrently(key_slice, value, hint);
    if (UNLIKELY(!res)) {
      return Status::TryAgain("key+seq exists");
    }

    assert(post_process_info != nullptr);
    post_process_info->num_entries++;
    post_process_info->data_size += encoded_len;
    if (type == kTypeDeletion) {
      post_process_info->num_deletes++;
    }

    if (bloom_filter_) {
    #if defined(TOPLINGDB_WITH_TIMESTAMP)
      size_t ts_sz = GetInternalKeyComparator().user_comparator()->timestamp_size();
      Slice key_without_ts = StripTimestampFromUserKey(key, ts_sz);
    #else
      const Slice& key_without_ts = key;
    #endif
      if (prefix_extractor_ && prefix_extractor_->InDomain(key_without_ts)) {
        bloom_filter_->AddConcurrently(
            prefix_extractor_->Transform(key_without_ts));
      }
      if (moptions_.memtable_whole_key_filtering) {
        bloom_filter_->AddConcurrently(key_without_ts);
      }
    }

    // atomically update first_seqno_ and earliest_seqno_.
    uint64_t cur_seq_num = first_seqno_.load(std::memory_order_relaxed);
    while ((cur_seq_num == 0 || s < cur_seq_num) &&
           !first_seqno_.compare_exchange_weak(cur_seq_num, s)) {
    }
    uint64_t cur_earliest_seqno =
        earliest_seqno_.load(std::memory_order_relaxed);
    while (
        (cur_earliest_seqno == kMaxSequenceNumber || s < cur_earliest_seqno) &&
        !earliest_seqno_.compare_exchange_weak(cur_earliest_seqno, s)) {
    }
  }
  if (UNLIKELY(type == kTypeRangeDeletion)) {
    auto new_cache = std::make_shared<FragmentedRangeTombstoneListCache>();
    size_t size = cached_range_tombstone_.Size();
    if (allow_concurrent) {
      range_del_mutex_.lock();
    }
    for (size_t i = 0; i < size; ++i) {
      std::shared_ptr<FragmentedRangeTombstoneListCache>* local_cache_ref_ptr =
          cached_range_tombstone_.AccessAtCore(i);
      auto new_local_cache_ref = std::make_shared<
          const std::shared_ptr<FragmentedRangeTombstoneListCache>>(new_cache);
      // It is okay for some reader to load old cache during invalidation as
      // the new sequence number is not published yet.
      // Each core will have a shared_ptr to a shared_ptr to the cached
      // fragmented range tombstones, so that ref count is maintianed locally
      // per-core using the per-core shared_ptr.
      std::atomic_store_explicit(
          local_cache_ref_ptr,
          std::shared_ptr<FragmentedRangeTombstoneListCache>(
              new_local_cache_ref, new_cache.get()),
          std::memory_order_relaxed);
    }
    if (allow_concurrent) {
      range_del_mutex_.unlock();
    }
    is_range_del_table_empty_.store(false, std::memory_order_relaxed);
  }
  UpdateOldestKeyTime();
  return Status::OK();
}

// Callback from MemTable::Get()
namespace {

struct Saver {
  Status* status;
  const LookupKey* key;
  PinnableSlice* value;
  PinnableWideColumns* columns;
  SequenceNumber seq;
  std::string* timestamp;
  const MergeOperator* merge_operator;
  // the merge operations encountered;
  MergeContext* merge_context;
  SequenceNumber max_covering_tombstone_seq;
  MemTable* mem;
  Logger* logger;
  Statistics* statistics;
  SystemClock* clock;

  ReadCallback* callback_;
  bool* is_blob_index;
  bool found_final_value;  // Is value set correctly? Used by KeyMayExist
  bool merge_in_progress;
  bool inplace_update_support;
  bool do_merge;
  bool allow_data_in_errors;
  bool is_zero_copy;
  bool needs_user_key_cmp_in_get;
  bool CheckCallback(SequenceNumber _seq) {
    if (callback_) {
      return callback_->IsVisible(_seq);
    }
    return true;
  }
};
}  // anonymous namespace

static bool SaveValue(void* arg, const MemTableRep::KeyValuePair& pair) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  assert(s != nullptr);
  assert(!s->value || !s->columns);

  MergeContext* merge_context = s->merge_context;
  SequenceNumber max_covering_tombstone_seq = s->max_covering_tombstone_seq;
  const MergeOperator* merge_operator = s->merge_operator;

  assert(merge_context != nullptr);

  // Check that it belongs to same user key.  We do not check the
  // sequence number since the Seek() call above should have skipped
  // all entries with overly large sequence numbers.
  const Slice ikey = pair.ikey;
  Slice v = pair.value;
  const size_t key_length = ikey.size();
  const char* key_ptr = ikey.data();
  assert(key_length >= 8);
  Slice user_key_slice = Slice(key_ptr, key_length - 8);
#if defined(TOPLINGDB_WITH_TIMESTAMP)
  const Comparator* user_comparator =
      s->mem->GetInternalKeyComparator().user_comparator();
  size_t ts_sz = user_comparator->timestamp_size();
  if (ts_sz && s->timestamp && max_covering_tombstone_seq > 0) {
    // timestamp should already be set to range tombstone timestamp
    assert(s->timestamp->size() == ts_sz);
  }
#else
  #if defined(__GNUC__)
    #pragma GCC diagnostic ignored "-Wparentheses" // fuck
  #endif
  const Comparator* user_comparator = nullptr;
  constexpr size_t ts_sz = 0; // let compiler optimize it out
#endif
  if (!s->needs_user_key_cmp_in_get ||
#if !defined(TOPLINGDB_WITH_TIMESTAMP)
  // user_comparator is not need if !needs_user_key_cmp_in_get without timestamp,
  // omit load it from ptr to ptr
  (user_comparator = s->mem->GetInternalKeyComparator().user_comparator(), true) &&
#endif
      user_comparator->EqualWithoutTimestamp(user_key_slice,
                                             s->key->user_key())) {
    assert(user_comparator->EqualWithoutTimestamp(user_key_slice,
                                             s->key->user_key()));
    // Correct user key
    const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
    ValueType type;
    SequenceNumber seq;
    UnPackSequenceAndType(tag, &seq, &type);
    // If the value is not in the snapshot, skip it
    if (!s->CheckCallback(seq)) {
      return true;  // to continue to the next seq
    }

    if (s->seq == kMaxSequenceNumber) {
      s->seq = seq;
      if (s->seq > max_covering_tombstone_seq) {
        if (ts_sz && s->timestamp != nullptr) {
          // `timestamp` was set to range tombstone's timestamp before
          // `SaveValue` is ever called. This key has a higher sequence number
          // than range tombstone, and is the key with the highest seqno across
          // all keys with this user_key, so we update timestamp here.
          Slice ts = ExtractTimestampFromUserKey(user_key_slice, ts_sz);
          s->timestamp->assign(ts.data(), ts_sz);
        }
      } else {
        s->seq = max_covering_tombstone_seq;
      }
    }

    if (ts_sz > 0 && s->timestamp != nullptr) {
      if (!s->timestamp->empty()) {
        assert(ts_sz == s->timestamp->size());
      }
      // TODO optimize for smaller size ts
      const std::string kMaxTs(ts_sz, '\xff');
      if (s->timestamp->empty() ||
          user_comparator->CompareTimestamp(*(s->timestamp), kMaxTs) == 0) {
        Slice ts = ExtractTimestampFromUserKey(user_key_slice, ts_sz);
        s->timestamp->assign(ts.data(), ts_sz);
      }
    }

    if ((type == kTypeValue || type == kTypeMerge || type == kTypeBlobIndex ||
         type == kTypeWideColumnEntity || type == kTypeDeletion ||
         type == kTypeSingleDeletion || type == kTypeDeletionWithTimestamp) &&
        max_covering_tombstone_seq > seq) {
      type = kTypeRangeDeletion;
    }
    switch (type) {
      case kTypeBlobIndex: {
        if (!s->do_merge) {
          *(s->status) = Status::NotSupported(
              "GetMergeOperands not supported by stacked BlobDB");
          s->found_final_value = true;
          return false;
        }

        if (s->merge_in_progress) {
          *(s->status) = Status::NotSupported(
              "Merge operator not supported by stacked BlobDB");
          s->found_final_value = true;
          return false;
        }

        if (s->is_blob_index == nullptr) {
          ROCKS_LOG_ERROR(s->logger, "Encountered unexpected blob index.");
          *(s->status) = Status::NotSupported(
              "Encountered unexpected blob index. Please open DB with "
              "ROCKSDB_NAMESPACE::blob_db::BlobDB.");
          s->found_final_value = true;
          return false;
        }

        if (s->inplace_update_support) {
          s->mem->GetLock(s->key->user_key())->ReadLock();
        }

        s->status->SetAsOK();

        if (s->value) {
          if (s->is_zero_copy)
            s->value->PinSlice(v, nullptr);
          else
            s->value->PinSelf(v);
        } else if (s->columns) {
          s->columns->SetPlainValue(v);
        }

        if (s->inplace_update_support) {
          s->mem->GetLock(s->key->user_key())->ReadUnlock();
        }

        s->found_final_value = true;
        *(s->is_blob_index) = true;

        return false;
      }
      case kTypeValue: {
        if (UNLIKELY(s->inplace_update_support)) {
          s->mem->GetLock(s->key->user_key())->ReadLock();
        }

        s->status->SetAsOK();

        if (UNLIKELY(!s->do_merge)) {
          // Preserve the value with the goal of returning it as part of
          // raw merge operands to the user
          // TODO(yanqin) update MergeContext so that timestamps information
          // can also be retained.

          merge_context->PushOperand(
              v, s->inplace_update_support == false /* operand_pinned */);
        } else if (UNLIKELY(s->merge_in_progress)) {
          assert(s->do_merge);

          if (s->value || s->columns) {
            std::string result;
            // `op_failure_scope` (an output parameter) is not provided (set to
            // nullptr) since a failure must be propagated regardless of its
            // value.
            *(s->status) = MergeHelper::TimedFullMerge(
                merge_operator, s->key->user_key(), &v,
                merge_context->GetOperands(), &result, s->logger, s->statistics,
                s->clock, /* result_operand */ nullptr,
                /* update_num_ops_stats */ true,
                /* op_failure_scope */ nullptr);

            if (s->status->ok()) {
              if (s->value) {
                *(s->value->GetSelf()) = std::move(result);
                s->value->PinSelf();
              } else {
                assert(s->columns);
                s->columns->SetPlainValue(std::move(result));
              }
            }
          }
        } else if (LIKELY(s->value != nullptr)) {
          if (s->is_zero_copy)
            s->value->PinSlice(v, nullptr);
          else
            s->value->PinSelf(v);
        } else if (s->columns) {
          s->columns->SetPlainValue(v);
        }

        if (UNLIKELY(s->inplace_update_support)) {
          s->mem->GetLock(s->key->user_key())->ReadUnlock();
        }

        s->found_final_value = true;

        if (UNLIKELY(s->is_blob_index != nullptr)) {
          *(s->is_blob_index) = false;
        }

        return false;
      }
      case kTypeWideColumnEntity: {
        if (UNLIKELY(s->inplace_update_support)) {
          s->mem->GetLock(s->key->user_key())->ReadLock();
        }

        s->status->SetAsOK();

        if (!s->do_merge) {
          // Preserve the value with the goal of returning it as part of
          // raw merge operands to the user

          Slice value_of_default;
          *(s->status) = WideColumnSerialization::GetValueOfDefaultColumn(
              v, value_of_default);

          if (s->status->ok()) {
            merge_context->PushOperand(
                value_of_default,
                s->inplace_update_support == false /* operand_pinned */);
          }
        } else if (s->merge_in_progress) {
          assert(s->do_merge);

          if (s->value) {
            Slice value_of_default;
            *(s->status) = WideColumnSerialization::GetValueOfDefaultColumn(
                v, value_of_default);
            if (s->status->ok()) {
              // `op_failure_scope` (an output parameter) is not provided (set
              // to nullptr) since a failure must be propagated regardless of
              // its value.
              *(s->status) = MergeHelper::TimedFullMerge(
                  merge_operator, s->key->user_key(), &value_of_default,
                  merge_context->GetOperands(), s->value->GetSelf(), s->logger,
                  s->statistics, s->clock, /* result_operand */ nullptr,
                  /* update_num_ops_stats */ true,
                  /* op_failure_scope */ nullptr);
              s->value->PinSelf();
            }
          } else if (s->columns) {
            std::string result;
            // `op_failure_scope` (an output parameter) is not provided (set to
            // nullptr) since a failure must be propagated regardless of its
            // value.
            *(s->status) = MergeHelper::TimedFullMergeWithEntity(
                merge_operator, s->key->user_key(), v,
                merge_context->GetOperands(), &result, s->logger, s->statistics,
                s->clock, /* update_num_ops_stats */ true,
                /* op_failure_scope */ nullptr);

            if (s->status->ok()) {
              *(s->status) = s->columns->SetWideColumnValue(std::move(result));
            }
          }
        } else if (s->value) {
          Slice value_of_default;
          *(s->status) = WideColumnSerialization::GetValueOfDefaultColumn(
              v, value_of_default);
          if (s->status->ok()) {
            s->value->PinSelf(value_of_default);
          }
        } else if (s->columns) {
          *(s->status) = s->columns->SetWideColumnValue(v);
        }

        if (UNLIKELY(s->inplace_update_support)) {
          s->mem->GetLock(s->key->user_key())->ReadUnlock();
        }

        s->found_final_value = true;

        if (s->is_blob_index != nullptr) {
          *(s->is_blob_index) = false;
        }

        return false;
      }
      case kTypeDeletion:
      case kTypeDeletionWithTimestamp:
      case kTypeSingleDeletion:
      case kTypeRangeDeletion: {
        if (s->merge_in_progress) {
          if (s->value || s->columns) {
            std::string result;
            // `op_failure_scope` (an output parameter) is not provided (set to
            // nullptr) since a failure must be propagated regardless of its
            // value.
            *(s->status) = MergeHelper::TimedFullMerge(
                merge_operator, s->key->user_key(), nullptr,
                merge_context->GetOperands(), &result, s->logger, s->statistics,
                s->clock, /* result_operand */ nullptr,
                /* update_num_ops_stats */ true,
                /* op_failure_scope */ nullptr);

            if (s->status->ok()) {
              if (s->value) {
                *(s->value->GetSelf()) = std::move(result);
                s->value->PinSelf();
              } else {
                assert(s->columns);
                s->columns->SetPlainValue(std::move(result));
              }
            }
          } else {
            // We have found a final value (a base deletion) and have newer
            // merge operands that we do not intend to merge. Nothing remains
            // to be done so assign status to OK.
            *(s->status) = Status::OK();
          }
        } else {
          *(s->status) = Status::NotFound();
        }
        s->found_final_value = true;
        return false;
      }
      case kTypeMerge: {
        if (UNLIKELY(!merge_operator)) {
          *(s->status) = Status::InvalidArgument(
              "merge_operator is not properly initialized.");
          // Normally we continue the loop (return true) when we see a merge
          // operand.  But in case of an error, we should stop the loop
          // immediately and pretend we have found the value to stop further
          // seek.  Otherwise, the later call will override this error status.
          s->found_final_value = true;
          return false;
        }
        s->merge_in_progress = true;
        merge_context->PushOperand(
            v, s->inplace_update_support == false /* operand_pinned */);
        PERF_COUNTER_ADD(internal_merge_point_lookup_count, 1);

        if (s->do_merge && merge_operator->ShouldMerge(
                               merge_context->GetOperandsDirectionBackward())) {
          if (s->value || s->columns) {
            std::string result;
            // `op_failure_scope` (an output parameter) is not provided (set to
            // nullptr) since a failure must be propagated regardless of its
            // value.
            *(s->status) = MergeHelper::TimedFullMerge(
                merge_operator, s->key->user_key(), nullptr,
                merge_context->GetOperands(), &result, s->logger, s->statistics,
                s->clock, /* result_operand */ nullptr,
                /* update_num_ops_stats */ true,
                /* op_failure_scope */ nullptr);

            if (s->status->ok()) {
              if (s->value) {
                *(s->value->GetSelf()) = std::move(result);
                s->value->PinSelf();
              } else {
                assert(s->columns);
                s->columns->SetPlainValue(std::move(result));
              }
            }
          }

          s->found_final_value = true;
          return false;
        }
        return true;
      }
      default: {
        std::string msg("Corrupted value not expected.");
        if (s->allow_data_in_errors) {
          msg.append("Unrecognized value type: " +
                     std::to_string(static_cast<int>(type)) + ". ");
          msg.append("User key: " + user_key_slice.ToString(/*hex=*/true) +
                     ". ");
          msg.append("seq: " + std::to_string(seq) + ".");
        }
        *(s->status) = Status::Corruption(msg.c_str());
        return false;
      }
    }
  }

  // s->state could be Corrupt, merge or notfound
  return false;
}

ROCKSDB_FLATTEN
bool MemTable::Get(const LookupKey& key, PinnableSlice* value,
                   PinnableWideColumns* columns, std::string* timestamp,
                   Status* s, MergeContext* merge_context,
                   SequenceNumber* max_covering_tombstone_seq,
                   SequenceNumber* seq, const ReadOptions& read_opts,
                   bool immutable_memtable, ReadCallback* callback,
                   bool* is_blob_index, bool do_merge) {
  // The sequence number is updated synchronously in version_set.h
  if (UNLIKELY(IsEmpty())) {
    // Avoiding recording stats for speed.
    return false;
  }
  PERF_TIMER_GUARD(get_from_memtable_time);

  std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
      NewRangeTombstoneIterator(read_opts,
                                GetInternalKeySeqno(key.internal_key()),
                                immutable_memtable));
  if (range_del_iter != nullptr) {
    SequenceNumber covering_seq =
        range_del_iter->MaxCoveringTombstoneSeqnum(key.user_key());
    if (covering_seq > *max_covering_tombstone_seq) {
      *max_covering_tombstone_seq = covering_seq;
      if (timestamp) {
        // Will be overwritten in SaveValue() if there is a point key with
        // a higher seqno.
        timestamp->assign(range_del_iter->timestamp().data(),
                          range_del_iter->timestamp().size());
      }
    }
  }

  if (UNLIKELY(bloom_filter_ != nullptr)) {
    bool may_contain = true;
  #if defined(TOPLINGDB_WITH_TIMESTAMP)
    size_t ts_sz = GetInternalKeyComparator().user_comparator()->timestamp_size();
    Slice user_key_without_ts = StripTimestampFromUserKey(key.user_key(), ts_sz);
  #else
    Slice user_key_without_ts = key.user_key();
  #endif
    bool bloom_checked = false;
    // when both memtable_whole_key_filtering and prefix_extractor_ are set,
    // only do whole key filtering for Get() to save CPU
    if (moptions_.memtable_whole_key_filtering) {
      may_contain = bloom_filter_->MayContain(user_key_without_ts);
      bloom_checked = true;
    } else {
      assert(prefix_extractor_);
      if (prefix_extractor_->InDomain(user_key_without_ts)) {
        may_contain = bloom_filter_->MayContain(
            prefix_extractor_->Transform(user_key_without_ts));
        bloom_checked = true;
      }
    }
    if (UNLIKELY(!may_contain)) {
      // iter is null if prefix bloom says the key does not exist
      PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
      *seq = kMaxSequenceNumber;
      PERF_COUNTER_ADD(get_from_memtable_count, 1);
      return false;
    } else {
      if (UNLIKELY(bloom_checked)) {
        PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
      }
    }
  }

  Saver saver;
  saver.status = s;
  saver.found_final_value = false;
  saver.merge_in_progress = s->IsMergeInProgress();
  saver.key = &key;
  saver.value = value;
  saver.columns = columns;
  saver.timestamp = timestamp;
  saver.seq = kMaxSequenceNumber;
  saver.mem = this;
  saver.merge_context = merge_context;
  saver.max_covering_tombstone_seq = *max_covering_tombstone_seq;
  saver.merge_operator = moptions_.merge_operator;
  saver.logger = moptions_.info_log;
  saver.inplace_update_support = moptions_.inplace_update_support;
  saver.statistics = moptions_.statistics;
  saver.clock = clock_;
  saver.callback_ = callback;
  saver.is_blob_index = is_blob_index;
  saver.do_merge = do_merge;
  saver.allow_data_in_errors = moptions_.allow_data_in_errors;
  saver.is_zero_copy = read_opts.pinning_tls != nullptr;
  saver.needs_user_key_cmp_in_get = needs_user_key_cmp_in_get_;
  if (LIKELY(value != nullptr)) {
    value->Reset();
  }
  table_->Get(read_opts, key, &saver, SaveValue);
  *seq = saver.seq;

  // No change to value, since we have not yet found a Put/Delete
  // Propagate corruption error
  if (!saver.found_final_value && saver.merge_in_progress && !s->IsCorruption()) {
    *s = Status::MergeInProgress();
  }
  PERF_COUNTER_ADD(get_from_memtable_count, 1);
  return saver.found_final_value;
}

void MemTable::MultiGet(const ReadOptions& read_options, MultiGetRange* range,
                        ReadCallback* callback, bool immutable_memtable) {
  // The sequence number is updated synchronously in version_set.h
  if (IsEmpty()) {
    // Avoiding recording stats for speed.
    return;
  }
  PERF_TIMER_GUARD(get_from_memtable_time);

  // For now, memtable Bloom filter is effectively disabled if there are any
  // range tombstones. This is the simplest way to ensure range tombstones are
  // handled. TODO: allow Bloom checks where max_covering_tombstone_seq==0
  bool no_range_del = read_options.ignore_range_deletions ||
                      is_range_del_table_empty_.load(std::memory_order_relaxed);
  MultiGetRange temp_range(*range, range->begin(), range->end());
  if (bloom_filter_ && no_range_del) {
    bool whole_key =
        !prefix_extractor_ || moptions_.memtable_whole_key_filtering;
    std::array<Slice, MultiGetContext::MAX_BATCH_SIZE> bloom_keys;
    std::array<bool, MultiGetContext::MAX_BATCH_SIZE> may_match;
    std::array<size_t, MultiGetContext::MAX_BATCH_SIZE> range_indexes;
    int num_keys = 0;
    for (auto iter = temp_range.begin(); iter != temp_range.end(); ++iter) {
      if (whole_key) {
        bloom_keys[num_keys] = iter->ukey_without_ts;
        range_indexes[num_keys++] = iter.index();
      } else if (prefix_extractor_->InDomain(iter->ukey_without_ts)) {
        bloom_keys[num_keys] =
            prefix_extractor_->Transform(iter->ukey_without_ts);
        range_indexes[num_keys++] = iter.index();
      }
    }
    bloom_filter_->MayContain(num_keys, &bloom_keys[0], &may_match[0]);
    for (int i = 0; i < num_keys; ++i) {
      if (!may_match[i]) {
        temp_range.SkipIndex(range_indexes[i]);
        PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
      } else {
        PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
      }
    }
  }
  for (auto iter = temp_range.begin(); iter != temp_range.end(); ++iter) {
    if (!no_range_del) {
      std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
          NewRangeTombstoneIteratorInternal(
              read_options, GetInternalKeySeqno(iter->lkey->internal_key()),
              immutable_memtable));
      SequenceNumber covering_seq =
          range_del_iter->MaxCoveringTombstoneSeqnum(iter->lkey->user_key());
      if (covering_seq > iter->max_covering_tombstone_seq) {
        iter->max_covering_tombstone_seq = covering_seq;
        if (iter->timestamp) {
          // Will be overwritten in SaveValue() if there is a point key with
          // a higher seqno.
          iter->timestamp->assign(range_del_iter->timestamp().data(),
                                  range_del_iter->timestamp().size());
        }
      }
    }
    Saver saver;
    saver.status = iter->s;
    saver.found_final_value = false;
    saver.merge_in_progress = iter->s->IsMergeInProgress();
    saver.key = iter->lkey;
    saver.value = iter->value; // not null
    if (saver.value)
      saver.value->Reset();
    saver.columns = iter->columns;
    saver.timestamp = iter->timestamp;
    saver.seq = kMaxSequenceNumber; // dummy_seq
    saver.mem = this;
    saver.merge_context = &(iter->merge_context);
    saver.max_covering_tombstone_seq = iter->max_covering_tombstone_seq;
    saver.merge_operator = moptions_.merge_operator;
    saver.logger = moptions_.info_log;
    saver.inplace_update_support = moptions_.inplace_update_support;
    saver.statistics = moptions_.statistics;
    saver.clock = clock_;
    saver.callback_ = callback;
    saver.is_blob_index = &iter->is_blob_index;
    saver.do_merge = true;
    saver.allow_data_in_errors = moptions_.allow_data_in_errors;
    saver.is_zero_copy = read_options.pinning_tls != nullptr;
    saver.needs_user_key_cmp_in_get = needs_user_key_cmp_in_get_;
    table_->Get(read_options, *(iter->lkey), &saver, SaveValue);

    if (!saver.found_final_value && saver.merge_in_progress) {
      *(iter->s) = Status::MergeInProgress();
    }

    if (saver.found_final_value) {
      if (iter->value) {
        range->AddValueSize(iter->value->size());
      } else {
        assert(iter->columns);
        range->AddValueSize(iter->columns->serialized_size());
      }

      range->MarkKeyDone(iter);
      RecordTick(moptions_.statistics, MEMTABLE_HIT);
      if (range->GetValueSize() > read_options.value_size_soft_limit) {
        // Set all remaining keys in range to Abort
        for (auto range_iter = range->begin(); range_iter != range->end();
             ++range_iter) {
          range->MarkKeyDone(range_iter);
          *(range_iter->s) = Status::Aborted();
        }
        break;
      }
    }
  }
  PERF_COUNTER_ADD(get_from_memtable_count, 1);
}

Status MemTable::Update(SequenceNumber seq, ValueType value_type,
                        const Slice& key, const Slice& value,
                        const ProtectionInfoKVOS64* kv_prot_info) {
  assert(moptions_.inplace_update_support);
  LookupKey lkey(key, seq);

  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(lkey.internal_key(), lkey.memtable_key_data());

  if (iter->Valid()) {
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    auto [internal_key, prev_value] = iter->GetKeyValue();
    size_t key_length = internal_key.size();
    const char* key_ptr = internal_key.data();
    assert(key_length >= 8);
    if (comparator_.comparator.user_comparator()->Equal(
            Slice(key_ptr, key_length - 8), lkey.user_key())) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      ValueType type;
      SequenceNumber existing_seq;
      UnPackSequenceAndType(tag, &existing_seq, &type);
      assert(existing_seq != seq);
      if (type == value_type) {
        uint32_t prev_size = static_cast<uint32_t>(prev_value.size());
        uint32_t new_size = static_cast<uint32_t>(value.size());

        // Update value, if new value size <= previous value size
        if (new_size <= prev_size) {
          char* p =
              const_cast<char*>(prev_value.data()) - VarintLength(prev_size);
          WriteLock wl(GetLock(lkey.user_key()));
          p = EncodeVarint32(p, new_size);
          memcpy(p, value.data(), value.size());
          RecordTick(moptions_.statistics, NUMBER_KEYS_UPDATED);
          if (kv_prot_info != nullptr) {
            ProtectionInfoKVOS64 updated_kv_prot_info(*kv_prot_info);
            // `seq` is swallowed and `existing_seq` prevails.
            updated_kv_prot_info.UpdateS(seq, existing_seq);
            Slice ikey = lkey.internal_key();
            return VerifyEncodedEntry(ikey, value, updated_kv_prot_info);
          }
          return Status::OK();
        }
      }
    }
  }

  // The latest value is not value_type or key doesn't exist
  return Add(seq, value_type, key, value, kv_prot_info);
}

Status MemTable::UpdateCallback(SequenceNumber seq, const Slice& key,
                                const Slice& delta,
                                const ProtectionInfoKVOS64* kv_prot_info) {
  assert(moptions_.inplace_update_support);
  LookupKey lkey(key, seq);

  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(lkey.internal_key(), lkey.memtable_key_data());

  if (iter->Valid()) {
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    auto [internal_key, prev_value] = iter->GetKeyValue();
    size_t key_length = internal_key.size();
    const char* key_ptr = internal_key.data();
    assert(key_length >= 8);
    if (comparator_.comparator.user_comparator()->Equal(
            Slice(key_ptr, key_length - 8), lkey.user_key())) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      ValueType type;
      uint64_t existing_seq;
      UnPackSequenceAndType(tag, &existing_seq, &type);
      if (type == kTypeValue) {
        uint32_t prev_size = static_cast<uint32_t>(prev_value.size());

        char* prev_buffer = const_cast<char*>(prev_value.data());
        uint32_t new_prev_size = prev_size;

        std::string str_value;
        WriteLock wl(GetLock(lkey.user_key()));
        auto status = moptions_.inplace_callback(prev_buffer, &new_prev_size,
                                                 delta, &str_value);
        if (status == UpdateStatus::UPDATED_INPLACE) {
          // Value already updated by callback.
          char* p = prev_buffer - VarintLength(prev_size);
          assert(new_prev_size <= prev_size);
          if (new_prev_size < prev_size) {
            // overwrite the new prev_size
            p = EncodeVarint32(p, new_prev_size);
            if (p < prev_buffer) {
              // shift the value buffer as well.
              memmove(p, prev_buffer, new_prev_size);
              prev_buffer = p;
            }
          }
          RecordTick(moptions_.statistics, NUMBER_KEYS_UPDATED);
          UpdateFlushState();
          if (kv_prot_info != nullptr) {
            ProtectionInfoKVOS64 updated_kv_prot_info(*kv_prot_info);
            // `seq` is swallowed and `existing_seq` prevails.
            updated_kv_prot_info.UpdateS(seq, existing_seq);
            updated_kv_prot_info.UpdateV(delta,
                                         Slice(prev_buffer, new_prev_size));
            Slice ikey = lkey.internal_key();
            Slice value(p, new_prev_size); // new value without size prefix
            return VerifyEncodedEntry(ikey, value, updated_kv_prot_info);
          }
          return Status::OK();
        } else if (status == UpdateStatus::UPDATED) {
          Status s;
          if (kv_prot_info != nullptr) {
            ProtectionInfoKVOS64 updated_kv_prot_info(*kv_prot_info);
            updated_kv_prot_info.UpdateV(delta, str_value);
            s = Add(seq, kTypeValue, key, Slice(str_value),
                    &updated_kv_prot_info);
          } else {
            s = Add(seq, kTypeValue, key, Slice(str_value),
                    nullptr /* kv_prot_info */);
          }
          RecordTick(moptions_.statistics, NUMBER_KEYS_WRITTEN);
          UpdateFlushState();
          return s;
        } else if (status == UpdateStatus::UPDATE_FAILED) {
          // `UPDATE_FAILED` is named incorrectly. It indicates no update
          // happened. It does not indicate a failure happened.
          UpdateFlushState();
          return Status::OK();
        }
      }
    }
  }
  // The latest value is not `kTypeValue` or key doesn't exist
  return Status::NotFound();
}

size_t MemTable::CountSuccessiveMergeEntries(const LookupKey& key) {
  // A total ordered iterator is costly for some memtablerep (prefix aware
  // reps). By passing in the user key, we allow efficient iterator creation.
  // The iterator only needs to be ordered within the same user key.
  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(key.internal_key(), key.memtable_key_data());

  size_t num_successive_merges = 0;

  for (; iter->Valid(); iter->Next()) {
    Slice internal_key = iter->GetKey();
    size_t key_length = internal_key.size();
    const char* iter_key_ptr = internal_key.data();
    if (!comparator_.comparator.user_comparator()->Equal(
            Slice(iter_key_ptr, key_length - 8), key.user_key())) {
      break;
    }

    const uint64_t tag = DecodeFixed64(iter_key_ptr + key_length - 8);
    ValueType type;
    uint64_t unused;
    UnPackSequenceAndType(tag, &unused, &type);
    if (type != kTypeMerge) {
      break;
    }

    ++num_successive_merges;
  }

  return num_successive_merges;
}

MemTableRep::KeyValuePair::KeyValuePair(const char* key)
  : ikey(GetLengthPrefixedSlice(key)),
    value(GetLengthPrefixedSlice(ikey.end())) {}

Slice MemTableRep::Iterator::GetKey() const {
  assert(Valid());
  return GetLengthPrefixedSlice(key());
}

Slice MemTableRep::Iterator::GetValue() const {
  assert(Valid());
  Slice k = GetLengthPrefixedSlice(key());
  return GetLengthPrefixedSlice(k.data() + k.size());
}
std::pair<Slice, Slice> MemTableRep::Iterator::GetKeyValue() const {
  assert(Valid());
  Slice k = GetLengthPrefixedSlice(key());
  Slice v = GetLengthPrefixedSlice(k.data() + k.size());
  return {k, v};
}

void MemTable::RefLogContainingPrepSection(uint64_t log) {
  assert(log > 0);
  auto cur = min_prep_log_referenced_.load();
  while ((log < cur || cur == 0) &&
         !min_prep_log_referenced_.compare_exchange_strong(cur, log)) {
    cur = min_prep_log_referenced_.load();
  }
}

uint64_t MemTable::GetMinLogContainingPrepSection() {
  return min_prep_log_referenced_.load();
}

}  // namespace ROCKSDB_NAMESPACE
