//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <cstdint>
#include <string>

#include "db/db_impl/db_impl.h"
#include "db/range_del_aggregator.h"
#include "memory/arena.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/wide_columns.h"
#include "table/iterator_wrapper.h"
#include "util/autovector.h"

#include <terark/sso.hpp>

namespace ROCKSDB_NAMESPACE {
class Version;

// This file declares the factory functions of DBIter, in its original form
// or a wrapped form with class ArenaWrappedDBIter, which is defined here.
// Class DBIter, which is declared and implemented inside db_iter.cc, is
// an iterator that converts internal keys (yielded by an InternalIterator)
// that were live at the specified sequence number into appropriate user
// keys.
// Each internal key consists of a user key, a sequence number, and a value
// type. DBIter deals with multiple key versions, tombstones, merge operands,
// etc, and exposes an Iterator.
// For example, DBIter may wrap following InternalIterator:
//    user key: AAA  value: v3   seqno: 100    type: Put
//    user key: AAA  value: v2   seqno: 97     type: Put
//    user key: AAA  value: v1   seqno: 95     type: Put
//    user key: BBB  value: v1   seqno: 90     type: Put
//    user key: BBC  value: N/A  seqno: 98     type: Delete
//    user key: BBC  value: v1   seqno: 95     type: Put
// If the snapshot passed in is 102, then the DBIter is expected to
// expose the following iterator:
//    key: AAA  value: v3
//    key: BBB  value: v1
// If the snapshot passed in is 96, then it should expose:
//    key: AAA  value: v1
//    key: BBB  value: v1
//    key: BBC  value: v1
//

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter final : public Iterator {
 public:
  // The following is grossly complicated. TODO: clean it up
  // Which direction is the iterator currently moving?
  // (1) When moving forward:
  //   (1a) if current_entry_is_merged_ = false, the internal iterator is
  //        positioned at the exact entry that yields this->key(), this->value()
  //   (1b) if current_entry_is_merged_ = true, the internal iterator is
  //        positioned immediately after the last entry that contributed to the
  //        current this->value(). That entry may or may not have key equal to
  //        this->key().
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction : uint8_t { kForward, kReverse };

  // LocalStatistics contain Statistics counters that will be aggregated per
  // each iterator instance and then will be sent to the global statistics when
  // the iterator is destroyed.
  //
  // The purpose of this approach is to avoid perf regression happening
  // when multiple threads bump the atomic counters from a DBIter::Next().
  struct LocalStatistics {
    explicit LocalStatistics() { ResetCounters(); }

    void ResetCounters() {
      next_count_ = 0;
      next_found_count_ = 0;
      prev_count_ = 0;
      prev_found_count_ = 0;
      bytes_read_ = 0;
      skip_count_ = 0;
    }

    void BumpGlobalStatistics(Statistics* global_statistics) {
      RecordTick(global_statistics, NUMBER_DB_NEXT, next_count_);
      RecordTick(global_statistics, NUMBER_DB_NEXT_FOUND, next_found_count_);
      RecordTick(global_statistics, NUMBER_DB_PREV, prev_count_);
      RecordTick(global_statistics, NUMBER_DB_PREV_FOUND, prev_found_count_);
      RecordTick(global_statistics, ITER_BYTES_READ, bytes_read_);
      RecordTick(global_statistics, NUMBER_ITER_SKIP, skip_count_);
      PERF_COUNTER_ADD(iter_read_bytes, bytes_read_);
      PERF_COUNTER_ADD(iter_next_count, next_count_);
      ResetCounters();
    }

    // Map to Tickers::NUMBER_DB_NEXT
    uint64_t next_count_;
    // Map to Tickers::NUMBER_DB_NEXT_FOUND
    uint64_t next_found_count_;
    // Map to Tickers::NUMBER_DB_PREV
    uint64_t prev_count_;
    // Map to Tickers::NUMBER_DB_PREV_FOUND
    uint64_t prev_found_count_;
    // Map to Tickers::ITER_BYTES_READ
    uint64_t bytes_read_;
    // Map to Tickers::NUMBER_ITER_SKIP
    uint64_t skip_count_;
  };

  DBIter(Env* _env, const ReadOptions& read_options,
         const ImmutableOptions& ioptions,
         const MutableCFOptions& mutable_cf_options, const Comparator* cmp,
         InternalIterator* iter, const Version* version, SequenceNumber s,
         bool arena_mode, uint64_t max_sequential_skip_in_iterations,
         ReadCallback* read_callback, DBImpl* db_impl, ColumnFamilyData* cfd,
         bool expose_blob_index);

  // No copying allowed
  DBIter(const DBIter&) = delete;
  void operator=(const DBIter&) = delete;

  ~DBIter() override {
    // Release pinned data if any
    if (pinned_iters_mgr_.PinningEnabled()) {
      pinned_iters_mgr_.ReleasePinnedData();
    }
    RecordTick(statistics_, NO_ITERATOR_DELETED);
    ResetInternalKeysSkippedCounter();
    local_stats_.BumpGlobalStatistics(statistics_);
    iter_.DeleteIter(arena_mode_);
  }
  void SetIter(InternalIterator* iter) {
    assert(iter_.iter() == nullptr);
    iter_.Set(iter);
    iter_.iter()->SetPinnedItersMgr(&pinned_iters_mgr_);
  }

  bool Valid() const override {
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
    if (valid_) {
      status_.PermitUncheckedError();
    }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
    return valid_;
  }
  Slice key() const override {
    assert(valid_);
    if (timestamp_lb_) {
      return saved_key_.GetInternalKey();
    } else {
      const Slice ukey_and_ts = saved_key_.GetUserKey();
      return Slice(ukey_and_ts.data(), ukey_and_ts.size() - timestamp_size_);
    }
  }

  Slice value() const override {
    assert(valid_);
  #if defined(TOPLINGDB_WITH_WIDE_COLUMNS)
    assert(is_value_prepared_);
  #endif
    if (!is_value_prepared_) {
      auto mut = const_cast<DBIter*>(this);
      if (LIKELY(mut->iter_.PrepareAndGetValue(&mut->value_))) {
        mut->is_value_prepared_ = true;
        mut->local_stats_.bytes_read_ += value_.size_;
      } else { // Can not go on, die with message
        ROCKSDB_DIE("PrepareAndGetValue() failed, status = %s",
                    iter_.status().ToString().c_str());
      }
    }
    return value_;
  }

  // without PrepareValue, user can not check iter_.PrepareAndGetValue(),
  // thus must die in DBIter::value() if iter_.PrepareAndGetValue() fails.
  bool PrepareValue() override { // enable error check for lazy load
    assert(valid_);
    if (!is_value_prepared_) {
      if (LIKELY(iter_.PrepareAndGetValue(&value_))) {
        is_value_prepared_ = true;
        local_stats_.bytes_read_ += value_.size_;
      } else {
        valid_ = false;
        status_ = iter_.status();
        ROCKSDB_VERIFY(!status_.ok());
        return false;
      }
    }
    return true;
  }

#if defined(TOPLINGDB_WITH_WIDE_COLUMNS)
  const WideColumns& columns() const override {
    assert(valid_);

    return wide_columns_;
  }
#endif

  Status status() const override {
    if (status_.ok()) {
      return iter_.status();
    } else {
      assert(!valid_);
      return status_;
    }
  }
  Slice timestamp() const override {
    assert(valid_);
    assert(timestamp_size_ > 0);
    if (direction_ == kReverse) {
      return saved_timestamp_;
    }
    const Slice ukey_and_ts = saved_key_.GetUserKey();
    assert(timestamp_size_ < ukey_and_ts.size());
    return ExtractTimestampFromUserKey(ukey_and_ts, timestamp_size_);
  }
  bool IsBlob() const {
    assert(valid_);
    return is_blob_;
  }

  Status GetProperty(std::string prop_name, std::string* prop) override;

  void Next() final override;
  void Prev() final override;
  // 'target' does not contain timestamp, even if user timestamp feature is
  // enabled.
  void Seek(const Slice& target) final override;
  void SeekForPrev(const Slice& target) final override;
  void SeekToFirst() final override;
  void SeekToLast() final override;
  Env* env() const { return env_; }
  uint64_t get_sequence() const { return sequence_; }
  void set_sequence(uint64_t s) {
    sequence_ = s;
    if (read_callback_) {
      read_callback_->Refresh(s);
    }
  }
  void set_valid(bool v) { valid_ = v; }
  void UpdateCounters();

  enum TriBool { kFalse, kTrue, kUnknown };

 private:
  // For all methods in this block:
  // PRE: iter_->Valid() && status_.ok()
  // Return false if there was an error, and status() is non-ok, valid_ = false;
  // in this case callers would usually stop what they were doing and return.
  bool ReverseToForward();
  bool ReverseToBackward();
  // Set saved_key_ to the seek key to target, with proper sequence number set.
  // It might get adjusted if the seek key is smaller than iterator lower bound.
  // target does not have timestamp.
  void SetSavedKeyToSeekTarget(const Slice& target);
  // Set saved_key_ to the seek key to target, with proper sequence number set.
  // It might get adjusted if the seek key is larger than iterator upper bound.
  // target does not have timestamp.
  void SetSavedKeyToSeekForPrevTarget(const Slice& target);
  bool FindValueForCurrentKey();
  bool FindValueForCurrentKeyUsingSeek();
  bool FindUserKeyBeforeSavedKey();
  // If `skipping_saved_key` is true, the function will keep iterating until it
  // finds a user key that is larger than `saved_key_`.
  // If `prefix` is not null, the iterator needs to stop when all keys for the
  // prefix are exhausted and the iterator is set to invalid.
  bool FindNextUserEntry(bool skipping_saved_key, const Slice* prefix);
  template<bool HasPrefix, bool HasUpperBound, TriBool MayHasCallback, size_t FixLen, class CmpNoTS>
  bool FindNextUserEntryInternalTmpl(bool, const Slice* prefix);
  bool ParseKey(ParsedInternalKey* key);
  bool MergeValuesNewToOld();

  // If prefix is not null, we need to set the iterator to invalid if no more
  // entry can be found within the prefix.
  void PrevInternal(const Slice* prefix);
  bool TooManyInternalKeysSkipped(bool increment = true);
  template<TriBool MayHasCallback = kUnknown>
  bool IsVisible(SequenceNumber sequence, const Slice& ts,
                 bool* more_recent = nullptr);

  // Temporarily pin the blocks that we encounter until ReleaseTempPinnedData()
  // is called
  void TempPinData() {
    if (!pin_thru_lifetime_) {
      pinned_iters_mgr_.StartPinning();
    }
  }

  // Release blocks pinned by TempPinData()
  void ReleaseTempPinnedData() {
    if (!pin_thru_lifetime_ && pinned_iters_mgr_.PinningEnabled()) {
      pinned_iters_mgr_.ReleasePinnedData();
    }
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  inline void ResetInternalKeysSkippedCounter() {
    local_stats_.skip_count_ += num_internal_keys_skipped_;
    if (valid_) {
      local_stats_.skip_count_--;
    }
    num_internal_keys_skipped_ = 0;
  }

  bool expect_total_order_inner_iter() {
    assert(expect_total_order_inner_iter_ || prefix_extractor_ != nullptr);
    return expect_total_order_inner_iter_;
  }

  // If lower bound of timestamp is given by ReadOptions.iter_start_ts, we need
  // to return versions of the same key. We cannot just skip if the key value
  // is the same but timestamps are different but fall in timestamp range.
  inline int CompareKeyForSkip(const Slice& a, const Slice& b) {
    return timestamp_lb_ != nullptr
               ? user_comparator_.Compare(a, b)
               : user_comparator_.CompareWithoutTimestamp(a, b);
  }

  template<class CmpNoTS>
  inline bool CmpKeyForSkip(const Slice& a, const Slice& b, const CmpNoTS& c) {
    return timestamp_lb_ != nullptr
               ? user_comparator_.Compare(a, b) < 0
               : c(a, b);
  }

  template<class CmpNoTS>
  inline bool EqKeyForSkip(const Slice& a, const Slice& b, const CmpNoTS& c) {
    return timestamp_lb_ != nullptr // semantic exactly same with origin code
               ? user_comparator_.Compare(a, b) >= 0 // ^^^^^^^^^^^^^^^^^^^^^
               : c.equal(a, b);
  }

  // Retrieves the blob value for the specified user key using the given blob
  // index when using the integrated BlobDB implementation.
  bool SetBlobValueIfNeeded(const Slice& user_key, const Slice& blob_index);

  void ResetBlobValue() {
    is_blob_ = false;
    blob_value_.Reset();
  }

  void SetValueAndColumnsFromPlain(const Slice& slice) {
    assert(value_.empty());
    value_ = slice;

#if defined(TOPLINGDB_WITH_WIDE_COLUMNS)
    assert(wide_columns_.empty());
    wide_columns_.emplace_back(kDefaultWideColumnName, slice);
#endif
  }

  bool SetValueAndColumnsFromEntity(Slice slice);

  void ResetValueAndColumns() {
    value_.clear();
#if defined(TOPLINGDB_WITH_WIDE_COLUMNS)
    wide_columns_.clear();
#endif
  }

  // If user-defined timestamp is enabled, `user_key` includes timestamp.
  bool Merge(const Slice* val, const Slice& user_key);
  bool MergeEntity(const Slice& entity, const Slice& user_key);

  const SliceTransform* prefix_extractor_;
  Env* const env_;
#if !defined(CLOCK_MONOTONIC) || defined(ROCKSDB_UNIT_TEST)
  SystemClock* clock_;
#else
  static constexpr SystemClock* clock_ = nullptr;
#endif
  Logger* logger_;
  UserComparatorWrapper user_comparator_;
  const MergeOperator* const merge_operator_;
  IteratorWrapper iter_;
  const Version* version_;
  ReadCallback* read_callback_;
  // Max visible sequence number. It is normally the snapshot seq unless we have
  // uncommitted data in db as in WriteUnCommitted.
  SequenceNumber sequence_;

  template<bool HasPrefix, bool HasUpperBound, TriBool MayHasCallback, size_t FixLen, class CmpNoTS>
  bool FindNextUserEntryPerf(bool skipping_saved_key, const Slice* prefix);
  void SetFuncPtr();
#if defined(_MSC_VER) || defined(__clang__)
  typedef bool (DBIter::*FindNextUserEntryFN)(bool, const Slice*);
#else
  typedef bool (*FindNextUserEntryFN)(DBIter*, bool, const Slice*);
#endif
  FindNextUserEntryFN m_find_next_entry;

#if 0
  IterKey saved_key_;
  #define ROCKSDB_TEST_PinnedDataIterator 1
#else
  #define ROCKSDB_TEST_PinnedDataIterator 0
  struct FastIterKey {
    terark::minimal_sso<64, false> key;
    void Clear() { key.clear(); }
    void SetUserKey(const Slice& uk, bool copy = true) {
      key.assign(uk.size_ + 8, [=](char* buf, size_t len) {
        memcpy(buf, uk.data_, uk.size_);
        // do not write last 8 bytes(seq + value_type)
      });
    }
    void SetUserKey(const char* uk, size_t uk_len) {
      key.risk_assign_local(uk_len + 8, [=](char* buf, size_t) {
        memcpy(buf, uk, uk_len);
        // do not write last 8 bytes(seq + value_type)
      });
    }
    void SetInternalKey(const ParsedInternalKey& ikey) {
      SetInternalKey(ikey.user_key, ikey.sequence, ikey.type);
    }
    void SetInternalKey(const Slice& uk, uint64_t seq,
                        ValueType vt = kValueTypeForSeek,
                        const Slice* ts = nullptr) {
      if (ts) {
        key.assign(uk.size_ + ts->size_ + 8, [=](char* buf, size_t len) {
          memcpy(buf, uk.data_, uk.size_);
          memcpy(buf + uk.size_, ts->data_, ts->size_);
          rocksdb::EncodeFixed64(buf + len - 8, PackSequenceAndType(seq, vt));
        });
      } else {
        key.assign(uk.size_ + 8, [=](char* buf, size_t len) {
          memcpy(buf, uk.data_, uk.size_);
          rocksdb::EncodeFixed64(buf + uk.size_, PackSequenceAndType(seq, vt));
        });
      }
    }
    void UpdateInternalKey(uint64_t seq, ValueType vt,
                           const Slice* ts = nullptr) {
      char* end = key.end();
      if (ts) {
        ROCKSDB_ASSERT_GE(key.size(), 8 + ts->size_);
        memcpy(end - 8 - ts->size_, ts->data_, ts->size_);
      } else {
        ROCKSDB_ASSERT_GE(key.size(), 8);
      }
      rocksdb::EncodeFixed64(end - 8, PackSequenceAndType(seq, vt));
    }
    Slice GetUserKey() const { return key.notail<Slice>(8); }
    Slice GetInternalKey() const { return key.to<Slice>(); }
    size_t Size() const { return key.size() - 8; }
    bool IsKeyPinned() const { return false; }
  };
  FastIterKey saved_key_;
#endif
  // Reusable internal key data structure. This is only used inside one function
  // and should not be used across functions. Reusing this object can reduce
  // overhead of calling construction of the function if creating it each time.
  //ParsedInternalKey ikey_;
  std::string saved_value_;
  //Slice pinned_value_;
  // for prefix seek mode to support prev()
  PinnableSlice blob_value_;
  // Value of the default column
  Slice value_;
#if defined(TOPLINGDB_WITH_WIDE_COLUMNS)
  // All columns (i.e. name-value pairs)
  WideColumns wide_columns_;
#endif
  Statistics* statistics_;
  uint64_t max_skip_;
  uint64_t max_skippable_internal_keys_;
  uint64_t num_internal_keys_skipped_;
  const Slice* iterate_lower_bound_;
  const Slice* iterate_upper_bound_;

  // The prefix of the seek key. It is only used when prefix_same_as_start_
  // is true and prefix extractor is not null. In Next() or Prev(), current keys
  // will be checked against this prefix, so that the iterator can be
  // invalidated if the keys in this prefix has been exhausted. Set it using
  // SetUserKey() and use it using GetUserKey().
  IterKey prefix_;

  Status status_;
  Direction direction_;
  bool valid_;
  bool is_value_prepared_;
  bool current_entry_is_merged_;
  // True if we know that the current entry's seqnum is 0.
  // This information is used as that the next entry will be for another
  // user key.
  bool is_key_seqnum_zero_;
  const bool prefix_same_as_start_;
  // Means that we will pin all data blocks we read as long the Iterator
  // is not deleted, will be true if ReadOptions::pin_data is true
#if defined(ROCKSDB_UNIT_TEST)
  const bool pin_thru_lifetime_;
#else
  static constexpr bool pin_thru_lifetime_ = false;
#endif
  // Expect the inner iterator to maintain a total order.
  // prefix_extractor_ must be non-NULL if the value is false.
  const bool expect_total_order_inner_iter_;
  ReadTier read_tier_;
  bool fill_cache_;
  bool verify_checksums_;
  // Whether the iterator is allowed to expose blob references. Set to true when
  // the stacked BlobDB implementation is used, false otherwise.
  bool expose_blob_index_;
  bool is_blob_;
  bool arena_mode_;
  bool enable_perf_timer_;
  uint8_t fixed_user_key_len_;
  // List of operands for merge operator.
  MergeContext merge_context_;
  LocalStatistics local_stats_;
  PinnedIteratorsManager pinned_iters_mgr_;
#if defined(TOPLINGDB_WITH_TIMESTAMP)
  const Slice* const timestamp_ub_;
  const Slice* const timestamp_lb_;
  const size_t timestamp_size_;
  std::string saved_timestamp_;
#else
  static constexpr const Slice* const timestamp_ub_ = nullptr;
  static constexpr const Slice* const timestamp_lb_ = nullptr;
  static constexpr size_t timestamp_size_ = 0;
  static std::string saved_timestamp_;
#endif
  DBImpl* db_impl_;
  ColumnFamilyData* cfd_;
};

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified `sequence` number
// into appropriate user keys.
extern Iterator* NewDBIterator(
    Env* env, const ReadOptions& read_options, const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options,
    const Comparator* user_key_comparator, InternalIterator* internal_iter,
    const Version* version, const SequenceNumber& sequence,
    uint64_t max_sequential_skip_in_iterations, ReadCallback* read_callback,
    DBImpl* db_impl = nullptr, ColumnFamilyData* cfd = nullptr,
    bool expose_blob_index = false);

}  // namespace ROCKSDB_NAMESPACE
