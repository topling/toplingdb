//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
BaseDeltaIterator::BaseDeltaIterator(ColumnFamilyHandle* column_family,
                                     Iterator* base_iterator,
                                     WBWIIterator* delta_iterator,
                                     const Comparator* comparator,
                                     const ReadOptions* read_options)
    : forward_(true),
      current_at_base_(true),
      equal_keys_(false),
      status_(Status::OK()),
      base_iterator_(base_iterator),
      delta_iterator_(delta_iterator),
      comparator_(comparator),
      iterate_upper_bound_(read_options ? read_options->iterate_upper_bound
                                        : nullptr) {
  assert(comparator_);
  wbwii_.reset(new WriteBatchWithIndexInternal(column_family));
  delta_valid_ = false;
  opt_cmp_type_ = comparator->opt_cmp_type();
}

ROCKSDB_FLATTEN
bool BaseDeltaIterator::Valid() const {
  return status_.ok() ? (current_at_base_ ? BaseValid() : DeltaValid()) : false;
}

void BaseDeltaIterator::SeekToFirst() {
  forward_ = true;
  base_iterator_->SeekToFirst();
  delta_iterator_->SeekToFirst();
  delta_valid_ = delta_iterator_->Valid();
  UpdateCurrent(true);
}

void BaseDeltaIterator::SeekToLast() {
  forward_ = false;
  base_iterator_->SeekToLast();
  delta_iterator_->SeekToLast();
  delta_valid_ = delta_iterator_->Valid();
  UpdateCurrent(false);
}

void BaseDeltaIterator::Seek(const Slice& k) {
  forward_ = true;
  base_iterator_->Seek(k);
  delta_iterator_->Seek(k);
  delta_valid_ = delta_iterator_->Valid();
  UpdateCurrent(true);
}

void BaseDeltaIterator::SeekForPrev(const Slice& k) {
  forward_ = false;
  base_iterator_->SeekForPrev(k);
  delta_iterator_->SeekForPrev(k);
  delta_valid_ = delta_iterator_->Valid();
  UpdateCurrent(false);
}

void BaseDeltaIterator::Next() {
#if 0
  if (UNLIKELY(!Valid())) {
    status_ = Status::NotSupported("Next() on invalid iterator");
    return;
  }
#else
  assert(Valid());
#endif

  if (UNLIKELY(!forward_)) {
    // Need to change direction
    // if our direction was backward and we're not equal, we have two states:
    // * both iterators are valid: we're already in a good state (current
    // shows to smaller)
    // * only one iterator is valid: we need to advance that iterator
    forward_ = true;
    equal_keys_ = false;
    if (!BaseValid()) {
      assert(DeltaValid());
      base_iterator_->SeekToFirst();
    } else if (!DeltaValid()) {
      delta_iterator_->SeekToFirst();
      delta_valid_ = delta_iterator_->Valid();
    } else if (current_at_base_) {
      // Change delta from larger than base to smaller
      AdvanceDelta(true);
    } else {
      // Change base from larger than delta to smaller
      AdvanceBase(true);
    }
    if (DeltaValid() && BaseValid()) {
      if (0 == comparator_->CompareWithoutTimestamp(
                   delta_iterator_->user_key(), /*a_has_ts=*/false,
                   base_iterator_->key(), /*b_has_ts=*/false)) {
        equal_keys_ = true;
      }
    }
  }
  Advance(true);
}

void BaseDeltaIterator::Prev() {
#if 0
  if (UNLIKELY(!Valid())) {
    status_ = Status::NotSupported("Prev() on invalid iterator");
    return;
  }
#else
  assert(Valid());
#endif

  if (UNLIKELY(forward_)) {
    // Need to change direction
    // if our direction was backward and we're not equal, we have two states:
    // * both iterators are valid: we're already in a good state (current
    // shows to smaller)
    // * only one iterator is valid: we need to advance that iterator
    forward_ = false;
    equal_keys_ = false;
    if (!BaseValid()) {
      assert(DeltaValid());
      base_iterator_->SeekToLast();
    } else if (!DeltaValid()) {
      delta_iterator_->SeekToLast();
      delta_valid_ = delta_iterator_->Valid();
    } else if (current_at_base_) {
      // Change delta from less advanced than base to more advanced
      AdvanceDelta(false);
    } else {
      // Change base from less advanced than delta to more advanced
      AdvanceBase(false);
    }
    if (DeltaValid() && BaseValid()) {
      if (0 == comparator_->CompareWithoutTimestamp(
                   delta_iterator_->user_key(), /*a_has_ts=*/false,
                   base_iterator_->key(), /*b_has_ts=*/false)) {
        equal_keys_ = true;
      }
    }
  }

  Advance(false);
}

Slice BaseDeltaIterator::key() const {
  return current_at_base_ ? base_iterator_->key()
                          : delta_iterator_->user_key();
}

Slice BaseDeltaIterator::value() const {
  if (current_at_base_) {
    return base_iterator_->value();
  } else {
    WriteEntry delta_entry = delta_iterator_->Entry();
    if (wbwii_->GetNumOperands() == 0) {
      return delta_entry.value;
    } else if (delta_entry.type == kDeleteRecord ||
               delta_entry.type == kSingleDeleteRecord) {
      status_ =
          wbwii_->MergeKey(delta_entry.key, nullptr, merge_result_.GetSelf());
    } else if (delta_entry.type == kPutRecord) {
      status_ = wbwii_->MergeKey(delta_entry.key, &delta_entry.value,
                                 merge_result_.GetSelf());
    } else if (delta_entry.type == kMergeRecord) {
      if (equal_keys_) {
        Slice base_value = base_iterator_->value();
        status_ = wbwii_->MergeKey(delta_entry.key, &base_value,
                                   merge_result_.GetSelf());
      } else {
        status_ =
            wbwii_->MergeKey(delta_entry.key, nullptr, merge_result_.GetSelf());
      }
    }
    merge_result_.PinSelf();
    return merge_result_;
  }
}

bool BaseDeltaIterator::PrepareValue() {
  if (current_at_base_) {
    return base_iterator_->PrepareValue();
  } else {
    return true;
  }
}

Status BaseDeltaIterator::status() const {
  if (!status_.ok()) {
    return status_;
  }
  if (!base_iterator_->status().ok()) {
    return base_iterator_->status();
  }
  return delta_iterator_->status();
}

Status BaseDeltaIterator::Refresh(const Snapshot* snap, bool keep_iter_pos) {
  return base_iterator_->Refresh(snap, keep_iter_pos);
}

void BaseDeltaIterator::Invalidate(Status s) { status_ = s; }

void BaseDeltaIterator::AssertInvariants() {
#ifndef NDEBUG
  bool not_ok = false;
  if (!base_iterator_->status().ok()) {
    assert(!base_iterator_->Valid());
    not_ok = true;
  }
  if (!delta_iterator_->status().ok()) {
    assert(!delta_valid_);
    assert(!delta_iterator_->Valid());
    not_ok = true;
  }
  if (not_ok) {
    assert(!Valid());
    assert(!status().ok());
    return;
  }

  if (!Valid()) {
    return;
  }
  if (!BaseValid()) {
    assert(!current_at_base_ && delta_iterator_->Valid());
    return;
  }
  if (!DeltaValid()) {
    assert(current_at_base_ && base_iterator_->Valid());
    return;
  }
  // we don't support those yet
  assert(delta_iterator_->Entry().type != kMergeRecord &&
         delta_iterator_->Entry().type != kLogDataRecord);
  int compare = comparator_->CompareWithoutTimestamp(
      delta_iterator_->Entry().key, /*a_has_ts=*/false, base_iterator_->key(),
      /*b_has_ts=*/false);
  if (forward_) {
    // current_at_base -> compare < 0
    assert(!current_at_base_ || compare < 0);
    // !current_at_base -> compare <= 0
    assert(current_at_base_ && compare >= 0);
  } else {
    // current_at_base -> compare > 0
    assert(!current_at_base_ || compare > 0);
    // !current_at_base -> compare <= 0
    assert(current_at_base_ && compare <= 0);
  }
  // equal_keys_ <=> compare == 0
  assert((equal_keys_ || compare != 0) && (!equal_keys_ || compare == 0));
#endif
}

ROCKSDB_FLATTEN
void BaseDeltaIterator::Advance(bool const_forward) {
  if (UNLIKELY(equal_keys_)) {
    assert(BaseValid() && DeltaValid());
    AdvanceBase(const_forward);
    AdvanceDelta(const_forward);
  } else {
    if (LIKELY(current_at_base_)) {
      assert(BaseValid());
      AdvanceBase(const_forward);
    } else {
      assert(DeltaValid());
      AdvanceDelta(const_forward);
    }
  }
  UpdateCurrent(const_forward);
}

inline static bool AdvanceIter(WBWIIterator* i, bool forward) {
  if (forward) {
    return i->NextKey();
  } else {
    return i->PrevKey();
  }
}
inline static void AdvanceIter(Iterator* i, bool forward) {
  if (forward) {
    i->Next();
  } else {
    i->Prev();
  }
}

inline void BaseDeltaIterator::AdvanceDelta(bool const_forward) {
  assert(const_forward == forward_);
  if (const_forward) {
    delta_valid_ = delta_iterator_->NextKey();
  } else {
    delta_valid_ = delta_iterator_->PrevKey();
  }
}
inline void BaseDeltaIterator::AdvanceBase(bool const_forward) {
  assert(const_forward == forward_);
  if (const_forward) {
    base_iterator_->Next();
  } else {
    base_iterator_->Prev();
  }
}

inline bool BaseDeltaIterator::BaseValid() const {
  return base_iterator_->Valid();
}
inline bool BaseDeltaIterator::DeltaValid() const {
  assert(delta_iterator_->Valid() == delta_valid_);
  return delta_valid_;
}

struct BDI_BytewiseCmpNoTS {
  int compare(const Slice& x, const Slice& y) const { return x.compare(y); }
};
struct BDI_RevBytewiseCmpNoTS {
  int compare(const Slice& x, const Slice& y) const { return y.compare(x); }
};
struct BDI_VirtualCmpNoTS {
  int compare(const Slice& x, const Slice& y) const {
    return cmp->CompareWithoutTimestamp(x, false, y, false);
  }
  const Comparator* cmp;
};

ROCKSDB_FLATTEN
void BaseDeltaIterator::UpdateCurrent(bool const_forward) {
  if (0 == opt_cmp_type_)
    UpdateCurrentTpl(const_forward, BDI_BytewiseCmpNoTS());
  else if (1 == opt_cmp_type_)
    UpdateCurrentTpl(const_forward, BDI_RevBytewiseCmpNoTS());
  else
    UpdateCurrentTpl(const_forward, BDI_VirtualCmpNoTS{comparator_});
}
template<class CmpNoTS>
void BaseDeltaIterator::UpdateCurrentTpl(bool const_forward, CmpNoTS cmp) {
// Suppress false positive clang analyzer warnings.
#ifndef __clang_analyzer__
  status_.SetAsOK();
  Iterator* base_iterator_ = this->base_iterator_.get();
  WBWIIterator* delta_iterator_ = this->delta_iterator_.get();
  while (true) {
    if (LIKELY(delta_valid_)) {
      assert(delta_iterator_->status().ok());
    } else if (!delta_iterator_->status().ok()) {
      // Expose the error status and stop.
      current_at_base_ = false;
      return;
    }
    equal_keys_ = false;
    if (UNLIKELY(!base_iterator_->Valid())) {
      if (!base_iterator_->status().ok()) {
        // Expose the error status and stop.
        current_at_base_ = true;
        return;
      }

      // Base has finished.
      if (!delta_valid_) {
        // Finished
        return;
      }
      if (iterate_upper_bound_) {
        Slice delta_key = delta_iterator_->user_key();
        if (cmp.compare(delta_key, *iterate_upper_bound_) >= 0) {
          // out of upper bound -> finished.
          return;
        }
      }
      const auto delta_result =
          delta_iterator_->FindLatestUpdate(wbwii_->GetMergeContext());
      if (delta_result == WBWIIteratorImpl::kDeleted &&
          wbwii_->GetNumOperands() == 0) {
        delta_valid_ = AdvanceIter(delta_iterator_, const_forward);
      } else {
        current_at_base_ = false;
        return;
      }
    } else if (UNLIKELY(!delta_valid_)) {
      // Delta has finished.
      current_at_base_ = true;
      return;
    } else {
      Slice delta_key = delta_iterator_->user_key();
      int compare = const_forward
                  ? cmp.compare(delta_key, base_iterator_->key())
                  : cmp.compare(base_iterator_->key(), delta_key)
                  ;
      if (UNLIKELY(compare <= 0)) {  // delta is less or equal
        if (compare == 0) {
          equal_keys_ = true;
        }
        const auto delta_result =
            delta_iterator_->FindLatestUpdate(wbwii_->GetMergeContext());
        if (delta_result != WBWIIteratorImpl::kDeleted ||
            wbwii_->GetNumOperands() > 0) {
          current_at_base_ = false;
          return;
        }
        // Delta is less advanced and is delete.
        delta_valid_ = AdvanceIter(delta_iterator_, const_forward);
        if (equal_keys_) {
          AdvanceIter(base_iterator_, const_forward);
        }
      } else {
        current_at_base_ = true;
        return;
      }
    }
  }

  AssertInvariants();
#endif  // __clang_analyzer__
}

void WBWIIteratorImpl::AdvanceKey(bool forward) {
  if (Valid()) {
    Slice key = user_key();
    do {
      if (forward) {
        Next();
      } else {
        Prev();
      }
    } while (MatchesKey(column_family_id_, key));
  }
}

bool WBWIIteratorImpl::NextKey() { AdvanceKey(true); return Valid(); }

bool WBWIIteratorImpl::PrevKey() {
  AdvanceKey(false);  // Move to the tail of the previous key
  if (Valid()) {
    AdvanceKey(false);  // Move back another key.  Now we are at the start of
                        // the previous key
    if (Valid()) {      // Still a valid
      Next();           // Move forward one onto this key
    } else {
      SeekToFirst();  // Not valid, move to the start
    }
  }
  return Valid();
}

WBWIIteratorImpl::Result WBWIIterator::FindLatestUpdate(
    MergeContext* merge_context) {
  if (Valid()) {
    Slice key = user_key();
    return FindLatestUpdate(key, merge_context);
  } else {
    merge_context->Clear();  // Clear any entries in the MergeContext
    return WBWIIteratorImpl::kNotFound;
  }
}

bool WBWIIteratorImpl::EqualsKey(const Slice& key) const {
  return comparator_->CompareKey(column_family_id_, user_key(), key) == 0;
}

WBWIIteratorImpl::Result WBWIIterator::FindLatestUpdate(
    const Slice& key, MergeContext* merge_context) {
  Result result = WBWIIteratorImpl::kNotFound;
  merge_context->Clear();  // Clear any entries in the MergeContext
  // TODO(agiardullo): consider adding support for reverse iteration
  if (!Valid()) {
    return result;
  } else if (!EqualsKey(key)) {
    return result;
  } else {
    // We want to iterate in the reverse order that the writes were added to the
    // batch.  Since we don't have a reverse iterator, we must seek past the
    // end. We do this by seeking to the next key, and then back one step
    NextKey();
    if (Valid()) {
      Prev();
    } else {
      SeekToLast();
    }

    // We are at the end of the iterator for this key.  Search backwards for the
    // last Put or Delete, accumulating merges along the way.
    while (Valid()) {
      const WriteEntry entry = Entry();
      if (!EqualsKey(key)) {
        break;  // Unexpected error or we've reached a different next key
      }

      switch (entry.type) {
        case kPutRecord:
          return WBWIIteratorImpl::kFound;
        case kDeleteRecord:
          return WBWIIteratorImpl::kDeleted;
        case kSingleDeleteRecord:
          return WBWIIteratorImpl::kDeleted;
        case kMergeRecord:
          result = WBWIIteratorImpl::kMergeInProgress;
          merge_context->PushOperand(entry.value);
          break;
        case kLogDataRecord:
          break;  // ignore
        case kXIDRecord:
          break;  // ignore
        default:
          return WBWIIteratorImpl::kError;
      }  // end switch statement
      Prev();
    }  // End while Valid()
    // At this point, we have been through the whole list and found no Puts or
    // Deletes. The iterator points to the previous key.  Move the iterator back
    // onto this one.
    if (Valid()) {
      Next();
    } else {
      SeekToFirst();
    }
  }
  return result;
}

Status ReadableWriteBatch::GetEntryFromDataOffset(size_t data_offset,
                                                  WriteType* type, Slice* Key,
                                                  Slice* value, Slice* blob,
                                                  Slice* xid) const {
  if (type == nullptr || Key == nullptr || value == nullptr ||
      blob == nullptr || xid == nullptr) {
    return Status::InvalidArgument("Output parameters cannot be null");
  }

  if (data_offset == GetDataSize()) {
    // reached end of batch.
    return Status::NotFound();
  }

  if (data_offset > GetDataSize()) {
    return Status::InvalidArgument("data offset exceed write batch size");
  }
  Slice input = Slice(rep_.data() + data_offset, rep_.size() - data_offset);
  char tag;
  uint32_t column_family;
  Status s = ReadRecordFromWriteBatch(&input, &tag, &column_family, Key, value,
                                      blob, xid);
  if (!s.ok()) {
    return s;
  }

  switch (tag) {
    case kTypeColumnFamilyValue:
    case kTypeValue:
      *type = kPutRecord;
      break;
    case kTypeColumnFamilyDeletion:
    case kTypeDeletion:
      *type = kDeleteRecord;
      break;
    case kTypeColumnFamilySingleDeletion:
    case kTypeSingleDeletion:
      *type = kSingleDeleteRecord;
      break;
    case kTypeColumnFamilyRangeDeletion:
    case kTypeRangeDeletion:
      *type = kDeleteRangeRecord;
      break;
    case kTypeColumnFamilyMerge:
    case kTypeMerge:
      *type = kMergeRecord;
      break;
    case kTypeLogData:
      *type = kLogDataRecord;
      break;
    case kTypeNoop:
    case kTypeBeginPrepareXID:
    case kTypeBeginPersistedPrepareXID:
    case kTypeBeginUnprepareXID:
    case kTypeEndPrepareXID:
    case kTypeCommitXID:
    case kTypeRollbackXID:
      *type = kXIDRecord;
      break;
    default:
      return Status::Corruption("unknown WriteBatch tag ",
                                std::to_string(static_cast<unsigned int>(tag)));
  }
  return Status::OK();
}

// If both of `entry1` and `entry2` point to real entry in write batch, we
// compare the entries as following:
// 1. first compare the column family, the one with larger CF will be larger;
// 2. Inside the same CF, we first decode the entry to find the key of the entry
//    and the entry with larger key will be larger;
// 3. If two entries are of the same CF and key, the one with larger offset
//    will be larger.
// Some times either `entry1` or `entry2` is dummy entry, which is actually
// a search key. In this case, in step 2, we don't go ahead and decode the
// entry but use the value in WriteBatchIndexEntry::search_key.
// One special case is WriteBatchIndexEntry::key_size is kFlagMinInCf.
// This indicate that we are going to seek to the first of the column family.
// Once we see this, this entry will be smaller than all the real entries of
// the column family.
int WriteBatchEntryComparator::operator()(
    const WriteBatchIndexEntry* entry1,
    const WriteBatchIndexEntry* entry2) const {
  if (entry1->column_family > entry2->column_family) {
    return 1;
  } else if (entry1->column_family < entry2->column_family) {
    return -1;
  }

  // Deal with special case of seeking to the beginning of a column family
  if (entry1->is_min_in_cf()) {
    return -1;
  } else if (entry2->is_min_in_cf()) {
    return 1;
  }

  Slice key1, key2;
  if (entry1->search_key == nullptr) {
    key1 = Slice(write_batch_->Data().data() + entry1->key_offset,
                 entry1->key_size);
  } else {
    key1 = *(entry1->search_key);
  }
  if (entry2->search_key == nullptr) {
    key2 = Slice(write_batch_->Data().data() + entry2->key_offset,
                 entry2->key_size);
  } else {
    key2 = *(entry2->search_key);
  }

  int cmp = CompareKey(entry1->column_family, key1, key2);
  if (cmp != 0) {
    return cmp;
  } else if (entry1->offset > entry2->offset) {
    return 1;
  } else if (entry1->offset < entry2->offset) {
    return -1;
  }
  return 0;
}

int WriteBatchEntryComparator::CompareKey(uint32_t column_family,
                                          const Slice& key1,
                                          const Slice& key2) const {
  if (column_family < cf_comparators_.size() &&
      cf_comparators_[column_family] != nullptr) {
    return cf_comparators_[column_family]->CompareWithoutTimestamp(
        key1, /*a_has_ts=*/false, key2, /*b_has_ts=*/false);
  } else {
    return default_comparator_->CompareWithoutTimestamp(
        key1, /*a_has_ts=*/false, key2, /*b_has_ts=*/false);
  }
}

const Comparator* WriteBatchEntryComparator::GetComparator(
    const ColumnFamilyHandle* column_family) const {
  return column_family ? column_family->GetComparator() : default_comparator_;
}

const Comparator* WriteBatchEntryComparator::GetComparator(
    uint32_t column_family) const {
  if (column_family < cf_comparators_.size() &&
      cf_comparators_[column_family]) {
    return cf_comparators_[column_family];
  }
  return default_comparator_;
}

WriteEntry WBWIIteratorImpl::Entry() const {
  WriteEntry ret;
  Slice blob, xid;
  const WriteBatchIndexEntry* iter_entry = skip_list_iter_.key();
  // this is guaranteed with Valid()
  assert(iter_entry != nullptr &&
         iter_entry->column_family == column_family_id_);
  auto s = write_batch_->GetEntryFromDataOffset(
      iter_entry->offset, &ret.type, &ret.key, &ret.value, &blob, &xid);
  assert(s.ok());
  assert(ret.type == kPutRecord || ret.type == kDeleteRecord ||
         ret.type == kSingleDeleteRecord || ret.type == kDeleteRangeRecord ||
         ret.type == kMergeRecord);
#if defined(TOPLINGDB_WITH_TIMESTAMP)
  // Make sure entry.key does not include user-defined timestamp.
  const Comparator* const ucmp = comparator_->GetComparator(column_family_id_);
  size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz > 0) {
    ret.key = StripTimestampFromUserKey(ret.key, ts_sz);
  }
#endif
  return ret;
}

Slice WBWIIteratorImpl::user_key() const {
  return Entry().key;
}

bool WBWIIteratorImpl::MatchesKey(uint32_t cf_id, const Slice& key) {
  if (Valid()) {
    return comparator_->CompareKey(cf_id, key, user_key()) == 0;
  } else {
    return false;
  }
}

WriteBatchWithIndexInternal::WriteBatchWithIndexInternal(
    ColumnFamilyHandle* column_family)
    : db_(nullptr), db_options_(nullptr), column_family_(column_family) {}

WriteBatchWithIndexInternal::WriteBatchWithIndexInternal(
    DB* db, ColumnFamilyHandle* column_family)
    : db_(db), db_options_(nullptr), column_family_(column_family) {
  if (db_ != nullptr && column_family_ == nullptr) {
    column_family_ = db_->DefaultColumnFamily();
  }
}

WriteBatchWithIndexInternal::WriteBatchWithIndexInternal(
    const DBOptions* db_options, ColumnFamilyHandle* column_family)
    : db_(nullptr), db_options_(db_options), column_family_(column_family) {}

Status WriteBatchWithIndexInternal::MergeKey(const Slice& key,
                                             const Slice* value,
                                             const MergeContext& context,
                                             std::string* result) const {
  if (column_family_ != nullptr) {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family_);
    const auto merge_operator = cfh->cfd()->ioptions()->merge_operator.get();
    if (merge_operator == nullptr) {
      return Status::InvalidArgument(
          "Merge_operator must be set for column_family");
    } else if (db_ != nullptr) {
      const ImmutableDBOptions& immutable_db_options =
          static_cast_with_check<DBImpl>(db_->GetRootDB())
              ->immutable_db_options();
      Statistics* statistics = immutable_db_options.statistics.get();
      Logger* logger = immutable_db_options.info_log.get();
      SystemClock* clock = immutable_db_options.clock;
      return MergeHelper::TimedFullMerge(
          merge_operator, key, value, context.GetOperands(), result, logger,
          statistics, clock, /* result_operand */ nullptr,
          /* update_num_ops_stats */ false);
    } else if (db_options_ != nullptr) {
      Statistics* statistics = db_options_->statistics.get();
      Env* env = db_options_->env;
      Logger* logger = db_options_->info_log.get();
      SystemClock* clock = env->GetSystemClock().get();
      return MergeHelper::TimedFullMerge(
          merge_operator, key, value, context.GetOperands(), result, logger,
          statistics, clock, /* result_operand */ nullptr,
          /* update_num_ops_stats */ false);
    } else {
      const auto cf_opts = cfh->cfd()->ioptions();
      return MergeHelper::TimedFullMerge(
          merge_operator, key, value, context.GetOperands(), result,
          cf_opts->logger, cf_opts->stats, cf_opts->clock,
          /* result_operand */ nullptr, /* update_num_ops_stats */ false);
    }
  } else {
    return Status::InvalidArgument("Must provide a column_family");
  }
}

WBWIIteratorImpl::Result WriteBatchWithIndexInternal::GetFromBatch(
    WriteBatchWithIndex* batch, const Slice& key, MergeContext* context,
    std::string* value, Status* s) {
  s->SetAsOK();

#if 0
  std::unique_ptr<WBWIIteratorImpl> iter(
      static_cast_with_check<WBWIIteratorImpl>(
          batch->NewIterator(column_family_)));
#else // topling: use base class WBWIIterator
  std::unique_ptr<WBWIIterator> iter(batch->NewIterator(column_family_));
#endif

  // Search the iterator for this key, and updates/merges to it.
  iter->Seek(key);
  auto result = iter->FindLatestUpdate(key, context);
  if (result == WBWIIteratorImpl::kError) {
    (*s) = Status::Corruption("Unexpected entry in WriteBatchWithIndex:",
                              std::to_string(iter->Entry().type));
    return result;
  } else if (result == WBWIIteratorImpl::kNotFound) {
    return result;
  } else if (result == WBWIIteratorImpl::Result::kFound) {  // PUT
    Slice entry_value = iter->Entry().value;
    if (context->GetNumOperands() > 0) {
      *s = MergeKey(key, &entry_value, *context, value);
      if (!s->ok()) {
        result = WBWIIteratorImpl::Result::kError;
      }
    } else {
      value->assign(entry_value.data(), entry_value.size());
    }
  } else if (result == WBWIIteratorImpl::kDeleted) {
    if (context->GetNumOperands() > 0) {
      *s = MergeKey(key, nullptr, *context, value);
      if (s->ok()) {
        result = WBWIIteratorImpl::Result::kFound;
      } else {
        result = WBWIIteratorImpl::Result::kError;
      }
    }
  }
  return result;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
