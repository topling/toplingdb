//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

#include "db/column_family.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "memtable/skiplist.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/coding.h"
#include "util/string_util.h"
#include "util/threaded_rbtree.h"

namespace rocksdb {

class Env;
class Logger;
class Statistics;

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
    case kTypeEndPrepareXID:
    case kTypeCommitXID:
    case kTypeRollbackXID:
      *type = kXIDRecord;
      break;
    default:
      return Status::Corruption("unknown WriteBatch tag");
  }
  return Status::OK();
}

Slice WriteBatchKeyExtractor::operator()(
    const WriteBatchIndexEntry* entry) const {
  if (entry->search_key == nullptr) {
    return Slice(write_batch_->Data().data() + entry->key_offset,
                 entry->key_size);
  } else {
    return *(entry->search_key);
  }
}

WriteBatchWithIndexInternal::Result WriteBatchWithIndexInternal::GetFromBatch(
    const ImmutableDBOptions& immuable_db_options, WriteBatchWithIndex* batch,
    ColumnFamilyHandle* column_family, const Slice& key,
    MergeContext* merge_context, const Comparator* cmp,
    std::string* value, bool overwrite_key, Status* s) {
  *s = Status::OK();
  WriteBatchWithIndexInternal::Result result =
      WriteBatchWithIndexInternal::Result::kNotFound;

  if (cmp == nullptr) {
    return result;
  }

  WBWIIterator::IteratorStorage iter;
  batch->NewIterator(column_family, iter);

  // We want to iterate in the reverse order that the writes were added to the
  // batch.  Since we don't have a reverse iterator, we must seek past the end.
  // TODO(agiardullo): consider adding support for reverse iteration
  iter->Seek(key);
  while (iter->Valid()) {
    const WriteEntry entry = iter->Entry();
    if (cmp->Compare(entry.key, key) != 0) {
      break;
    }

    iter->Next();
  }

  if (!(*s).ok()) {
    return WriteBatchWithIndexInternal::Result::kError;
  }

  if (!iter->Valid()) {
    // Read past end of results.  Reposition on last result.
    iter->SeekToLast();
  } else {
    iter->Prev();
  }

  Slice entry_value;
  while (iter->Valid()) {
    const WriteEntry entry = iter->Entry();
    if (cmp->Compare(entry.key, key) != 0) {
      // Unexpected error or we've reached a different next key
      break;
    }

    switch (entry.type) {
      case kPutRecord: {
        result = WriteBatchWithIndexInternal::Result::kFound;
        entry_value = entry.value;
        break;
      }
      case kMergeRecord: {
        result = WriteBatchWithIndexInternal::Result::kMergeInProgress;
        merge_context->PushOperand(entry.value);
        break;
      }
      case kDeleteRecord:
      case kSingleDeleteRecord: {
        result = WriteBatchWithIndexInternal::Result::kDeleted;
        break;
      }
      case kLogDataRecord:
      case kXIDRecord: {
        // ignore
        break;
      }
      default: {
        result = WriteBatchWithIndexInternal::Result::kError;
        (*s) = Status::Corruption("Unexpected entry in WriteBatchWithIndex:",
                                  ToString(entry.type));
        break;
      }
    }
    if (result == WriteBatchWithIndexInternal::Result::kFound ||
        result == WriteBatchWithIndexInternal::Result::kDeleted ||
        result == WriteBatchWithIndexInternal::Result::kError) {
      // We can stop iterating once we find a PUT or DELETE
      break;
    }
    if (result == WriteBatchWithIndexInternal::Result::kMergeInProgress &&
        overwrite_key == true) {
      // Since we've overwritten keys, we do not know what other operations are
      // in this batch for this key, so we cannot do a Merge to compute the
      // result.  Instead, we will simply return MergeInProgress.
      break;
    }

    iter->Prev();
  }

  if ((*s).ok()) {
    if (result == WriteBatchWithIndexInternal::Result::kFound ||
        result == WriteBatchWithIndexInternal::Result::kDeleted) {
      // Found a Put or Delete.  Merge if necessary.
      if (merge_context->GetNumOperands() > 0) {
        const MergeOperator* merge_operator;

        if (column_family != nullptr) {
          auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
          merge_operator = cfh->cfd()->ioptions()->merge_operator;
        } else {
          *s = Status::InvalidArgument("Must provide a column_family");
          result = WriteBatchWithIndexInternal::Result::kError;
          return result;
        }
        Statistics* statistics = immuable_db_options.statistics.get();
        Env* env = immuable_db_options.env;
        Logger* logger = immuable_db_options.info_log.get();

        if (merge_operator) {
          *s = MergeHelper::TimedFullMerge(merge_operator, key, &entry_value,
                                           merge_context->GetOperands(), value,
                                           logger, statistics, env);
        } else {
          *s = Status::InvalidArgument("Options::merge_operator must be set");
        }
        if ((*s).ok()) {
          result = WriteBatchWithIndexInternal::Result::kFound;
        } else {
          result = WriteBatchWithIndexInternal::Result::kError;
        }
      } else {  // nothing to merge
        if (result == WriteBatchWithIndexInternal::Result::kFound) {  // PUT
          value->assign(entry_value.data(), entry_value.size());
        }
      }
    }
  }

  return result;
}

template<bool OverwriteKey>
struct WriteBatchEntryComparator {
  int operator()(WriteBatchIndexEntry* l, WriteBatchIndexEntry* r) const {
    int cmp = c->Compare(extractor(l), extractor(r));
    if (OverwriteKey || cmp != 0) {
      return cmp;
    }
    if (l->offset > r->offset) {
      return 1;
    }
    if (l->offset < r->offset) {
      return -1;
    }
    return 0;
  }
  WriteBatchKeyExtractor extractor;
  const Comparator* c;
};

template<bool OverwriteKey>
class WriteBatchEntrySkipListIndex : public WriteBatchEntryIndex {
 protected:
  typedef WriteBatchEntryComparator<OverwriteKey> EntryComparator;
  typedef SkipList<WriteBatchIndexEntry*, const EntryComparator&> Index;
  EntryComparator comparator_;
  Index index_;
  bool overwrite_key_;

  class SkipListIterator : public WriteBatchEntryIndex::Iterator {
   public:
    SkipListIterator(Index* index) : iter_(index) {}
    typename Index::Iterator iter_;

   public:
    virtual bool Valid() const override {
      return iter_.Valid();
    }
    virtual void SeekToFirst() override {
      iter_.SeekToFirst();
    }
    virtual void SeekToLast() override {
      iter_.SeekToLast();
    }
    virtual void Seek(WriteBatchIndexEntry* target) override {
      iter_.Seek(target);
    }
    virtual void SeekForPrev(WriteBatchIndexEntry* target) override {
      iter_.SeekForPrev(target);
    }
    virtual void Next() override {
      iter_.Next();
    }
    virtual void Prev() override {
      iter_.Prev();
    }
    virtual WriteBatchIndexEntry* key() const override {
      return iter_.key();
    }
  };

 public:
  WriteBatchEntrySkipListIndex(WriteBatchKeyExtractor e, const Comparator* c,
                               Arena* a)
      : comparator_({e, c}),
        index_(comparator_, a) {
  }

  virtual Iterator* NewIterator() override {
    return new SkipListIterator(&index_);
  }
  virtual void NewIterator(IteratorStorage& storage) override {
    static_assert(sizeof(SkipListIterator) <= sizeof storage.buffer,
                  "Need larger buffer for SkipListIterator");
    storage.iter = new (storage.buffer) SkipListIterator(&index_);
  }
  virtual bool Upsert(WriteBatchIndexEntry* key) override {
    if (OverwriteKey) {
      Slice sraech_key = comparator_.extractor(key);
      WriteBatchIndexEntry search_entry(&sraech_key, key->column_family);
      typename Index::Iterator iter(&index_);
      iter.Seek(&search_entry);
      if (iter.Valid() &&
          comparator_.c->Compare(sraech_key,
                                 comparator_.extractor(iter.key())) == 0) {
        // found , replace
        std::swap(iter.key()->offset, key->offset);
        return false;
      }
    }
    index_.Insert(key);
    return true;
  }
};

WriteBatchEntryIndexFactory* WriteBatchEntrySkipListIndexFactory() {
  class SkipListIndexFactory : public WriteBatchEntryIndexFactory {
   public:
    WriteBatchEntryIndex* New(WriteBatchKeyExtractor e,
                              const Comparator* c, Arena* a,
                              bool overwrite_key) override {
      if (overwrite_key) {
        return new WriteBatchEntrySkipListIndex<true>(e, c, a);
      } else {
        return new WriteBatchEntrySkipListIndex<false>(e, c, a);
      }
    }
  };
  static SkipListIndexFactory factory;
  return &factory;
}

template<bool OverwriteKey>
class WriteBatchEntryRBTreeIndex : public WriteBatchEntryIndex {
 protected:
  typedef WriteBatchEntryComparator<OverwriteKey> EntryComparator;
  struct TrbComp {
    bool operator()(WriteBatchIndexEntry* l, WriteBatchIndexEntry* r) const {
      return comparator(l, r) < 0;
    }
    int compare(WriteBatchIndexEntry* l, WriteBatchIndexEntry* r) const {
      return comparator(l, r);
    }
    EntryComparator comparator;
  };
  typedef std::integral_constant<bool, OverwriteKey> IndexUnique;
  typedef threaded_rbtree_impl<threaded_rbtree_default_set_config_t<
                                   WriteBatchIndexEntry*, TrbComp,
                                   uint32_t, IndexUnique>> Index;
  Index index_;

  class RBTreeIterator : public WriteBatchEntryIndex::Iterator {
   public:
    RBTreeIterator(Index* index) : index_(index), where_(index->end_i()) {}
    Index* index_;
    typename Index::size_type where_;

   public:
    virtual bool Valid() const override {
      return where_ != index_->end_i();
    }
    virtual void SeekToFirst() override {
      where_ = index_->beg_i();
    }
    virtual void SeekToLast() override {
      where_ = index_->rbeg_i();
    }
    virtual void Seek(WriteBatchIndexEntry* target) override {
      where_ = index_->lwb_i(target);
    }
    virtual void SeekForPrev(WriteBatchIndexEntry* target) override {
      where_ = index_->rlwb_i(target);
    }
    virtual void Next() override {
      where_ = index_->next_i(where_);
    }
    virtual void Prev() override {
      where_ = index_->prev_i(where_);
    }
    virtual WriteBatchIndexEntry* key() const override {
      return index_->key_at(where_);
    }
  };

 public:
  WriteBatchEntryRBTreeIndex(WriteBatchKeyExtractor e, const Comparator* c,
                             Arena* a)
      : index_(TrbComp({EntryComparator({e, c})})) {
  }

  virtual Iterator* NewIterator() override {
    return new RBTreeIterator(&index_);
  }
  virtual void NewIterator(IteratorStorage& storage) override {
    static_assert(sizeof(RBTreeIterator) <= sizeof storage.buffer,
                  "Need larger buffer for RBTreeIterator");
    storage.iter = new (storage.buffer) RBTreeIterator(&index_);
  }
  virtual bool Upsert(WriteBatchIndexEntry* key) override {
    struct result_adapter {
      typename Index::iterator iter;
      bool success;

      result_adapter(std::pair<typename Index::iterator, bool> ib)
          : iter(ib.first),
            success(ib.second){}
      result_adapter(typename Index::iterator i)
          : iter(i),
            success(true){}
    } result = index_.emplace(key);
    if (!OverwriteKey || result.success) {
      return true;
    }
    WriteBatchIndexEntry* entry = *result.iter;
    std::swap(entry->offset, key->offset);
    return false;
  }
};

WriteBatchEntryIndexFactory* WriteBatchEntryRBTreeIndexFactory() {
  class RBTreeIndexFactory : public WriteBatchEntryIndexFactory {
   public:
    WriteBatchEntryIndex* New(WriteBatchKeyExtractor e,
                              const Comparator* c, Arena* a,
                              bool overwrite_key) override {
      if (overwrite_key) {
        return new WriteBatchEntryRBTreeIndex<true>(e, c, a);
      } else {
        return new WriteBatchEntryRBTreeIndex<false>(e, c, a);
      }
    }
  };
  static RBTreeIndexFactory factory;
  return &factory;
}

}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
