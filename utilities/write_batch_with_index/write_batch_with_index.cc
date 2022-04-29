//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/write_batch_with_index.h"

#include <memory>
#include <utility>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "memory/arena.h"
#include "memtable/skiplist.h"
#include "options/db_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "util/cast_util.h"
#include "util/string_util.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

namespace ROCKSDB_NAMESPACE {
struct WriteBatchWithIndex::Rep {
  explicit Rep(const Comparator* index_comparator, size_t reserved_bytes = 0,
               size_t max_bytes = 0, bool _overwrite_key = false,
               const WriteBatchEntryIndexFactory* _index_factory = nullptr)
      : write_batch(reserved_bytes, max_bytes, /*protection_bytes_per_key=*/0,
                    index_comparator ? index_comparator->timestamp_size() : 0),
        default_comparator(index_comparator),
        index_factory(_index_factory == nullptr
                          ? skip_list_WriteBatchEntryIndexFactory()
                          : _index_factory),
        overwrite_key(_overwrite_key),
        free_entry(nullptr),
        last_entry_offset(0),
        last_sub_batch_offset(0),
        sub_batch_cnt(1)
        {
          factory_context = index_factory->NewContext(&arena);
        }

  ~Rep() {
    for (auto& pair : entry_indices) {
      if (pair.index != nullptr) {
        pair.index->~WriteBatchEntryIndex();
      }
    }
    if (factory_context != nullptr) {
      factory_context->FreeMapContent();
      factory_context->~WriteBatchEntryIndexContext();
    }
  }

  ReadableWriteBatch write_batch;
  const Comparator* default_comparator;
  struct ComparatorIndexPair {
    const Comparator* comparator = nullptr;
    WriteBatchEntryIndex* index = nullptr;
  };
  std::vector<ComparatorIndexPair> entry_indices;
  Arena arena;
  const WriteBatchEntryIndexFactory* index_factory;
  WriteBatchEntryIndexContext* factory_context;
  bool overwrite_key;
  WriteBatchIndexEntry* free_entry;
  size_t last_entry_offset;
  // The starting offset of the last sub-batch. A sub-batch starts right before
  // inserting a key that is a duplicate of a key in the last sub-batch. Zero,
  // the default, means that no duplicate key is detected so far.
  size_t last_sub_batch_offset;
  // Total number of sub-batches in the write batch. Default is 1.
  size_t sub_batch_cnt;

  // Remember current offset of internal write batch, which is used as
  // the starting offset of the next record.
  void SetLastEntryOffset() { last_entry_offset = write_batch.GetDataSize(); }

  #if 0
  // In overwrite mode, find the existing entry for the same key and update it
  // to point to the current entry.
  // Return true if the key is found and updated.
  bool UpdateExistingEntry(ColumnFamilyHandle* column_family, const Slice& key,
                           WriteType type);
  bool UpdateExistingEntryWithCfId(uint32_t column_family_id, const Slice& key,
                                   WriteType type);
  #endif

  // Add the recent entry to the update.
  // In overwrite mode, if key already exists in the index, update it.
  void AddOrUpdateIndex(ColumnFamilyHandle* column_family, WriteType type);
  void AddOrUpdateIndex(uint32_t column_family_id, WriteType type);
  void AddOrUpdateIndex(WriteType type);
  void AddOrUpdateIndex(uint32_t column_family_id,
                        WriteBatchEntryIndex* entry_index,
                        WriteType type);

  // Get entry_index, create if missing
  WriteBatchEntryIndex* GetEntryIndex(ColumnFamilyHandle* column_family);
  WriteBatchEntryIndex* GetEntryIndexWithCfId(uint32_t column_family_id);
  #if 0
  void AddOrUpdateIndex(ColumnFamilyHandle* column_family, const Slice& key,
                        WriteType type);
  void AddOrUpdateIndex(const Slice& key, WriteType type);
  #endif

  #if 0
  // Allocate an index entry pointing to the last entry in the write batch and
  // put it to skip list.
  void AddNewEntry(uint32_t column_family_id);
  #endif
  // Get comparator, nullptr if missing
  const Comparator* GetComparator(ColumnFamilyHandle* column_family);
  const Comparator* GetComparatorByCfId(uint32_t cf_id);


  // Clear all updates buffered in this batch.
  void Clear();
  void ClearIndex();

  // Rebuild index by reading all records from the batch.
  // Returns non-ok status on corruption.
  Status ReBuildIndex();
};

#if 0
bool WriteBatchWithIndex::Rep::UpdateExistingEntry(
    ColumnFamilyHandle* column_family, const Slice& key, WriteType type) {
  uint32_t cf_id = GetColumnFamilyID(column_family);
  return UpdateExistingEntryWithCfId(cf_id, key, type);
}
#endif
void WriteBatchWithIndex::Rep::AddOrUpdateIndex(ColumnFamilyHandle* column_family,
                                                WriteType type) {
  AddOrUpdateIndex(GetColumnFamilyID(column_family), GetEntryIndex(column_family), type);
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(uint32_t column_family_id,
                                                WriteType type) {
  AddOrUpdateIndex(column_family_id, GetEntryIndexWithCfId(column_family_id), type);
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(WriteType type) {
  AddOrUpdateIndex(0, GetEntryIndex(nullptr), type);
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(uint32_t column_family_id,
                                                WriteBatchEntryIndex* entry_index,
                                                WriteType type) {
  assert(entry_index == GetEntryIndexWithCfId(column_family_id));
  const std::string& wb_data = write_batch.Data();
  Slice entry_ptr = Slice(wb_data.data() + last_entry_offset,
                          wb_data.size() - last_entry_offset);
  // Extract key
  Slice key;
  bool success __attribute__((__unused__));
  success = ReadKeyFromWriteBatchEntry(&entry_ptr, &key, column_family_id != 0);
  assert(success);

  void* mem = free_entry;
  if (mem == nullptr) {
    mem = arena.Allocate(sizeof(WriteBatchIndexEntry));
  }
  auto* index_entry =
      new (mem) WriteBatchIndexEntry(last_entry_offset, column_family_id,
                                     key.data() - wb_data.data(), key.size());
  
  bool isMergeRecord = overwrite_key && type == kMergeRecord;
  if (!entry_index->Upsert(index_entry , isMergeRecord)) {
    // overwrite key
    if (LIKELY(last_sub_batch_offset <= index_entry->offset)) {
      last_sub_batch_offset = last_entry_offset;
      sub_batch_cnt++;
    }
    
    if(isMergeRecord) {
      index_entry->offset = last_entry_offset;
      free_entry = nullptr;
    }
    else {
      free_entry = index_entry;
    }
  } else {
    free_entry = nullptr;
  }
}


#if 0
bool WriteBatchWithIndex::Rep::UpdateExistingEntryWithCfId(
    uint32_t column_family_id, const Slice& key, WriteType type) {
  if (!overwrite_key) {
    return false;
  }

  WBWIIteratorImpl iter(column_family_id, &skip_list, &write_batch,
                        &comparator);
  iter.Seek(key);
  if (!iter.Valid()) {
    return false;
  } else if (!iter.MatchesKey(column_family_id, key)) {
    return false;
  } else {
    // Move to the end of this key (NextKey-Prev)
    iter.NextKey();  // Move to the next key
    if (iter.Valid()) {
      iter.Prev();  // Move back one entry
    } else {
      iter.SeekToLast();
    }
  }
  WriteBatchIndexEntry* non_const_entry =
      const_cast<WriteBatchIndexEntry*>(iter.GetRawEntry());
  if (LIKELY(last_sub_batch_offset <= non_const_entry->offset)) {
    last_sub_batch_offset = last_entry_offset;
    sub_batch_cnt++;
  }
  if (type == kMergeRecord) {
    return false;
  } else {
    non_const_entry->offset = last_entry_offset;
    return true;
  }
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(
    ColumnFamilyHandle* column_family, const Slice& key, WriteType type) {
  if (!UpdateExistingEntry(column_family, key, type)) {
    uint32_t cf_id = GetColumnFamilyID(column_family);
    const auto* cf_cmp = GetColumnFamilyUserComparator(column_family);
    if (cf_cmp != nullptr) {
      comparator.SetComparatorForCF(cf_id, cf_cmp);
    }
    AddNewEntry(cf_id);
  }
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndex(const Slice& key,
                                                WriteType type) {
  if (!UpdateExistingEntryWithCfId(0, key, type)) {
    AddNewEntry(0);
  }
}

void WriteBatchWithIndex::Rep::AddNewEntry(uint32_t column_family_id) {
  const std::string& wb_data = write_batch.Data();
  Slice entry_ptr = Slice(wb_data.data() + last_entry_offset,
                          wb_data.size() - last_entry_offset);
  // Extract key
  Slice key;
  bool success =
      ReadKeyFromWriteBatchEntry(&entry_ptr, &key, column_family_id != 0);
#ifdef NDEBUG
  (void)success;
#endif
  assert(success);

  auto* mem = arena.Allocate(sizeof(WriteBatchIndexEntry));
  auto* index_entry =
      new (mem) WriteBatchIndexEntry(last_entry_offset, column_family_id,
                                      key.data() - wb_data.data(), key.size());
  skip_list.Insert(index_entry);
}
#endif 

WriteBatchEntryIndex* WriteBatchWithIndex::Rep::GetEntryIndex(
    ColumnFamilyHandle* column_family) {
  uint32_t cf_id = GetColumnFamilyID(column_family);
  if (cf_id >= entry_indices.size()) {
    entry_indices.resize(cf_id + 1);
  }
  auto index = entry_indices[cf_id].index;
  if (index == nullptr) {
    const auto* cf_cmp = GetColumnFamilyUserComparator(column_family);
    if (cf_cmp == nullptr) {
      cf_cmp = default_comparator;
    }
    entry_indices[cf_id].comparator = cf_cmp;
    entry_indices[cf_id].index = index =
        index_factory->New(factory_context, WriteBatchKeyExtractor(&write_batch),
                           cf_cmp, &arena, overwrite_key);
  }
  return index;
}

WriteBatchEntryIndex* WriteBatchWithIndex::Rep::GetEntryIndexWithCfId(
    uint32_t column_family_id) {
  assert(column_family_id < entry_indices.size());
  auto index = entry_indices[column_family_id].index;
  if (index == nullptr) {
    const auto* cf_cmp = entry_indices[column_family_id].comparator;
    assert(cf_cmp != nullptr);
    entry_indices[column_family_id].index = index =
        index_factory->New(factory_context, WriteBatchKeyExtractor(&write_batch),
                           cf_cmp, &arena, overwrite_key);
  }
  return index;
}

const Comparator* WriteBatchWithIndex::Rep::GetComparator(
    ColumnFamilyHandle* column_family) {
  uint32_t cf_id = GetColumnFamilyID(column_family);
  if (cf_id >= entry_indices.size()) {
    entry_indices.resize(cf_id + 1);
  }
  auto cf_cmp = entry_indices[cf_id].comparator;
  if (cf_cmp == nullptr) {
    cf_cmp = GetColumnFamilyUserComparator(column_family);
    if (cf_cmp == nullptr) {
      cf_cmp = default_comparator;
    }
  }
  entry_indices[cf_id].comparator = cf_cmp;
  return cf_cmp;
}

const Comparator* WriteBatchWithIndex::Rep::GetComparatorByCfId(
    uint32_t cf_id) {
  if(cf_id < entry_indices.size() &&
     entry_indices[cf_id].comparator) {
       return entry_indices[cf_id].comparator;
  }
  return default_comparator;
}

void WriteBatchWithIndex::Rep::Clear() {
  write_batch.Clear();
  ClearIndex();
}

void WriteBatchWithIndex::Rep::ClearIndex() {
  WriteBatchEntryIndexContext::map_type index_map;
  for (auto& pair : entry_indices) {
    if (pair.index != nullptr) {
      pair.index->~WriteBatchEntryIndex();
    }
  }
  if (factory_context != nullptr) {
    factory_context->ExportMap(&index_map);
    factory_context->~WriteBatchEntryIndexContext();
  }
  arena.~Arena();
  new (&arena) Arena();
  factory_context = index_factory->NewContext(&arena);
  factory_context->ImportMap(index_map);
  for (auto& pair : entry_indices) {
    if (pair.index != nullptr) {
      pair.index = index_factory->New(factory_context, WriteBatchKeyExtractor(&write_batch),
                                      pair.comparator, &arena, overwrite_key, pair.index);
    }
  }
  last_entry_offset = 0;
  last_sub_batch_offset = 0;
  sub_batch_cnt = 1;
  free_entry = nullptr;
}

Status WriteBatchWithIndex::Rep::ReBuildIndex() {
  Status s;

  ClearIndex();

  if (write_batch.Count() == 0) {
    // Nothing to re-index
    return s;
  }

  size_t offset = WriteBatchInternal::GetFirstOffset(&write_batch);

  Slice input(write_batch.Data());
  input.remove_prefix(offset);

  // Loop through all entries in Rep and add each one to the index
  uint32_t found = 0;
  while (s.ok() && !input.empty()) {
    Slice key, value, blob, xid;
    uint32_t column_family_id = 0;  // default
    char tag = 0;

    // set offset of current entry for call to AddNewEntry()
    last_entry_offset = input.data() - write_batch.Data().data();

    s = ReadRecordFromWriteBatch(&input, &tag, &column_family_id, &key,
                                  &value, &blob, &xid);
    if (!s.ok()) {
      break;
    }

    switch (tag) {
      case kTypeColumnFamilyValue:
      case kTypeValue:
        found++;
        AddOrUpdateIndex(column_family_id, kPutRecord);
        break;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        found++;
        AddOrUpdateIndex(column_family_id, kDeleteRecord);
        break;
      case kTypeColumnFamilySingleDeletion:
      case kTypeSingleDeletion:
        found++;
        AddOrUpdateIndex(column_family_id, kSingleDeleteRecord);
        break;
      case kTypeColumnFamilyMerge:
      case kTypeMerge:
        found++;
        AddOrUpdateIndex(column_family_id, kMergeRecord);
        break;
      case kTypeLogData:
      case kTypeBeginPrepareXID:
      case kTypeBeginPersistedPrepareXID:
      case kTypeBeginUnprepareXID:
      case kTypeEndPrepareXID:
      case kTypeCommitXID:
      case kTypeCommitXIDAndTimestamp:
      case kTypeRollbackXID:
      case kTypeNoop:
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag in ReBuildIndex",
                                  ToString(static_cast<unsigned int>(tag)));
    }
  }

  if (s.ok() && found != write_batch.Count()) {
    s = Status::Corruption("WriteBatch has wrong count");
  }

  return s;
}

WriteBatchWithIndex::WriteBatchWithIndex(
    const Comparator* default_index_comparator, size_t reserved_bytes,
    bool overwrite_key, size_t max_bytes, 
    const WriteBatchEntryIndexFactory* index_factory)
    : rep(new Rep(default_index_comparator, reserved_bytes, max_bytes,
                  overwrite_key, index_factory)) {}

WriteBatchWithIndex::~WriteBatchWithIndex() {}

WriteBatchWithIndex::WriteBatchWithIndex(WriteBatchWithIndex&&) = default;

WriteBatchWithIndex& WriteBatchWithIndex::operator=(WriteBatchWithIndex&&) =
    default;

WriteBatch* WriteBatchWithIndex::GetWriteBatch() { return &rep->write_batch; }

size_t WriteBatchWithIndex::SubBatchCnt() { return rep->sub_batch_cnt; }

WBWIIterator* WriteBatchWithIndex::NewIterator() {
  return new WBWIIteratorImpl(0, rep->GetEntryIndex(nullptr), &rep->write_batch,
                              rep->GetComparator(nullptr), false);
}

WBWIIterator* WriteBatchWithIndex::NewIterator(
    ColumnFamilyHandle* column_family) {
  return new WBWIIteratorImpl(GetColumnFamilyID(column_family),
                              rep->GetEntryIndex(column_family), &rep->write_batch,
                              rep->GetComparator(column_family), false);
}

void WriteBatchWithIndex::NewIterator(ColumnFamilyHandle* column_family,
                                      WBWIIterator::IteratorStorage& storage,
                                      bool ephemeral) {
  static_assert(sizeof(WBWIIteratorImpl) <= sizeof(storage.buffer),
               "Need larger buffer for WBWIIteratorImpl");
                
  storage.iter = new (storage.buffer)
      WBWIIteratorImpl(GetColumnFamilyID(column_family),
                       rep->GetEntryIndex(column_family), &rep->write_batch,
                       rep->GetComparator(column_family), ephemeral);
}

Iterator* WriteBatchWithIndex::NewIteratorWithBase(
    ColumnFamilyHandle* column_family, Iterator* base_iterator,
    const ReadOptions* read_options) {
  auto wbwiii =
      new WBWIIteratorImpl(GetColumnFamilyID(column_family),
                           rep->GetEntryIndex(column_family),
                           &rep->write_batch, rep->GetComparator(column_family), false);
  return new BaseDeltaIterator(column_family, base_iterator, wbwiii,
                               GetColumnFamilyUserComparator(column_family),
                               read_options);
}

Iterator* WriteBatchWithIndex::NewIteratorWithBase(Iterator* base_iterator) {
  // default column family's comparator
  auto wbwiii = new WBWIIteratorImpl(0, rep->GetEntryIndex(nullptr), &rep->write_batch,
                                     rep->GetComparator(nullptr), false);
  return new BaseDeltaIterator(nullptr, base_iterator, wbwiii,
                               rep->default_comparator);
}

Status WriteBatchWithIndex::Put(ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Put(column_family, key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, kPutRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Put(const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Put(key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(kPutRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Put(ColumnFamilyHandle* column_family,
                                const Slice& /*key*/, const Slice& /*ts*/,
                                const Slice& /*value*/) {
  if (!column_family) {
    return Status::InvalidArgument("column family handle cannot be nullptr");
  }
  // TODO: support WBWI::Put() with timestamp.
  return Status::NotSupported();
}

Status WriteBatchWithIndex::Delete(ColumnFamilyHandle* column_family,
                                   const Slice& key) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Delete(column_family, key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, kDeleteRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Delete(const Slice& key) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Delete(key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(kDeleteRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Delete(ColumnFamilyHandle* column_family,
                                   const Slice& /*key*/, const Slice& /*ts*/) {
  if (!column_family) {
    return Status::InvalidArgument("column family handle cannot be nullptr");
  }
  // TODO: support WBWI::Delete() with timestamp.
  return Status::NotSupported();
}

Status WriteBatchWithIndex::SingleDelete(ColumnFamilyHandle* column_family,
                                         const Slice& key) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.SingleDelete(column_family, key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, kSingleDeleteRecord);
  }
  return s;
}

Status WriteBatchWithIndex::SingleDelete(const Slice& key) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.SingleDelete(key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(kSingleDeleteRecord);
  }
  return s;
}

Status WriteBatchWithIndex::SingleDelete(ColumnFamilyHandle* column_family,
                                         const Slice& /*key*/,
                                         const Slice& /*ts*/) {
  if (!column_family) {
    return Status::InvalidArgument("column family handle cannot be nullptr");
  }
  // TODO: support WBWI::SingleDelete() with timestamp.
  return Status::NotSupported();
}

Status WriteBatchWithIndex::Merge(ColumnFamilyHandle* column_family,
                                  const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Merge(column_family, key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, kMergeRecord);
  }
  return s;
}

Status WriteBatchWithIndex::Merge(const Slice& key, const Slice& value) {
  rep->SetLastEntryOffset();
  auto s = rep->write_batch.Merge(key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(kMergeRecord);
  }
  return s;
}

Status WriteBatchWithIndex::PutLogData(const Slice& blob) {
  return rep->write_batch.PutLogData(blob);
}

void WriteBatchWithIndex::Clear() { rep->Clear(); }

Status WriteBatchWithIndex::GetFromBatch(ColumnFamilyHandle* column_family,
                                         const DBOptions& options,
                                         const Slice& key, std::string* value) {
  Status s;
  WriteBatchWithIndexInternal wbwii(&options, column_family);
  auto result = wbwii.GetFromBatch(this, key, value, &s);

  switch (result) {
    case WBWIIteratorImpl::kFound:
    case WBWIIteratorImpl::kError:
      // use returned status
      break;
    case WBWIIteratorImpl::kDeleted:
    case WBWIIteratorImpl::kNotFound:
      s = Status::NotFound();
      break;
    case WBWIIteratorImpl::kMergeInProgress:
      s = Status::MergeInProgress();
      break;
    default:
      assert(false);
  }

  return s;
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              const Slice& key,
                                              std::string* value) {
  assert(value != nullptr);
  PinnableSlice pinnable_val(value);
  assert(!pinnable_val.IsPinned());
  auto s = GetFromBatchAndDB(db, read_options, db->DefaultColumnFamily(), key,
                             &pinnable_val);
  if (s.ok() && pinnable_val.IsPinned()) {
    value->assign(pinnable_val.data(), pinnable_val.size());
  }  // else value is already assigned
  return s;
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              const Slice& key,
                                              PinnableSlice* pinnable_val) {
  return GetFromBatchAndDB(db, read_options, db->DefaultColumnFamily(), key,
                           pinnable_val);
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key,
                                              std::string* value) {
  assert(value != nullptr);
  PinnableSlice pinnable_val(value);
  assert(!pinnable_val.IsPinned());
  auto s =
      GetFromBatchAndDB(db, read_options, column_family, key, &pinnable_val);
  if (s.ok() && pinnable_val.IsPinned()) {
    value->assign(pinnable_val.data(), pinnable_val.size());
  }  // else value is already assigned
  return s;
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key,
                                              PinnableSlice* pinnable_val) {
  return GetFromBatchAndDB(db, read_options, column_family, key, pinnable_val,
                           nullptr);
}

Status WriteBatchWithIndex::GetFromBatchAndDB(
    DB* db, const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const Slice& key, PinnableSlice* pinnable_val, ReadCallback* callback) {
  const Comparator* const ucmp = rep->GetComparator(column_family);
  size_t ts_sz = ucmp ? ucmp->timestamp_size() : 0;
  if (ts_sz > 0 && !read_options.timestamp) {
    return Status::InvalidArgument("Must specify timestamp");
  }

  Status s;
  WriteBatchWithIndexInternal wbwii(db, column_family);

  // Since the lifetime of the WriteBatch is the same as that of the transaction
  // we cannot pin it as otherwise the returned value will not be available
  // after the transaction finishes.
  std::string& batch_value = *pinnable_val->GetSelf();
  auto result = wbwii.GetFromBatch(this, key, &batch_value, &s);

  if (result == WBWIIteratorImpl::kFound) {
    pinnable_val->PinSelf();
    return s;
  } else if (!s.ok() || result == WBWIIteratorImpl::kError) {
    return s;
  } else if (result == WBWIIteratorImpl::kDeleted) {
    return Status::NotFound();
  }
  assert(result == WBWIIteratorImpl::kMergeInProgress ||
         result == WBWIIteratorImpl::kNotFound);

  // Did not find key in batch OR could not resolve Merges.  Try DB.
  if (!callback) {
    s = db->Get(read_options, column_family, key, pinnable_val);
  } else {
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = column_family;
    get_impl_options.value = pinnable_val;
    get_impl_options.callback = callback;
    s = static_cast_with_check<DBImpl>(db->GetRootDB())
            ->GetImpl(read_options, key, get_impl_options);
  }

  if (s.ok() || s.IsNotFound()) {  // DB Get Succeeded
    if (result == WBWIIteratorImpl::kMergeInProgress) {
      // Merge result from DB with merges in Batch
      std::string merge_result;
      if (s.ok()) {
        s = wbwii.MergeKey(key, pinnable_val, &merge_result);
      } else {  // Key not present in db (s.IsNotFound())
        s = wbwii.MergeKey(key, nullptr, &merge_result);
      }
      if (s.ok()) {
        pinnable_val->Reset();
        *pinnable_val->GetSelf() = std::move(merge_result);
        pinnable_val->PinSelf();
      }
    }
  }

  return s;
}

void WriteBatchWithIndex::MultiGetFromBatchAndDB(
    DB* db, const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const size_t num_keys, const Slice* keys, PinnableSlice* values,
    Status* statuses, bool sorted_input) {
  MultiGetFromBatchAndDB(db, read_options, column_family, num_keys, keys,
                         values, statuses, sorted_input, nullptr);
}

void WriteBatchWithIndex::MultiGetFromBatchAndDB(
    DB* db, const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const size_t num_keys, const Slice* keys, PinnableSlice* values,
    Status* statuses, bool sorted_input, ReadCallback* callback) {
  const Comparator* const ucmp = rep->GetComparator(column_family);
  size_t ts_sz = ucmp ? ucmp->timestamp_size() : 0;
  if (ts_sz > 0 && !read_options.timestamp) {
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = Status::InvalidArgument("Must specify timestamp");
    }
    return;
  }

  WriteBatchWithIndexInternal wbwii(db, column_family);

  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  // To hold merges from the write batch
  autovector<std::pair<WBWIIteratorImpl::Result, MergeContext>,
             MultiGetContext::MAX_BATCH_SIZE>
      merges;
  // Since the lifetime of the WriteBatch is the same as that of the transaction
  // we cannot pin it as otherwise the returned value will not be available
  // after the transaction finishes.
  for (size_t i = 0; i < num_keys; ++i) {
    MergeContext merge_context;
    std::string batch_value;
    Status* s = &statuses[i];
    PinnableSlice* pinnable_val = &values[i];
    pinnable_val->Reset();
    auto result =
        wbwii.GetFromBatch(this, keys[i], &merge_context, &batch_value, s);

    if (result == WBWIIteratorImpl::kFound) {
      *pinnable_val->GetSelf() = std::move(batch_value);
      pinnable_val->PinSelf();
      continue;
    }
    if (result == WBWIIteratorImpl::kDeleted) {
      *s = Status::NotFound();
      continue;
    }
    if (result == WBWIIteratorImpl::kError) {
      continue;
    }
    assert(result == WBWIIteratorImpl::kMergeInProgress ||
           result == WBWIIteratorImpl::kNotFound);
    key_context.emplace_back(column_family, keys[i], &values[i],
                             /*timestamp*/ nullptr, &statuses[i]);
    merges.emplace_back(result, std::move(merge_context));
  }

  for (KeyContext& key : key_context) {
    sorted_keys.emplace_back(&key);
  }

  // Did not find key in batch OR could not resolve Merges.  Try DB.
  bool same_cf = true;
  static_cast_with_check<DBImpl>(db->GetRootDB())
      ->PrepareMultiGetKeys(num_keys, sorted_input, same_cf, &sorted_keys);
  static_cast_with_check<DBImpl>(db->GetRootDB())
      ->MultiGetWithCallback(read_options, column_family, callback,
                             &sorted_keys);

  for (auto iter = key_context.begin(); iter != key_context.end(); ++iter) {
    KeyContext& key = *iter;
    if (key.s->ok() || key.s->IsNotFound()) {  // DB Get Succeeded
      size_t index = iter - key_context.begin();
      std::pair<WBWIIteratorImpl::Result, MergeContext>& merge_result =
          merges[index];
      if (merge_result.first == WBWIIteratorImpl::kMergeInProgress) {
        std::string merged_value;
        // Merge result from DB with merges in Batch
        if (key.s->ok()) {
          *key.s = wbwii.MergeKey(*key.key, iter->value, merge_result.second,
                                  &merged_value);
        } else {  // Key not present in db (s.IsNotFound())
          *key.s = wbwii.MergeKey(*key.key, nullptr, merge_result.second,
                                  &merged_value);
        }
        if (key.s->ok()) {
          key.value->Reset();
          *key.value->GetSelf() = std::move(merged_value);
          key.value->PinSelf();
        }
      }
    }
  }
}

void WriteBatchWithIndex::SetSavePoint() { rep->write_batch.SetSavePoint(); }

Status WriteBatchWithIndex::RollbackToSavePoint() {
  Status s = rep->write_batch.RollbackToSavePoint();

  if (s.ok()) {
    rep->sub_batch_cnt = 1;
    rep->last_sub_batch_offset = 0;
    s = rep->ReBuildIndex();
  }

  return s;
}

Status WriteBatchWithIndex::PopSavePoint() {
  return rep->write_batch.PopSavePoint();
}

void WriteBatchWithIndex::SetMaxBytes(size_t max_bytes) {
  rep->write_batch.SetMaxBytes(max_bytes);
}

size_t WriteBatchWithIndex::GetDataSize() const {
  return rep->write_batch.GetDataSize();
}

const Comparator* WriteBatchWithIndexInternal::GetUserComparator(
    const WriteBatchWithIndex& wbwi, uint32_t cf_id) {
  
  return wbwi.rep->GetComparatorByCfId(cf_id);
}



const std::string kSkipListWriteBatchEntryFactoryName = "skiplist";

static std::unordered_map<std::string, const WriteBatchEntryIndexFactory*>
    write_batch_entry_index_factory_info = {
        {kSkipListWriteBatchEntryFactoryName,
         skip_list_WriteBatchEntryIndexFactory()}
    };


struct WriteBatchEntryComparator {
  int operator()(WriteBatchIndexEntry* l, WriteBatchIndexEntry* r) const {
    if(l->column_family > r->column_family) {
      return 1;
    } else if(l->column_family < r->column_family) {
      return -1;
    }

    // Deal with special case of seeking to the beginning of a column family
    if (l->is_min_in_cf()) {
      return -1;
    } else if (r->is_min_in_cf()) {
      return 1;
    }
      
    int cmp = c->CompareWithoutTimestamp(extractor(l), false, extractor(r), false);

    // To support merge, there may be many entries of the same key even if overwrite_key is true
    // comp offset is necessary, overwrite_key makes no sense.
    if (cmp != 0) {
      return cmp;
    } else if (l->offset > r->offset) {
      return 1;
    } else if (l->offset < r->offset) {
      return -1;
    }
    return 0;
  }
  WriteBatchKeyExtractor extractor;
  const Comparator* c;
};

class SkipListIndexContext : public WriteBatchEntryIndexContext {
 public:
  virtual void ImportMap(const map_type& _index_map) {
    index_map = _index_map;
  }

  virtual void ExportMap(map_type* _index_map) {
    *_index_map = index_map;
  }

  virtual void FreeMapContent() {
    typedef SkipList<WriteBatchIndexEntry*, const WriteBatchEntryComparator&> Index;
    for(auto& pairs: index_map)
      ((Index*)pairs.second)->~Index();
  }

  uint8_t* GetLastSkipListAddress(WriteBatchEntryIndex* entry_addr) {
    auto iter = index_map.find(entry_addr);
    assert(iter != index_map.end());
    return iter->second;
  }

  void RegistSkipListAddress(WriteBatchEntryIndex* entry_addr, uint8_t* skip_list_addr) {
    index_map[entry_addr] = skip_list_addr;
  }

};

template<bool OverwriteKey>
class WriteBatchEntrySkipListIndex : public WriteBatchEntryIndex {
 protected:
  typedef WriteBatchEntryComparator EntryComparator;
  typedef SkipList<WriteBatchIndexEntry*, const EntryComparator&> Index;
  SkipListIndexContext* ctx_;
  EntryComparator comparator_;
  Index* index_;

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
  WriteBatchEntrySkipListIndex(WriteBatchEntryIndexContext *ctx, WriteBatchKeyExtractor e,
                               const Comparator* c, Arena* a,
                               WriteBatchEntryIndex* last_address)
      : ctx_(static_cast<SkipListIndexContext*>(ctx)),
        comparator_({e, c}) {
          if(!last_address) {
            index_ = new Index(comparator_, a);
          } else {
            index_ = new (ctx_->GetLastSkipListAddress(last_address)) Index(comparator_, a);
          }
      }

  ~WriteBatchEntrySkipListIndex() {
    ctx_->RegistSkipListAddress(this, (uint8_t*)index_);
  }

  virtual Iterator* NewIterator() override {
    return new SkipListIterator(index_);
  }
  virtual void NewIterator(IteratorStorage& storage, bool ephemeral) override {
    static_assert(sizeof(SkipListIterator) <= sizeof storage.buffer,
                  "Need larger buffer for SkipListIterator");
    storage.iter = new (storage.buffer) SkipListIterator(index_);
  }
  virtual bool Upsert(WriteBatchIndexEntry* key , bool isMergeRecord) override {
    if constexpr (OverwriteKey) {
      Slice search_key = comparator_.extractor(key);
      typename Index::Iterator iter(index_);
      iter.Seek(key);
      if(!iter.Valid()) {
        iter.SeekToLast();
      } else {
        iter.Prev();
      }

      if (iter.Valid() &&
          comparator_.c->CompareWithoutTimestamp(search_key, false,
                                                comparator_.extractor(iter.key()), false) == 0) {  
        // found , replace
        if(isMergeRecord) {
          auto last_same_key_offset = iter.key()->offset;
          index_->Insert(key);
           // set key->offset to update last_sub_batch_offset, and then reset to last_entry_offset
          key->offset = last_same_key_offset;
        }
        else {
          std::swap(iter.key()->offset, key->offset);
        }
        return false;
      }
    }
    index_->Insert(key);
    return true;
  }
};

const WriteBatchEntryIndexFactory* skip_list_WriteBatchEntryIndexFactory()
{
  class SkipListIndexFactory : public WriteBatchEntryIndexFactory {
   public:
    WriteBatchEntryIndexContext* NewContext(Arena *a) const {
      return new (a->AllocateAligned(sizeof(SkipListIndexContext))) SkipListIndexContext();
    }
    WriteBatchEntryIndex* New(WriteBatchEntryIndexContext* ctx,
                              WriteBatchKeyExtractor e,
                              const Comparator* c, Arena* a,
                              bool overwrite_key,
                              WriteBatchEntryIndex* last_address = nullptr) const override {
      if (overwrite_key) {
        typedef WriteBatchEntrySkipListIndex<true> index_t;
        return new (a->AllocateAligned(sizeof(index_t))) index_t(ctx, e, c, a, last_address);
      } else {
        typedef WriteBatchEntrySkipListIndex<false> index_t;
        return new (a->AllocateAligned(sizeof(index_t))) index_t(ctx, e, c, a, last_address);
      }
    }
    virtual const char* Name() const override {
      return "SkipListIndexFactory";
    }
  };

  static SkipListIndexFactory factory;
  return &factory;
};

void RegistWriteBatchEntryIndexFactory(const char* name,
                                       const WriteBatchEntryIndexFactory* factory) {
  write_batch_entry_index_factory_info.emplace(name, factory);
}

const WriteBatchEntryIndexFactory* GetWriteBatchEntryIndexFactory(const char* name) {
  auto find = write_batch_entry_index_factory_info.find(name);
  if (find == write_batch_entry_index_factory_info.end()) {
    return nullptr;
  }
  return find->second;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
