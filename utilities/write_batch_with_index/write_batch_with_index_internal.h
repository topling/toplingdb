// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#ifndef ROCKSDB_LITE

#include <limits>
#include <string>
#include <vector>

#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace ROCKSDB_NAMESPACE {

class MergeContext;
struct Options;

// Key used by skip list, as the binary searchable index of WriteBatchWithIndex.
struct WriteBatchIndexEntry {
  WriteBatchIndexEntry(size_t o, uint32_t c, size_t ko, size_t ksz)
      : offset(o),
        column_family(c),
        key_offset(ko),
        key_size(ksz),
        search_key(nullptr) {}
  // Create a dummy entry as the search key. This index entry won't be backed
  // by an entry from the write batch, but a pointer to the search key. Or a
  // special flag of offset can indicate we are seek to first.
  // @_search_key: the search key
  // @_column_family: column family
  // @is_forward_direction: true for Seek(). False for SeekForPrev()
  // @is_seek_to_first: true if we seek to the beginning of the column family
  //                    _search_key should be null in this case.
  WriteBatchIndexEntry(const Slice* _search_key, uint32_t _column_family,
                       bool is_forward_direction, bool is_seek_to_first)
      // For SeekForPrev(), we need to make the dummy entry larger than any
      // entry who has the same search key. Otherwise, we'll miss those entries.
      : offset(is_forward_direction ? 0 : port::kMaxSizet),
        column_family(_column_family),
        key_offset(0),
        key_size(is_seek_to_first ? kFlagMinInCf : 0),
        search_key(_search_key) {
    assert(_search_key != nullptr || is_seek_to_first);
  }

  // If this flag appears in the key_size, it indicates a
  // key that is smaller than any other entry for the same column family.
  static const size_t kFlagMinInCf = port::kMaxSizet;

  bool is_min_in_cf() const {
    assert(key_size != kFlagMinInCf ||
           (key_offset == 0 && search_key == nullptr));
    return key_size == kFlagMinInCf;
  }

  // offset of an entry in write batch's string buffer. If this is a dummy
  // lookup key, in which case search_key != nullptr, offset is set to either
  // 0 or max, only for comparison purpose. Because when entries have the same
  // key, the entry with larger offset is larger, offset = 0 will make a seek
  // key small or equal than all the entries with the seek key, so that Seek()
  // will find all the entries of the same key. Similarly, offset = MAX will
  // make the entry just larger than all entries with the search key so
  // SeekForPrev() will see all the keys with the same key.
  size_t offset;
  uint32_t column_family;  // c1olumn family of the entry.
  size_t key_offset;       // offset of the key in write batch's string buffer.
  size_t key_size;         // size of the key. kFlagMinInCf indicates
                           // that this is a dummy look up entry for
                           // SeekToFirst() to the beginning of the column
                           // family. We use the flag here to save a boolean
                           // in the struct.

  const Slice* search_key;  // if not null, instead of reading keys from
                            // write batch, use it to compare. This is used
                            // for lookup key.
};

class ReadableWriteBatch : public WriteBatch {
 public:
  explicit ReadableWriteBatch(size_t reserved_bytes = 0, size_t max_bytes = 0)
      : WriteBatch(reserved_bytes, max_bytes) {}
  // Retrieve some information from a write entry in the write batch, given
  // the start offset of the write entry.
  Status GetEntryFromDataOffset(size_t data_offset, WriteType* type, Slice* Key,
                                Slice* value, Slice* blob, Slice* xid) const;
};

class WriteBatchWithIndexInternal {
 public:
  enum Result { kFound, kDeleted, kNotFound, kMergeInProgress, kError };

  // If batch contains a value for key, store it in *value and return kFound.
  // If batch contains a deletion for key, return Deleted.
  // If batch contains Merge operations as the most recent entry for a key,
  //   and the merge process does not stop (not reaching a value or delete),
  //   prepend the current merge operands to *operands,
  //   and return kMergeInProgress
  // If batch does not contain this key, return kNotFound
  // Else, return kError on error with error Status stored in *s.
  static WriteBatchWithIndexInternal::Result GetFromBatch(
      const ImmutableDBOptions& ioptions, WriteBatchWithIndex* batch,
      ColumnFamilyHandle* column_family, const Slice& key,
      MergeContext* merge_context, const Comparator* cmp,
      std::string* value, bool overwrite_key, Status* s);
};

class WriteBatchKeyExtractor {
 public:
  WriteBatchKeyExtractor(const ReadableWriteBatch* write_batch)
      : write_batch_(write_batch) {
  }

  Slice operator()(const WriteBatchIndexEntry* entry) const;

 private:
  const ReadableWriteBatch* write_batch_;
};

class WriteBatchEntryIndex {
 public:
  virtual ~WriteBatchEntryIndex() {}

  class Iterator {
   public:
    virtual ~Iterator() {}
    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(WriteBatchIndexEntry* target) = 0;
    virtual void SeekForPrev(WriteBatchIndexEntry* target) = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    virtual WriteBatchIndexEntry* key() const = 0;
  };
  typedef WBIteratorStorage<Iterator, 24> IteratorStorage;

  virtual Iterator* NewIterator() = 0;
  // sizeof(iterator) size must less or equal than 24
  // INCLUDE virtual table pointer
  virtual void NewIterator(IteratorStorage& storage, bool ephemeral) = 0;
  // return true if insert success
  // assign key->offset to exists entry's offset otherwise
  virtual bool Upsert(WriteBatchIndexEntry* key) = 0;
};

class WriteBatchEntryIndexContext {
 public:
  virtual ~WriteBatchEntryIndexContext(){};
};

class WriteBatchEntryIndexFactory {
 public:
  // object MUST allocated from arena, allow return nullptr
  // context will not delete, only call destruct func
  virtual WriteBatchEntryIndexContext* NewContext(Arena* a) const {
    return nullptr;
  }
  // object MUST allocated from arena
  // index will not delete, only call destruct func
  virtual WriteBatchEntryIndex* New(WriteBatchEntryIndexContext* ctx,
                                    WriteBatchKeyExtractor e,
                                    const Comparator* c, Arena* a,
                                    bool overwrite_key) const = 0;
  virtual ~WriteBatchEntryIndexFactory() {}
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
