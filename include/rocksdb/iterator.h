// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.

#pragma once

#include <string>

#include "rocksdb/cleanable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class Iterator : public Cleanable {
 public:
  Iterator() {}
  // No copying allowed
  Iterator(const Iterator&) = delete;
  void operator=(const Iterator&) = delete;

  virtual ~Iterator() {}

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  // Always returns false if !status().ok().
  virtual bool Valid() const = 0;

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() = 0;

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() = 0;

  // Position at the first key in the source that at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  // All Seek*() methods clear any error status() that the iterator had prior to
  // the call; after the seek, status() indicates only the error (if any) that
  // happened during the seek, not any past errors.
  // Target does not contain timestamp.
  virtual void Seek(const Slice& target) = 0;

  // Position at the last key in the source that at or before target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or before target.
  // Target does not contain timestamp.
  virtual void SeekForPrev(const Slice& target) = 0;

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() = 0;

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of the
  // iterator (i.e. the next SeekToFirst/SeekToLast/Seek/SeekForPrev/Next/Prev
  // operation).
  // REQUIRES: Valid()
  virtual Slice key() const = 0;

  // Return the value for the current entry.  If the entry is a plain key-value,
  // return the value as-is; if it is a wide-column entity, return the value of
  // the default anonymous column (see kDefaultWideColumnName) if any, or an
  // empty value otherwise.  The underlying storage for the returned slice is
  // valid only until the next modification of the iterator (i.e. the next
  // SeekToFirst/SeekToLast/Seek/SeekForPrev/Next/Prev operation).
  // REQUIRES: Valid()
  virtual Slice value() const = 0;

  // Return the wide columns for the current entry.  If the entry is a
  // wide-column entity, return it as-is; if it is a plain key-value, return it
  // as an entity with a single anonymous column (see kDefaultWideColumnName)
  // which contains the value.  The underlying storage for the returned
  // structure is valid only until the next modification of the iterator (i.e.
  // the next SeekToFirst/SeekToLast/Seek/SeekForPrev/Next/Prev operation).
  // REQUIRES: Valid()
  virtual const WideColumns& columns() const {
    assert(false);
    return kNoWideColumns;
  }

  // If an error has occurred, return it.  Else return an ok status.
  // If non-blocking IO is requested and this operation cannot be
  // satisfied without doing some IO, then this returns Status::Incomplete().
  virtual Status status() const = 0;

  // If supported, renew the iterator to represent the latest state. The
  // iterator will be invalidated after the call. Not supported if
  // ReadOptions.snapshot is given when creating the iterator.
  virtual Status Refresh() {
    return Refresh(nullptr, false);
  }

  virtual Status Refresh(const class Snapshot*, bool/*keep_iter_pos*/=false) {
    return Status::NotSupported("Refresh() is not supported");
  }

  Status RefreshKeepSnapshot(bool keep_iter_pos = true);

  // Property "rocksdb.iterator.is-key-pinned":
  //   If returning "1", this means that the Slice returned by key() is valid
  //   as long as the iterator is not deleted.
  //   It is guaranteed to always return "1" if
  //      - Iterator created with ReadOptions::pin_data = true
  //      - DB tables were created with
  //        BlockBasedTableOptions::use_delta_encoding = false.
  // Property "rocksdb.iterator.super-version-number":
  //   LSM version used by the iterator. The same format as DB Property
  //   kCurrentSuperVersionNumber. See its comment for more information.
  // Property "rocksdb.iterator.internal-key":
  //   Get the user-key portion of the internal key at which the iteration
  //   stopped.
  // Property "rocksdb.iterator.write-time":
  //   DO NOT USE, UNDER CONSTRUCTION
  //   Get the unix time of the best estimate of the write time of the entry.
  //   Returned as 64-bit raw value (8 bytes). It can be converted to uint64_t
  //   with util method `DecodeU64Ts`. The accuracy of the write time depends on
  //   settings like preserve_internal_time_seconds. If this feature is
  //   disabled, this property will always be empty. The actual write time of
  //   the entry should be the same or newer than the returned write time. So
  //   this property can be interpreted as the possible oldest write time for
  //   the entry.
  virtual Status GetProperty(std::string prop_name, std::string* prop);

  virtual Slice timestamp() const {
    assert(false);
    return Slice();
  }

  virtual bool PrepareValue() { return true; }
};

// Return an empty iterator (yields nothing).
extern Iterator* NewEmptyIterator();

// Return an empty iterator with the specified status.
extern Iterator* NewErrorIterator(const Status& status);

}  // namespace ROCKSDB_NAMESPACE
