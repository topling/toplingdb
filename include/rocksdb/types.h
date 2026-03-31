// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>

#include <memory>
#include <unordered_map>

#include "rocksdb/slice.h"
#include "enum_reflection.h"
#include <terark/hash_strmap.hpp>

namespace ROCKSDB_NAMESPACE {

// Define all public custom types here.

using ColumnFamilyId = uint32_t;

// Represents a sequence number in a WAL file.
using SequenceNumber = uint64_t;

struct TableProperties;
using TablePropertiesCollection =
    terark::hash_strmap<std::shared_ptr<const TableProperties>>;

const SequenceNumber kMinUnCommittedSeq = 1;  // 0 is always committed

ROCKSDB_ENUM_CLASS(TableFileCreationReason, unsigned char,
  kFlush,
  kCompaction,
  kRecovery,
  kMisc
);

enum class BlobFileCreationReason : unsigned char {
  kFlush,
  kCompaction,
  kRecovery,
};

// The types of files RocksDB uses in a DB directory. (Available for
// advanced options.)
enum FileType : unsigned char {
  kWalFile,
  kDBLockFile,
  kTableFile,
  kDescriptorFile,
  kCurrentFile,
  kTempFile,
  kInfoLogFile,  // Either the current one, or an old one
  kMetaDatabase,
  kIdentityFile,
  kOptionsFile,
  kBlobFile
};

// User-oriented representation of internal key types.
// Ordering of this enum entries should not change.
enum EntryType : unsigned char {
  kEntryPut,
  kEntryDelete,
  kEntrySingleDelete,
  kEntryMerge,
  kEntryRangeDeletion,
  kEntryBlobIndex,
  kEntryDeleteWithTimestamp,
  kEntryWideColumnEntity,
  kEntryOther,
};

enum class WriteStallCause : unsigned char {
  // Beginning of CF-scope write stall causes
  //
  // Always keep `kMemtableLimit` as the first stat in this section
  kMemtableLimit,
  kL0FileCountLimit,
  kPendingCompactionBytes,
  kCFScopeWriteStallCauseEnumMax,
  // End of CF-scope write stall causes

  // Beginning of DB-scope write stall causes
  //
  // Always keep `kWriteBufferManagerLimit` as the first stat in this section
  kWriteBufferManagerLimit,
  kDBScopeWriteStallCauseEnumMax,
  // End of DB-scope write stall causes

  // Always add new WriteStallCause before `kNone`
  kNone,
};

ROCKSDB_ENUM_CLASS(WriteStallCondition, unsigned char,
  kDelayed,
  kStopped,
  // Always add new WriteStallCondition before `kNormal`
  kNormal
);

}  // namespace ROCKSDB_NAMESPACE
