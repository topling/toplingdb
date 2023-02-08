//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/output_validator.h"

#include "test_util/sync_point.h"
#include "util/hash.h"
#include <terark/fstring.hpp>

namespace ROCKSDB_NAMESPACE {

static bool g_full_check = terark::getEnvBool("OutputValidator_full_check");

Status OutputValidator::Add(const Slice& key, const Slice& value) {
  if (enable_hash_) {
    // Generate a rolling 64-bit hash of the key and values
    paranoid_hash_ = NPHash64(key.data(), key.size(), paranoid_hash_);
    paranoid_hash_ = NPHash64(value.data(), value.size(), paranoid_hash_);
  }
  if (enable_order_check_) {
    TEST_SYNC_POINT_CALLBACK("OutputValidator::Add:order_check",
                             /*arg=*/nullptr);
    if (key.size() < kNumInternalBytes) {
      return Status::Corruption(
          "Compaction tries to write a key without internal bytes.");
    }
    // prev_key_ starts with empty.
    if (!prev_key_.empty() && icmp_.Compare(key, prev_key_) < 0) {
      return Status::Corruption("Compaction sees out-of-order keys.");
    }
    prev_key_.assign(key.data(), key.size());
  }
  if (g_full_check) {
    kv_vec_.emplace_back(key.ToString(), value.ToString());
  }
  return Status::OK();
}

bool OutputValidator::CompareValidator(const OutputValidator& other) {
  if (g_full_check) {
    long long file_number = m_file_number ? m_file_number : other.m_file_number;
    ROCKSDB_VERIFY_EQ(kv_vec_.size(), other.kv_vec_.size());
    for (size_t i = 0, n = kv_vec_.size(); i < n; i++) {
      #define hex(deref, field) ParsedInternalKey(deref kv_vec_[i].field).DebugString(true, true).c_str()
      ROCKSDB_VERIFY_F(kv_vec_[i].first  == other.kv_vec_[i].first , "%06lld.sst[%zd]: %s %s", file_number, i, hex(,first ), hex(other., first ));
      ROCKSDB_VERIFY_F(kv_vec_[i].second == other.kv_vec_[i].second, "%06lld.sst[%zd]: %s %s", file_number, i, hex(,second), hex(other., second));
    }
    ROCKSDB_VERIFY_EQ(GetHash(), other.GetHash());
  }
  return GetHash() == other.GetHash();
}


}  // namespace ROCKSDB_NAMESPACE
