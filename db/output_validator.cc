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

void OutputValidator::Init() {
  full_check_ = g_full_check;
  if (full_check_) {
    std::destroy_at(&kv_vec_);
    new(&kv_vec_)decltype(kv_vec_)(terark::valvec_reserve(), 128<<10, 32<<20);
  }
  if (icmp_.IsForwardBytewise())
    m_add = &OutputValidator::Add_tpl<BytewiseCompareInternalKey>;
  else if (icmp_.IsReverseBytewise())
    m_add = &OutputValidator::Add_tpl<RevBytewiseCompareInternalKey>;
  else
    m_add = &OutputValidator::Add_tpl<FallbackVirtCmp>;
}

template<class Cmp>
Status OutputValidator::Add_tpl(const Slice key, const Slice value) {
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
    if (!prev_key_.empty() && Cmp{&icmp_}(key, SliceOf(prev_key_))) {
      return Status::Corruption("Compaction sees out-of-order keys.");
    }
   #if 0
    prev_key_.assign(key.data(), key.size());
   #else
    // faster
    prev_key_.resize_no_init(key.size());
    memcpy(prev_key_.data(), key.data(), key.size());
   #endif
  }
  if (full_check_) {
    kv_vec_.push_back(key);
    kv_vec_.push_back(value);
  }
  return Status::OK();
}

static inline Slice SliceOf(terark::fstring s) { return {s.p, s.size()}; }
bool OutputValidator::CompareValidator(const OutputValidator& other) {
  if (full_check_) {
    long long file_number = m_file_number ? m_file_number : other.m_file_number;
    ROCKSDB_VERIFY_EQ(kv_vec_.size(), other.kv_vec_.size());
    for (size_t i = 0, n = kv_vec_.size() / 2; i < n; i++) {
      #define hex(deref, field) ParsedInternalKey(SliceOf(deref kv_vec_[field])).DebugString(true, true).c_str()
      size_t key = 2*i + 0, val = 2*i + 1;
      ROCKSDB_VERIFY_F(kv_vec_[key] == other.kv_vec_[key], "%06lld.sst[%zd]: %s %s", file_number, i, hex(,key), hex(other.,key));
      ROCKSDB_VERIFY_F(kv_vec_[val] == other.kv_vec_[val], "%06lld.sst[%zd]: %s %s", file_number, i, hex(,val), hex(other.,val));
    }
    ROCKSDB_VERIFY_EQ(GetHash(), other.GetHash());
  }
  return GetHash() == other.GetHash();
}


}  // namespace ROCKSDB_NAMESPACE
