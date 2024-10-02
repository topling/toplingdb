//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/output_validator.h"

#include "test_util/sync_point.h"
#include "util/hash.h"
#include <terark/fstring.hpp>
#include <terark/io/var_int.hpp>

namespace ROCKSDB_NAMESPACE {

using terark::fstring;
static bool g_full_check = terark::getEnvBool("OutputValidator_full_check");

void OutputValidator::Init() {
  full_check_ = g_full_check;
  if (full_check_) {
    kv_vec_.reserve(32 << 20); // 32M
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
    auto WriteSlice = [this](Slice s) {
      unsigned char buf[16];
      size_t len = terark::save_var_uint64(buf, s.size_) - buf;
      kv_vec_.append(buf, len);
      kv_vec_.append(s.data_, s.size_);
    };
    WriteSlice(key);
    WriteSlice(value);
  }
  num_kv_++;
  return Status::OK();
}

static Slice ReadSlice(const unsigned char** ptr) {
  size_t len = (size_t)terark::load_var_uint64(*ptr, ptr);
  auto data = (const char*)(*ptr);
  *ptr += len;
  return Slice(data, len);
}

bool OutputValidator::CompareValidator(const OutputValidator& other) {
  if (full_check_) {
    long long file_number = m_file_number ? m_file_number : other.m_file_number;
    if (kv_vec_.size() != other.kv_vec_.size()) {
      fprintf(stderr,
        "FATAL: OutputValidator::CompareValidator: kv_vec_.size: %zd != %zd\n",
        kv_vec_.size(), other.kv_vec_.size());
    }
    ROCKSDB_VERIFY_EQ(num_kv_, other.num_kv_);
    const unsigned char* x_reader = kv_vec_.begin();
    const unsigned char* y_reader = other.kv_vec_.begin();
    for (size_t i = 0, n = num_kv_; i < n; i++) {
      Slice kx = ReadSlice(&x_reader);
      Slice vx = ReadSlice(&x_reader);
      Slice ky = ReadSlice(&y_reader);
      Slice vy = ReadSlice(&y_reader);
      #define HexKey(key) ParsedInternalKey(key).DebugString(true, true).c_str()
      ROCKSDB_VERIFY_F(kx == ky, "%06lld.sst[%zd]: %zd(%s) %zd(%s)", file_number, i, kx.size_, HexKey(kx), ky.size_, HexKey(ky));
      ROCKSDB_VERIFY_F(vx == vy, "%06lld.sst[%zd]: %zd(%s) %zd(%s)", file_number, i, vx.size_, vx.hex().c_str(), vy.size_, vy.hex().c_str());
    }
    ROCKSDB_VERIFY_EQ(x_reader, kv_vec_.end());
    ROCKSDB_VERIFY_EQ(y_reader, other.kv_vec_.end());
    ROCKSDB_VERIFY_EQ(GetHash(), other.GetHash());
  }
  return GetHash() == other.GetHash();
}


}  // namespace ROCKSDB_NAMESPACE
