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

#if defined(_MSC_VER) /* Visual Studio */
#define FORCE_INLINE __forceinline
#define __attribute_noinline__
#define __builtin_prefetch(ptr) _mm_prefetch((const char*)(ptr), _MM_HINT_T0)
#elif defined(__GNUC__)
#define FORCE_INLINE __always_inline
#pragma GCC diagnostic ignored "-Wattributes"
#else
#define FORCE_INLINE inline
#define __attribute_noinline__
#define __builtin_prefetch(ptr)
#endif

static FORCE_INLINE uint64_t GetUnalignedU64(const void* ptr) noexcept {
  uint64_t x;
  memcpy(&x, ptr, sizeof(uint64_t));
  return x;
}

struct BytewiseCompareInternalKey {
  BytewiseCompareInternalKey(...) {}
  FORCE_INLINE bool operator()(Slice x, Slice y) const noexcept {
    size_t n = std::min(x.size_, y.size_) - 8;
    int cmp = memcmp(x.data_, y.data_, n);
    if (0 != cmp) return cmp < 0;
    if (x.size_ != y.size_) return x.size_ < y.size_;
    return GetUnalignedU64(x.data_ + n) > GetUnalignedU64(y.data_ + n);
  }
  FORCE_INLINE bool operator()(uint64_t x, uint64_t y) const noexcept {
    return x < y;
  }
};
struct RevBytewiseCompareInternalKey {
  RevBytewiseCompareInternalKey(...) {}
  FORCE_INLINE bool operator()(Slice x, Slice y) const noexcept {
    size_t n = std::min(x.size_, y.size_) - 8;
    int cmp = memcmp(x.data_, y.data_, n);
    if (0 != cmp) return cmp > 0;
    if (x.size_ != y.size_) return x.size_ > y.size_;
    return GetUnalignedU64(x.data_ + n) > GetUnalignedU64(y.data_ + n);
  }
  FORCE_INLINE bool operator()(uint64_t x, uint64_t y) const noexcept {
    return x > y;
  }
};
struct FallbackVirtCmp {
  FORCE_INLINE bool operator()(Slice x, Slice y) const {
    return icmp->Compare(x, y) < 0;
  }
  const InternalKeyComparator* icmp;
};

void OutputValidator::Init() {
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
