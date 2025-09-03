// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <terark/gold_hash_map.hpp>
#include <terark/hash_strmap.hpp>
#include <terark/util/vec_idx_map.hpp>

#include "utilities/transactions/lock/lock_tracker.h"

namespace ROCKSDB_NAMESPACE {

struct TrackedKeyInfo {
  // Earliest sequence number that is relevant to this transaction for this key
  SequenceNumber seq;

  uint32_t num_writes : 31;
  uint32_t exclusive  :  1;
  uint32_t num_reads;
  size_t   key_hash;

  explicit TrackedKeyInfo(SequenceNumber seq_no)
      : seq(seq_no), num_writes(0), exclusive(false), num_reads(0) {}

  void Merge(const TrackedKeyInfo& info) {
    assert(seq <= info.seq);
    num_reads += info.num_reads;
    num_writes += info.num_writes;
    exclusive = exclusive || info.exclusive;
  }
};

#if 0
using TrackedKeyInfos = std::unordered_map<std::string, TrackedKeyInfo>;
#elif POINT_LOCK_HASH_MAP_TYPE == 2
#pragma pack(push, 4)
struct TrackedKeyAndInfo {
  using KeySSO = terark::minimal_sso<36, false, 4>;
  TrackedKeyInfo second; // here, second should before first
  KeySSO first;
  // uint32_t link will be added by gold_hash_tab here
};
#pragma pack(pop)
static_assert(sizeof(TrackedKeyAndInfo) == 60);
struct TrackedKeyExtractor {
  terark::fstring operator()(const TrackedKeyAndInfo& x) const
    { return x.first.to<terark::fstring>(); }
};
struct TrackedKeyInfos : terark::gold_hash_tab<terark::fstring
     , TrackedKeyAndInfo
     , terark::hash_and_equal<terark::fstring, StrNPHash64
                             ,terark::fstring_func::equal_align
                             >
     , TrackedKeyExtractor
     , terark::node_layout<TrackedKeyAndInfo, uint32_t,
                           terark::FastCopy, terark::ValueInline>
     > {
  TrackedKeyInfos() {
    size_t cap = 8;
    this->reserve(cap);
    this->enable_freelist();
    this->disable_auto_compact();
  }
  auto try_emplace(terark::fstring key, size_t key_hash, uint64_t seq) {
    auto cons_kv = [=](TrackedKeyAndInfo* pair) {
      new(&pair->first)TrackedKeyAndInfo::KeySSO(key);
      new(&pair->second)TrackedKeyInfo(seq);
    };
    auto ib = lazy_insert_elem_with_hash_i(key, key_hash, cons_kv);
    m_last_hit = ib.first;
    return std::pair<iterator, bool>(iterator(this, ib.first), ib.second);
  }
  template<class ConsValue>
  auto lazy_insert_with_hash_i(terark::fstring key, size_t h, ConsValue cons) {
    auto cons_kv = [=](TrackedKeyAndInfo* pair) {
      new(&pair->first)TrackedKeyAndInfo::KeySSO(key);
      cons(&pair->second);
    };
    return lazy_insert_elem_with_hash_i(key, h, cons_kv);
  }
  template<class ConsValue>
  size_t hint_insert_with_hash_i(terark::fstring key, size_t key_hash,
                                 size_t hint, ConsValue cons) {
    auto cons_kv = [=](TrackedKeyAndInfo* pair) {
      new(&pair->first)TrackedKeyAndInfo::KeySSO(key);
      cons(&pair->second);
    };
    return hint_insert_elem_with_hash_i(key, key_hash, hint, cons_kv);
  }
  // key must return by value, gold_hash_tab::key is return by ref
  auto  key(size_t i) const { return elem_at(i).first.to<terark::fstring>(); }
  auto& val(size_t i) const { return elem_at(i).second; }
  auto& val(size_t i)       { return elem_at(i).second; }
  size_t m_last_hit = SIZE_MAX;
};
#else
struct TrackedKeyInfos : terark::hash_strmap<TrackedKeyInfo
      , StrNPHash64
      , terark::fstring_func::equal_align
      , terark::ValueInline, terark::FastCopy
      , unsigned, size_t, true
      > {
  TrackedKeyInfos() {
    size_t cap = 8;
    size_t strpool_cap = 1024;
    this->reserve(cap, strpool_cap);
    this->enable_freelist();
    this->disable_auto_compact();
  }
  auto try_emplace(terark::fstring key, size_t key_hash, uint64_t seq) {
    auto ib = lazy_insert_with_hash_i(key, key_hash,
                  terark::CopyConsFunc<TrackedKeyInfo>(seq));
    m_last_hit = ib.first;
    return std::pair<iterator, bool>(iterator(this, ib.first), ib.second);
  }
  size_t m_last_hit = SIZE_MAX;
};
#endif

using TrackedKeys = terark::VectorIndexMap<ColumnFamilyId, TrackedKeyInfos>;

// Tracks point locks on single keys.
class PointLockTracker : public LockTracker {
 public:
  PointLockTracker();
  ~PointLockTracker() override;

  PointLockTracker(const PointLockTracker&) = delete;
  PointLockTracker& operator=(const PointLockTracker&) = delete;

  void Track(const PointLockRequest& lock_request) override;

  UntrackStatus Untrack(const PointLockRequest& lock_request) override;

  void Track(const RangeLockRequest& /*lock_request*/) override {}

  UntrackStatus Untrack(const RangeLockRequest& /*lock_request*/) override {
    return UntrackStatus::NOT_TRACKED;
  }

  void Merge(const LockTracker& tracker) override;

  void Subtract(const LockTracker& tracker) override;

  void Clear() override;

  virtual LockTracker* GetTrackedLocksSinceSavePoint(
      const LockTracker& save_point_tracker) const override;

  PointLockStatus GetPointLockStatus(ColumnFamilyId column_family_id,
                                     const LockString& key,
                                     size_t key_hash) const override;

  uint64_t GetNumPointLocks() const override;

  ColumnFamilyIterator* GetColumnFamilyIterator() const override;

  KeyIterator* GetKeyIterator(ColumnFamilyId column_family_id) const override;

 //private:
  TrackedKeys tracked_keys_;
};

class PointLockTrackerFactory : public LockTrackerFactory {
 public:
  static const PointLockTrackerFactory& Get() {
    static const PointLockTrackerFactory instance;
    return instance;
  }

  LockTracker* Create() const override { return new PointLockTracker(); }
  LockTracker* CreateDelta(const LockTracker*) const override;

 private:
  PointLockTrackerFactory() {}
};

}  // namespace ROCKSDB_NAMESPACE
