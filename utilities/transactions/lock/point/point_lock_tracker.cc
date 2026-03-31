// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).


#include "utilities/transactions/lock/point/point_lock_tracker.h"
#include <port/likely.h>
#include <terark/valvec32.hpp>

namespace ROCKSDB_NAMESPACE {

namespace {

class TrackedKeysColumnFamilyIterator
    : public LockTracker::ColumnFamilyIterator {
 public:
  explicit TrackedKeysColumnFamilyIterator(const TrackedKeys& keys)
      : tracked_keys_(keys), it_(keys.begin()) {}

  bool HasNext() const override { return it_ != tracked_keys_.end(); }

  ColumnFamilyId Next() override { return (it_++)->first; }

 private:
  const TrackedKeys& tracked_keys_;
  TrackedKeys::const_iterator it_;
};

class TrackedKeysIterator : public LockTracker::KeyIterator {
 public:
  TrackedKeysIterator(const TrackedKeys& keys, ColumnFamilyId id)
      : key_infos_(keys.at(id)), it_(key_infos_.begin()) {}

  bool HasNext() const override { return it_ != key_infos_.end(); }

#if 0
  const std::string& Next() override { return (it_++)->first; }
#else
  const terark::fstring Next() override { return (it_++)->first; }
#endif

 private:
  const TrackedKeyInfos& key_infos_;
  TrackedKeyInfos::const_iterator it_;
};

}  // namespace

struct PointLockTrackerDelta : public LockTracker {
  struct DeltaEntry {
    uint32_t num_writes : 31;
    uint32_t exclusive  :  1;
    uint32_t num_reads  : 31;
    uint32_t is_in_use  :  1;
    DeltaEntry() : num_writes(0), exclusive(0), num_reads(0), is_in_use(0) {}
  };
  struct OneCF : terark::valvec32<DeltaEntry>, boost::noncopyable {
    size_t num_locks = 0;
    size_t min_idx = SIZE_MAX;
  };
  using CFMap = terark::VectorIndexMap<ColumnFamilyId, OneCF>;
  CFMap cf_delta_vec_;
  const PointLockTracker* base_tracker_;
  struct CF_Iter;
  struct Key_Iter;
  PointLockTrackerDelta(const PointLockTracker*);
  PointLockTrackerDelta(const PointLockTrackerDelta&) = delete;
  PointLockTrackerDelta& operator=(const PointLockTrackerDelta&) = delete;
  void Track(const PointLockRequest&) override;
  void Track(const RangeLockRequest&) override;
  UntrackStatus Untrack(const PointLockRequest&) override;
  UntrackStatus Untrack(const RangeLockRequest&) override;
  void Merge(const LockTracker&) override;
  void Subtract(const LockTracker&) override;
  void Clear() override;
  LockTracker* GetTrackedLocksSinceSavePoint(const LockTracker&) const override;
  PointLockStatus GetPointLockStatus(ColumnFamilyId, const LockString& key, size_t key_hash) const override;
  uint64_t GetNumPointLocks() const override;
  ColumnFamilyIterator* GetColumnFamilyIterator() const override;
  KeyIterator* GetKeyIterator(ColumnFamilyId) const override;
};

PointLockTracker::PointLockTracker() : tracked_keys_(0) {
  m_is_point_lock_supported = true;
}

PointLockTracker::~PointLockTracker() {
}

void PointLockTracker::Track(const PointLockRequest& r) {
  auto& keys = tracked_keys_[r.column_family_id];
  if (r.hint != SIZE_MAX) {
    if (r.iter >= keys.end_i()) {
      auto  cons = terark::CopyConsFunc<TrackedKeyInfo>(r.seq);
      auto  iter = keys.hint_insert_with_hash_i(r.key, r.key_hash, r.hint, cons);
      auto& info = keys.val(iter);
      if (r.read_only) {
        info.num_reads = 1;
      } else {
        info.num_writes = 1;
      }
      info.exclusive = r.exclusive;
      info.key_hash = r.key_hash;
      keys.m_last_hit = iter;
    } else {
      keys.m_last_hit = r.iter;
      auto& info = keys.val(r.iter);
      if (r.read_only) {
        info.num_reads++;
      } else {
        info.num_writes++;
      }
      info.exclusive = r.exclusive || info.exclusive;
      ROCKSDB_ASSERT_EQ(info.key_hash, r.key_hash);
      assert(r.key == keys.key(r.iter));
    }
    return;
  }
  auto result = keys.try_emplace(r.key, r.key_hash, r.seq);
  auto it = result.first;
  if (!result.second && r.seq < it->second.seq) {
    // Now tracking this key with an earlier sequence number
    it->second.seq = r.seq;
  }
  // else we do not update the seq. The smaller the tracked seq, the stronger it
  // the guarantee since it implies from the seq onward there has not been a
  // concurrent update to the key. So we update the seq if it implies stronger
  // guarantees, i.e., if it is smaller than the existing tracked seq.

  if (r.read_only) {
    it->second.num_reads++;
  } else {
    it->second.num_writes++;
  }

  it->second.exclusive = it->second.exclusive || r.exclusive;
  it->second.key_hash = r.key_hash;
}

UntrackStatus PointLockTracker::Untrack(const PointLockRequest& r) {
  auto cf_keys = tracked_keys_.find(r.column_family_id);
  if (cf_keys == tracked_keys_.end()) {
    return UntrackStatus::NOT_TRACKED;
  }

  auto& keys = cf_keys->second;
  const size_t idx = keys.find_with_hash_i(r.key, r.key_hash);
  if (idx >= keys.end_i()) {
    return UntrackStatus::NOT_TRACKED;
  }

  bool untracked = false;
  auto& info = keys.val(idx);
  if (r.read_only) {
    if (info.num_reads > 0) {
      info.num_reads--;
      untracked = true;
    }
  } else {
    if (info.num_writes > 0) {
      info.num_writes--;
      untracked = true;
    }
  }

  bool removed = false;
  if (info.num_reads == 0 && info.num_writes == 0) {
    keys.erase_with_hash_i(idx, r.key_hash);
    if (keys.empty()) {
      keys.erase_all(); // set to clean state and keep memory
    }
    removed = true;
  }

  if (removed) {
    return UntrackStatus::REMOVED;
  }
  if (untracked) {
    return UntrackStatus::UNTRACKED;
  }
  return UntrackStatus::NOT_TRACKED;
}

void PointLockTracker::Merge(const LockTracker& tracker) {
  assert(dynamic_cast<const PointLockTrackerDelta*>(&tracker) == nullptr);
  const PointLockTracker& t = static_cast<const PointLockTracker&>(tracker);
  for (const auto& cf_keys : t.tracked_keys_) {
    const auto& keys = cf_keys.second;

    auto [current_cf_keys, insert_cf_ok] = tracked_keys_.emplace(cf_keys);
    if (!insert_cf_ok) { // cf existed, do merge
      auto& current_keys = current_cf_keys->second;
      for (const auto& key_info : keys) {
        const auto& key = key_info.first;
        const TrackedKeyInfo& info = key_info.second;
        // If key was not previously tracked, just copy the whole struct over.
        // Otherwise, some merging needs to occur.
      #if 0
        auto current_info = current_keys.find(key);
        if (current_info == current_keys.end()) {
          current_keys.emplace(key_info);
        } else {
          current_info->second.Merge(info);
        }
      #else
        auto [idx, success] = current_keys.lazy_insert_with_hash_i
             (key, info.key_hash, terark::CopyConsFunc<TrackedKeyInfo>(info));
        if (!success) {
          current_keys.val(idx).Merge(info);
        }
      #endif
      }
    }
  }
}

void PointLockTracker::Subtract(const LockTracker& tracker) {
 #if 1
  auto delta_tracker = dynamic_cast<const PointLockTrackerDelta*>(&tracker);
  assert(delta_tracker != nullptr);
  for (const auto& [cf_id, delta_vec] : delta_tracker->cf_delta_vec_) {
    auto& base_map = tracked_keys_.at(cf_id);
    for (size_t i = delta_vec.min_idx; i < delta_vec.size(); i++) {
      if (base_map.is_deleted(i)) {
        assert(delta_vec[i].is_in_use == 0);
        continue;
      }
      if (auto& delta_info = delta_vec[i]; delta_info.is_in_use) {
        auto& base_info = base_map.val(i);
        assert(base_info.num_reads >= delta_info.num_reads);
        assert(base_info.num_writes >= delta_info.num_writes);
        if (delta_info.num_reads > 0)
          base_info.num_reads -= delta_info.num_reads;
        if (delta_info.num_writes > 0)
          base_info.num_writes -= delta_info.num_writes;
        if (base_info.num_reads == 0 && base_info.num_writes == 0)
          base_map.erase_with_hash_i(i, base_info.key_hash);
      }
    }
  }
 #else
  const PointLockTracker& t = static_cast<const PointLockTracker&>(tracker);
  for (const auto& cf_keys : t.tracked_keys_) {
    ColumnFamilyId cf = cf_keys.first;
    const auto& keys = cf_keys.second;

    auto& current_keys = tracked_keys_.at(cf);
    for (const auto& key_info : keys) {
      const auto& key = key_info.first;
      const TrackedKeyInfo& info = key_info.second;
      uint32_t num_reads = info.num_reads;
      uint32_t num_writes = info.num_writes;

      auto current_key_info = current_keys.find(key);
      assert(current_key_info != current_keys.end());

      // Decrement the total reads/writes of this key by the number of
      // reads/writes done since the last SavePoint.
      if (num_reads > 0) {
        assert(current_key_info->second.num_reads >= num_reads);
        current_key_info->second.num_reads -= num_reads;
      }
      if (num_writes > 0) {
        assert(current_key_info->second.num_writes >= num_writes);
        current_key_info->second.num_writes -= num_writes;
      }
      if (current_key_info->second.num_reads == 0 &&
          current_key_info->second.num_writes == 0) {
        current_keys.erase(current_key_info);
      }
    }
  }
 #endif
}

LockTracker* PointLockTracker::GetTrackedLocksSinceSavePoint(
    const LockTracker& save_point_tracker) const {
  // Examine the number of reads/writes performed on all keys written
  // since the last SavePoint and compare to the total number of reads/writes
  // for each key.
  LockTracker* t = new PointLockTracker();
 #if 1
  auto delta = dynamic_cast<const PointLockTrackerDelta*>(&save_point_tracker);
  assert(delta != nullptr);
  for (const auto& [cf_id, delta_vec] : delta->cf_delta_vec_) {
    const auto& base_map = tracked_keys_.at(cf_id);
    for (size_t i = delta_vec.min_idx; i < delta_vec.size(); i++) {
      if (base_map.is_deleted(i)) {
        assert(delta_vec[i].is_in_use == 0);
        continue;
      }
      const auto& key = base_map.key(i);
      const auto& delta_info = delta_vec[i];
      auto base_info = base_map.val(i);
      assert(base_info.num_reads >= delta_info.num_reads);
      assert(base_info.num_writes >= delta_info.num_writes);
      if (base_info.num_reads == delta_info.num_reads &&
          base_info.num_writes == delta_info.num_writes) {
        // All the reads/writes to this key were done in the last savepoint.
        PointLockRequest r;
        r.column_family_id = cf_id;
        r.key = Slice(key.data(), key.size());
        r.seq = base_info.seq;
        r.key_hash = base_info.key_hash;
        r.read_only = (delta_info.num_writes == 0);
        r.exclusive = delta_info.exclusive;
        t->Track(r);
      }
    }
  }
 #else
  const PointLockTracker& save_point_t =
      static_cast<const PointLockTracker&>(save_point_tracker);
  for (const auto& cf_keys : save_point_t.tracked_keys_) {
    ColumnFamilyId cf = cf_keys.first;
    const auto& keys = cf_keys.second;

    auto& current_keys = tracked_keys_.at(cf);
    for (const auto& key_info : keys) {
      const auto& key = key_info.first;
      const TrackedKeyInfo& info = key_info.second;
      uint32_t num_reads = info.num_reads;
      uint32_t num_writes = info.num_writes;

      auto current_key_info = current_keys.find(key);
      assert(current_key_info != current_keys.end());
      assert(current_key_info->second.num_reads >= num_reads);
      assert(current_key_info->second.num_writes >= num_writes);

      if (current_key_info->second.num_reads == num_reads &&
          current_key_info->second.num_writes == num_writes) {
        // All the reads/writes to this key were done in the last savepoint.
        PointLockRequest r;
        r.column_family_id = cf;
        r.key = Slice(key.data(), key.size());
        r.seq = info.seq;
        r.read_only = (num_writes == 0);
        r.exclusive = info.exclusive;
        t->Track(r);
      }
    }
  }
 #endif
  return t;
}

PointLockStatus PointLockTracker::GetPointLockStatus(
    ColumnFamilyId column_family_id, const LockString& key, size_t key_hash) const {
  assert(IsPointLockSupported());
  PointLockStatus status;
  auto keys = tracked_keys_.get_value_ptr(column_family_id);
  if (LIKELY(nullptr != keys)) {
    auto [idx, hint] = keys->find_hint_with_hash_i(key, key_hash);
    if (LIKELY(idx < keys->end_i())) {
      const TrackedKeyInfo& key_info = keys->val(idx);
      status.locked = true;
      status.exclusive = key_info.exclusive;
      status.seq = key_info.seq;
      status.iter = uint32_t(idx);
    }
    status.hint = hint;
  }
  return status;
}

uint64_t PointLockTracker::GetNumPointLocks() const {
  uint64_t num_keys = 0;
  for (const auto& cf_keys : tracked_keys_) {
    num_keys += cf_keys.second.size();
  }
  return num_keys;
}

LockTracker::ColumnFamilyIterator* PointLockTracker::GetColumnFamilyIterator()
    const {
  return new TrackedKeysColumnFamilyIterator(tracked_keys_);
}

LockTracker::KeyIterator* PointLockTracker::GetKeyIterator(
    ColumnFamilyId column_family_id) const {
  assert(tracked_keys_.find(column_family_id) != tracked_keys_.end());
  return new TrackedKeysIterator(tracked_keys_, column_family_id);
}

void PointLockTracker::Clear() {
  for (auto& [cf_id, tk_info] : tracked_keys_) {
    tk_info.erase_all(); // will not free memory
  }
}

/////////////////////////////////////////////////////////////////////////////

PointLockTrackerDelta::PointLockTrackerDelta(const PointLockTracker* base) {
  m_is_point_lock_supported = true;
  base_tracker_ = base;
  for (auto& [cf_id, base_map] : base->tracked_keys_) {
    OneCF& cf = cf_delta_vec_[cf_id];
    cf.reserve(base_map.capacity());
    cf.resize(base_map.end_i());
  }
}

template<class Vec>
inline auto& EnsureGetLikelyPushBack(Vec& v, size_t i) {
  if (LIKELY(i < v.capacity())) {
    auto ptr = v.data();
    if (LIKELY(v.size() == i)) {
      // act as push_back({})
      using ValueType = typename Vec::value_type;
      new(&ptr[i])ValueType();
      v.risk_set_size(i + 1);
      return ptr[i];
    }
    if (LIKELY(i < v.size())) {
      return ptr[i];
    }
  }
  return v.ensure_get(i);
}

void PointLockTrackerDelta::Track(const PointLockRequest& r) {
  auto& base_map = base_tracker_->tracked_keys_.at(r.column_family_id);
  assert(base_map.end_i() > 0);
  size_t idx = base_map.m_last_hit;
 #if 0
  if (UNLIKELY(!(idx < base_map.end_i() && !base_map.is_deleted(idx) &&
                 SliceEqual(SliceOf(base_map.key(idx)), r.key)))) {
    idx = base_map.find_i(r.key);
    ROCKSDB_ASSERT_LT(idx, base_map.end_i());
  }
 #else
  ROCKSDB_ASSERT_LT(idx, base_map.end_i());
  ROCKSDB_ASSERT_F(!base_map.is_deleted(idx), "idx %zd", idx);
  ROCKSDB_ASSERT_F(base_map.key(idx) == r.key, "idx %zd", idx);
 #endif
  auto& delta_vec = cf_delta_vec_[r.column_family_id];
  terark::minimize(delta_vec.min_idx, idx);
  auto& info = EnsureGetLikelyPushBack(delta_vec, idx);
  if (!info.is_in_use) {
    delta_vec.num_locks++;
    info.is_in_use = 1;
    info.exclusive = r.exclusive;
    if (r.read_only)
      info.num_reads = 1;
    else
      info.num_writes = 1;
  } else {
    info.exclusive |= r.exclusive;
    if (r.read_only)
      info.num_reads++;
    else
      info.num_writes++;
  }
}

UntrackStatus PointLockTrackerDelta::Untrack(const PointLockRequest& r) {
  auto& base_map = base_tracker_->tracked_keys_.at(r.column_family_id);
  auto& delta_vec = cf_delta_vec_[r.column_family_id];
  ROCKSDB_ASSERT_LE(delta_vec.size(), base_map.end_i());
  size_t idx = base_map.find_with_hash_i(r.key, r.key_hash);
  if (UNLIKELY(idx >= delta_vec.size())) {
    return UntrackStatus::NOT_TRACKED;
  }
  auto& info = delta_vec[idx];
  if (info.is_in_use == 0) {
    return UntrackStatus::NOT_TRACKED;
  }
  assert(info.num_reads || info.num_writes);
  if (r.read_only) {
    if (info.num_reads > 0) {
      info.num_reads--;
    }
  } else {
    if (info.num_writes > 0) {
      info.num_writes--;
    }
  }
  if (info.num_reads == 0 && info.num_writes == 0) {
    info.is_in_use = 0;
    info.exclusive = 0;
    delta_vec.num_locks--;
    if (delta_vec.num_locks == 0) {
      delta_vec.min_idx = delta_vec.size();
    }
    else if (delta_vec.min_idx == idx) {
      delta_vec.min_idx = idx + 1; // reduce scan
    }
    return UntrackStatus::REMOVED;
  } else {
    return UntrackStatus::UNTRACKED;
  }
}

void PointLockTrackerDelta::Track(const RangeLockRequest&) {
  // do nothing
}

UntrackStatus PointLockTrackerDelta::Untrack(const RangeLockRequest&) {
  return UntrackStatus::NOT_TRACKED;
}

void PointLockTrackerDelta::Merge(const LockTracker& vsrc) {
  auto& src = static_cast<const PointLockTrackerDelta&>(vsrc);
  for (auto& [cf_id, src_vec] : src.cf_delta_vec_) {
    auto& dst_vec = cf_delta_vec_[cf_id];
    dst_vec.resize(base_tracker_->tracked_keys_.at(cf_id).size());
    size_t start = std::min(dst_vec.min_idx, src_vec.min_idx);
    for (; start < std::min(dst_vec.size() , src_vec.size()); start++) {
      auto& src_entry = src_vec[start];
      auto& dst_entry = dst_vec[start];
      if (src_entry.is_in_use || dst_entry.is_in_use)
        break;
    }
    dst_vec.min_idx = start; // update for optimize scan
    for (size_t i = start; i < src_vec.size(); i++) {
      if (auto& src_entry = src_vec[i]; src_entry.is_in_use) {
        auto& dst_entry = dst_vec[i];
        if (!dst_entry.is_in_use) {
          dst_entry.is_in_use = 1;
          dst_vec.num_locks++;
        }
        dst_entry.exclusive |= src_entry.exclusive;
        dst_entry.num_reads += src_entry.num_reads;
        dst_entry.num_writes += src_entry.num_writes;
      }
    }
  }
}

void PointLockTrackerDelta::Subtract(const LockTracker& tracker) {
  ROCKSDB_DIE("Should not goes here");
}

void PointLockTrackerDelta::Clear() {
  for (auto& [cf_id, delta_vec] : cf_delta_vec_) {
    delta_vec.erase_all(); // will not free memory
    delta_vec.min_idx = SIZE_MAX;
    delta_vec.num_locks = 0;
  }
}

LockTracker* PointLockTrackerDelta::GetTrackedLocksSinceSavePoint(
    const LockTracker& save_point_tracker) const {
  ROCKSDB_DIE("Should not goes here");
}

PointLockStatus
PointLockTrackerDelta::GetPointLockStatus(ColumnFamilyId cf_id,
                                          const LockString& key,
                                          size_t key_hash) const {
  PointLockStatus status;
  auto base_map = base_tracker_->tracked_keys_.get_value_ptr(cf_id);
  auto delta_vec = cf_delta_vec_.get_value_ptr(cf_id);
  if (LIKELY(base_map && delta_vec)) {
    auto idx = base_map->find_with_hash_i(key, key_hash);
    if (LIKELY(idx < base_map->end_i() && idx < delta_vec->size())) {
      const auto& delta_info = delta_vec->at(idx);
      if (delta_info.is_in_use) {
        const auto& base_info = base_map->val(idx);
        status.locked = true;
        status.exclusive = delta_info.exclusive;
        status.seq = base_info.seq;
      }
    }
  }
  return status;
}

uint64_t PointLockTrackerDelta::GetNumPointLocks() const {
  uint64_t num_keys = 0;
  for (const auto& cf_keys : cf_delta_vec_) {
    num_keys += cf_keys.second.num_locks;
  }
  return num_keys;
}

struct PointLockTrackerDelta::CF_Iter : public ColumnFamilyIterator {
  CF_Iter(const CFMap& keys) : cf_map_(keys), it_(keys.begin()) {}
  bool HasNext() const override { return it_ != cf_map_.end(); }
  ColumnFamilyId Next() override { return (it_++)->first; }
  const CFMap& cf_map_;
  CFMap::const_iterator it_;
};

struct PointLockTrackerDelta::Key_Iter : public KeyIterator {
  Key_Iter(const TrackedKeyInfos& base, const OneCF& delta_vec)
    : base_map_(base), delta_vec_(delta_vec) {
    size_t idx = delta_vec.min_idx, num = delta_vec.size();
    for (; idx < num; idx++) {
      if (delta_vec[idx].is_in_use)
        break;
    }
    idx_ = idx;
    num_ = num; // make HasNext() be fast
  }
  bool HasNext() const override { return idx_ < num_; }
  const terark::fstring Next() override {
    ROCKSDB_ASSERT_LT(idx_, num_);
    size_t curr = idx_, next = idx_ + 1;
    for (size_t num = num_; next < num; next++) {
      if (delta_vec_[next].is_in_use)
        break;
    }
    idx_ = next;
    assert(base_map_.is_deleted(curr) == false);
    return base_map_.key(curr);
  }
  const TrackedKeyInfos& base_map_;
  const OneCF& delta_vec_;
  size_t idx_, num_;
};

PointLockTrackerDelta::ColumnFamilyIterator*
PointLockTrackerDelta::GetColumnFamilyIterator() const {
  return new CF_Iter(cf_delta_vec_);
}

PointLockTrackerDelta::KeyIterator*
PointLockTrackerDelta::GetKeyIterator(ColumnFamilyId cf_id) const {
  auto& base_map = base_tracker_->tracked_keys_.at(cf_id);
  auto& delta_vec = cf_delta_vec_.at(cf_id);
  return new Key_Iter(base_map, delta_vec);
}

LockTracker*
PointLockTrackerFactory::CreateDelta(const LockTracker* base) const {
  assert(dynamic_cast<const PointLockTracker*>(base) != nullptr);
  return new PointLockTrackerDelta(static_cast<const PointLockTracker*>(base));
}

}  // namespace ROCKSDB_NAMESPACE

