// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).


#include "utilities/transactions/lock/point/point_lock_tracker.h"
#include <port/likely.h>

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

PointLockTracker::PointLockTracker() : tracked_keys_(0) {
}

void PointLockTracker::Track(const PointLockRequest& r) {
  auto& keys = tracked_keys_[r.column_family_id];
  auto result = keys.try_emplace(r.key, r.seq);
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
}

UntrackStatus PointLockTracker::Untrack(const PointLockRequest& r) {
  auto cf_keys = tracked_keys_.find(r.column_family_id);
  if (cf_keys == tracked_keys_.end()) {
    return UntrackStatus::NOT_TRACKED;
  }

  auto& keys = cf_keys->second;
  auto it = keys.find(r.key);
  if (it == keys.end()) {
    return UntrackStatus::NOT_TRACKED;
  }

  bool untracked = false;
  auto& info = it->second;
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
    keys.erase(it);
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
        auto [idx, success] = current_keys.insert_i(key, info);
        if (!success) {
          current_keys.val(idx).Merge(info);
        }
      #endif
      }
    }
  }
}

void PointLockTracker::Subtract(const LockTracker& tracker) {
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
}

LockTracker* PointLockTracker::GetTrackedLocksSinceSavePoint(
    const LockTracker& save_point_tracker) const {
  // Examine the number of reads/writes performed on all keys written
  // since the last SavePoint and compare to the total number of reads/writes
  // for each key.
  LockTracker* t = new PointLockTracker();
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
  return t;
}

PointLockStatus PointLockTracker::GetPointLockStatus(
    ColumnFamilyId column_family_id, const LockString& key) const {
  assert(IsPointLockSupported());
  PointLockStatus status;
  auto keys = tracked_keys_.get_value_ptr(column_family_id);
  if (LIKELY(nullptr != keys)) {
    auto idx = keys->find_i(key);
    if (LIKELY(idx < keys->end_i())) {
      const TrackedKeyInfo& key_info = keys->val(idx);
      status.locked = true;
      status.exclusive = key_info.exclusive;
      status.seq = key_info.seq;
    }
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
    if (tk_info.bucket_size() > 1000)
      tk_info.clear(); // will free memory
    else
      tk_info.erase_all(); // will not free memory
  }
}

}  // namespace ROCKSDB_NAMESPACE

