//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).


#include "utilities/transactions/lock/point/point_lock_manager.h"

#include <algorithm>
#include <cinttypes>
#include <mutex>

#include "monitoring/perf_context_imp.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction_db_mutex.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/hash.h"
#include "util/thread_local.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"

#include "point_lock_tracker.h"

namespace ROCKSDB_NAMESPACE {

struct LockInfo {
  static_assert(sizeof(bool) == 1);
  bool& exclusive() { return reinterpret_cast<bool&>(txn_ids.pad_u08()); }
  bool exclusive() const { return txn_ids.pad_u08() != 0; }
  autovector<TransactionID> txn_ids;

  // Transaction locks are not valid after this time in us
  uint64_t expiration_time;

  LockInfo(TransactionID id, uint64_t time, bool ex)
      : expiration_time(time) {
    exclusive() = ex;
    txn_ids.push_back(id);
  }
};

struct LockMapStripe : private boost::noncopyable {
  explicit LockMapStripe(TransactionDBMutexFactory* factory) {
    stripe_mutex = factory->AllocateMutex();
    stripe_cv = factory->AllocateCondVar();
    assert(stripe_mutex);
    assert(stripe_cv);
  }

  // Mutex must be held before modifying keys map
  std::shared_ptr<TransactionDBMutex> stripe_mutex;

  // Condition Variable per stripe for waiting on a lock
  std::shared_ptr<TransactionDBCondVar> stripe_cv;

  size_t padding[2] = {0};

  // Locked keys mapped to the info about the transactions that locked them.
  // TODO(agiardullo): Explore performance of other data structures.
#if 0
  UnorderedMap<std::string, LockInfo> keys;
#else
  struct KeyStrMap : terark::hash_strmap<LockInfo> {
    KeyStrMap() {
      size_t cap = 8;
      size_t strpool_cap = 1024;
      this->reserve(cap, strpool_cap);
      this->enable_freelist();
    }
  };
  KeyStrMap keys;
#endif
};
static_assert(sizeof(LockMapStripe) == 128);

// Map of #num_stripes LockMapStripes
struct LockMap : private boost::noncopyable {
  explicit LockMap(uint16_t key_prefix_len, uint16_t super_stripes,
                   size_t num_stripes, TransactionDBMutexFactory* factory) {
    key_prefix_len_ = std::min<uint16_t>(8, key_prefix_len);
    if (0 == key_prefix_len)
      super_stripes_ = 1;
    else
      super_stripes_ = std::max<uint16_t>(1, super_stripes);
    num_stripes_ = uint32_t(std::max<size_t>(1, num_stripes));
    lock_map_stripes_.reserve_aligned(128, num_stripes * super_stripes);
    for (size_t i = 0; i < num_stripes * super_stripes; i++) {
      lock_map_stripes_.unchecked_emplace_back(factory);
    }
  }

  // Number of sepearate LockMapStripes to create, each with their own Mutex
  uint16_t key_prefix_len_;
  uint16_t super_stripes_;
  uint32_t num_stripes_;

  terark::valvec<LockMapStripe> lock_map_stripes_;

  char padding[48] = {0}; // to avoid false sharing on lock_cnt

  // Count of keys that are currently locked in this column family.
  // (Only maintained if PointLockManager::max_num_locks_ is positive.)
  std::atomic<int64_t> lock_cnt{0};

  size_t GetStripe(const LockString& key) const;
};

#if defined(ROCKSDB_DYNAMIC_CREATE_CF)
namespace {
void UnrefLockMapsCache(void* ptr) {
  // Called when a thread exits or a ThreadLocalPtr gets destroyed.
  auto lock_maps_cache = static_cast<PointLockManager::LockMaps*>(ptr);
  delete lock_maps_cache;
}
}  // anonymous namespace
#endif

PointLockManager::PointLockManager(PessimisticTransactionDB* txn_db,
                                   const TransactionDBOptions& opt)
    : txn_db_impl_(txn_db),
      key_prefix_len_(opt.key_prefix_len),
      super_stripes_(opt.super_stripes),
      default_num_stripes_(opt.num_stripes),
      max_num_locks_(opt.max_num_locks),
#if defined(ROCKSDB_DYNAMIC_CREATE_CF)
      lock_maps_cache_(&UnrefLockMapsCache),
#endif
      dlock_buffer_(opt.max_num_deadlocks),
      mutex_factory_(opt.custom_mutex_factory
                         ? opt.custom_mutex_factory
                         : std::make_shared<TransactionDBMutexFactoryImpl>()) {}

terark_forceinline
size_t LockMap::GetStripe(const LockString& key) const {
  assert(num_stripes_ > 0);
  auto col = GetSliceNPHash64(key) % num_stripes_;
  if (1 == super_stripes_) {
    return col;
  } else {
    uint64_t pref = 0;
    memcpy(&pref, key.data(), std::min(size_t(key_prefix_len_), key.size()));
    size_t row = pref % super_stripes_;
    return row * num_stripes_ + col;
  }
}

void PointLockManager::AddColumnFamily(const ColumnFamilyHandle* cf) {
  InstrumentedMutexLock l(&lock_map_mutex_);

  auto& lock_map = lock_maps_[cf->GetID()];
  if (!lock_map) {
    lock_map = std::make_shared<LockMap>(key_prefix_len_,
        super_stripes_, default_num_stripes_, mutex_factory_.get());
  } else {
    // column_family already exists in lock map
    assert(false);
  }
}

void PointLockManager::RemoveColumnFamily(const ColumnFamilyHandle* cf) {
  // Remove lock_map for this column family.  Since the lock map is stored
  // as a shared ptr, concurrent transactions can still keep using it
  // until they release their references to it.
  {
    InstrumentedMutexLock l(&lock_map_mutex_);
    if (!lock_maps_.erase(cf->GetID())) {
      return; // not existed and erase did nothing, return immediately
    }
  }  // lock_map_mutex_

#if defined(ROCKSDB_DYNAMIC_CREATE_CF)
  // Clear all thread-local caches
  autovector<void*> local_caches;
  lock_maps_cache_.Scrape(&local_caches, nullptr);
  for (auto cache : local_caches) {
    delete static_cast<LockMaps*>(cache);
  }
#endif
}

template<class T>
static terark_returns_nonnull
inline T* get_ptr_nonnull(const std::shared_ptr<T>& p) { return p.get(); }

// Look up the LockMap std::shared_ptr for a given column_family_id.
// Note:  The LockMap is only valid as long as the caller is still holding on
//   to the returned std::shared_ptr.
inline
LockMap* PointLockManager::GetLockMap(
    ColumnFamilyId column_family_id) {
#if defined(ROCKSDB_DYNAMIC_CREATE_CF)
  // First check thread-local cache
  auto lock_maps_cache = static_cast<LockMaps*>(lock_maps_cache_.Get());
  if (UNLIKELY(lock_maps_cache == nullptr)) {
    lock_maps_cache = new LockMaps();
    lock_maps_cache_.Reset(lock_maps_cache);
  }

  auto lock_map_iter = lock_maps_cache->find(column_family_id);
  if (LIKELY(lock_map_iter != lock_maps_cache->end())) {
    // Found lock map for this column family.
    return lock_map_iter->second.get();
  }

  // Not found in local cache, grab mutex and check shared LockMaps
  InstrumentedMutexLock l(&lock_map_mutex_);

  lock_map_iter = lock_maps_.find(column_family_id);
  if (lock_map_iter == lock_maps_.end()) {
    return nullptr;
  } else {
    // Found lock map.  Store in thread-local cache and return.
    std::shared_ptr<LockMap>& lock_map = lock_map_iter->second;
    lock_maps_cache->insert({column_family_id, lock_map});

    return lock_map.get();
  }
#else
  if (auto result = lock_maps_.get_value_ptr(column_family_id))
    return get_ptr_nonnull(*result);
  else
    return nullptr;
#endif
}

// Returns true if this lock has expired and can be acquired by another
// transaction.
// If false, sets *expire_time to the expiration time of the lock according
// to Env->GetMicros() or 0 if no expiration.
bool PointLockManager::IsLockExpired(TransactionID txn_id,
                                     const LockInfo& lock_info, Env* env,
                                     uint64_t* expire_time) {
  if (lock_info.expiration_time == 0) {
    *expire_time = 0;
    return false;
  }

  auto now = env->NowMicros();
  bool expired = lock_info.expiration_time <= now;
  if (!expired) {
    // return how many microseconds until lock will be expired
    *expire_time = lock_info.expiration_time;
  } else {
    for (auto id : lock_info.txn_ids) {
      if (txn_id == id) {
        continue;
      }

      bool success = txn_db_impl_->TryStealingExpiredTransactionLocks(id);
      if (!success) {
        expired = false;
        *expire_time = 0;
        break;
      }
    }
  }

  return expired;
}

Status PointLockManager::TryLock(PessimisticTransaction* txn,
                                 ColumnFamilyId column_family_id,
                                 const Slice& key, Env* env,
                                 bool exclusive) {
  // Lookup lock map for this column family id
  LockMap* lock_map = GetLockMap(column_family_id);
  if (UNLIKELY(lock_map == nullptr)) {
    char msg[255];
    snprintf(msg, sizeof(msg), "Column family id not found: %" PRIu32,
             column_family_id);

    return Status::InvalidArgument(msg);
  }

  // Need to lock the mutex for the stripe that this key hashes to
  size_t stripe_num = lock_map->GetStripe(key);
  assert(lock_map->lock_map_stripes_.size() > stripe_num);
  LockMapStripe* stripe = &lock_map->lock_map_stripes_[stripe_num];

  LockInfo lock_info(txn->GetID(), txn->GetExpirationTime(), exclusive);
  int64_t timeout = txn->GetLockTimeout();

  return AcquireWithTimeout(txn, lock_map, stripe, column_family_id, key, env,
                            timeout, std::move(lock_info));
}

// Helper function for TryLock().
Status PointLockManager::AcquireWithTimeout(
    PessimisticTransaction* txn, LockMap* lock_map, LockMapStripe* stripe,
    ColumnFamilyId column_family_id, const Slice& key, Env* env,
    int64_t timeout, LockInfo&& lock_info) {
  Status result;
  uint64_t end_time = 0;

  if (timeout > 0) {
    uint64_t start_time = env->NowMicros();
    end_time = start_time + timeout;
  }

  if (timeout < 0) {
    // If timeout is negative, we wait indefinitely to acquire the lock
    result = stripe->stripe_mutex->Lock();
  } else {
    result = stripe->stripe_mutex->TryLockFor(timeout);
  }

  if (!result.ok()) {
    // failed to acquire mutex
    return result;
  }

  // Acquire lock if we are able to
  uint64_t expire_time_hint = 0;
  autovector<TransactionID> wait_ids(0); // init to size and cap = 0
  result = AcquireLocked(lock_map, stripe, key, env, std::move(lock_info),
                         &expire_time_hint, &wait_ids);

  if (!result.ok() && timeout != 0) {
    PERF_TIMER_GUARD(key_lock_wait_time);
    PERF_COUNTER_ADD(key_lock_wait_count, 1);
    // If we weren't able to acquire the lock, we will keep retrying as long
    // as the timeout allows.
    bool timed_out = false;
    do {
      // Decide how long to wait
      int64_t cv_end_time = -1;
      if (expire_time_hint > 0 && end_time > 0) {
        cv_end_time = std::min(expire_time_hint, end_time);
      } else if (expire_time_hint > 0) {
        cv_end_time = expire_time_hint;
      } else if (end_time > 0) {
        cv_end_time = end_time;
      }

      assert(result.IsBusy() || wait_ids.size() != 0);

      // We are dependent on a transaction to finish, so perform deadlock
      // detection.
      if (!wait_ids.empty()) {
        if (txn->IsDeadlockDetect()) {
          if (IncrementWaiters(txn, wait_ids, key, column_family_id,
                               lock_info.exclusive(), env)) {
            result = Status::Busy(Status::SubCode::kDeadlock);
            stripe->stripe_mutex->UnLock();
            return result;
          }
        }
        txn->SetWaitingTxn(wait_ids, column_family_id, &key);
      }

      TEST_SYNC_POINT("PointLockManager::AcquireWithTimeout:WaitingTxn");
      if (cv_end_time < 0) {
        // Wait indefinitely
        result = stripe->stripe_cv->Wait(stripe->stripe_mutex);
      } else {
        uint64_t now = env->NowMicros();
        if (static_cast<uint64_t>(cv_end_time) > now) {
          result = stripe->stripe_cv->WaitFor(stripe->stripe_mutex,
                                              cv_end_time - now);
        }
      }

      if (!wait_ids.empty()) {
        txn->ClearWaitingTxn();
        if (txn->IsDeadlockDetect()) {
          DecrementWaiters(txn, wait_ids);
        }
      }

      if (result.IsTimedOut()) {
        timed_out = true;
        // Even though we timed out, we will still make one more attempt to
        // acquire lock below (it is possible the lock expired and we
        // were never signaled).
      }

      if (result.ok() || result.IsTimedOut()) {
        result = AcquireLocked(lock_map, stripe, key, env, std::move(lock_info),
                               &expire_time_hint, &wait_ids);
      }
    } while (!result.ok() && !timed_out);
  }

  stripe->stripe_mutex->UnLock();

  return result;
}

void PointLockManager::DecrementWaiters(
    const PessimisticTransaction* txn,
    const autovector<TransactionID>& wait_ids) {
  std::lock_guard<std::mutex> lock(wait_txn_map_mutex_);
  DecrementWaitersImpl(txn, wait_ids);
}

void PointLockManager::DecrementWaitersImpl(
    const PessimisticTransaction* txn,
    const autovector<TransactionID>& wait_ids) {
  auto id = txn->GetID();
  assert(wait_txn_map_.contains(id));
  wait_txn_map_.erase(id);

  for (auto wait_id : wait_ids) {
    auto idx = rev_wait_txn_map_.find_i(wait_id);
    if (--rev_wait_txn_map_.val(idx) == 0) {
      rev_wait_txn_map_.erase_i(idx);
    }
  }
}

bool PointLockManager::IncrementWaiters(
    const PessimisticTransaction* txn,
    const autovector<TransactionID>& wait_ids, const Slice& key,
    const uint32_t& cf_id, const bool& exclusive, Env* const env) {
  auto id = txn->GetID();
#if 0
  std::vector<int> queue_parents(
      static_cast<size_t>(txn->GetDeadlockDetectDepth()));
  std::vector<TransactionID> queue_values(
      static_cast<size_t>(txn->GetDeadlockDetectDepth()));
#else
 #define T_alloca_z(T, n) (T*)memset(alloca(sizeof(T)*n), 0, sizeof(T)*n)
  auto depth = txn->GetDeadlockDetectDepth();
  auto queue_parents = T_alloca_z(int, depth);
  auto queue_values = T_alloca_z(TransactionID, depth);
  // if TransactionID is not trivially_destructible, destruct is required
  static_assert(std::is_trivially_destructible<TransactionID>::value);
#endif
  std::lock_guard<std::mutex> lock(wait_txn_map_mutex_);
  assert(!wait_txn_map_.contains(id));

  wait_txn_map_.insert_i(id, {wait_ids, cf_id, exclusive, key});

  for (auto wait_id : wait_ids) {
    rev_wait_txn_map_[wait_id]++;
  }

  // No deadlock if nobody is waiting on self.
  if (!rev_wait_txn_map_.contains(id)) {
    return false;
  }

  const auto* next_ids = &wait_ids;
  int parent = -1;
  int64_t deadlock_time = 0;
  for (int tail = 0, head = 0; head < txn->GetDeadlockDetectDepth(); head++) {
    int i = 0;
    if (next_ids) {
      for (; i < static_cast<int>(next_ids->size()) &&
             tail + i < txn->GetDeadlockDetectDepth();
           i++) {
        queue_values[tail + i] = (*next_ids)[i];
        queue_parents[tail + i] = parent;
      }
      tail += i;
    }

    // No more items in the list, meaning no deadlock.
    if (tail == head) {
      return false;
    }

    auto next = queue_values[head];
    if (next == id) {
      std::vector<DeadlockInfo> path;
      while (head != -1) {
        assert(wait_txn_map_.contains(queue_values[head]));

        auto extracted_info = wait_txn_map_.at(queue_values[head]);
        path.push_back({queue_values[head], extracted_info.m_cf_id,
                        extracted_info.m_exclusive,
                        extracted_info.m_waiting_key.ToString()});
        head = queue_parents[head];
      }
      if (!env->GetCurrentTime(&deadlock_time).ok()) {
        /*
          TODO(AR) this preserves the current behaviour whilst checking the
          status of env->GetCurrentTime to ensure that ASSERT_STATUS_CHECKED
          passes. Should we instead raise an error if !ok() ?
        */
        deadlock_time = 0;
      }
      std::reverse(path.begin(), path.end());
      dlock_buffer_.AddNewPath(DeadlockPath(std::move(path), deadlock_time));
      deadlock_time = 0;
      DecrementWaitersImpl(txn, wait_ids);
      return true;
    } else if (auto idx  = wait_txn_map_.find_i(next);
                    idx == wait_txn_map_.end_i()) {
      next_ids = nullptr;
      continue;
    } else {
      parent = head;
      next_ids = &(wait_txn_map_.val(idx).m_neighbors);
    }
  }

  // Wait cycle too big, just assume deadlock.
  if (!env->GetCurrentTime(&deadlock_time).ok()) {
    /*
      TODO(AR) this preserves the current behaviour whilst checking the status
      of env->GetCurrentTime to ensure that ASSERT_STATUS_CHECKED passes.
      Should we instead raise an error if !ok() ?
    */
    deadlock_time = 0;
  }
  dlock_buffer_.AddNewPath(DeadlockPath(deadlock_time, true));
  DecrementWaitersImpl(txn, wait_ids);
  return true;
}

// Try to lock this key after we have acquired the mutex.
// Sets *expire_time to the expiration time in microseconds
//  or 0 if no expiration.
// REQUIRED:  Stripe mutex must be held.
Status PointLockManager::AcquireLocked(LockMap* lock_map, LockMapStripe* stripe,
                                       const Slice& key, Env* env,
                                       LockInfo&& txn_lock_info,
                                       uint64_t* expire_time,
                                       autovector<TransactionID>* txn_ids) {
  assert(txn_lock_info.txn_ids.size() == 1);

  Status result;
  // Check if this key is already locked
  // topling: use lazy_insert_i(key, cons, check) reduce a find
  auto cons = terark::MoveConsFunc<LockInfo>(std::move(txn_lock_info));
  auto check = [this,&result,lock_map](auto/*keys*/) {
    // max_num_locks_ is signed int64_t
    if (0 != max_num_locks_) {
      if (max_num_locks_ > 0 &&
          lock_map->lock_cnt.load(std::memory_order_acquire) >= max_num_locks_) {
        result = Status::Busy(Status::SubCode::kLockLimit);
        return false; // can not insert the key
      }
      lock_map->lock_cnt.fetch_add(1, std::memory_order_relaxed);
    }
    return true; // ok, insert the key
  };
  auto [idx, miss] = stripe->keys.lazy_insert_i(key, cons, check);
  if (!miss) {
    LockInfo& lock_info = stripe->keys.val(idx);
    assert(lock_info.txn_ids.size() == 1 || !lock_info.exclusive());

    if (lock_info.exclusive() || txn_lock_info.exclusive()) {
      if (lock_info.txn_ids.num_stack_items() == 1 &&
          lock_info.txn_ids[0] == txn_lock_info.txn_ids[0]) {
        // The list contains one txn and we're it, so just take it.
        lock_info.exclusive() = txn_lock_info.exclusive();
        lock_info.expiration_time = txn_lock_info.expiration_time;
      } else {
        // Check if it's expired. Skips over txn_lock_info.txn_ids[0] in case
        // it's there for a shared lock with multiple holders which was not
        // caught in the first case.
        if (IsLockExpired(txn_lock_info.txn_ids[0], lock_info, env,
                          expire_time)) {
          // lock is expired, can steal it
          lock_info.txn_ids = std::move(txn_lock_info.txn_ids);
          lock_info.exclusive() = txn_lock_info.exclusive();
          lock_info.expiration_time = txn_lock_info.expiration_time;
          // lock_cnt does not change
        } else {
          result = Status::TimedOut(Status::SubCode::kLockTimeout);
          *txn_ids = lock_info.txn_ids;
        }
      }
    } else {
      // We are requesting shared access to a shared lock, so just grant it.
      lock_info.txn_ids.push_back(txn_lock_info.txn_ids[0]);
      // Using std::max means that expiration time never goes down even when
      // a transaction is removed from the list. The correct solution would be
      // to track expiry for every transaction, but this would also work for
      // now.
      lock_info.expiration_time =
          std::max(lock_info.expiration_time, txn_lock_info.expiration_time);
    }
  } else {  // Lock not held.
  // do nothing
  }

  return result;
}

void PointLockManager::UnLockKey(PessimisticTransaction* txn,
                                 const LockString& key, LockMapStripe* stripe,
                                 LockMap* lock_map, Env* env) {
#ifdef NDEBUG
  (void)env;
#endif
  TransactionID txn_id = txn->GetID();

  auto stripe_iter = stripe->keys.find(key);
  if (stripe_iter != stripe->keys.end()) {
    auto& txns = stripe_iter->second.txn_ids;
    auto txn_it = std::find(txns.begin(), txns.end(), txn_id);
    // Found the key we locked.  unlock it.
    if (txn_it != txns.end()) {
      if (txns.num_stack_items() == 1) {
        stripe->keys.erase(stripe_iter);
      } else {
        *txn_it = std::move(txns.back());
        txns.pop_back();
      }

      if (max_num_locks_ > 0) {
        // Maintain lock count if there is a limit on the number of locks.
        assert(lock_map->lock_cnt.load(std::memory_order_relaxed) > 0);
        lock_map->lock_cnt--;
      }
    }
  } else {
    // This key is either not locked or locked by someone else.  This should
    // only happen if the unlocking transaction has expired.
    assert(txn->GetExpirationTime() > 0 &&
           txn->GetExpirationTime() < env->NowMicros());
  }
}

void PointLockManager::UnLock(PessimisticTransaction* txn,
                              ColumnFamilyId column_family_id,
                              const Slice& key, Env* env) {
  LockMap* lock_map = GetLockMap(column_family_id);
  if (UNLIKELY(lock_map == nullptr)) {
    // Column Family must have been dropped.
    return;
  }

  // Lock the mutex for the stripe that this key hashes to
  size_t stripe_num = lock_map->GetStripe(key);
  assert(lock_map->lock_map_stripes_.size() > stripe_num);
  LockMapStripe* stripe = &lock_map->lock_map_stripes_[stripe_num];

  stripe->stripe_mutex->Lock().PermitUncheckedError();
  UnLockKey(txn, key, stripe, lock_map, env);
  stripe->stripe_mutex->UnLock();

  // Signal waiting threads to retry locking
  stripe->stripe_cv->NotifyAll();
}

void PointLockManager::UnLock(PessimisticTransaction* txn,
                              const LockTracker& tracker, Env* env) {
  // use single linked list instead of vector to store stripe(partition)
  // this just needs 2 fixed size uint32 vector(valvec)
  auto& ptracker = static_cast<const PointLockTracker&>(tracker);
  for (auto& [cf_id, keyinfos] : ptracker.tracked_keys_) {
    LockMap* lock_map = GetLockMap(cf_id);
    if (!lock_map) continue;
    const uint32_t nil = UINT32_MAX;
    using namespace terark;
    const size_t max_key_idx = keyinfos.end_i();
    const size_t num_stripes = lock_map->lock_map_stripes_.size();
    auto stripe_heads = (uint32_t*)alloca(sizeof(uint32_t) * num_stripes);
    std::fill_n(stripe_heads, num_stripes, nil);
    valvec<uint32_t> keys_link(max_key_idx, valvec_no_init());
    for (size_t idx = 0; idx < max_key_idx; idx++) {
      if (!keyinfos.is_deleted(idx)) {
        const fstring key = keyinfos.key(idx);
        size_t strip_idx = lock_map->GetStripe(key);
        keys_link[idx] = stripe_heads[strip_idx]; // insert to single
        stripe_heads[strip_idx] = uint32_t(idx);  // list front
      }
    }
    for (size_t strip_idx = 0; strip_idx < num_stripes; strip_idx++) {
      uint32_t head = stripe_heads[strip_idx];
      if (nil == head) continue;
      LockMapStripe* stripe = &lock_map->lock_map_stripes_[strip_idx];
      stripe->stripe_mutex->Lock().PermitUncheckedError();
      for (uint32_t idx = head; nil != idx; idx = keys_link[idx]) {
        const fstring key = keyinfos.key(idx);
        UnLockKey(txn, key, stripe, lock_map, env);
      }
      stripe->stripe_mutex->UnLock();
      stripe->stripe_cv->NotifyAll();
    }
  }
}

PointLockManager::PointLockStatus PointLockManager::GetPointLockStatus() {
  PointLockStatus data;
  // Lock order here is important. The correct order is lock_map_mutex_, then
  // for every column family ID in ascending order lock every stripe in
  // ascending order.
  InstrumentedMutexLock l(&lock_map_mutex_);

  // cf num is generally small, very large cf num is ill
  auto cf_ids = (uint32_t*)alloca(sizeof(uint32_t) * lock_maps_.size());
  size_t cf_num = 0;
  for (const auto& map : lock_maps_) {
    cf_ids[cf_num++] = map.first;
  }
  ROCKSDB_ASSERT_EQ(cf_num, lock_maps_.size());
  std::sort(cf_ids, cf_ids + cf_num);

  for (size_t k = 0; k < cf_num; ++k) {
    auto i = cf_ids[k];
    const auto& stripes = lock_maps_[i]->lock_map_stripes_;
    // Iterate and lock all stripes in ascending order.
    for (const auto& j : stripes) {
      j.stripe_mutex->Lock().PermitUncheckedError();
      for (const auto& it : j.keys) {
        struct KeyLockInfo info;
        info.exclusive = it.second.exclusive();
        info.key.assign(it.first.data(), it.first.size());
        for (const auto& id : it.second.txn_ids) {
          info.ids.push_back(id);
        }
        data.insert({i, info});
      }
    }
  }

  // Unlock everything. Unlocking order is not important.
  for (size_t k = 0; k < cf_num; ++k) {
    auto i = cf_ids[k];
    const auto& stripes = lock_maps_[i]->lock_map_stripes_;
    for (const auto& j : stripes) {
      j.stripe_mutex->UnLock();
    }
  }

  return data;
}

std::vector<DeadlockPath> PointLockManager::GetDeadlockInfoBuffer() {
  return dlock_buffer_.PrepareBuffer();
}

void PointLockManager::Resize(uint32_t target_size) {
  dlock_buffer_.Resize(target_size);
}

PointLockManager::RangeLockStatus PointLockManager::GetRangeLockStatus() {
  return {};
}

Status PointLockManager::TryLock(PessimisticTransaction* /* txn */,
                                 ColumnFamilyId /* cf_id */,
                                 const Endpoint& /* start */,
                                 const Endpoint& /* end */, Env* /* env */,
                                 bool /* exclusive */) {
  return Status::NotSupported(
      "PointLockManager does not support range locking");
}

void PointLockManager::UnLock(PessimisticTransaction* /* txn */,
                              ColumnFamilyId /* cf_id */,
                              const Endpoint& /* start */,
                              const Endpoint& /* end */, Env* /* env */) {
  // no-op
}

}  // namespace ROCKSDB_NAMESPACE
