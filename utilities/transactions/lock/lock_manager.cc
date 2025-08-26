// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).


#include "utilities/transactions/lock/lock_manager.h"

#include "utilities/transactions/lock/point/point_lock_manager.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

std::shared_ptr<LockManager> NewLockManager(PessimisticTransactionDB* db,
                                            const TransactionDBOptions& opt) {
  assert(db);
  if (opt.lock_mgr_handle) {
    // A custom lock manager was provided in options
    auto mgr = opt.lock_mgr_handle->getLockManager();
    return std::shared_ptr<LockManager>(opt.lock_mgr_handle, mgr);
  } else {
    // Use a point lock manager by default
    return std::shared_ptr<LockManager>(new PointLockManager(db, opt));
  }
}

#if defined(ROCKSDB_UNIT_TEST)
Status LockManager::TryLock(PessimisticTransaction* txn, ColumnFamilyId cf_id,
                            const Slice& key, Env* env, bool exclusive) {
  size_t key_hash = NPHash64(key.data(), key.size());
  return TryLock(txn, cf_id, key, key_hash, env, exclusive);
}
#endif

}  // namespace ROCKSDB_NAMESPACE

