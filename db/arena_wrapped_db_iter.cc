//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/arena_wrapped_db_iter.h"
#include "db/snapshot_impl.h"
#include "memory/arena.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "util/user_comparator_wrapper.h"

namespace ROCKSDB_NAMESPACE {

static constexpr size_t KEEP_SNAPSHOT = 16;

inline static
SequenceNumber GetSeqNum(const DBImpl* db, const Snapshot* s, const DBIter* i) {
  if (size_t(s) == KEEP_SNAPSHOT)
    return i->get_sequence();
  else if (s)
    //return static_cast_with_check<const SnapshotImpl>(s)->number_;
    return s->GetSequenceNumber();
  else
    return db->GetLatestSequenceNumber();
}

Status ArenaWrappedDBIter::GetProperty(std::string prop_name,
                                       std::string* prop) {
  if (prop_name == "rocksdb.iterator.super-version-number") {
    // First try to pass the value returned from inner iterator.
    if (!db_iter_->GetProperty(prop_name, prop).ok()) {
      *prop = std::to_string(sv_number_);
    }
    return Status::OK();
  }
  return db_iter_->GetProperty(prop_name, prop);
}

void ArenaWrappedDBIter::Init(
    Env* env, const ReadOptions& read_options, const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, const Version* version,
    const SequenceNumber& sequence, uint64_t max_sequential_skip_in_iteration,
    uint64_t version_number, ReadCallback* read_callback, DBImpl* db_impl,
    ColumnFamilyData* cfd, bool expose_blob_index, bool allow_refresh) {
  auto mem = arena_.AllocateAligned(sizeof(DBIter));
  db_iter_ =
      new (mem) DBIter(env, read_options, ioptions, mutable_cf_options,
                       ioptions.user_comparator, /* iter */ nullptr, version,
                       sequence, true, max_sequential_skip_in_iteration,
                       read_callback, db_impl, cfd, expose_blob_index);
  sv_number_ = version_number;
  read_options_ = read_options;
  allow_refresh_ = allow_refresh;
}

Status ArenaWrappedDBIter::Refresh() {
  return Refresh(nullptr, false); // do not keep iter pos
}

// when keep_iter_pos is true, user code should ensure ReadOptions's
// lower_bound and upper_bound are not changed
Status ArenaWrappedDBIter::Refresh(const Snapshot* snap, bool keep_iter_pos) {
  if (cfd_ == nullptr || db_impl_ == nullptr || !allow_refresh_) {
    return Status::NotSupported("Creating renew iterator is not allowed.");
  }
  assert(db_iter_ != nullptr);
  // TODO(yiwu): For last_seq_same_as_publish_seq_==false, this is not the
  // correct behavior. Will be corrected automatically when we take a snapshot
  // here for the case of WritePreparedTxnDB.
  uint64_t cur_sv_number = cfd_->GetSuperVersionNumber();
  TEST_SYNC_POINT("ArenaWrappedDBIter::Refresh:1");
  TEST_SYNC_POINT("ArenaWrappedDBIter::Refresh:2");
  while (true) {
    if (sv_number_ != cur_sv_number) {
      std::string curr_key;
      bool is_valid = this->Valid();
      SequenceNumber old_iter_seq = db_iter_->get_sequence();
      SequenceNumber latest_seq = GetSeqNum(db_impl_, snap, db_iter_);
      if (is_valid && keep_iter_pos) {
        curr_key = this->key().ToString();
      }
      Snapshot* pin_snap = nullptr;
      if (size_t(snap) == KEEP_SNAPSHOT) {
        // pin the snapshot latest_seq to avoid race condition caused by
        // the the snapshot latest_seq being garbage collected by a
        // compaction, which may cause many errors, for example an external
        // behavior is Seek on belowing new iterator failed(with same
        // read_opt.lower_bound/upper_bound...)
        pin_snap = db_impl_->GetSnapshotImpl(latest_seq, false);
      }
      Env* env = db_iter_->env();
      db_iter_->~DBIter();
      arena_.~Arena();
      new (&arena_) Arena();

      SuperVersion* sv = cfd_->GetReferencedSuperVersion(db_impl_);
      if (read_callback_) {
        read_callback_->Refresh(latest_seq);
      }
      Init(env, read_options_, *(cfd_->ioptions()), sv->mutable_cf_options,
           sv->current, latest_seq,
           sv->mutable_cf_options.max_sequential_skip_in_iterations,
           cur_sv_number, read_callback_, db_impl_, cfd_, expose_blob_index_,
           allow_refresh_);

      InternalIterator* internal_iter = db_impl_->NewInternalIterator(
          read_options_, cfd_, sv, &arena_, db_iter_->GetRangeDelAggregator(),
          latest_seq, /* allow_unprepared_value */ true);
      SetIterUnderDBIter(internal_iter);
      if (is_valid && keep_iter_pos) {
        this->Seek(curr_key);
        ROCKSDB_VERIFY_F(this->Valid(),
          "old_iter_seq = %lld, latest_seq = %lld, snap = %p, pin_snap = %p",
          (long long)old_iter_seq, (long long)latest_seq, snap, pin_snap);
        ROCKSDB_VERIFY_F(key() == curr_key, "%s %s",
          key().ToString(true).c_str(), Slice(curr_key).ToString(true).c_str());
      }
      if (pin_snap) {
        db_impl_->ReleaseSnapshot(pin_snap);
      }
      break;
    } else {
      SequenceNumber latest_seq = GetSeqNum(db_impl_, snap, db_iter_);
      if (latest_seq == db_iter_->get_sequence()) {
        break;
      }
      // Refresh range-tombstones in MemTable
      if (!read_options_.ignore_range_deletions) {
        SuperVersion* sv = cfd_->GetThreadLocalSuperVersion(db_impl_);
        ReadRangeDelAggregator* range_del_agg =
            db_iter_->GetRangeDelAggregator();
        std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter;
        range_del_iter.reset(
            sv->mem->NewRangeTombstoneIterator(read_options_, latest_seq));
        range_del_agg->AddTombstones(std::move(range_del_iter));
        cfd_->ReturnThreadLocalSuperVersion(sv);
      }
      // Refresh latest sequence number
      db_iter_->set_sequence(latest_seq);
      // db_iter_->set_valid(false); // comment out for ToplingDB
      // Check again if the latest super version number is changed
      uint64_t latest_sv_number = cfd_->GetSuperVersionNumber();
      if (latest_sv_number != cur_sv_number) {
        // If the super version number is changed after refreshing,
        // fallback to Re-Init the InternalIterator
        cur_sv_number = latest_sv_number;
        continue;
      }
      break;
    }
  }
  if (size_t(snap) > KEEP_SNAPSHOT) {
    this->read_options_.snapshot = snap;
  }
  return Status::OK();
}

ArenaWrappedDBIter* NewArenaWrappedDbIterator(
    const ReadOptions& read_options, const SuperVersion* sv,
    SequenceNumber sequence, ReadCallback* read_callback, DBImpl* db_impl,
    bool expose_blob_index, bool allow_refresh) {
  auto version = sv->current;
  auto version_number = sv->version_number;
  auto env = version->env();
  auto cfd = sv->cfd;
  const auto& ioptions = *cfd->ioptions();
  const auto& mutable_cf_options = sv->mutable_cf_options;
  auto max_sequential_skip_in_iterations =
    mutable_cf_options.max_sequential_skip_in_iterations;
  ArenaWrappedDBIter* iter = new ArenaWrappedDBIter();
  iter->Init(env, read_options, ioptions, mutable_cf_options, version, sequence,
             max_sequential_skip_in_iterations, version_number, read_callback,
             db_impl, cfd, expose_blob_index, allow_refresh);
  if (db_impl != nullptr && cfd != nullptr && allow_refresh) {
    iter->StoreRefreshInfo(db_impl, cfd, read_callback, expose_blob_index);
  }

  return iter;
}

}  // namespace ROCKSDB_NAMESPACE
