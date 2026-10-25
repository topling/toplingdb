//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

// This class is used to track the log files with outstanding prepare entries.
class LogsWithPrepTracker {
 public:
  // Called when a transaction prepared in `log` has been committed or aborted.
  void MarkLogAsHavingPrepSectionFlushed(uint64_t log);
  // Called when a transaction is prepared in `log`. `offset` is the position
  // of the prepare record inside `log`, in the same coordinate as
  // log::Reader::LastRecordOffset(). The default 0 means "position unknown"
  // and pins the log at its beginning, which is always safe.
  void MarkLogAsContainingPrepSection(uint64_t log, uint64_t offset = 0);
  // Return the earliest log file with outstanding prepare entries.
  uint64_t FindMinLogContainingOutstandingPrep();
  // Return true and set `*pos` to {log, offset}, the earliest log file with
  // outstanding prepare entries and the earliest prepare record position
  // inside that log. Logs whose outstanding prepares are all resolved by this
  // write group (`resolves`: log -> count) are skipped, but never erased: WAL
  // retention still needs them. Returns false without locking when no prepare
  // is pending.
  bool FindMinPendingPos(
      std::pair<uint64_t, uint64_t>* pos,
      const autovector<std::pair<uint64_t, uint64_t>>* resolves = nullptr);
  size_t TEST_PreparedSectionCompletedSize() {
    return prepared_section_completed_.size();
  }
  size_t TEST_LogsWithPrepSize() { return logs_with_prep_.size(); }
  uint64_t TEST_OutstandingPrep(uint64_t log);

 private:
  // Return the earliest log file with outstanding prepare entries. The
  // lazy cleanup of fully resolved logs happens here and only here.
  uint64_t FindMinPrep(
      uint64_t* offset,
      const autovector<std::pair<uint64_t, uint64_t>>* resolves);

  // REQUIRES: logs_with_prep_mutex_ held
  //
  // sorted list of log numbers still containing prepared data.
  // this is used by FindObsoleteFiles to determine which
  // flushed logs we must keep around because they still
  // contain prepared data which has not been committed or rolled back
  struct LogCnt {
    uint64_t log;  // the log number
    uint64_t cnt;  // number of prepared sections in the log
    // Record position of the earliest prepare record in this log, intra-log
    // only (the log number is `log` above). Same coordinate as
    // log::Reader::LastRecordOffset().
    uint64_t offset;
  };
  std::vector<LogCnt> logs_with_prep_;
  std::mutex logs_with_prep_mutex_;

  // Number of registered prepare sections not yet marked as flushed. Exact,
  // maintained together with `logs_with_prep_`/`prepared_section_completed_`;
  // only used to skip the locks in FindMinPendingPos() when it is 0.
  std::atomic<uint64_t> outstanding_{0};

  // REQUIRES: prepared_section_completed_mutex_ held
  //
  // to be used in conjunction with logs_with_prep_.
  // once a transaction with data in log L is committed or rolled back
  // rather than updating logs_with_prep_ directly we keep track of that
  // in prepared_section_completed_ which maps LOG -> instance_count. This helps
  // avoiding contention between a commit thread and the prepare threads.
  //
  // when trying to determine the minimum log still active we first
  // consult logs_with_prep_. while that root value maps to
  // an equal value in prepared_section_completed_ we erase the log from
  // both logs_with_prep_ and prepared_section_completed_.
  std::unordered_map<uint64_t, uint64_t> prepared_section_completed_;
  std::mutex prepared_section_completed_mutex_;
};
}  // namespace ROCKSDB_NAMESPACE
