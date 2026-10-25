//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/logs_with_prep_tracker.h"

#include <algorithm>

#include "port/likely.h"

namespace ROCKSDB_NAMESPACE {
void LogsWithPrepTracker::MarkLogAsHavingPrepSectionFlushed(uint64_t log) {
  assert(log != 0);
  std::lock_guard<std::mutex> lock(prepared_section_completed_mutex_);
  auto it = prepared_section_completed_.find(log);
  if (UNLIKELY(it == prepared_section_completed_.end())) {
    prepared_section_completed_[log] = 1;
  } else {
    it->second += 1;
  }
  auto prev = outstanding_.fetch_sub(1, std::memory_order_relaxed);
  (void)prev;
  assert(prev > 0);
}

void LogsWithPrepTracker::MarkLogAsContainingPrepSection(uint64_t log,
                                                         uint64_t offset) {
  assert(log != 0);
  std::lock_guard<std::mutex> lock(logs_with_prep_mutex_);

  auto rit = logs_with_prep_.rbegin();
  bool updated = false;
  // Most probably the last log is the one that is being marked for
  // having a prepare section; so search from the end.
  for (; rit != logs_with_prep_.rend() && rit->log >= log; ++rit) {
    if (rit->log == log) {
      rit->cnt++;
      rit->offset = std::min(rit->offset, offset);
      updated = true;
      break;
    }
  }
  if (!updated) {
    // We are either at the start, or at a position with rit->log < log
    logs_with_prep_.insert(rit.base(), {log, 1, offset});
  }
  outstanding_.fetch_add(1, std::memory_order_relaxed);
}

uint64_t LogsWithPrepTracker::FindMinLogContainingOutstandingPrep() {
  return FindMinPrep(nullptr, nullptr);
}

bool LogsWithPrepTracker::FindMinPendingPos(
    std::pair<uint64_t, uint64_t>* pos,
    const autovector<std::pair<uint64_t, uint64_t>>* resolves) {
  // The lock-free read is safe only because a registration can never race a
  // publication: both happen inside one DoWriteWAL(), and DoWriteWAL() calls
  // are serialized by the write thread (single queue) or by log_write_mutex_
  // (ConcurrentWriteToWAL). A caller outside that serialization must take
  // logs_with_prep_mutex_.
  if (outstanding_.load(std::memory_order_acquire) == 0) {
    return false;
  }
  uint64_t offset = 0;
  const uint64_t log = FindMinPrep(&offset, resolves);
  if (log == 0) {
    return false;
  }
  if (pos != nullptr) {
    *pos = std::pair<uint64_t, uint64_t>(log, offset);
  }
  return true;
}

uint64_t LogsWithPrepTracker::FindMinPrep(
    uint64_t* offset,
    const autovector<std::pair<uint64_t, uint64_t>>* resolves) {
  std::lock_guard<std::mutex> lock(logs_with_prep_mutex_);
  auto it = logs_with_prep_.begin();
  // start with the smallest log
  for (; it != logs_with_prep_.end();) {
    auto min_log = it->log;
    bool resolved_by_group = false;
    {
      std::lock_guard<std::mutex> lock2(prepared_section_completed_mutex_);
      auto completed_it = prepared_section_completed_.find(min_log);
      if (completed_it == prepared_section_completed_.end() ||
          completed_it->second < it->cnt) {
        if (resolves != nullptr) {
          // Skip the log only if this write group resolves all of its
          // outstanding prepares. Do not erase it: it still pins the WAL.
          uint64_t completed = completed_it == prepared_section_completed_.end()
                                   ? 0
                                   : completed_it->second;
          const std::pair<uint64_t, uint64_t> all_of_log(min_log,
                                                         it->cnt - completed);
          resolved_by_group = resolves->find_p(all_of_log) != nullptr;
        }
        if (!resolved_by_group) {
          if (offset != nullptr) {
            *offset = it->offset;
          }
          return min_log;
        }
      } else {
        assert(completed_it != prepared_section_completed_.end() &&
               completed_it->second == it->cnt);
        prepared_section_completed_.erase(completed_it);
      }
    }
    if (resolved_by_group) {
      ++it;
      continue;
    }
    // erase from beginning in vector is not efficient but this function is not
    // on the fast path.
    it = logs_with_prep_.erase(it);
  }
  // no such log found
  return 0;
}

uint64_t LogsWithPrepTracker::TEST_OutstandingPrep(uint64_t log) {
  std::lock_guard<std::mutex> lock(logs_with_prep_mutex_);
  std::lock_guard<std::mutex> lock2(prepared_section_completed_mutex_);
  auto it = logs_with_prep_.begin();
  for (; it != logs_with_prep_.end() && it->log != log; ++it) {
  }
  if (it == logs_with_prep_.end()) {
    return 0;
  }
  auto completed_it = prepared_section_completed_.find(log);
  uint64_t completed = completed_it == prepared_section_completed_.end()
                           ? 0
                           : completed_it->second;
  assert(completed <= it->cnt);
  return it->cnt - completed;
}
}  // namespace ROCKSDB_NAMESPACE
