//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/statistics.h"
#include "rocksdb/system_clock.h"
#include <time.h> // for clock_gettime

namespace ROCKSDB_NAMESPACE {
// Auto-scoped.
// Records the measure time into the corresponding histogram if statistics
// is not nullptr. It is also saved into *elapsed if the pointer is not nullptr
// and overwrite is true, it will be added to *elapsed if overwrite is false.
class StopWatch {
 public:
  inline
  StopWatch(SystemClock* clock, Statistics* statistics,
            const uint32_t hist_type, uint64_t* elapsed = nullptr,
            bool overwrite = true, bool delay_enabled = false)
      :
#ifndef CLOCK_MONOTONIC_RAW
        clock_(clock),
#endif
        statistics_(statistics),
        hist_type_(hist_type),
        overwrite_(overwrite),
        stats_enabled_(statistics &&
                       statistics->get_stats_level() >=
                           StatsLevel::kExceptTimers &&
                       statistics->HistEnabledForType(hist_type)),
        delay_enabled_(delay_enabled),
        elapsed_(elapsed),
        total_delay_(0),
        delay_start_time_(0),
        start_time_((stats_enabled_ || elapsed != nullptr) ? now_nanos()
                                                           : 0) {}

  ~StopWatch() {
    if (elapsed_) {
      if (overwrite_) {
        *elapsed_ = (now_nanos() - start_time_) / 1000;
      } else {
        *elapsed_ += (now_nanos() - start_time_) / 1000;
      }
    }
    if (elapsed_ && delay_enabled_) {
      *elapsed_ -= total_delay_ / 1000;
    }
    if (stats_enabled_) {
      statistics_->reportTimeToHistogram(
          hist_type_, (elapsed_ != nullptr)
                          ? *elapsed_
                          : (now_nanos() - start_time_) / 1000);
    }
  }

  void DelayStart() {
    // if delay_start_time_ is not 0, it means we are already tracking delay,
    // so delay_start_time_ should not be overwritten
    if (elapsed_ && delay_enabled_ && delay_start_time_ == 0) {
      delay_start_time_ = now_nanos();
    }
  }

  void DelayStop() {
    if (elapsed_ && delay_enabled_ && delay_start_time_ != 0) {
      total_delay_ += now_nanos() - delay_start_time_;
    }
    // reset to 0 means currently no delay is being tracked, so two consecutive
    // calls to DelayStop will not increase total_delay_
    delay_start_time_ = 0;
  }

  uint64_t GetDelay() const { return delay_enabled_ ? total_delay_/1000 : 0; }

  uint64_t start_time() const { return start_time_ / 1000; }

 private:
  inline static uint64_t now_nanos() {
#ifdef CLOCK_MONOTONIC_RAW
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return ts.tv_sec * 1000000000 + ts.tv_nsec;
#else
    return clock_->NowNanos();
#endif
  }
#ifndef CLOCK_MONOTONIC_RAW
  SystemClock* clock_;
#endif
  Statistics* statistics_;
  const uint32_t hist_type_;
  bool overwrite_;
  bool stats_enabled_;
  bool delay_enabled_;
  uint64_t* elapsed_;
  uint64_t total_delay_;
  uint64_t delay_start_time_;
  const uint64_t start_time_;
};

// a nano second precision stopwatch
class StopWatchNano {
 public:
  inline
  explicit StopWatchNano(SystemClock* clock, bool auto_start = false)
      :
#ifndef CLOCK_MONOTONIC_RAW
      clock_(clock),
#endif
      start_(0) {
    if (auto_start) {
      Start();
    }
  }

  void Start() { start_ = now_nanos(); }

  uint64_t ElapsedNanos(bool reset = false) {
    auto now = now_nanos();
    auto elapsed = now - start_;
    if (reset) {
      start_ = now;
    }
    return elapsed;
  }

  uint64_t ElapsedNanosSafe(bool reset = false) {
#ifdef CLOCK_MONOTONIC_RAW
    return ElapsedNanos(reset);
#else
    return (clock_ != nullptr) ? ElapsedNanos(reset) : 0U;
#endif
  }

 private:
  inline static uint64_t now_nanos() {
#ifdef CLOCK_MONOTONIC_RAW
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return ts.tv_sec * 1000000000 + ts.tv_nsec;
#else
    return clock_->NowNanos();
#endif
  }
#ifndef CLOCK_MONOTONIC_RAW
  SystemClock* clock_;
#endif
  uint64_t start_;
};

}  // namespace ROCKSDB_NAMESPACE
