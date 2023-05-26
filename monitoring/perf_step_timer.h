//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_level_imp.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/system_clock.h"
#include <time.h> // for clock_gettime

namespace ROCKSDB_NAMESPACE {

class PerfStepTimer {
 public:
  explicit PerfStepTimer(
      uint64_t* metric,
      SystemClock* clock __attribute__((__unused__)) = nullptr,
      bool use_cpu_time __attribute__((__unused__)) = false,
      PerfLevel enable_level = PerfLevel::kEnableTimeExceptForMutex,
      Statistics* statistics = nullptr, uint32_t ticker_type = UINT32_MAX,
      uint16_t histogram_type = UINT16_MAX)
      : perf_counter_enabled_(perf_level >= enable_level || statistics != nullptr),
#if !defined(CLOCK_MONOTONIC) || defined(ROCKSDB_UNIT_TEST)
        use_cpu_time_(use_cpu_time),
#endif
        histogram_type_(histogram_type),
        ticker_type_(ticker_type),
#if !defined(CLOCK_MONOTONIC) || defined(ROCKSDB_UNIT_TEST)
        clock_((perf_counter_enabled_ || statistics != nullptr)
                   ? (clock ? clock : SystemClock::Default().get())
                   : nullptr),
#endif
        start_(0),
        metric_(metric),
        statistics_(statistics) {}

  ~PerfStepTimer() { Stop(); }

  void Start() {
    if (perf_counter_enabled_) {
      start_ = time_now();
    }
  }

  void Measure() {
    if (start_) {
      uint64_t now = time_now();
      if (metric_) {
        *metric_ += now - start_;
      }
      start_ = now;
    }
  }

  void Stop() {
    if (start_) {
      uint64_t duration = time_now() - start_;
      if (perf_counter_enabled_) {
        *metric_ += duration;
      }

      if (auto stats = statistics_) {
        if (UINT32_MAX != ticker_type_)
          stats->recordTick(ticker_type_, duration);
        if (UINT16_MAX != histogram_type_)
          stats->recordInHistogram(histogram_type_, duration);
      }
      start_ = 0;
    }
  }

 private:
  uint64_t time_now() {
   #if defined(CLOCK_MONOTONIC) && !defined(ROCKSDB_UNIT_TEST)
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000 + ts.tv_nsec;
   #else
    if (!use_cpu_time_) {
      return clock_->NowNanos();
    } else {
      return clock_->CPUNanos();
    }
   #endif
  }

  const bool perf_counter_enabled_;
#if !defined(CLOCK_MONOTONIC) || defined(ROCKSDB_UNIT_TEST)
  const bool use_cpu_time_;
#endif
  uint16_t histogram_type_;
  uint32_t ticker_type_;
#if !defined(CLOCK_MONOTONIC) || defined(ROCKSDB_UNIT_TEST)
  SystemClock* const clock_;
#endif
  uint64_t start_;
  uint64_t* metric_;
  Statistics* statistics_;
};

}  // namespace ROCKSDB_NAMESPACE
