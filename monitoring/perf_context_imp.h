//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_step_timer.h"
#include "port/lang.h"
#include "rocksdb/perf_context.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
#if defined(NPERF_CONTEXT)
extern PerfContext perf_context;
#else
#if defined(OS_SOLARIS)
extern thread_local PerfContext perf_context_;
#define perf_context (*get_perf_context())
#else
  extern PerfContext* init_perf_context() noexcept;
  extern ROCKSDB_STATIC_TLS ROCKSDB_RAW_TLS PerfContext* p_perf_context;
  #define perf_context (*(p_perf_context?:init_perf_context()))
#endif
#endif

#if defined(NPERF_CONTEXT)

#define PERF_TIMER_STOP(metric)
#define PERF_TIMER_START(metric)
#define PERF_TIMER_GUARD(metric)
#define PERF_TIMER_GUARD_WITH_CLOCK(metric, clock)
#define PERF_CPU_TIMER_GUARD(metric, clock)
#define PERF_TIMER_FULL_STATS(metric, ticker, histogram, stats)
#define PERF_TIMER_WITH_HISTOGRAM(metric, histogram, stats)
#define PERF_TIMER_WITH_TICKER(metric, ticker, stats, clock)
#define PERF_TIMER_MEASURE(metric)
#define PERF_COUNTER_ADD(metric, value)
#define PERF_COUNTER_BY_LEVEL_ADD(metric, value, level)

#else

// Stop the timer and update the metric
#define PERF_TIMER_STOP(metric) perf_step_timer_##metric.Stop();

#define PERF_TIMER_START(metric) perf_step_timer_##metric.Start();

#define PerfStepTimerDecl(metric, clock, use_cpu_time, enable_level, ...) \
  PerfStepTimer perf_step_timer_##metric( \
   &perf_context.metric, \
   clock, use_cpu_time, enable_level, ##__VA_ARGS__)

#define PERF_TIMER_FULL_STATS(metric, ticker, histogram, stats) \
  PerfStepTimerDecl(metric, nullptr, \
    false, kEnableTimeExceptForMutex, stats, ticker, histogram); \
  perf_step_timer_##metric.Start();

#define PERF_TIMER_WITH_HISTOGRAM(metric, histogram, stats) \
  PERF_TIMER_FULL_STATS(metric, UINT32_MAX, histogram, stats)

#define PERF_TIMER_WITH_TICKER(metric, ticker, stats, clock) \
  PERF_TIMER_FULL_STATS(metric, ticker, UINT16_MAX, stats)

// Declare and set start time of the timer
#define PERF_TIMER_GUARD(metric)                                  \
  PerfStepTimerDecl(metric, nullptr, false, PerfLevel::kEnableTimeExceptForMutex); \
  perf_step_timer_##metric.Start();

// Declare and set start time of the timer
#define PERF_TIMER_GUARD_WITH_CLOCK(metric, clock)                       \
  PerfStepTimerDecl(metric, clock, false, PerfLevel::kEnableTimeExceptForMutex); \
  perf_step_timer_##metric.Start();

// Declare and set start time of the timer
#define PERF_CPU_TIMER_GUARD(metric, clock)            \
  PerfStepTimerDecl(metric, clock, true, \
      PerfLevel::kEnableTimeAndCPUTimeExceptForMutex); \
  perf_step_timer_##metric.Start();

#define PERF_TIMER_MUTEX_WAIT_GUARD(metric, stats)                 \
  PerfStepTimerDecl(metric, nullptr, \
      false, PerfLevel::kEnableTime, stats, DB_MUTEX_WAIT_NANOS,         \
      HISTOGRAM_MUTEX_WAIT_NANOS);                                       \
  perf_step_timer_##metric.Start();

#define PERF_TIMER_COND_WAIT_GUARD(metric, stats)                         \
  PerfStepTimerDecl(metric, nullptr, \
      false, PerfLevel::kEnableTime, stats, DB_COND_WAIT_NANOS,           \
      HISTOGRAM_COND_WAIT_NANOS);                                         \
  perf_step_timer_##metric.Start();

// Update metric with time elapsed since last START. start time is reset
// to current timestamp.
#define PERF_TIMER_MEASURE(metric) perf_step_timer_##metric.Measure();

// Increase metric value
#define PERF_COUNTER_ADD(metric, value)        \
  if (perf_level >= PerfLevel::kEnableCount) { \
    perf_context.metric += value;              \
  }                                            \
  static_assert(true, "semicolon required")

// Increase metric value
#define PERF_COUNTER_BY_LEVEL_ADD(metric, value, level)               \
  if (perf_level >= PerfLevel::kEnableCount &&                        \
      perf_context.per_level_perf_context_enabled) { \
    perf_context.level_to_perf_context[level].metric += value;        \
  }

#endif

}  // namespace ROCKSDB_NAMESPACE
