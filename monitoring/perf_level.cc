//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <assert.h>

#include "monitoring/perf_level_imp.h"

namespace ROCKSDB_NAMESPACE {

#if !defined(ROCKSDB_NON_TLS_PERF_LEVEL)
  ROCKSDB_RAW_TLS PerfLevel perf_level ROCKSDB_STATIC_TLS = kEnableCount;
#else
PerfLevel perf_level = kEnableCount;
#endif

void SetPerfLevel(PerfLevel level) {
  assert(level > kUninitialized);
  assert(level < kOutOfBounds);
  perf_level = level;
}

PerfLevel GetPerfLevel() { return perf_level; }

}  // namespace ROCKSDB_NAMESPACE
