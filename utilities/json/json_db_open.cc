//
// Created by leipeng on 2020/7/28.
//
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "options/db_options.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/wal_filter.h"
#include "json.h"
#include "json_plugin_factory.h"

namespace ROCKSDB_NAMESPACE {

using std::shared_ptr;
using std::unordered_map;
using std::vector;
using std::string;

/*
{
  "DB::Open": {
    "cf_options" : {
      "default": {},
      "user-cf1": {}
    },
    "db_options" : {

    }
  },
  "db": "DB::Open"
}

*/

Status JsonOptionsRepo::OpenDB(DB** dbp) {

}


} // namespace ROCKSDB_NAMESPACE
