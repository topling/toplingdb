//
// Created by leipeng on 2020/7/1.
//

#include "utilities/table_properties_collectors/compact_on_deletion_collector.h"
//#include "utilities/ttl/db_ttl_impl.h"

#include <memory>
#include <cinttypes>

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
#include "rocksdb/table.h"
#include "rocksdb/wal_filter.h"
#include "util/compression.h"
#include "util/rate_limiter.h"
#include "json.h"
#include "factoryable.h"
#include "json_options_repo.h"

namespace ROCKSDB_NAMESPACE {

using std::shared_ptr;
using std:: unordered_map;
using std:: vector;
using std:: string;

/*
class Cache;
class CompactionFilterFactory;
class Comparator;
class ConcurrentTaskLimiter;
class Env;
class EventListener;
class FileChecksumGenFactory;
class FilterPolicy;
class FlushBlockPolicyFactory;
class Logger;
class MemTableRepFactory;
class MergeOperator;
class PersistentCache;
class RateLimiter;
class SliceTransform;
class SstFileManager;
class Statistics;
class TableFactory;
class TablePropertiesCollectorFactory;
*/

static DbPath DbPathFromJson(const json& js) {
  DbPath x;
  if (js.is_string()) {
    x.path = js.get<std::string>();
  } else {
    x.path = js.at("path").get<std::string>();
    x.target_size = js.at("target_size").get<uint64_t>();
  }
  return x;
}

static void Json_DbPathVec(const json& js,
                           std::vector<DbPath>& db_paths) {
  db_paths.clear();
  if (js.is_string() || js.is_object()) {
    // only one path
    db_paths.push_back(DbPathFromJson(js));
  }
  else if (js.is_array()) {
    for (auto& one : js.items()) {
      db_paths.push_back(DbPathFromJson(one.value()));
    }
  }
}

static Status Json_EventListenerVec(const json& js, const JsonOptionsRepo& repo,
    std::vector<shared_ptr<EventListener>>& listeners) {
  listeners.clear();
  if (js.is_string()) {
    shared_ptr<EventListener> el;
    ROCKSDB_JSON_OPT_FACT_IMPL(js, el);
    listeners.emplace_back(el);
  }
  else if (js.is_array()) {
    for (auto& one : js.items()) {
      shared_ptr<EventListener> el;
      ROCKSDB_JSON_OPT_FACT_IMPL(one.value(), el);
      listeners.emplace_back(el);
    }
  }
  return Status::OK();
}

struct DBOptions_Json : DBOptions {
  Status UpdateFromJson(const json& js, const JsonOptionsRepo& repo) try {
    ROCKSDB_JSON_OPT_PROP(js, paranoid_checks);
    ROCKSDB_JSON_OPT_FACT(js, env);
    ROCKSDB_JSON_OPT_FACT(js, rate_limiter);
    ROCKSDB_JSON_OPT_FACT(js, sst_file_manager);
    ROCKSDB_JSON_OPT_FACT(js, info_log);
    ROCKSDB_JSON_OPT_ENUM(js, info_log_level);
    ROCKSDB_JSON_OPT_PROP(js, max_open_files);
    ROCKSDB_JSON_OPT_PROP(js, max_file_opening_threads);
    ROCKSDB_JSON_OPT_PROP(js, max_total_wal_size);
    ROCKSDB_JSON_OPT_FACT(js, statistics);
    ROCKSDB_JSON_OPT_PROP(js, use_fsync);
    {
      auto iter = js.find("db_paths");
      if (js.end() != iter)
        Json_DbPathVec(js, db_paths);
    }
    //ROCKSDB_JSON_OPT_PROP(js, db_paths);
    ROCKSDB_JSON_OPT_PROP(js, db_log_dir);
    ROCKSDB_JSON_OPT_PROP(js, wal_dir);
    ROCKSDB_JSON_OPT_PROP(js, delete_obsolete_files_period_micros);
    ROCKSDB_JSON_OPT_PROP(js, max_background_jobs);
    ROCKSDB_JSON_OPT_PROP(js, base_background_compactions);
    ROCKSDB_JSON_OPT_PROP(js, max_background_compactions);
    ROCKSDB_JSON_OPT_PROP(js, max_subcompactions);
    ROCKSDB_JSON_OPT_PROP(js, max_background_flushes);
    ROCKSDB_JSON_OPT_PROP(js, max_log_file_size);
    ROCKSDB_JSON_OPT_PROP(js, log_file_time_to_roll);
    ROCKSDB_JSON_OPT_PROP(js, keep_log_file_num);
    ROCKSDB_JSON_OPT_PROP(js, recycle_log_file_num);
    ROCKSDB_JSON_OPT_PROP(js, max_manifest_file_size);
    ROCKSDB_JSON_OPT_PROP(js, table_cache_numshardbits);
    ROCKSDB_JSON_OPT_PROP(js, WAL_ttl_seconds);
    ROCKSDB_JSON_OPT_PROP(js, WAL_size_limit_MB);
    ROCKSDB_JSON_OPT_PROP(js, manifest_preallocation_size);
    ROCKSDB_JSON_OPT_PROP(js, allow_mmap_reads);
    ROCKSDB_JSON_OPT_PROP(js, allow_mmap_writes);
    ROCKSDB_JSON_OPT_PROP(js, use_direct_reads);
    ROCKSDB_JSON_OPT_PROP(js, use_direct_io_for_flush_and_compaction);
    ROCKSDB_JSON_OPT_PROP(js, allow_fallocate);
    ROCKSDB_JSON_OPT_PROP(js, is_fd_close_on_exec);
    ROCKSDB_JSON_OPT_PROP(js, skip_log_error_on_recovery);
    ROCKSDB_JSON_OPT_PROP(js, stats_dump_period_sec);
    ROCKSDB_JSON_OPT_PROP(js, stats_persist_period_sec);
    ROCKSDB_JSON_OPT_PROP(js, persist_stats_to_disk);
    ROCKSDB_JSON_OPT_PROP(js, stats_history_buffer_size);
    ROCKSDB_JSON_OPT_PROP(js, advise_random_on_open);
    ROCKSDB_JSON_OPT_PROP(js, db_write_buffer_size);
    {
      auto iter = js.find("write_buffer_manager");
      if (js.end() != iter) {
        auto& wbm = iter.value();
        size_t buffer_size = db_write_buffer_size;
        shared_ptr<Cache> cache;
        ROCKSDB_JSON_OPT_FACT(wbm, cache);
        ROCKSDB_JSON_OPT_PROP(wbm, buffer_size);
        write_buffer_manager = std::make_shared<WriteBufferManager>(
            buffer_size, cache);
      }
    }
    ROCKSDB_JSON_OPT_ENUM(js, access_hint_on_compaction_start);
    ROCKSDB_JSON_OPT_PROP(js, new_table_reader_for_compaction_inputs);
    ROCKSDB_JSON_OPT_PROP(js, compaction_readahead_size);
    ROCKSDB_JSON_OPT_PROP(js, random_access_max_buffer_size);
    ROCKSDB_JSON_OPT_PROP(js, writable_file_max_buffer_size);
    ROCKSDB_JSON_OPT_PROP(js, use_adaptive_mutex);
    ROCKSDB_JSON_OPT_PROP(js, bytes_per_sync);
    ROCKSDB_JSON_OPT_PROP(js, wal_bytes_per_sync);
    ROCKSDB_JSON_OPT_PROP(js, strict_bytes_per_sync);
    {
      auto iter = js.find("listeners");
      if (js.end() != iter)
        Json_EventListenerVec(js, repo, listeners);
    }
    ROCKSDB_JSON_OPT_PROP(js, enable_thread_tracking);
    ROCKSDB_JSON_OPT_PROP(js, delayed_write_rate);
    ROCKSDB_JSON_OPT_PROP(js, enable_pipelined_write);
    ROCKSDB_JSON_OPT_PROP(js, unordered_write);
    ROCKSDB_JSON_OPT_PROP(js, allow_concurrent_memtable_write);
    ROCKSDB_JSON_OPT_PROP(js, enable_write_thread_adaptive_yield);
    ROCKSDB_JSON_OPT_PROP(js, max_write_batch_group_size_bytes);
    ROCKSDB_JSON_OPT_PROP(js, write_thread_max_yield_usec);
    ROCKSDB_JSON_OPT_PROP(js, write_thread_slow_yield_usec);
    ROCKSDB_JSON_OPT_PROP(js, skip_stats_update_on_db_open);
    ROCKSDB_JSON_OPT_PROP(js, skip_checking_sst_file_sizes_on_db_open);
    ROCKSDB_JSON_OPT_ENUM(js, wal_recovery_mode);
    ROCKSDB_JSON_OPT_PROP(js, allow_2pc);
    ROCKSDB_JSON_OPT_FACT(js, row_cache);
    //ROCKSDB_JSON_OPT_FACT(js, wal_filter);
    ROCKSDB_JSON_OPT_PROP(js, fail_if_options_file_error);
    ROCKSDB_JSON_OPT_PROP(js, dump_malloc_stats);
    ROCKSDB_JSON_OPT_PROP(js, avoid_flush_during_recovery);
    ROCKSDB_JSON_OPT_PROP(js, avoid_flush_during_shutdown);
    ROCKSDB_JSON_OPT_PROP(js, allow_ingest_behind);
    ROCKSDB_JSON_OPT_PROP(js, preserve_deletes);
    ROCKSDB_JSON_OPT_PROP(js, two_write_queues);
    ROCKSDB_JSON_OPT_PROP(js, manual_wal_flush);
    ROCKSDB_JSON_OPT_PROP(js, atomic_flush);
    ROCKSDB_JSON_OPT_PROP(js, avoid_unnecessary_blocking_io);
    ROCKSDB_JSON_OPT_PROP(js, write_dbid_to_manifest);
    ROCKSDB_JSON_OPT_PROP(js, log_readahead_size);
    ROCKSDB_JSON_OPT_FACT(js, file_checksum_gen_factory);
    ROCKSDB_JSON_OPT_PROP(js, best_efforts_recovery);
    return Status::OK();
  }
  catch (const std::exception& ex) {
    return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
  }
};

template<class Vec>
bool Init_vec(const json& js, Vec& vec) {
  if (js.is_array()) {
    vec.clear();
    for (auto& iv : js.items()) {
      vec.push_back(iv.value().get<typename Vec::value_type>());
    }
    return true;
  } else {
    return false;
  }
}

//NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(CompactionOptionsFIFO,
//                                   max_table_files_size,
//                                   allow_compaction);

struct CompactionOptionsFIFO_Json : CompactionOptionsFIFO {
  explicit CompactionOptionsFIFO_Json(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, max_table_files_size);
    ROCKSDB_JSON_OPT_PROP(js, allow_compaction);
  }
};
CompactionOptionsFIFO_Json NestForBase(CompactionOptionsFIFO&);

struct CompactionOptionsUniversal_Json : CompactionOptionsUniversal {
  explicit CompactionOptionsUniversal_Json(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, size_ratio);
    ROCKSDB_JSON_OPT_PROP(js, min_merge_width);
    ROCKSDB_JSON_OPT_PROP(js, max_merge_width);
    ROCKSDB_JSON_OPT_PROP(js, max_size_amplification_percent);
    ROCKSDB_JSON_OPT_PROP(js, compression_size_percent);
    ROCKSDB_JSON_OPT_ENUM(js, stop_style);
    ROCKSDB_JSON_OPT_PROP(js, allow_trivial_move);
  }
};
CompactionOptionsUniversal_Json NestForBase(CompactionOptionsUniversal&);

struct CompressionOptions_Json : CompressionOptions {
  explicit CompressionOptions_Json(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, window_bits);
    ROCKSDB_JSON_OPT_PROP(js, level);
    ROCKSDB_JSON_OPT_PROP(js, strategy);
    ROCKSDB_JSON_OPT_PROP(js, max_dict_bytes);
    ROCKSDB_JSON_OPT_PROP(js, zstd_max_train_bytes);
    ROCKSDB_JSON_OPT_PROP(js, parallel_threads);
    ROCKSDB_JSON_OPT_PROP(js, enabled);
  }
};
CompressionOptions_Json NestForBase(CompressionOptions&);

struct ColumnFamilyOptions_Json : ColumnFamilyOptions {
  Status UpdateFromJson(const json& js, const JsonOptionsRepo& repo) try {
    ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_number);
    ROCKSDB_JSON_OPT_PROP(js, min_write_buffer_number_to_merge);
    ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_number_to_maintain);
    ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_size_to_maintain);
    ROCKSDB_JSON_OPT_PROP(js, inplace_update_support);
    ROCKSDB_JSON_OPT_PROP(js, inplace_update_num_locks);
    // ROCKSDB_JSON_OPT_PROP(js, inplace_callback); // not need update
    ROCKSDB_JSON_OPT_PROP(js, memtable_prefix_bloom_size_ratio);
    ROCKSDB_JSON_OPT_PROP(js, memtable_whole_key_filtering);
    ROCKSDB_JSON_OPT_PROP(js, memtable_huge_page_size);
    ROCKSDB_JSON_OPT_FACT(js, memtable_insert_with_hint_prefix_extractor);
    ROCKSDB_JSON_OPT_PROP(js, bloom_locality);
    ROCKSDB_JSON_OPT_PROP(js, arena_block_size);
    try { // compression_per_level is an enum array
      auto iter = js.find("compression_per_level");
      if (js.end() != iter) {
        if (!iter.value().is_array()) {
          return Status::InvalidArgument(ROCKSDB_FUNC,
              "compression_per_level must be an array");
        }
        for (auto& item : iter.value().items()) {
          const std::string& val = item.value().get<std::string>();
          CompressionType compressionType;
          if (!enum_value(val, &compressionType)) {
            return Status::InvalidArgument(ROCKSDB_FUNC,
                std::string("compression_per_level: invalid enum: ") + val);
          }
          compression_per_level.push_back(compressionType);
        }
      }
    } catch (const std::exception& ex) {
      return Status::InvalidArgument(ROCKSDB_FUNC,
          std::string("compression_per_level: ") + ex.what());
    }
    ROCKSDB_JSON_OPT_PROP(js, num_levels);
    ROCKSDB_JSON_OPT_PROP(js, level0_slowdown_writes_trigger);
    ROCKSDB_JSON_OPT_PROP(js, level0_stop_writes_trigger);
    ROCKSDB_JSON_OPT_PROP(js, target_file_size_base);
    ROCKSDB_JSON_OPT_PROP(js, target_file_size_multiplier);
    ROCKSDB_JSON_OPT_PROP(js, level_compaction_dynamic_level_bytes);
    ROCKSDB_JSON_OPT_PROP(js, max_bytes_for_level_multiplier);
    try {
      auto iter = js.find("max_bytes_for_level_multiplier_additional");
      if (js.end() != iter)
        if (!Init_vec(iter.value(), max_bytes_for_level_multiplier_additional))
          return Status::InvalidArgument(ROCKSDB_FUNC,
              "max_bytes_for_level_multiplier_additional must be a int vector");
    } catch (const std::exception& ex) {
      return Status::InvalidArgument(ROCKSDB_FUNC,
        std::string(
           "max_bytes_for_level_multiplier_additional must be a int vector, "
          "details: ") + ex.what());
    }
    ROCKSDB_JSON_OPT_PROP(js, max_compaction_bytes);
    ROCKSDB_JSON_OPT_PROP(js, soft_pending_compaction_bytes_limit);
    ROCKSDB_JSON_OPT_PROP(js, hard_pending_compaction_bytes_limit);
    ROCKSDB_JSON_OPT_ENUM(js, compaction_style);
    ROCKSDB_JSON_OPT_PROP(js, compaction_pri);
    ROCKSDB_JSON_OPT_NEST(js, compaction_options_universal);
    ROCKSDB_JSON_OPT_NEST(js, compaction_options_fifo);
    ROCKSDB_JSON_OPT_PROP(js, max_sequential_skip_in_iterations);
    ROCKSDB_JSON_OPT_FACT(js, memtable_factory);
    try {
      auto iter = js.find("table_properties_collector_factories");
      if (js.end() != iter) {
        if (!iter.value().is_array()) {
          return Status::InvalidArgument(
              ROCKSDB_FUNC,
              "table_properties_collector_factories must be an array");
        }
        decltype(table_properties_collector_factories) vec;
        for (auto& item : iter.value().items()) {
          decltype(vec)::value_type p;
          ROCKSDB_JSON_OPT_FACT_IMPL(item.value(), p);
          vec.push_back(p);
        }
        table_properties_collector_factories.swap(vec);
      }
    } catch (const std::exception& ex) {
      return Status::InvalidArgument(
          ROCKSDB_FUNC,
          std::string("table_properties_collector_factories: ") + ex.what());
    }
    ROCKSDB_JSON_OPT_PROP(js, max_successive_merges);
    ROCKSDB_JSON_OPT_PROP(js, optimize_filters_for_hits);
    ROCKSDB_JSON_OPT_PROP(js, paranoid_file_checks);
    ROCKSDB_JSON_OPT_PROP(js, force_consistency_checks);
    ROCKSDB_JSON_OPT_PROP(js, report_bg_io_stats);
    ROCKSDB_JSON_OPT_PROP(js, ttl);
    ROCKSDB_JSON_OPT_PROP(js, periodic_compaction_seconds);
    ROCKSDB_JSON_OPT_PROP(js, sample_for_compression);
    ROCKSDB_JSON_OPT_PROP(js, max_mem_compaction_level);
    ROCKSDB_JSON_OPT_PROP(js, soft_rate_limit);
    ROCKSDB_JSON_OPT_PROP(js, hard_rate_limit);
    ROCKSDB_JSON_OPT_PROP(js, rate_limit_delay_max_milliseconds);
    ROCKSDB_JSON_OPT_PROP(js, purge_redundant_kvs_while_flush);
    // ------- ColumnFamilyOptions specific --------------------------
    ROCKSDB_JSON_OPT_FACT(js, comparator);
    ROCKSDB_JSON_OPT_FACT(js, merge_operator);
    // ROCKSDB_JSON_OPT_FACT(js, compaction_filter);
    ROCKSDB_JSON_OPT_FACT(js, compaction_filter_factory);
    ROCKSDB_JSON_OPT_PROP(js, write_buffer_size);
    ROCKSDB_JSON_OPT_ENUM(js, compression);
    ROCKSDB_JSON_OPT_ENUM(js, bottommost_compression);
    ROCKSDB_JSON_OPT_NEST(js, bottommost_compression_opts);
    ROCKSDB_JSON_OPT_NEST(js, compression_opts);
    ROCKSDB_JSON_OPT_PROP(js, level0_file_num_compaction_trigger);
    ROCKSDB_JSON_OPT_FACT(js, prefix_extractor);
    ROCKSDB_JSON_OPT_PROP(js, max_bytes_for_level_base);
    ROCKSDB_JSON_OPT_PROP(js, snap_refresh_nanos);
    ROCKSDB_JSON_OPT_PROP(js, disable_auto_compactions);
    ROCKSDB_JSON_OPT_FACT(js, table_factory);
    {
      auto iter = js.find("cf_paths");
      if (js.end() != iter) Json_DbPathVec(js, cf_paths);
    }
    ROCKSDB_JSON_OPT_FACT(js, compaction_thread_limiter);
    return Status::OK();
  } catch (const std::exception& ex) {
    return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
  }
};

//////////////////////////////////////////////////////////////////////////////

static shared_ptr<TablePropertiesCollectorFactory>
NewCompactOnDeletionCollectorFactoryForJson(
    const json& js, const JsonOptionsRepo&, Status* s)
try {
  size_t sliding_window_size = 0;
  size_t deletion_trigger = 0;
  double deletion_ratio = 0;
  ROCKSDB_JSON_REQ_PROP(js, sliding_window_size);
  ROCKSDB_JSON_REQ_PROP(js, deletion_trigger);
  ROCKSDB_JSON_OPT_PROP(js, deletion_ratio);  // this is optional
  *s = Status::OK();
  return NewCompactOnDeletionCollectorFactory(sliding_window_size,
                                              deletion_trigger, deletion_ratio);
} catch (const std::exception& ex) {
  *s = Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
  return nullptr;
}
ROCKSDB_FACTORY_REG("CompactOnDeletionCollector",
                    NewCompactOnDeletionCollectorFactoryForJson);

//////////////////////////////////////////////////////////////////////////////

shared_ptr<RateLimiter>
NewGenericRateLimiterFromJson(const json& js, const JsonOptionsRepo& repo, Status* s) {
  int64_t rate_bytes_per_sec = 0;
  int64_t refill_period_us = 100 * 1000;
  int32_t fairness = 10;
  RateLimiter::Mode mode = RateLimiter::Mode::kWritesOnly;
  bool auto_tuned = false;
  ROCKSDB_JSON_REQ_PROP(js, rate_bytes_per_sec); // required
  ROCKSDB_JSON_OPT_PROP(js, refill_period_us);
  ROCKSDB_JSON_OPT_PROP(js, fairness);
  ROCKSDB_JSON_OPT_ENUM(js, mode);
  ROCKSDB_JSON_OPT_PROP(js, auto_tuned);
  if (rate_bytes_per_sec <= 0) {
    *s = Status::InvalidArgument(ROCKSDB_FUNC, "rate_bytes_per_sec must > 0");
    return nullptr;
  }
  if (refill_period_us <= 0) {
    *s = Status::InvalidArgument(ROCKSDB_FUNC, "refill_period_us must > 0");
    return nullptr;
  }
  if (fairness <= 0) {
    *s = Status::InvalidArgument(ROCKSDB_FUNC, "fairness must > 0");
    return nullptr;
  }
  Env* env = Env::Default();
  auto iter = js.find("env");
  if (js.end() != iter) {
    const auto& env_js = iter.value();
    env = FactoryFor<Env*>::GetInstance("env", ROCKSDB_FUNC, env_js, repo, s);
    if (!env)
      return nullptr;
  }
  *s = Status::OK();
  return std::make_shared<GenericRateLimiter>(
      rate_bytes_per_sec, refill_period_us, fairness,
      mode, env, auto_tuned);
}

ROCKSDB_FACTORY_REG("GenericRateLimiter", NewGenericRateLimiterFromJson);

//////////////////////////////////////////////////////////////////////////////
static const Comparator*
BytewiseComp(const json&, const JsonOptionsRepo&, Status*) {
  return BytewiseComparator();
}
static const Comparator*
RevBytewiseComp(const json&, const JsonOptionsRepo&, Status*) {
  return ReverseBytewiseComparator();
}
ROCKSDB_FACTORY_REG(        "BytewiseComparator", BytewiseComp);
ROCKSDB_FACTORY_REG("leveldb.BytewiseComparator", BytewiseComp);
ROCKSDB_FACTORY_REG(        "ReverseBytewiseComparator", RevBytewiseComp);
ROCKSDB_FACTORY_REG("leveldb.ReverseBytewiseComparator", RevBytewiseComp);

//////////////////////////////////////////////////////////////////////////////
static Env* DefaultEnv(const json&, const JsonOptionsRepo&, Status*) {
  return Env::Default();
}
ROCKSDB_FACTORY_REG("default", DefaultEnv);

/////////////////////////////////////////////////////////////////////////////
static shared_ptr<FlushBlockBySizePolicyFactory>
NewFlushBlockBySizePolicyFactoryFactoryJson(const json&,
                                    const JsonOptionsRepo&, Status*) {
  return std::make_shared<FlushBlockBySizePolicyFactory>();
}
ROCKSDB_FACTORY_REG("FlushBlockBySize",
                    NewFlushBlockBySizePolicyFactoryFactoryJson);

/////////////////////////////////////////////////////////////////////////////
static shared_ptr<FileChecksumGenFactory>
GetFileChecksumGenCrc32cFactoryJson(const json&,
                                    const JsonOptionsRepo&, Status*) {
  return GetFileChecksumGenCrc32cFactory();
}
ROCKSDB_FACTORY_REG("Crc32c", GetFileChecksumGenCrc32cFactoryJson);
ROCKSDB_FACTORY_REG("crc32c", GetFileChecksumGenCrc32cFactoryJson);

/////////////////////////////////////////////////////////////////////////////
static shared_ptr<MemTableRepFactory>
NewSkipListMemTableRepFactoryJson(const json& js,
                                  const JsonOptionsRepo&, Status* s)
try {
  size_t lookahead = 0;
  ROCKSDB_JSON_OPT_PROP(js, lookahead);
  return std::make_shared<SkipListFactory>(lookahead);
}
catch (const std::exception& ex) {
  *s = Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
  return nullptr;
}
ROCKSDB_FACTORY_REG("SkipListRep", NewSkipListMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("SkipList", NewSkipListMemTableRepFactoryJson);

static shared_ptr<MemTableRepFactory>
NewVectorMemTableRepFactoryJson(const json& js,
                                const JsonOptionsRepo&, Status* s)
try {
  size_t count = 0;
  ROCKSDB_JSON_OPT_PROP(js, count);
  return std::make_shared<VectorRepFactory>(count);
}
catch (const std::exception& ex) {
  *s = Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
  return nullptr;
}
ROCKSDB_FACTORY_REG("VectorRep", NewVectorMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("Vector", NewVectorMemTableRepFactoryJson);

static shared_ptr<MemTableRepFactory>
NewHashSkipListMemTableRepFactoryJson(const json& js,
                                      const JsonOptionsRepo&, Status* s)
try {
  size_t bucket_count = 1000000;
  int32_t height = 4;
  int32_t branching_factor = 4;
  ROCKSDB_JSON_OPT_PROP(js, bucket_count);
  ROCKSDB_JSON_OPT_PROP(js, height);
  ROCKSDB_JSON_OPT_PROP(js, branching_factor);
  return shared_ptr<MemTableRepFactory>(
      NewHashSkipListRepFactory(bucket_count, height, branching_factor));
}
catch (const std::exception& ex) {
  *s = Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
  return nullptr;
}
ROCKSDB_FACTORY_REG("HashSkipListRep", NewHashSkipListMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("HashSkipList", NewHashSkipListMemTableRepFactoryJson);

static shared_ptr<MemTableRepFactory>
NewHashLinkListMemTableRepFactoryJson(const json& js,
                                      const JsonOptionsRepo&, Status* s)
try {
  size_t bucket_count = 50000;
  size_t huge_page_tlb_size = 0;
  int bucket_entries_logging_threshold = 4096;
  bool if_log_bucket_dist_when_flash = true;
  uint32_t threshold_use_skiplist = 256;
  ROCKSDB_JSON_OPT_PROP(js, bucket_count);
  ROCKSDB_JSON_OPT_PROP(js, huge_page_tlb_size);
  ROCKSDB_JSON_OPT_PROP(js, bucket_entries_logging_threshold);
  ROCKSDB_JSON_OPT_PROP(js, if_log_bucket_dist_when_flash);
  ROCKSDB_JSON_OPT_PROP(js, threshold_use_skiplist);
  return shared_ptr<MemTableRepFactory>(
      NewHashLinkListRepFactory(bucket_count,
                                huge_page_tlb_size,
                                bucket_entries_logging_threshold,
                                if_log_bucket_dist_when_flash,
                                threshold_use_skiplist));
}
catch (const std::exception& ex) {
  *s = Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
  return nullptr;
}
ROCKSDB_FACTORY_REG("HashLinkListRep", NewHashLinkListMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("HashLinkList", NewHashLinkListMemTableRepFactoryJson);

/////////////////////////////////////////////////////////////////////////////

struct JsonOptionsRepo::Impl {
  template<class T>
  using ObjRepo = shared_ptr<unordered_map<std::string, shared_ptr<T>>>;
  template<class T> // just for type deduction
  static shared_ptr<T> RepoPtrType(const ObjRepo<T>&);
  template<class T> // just for type deduction
  static const shared_ptr<T>& RepoPtrCref(const ObjRepo<T>&);
  template<class T> // just for type deduction
  static T* RepoPtrCref(const shared_ptr<unordered_map<std::string, T*>>&);
  template<class T> // just for type deduction
  static T* RepoPtrType(const shared_ptr<unordered_map<std::string, T*>>&);

  ObjRepo<Cache> cache;
  ObjRepo<PersistentCache> persistent_cache;
  ObjRepo<CompactionFilterFactory> compaction_filter_factory;
  shared_ptr<unordered_map<std::string, const Comparator*> > comparator;
  ObjRepo<ConcurrentTaskLimiter> concurrent_task_limiter;
  shared_ptr<unordered_map<std::string, Env*> > env;
  ObjRepo<EventListener> event_listener;
  ObjRepo<FileChecksumGenFactory> file_checksum_gen_factory;
  ObjRepo<const FilterPolicy> filter_policy;
  ObjRepo<FlushBlockPolicyFactory> flush_block_policy_factory;
  ObjRepo<Logger> logger;
  ObjRepo<MemTableRepFactory> mem_table_rep_factory;
  ObjRepo<MergeOperator> merge_operator;
  ObjRepo<RateLimiter> rate_limiter;
  ObjRepo<SstFileManager> sst_file_manager;
  ObjRepo<Statistics> statistics;
  ObjRepo<TableFactory> table_factory;
  ObjRepo<TablePropertiesCollectorFactory> table_properties_collector_factory;
  ObjRepo<const SliceTransform> slice_transform;

  ObjRepo<Options> options;
  ObjRepo<DBOptions> db_options;
  ObjRepo<ColumnFamilyOptions> cf_options;

  template<class Ptr>
  static void Import(unordered_map<string, Ptr>* field,
      const char* name, const char* func,
      const json& main_js, const JsonOptionsRepo& repo) {
    auto iter = main_js.find(name);
    if (main_js.end() != iter) {
      if (!iter.value().is_object()) {
        throw Status::InvalidArgument(func,
            string(name) + " must be an object with class and options");
      }
      for (auto& item : iter.value().items()) {
        const string& inst_id = item.key();
        const auto& value = item.value();
        Status s;
        // name and func are just for error report in this call
        Ptr p = FactoryFor<Ptr>::GetOrNewInstance(name, func, value, repo, &s);
        if (!s.ok()) throw s;
        field->emplace(inst_id, p);
      }
    }
  }
};

JsonOptionsRepo::JsonOptionsRepo() noexcept {
  m_impl.reset(new Impl);
}
JsonOptionsRepo::~JsonOptionsRepo() = default;
JsonOptionsRepo::JsonOptionsRepo(const JsonOptionsRepo&) noexcept = default;
JsonOptionsRepo::JsonOptionsRepo(JsonOptionsRepo&&) noexcept = default;
JsonOptionsRepo& JsonOptionsRepo::operator=(const JsonOptionsRepo&) noexcept = default;
JsonOptionsRepo& JsonOptionsRepo::operator=(JsonOptionsRepo&&) noexcept = default;

Status JsonOptionsRepo::Import(const std::string& json_str) {
  json js(json_str);
  return Import(js);
}

#define SHARED_PTR_TYPE(field) decltype(Impl::RepoPtrType(((Impl*)0)->field))

Status JsonOptionsRepo::Import(const nlohmann::json& main_js) try {
  const auto& repo = *this;
#define JSON_PARSE_REPO(field) \
  Impl::Import(m_impl->field.get(), #field, ROCKSDB_FUNC, main_js, repo)
/*
#define JSON_PARSE_REPO(field) \
  do { \
    auto iter = main_js.find(#field); \
    if (main_js.end() != iter) { \
      if (!iter.value().is_object()) { \
        return Status::InvalidArgument(ROCKSDB_FUNC, \
        #field " must be an object with class and options sub object"); \
      } \
      for (auto& item : iter.value().items()) { \
        const std::string& name = item.key(); \
        const auto& value = item.value(); \
        SHARED_PTR_TYPE(field) field; \
        ROCKSDB_JSON_OPT_FACT(value, field); \
        m_impl->field->emplace(name, field); \
      } \
    } \
  } while (0)
*/
  JSON_PARSE_REPO(comparator);
  JSON_PARSE_REPO(env);
  JSON_PARSE_REPO(logger);
  JSON_PARSE_REPO(slice_transform);
  JSON_PARSE_REPO(cache);
  JSON_PARSE_REPO(persistent_cache);
  JSON_PARSE_REPO(compaction_filter_factory);
  JSON_PARSE_REPO(concurrent_task_limiter);
  JSON_PARSE_REPO(event_listener);
  JSON_PARSE_REPO(file_checksum_gen_factory);
  JSON_PARSE_REPO(filter_policy);
  JSON_PARSE_REPO(flush_block_policy_factory);
  JSON_PARSE_REPO(merge_operator);
  JSON_PARSE_REPO(rate_limiter);
  JSON_PARSE_REPO(sst_file_manager);
  JSON_PARSE_REPO(statistics);
  JSON_PARSE_REPO(table_properties_collector_factory);

  JSON_PARSE_REPO(mem_table_rep_factory);
  JSON_PARSE_REPO(table_factory);

  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& s) {
  return s;
}

Status JsonOptionsRepo::Export(nlohmann::json* js) const {
  assert(NULL != js);
  return Status::OK();
}

Status JsonOptionsRepo::Export(std::string* json_str, bool pretty) const {
  assert(NULL != json_str);
  nlohmann::json js;
  Status s = Export(&js);
  if (s.ok()) {
    *json_str = js.dump(pretty ? 4 : -1);
  }
  return s;
}

#define JSON_REPO_TYPE_IMPL(field) \
void JsonOptionsRepo::Add(const std::string& name, \
                          decltype((Impl::RepoPtrCref(((Impl*)0)->field))) p) { \
  auto ib = m_impl->field->emplace(name, p); \
  if (ib.second) \
    ib.first->second = p; \
} \
bool JsonOptionsRepo::Get(const std::string& name, \
                          SHARED_PTR_TYPE(field)* pp) const { \
  auto& __map = *m_impl->field; \
  auto iter = __map.find(name); \
  if (__map.end() != iter) { \
    *pp = iter->second; \
    return true; \
  } \
  return false; \
}

JSON_REPO_TYPE_IMPL(cache)
JSON_REPO_TYPE_IMPL(persistent_cache)
JSON_REPO_TYPE_IMPL(compaction_filter_factory)
JSON_REPO_TYPE_IMPL(comparator)
JSON_REPO_TYPE_IMPL(concurrent_task_limiter)
JSON_REPO_TYPE_IMPL(env)
JSON_REPO_TYPE_IMPL(event_listener)
JSON_REPO_TYPE_IMPL(file_checksum_gen_factory)
JSON_REPO_TYPE_IMPL(filter_policy)
JSON_REPO_TYPE_IMPL(flush_block_policy_factory)
JSON_REPO_TYPE_IMPL(logger)
JSON_REPO_TYPE_IMPL(mem_table_rep_factory)
JSON_REPO_TYPE_IMPL(merge_operator)
JSON_REPO_TYPE_IMPL(rate_limiter)
JSON_REPO_TYPE_IMPL(sst_file_manager)
JSON_REPO_TYPE_IMPL(statistics)
JSON_REPO_TYPE_IMPL(table_factory)
JSON_REPO_TYPE_IMPL(table_properties_collector_factory)
JSON_REPO_TYPE_IMPL(slice_transform)

JSON_REPO_TYPE_IMPL(options)
JSON_REPO_TYPE_IMPL(db_options)
JSON_REPO_TYPE_IMPL(cf_options)

void JsonOptionsRepo::GetMap(
    shared_ptr<unordered_map<std::string,
                             shared_ptr<TableFactory>>>* pp) const {
  *pp = m_impl->table_factory;
}

void JsonOptionsRepo::GetMap(
    shared_ptr<unordered_map<std::string,
                             shared_ptr<MemTableRepFactory>>>* pp) const {
  *pp = m_impl->mem_table_rep_factory;
}

}
