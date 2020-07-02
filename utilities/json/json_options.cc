#include <sys/cdefs.h>
//
// Created by leipeng on 2020/7/1.
//

#include "utilities/table_properties_collectors/compact_on_deletion_collector.h"

#include <memory>
#include <sstream>
#include <cinttypes>
#include <limits>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "monitoring/statistics.h"
#include "options/db_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/listener.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/table.h"
#include "db/table_properties_collector.h"
#include "rocksdb/wal_filter.h"
#include "util/compression.h"

#include "util/rate_limiter.h"
#include "json.h"
#include "factoryable.h"
#include "json_options_repo.h"

namespace ROCKSDB_NAMESPACE {

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

static Status Json_EventListenerVec(const json& js,
    std::vector<std::shared_ptr<EventListener>>& listeners) {
  listeners.clear();
  if (js.is_string()) {
    std::shared_ptr<EventListener> el;
    ROCKSDB_JSON_OPT_FACT_IMPL(js, el);
    listeners.emplace_back(el);
  }
  else if (js.is_array()) {
    for (auto& one : js.items()) {
      std::shared_ptr<EventListener> el;
      ROCKSDB_JSON_OPT_FACT_IMPL(one.value(), el);
      listeners.emplace_back(el);
    }
  }
  return Status::OK();
}

struct DBOptions_Json : DBOptions {
  Status UpdateFromJson(const json& js) try {
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
        std::shared_ptr<Cache> cache;
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
        Json_EventListenerVec(js, listeners);
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
friend CompactionOptionsFIFO_Json NestForBase(CompactionOptionsFIFO&);
  explicit CompactionOptionsFIFO_Json(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, max_table_files_size);
    ROCKSDB_JSON_OPT_PROP(js, allow_compaction);
  }
};
struct CompactionOptionsUniversal_Json : CompactionOptionsUniversal {
friend CompactionOptionsUniversal_Json NestForBase(CompactionOptionsUniversal&);
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
struct CompressionOptions_Json : CompressionOptions {
friend CompressionOptions_Json NestForBase(CompressionOptions&);
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

struct ColumnFamilyOptions_Json : ColumnFamilyOptions {
  Status UpdateFromJson(const json& js) try {
    ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_number);
    ROCKSDB_JSON_OPT_PROP(js, min_write_buffer_number_to_merge);
    ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_number_to_maintain);
    ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_size_to_maintain);
    ROCKSDB_JSON_OPT_PROP(js, inplace_update_support);
    ROCKSDB_JSON_OPT_ENUM(js, inplace_update_num_locks);
    // ROCKSDB_JSON_OPT_PROP(js, inplace_callback); // not need update
    ROCKSDB_JSON_OPT_PROP(js, memtable_prefix_bloom_size_ratio);
    ROCKSDB_JSON_OPT_PROP(js, memtable_whole_key_filtering);
    ROCKSDB_JSON_OPT_PROP(js, memtable_huge_page_size);
    ROCKSDB_JSON_OPT_FACT(js, memtable_insert_with_hint_prefix_extractor);
    ROCKSDB_JSON_OPT_PROP(js, bloom_locality);
    ROCKSDB_JSON_OPT_PROP(js, arena_block_size);
    ROCKSDB_JSON_OPT_ENUM(js, compression_per_level);
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
          ROCKSDB_JSON_OPT_FACT_IMPL(js, p);
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

static std::shared_ptr<TablePropertiesCollectorFactory>
NewCompactOnDeletionCollectorFactoryForJson(const json& js, Status* s) try {
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

std::shared_ptr<RateLimiter>
NewGenericRateLimiterFromJson(const json& js, Status* s) {
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
  *s = Status::OK();
  Env* env = Env::Default();
  return std::make_shared<GenericRateLimiter>(
      rate_bytes_per_sec, refill_period_us, fairness,
      mode, env, auto_tuned);
}

ROCKSDB_FACTORY_REG("GenericRateLimiter", NewGenericRateLimiterFromJson);


//////////////////////////////////////////////////////////////////////////////
static const Comparator* BytewiseComp(const json&, Status*) {
  return BytewiseComparator();
}
static const Comparator* RevBytewiseComp(const json&, Status*) {
  return ReverseBytewiseComparator();
}
ROCKSDB_FACTORY_REG(        "BytewiseComparator", BytewiseComp);
ROCKSDB_FACTORY_REG("leveldb.BytewiseComparator", BytewiseComp);
ROCKSDB_FACTORY_REG(        "ReverseBytewiseComparator", RevBytewiseComp);
ROCKSDB_FACTORY_REG("leveldb.ReverseBytewiseComparator", RevBytewiseComp);

//////////////////////////////////////////////////////////////////////////////
static Env* DefaultEnv(const json&, Status*) {
  return Env::Default();
}
ROCKSDB_FACTORY_REG("default", DefaultEnv);

//////////////////////////////////////////////////////////////////////////////

template<class T>
static Status InitRepo(const json& repo_js, const char* name) {
  auto iter = repo_js.find(name);
  if (repo_js.end() != iter) {
    for (auto& one : iter.value().items()) {
      const std::string& inst_id = one.key();
      const json& js = one.value();
      std::shared_ptr<T> obj;
      ROCKSDB_JSON_OPT_FACT_IMPL(js, obj);
      FactoryFor<std::shared_ptr<T> >::InsertRepoInstance(inst_id, obj);
    }
  }
  return Status::OK();
}

Status InitConfigRepo(const json& main_js) try {
  Status s;
#define INIT_REPO(js, T, name) \
  s = InitRepo<T>(js, name); if (!s.ok()) return s

  auto iter = main_js.find("global");
  if (main_js.end() != iter) {
    auto& global_js = iter.value();
    iter = global_js.find("DBOptions");
    if (global_js.end() != iter) {
      // TODO:
    }
    INIT_REPO(global_js, Cache, "cache");
    INIT_REPO(global_js, PersistentCache, "persistent_cache");
    INIT_REPO(global_js, FilterPolicy, "filter_policy");
    INIT_REPO(global_js, TableFactory, "table_factory");
  }
/* TODO:
  iter = main_js.find("column_family");
  if (main_js.end() != iter) {
    auto& cfset_js = iter.value();
    for (auto& cf : cfset_js.items()) {
      const std::string& cf_name = cf.key();
      // TODO:
    }
  }
*/
  return s;
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}

Status InitConfigRepo(const std::string& json_text) try {
  json main_js(json_text);
  return InitConfigRepo(main_js);
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}

/////////////////////////////////////////////////////////////////////////////
struct JsonOptionsRepo::Impl {
  template<class T>
  using ObjRepo = std::unordered_map<std::string, std::shared_ptr<T> >;
  template<class T> // just for type deduction
  static std::shared_ptr<T> SharedptrType(ObjRepo<T>&);

  ObjRepo<Cache> cache;
  ObjRepo<PersistentCache> persistent_cache;
  ObjRepo<CompactionFilterFactory> compaction_filter_factory;
  ObjRepo<Comparator> comparator;
  ObjRepo<ConcurrentTaskLimiter> concurrent_task_limiter;
  ObjRepo<Env> env;
  ObjRepo<EventListener> event_listener;
  ObjRepo<FileChecksumGenFactory> file_checksum_gen_factory;
  ObjRepo<FilterPolicy> filter_policy;
  ObjRepo<FlushBlockPolicyFactory> flush_block_policy_factory;
  ObjRepo<Logger> logger;
  ObjRepo<MemTableRepFactory> mem_table_rep_factory;
  ObjRepo<MergeOperator> merge_operator;
  ObjRepo<RateLimiter> rate_limiter;
  ObjRepo<SliceTransform> slice_ransform;
  ObjRepo<SstFileManager> sst_file_manager;
  ObjRepo<Statistics> statistics;
  ObjRepo<TableFactory> table_factory;
  ObjRepo<TablePropertiesCollectorFactory> table_properties_collector_factory;
  ObjRepo<SliceTransform> slice_transform;

  ObjRepo<Options> options;
  ObjRepo<DBOptions> db_options;
  ObjRepo<ColumnFamilyOptions> cf_options;
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

#define SHARED_PTR_TYPE(field) decltype(Impl::SharedptrType(((Impl*)0)->field))

Status JsonOptionsRepo::Import(const nlohmann::json& main_js) try {
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
        SHARED_PTR_TYPE(field); \
        ROCKSDB_JSON_OPT_FACT(value, field); \
        m_impl->field.emplace(name, field); \
      } \
    } \
  } while (0)

  JSON_PARSE_REPO(cache);
  JSON_PARSE_REPO(persistent_cache);
  JSON_PARSE_REPO(compaction_filter_factory);
  JSON_PARSE_REPO(comparator);
  JSON_PARSE_REPO(concurrent_task_limiter);
  JSON_PARSE_REPO(env);
  JSON_PARSE_REPO(event_listener);
  JSON_PARSE_REPO(file_checksum_gen_factory);
  JSON_PARSE_REPO(filter_policy);
  JSON_PARSE_REPO(flush_block_policy_factory);
  JSON_PARSE_REPO(logger);
  JSON_PARSE_REPO(mem_table_rep_factory);
  JSON_PARSE_REPO(merge_operator);
  JSON_PARSE_REPO(rate_limiter);
  JSON_PARSE_REPO(slice_ransform);
  JSON_PARSE_REPO(sst_file_manager);
  JSON_PARSE_REPO(statistics);
  JSON_PARSE_REPO(table_factory);
  JSON_PARSE_REPO(table_properties_collector_factory);
  JSON_PARSE_REPO(slice_transform);

  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}

Status JsonOptionsRepo::Export(nlohmann::json* js) const {

}

Status JsonOptionsRepo::Export(std::string* json_str, bool pretty) const {
  nlohmann::json js;
  Status s = Export(&js);
  if (s.ok()) {
    js.dump(pretty ? 4 : -1);
  }
  return s;
}

#define JSON_REPO_TYPE_IMPL(field) \
void JsonOptionsRepo::Add(const std::string& name, \
                          const SHARED_PTR_TYPE(field)& p) { \
  auto ib = m_impl->field.emplace(name, p); \
  if (ib.second) \
    ib.first->second = p; \
} \
bool JsonOptionsRepo::Get(const std::string& name, \
                          SHARED_PTR_TYPE(field)* pp) { \
  auto iter = m_impl->field.find(name); \
  if (m_impl->field.end() != iter) { \
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
JSON_REPO_TYPE_IMPL(slice_ransform)
JSON_REPO_TYPE_IMPL(sst_file_manager)
JSON_REPO_TYPE_IMPL(statistics)
JSON_REPO_TYPE_IMPL(table_factory)
JSON_REPO_TYPE_IMPL(table_properties_collector_factory)
JSON_REPO_TYPE_IMPL(slice_transform)

JSON_REPO_TYPE_IMPL(options)
JSON_REPO_TYPE_IMPL(db_options)
JSON_REPO_TYPE_IMPL(cf_options)


}