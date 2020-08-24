//
// Created by leipeng on 2020/7/12.
//


#include "utilities/table_properties_collectors/compact_on_deletion_collector.h"

#include <memory>
#include <cinttypes>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "options/db_options.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/transaction_db_mutex.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/wal_filter.h"
#include "util/rate_limiter.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"
#include "json.h"
#include "json_plugin_factory.h"
#if defined(MEMKIND)
  #include "memory/memkind_kmem_allocator.h"
#endif

namespace ROCKSDB_NAMESPACE {

using std::shared_ptr;
using std::unordered_map;
using std::vector;
using std::string;

static std::shared_ptr<FileSystem>
DefaultFileSystemForJson(const json&, const JsonPluginRepo&) {
  return FileSystem::Default();
}
ROCKSDB_FACTORY_REG("posix", DefaultFileSystemForJson);
ROCKSDB_FACTORY_REG("Posix", DefaultFileSystemForJson);
ROCKSDB_FACTORY_REG("default", DefaultFileSystemForJson);
ROCKSDB_FACTORY_REG("Default", DefaultFileSystemForJson);

static json DbPathToJson(const DbPath& x) {
  if (0 == x.target_size)
    return json{x.path};
  else
    return json{
        { "path", x.path },
        { "target_size", x.target_size }
    };
}
static DbPath DbPathFromJson(const json& js) {
  DbPath x;
  if (js.is_string()) {
    x.path = js.get<string>();
  } else {
    x.path = js.at("path").get<string>();
    x.target_size = ParseSizeXiB(js, "target_size");
  }
  return x;
}

static void Json_DbPathVec(const json& js, std::vector<DbPath>& db_paths) {
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

static Status Json_EventListenerVec(const json& js, const JsonPluginRepo& repo,
                                    std::vector<shared_ptr<EventListener>>& listeners) {
  listeners.clear();
  if (js.is_string()) {
    shared_ptr<EventListener> el;
    ROCKSDB_JSON_OPT_FACT_INNER(js, el);
    listeners.emplace_back(el);
  }
  else if (js.is_array()) {
    for (auto& one : js.items()) {
      shared_ptr<EventListener> el;
      ROCKSDB_JSON_OPT_FACT_INNER(one.value(), el);
      listeners.emplace_back(el);
    }
  }
  return Status::OK();
}

struct DBOptions_Json : DBOptions {
  DBOptions_Json(const json& js, const JsonPluginRepo& repo) {
    Update(js, repo);
  }
  void Update(const json& js, const JsonPluginRepo& repo) {
    ROCKSDB_JSON_OPT_PROP(js, create_if_missing);
    ROCKSDB_JSON_OPT_PROP(js, create_missing_column_families);
    ROCKSDB_JSON_OPT_PROP(js, error_if_exists);
    ROCKSDB_JSON_OPT_PROP(js, paranoid_checks);
    ROCKSDB_JSON_OPT_FACT(js, env);
    ROCKSDB_JSON_OPT_FACT(js, rate_limiter);
    ROCKSDB_JSON_OPT_FACT(js, sst_file_manager);
    ROCKSDB_JSON_OPT_FACT(js, info_log);
    ROCKSDB_JSON_OPT_ENUM(js, info_log_level);
    ROCKSDB_JSON_OPT_PROP(js, max_open_files);
    ROCKSDB_JSON_OPT_PROP(js, max_file_opening_threads);
    ROCKSDB_JSON_OPT_SIZE(js, max_total_wal_size);
    ROCKSDB_JSON_OPT_FACT(js, statistics);
    ROCKSDB_JSON_OPT_PROP(js, use_fsync);
    {
      auto iter = js.find("db_paths");
      if (js.end() != iter)
        Json_DbPathVec(js, db_paths);
    }
    ROCKSDB_JSON_OPT_PROP(js, db_log_dir);
    ROCKSDB_JSON_OPT_PROP(js, wal_dir);
    ROCKSDB_JSON_OPT_PROP(js, delete_obsolete_files_period_micros);
    ROCKSDB_JSON_OPT_PROP(js, max_background_jobs);
    ROCKSDB_JSON_OPT_PROP(js, base_background_compactions);
    ROCKSDB_JSON_OPT_PROP(js, max_background_compactions);
    ROCKSDB_JSON_OPT_PROP(js, max_subcompactions);
    ROCKSDB_JSON_OPT_PROP(js, max_background_flushes);
    ROCKSDB_JSON_OPT_SIZE(js, max_log_file_size);
    ROCKSDB_JSON_OPT_PROP(js, log_file_time_to_roll);
    ROCKSDB_JSON_OPT_PROP(js, keep_log_file_num);
    ROCKSDB_JSON_OPT_PROP(js, recycle_log_file_num);
    ROCKSDB_JSON_OPT_SIZE(js, max_manifest_file_size);
    ROCKSDB_JSON_OPT_PROP(js, table_cache_numshardbits);
    ROCKSDB_JSON_OPT_PROP(js, WAL_ttl_seconds);
    ROCKSDB_JSON_OPT_PROP(js, WAL_size_limit_MB);
    ROCKSDB_JSON_OPT_SIZE(js, manifest_preallocation_size);
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
    ROCKSDB_JSON_OPT_SIZE(js, stats_history_buffer_size);
    ROCKSDB_JSON_OPT_PROP(js, advise_random_on_open);
    ROCKSDB_JSON_OPT_SIZE(js, db_write_buffer_size);
    {
      auto iter = js.find("write_buffer_manager");
      if (js.end() != iter) {
        auto& wbm = iter.value();
        size_t buffer_size = db_write_buffer_size;
        shared_ptr<Cache> cache;
        ROCKSDB_JSON_OPT_FACT(wbm, cache);
        ROCKSDB_JSON_OPT_SIZE(wbm, buffer_size);
        write_buffer_manager = std::make_shared<WriteBufferManager>(
            buffer_size, cache);
      }
    }
    ROCKSDB_JSON_OPT_ENUM(js, access_hint_on_compaction_start);
    ROCKSDB_JSON_OPT_PROP(js, new_table_reader_for_compaction_inputs);
    ROCKSDB_JSON_OPT_SIZE(js, compaction_readahead_size);
    ROCKSDB_JSON_OPT_SIZE(js, random_access_max_buffer_size);
    ROCKSDB_JSON_OPT_SIZE(js, writable_file_max_buffer_size);
    ROCKSDB_JSON_OPT_PROP(js, use_adaptive_mutex);
    ROCKSDB_JSON_OPT_SIZE(js, bytes_per_sync);
    ROCKSDB_JSON_OPT_SIZE(js, wal_bytes_per_sync);
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
    ROCKSDB_JSON_OPT_SIZE(js, max_write_batch_group_size_bytes);
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
    ROCKSDB_JSON_OPT_SIZE(js, log_readahead_size);
    ROCKSDB_JSON_OPT_FACT(js, file_checksum_gen_factory);
    ROCKSDB_JSON_OPT_PROP(js, best_efforts_recovery);
  }

  void SaveToJson(json& js, const JsonPluginRepo& repo, bool html) const {
    ROCKSDB_JSON_SET_PROP(js, paranoid_checks);
    ROCKSDB_JSON_SET_FACT(js, env);
    ROCKSDB_JSON_SET_FACT(js, rate_limiter);
    ROCKSDB_JSON_SET_FACT(js, sst_file_manager);
    ROCKSDB_JSON_SET_FACT(js, info_log);
    ROCKSDB_JSON_SET_ENUM(js, info_log_level);
    ROCKSDB_JSON_SET_PROP(js, max_open_files);
    ROCKSDB_JSON_SET_PROP(js, max_file_opening_threads);
    ROCKSDB_JSON_SET_SIZE(js, max_total_wal_size);
    ROCKSDB_JSON_SET_FACT(js, statistics);
    ROCKSDB_JSON_SET_PROP(js, use_fsync);
    if (!db_paths.empty()) {
      json& db_pathes_js = js["db_pathes"];
      for (auto& x : db_paths) {
        db_pathes_js.push_back(DbPathToJson(x));
      }
      if (html)
        db_pathes_js[0]["<htmltab:col>"] = 1;
    }
    ROCKSDB_JSON_SET_PROP(js, db_log_dir);
    ROCKSDB_JSON_SET_PROP(js, wal_dir);
    ROCKSDB_JSON_SET_PROP(js, delete_obsolete_files_period_micros);
    ROCKSDB_JSON_SET_PROP(js, max_background_jobs);
    ROCKSDB_JSON_SET_PROP(js, base_background_compactions);
    ROCKSDB_JSON_SET_PROP(js, max_background_compactions);
    ROCKSDB_JSON_SET_PROP(js, max_subcompactions);
    ROCKSDB_JSON_SET_PROP(js, max_background_flushes);
    ROCKSDB_JSON_SET_SIZE(js, max_log_file_size);
    ROCKSDB_JSON_SET_PROP(js, log_file_time_to_roll);
    ROCKSDB_JSON_SET_PROP(js, keep_log_file_num);
    ROCKSDB_JSON_SET_PROP(js, recycle_log_file_num);
    ROCKSDB_JSON_SET_SIZE(js, max_manifest_file_size);
    ROCKSDB_JSON_SET_PROP(js, table_cache_numshardbits);
    ROCKSDB_JSON_SET_PROP(js, WAL_ttl_seconds);
    ROCKSDB_JSON_SET_PROP(js, WAL_size_limit_MB);
    ROCKSDB_JSON_SET_SIZE(js, manifest_preallocation_size);
    ROCKSDB_JSON_SET_PROP(js, allow_mmap_reads);
    ROCKSDB_JSON_SET_PROP(js, allow_mmap_writes);
    ROCKSDB_JSON_SET_PROP(js, use_direct_reads);
    ROCKSDB_JSON_SET_PROP(js, use_direct_io_for_flush_and_compaction);
    ROCKSDB_JSON_SET_PROP(js, allow_fallocate);
    ROCKSDB_JSON_SET_PROP(js, is_fd_close_on_exec);
    ROCKSDB_JSON_SET_PROP(js, skip_log_error_on_recovery);
    ROCKSDB_JSON_SET_PROP(js, stats_dump_period_sec);
    ROCKSDB_JSON_SET_PROP(js, stats_persist_period_sec);
    ROCKSDB_JSON_SET_PROP(js, persist_stats_to_disk);
    ROCKSDB_JSON_SET_SIZE(js, stats_history_buffer_size);
    ROCKSDB_JSON_SET_PROP(js, advise_random_on_open);
    ROCKSDB_JSON_SET_SIZE(js, db_write_buffer_size);
    {
      // class WriteBufferManager is damaged, cache passed to
      // its cons is not used, and there is no way to get its internal
      // cache object
      json& wbm = js["write_buffer_manager"];
      shared_ptr<Cache> cache;
      ROCKSDB_JSON_SET_FACT(wbm, cache);
      JsonSetSize(wbm["buffer_size"], db_write_buffer_size);
    }
    ROCKSDB_JSON_SET_ENUM(js, access_hint_on_compaction_start);
    ROCKSDB_JSON_SET_PROP(js, new_table_reader_for_compaction_inputs);
    ROCKSDB_JSON_SET_SIZE(js, compaction_readahead_size);
    ROCKSDB_JSON_SET_SIZE(js, random_access_max_buffer_size);
    ROCKSDB_JSON_SET_SIZE(js, writable_file_max_buffer_size);
    ROCKSDB_JSON_SET_PROP(js, use_adaptive_mutex);
    ROCKSDB_JSON_SET_SIZE(js, bytes_per_sync);
    ROCKSDB_JSON_SET_SIZE(js, wal_bytes_per_sync);
    ROCKSDB_JSON_SET_PROP(js, strict_bytes_per_sync);
    for (auto& listener : listeners) {
      json inner;
      ROCKSDB_JSON_SET_FACT_INNER(inner, listener, event_listener);
      js["listeners"].push_back(inner);
    }
    ROCKSDB_JSON_SET_PROP(js, enable_thread_tracking);
    ROCKSDB_JSON_SET_PROP(js, delayed_write_rate);
    ROCKSDB_JSON_SET_PROP(js, enable_pipelined_write);
    ROCKSDB_JSON_SET_PROP(js, unordered_write);
    ROCKSDB_JSON_SET_PROP(js, allow_concurrent_memtable_write);
    ROCKSDB_JSON_SET_PROP(js, enable_write_thread_adaptive_yield);
    ROCKSDB_JSON_SET_SIZE(js, max_write_batch_group_size_bytes);
    ROCKSDB_JSON_SET_PROP(js, write_thread_max_yield_usec);
    ROCKSDB_JSON_SET_PROP(js, write_thread_slow_yield_usec);
    ROCKSDB_JSON_SET_PROP(js, skip_stats_update_on_db_open);
    ROCKSDB_JSON_SET_PROP(js, skip_checking_sst_file_sizes_on_db_open);
    ROCKSDB_JSON_SET_ENUM(js, wal_recovery_mode);
    ROCKSDB_JSON_SET_PROP(js, allow_2pc);
    ROCKSDB_JSON_SET_FACX(js, row_cache, cache);
    //ROCKSDB_JSON_SET_FACT(js, wal_filter);
    ROCKSDB_JSON_SET_PROP(js, fail_if_options_file_error);
    ROCKSDB_JSON_SET_PROP(js, dump_malloc_stats);
    ROCKSDB_JSON_SET_PROP(js, avoid_flush_during_recovery);
    ROCKSDB_JSON_SET_PROP(js, avoid_flush_during_shutdown);
    ROCKSDB_JSON_SET_PROP(js, allow_ingest_behind);
    ROCKSDB_JSON_SET_PROP(js, preserve_deletes);
    ROCKSDB_JSON_SET_PROP(js, two_write_queues);
    ROCKSDB_JSON_SET_PROP(js, manual_wal_flush);
    ROCKSDB_JSON_SET_PROP(js, atomic_flush);
    ROCKSDB_JSON_SET_PROP(js, avoid_unnecessary_blocking_io);
    ROCKSDB_JSON_SET_PROP(js, write_dbid_to_manifest);
    ROCKSDB_JSON_SET_SIZE(js, log_readahead_size);
    ROCKSDB_JSON_SET_FACT(js, file_checksum_gen_factory);
    ROCKSDB_JSON_SET_PROP(js, best_efforts_recovery);
  }
};
static shared_ptr<DBOptions>
NewDBOptionsJS(const json& js, const JsonPluginRepo& repo) {
  return std::make_shared<DBOptions_Json>(js, repo);
}
ROCKSDB_FACTORY_REG("DBOptions", NewDBOptionsJS);
struct DBOptions_Manip : PluginManipFunc<DBOptions> {
  void Update(DBOptions* p, const json& js, const JsonPluginRepo& repo)
  const final {
    static_cast<DBOptions_Json*>(p)->Update(js, repo);
  }
  std::string ToString(const DBOptions& x, const json& dump_options,
                       const JsonPluginRepo& repo) const final {
    json djs;
    bool html = JsonSmartBool(dump_options, "html");
    static_cast<const DBOptions_Json&>(x).SaveToJson(djs, repo, html);
    return JsonToString(djs, dump_options);
  }
};
static const PluginManipFunc<DBOptions>*
JS_DBOptionsManip(const json&, const JsonPluginRepo&) {
  static const DBOptions_Manip manip;
  return &manip;
}
ROCKSDB_FACTORY_REG("DBOptions", JS_DBOptionsManip);

///////////////////////////////////////////////////////////////////////////
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
  void SaveToJson(json& js) const {
    ROCKSDB_JSON_SET_PROP(js, max_table_files_size);
    ROCKSDB_JSON_SET_PROP(js, allow_compaction);
  }
};
CompactionOptionsFIFO_Json NestForBase(const CompactionOptionsFIFO&);

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
  void SaveToJson(json& js) const {
    ROCKSDB_JSON_SET_PROP(js, size_ratio);
    ROCKSDB_JSON_SET_PROP(js, min_merge_width);
    ROCKSDB_JSON_SET_PROP(js, max_merge_width);
    ROCKSDB_JSON_SET_PROP(js, max_size_amplification_percent);
    ROCKSDB_JSON_SET_PROP(js, compression_size_percent);
    ROCKSDB_JSON_SET_ENUM(js, stop_style);
    ROCKSDB_JSON_SET_PROP(js, allow_trivial_move);
  }
};
CompactionOptionsUniversal_Json NestForBase(const CompactionOptionsUniversal&);

struct CompressionOptions_Json : CompressionOptions {
  explicit CompressionOptions_Json(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, window_bits);
    ROCKSDB_JSON_OPT_PROP(js, level);
    ROCKSDB_JSON_OPT_PROP(js, strategy);
    ROCKSDB_JSON_OPT_SIZE(js, max_dict_bytes);
    ROCKSDB_JSON_OPT_PROP(js, zstd_max_train_bytes);
    ROCKSDB_JSON_OPT_PROP(js, parallel_threads);
    ROCKSDB_JSON_OPT_PROP(js, enabled);
  }
  void SaveToJson(json& js) const {
    ROCKSDB_JSON_SET_PROP(js, window_bits);
    ROCKSDB_JSON_SET_PROP(js, level);
    ROCKSDB_JSON_SET_PROP(js, strategy);
    ROCKSDB_JSON_SET_SIZE(js, max_dict_bytes);
    ROCKSDB_JSON_SET_PROP(js, zstd_max_train_bytes);
    ROCKSDB_JSON_SET_PROP(js, parallel_threads);
    ROCKSDB_JSON_SET_PROP(js, enabled);
  }
};
CompressionOptions_Json NestForBase(const CompressionOptions&);

struct ColumnFamilyOptions_Json : ColumnFamilyOptions {
  ColumnFamilyOptions_Json(const json& js, const JsonPluginRepo& repo) {
    Update(js, repo);
  }
  void Update(const json& js, const JsonPluginRepo& repo) {
    ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_number);
    ROCKSDB_JSON_OPT_PROP(js, min_write_buffer_number_to_merge);
    ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_number_to_maintain);
    ROCKSDB_JSON_OPT_SIZE(js, max_write_buffer_size_to_maintain);
    ROCKSDB_JSON_OPT_PROP(js, inplace_update_support);
    ROCKSDB_JSON_OPT_PROP(js, inplace_update_num_locks);
    // ROCKSDB_JSON_OPT_PROP(js, inplace_callback); // not need update
    ROCKSDB_JSON_OPT_PROP(js, memtable_prefix_bloom_size_ratio);
    ROCKSDB_JSON_OPT_PROP(js, memtable_whole_key_filtering);
    ROCKSDB_JSON_OPT_PROP(js, memtable_huge_page_size);
    ROCKSDB_JSON_OPT_FACT(js, memtable_insert_with_hint_prefix_extractor);
    ROCKSDB_JSON_OPT_PROP(js, bloom_locality);
    ROCKSDB_JSON_OPT_SIZE(js, arena_block_size);
    { // compression_per_level is an enum array
      auto iter = js.find("compression_per_level");
      if (js.end() != iter) {
        if (!iter.value().is_array()) {
          THROW_InvalidArgument("compression_per_level must be an array");
        }
        for (auto& item : iter.value().items()) {
          const string& val = item.value().get_ref<const string&>();
          CompressionType compressionType;
          if (!enum_value(val, &compressionType)) {
            THROW_InvalidArgument("compression_per_level: invalid enum: " + val);
          }
          compression_per_level.push_back(compressionType);
        }
      }
    }
    ROCKSDB_JSON_OPT_PROP(js, num_levels);
    ROCKSDB_JSON_OPT_PROP(js, level0_slowdown_writes_trigger);
    ROCKSDB_JSON_OPT_PROP(js, level0_stop_writes_trigger);
    ROCKSDB_JSON_OPT_SIZE(js, target_file_size_base);
    ROCKSDB_JSON_OPT_PROP(js, target_file_size_multiplier);
    ROCKSDB_JSON_OPT_PROP(js, level_compaction_dynamic_level_bytes);
    ROCKSDB_JSON_OPT_PROP(js, max_bytes_for_level_multiplier);
    {
      auto iter = js.find("max_bytes_for_level_multiplier_additional");
      if (js.end() != iter)
        if (!Init_vec(iter.value(), max_bytes_for_level_multiplier_additional))
          THROW_InvalidArgument(
              "max_bytes_for_level_multiplier_additional must be an int vector");
    }
    ROCKSDB_JSON_OPT_SIZE(js, max_compaction_bytes);
    ROCKSDB_JSON_OPT_SIZE(js, soft_pending_compaction_bytes_limit);
    ROCKSDB_JSON_OPT_SIZE(js, hard_pending_compaction_bytes_limit);
    ROCKSDB_JSON_OPT_ENUM(js, compaction_style);
    ROCKSDB_JSON_OPT_PROP(js, compaction_pri);
    ROCKSDB_JSON_OPT_NEST(js, compaction_options_universal);
    ROCKSDB_JSON_OPT_NEST(js, compaction_options_fifo);
    ROCKSDB_JSON_OPT_PROP(js, max_sequential_skip_in_iterations);
    ROCKSDB_JSON_OPT_FACT(js, memtable_factory);
    {
      auto iter = js.find("table_properties_collector_factories");
      if (js.end() != iter) {
        if (!iter.value().is_array()) {
          THROW_InvalidArgument(
              "table_properties_collector_factories must be an array");
        }
        decltype(table_properties_collector_factories) vec;
        for (auto& item : iter.value().items()) {
          decltype(vec)::value_type p;
          ROCKSDB_JSON_OPT_FACT_INNER(item.value(), p);
          vec.push_back(p);
        }
        table_properties_collector_factories.swap(vec);
      }
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
    ROCKSDB_JSON_OPT_SIZE(js, write_buffer_size);
    ROCKSDB_JSON_OPT_ENUM(js, compression);
    ROCKSDB_JSON_OPT_ENUM(js, bottommost_compression);
    ROCKSDB_JSON_OPT_NEST(js, bottommost_compression_opts);
    ROCKSDB_JSON_OPT_NEST(js, compression_opts);
    ROCKSDB_JSON_OPT_PROP(js, level0_file_num_compaction_trigger);
    ROCKSDB_JSON_OPT_FACT(js, prefix_extractor);
    ROCKSDB_JSON_OPT_SIZE(js, max_bytes_for_level_base);
    ROCKSDB_JSON_OPT_PROP(js, snap_refresh_nanos);
    ROCKSDB_JSON_OPT_PROP(js, disable_auto_compactions);
    ROCKSDB_JSON_OPT_FACT(js, table_factory);
    {
      auto iter = js.find("cf_paths");
      if (js.end() != iter) Json_DbPathVec(js, cf_paths);
    }
    ROCKSDB_JSON_OPT_FACT(js, compaction_thread_limiter);
  }

  void SaveToJson(json& js, const JsonPluginRepo& repo, bool html) const {
    ROCKSDB_JSON_SET_PROP(js, max_write_buffer_number);
    ROCKSDB_JSON_SET_PROP(js, min_write_buffer_number_to_merge);
    ROCKSDB_JSON_SET_PROP(js, max_write_buffer_number_to_maintain);
    ROCKSDB_JSON_SET_SIZE(js, max_write_buffer_size_to_maintain);
    ROCKSDB_JSON_SET_PROP(js, inplace_update_support);
    ROCKSDB_JSON_SET_PROP(js, inplace_update_num_locks);
    // ROCKSDB_JSON_SET_PROP(js, inplace_callback); // not need update
    ROCKSDB_JSON_SET_PROP(js, memtable_prefix_bloom_size_ratio);
    ROCKSDB_JSON_SET_PROP(js, memtable_whole_key_filtering);
    ROCKSDB_JSON_SET_PROP(js, memtable_huge_page_size);
    ROCKSDB_JSON_SET_FACX(js, memtable_insert_with_hint_prefix_extractor,
                          slice_transform);
    ROCKSDB_JSON_SET_PROP(js, bloom_locality);
    ROCKSDB_JSON_SET_SIZE(js, arena_block_size);
    auto& js_compression_per_level = js["compression_per_level"];
    for (auto one_enum : compression_per_level) {
      js_compression_per_level.push_back(enum_stdstr(one_enum));
    }
    ROCKSDB_JSON_SET_PROP(js, num_levels);
    ROCKSDB_JSON_SET_PROP(js, level0_slowdown_writes_trigger);
    ROCKSDB_JSON_SET_PROP(js, level0_stop_writes_trigger);
    ROCKSDB_JSON_SET_SIZE(js, target_file_size_base);
    ROCKSDB_JSON_SET_PROP(js, target_file_size_multiplier);
    ROCKSDB_JSON_SET_PROP(js, level_compaction_dynamic_level_bytes);
    ROCKSDB_JSON_SET_PROP(js, max_bytes_for_level_multiplier);
    ROCKSDB_JSON_SET_PROP(js, max_bytes_for_level_multiplier_additional);
    ROCKSDB_JSON_SET_SIZE(js, max_compaction_bytes);
    ROCKSDB_JSON_SET_SIZE(js, soft_pending_compaction_bytes_limit);
    ROCKSDB_JSON_SET_SIZE(js, hard_pending_compaction_bytes_limit);
    ROCKSDB_JSON_SET_ENUM(js, compaction_style);
    ROCKSDB_JSON_SET_PROP(js, compaction_pri);
    ROCKSDB_JSON_SET_NEST(js, compaction_options_universal);
    ROCKSDB_JSON_SET_NEST(js, compaction_options_fifo);
    ROCKSDB_JSON_SET_PROP(js, max_sequential_skip_in_iterations);
    ROCKSDB_JSON_SET_FACX(js, memtable_factory, mem_table_rep_factory);
    for (auto& table_properties_collector_factory :
               table_properties_collector_factories) {
      json inner;
      ROCKSDB_JSON_SET_FACT_INNER(inner,
                                  table_properties_collector_factory,
                                  table_properties_collector_factory);
      js["table_properties_collector_factories"].push_back(inner);
    }
    ROCKSDB_JSON_SET_PROP(js, max_successive_merges);
    ROCKSDB_JSON_SET_PROP(js, optimize_filters_for_hits);
    ROCKSDB_JSON_SET_PROP(js, paranoid_file_checks);
    ROCKSDB_JSON_SET_PROP(js, force_consistency_checks);
    ROCKSDB_JSON_SET_PROP(js, report_bg_io_stats);
    ROCKSDB_JSON_SET_PROP(js, ttl);
    ROCKSDB_JSON_SET_PROP(js, periodic_compaction_seconds);
    ROCKSDB_JSON_SET_PROP(js, sample_for_compression);
    ROCKSDB_JSON_SET_PROP(js, max_mem_compaction_level);
    ROCKSDB_JSON_SET_PROP(js, soft_rate_limit);
    ROCKSDB_JSON_SET_PROP(js, hard_rate_limit);
    ROCKSDB_JSON_SET_PROP(js, rate_limit_delay_max_milliseconds);
    ROCKSDB_JSON_SET_PROP(js, purge_redundant_kvs_while_flush);
    // ------- ColumnFamilyOptions specific --------------------------
    ROCKSDB_JSON_SET_FACT(js, comparator);
    ROCKSDB_JSON_SET_FACT(js, merge_operator);
    // ROCKSDB_JSON_OPT_FACT(js, compaction_filter);
    ROCKSDB_JSON_SET_FACT(js, compaction_filter_factory);
    ROCKSDB_JSON_SET_SIZE(js, write_buffer_size);
    ROCKSDB_JSON_SET_ENUM(js, compression);
    ROCKSDB_JSON_SET_ENUM(js, bottommost_compression);
    ROCKSDB_JSON_SET_NEST(js, bottommost_compression_opts);
    ROCKSDB_JSON_SET_NEST(js, compression_opts);
    ROCKSDB_JSON_SET_PROP(js, level0_file_num_compaction_trigger);
    ROCKSDB_JSON_SET_FACX(js, prefix_extractor, slice_transform);
    ROCKSDB_JSON_SET_SIZE(js, max_bytes_for_level_base);
    ROCKSDB_JSON_SET_PROP(js, snap_refresh_nanos);
    ROCKSDB_JSON_SET_PROP(js, disable_auto_compactions);
    ROCKSDB_JSON_SET_FACT(js, table_factory);
    if (!cf_paths.empty()) {
      json& cf_paths_js = js["cf_paths"];
      for (auto& x : cf_paths) {
        cf_paths_js.push_back(DbPathToJson(x));
      }
      if (html)
        cf_paths_js[0]["<htmltab:col>"] = 1;
    }
    ROCKSDB_JSON_SET_FACT(js, compaction_thread_limiter);
  }
};
using CFOptions = ColumnFamilyOptions;
using CFOptions_Json = ColumnFamilyOptions_Json;

static shared_ptr<ColumnFamilyOptions>
NewCFOptionsJS(const json& js, const JsonPluginRepo& repo) {
  return std::make_shared<ColumnFamilyOptions_Json>(js, repo);
}
ROCKSDB_FACTORY_REG("ColumnFamilyOptions", NewCFOptionsJS);
ROCKSDB_FACTORY_REG("CFOptions", NewCFOptionsJS);
struct CFOptions_Manip : PluginManipFunc<ColumnFamilyOptions> {
  void Update(ColumnFamilyOptions* p, const json& js,
              const JsonPluginRepo& repo) const final {
    static_cast<ColumnFamilyOptions_Json*>(p)->Update(js, repo);
  }
  std::string ToString(const ColumnFamilyOptions& x, const json& dump_options,
                       const JsonPluginRepo& repo) const final {
    json djs;
    bool html = JsonSmartBool(dump_options, "html");
    static_cast<const ColumnFamilyOptions_Json&>(x).SaveToJson(djs, repo, html);
    return JsonToString(djs, dump_options);
  }
};
static const PluginManipFunc<ColumnFamilyOptions>*
JS_CFOptionsManip(const json&, const JsonPluginRepo&) {
  static const CFOptions_Manip manip;
  return &manip;
}
ROCKSDB_FACTORY_REG("ColumnFamilyOptions", JS_CFOptionsManip);
ROCKSDB_FACTORY_REG("CFOptions", JS_CFOptionsManip);

//////////////////////////////////////////////////////////////////////////////

static shared_ptr<TablePropertiesCollectorFactory>
NewCompactOnDeletionCollectorFactoryForJson(
    const json& js, const JsonPluginRepo&) {
  size_t sliding_window_size = 0;
  size_t deletion_trigger = 0;
  double deletion_ratio = 0;
  ROCKSDB_JSON_REQ_PROP(js, sliding_window_size);
  ROCKSDB_JSON_REQ_PROP(js, deletion_trigger);
  ROCKSDB_JSON_OPT_PROP(js, deletion_ratio);  // this is optional
  return NewCompactOnDeletionCollectorFactory(sliding_window_size,
                                              deletion_trigger, deletion_ratio);
}
ROCKSDB_FACTORY_REG("CompactOnDeletionCollector",
                    NewCompactOnDeletionCollectorFactoryForJson);

//----------------------------------------------------------------------------
// SerDe example for TablePropertiesCollector
struct MySerDe : SerDeFunc<TablePropertiesCollector> {
  Status Serialize(const TablePropertiesCollector&, std::string* /*output*/)
  const override {
    // do serialize
    return Status::OK();
  }
  Status DeSerialize(TablePropertiesCollector*, const Slice& /*input*/)
  const override {
    // do deserialize
    return Status::OK();
  }
};
static const SerDeFunc<TablePropertiesCollector>*
CreateMySerDe(const json&,const JsonPluginRepo&) {
  static MySerDe serde;
  return &serde;
}
//ROCKSDB_FACTORY_REG("CompactOnDeletionCollector", CreateMySerDe);
ROCKSDB_FACTORY_REG("MySerDe", CreateMySerDe);

void ExampleUseMySerDe(const string& clazz) {
  // SerDe should have same class name(clazz) with PluginFactory
  Status s;
  const SerDeFunc<TablePropertiesCollector>* serde =
      SerDeFactory<TablePropertiesCollector>::AcquirePlugin(
          clazz, json{}, JsonPluginRepo{});
  auto factory =
      PluginFactorySP<TablePropertiesCollectorFactory>::AcquirePlugin(
          clazz, json{}, JsonPluginRepo{});
  auto instance = factory->CreateTablePropertiesCollector({});
  std::string bytes;
  s = serde->Serialize(*instance, &bytes);
  // send bytes ...
  // recv bytes ...
  s = serde->DeSerialize(&*instance, bytes);
}
// end. SerDe example for TablePropertiesCollector

//////////////////////////////////////////////////////////////////////////////

static shared_ptr<RateLimiter>
JS_NewGenericRateLimiter(const json& js, const JsonPluginRepo& repo) {
  int64_t rate_bytes_per_sec = 0;
  int64_t refill_period_us = 100 * 1000;
  int32_t fairness = 10;
  RateLimiter::Mode mode = RateLimiter::Mode::kWritesOnly;
  bool auto_tuned = false;
  ROCKSDB_JSON_REQ_SIZE(js, rate_bytes_per_sec); // required
  ROCKSDB_JSON_OPT_PROP(js, refill_period_us);
  ROCKSDB_JSON_OPT_PROP(js, fairness);
  ROCKSDB_JSON_OPT_ENUM(js, mode);
  ROCKSDB_JSON_OPT_PROP(js, auto_tuned);
  if (rate_bytes_per_sec <= 0) {
    THROW_InvalidArgument("rate_bytes_per_sec must > 0");
  }
  if (refill_period_us <= 0) {
    THROW_InvalidArgument("refill_period_us must > 0");
  }
  if (fairness <= 0) {
    THROW_InvalidArgument("fairness must > 0");
  }
  Env* env = Env::Default();
  auto iter = js.find("env");
  if (js.end() != iter) {
    const auto& env_js = iter.value();
    env = PluginFactory<Env*>::GetPlugin("env", ROCKSDB_FUNC, env_js, repo);
    if (!env)
      THROW_InvalidArgument("param env is specified but got null");
  }
  return std::make_shared<GenericRateLimiter>(
      rate_bytes_per_sec, refill_period_us, fairness,
      mode, env, auto_tuned);
}
ROCKSDB_FACTORY_REG("GenericRateLimiter", JS_NewGenericRateLimiter);

//////////////////////////////////////////////////////////////////////////////
struct JemallocAllocatorOptions_Json : JemallocAllocatorOptions {
  JemallocAllocatorOptions_Json(const json& js, const JsonPluginRepo&) {
    ROCKSDB_JSON_OPT_PROP(js, limit_tcache_size);
    ROCKSDB_JSON_OPT_SIZE(js, tcache_size_lower_bound);
    ROCKSDB_JSON_OPT_SIZE(js, tcache_size_upper_bound);
  }
  json ToJson(const JsonPluginRepo&) {
    json js;
    ROCKSDB_JSON_SET_PROP(js, limit_tcache_size);
    ROCKSDB_JSON_SET_SIZE(js, tcache_size_lower_bound);
    ROCKSDB_JSON_SET_SIZE(js, tcache_size_upper_bound);
    return js;
  }
};
std::shared_ptr<MemoryAllocator>
JS_NewJemallocNodumpAllocator(const json& js, const JsonPluginRepo& repo) {
  JemallocAllocatorOptions_Json opt(js, repo);
  std::shared_ptr<MemoryAllocator> p;
  Status s = NewJemallocNodumpAllocator(opt, &p);
  if (!s.ok()) {
    throw s;
  }
  return p;
}
ROCKSDB_FACTORY_REG("JemallocNodumpAllocator", JS_NewJemallocNodumpAllocator);
#if defined(MEMKIND)
std::shared_ptr<MemoryAllocator>
JS_NewMemkindKmemAllocator(const json&, const JsonPluginRepo&) {
  return std::make_shared<MemkindKmemAllocator>();
}
ROCKSDB_FACTORY_REG("MemkindKmemAllocator", JS_NewMemkindKmemAllocator);
#endif

struct LRUCacheOptions_Json : LRUCacheOptions {
  LRUCacheOptions_Json(const json& js, const JsonPluginRepo& repo) {
    ROCKSDB_JSON_REQ_SIZE(js, capacity);
    ROCKSDB_JSON_OPT_PROP(js, num_shard_bits);
    ROCKSDB_JSON_OPT_PROP(js, strict_capacity_limit);
    ROCKSDB_JSON_OPT_PROP(js, high_pri_pool_ratio);
    ROCKSDB_JSON_OPT_FACT(js, memory_allocator);
    ROCKSDB_JSON_OPT_PROP(js, use_adaptive_mutex);
    ROCKSDB_JSON_OPT_ENUM(js, metadata_charge_policy);
  }
  json ToJson(const JsonPluginRepo& repo, bool html) const {
    json js;
    ROCKSDB_JSON_SET_SIZE(js, capacity);
    ROCKSDB_JSON_SET_PROP(js, num_shard_bits);
    ROCKSDB_JSON_SET_PROP(js, strict_capacity_limit);
    ROCKSDB_JSON_SET_PROP(js, high_pri_pool_ratio);
    ROCKSDB_JSON_SET_FACT(js, memory_allocator);
    ROCKSDB_JSON_SET_PROP(js, use_adaptive_mutex);
    ROCKSDB_JSON_SET_ENUM(js, metadata_charge_policy);
    return js;
  }
};
static std::shared_ptr<Cache>
JS_NewLRUCache(const json& js, const JsonPluginRepo& repo) {
  return NewLRUCache(LRUCacheOptions_Json(js, repo));
}
ROCKSDB_FACTORY_REG("LRUCache", JS_NewLRUCache);

struct LRUCache_Manip : PluginManipFunc<Cache> {
  void Update(Cache* p, const json& js, const JsonPluginRepo& repo)
  const override {

  }

  string ToString(const Cache& r, const json& dump_options, const JsonPluginRepo& repo)
  const override {
    bool html = JsonSmartBool(dump_options, "html");
    auto& p2name = repo.m_impl->cache.p2name;
    auto iter = p2name.find((Cache*)&r);
    json js;
    if (p2name.end() != iter) {
      js = iter->second.params;
    }
    size_t usage = r.GetUsage();
    size_t pined_usage = r.GetPinnedUsage();
    size_t capacity = r.GetCapacity();
    size_t strict_capacity = r.HasStrictCapacityLimit();
    double usage_rate = 1.0*usage / capacity;
    double pined_rate = 1.0*pined_usage / capacity;
    MemoryAllocator* memory_allocator = r.memory_allocator();
    ROCKSDB_JSON_SET_SIZE(js, usage);
    ROCKSDB_JSON_SET_SIZE(js, pined_usage);
    ROCKSDB_JSON_SET_SIZE(js, capacity);
    ROCKSDB_JSON_SET_PROP(js, strict_capacity);
    ROCKSDB_JSON_SET_PROP(js, usage_rate);
    ROCKSDB_JSON_SET_PROP(js, pined_rate);
    ROCKSDB_JSON_SET_FACT(js, memory_allocator);
    return JsonToString(js, dump_options);
  }
};
static const PluginManipFunc<Cache>*
JS_LRUCache_Manip(const json&, const JsonPluginRepo&) {
  static const LRUCache_Manip manip;
  return &manip;
}
ROCKSDB_FACTORY_REG("LRUCache", JS_LRUCache_Manip);

//////////////////////////////////////////////////////////////////////////////
static std::shared_ptr<Cache>
JS_NewClockCache(const json& js, const JsonPluginRepo& repo) {
#ifdef SUPPORT_CLOCK_CACHE
  LRUCacheOptions_Json opt(js, repo); // similar with ClockCache param
  auto p = NewClockCache(opt.capacity, opt.num_shard_bits,
                         opt.strict_capacity_limit, opt.metadata_charge_policy);
  if (nullptr != p) {
	THROW_InvalidArgument(
		"SUPPORT_CLOCK_CACHE is defined but NewClockCache returns null");
  }
  return p;
#else
  (void)js;
  (void)repo;
  THROW_InvalidArgument(
      "SUPPORT_CLOCK_CACHE is not defined, "
      "need to recompile with -D SUPPORT_CLOCK_CACHE=1");
#endif
}
ROCKSDB_FACTORY_REG("ClockCache", JS_NewClockCache);

//////////////////////////////////////////////////////////////////////////////
static std::shared_ptr<const SliceTransform>
JS_NewFixedPrefixTransform(const json& js, const JsonPluginRepo&) {
  size_t prefix_len = 0;
  ROCKSDB_JSON_REQ_PROP(js, prefix_len);
  return std::shared_ptr<const SliceTransform>(
      NewFixedPrefixTransform(prefix_len));
}
ROCKSDB_FACTORY_REG("FixedPrefixTransform", JS_NewFixedPrefixTransform);

//////////////////////////////////////////////////////////////////////////////
static std::shared_ptr<const SliceTransform>
JS_NewCappedPrefixTransform(const json& js, const JsonPluginRepo&) {
  size_t cap_len = 0;
  ROCKSDB_JSON_REQ_PROP(js, cap_len);
  return std::shared_ptr<const SliceTransform>(
      NewCappedPrefixTransform(cap_len));
}
ROCKSDB_FACTORY_REG("CappedPrefixTransform", JS_NewCappedPrefixTransform);

//////////////////////////////////////////////////////////////////////////////
static const Comparator*
BytewiseComp(const json&, const JsonPluginRepo&) {
  return BytewiseComparator();
}
static const Comparator*
RevBytewiseComp(const json&, const JsonPluginRepo&) {
  return ReverseBytewiseComparator();
}
ROCKSDB_FACTORY_REG(                   "default", BytewiseComp);
ROCKSDB_FACTORY_REG(                   "Default", BytewiseComp);
ROCKSDB_FACTORY_REG(                  "bytewise", BytewiseComp);
ROCKSDB_FACTORY_REG(                  "Bytewise", BytewiseComp);
ROCKSDB_FACTORY_REG(        "BytewiseComparator", BytewiseComp);
ROCKSDB_FACTORY_REG("leveldb.BytewiseComparator", BytewiseComp);
ROCKSDB_FACTORY_REG(        "ReverseBytewise", RevBytewiseComp);
ROCKSDB_FACTORY_REG(        "ReverseBytewiseComparator", RevBytewiseComp);
ROCKSDB_FACTORY_REG("leveldb.ReverseBytewiseComparator", RevBytewiseComp);

//////////////////////////////////////////////////////////////////////////////
static Env* DefaultEnv(const json&, const JsonPluginRepo&) {
  return Env::Default();
}
ROCKSDB_FACTORY_REG("default", DefaultEnv);

/////////////////////////////////////////////////////////////////////////////
static shared_ptr<FlushBlockBySizePolicyFactory>
NewFlushBlockBySizePolicyFactoryFactoryJson(const json&,
                                            const JsonPluginRepo&) {
  return std::make_shared<FlushBlockBySizePolicyFactory>();
}
ROCKSDB_FACTORY_REG("FlushBlockBySize",
                    NewFlushBlockBySizePolicyFactoryFactoryJson);

/////////////////////////////////////////////////////////////////////////////
static shared_ptr<FileChecksumGenFactory>
GetFileChecksumGenCrc32cFactoryJson(const json&,
                                    const JsonPluginRepo&) {
  return GetFileChecksumGenCrc32cFactory();
}
ROCKSDB_FACTORY_REG("Crc32c", GetFileChecksumGenCrc32cFactoryJson);
ROCKSDB_FACTORY_REG("crc32c", GetFileChecksumGenCrc32cFactoryJson);

/////////////////////////////////////////////////////////////////////////////
static shared_ptr<MemTableRepFactory>
NewSkipListMemTableRepFactoryJson(const json& js, const JsonPluginRepo&) {
  size_t lookahead = 0;
  ROCKSDB_JSON_OPT_PROP(js, lookahead);
  return std::make_shared<SkipListFactory>(lookahead);
}
ROCKSDB_FACTORY_REG("SkipListRep", NewSkipListMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("SkipList", NewSkipListMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("skiplist", NewSkipListMemTableRepFactoryJson);

static shared_ptr<MemTableRepFactory>
NewVectorMemTableRepFactoryJson(const json& js, const JsonPluginRepo&) {
  size_t count = 0;
  ROCKSDB_JSON_OPT_PROP(js, count);
  return std::make_shared<VectorRepFactory>(count);
}
ROCKSDB_FACTORY_REG("VectorRep", NewVectorMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("Vector", NewVectorMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("vector", NewVectorMemTableRepFactoryJson);

static shared_ptr<MemTableRepFactory>
NewHashSkipListMemTableRepFactoryJson(const json& js, const JsonPluginRepo&) {
  size_t bucket_count = 1000000;
  int32_t height = 4;
  int32_t branching_factor = 4;
  ROCKSDB_JSON_OPT_PROP(js, bucket_count);
  ROCKSDB_JSON_OPT_PROP(js, height);
  ROCKSDB_JSON_OPT_PROP(js, branching_factor);
  return shared_ptr<MemTableRepFactory>(
      NewHashSkipListRepFactory(bucket_count, height, branching_factor));
}
ROCKSDB_FACTORY_REG("HashSkipListRep", NewHashSkipListMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("HashSkipList", NewHashSkipListMemTableRepFactoryJson);

static shared_ptr<MemTableRepFactory>
NewHashLinkListMemTableRepFactoryJson(const json& js, const JsonPluginRepo&) {
  size_t bucket_count = 50000;
  size_t huge_page_tlb_size = 0;
  int bucket_entries_logging_threshold = 4096;
  bool if_log_bucket_dist_when_flash = true;
  uint32_t threshold_use_skiplist = 256;
  ROCKSDB_JSON_OPT_PROP(js, bucket_count);
  ROCKSDB_JSON_OPT_SIZE(js, huge_page_tlb_size);
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
ROCKSDB_FACTORY_REG("HashLinkListRep", NewHashLinkListMemTableRepFactoryJson);
ROCKSDB_FACTORY_REG("HashLinkList", NewHashLinkListMemTableRepFactoryJson);

/////////////////////////////////////////////////////////////////////////////
// OpenDB implementations
//
const char* db_options_class = "DBOptions";
const char* cf_options_class = "CFOptions";
template<class Ptr>
Ptr ObtainOPT(JsonPluginRepo::Impl::ObjMap<Ptr>& field,
              const char* option_class, // "DBOptions" or "CFOptions"
              const json& option_js, const JsonPluginRepo& repo) {
  if (option_js.is_string()) {
    const std::string& option_name = option_js.get_ref<const std::string&>();
    auto iter = field.name2p->find(option_name);
    if (field.name2p->end() == iter) {
        THROW_NotFound("option_name = \"" + option_name + "\"");
    }
    return iter->second;
  }
  if (!option_js.is_object()) {
    THROW_InvalidArgument(
        "option_js must be string or object, but is: " + option_js.dump());
  }
  return PluginFactory<Ptr>::AcquirePlugin(option_class, option_js, repo);
}
#define ROCKSDB_OBTAIN_OPT(field, option_js, repo) \
   ObtainOPT(repo.m_impl->field, field##_class, option_js, repo)

Options JS_Options(const json& js, const JsonPluginRepo& repo, string* name) {
  if (!js.is_object()) {
    THROW_InvalidArgument( "param json must be an object");
  }
  auto iter = js.find("name");
  if (js.end() == iter) {
    THROW_InvalidArgument("missing param \"name\"");
  }
  *name = iter.value().get_ref<const std::string&>();
  iter = js.find("options");
  if (js.end() == iter) {
    iter = js.find("db_options");
    if (js.end() == iter) {
      THROW_InvalidArgument("missing param \"db_options\"");
    }
    auto& db_options_js = iter.value();
    iter = js.find("cf_options");
    if (js.end() == iter) {
      THROW_InvalidArgument("missing param \"cf_options\"");
    }
    auto& cf_options_js = iter.value();
    auto db_options = ROCKSDB_OBTAIN_OPT(db_options, db_options_js, repo);
    auto cf_options = ROCKSDB_OBTAIN_OPT(cf_options, cf_options_js, repo);
    return Options(*db_options, *cf_options);
  }
  auto& js_options = iter.value();
  DBOptions_Json db_options(js_options, repo);
  ColumnFamilyOptions_Json cf_options(js_options, repo);
  return Options(db_options, cf_options);
}

struct DB_Manip : PluginManipFunc<DB> {
  void Update(DB* db, const json& js,
              const JsonPluginRepo& repo) const final {

  }
  std::string ToString(const DB& db, const json& dump_options,
                       const JsonPluginRepo& repo) const final {
    Options opt = db.GetOptions();
    auto& dbo = static_cast<DBOptions_Json&>(static_cast<DBOptions&>(opt));
    auto& cfo = static_cast<CFOptions_Json&>(static_cast<CFOptions&>(opt));
    const auto& dbmap = repo.m_impl->db;
    json djs;
    std::string dbo_name, cfo_name;
    const std::string& dbname = db.GetName();
    auto i1 = dbmap.p2name.find((DB*)&db);
    if (dbmap.p2name.end() == i1) {
      THROW_NotFound("db ptr is not registered in repo, dbname = " + dbname);
    }
    auto ijs = i1->second.params.find("params");
    if (i1->second.params.end() == ijs) {
      THROW_Corruption("p2name[" + dbname + "].params is missing");
    }
    const json& params_js = ijs.value();
    if (params_js.end() != (ijs = params_js.find("options"))) {
      dbo_name = "json varname: " + ijs.value().get_ref<const std::string&>() + "(Options)";
      cfo_name = "json varname: " + ijs.value().get_ref<const std::string&>() + "(Options)";
    } else {
      if (params_js.end() != (ijs = params_js.find("db_options"))) {
        if (ijs.value().is_string())
          dbo_name = "json varname: " + ijs.value().get_ref<const std::string&>();
      } else {
        THROW_Corruption("p2name[" + dbname + "].params[db_options|options] are all missing");
      }
      if (params_js.end() != (ijs = params_js.find("cf_options"))) {
        if (ijs.value().is_string())
          cfo_name = "json varname: " + ijs.value().get_ref<const std::string&>();
      } else {
        THROW_Corruption("p2name[" + dbname + "].params[cf_options|options] are all missing");
      }
    }
    bool html = JsonSmartBool(dump_options, "html");
    if (dbo_name.empty()) dbo_name = "json varname: (defined inline)";
    if (cfo_name.empty()) cfo_name = "json varname: (defined inline)";
    djs["DBOptions"][0] = dbo_name; dbo.SaveToJson(djs["DBOptions"][1], repo, html);
    djs["CFOptions"][0] = dbo_name; cfo.SaveToJson(djs["CFOptions"][1], repo, html);
    djs["CFOptions"][1]["MaxMemCompactionLevel"] = const_cast<DB&>(db).MaxMemCompactionLevel();
    djs["CFOptions"][1]["Level0StopWriteTrigger"] = const_cast<DB&>(db).Level0StopWriteTrigger();
    return JsonToString(djs, dump_options);
  }
};
static const PluginManipFunc<DB>*
JS_DB_Manip(const json&, const JsonPluginRepo&) {
  static const DB_Manip manip;
  return &manip;
}

struct DB_MultiCF_Manip : PluginManipFunc<DB_MultiCF> {
  void Update(DB_MultiCF* db, const json& js,
              const JsonPluginRepo& repo) const final {

  }
  std::string ToString(const DB_MultiCF& db, const json& dump_options,
                       const JsonPluginRepo& repo) const final {
    json djs;
    auto dbo = static_cast<DBOptions_Json&&>(db.db->GetDBOptions());
    const auto& dbmap = repo.m_impl->db;
    const std::string& dbname = db.db->GetName();
    auto i1 = dbmap.p2name.find(db.db);
    if (dbmap.p2name.end() == i1) {
      THROW_NotFound("db ptr is not registered in repo, dbname = " + dbname);
    }
    auto ijs = i1->second.params.find("params");
    if (i1->second.params.end() == ijs) {
      THROW_Corruption("p2name[" + dbname + "].params is missing");
    }
    std::string dbo_name;
    const json& params_js = ijs.value();
    if (params_js.end() != (ijs = params_js.find("db_options"))) {
      if (ijs.value().is_string()) {
        dbo_name = "json varname: " + ijs.value().get_ref<const std::string&>();
      }
    } else {
      THROW_Corruption("p2name[" + dbname + "].params[db_options|options] are all missing");
    }
    if (params_js.end() == (ijs = params_js.find("column_families"))) {
      THROW_Corruption("p2name[" + dbname + "].params.column_families are all missing");
    }
    const auto& def_cfo_js = ijs.value();
    bool html = JsonSmartBool(dump_options, "html");
    if (dbo_name.empty()) dbo_name = "json varname: (defined inline)";
    djs["DBOptions"][0] = dbo_name;
    dbo.SaveToJson(djs["DBOptions"][1], repo, html);
    auto& result_cfo_js = djs["CFOptions"];
    for (size_t i = 0; i < db.cf_handles.size(); ++i) {
      ColumnFamilyHandle* cf = db.cf_handles[i];
      ColumnFamilyDescriptor cfd;
      cf->GetDescriptor(&cfd);
      const std::string& cf_name = cfd.name;
      auto cfo = static_cast<CFOptions_Json&>(static_cast<CFOptions&>(cfd.options));
      if (def_cfo_js.end() == (ijs = def_cfo_js.find(cf_name))) {
        THROW_Corruption(dbname + ".params.column_families." + cf_name + " is missing");
      }
      if (ijs.value().is_string()) {
        // find in repo.m_impl->cf_options
        auto cfo_varname = ijs.value().get_ref<const std::string&>();
        auto icf = repo.m_impl->cf_options.name2p->find(cfo_varname);
        if (repo.m_impl->cf_options.name2p->end() == icf) {
          THROW_Corruption("Missing cfo_varname = " + cfo_varname);
        }
        auto picf = repo.m_impl->cf_options.p2name.find(icf->second.get());
        if (repo.m_impl->cf_options.p2name.end() == picf) {
          THROW_Corruption("Missing cfo p2name, cfo_varname = " + cfo_varname);
        }
        result_cfo_js[cf_name][0] = "json varname: " + cfo_varname;
        result_cfo_js[cf_name][1] = picf->second.params;
      }
      else { // ijs point to inline defined CFOptions
        result_cfo_js[cf_name][0] = "json varname: (defined inline)";
        result_cfo_js[cf_name][1] = ijs.value();
      }
      result_cfo_js[cf_name][1]["MaxMemCompactionLevel"] = db.db->MaxMemCompactionLevel(cf);
      result_cfo_js[cf_name][1]["Level0StopWriteTrigger"] = db.db->Level0StopWriteTrigger(cf);
      // overwrite with up to date cfo
      cfo.SaveToJson(result_cfo_js[cf_name][1], repo, html);
    }
    return JsonToString(djs, dump_options);
  }
};
static const PluginManipFunc<DB_MultiCF>*
JS_DB_MultiCF_Manip(const json&, const JsonPluginRepo&) {
  static const DB_MultiCF_Manip manip;
  return &manip;
}

static
DB* JS_DB_Open(const json& js, const JsonPluginRepo& repo) {
  std::string name;
  Options options(JS_Options(js, repo, &name));
  bool read_only = false; // default false
  ROCKSDB_JSON_OPT_PROP(js, read_only);
  DB* db = nullptr;
  Status s;
  if (read_only)
    s = DB::OpenForReadOnly(options, name, &db);
  else
    s = DB::Open(options, name, &db);
  if (!s.ok())
    throw s;
  return db;
}
ROCKSDB_FACTORY_REG("DB::Open", JS_DB_Open);
ROCKSDB_FACTORY_REG("DB::Open", JS_DB_Manip);

static
DB* JS_DB_OpenForReadOnly(const json& js, const JsonPluginRepo& repo) {
  std::string name;
  Options options(JS_Options(js, repo, &name));
  bool error_if_log_file_exist = false; // default
  ROCKSDB_JSON_OPT_PROP(js, error_if_log_file_exist);
  DB* db = nullptr;
  Status s = DB::OpenForReadOnly(options, name, &db, error_if_log_file_exist);
  if (!s.ok())
    throw s;
  return db;
}
ROCKSDB_FACTORY_REG("DB::OpenForReadOnly", JS_DB_OpenForReadOnly);
ROCKSDB_FACTORY_REG("DB::OpenForReadOnly", JS_DB_Manip);

static
DB* JS_DB_OpenAsSecondary(const json& js, const JsonPluginRepo& repo) {
  std::string name, secondary_path;
  ROCKSDB_JSON_REQ_PROP(js, secondary_path);
  Options options(JS_Options(js, repo, &name));
  DB* db = nullptr;
  Status s = DB::OpenAsSecondary(options, name, secondary_path, &db);
  if (!s.ok())
    throw s;
  return db;
}
ROCKSDB_FACTORY_REG("DB::OpenAsSecondary", JS_DB_OpenAsSecondary);
ROCKSDB_FACTORY_REG("DB::OpenAsSecondary", JS_DB_Manip);

std::unique_ptr<DB_MultiCF>
JS_DB_MultiCF_Options(const json& js, const JsonPluginRepo& repo,
                      std::shared_ptr<DBOptions>* db_options,
                      std::vector<ColumnFamilyDescriptor>* cf_descriptors,
                      std::string* name,
                      std::function<void(const json&)> parse_extra = nullptr) {
  if (!js.is_object()) {
    THROW_InvalidArgument("param json must be an object");
  }
  auto iter = js.find("name");
  if (js.end() == iter) {
    THROW_InvalidArgument("missing param \"name\"");
  }
  *name = iter.value().get_ref<const std::string&>();
  iter = js.find("column_families");
  if (js.end() == iter) {
    THROW_InvalidArgument("missing param \"column_families\"");
  }
  auto& js_cf_desc = iter.value();
  iter = js.find("db_options");
  if (js.end() == iter) {
    THROW_InvalidArgument("missing param \"db_options\"");
  }
  auto& db_options_js = iter.value();
  Status s;
  *db_options = ROCKSDB_OBTAIN_OPT(db_options, db_options_js, repo);
  std::unique_ptr<DB_MultiCF> db(new DB_MultiCF);
  for (auto& kv : js_cf_desc.items()) {
    const std::string& cf_name = kv.key();
    auto& cf_js = kv.value();
    auto cf_options = ROCKSDB_OBTAIN_OPT(cf_options, cf_js, repo);
    if (parse_extra) {
      parse_extra(cf_js);
    }
    cf_descriptors->push_back({cf_name, *cf_options});
  }
  if (cf_descriptors->empty()) {
    THROW_InvalidArgument("param \"column_families\" is empty");
  }
  return db;
}

static
DB_MultiCF* JS_DB_MultiCF_Open(const json& js, const JsonPluginRepo& repo) {
  shared_ptr<DBOptions> db_opt;
  std::vector<ColumnFamilyDescriptor> cfdvec;
  string name;
  auto db = JS_DB_MultiCF_Options(js, repo, &db_opt, &cfdvec, &name);
  bool read_only = false; // default false
  ROCKSDB_JSON_OPT_PROP(js, read_only);
  Status s;
  if (read_only)
    s = DB::OpenForReadOnly(
                 *db_opt, name, cfdvec, &db->cf_handles, &db->db);
  else
    s = DB::Open(*db_opt, name, cfdvec, &db->cf_handles, &db->db);
  if (!s.ok())
    throw s;
  if (db->cf_handles.size() != cfdvec.size())
    THROW_Corruption("cf_handles.size() != cfdvec.size()");
  return db.release();
}
ROCKSDB_FACTORY_REG("DB::Open", JS_DB_MultiCF_Open);
ROCKSDB_FACTORY_REG("DB::Open", JS_DB_MultiCF_Manip);

static
DB_MultiCF*
JS_DB_MultiCF_OpenForReadOnly(const json& js, const JsonPluginRepo& repo) {
  shared_ptr<DBOptions> db_opt;
  std::vector<ColumnFamilyDescriptor> cfdvec;
  string name;
  auto db = JS_DB_MultiCF_Options(js, repo, &db_opt, &cfdvec, &name);
  bool error_if_log_file_exist = false; // default is false
  ROCKSDB_JSON_OPT_PROP(js, error_if_log_file_exist);
  Status s = DB::OpenForReadOnly(*db_opt, name, cfdvec, &db->cf_handles,
                           &db->db, error_if_log_file_exist);
  if (!s.ok())
    throw s;
  if (db->cf_handles.size() != cfdvec.size())
    THROW_Corruption("cf_handles.size() != cfdvec.size()");
  return db.release();
}
ROCKSDB_FACTORY_REG("DB::OpenForReadOnly", JS_DB_MultiCF_OpenForReadOnly);
ROCKSDB_FACTORY_REG("DB::OpenForReadOnly", JS_DB_MultiCF_Manip);

static
DB_MultiCF*
JS_DB_MultiCF_OpenAsSecondary(const json& js, const JsonPluginRepo& repo) {
  shared_ptr<DBOptions> db_opt;
  std::vector<ColumnFamilyDescriptor> cfdvec;
  std::string name, secondary_path;
  ROCKSDB_JSON_REQ_PROP(js, secondary_path);
  auto db = JS_DB_MultiCF_Options(js, repo, &db_opt, &cfdvec, &name);
  Status s = DB::OpenAsSecondary(*db_opt, name, secondary_path, cfdvec,
                           &db->cf_handles, &db->db);
  if (!s.ok())
    throw s;
  if (db->cf_handles.size() != cfdvec.size())
    THROW_Corruption("cf_handles.size() != cfdvec.size()");
  return db.release();
}
ROCKSDB_FACTORY_REG("DB::OpenAsSecondary", JS_DB_MultiCF_OpenAsSecondary);
ROCKSDB_FACTORY_REG("DB::OpenAsSecondary", JS_DB_MultiCF_Manip);

/////////////////////////////////////////////////////////////////////////////
// DBWithTTL::Open

static
DB* JS_DBWithTTL_Open(const json& js, const JsonPluginRepo& repo) {
  std::string name;
  Options options(JS_Options(js, repo, &name));
  int32_t ttl = 0; // default 0
  bool read_only = false; // default false
  ROCKSDB_JSON_OPT_PROP(js, ttl);
  ROCKSDB_JSON_OPT_PROP(js, read_only);
  DBWithTTL* db = nullptr;
  Status s = DBWithTTL::Open(options, name, &db, ttl, read_only);
  if (!s.ok())
    throw s;
  return db;
}
ROCKSDB_FACTORY_REG("DBWithTTL::Open", JS_DBWithTTL_Open);
ROCKSDB_FACTORY_REG("DBWithTTL::Open", JS_DB_Manip);

static
DB_MultiCF*
JS_DBWithTTL_MultiCF_Open(const json& js, const JsonPluginRepo& repo) {
  shared_ptr<DBOptions> db_opt;
  std::vector<ColumnFamilyDescriptor> cfdvec;
  std::string name;
  std::vector<int32_t> ttls;
  bool read_only = false;
  ROCKSDB_JSON_OPT_PROP(js, read_only);
  auto parse_ttl = [&ttls](const json& cf_js) {
    int32_t ttl = 0;
    ROCKSDB_JSON_REQ_PROP(cf_js, ttl);
    ttls.push_back(ttl);
  };
  auto db = JS_DB_MultiCF_Options(js, repo, &db_opt, &cfdvec, &name, parse_ttl);
  DBWithTTL* dbptr = nullptr;
  Status s = DBWithTTL::Open(*db_opt, name, cfdvec,
                       &db->cf_handles, &dbptr, ttls, read_only);
  if (!s.ok())
    throw s;
  if (db->cf_handles.size() != cfdvec.size())
    THROW_Corruption("cf_handles.size() != cfdvec.size()");
  db->db = dbptr;
  return db.release();
}
ROCKSDB_FACTORY_REG("DBWithTTL::Open", JS_DBWithTTL_MultiCF_Open);
ROCKSDB_FACTORY_REG("DBWithTTL::Open", JS_DB_MultiCF_Manip);

/////////////////////////////////////////////////////////////////////////////
// TransactionDB::Open
static std::shared_ptr<TransactionDBMutexFactory>
JS_NewTransactionDBMutexFactoryImpl(const json&, const JsonPluginRepo&) {
  return std::make_shared<TransactionDBMutexFactoryImpl>();
}
ROCKSDB_FACTORY_REG("Default", JS_NewTransactionDBMutexFactoryImpl);
ROCKSDB_FACTORY_REG("default", JS_NewTransactionDBMutexFactoryImpl);
ROCKSDB_FACTORY_REG("TransactionDBMutexFactoryImpl", JS_NewTransactionDBMutexFactoryImpl);

struct TransactionDBOptions_Json : TransactionDBOptions {
  TransactionDBOptions_Json(const json& js, const JsonPluginRepo& repo) {
    ROCKSDB_JSON_OPT_PROP(js, max_num_locks);
    ROCKSDB_JSON_OPT_PROP(js, max_num_deadlocks);
    ROCKSDB_JSON_OPT_PROP(js, num_stripes);
    ROCKSDB_JSON_OPT_PROP(js, transaction_lock_timeout);
    ROCKSDB_JSON_OPT_PROP(js, default_lock_timeout);
    ROCKSDB_JSON_OPT_FACT(js, custom_mutex_factory);
    ROCKSDB_JSON_OPT_ENUM(js, write_policy);
    ROCKSDB_JSON_OPT_PROP(js, rollback_merge_operands);
    ROCKSDB_JSON_OPT_PROP(js, skip_concurrency_control);
    ROCKSDB_JSON_OPT_PROP(js, default_write_batch_flush_threshold);
  }
};

TransactionDBOptions
JS_TransactionDBOptions(const json& js, const JsonPluginRepo& repo) {
  auto iter = js.find("txn_db_options");
  if (js.end() == iter) {
    THROW_InvalidArgument("missing required param \"txn_db_options\"");
  }
  return TransactionDBOptions_Json(iter.value(), repo);
}

static
DB* JS_TransactionDB_Open(const json& js, const JsonPluginRepo& repo) {
  std::string name;
  Options options(JS_Options(js, repo, &name));
  TransactionDBOptions trx_db_options(JS_TransactionDBOptions(js, repo));
  TransactionDB* db = nullptr;
  Status s = TransactionDB::Open(options, trx_db_options, name, &db);
  if (!s.ok())
    throw s;
  return db;
}
ROCKSDB_FACTORY_REG("TransactionDB::Open", JS_TransactionDB_Open);
ROCKSDB_FACTORY_REG("TransactionDB::Open", JS_DB_Manip);

static
DB_MultiCF*
JS_TransactionDB_MultiCF_Open(const json& js, const JsonPluginRepo& repo) {
  shared_ptr<DBOptions> db_opt;
  std::vector<ColumnFamilyDescriptor> cfdvec;
  std::string name;
  TransactionDBOptions trx_db_options(JS_TransactionDBOptions(js, repo));
  auto db = JS_DB_MultiCF_Options(js, repo, &db_opt, &cfdvec, &name);
  TransactionDB* dbptr = nullptr;
  Status s = TransactionDB::Open(*db_opt, trx_db_options, name, cfdvec,
                           &db->cf_handles, &dbptr);
  if (!s.ok())
    throw s;
  if (db->cf_handles.size() != cfdvec.size())
    THROW_Corruption("cf_handles.size() != cfdvec.size()");
  db->db = dbptr;
  return db.release();
}
ROCKSDB_FACTORY_REG("TransactionDB::Open", JS_TransactionDB_MultiCF_Open);
ROCKSDB_FACTORY_REG("TransactionDB::Open", JS_DB_MultiCF_Manip);

/////////////////////////////////////////////////////////////////////////////
struct OptimisticTransactionDBOptions_Json: OptimisticTransactionDBOptions {
  OptimisticTransactionDBOptions_Json(const json& js, const JsonPluginRepo&) {
    ROCKSDB_JSON_OPT_ENUM(js, validate_policy);
    ROCKSDB_JSON_OPT_PROP(js, occ_lock_buckets);
  }
};
static
DB* JS_OccTransactionDB_Open(const json& js, const JsonPluginRepo& repo) {
  std::string name;
  Options options(JS_Options(js, repo, &name));
  OptimisticTransactionDB* db = nullptr;
  Status s = OptimisticTransactionDB::Open(options, name, &db);
  if (!s.ok())
    throw s;
  return db;
}
ROCKSDB_FACTORY_REG("OptimisticTransactionDB::Open", JS_OccTransactionDB_Open);
ROCKSDB_FACTORY_REG("OptimisticTransactionDB::Open", JS_DB_Manip);

static
DB_MultiCF*
JS_OccTransactionDB_MultiCF_Open(const json& js, const JsonPluginRepo& repo) {
  shared_ptr<DBOptions> db_opt;
  std::vector<ColumnFamilyDescriptor> cfdvec;
  std::string name;
  auto db = JS_DB_MultiCF_Options(js, repo, &db_opt, &cfdvec, &name);
  OptimisticTransactionDB* dbptr = nullptr;
  auto iter = js.find("occ_options");
  if (js.end() == iter) {
    Status s = OptimisticTransactionDB::Open(
        *db_opt, name, cfdvec, &db->cf_handles, &dbptr);
    if (!s.ok())
      throw s;
  }
  else {
    Status s = OptimisticTransactionDB::Open(
        *db_opt, OptimisticTransactionDBOptions_Json(iter.value(), repo),
        name, cfdvec, &db->cf_handles, &dbptr);
    if (!s.ok())
      throw s;
  }
  if (db->cf_handles.size() != cfdvec.size())
    THROW_Corruption("cf_handles.size() != cfdvec.size()");
  db->db = dbptr;
  return db.release();
}
ROCKSDB_FACTORY_REG("OptimisticTransactionDB::Open", JS_OccTransactionDB_MultiCF_Open);
ROCKSDB_FACTORY_REG("OptimisticTransactionDB::Open", JS_DB_MultiCF_Manip);

/////////////////////////////////////////////////////////////////////////////
// BlobDB::Open
namespace blob_db {

struct BlobDBOptions_Json : BlobDBOptions {
  BlobDBOptions_Json(const json& js, const JsonPluginRepo&) {
    ROCKSDB_JSON_OPT_PROP(js, blob_dir);
    ROCKSDB_JSON_OPT_PROP(js, path_relative);
    ROCKSDB_JSON_OPT_PROP(js, is_fifo);
    ROCKSDB_JSON_OPT_SIZE(js, max_db_size);
    ROCKSDB_JSON_OPT_PROP(js, ttl_range_secs);
    ROCKSDB_JSON_OPT_SIZE(js, min_blob_size);
    ROCKSDB_JSON_OPT_SIZE(js, bytes_per_sync);
    ROCKSDB_JSON_OPT_SIZE(js, blob_file_size);
    ROCKSDB_JSON_OPT_ENUM(js, compression);
    ROCKSDB_JSON_OPT_PROP(js, enable_garbage_collection);
    ROCKSDB_JSON_OPT_PROP(js, garbage_collection_cutoff);
    ROCKSDB_JSON_OPT_PROP(js, disable_background_tasks);
  }
  json ToJson(const JsonPluginRepo&) const {
    json js;
    ROCKSDB_JSON_SET_PROP(js, blob_dir);
    ROCKSDB_JSON_SET_PROP(js, path_relative);
    ROCKSDB_JSON_SET_PROP(js, is_fifo);
    ROCKSDB_JSON_SET_SIZE(js, max_db_size);
    ROCKSDB_JSON_SET_PROP(js, ttl_range_secs);
    ROCKSDB_JSON_SET_SIZE(js, min_blob_size);
    ROCKSDB_JSON_SET_SIZE(js, bytes_per_sync);
    ROCKSDB_JSON_SET_SIZE(js, blob_file_size);
    ROCKSDB_JSON_SET_ENUM(js, compression);
    ROCKSDB_JSON_SET_PROP(js, enable_garbage_collection);
    ROCKSDB_JSON_SET_PROP(js, garbage_collection_cutoff);
    ROCKSDB_JSON_SET_PROP(js, disable_background_tasks);
    return js;
  }
};

BlobDBOptions
JS_BlobDBOptions(const json& js, const JsonPluginRepo& repo) {
  auto iter = js.find("bdb_options");
  if (js.end() == iter) {
    THROW_InvalidArgument("missing required param \"bdb_options\"");
  }
  return BlobDBOptions_Json(iter.value(), repo);
}

static
DB* JS_BlobDB_Open(const json& js, const JsonPluginRepo& repo) {
  std::string name;
  Options options(JS_Options(js, repo, &name));
  BlobDBOptions bdb_options(JS_BlobDBOptions(js, repo));
  BlobDB* db = nullptr;
  Status s = BlobDB::Open(options, bdb_options, name, &db);
  if (!s.ok())
    throw s;
  return db;
}
ROCKSDB_FACTORY_REG("BlobDB::Open", JS_BlobDB_Open);
ROCKSDB_FACTORY_REG("BlobDB::Open", JS_DB_Manip);

static
DB_MultiCF*
JS_BlobDB_MultiCF_Open(const json& js, const JsonPluginRepo& repo) {
  shared_ptr<DBOptions> db_opt;
  std::vector<ColumnFamilyDescriptor> cfdvec;
  std::string name;
  BlobDBOptions bdb_options(JS_BlobDBOptions(js, repo));
  auto db = JS_DB_MultiCF_Options(js, repo, &db_opt, &cfdvec, &name);
  BlobDB* dbptr = nullptr;
  Status s = BlobDB::Open(*db_opt, bdb_options, name, cfdvec,
                    &db->cf_handles, &dbptr);
  if (!s.ok())
    throw s;
  if (db->cf_handles.size() != cfdvec.size())
    THROW_Corruption("cf_handles.size() != cfdvec.size()");
  db->db = dbptr;
  return db.release();
}
ROCKSDB_FACTORY_REG("BlobDB::Open", JS_BlobDB_MultiCF_Open);
ROCKSDB_FACTORY_REG("BlobDB::Open", JS_DB_MultiCF_Manip);

} // namespace blob_db

DB_MultiCF::DB_MultiCF() = default;
DB_MultiCF::~DB_MultiCF() = default;

// users should ensure databases are alive when calling this function
void JsonPluginRepo::CloseAllDB() {
  for (auto& kv : *m_impl->db.name2p) {
    assert(nullptr != kv.second.db);
    if (kv.second.dbm) {
      DB_MultiCF* dbm = kv.second.dbm;
      assert(kv.second.db = dbm->db);
      for (auto cfh : dbm->cf_handles) {
        delete cfh;
      }
      delete dbm->db;
      delete dbm;
    }
    else {
      DB* db = kv.second.db;
      delete db;
    }
  }
  m_impl->db.name2p->clear();
  m_impl->db.p2name.clear();
}

}
