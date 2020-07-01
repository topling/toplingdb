//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/options.h"

#include <cinttypes>
#include <limits>

#include "monitoring/statistics.h"
#include "options/db_options.h"
#include "options/options_helper.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/wal_filter.h"
#include "table/block_based/block_based_table_factory.h"
#include "util/compression.h"
#include "util/json.h"

namespace ROCKSDB_NAMESPACE {

AdvancedColumnFamilyOptions::AdvancedColumnFamilyOptions() {
  assert(memtable_factory.get() != nullptr);
}

AdvancedColumnFamilyOptions::AdvancedColumnFamilyOptions(const Options& options)
    : max_write_buffer_number(options.max_write_buffer_number),
      min_write_buffer_number_to_merge(
          options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          options.max_write_buffer_number_to_maintain),
      max_write_buffer_size_to_maintain(
          options.max_write_buffer_size_to_maintain),
      inplace_update_support(options.inplace_update_support),
      inplace_update_num_locks(options.inplace_update_num_locks),
      inplace_callback(options.inplace_callback),
      memtable_prefix_bloom_size_ratio(
          options.memtable_prefix_bloom_size_ratio),
      memtable_whole_key_filtering(options.memtable_whole_key_filtering),
      memtable_huge_page_size(options.memtable_huge_page_size),
      memtable_insert_with_hint_prefix_extractor(
          options.memtable_insert_with_hint_prefix_extractor),
      bloom_locality(options.bloom_locality),
      arena_block_size(options.arena_block_size),
      compression_per_level(options.compression_per_level),
      num_levels(options.num_levels),
      level0_slowdown_writes_trigger(options.level0_slowdown_writes_trigger),
      level0_stop_writes_trigger(options.level0_stop_writes_trigger),
      target_file_size_base(options.target_file_size_base),
      target_file_size_multiplier(options.target_file_size_multiplier),
      level_compaction_dynamic_level_bytes(
          options.level_compaction_dynamic_level_bytes),
      max_bytes_for_level_multiplier(options.max_bytes_for_level_multiplier),
      max_bytes_for_level_multiplier_additional(
          options.max_bytes_for_level_multiplier_additional),
      max_compaction_bytes(options.max_compaction_bytes),
      soft_pending_compaction_bytes_limit(
          options.soft_pending_compaction_bytes_limit),
      hard_pending_compaction_bytes_limit(
          options.hard_pending_compaction_bytes_limit),
      compaction_style(options.compaction_style),
      compaction_pri(options.compaction_pri),
      compaction_options_universal(options.compaction_options_universal),
      compaction_options_fifo(options.compaction_options_fifo),
      max_sequential_skip_in_iterations(
          options.max_sequential_skip_in_iterations),
      memtable_factory(options.memtable_factory),
      table_properties_collector_factories(
          options.table_properties_collector_factories),
      max_successive_merges(options.max_successive_merges),
      optimize_filters_for_hits(options.optimize_filters_for_hits),
      paranoid_file_checks(options.paranoid_file_checks),
      force_consistency_checks(options.force_consistency_checks),
      report_bg_io_stats(options.report_bg_io_stats),
      ttl(options.ttl),
      periodic_compaction_seconds(options.periodic_compaction_seconds),
      sample_for_compression(options.sample_for_compression) {
  assert(memtable_factory.get() != nullptr);
  if (max_bytes_for_level_multiplier_additional.size() <
      static_cast<unsigned int>(num_levels)) {
    max_bytes_for_level_multiplier_additional.resize(num_levels, 1);
  }
}

ColumnFamilyOptions::ColumnFamilyOptions()
    : compression(Snappy_Supported() ? kSnappyCompression : kNoCompression),
      table_factory(
          std::shared_ptr<TableFactory>(new BlockBasedTableFactory())) {}

ColumnFamilyOptions::ColumnFamilyOptions(const Options& options)
    : ColumnFamilyOptions(*static_cast<const ColumnFamilyOptions*>(&options)) {}

DBOptions::DBOptions() {}
DBOptions::DBOptions(const Options& options)
    : DBOptions(*static_cast<const DBOptions*>(&options)) {}

void DBOptions::Dump(Logger* log) const {
    ImmutableDBOptions(*this).Dump(log);
    MutableDBOptions(*this).Dump(log);
}  // DBOptions::Dump

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
    ROCKSDB_JSON_OPT_NEST_IMPL(js, el, EventListener);
    listeners.emplace_back(el);
  }
  else if (js.is_array()) {
    for (auto& one : js.items()) {
      std::shared_ptr<EventListener> el;
      ROCKSDB_JSON_OPT_NEST_IMPL(one.value(), el, EventListener);
      listeners.emplace_back(el);
    }
  }
  return Status::OK();
}

Status DBOptions::UpdateFromJson(const json& js) try {
  ROCKSDB_JSON_OPT_PROP(js, paranoid_checks);
  ROCKSDB_JSON_OPT_NEST(js, env);
  ROCKSDB_JSON_OPT_NEST(js, rate_limiter);
  ROCKSDB_JSON_OPT_NEST(js, sst_file_manager);
  ROCKSDB_JSON_OPT_NEST(js, info_log);
  ROCKSDB_JSON_OPT_ENUM(js, info_log_level);
  ROCKSDB_JSON_OPT_PROP(js, max_open_files);
  ROCKSDB_JSON_OPT_PROP(js, max_file_opening_threads);
  ROCKSDB_JSON_OPT_PROP(js, max_total_wal_size);
  ROCKSDB_JSON_OPT_NEST(js, statistics);
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
      ROCKSDB_JSON_OPT_NEST(wbm, cache);
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
  ROCKSDB_JSON_OPT_NEST(js, row_cache);
  //ROCKSDB_JSON_OPT_NEST(js, wal_filter);
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
  ROCKSDB_JSON_OPT_NEST(js, file_checksum_gen_factory);
  ROCKSDB_JSON_OPT_PROP(js, best_efforts_recovery);
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}

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

struct CompressionOptions_Impl : CompressionOptions {
  explicit CompressionOptions_Impl(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, window_bits);
    ROCKSDB_JSON_OPT_PROP(js, level);
    ROCKSDB_JSON_OPT_PROP(js, strategy);
    ROCKSDB_JSON_OPT_PROP(js, max_dict_bytes);
    ROCKSDB_JSON_OPT_PROP(js, zstd_max_train_bytes);
    ROCKSDB_JSON_OPT_PROP(js, parallel_threads);
    ROCKSDB_JSON_OPT_PROP(js, enabled);
  }
};

Status ColumnFamilyOptions::UpdateFromJson(const json& js) try {
  ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_number);
  ROCKSDB_JSON_OPT_PROP(js, min_write_buffer_number_to_merge);
  ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_number_to_maintain);
  ROCKSDB_JSON_OPT_PROP(js, max_write_buffer_size_to_maintain);
  ROCKSDB_JSON_OPT_PROP(js, inplace_update_support);
  ROCKSDB_JSON_OPT_ENUM(js, inplace_update_num_locks);
  //ROCKSDB_JSON_OPT_PROP(js, inplace_callback); // not need update
  ROCKSDB_JSON_OPT_PROP(js, memtable_prefix_bloom_size_ratio);
  ROCKSDB_JSON_OPT_PROP(js, memtable_whole_key_filtering);
  ROCKSDB_JSON_OPT_PROP(js, memtable_huge_page_size);
  //ROCKSDB_JSON_OPT_NEST(js, memtable_insert_with_hint_prefix_extractor);
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
        return Status::InvalidArgument(ROCKSDB_FUNC, "max_bytes_for_level_multiplier_additional must be a int vector");
  }
  catch (const std::exception& ex) {
    return Status::InvalidArgument(ROCKSDB_FUNC,
  "max_bytes_for_level_multiplier_additional must be a int vector, "
        "details: " + std::string(ex.what()));
  }
  ROCKSDB_JSON_OPT_PROP(js, max_compaction_bytes);
  ROCKSDB_JSON_OPT_PROP(js, soft_pending_compaction_bytes_limit);
  ROCKSDB_JSON_OPT_PROP(js, hard_pending_compaction_bytes_limit);
  ROCKSDB_JSON_OPT_ENUM(js, compaction_style);
  ROCKSDB_JSON_OPT_PROP(js, compaction_pri);
  //ROCKSDB_JSON_OPT_PROP(js, compaction_options_universal);
  //ROCKSDB_JSON_OPT_PROP(js, compaction_options_fifo);
  ROCKSDB_JSON_OPT_PROP(js, max_sequential_skip_in_iterations);
  ROCKSDB_JSON_OPT_NEST(js, memtable_factory);
  try {
    auto iter = js.find("table_properties_collector_factories");
    if (js.end() != iter) {
      if (!iter.value().is_array()) {
        return Status::InvalidArgument(ROCKSDB_FUNC,
         "table_properties_collector_factories must be an array");
      }
      decltype(table_properties_collector_factories) vec;
      for (auto& item : iter.value().items()) {
        decltype(vec)::value_type p;
        ROCKSDB_JSON_OPT_NEST_IMPL(js, p, TablePropertiesCollectorFactory);
        vec.push_back(p);
      }
      table_properties_collector_factories.swap(vec);
    }
  }
  catch (const std::exception& ex) {
    return Status::InvalidArgument(ROCKSDB_FUNC,
       "table_properties_collector_factories: " + std::string(ex.what()));
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
  ROCKSDB_JSON_OPT_NEST(js, comparator);
  ROCKSDB_JSON_OPT_NEST(js, merge_operator);
  //ROCKSDB_JSON_OPT_NEST(js, compaction_filter);
  ROCKSDB_JSON_OPT_NEST(js, compaction_filter_factory);
  ROCKSDB_JSON_OPT_PROP(js, write_buffer_size);
  ROCKSDB_JSON_OPT_ENUM(js, compression);
  ROCKSDB_JSON_OPT_ENUM(js, bottommost_compression);
  try {
    auto iter = js.find("bottommost_compression_opts");
    if (js.end() != iter)
      bottommost_compression_opts = CompressionOptions_Impl(iter.value());
  }
  catch (const std::exception& ex) {
    return Status::InvalidArgument(ROCKSDB_FUNC,
        "bottommost_compression_opts: " + std::string(ex.what()));
  }
  try {
    auto iter = js.find("compression_opts");
    if (js.end() != iter)
      compression_opts = CompressionOptions_Impl(iter.value());
  }
  catch (const std::exception& ex) {
    return Status::InvalidArgument(ROCKSDB_FUNC,
        "compression_opts: " + std::string(ex.what()));
  }
  ROCKSDB_JSON_OPT_PROP(js, level0_file_num_compaction_trigger);
  ROCKSDB_JSON_OPT_NEST(js, prefix_extractor);
  ROCKSDB_JSON_OPT_PROP(js, max_bytes_for_level_base);
  ROCKSDB_JSON_OPT_PROP(js, snap_refresh_nanos);
  ROCKSDB_JSON_OPT_PROP(js, disable_auto_compactions);
  ROCKSDB_JSON_OPT_NEST(js, table_factory);
  {
    auto iter = js.find("cf_paths");
    if (js.end() != iter) Json_DbPathVec(js, cf_paths);
  }
  ROCKSDB_JSON_OPT_NEST(js, compaction_thread_limiter);
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}

void ColumnFamilyOptions::Dump(Logger* log) const {
  ROCKS_LOG_HEADER(log, "              Options.comparator: %s",
                   comparator->Name());
  ROCKS_LOG_HEADER(log, "          Options.merge_operator: %s",
                   merge_operator ? merge_operator->Name() : "None");
  ROCKS_LOG_HEADER(log, "       Options.compaction_filter: %s",
                   compaction_filter ? compaction_filter->Name() : "None");
  ROCKS_LOG_HEADER(
      log, "       Options.compaction_filter_factory: %s",
      compaction_filter_factory ? compaction_filter_factory->Name() : "None");
  ROCKS_LOG_HEADER(log, "        Options.memtable_factory: %s",
                   memtable_factory->Name());
  ROCKS_LOG_HEADER(log, "           Options.table_factory: %s",
                   table_factory->Name());
  ROCKS_LOG_HEADER(log, "           table_factory options: %s",
                   table_factory->GetPrintableTableOptions().c_str());
  ROCKS_LOG_HEADER(log, "       Options.write_buffer_size: %" ROCKSDB_PRIszt,
                   write_buffer_size);
  ROCKS_LOG_HEADER(log, " Options.max_write_buffer_number: %d",
                   max_write_buffer_number);
  if (!compression_per_level.empty()) {
    for (unsigned int i = 0; i < compression_per_level.size(); i++) {
      ROCKS_LOG_HEADER(
          log, "       Options.compression[%d]: %s", i,
          CompressionTypeToString(compression_per_level[i]).c_str());
    }
    } else {
      ROCKS_LOG_HEADER(log, "         Options.compression: %s",
                       CompressionTypeToString(compression).c_str());
    }
    ROCKS_LOG_HEADER(
        log, "                 Options.bottommost_compression: %s",
        bottommost_compression == kDisableCompressionOption
            ? "Disabled"
            : CompressionTypeToString(bottommost_compression).c_str());
    ROCKS_LOG_HEADER(
        log, "      Options.prefix_extractor: %s",
        prefix_extractor == nullptr ? "nullptr" : prefix_extractor->Name());
    ROCKS_LOG_HEADER(log,
                     "  Options.memtable_insert_with_hint_prefix_extractor: %s",
                     memtable_insert_with_hint_prefix_extractor == nullptr
                         ? "nullptr"
                         : memtable_insert_with_hint_prefix_extractor->Name());
    ROCKS_LOG_HEADER(log, "            Options.num_levels: %d", num_levels);
    ROCKS_LOG_HEADER(log, "       Options.min_write_buffer_number_to_merge: %d",
                     min_write_buffer_number_to_merge);
    ROCKS_LOG_HEADER(log, "    Options.max_write_buffer_number_to_maintain: %d",
                     max_write_buffer_number_to_maintain);
    ROCKS_LOG_HEADER(log,
                     "    Options.max_write_buffer_size_to_maintain: %" PRIu64,
                     max_write_buffer_size_to_maintain);
    ROCKS_LOG_HEADER(
        log, "           Options.bottommost_compression_opts.window_bits: %d",
        bottommost_compression_opts.window_bits);
    ROCKS_LOG_HEADER(
        log, "                 Options.bottommost_compression_opts.level: %d",
        bottommost_compression_opts.level);
    ROCKS_LOG_HEADER(
        log, "              Options.bottommost_compression_opts.strategy: %d",
        bottommost_compression_opts.strategy);
    ROCKS_LOG_HEADER(
        log,
        "        Options.bottommost_compression_opts.max_dict_bytes: "
        "%" PRIu32,
        bottommost_compression_opts.max_dict_bytes);
    ROCKS_LOG_HEADER(
        log,
        "        Options.bottommost_compression_opts.zstd_max_train_bytes: "
        "%" PRIu32,
        bottommost_compression_opts.zstd_max_train_bytes);
    ROCKS_LOG_HEADER(
        log,
        "        Options.bottommost_compression_opts.parallel_threads: "
        "%" PRIu32,
        bottommost_compression_opts.parallel_threads);
    ROCKS_LOG_HEADER(
        log, "                 Options.bottommost_compression_opts.enabled: %s",
        bottommost_compression_opts.enabled ? "true" : "false");
    ROCKS_LOG_HEADER(log, "           Options.compression_opts.window_bits: %d",
                     compression_opts.window_bits);
    ROCKS_LOG_HEADER(log, "                 Options.compression_opts.level: %d",
                     compression_opts.level);
    ROCKS_LOG_HEADER(log, "              Options.compression_opts.strategy: %d",
                     compression_opts.strategy);
    ROCKS_LOG_HEADER(
        log,
        "        Options.compression_opts.max_dict_bytes: %" PRIu32,
        compression_opts.max_dict_bytes);
    ROCKS_LOG_HEADER(log,
                     "        Options.compression_opts.zstd_max_train_bytes: "
                     "%" PRIu32,
                     compression_opts.zstd_max_train_bytes);
    ROCKS_LOG_HEADER(log,
                     "        Options.compression_opts.parallel_threads: "
                     "%" PRIu32,
                     compression_opts.parallel_threads);
    ROCKS_LOG_HEADER(log,
                     "                 Options.compression_opts.enabled: %s",
                     compression_opts.enabled ? "true" : "false");
    ROCKS_LOG_HEADER(log, "     Options.level0_file_num_compaction_trigger: %d",
                     level0_file_num_compaction_trigger);
    ROCKS_LOG_HEADER(log, "         Options.level0_slowdown_writes_trigger: %d",
                     level0_slowdown_writes_trigger);
    ROCKS_LOG_HEADER(log, "             Options.level0_stop_writes_trigger: %d",
                     level0_stop_writes_trigger);
    ROCKS_LOG_HEADER(
        log, "                  Options.target_file_size_base: %" PRIu64,
        target_file_size_base);
    ROCKS_LOG_HEADER(log, "            Options.target_file_size_multiplier: %d",
                     target_file_size_multiplier);
    ROCKS_LOG_HEADER(
        log, "               Options.max_bytes_for_level_base: %" PRIu64,
        max_bytes_for_level_base);
    ROCKS_LOG_HEADER(log, "Options.level_compaction_dynamic_level_bytes: %d",
                     level_compaction_dynamic_level_bytes);
    ROCKS_LOG_HEADER(log, "         Options.max_bytes_for_level_multiplier: %f",
                     max_bytes_for_level_multiplier);
    for (size_t i = 0; i < max_bytes_for_level_multiplier_additional.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "Options.max_bytes_for_level_multiplier_addtl[%" ROCKSDB_PRIszt
               "]: %d",
          i, max_bytes_for_level_multiplier_additional[i]);
    }
    ROCKS_LOG_HEADER(
        log, "      Options.max_sequential_skip_in_iterations: %" PRIu64,
        max_sequential_skip_in_iterations);
    ROCKS_LOG_HEADER(
        log, "                   Options.max_compaction_bytes: %" PRIu64,
        max_compaction_bytes);
    ROCKS_LOG_HEADER(
        log,
        "                       Options.arena_block_size: %" ROCKSDB_PRIszt,
        arena_block_size);
    ROCKS_LOG_HEADER(log,
                     "  Options.soft_pending_compaction_bytes_limit: %" PRIu64,
                     soft_pending_compaction_bytes_limit);
    ROCKS_LOG_HEADER(log,
                     "  Options.hard_pending_compaction_bytes_limit: %" PRIu64,
                     hard_pending_compaction_bytes_limit);
    ROCKS_LOG_HEADER(log, "      Options.rate_limit_delay_max_milliseconds: %u",
                     rate_limit_delay_max_milliseconds);
    ROCKS_LOG_HEADER(log, "               Options.disable_auto_compactions: %d",
                     disable_auto_compactions);

    const auto& it_compaction_style =
        compaction_style_to_string.find(compaction_style);
    std::string str_compaction_style;
    if (it_compaction_style == compaction_style_to_string.end()) {
      assert(false);
      str_compaction_style = "unknown_" + std::to_string(compaction_style);
    } else {
      str_compaction_style = it_compaction_style->second;
    }
    ROCKS_LOG_HEADER(log,
                     "                       Options.compaction_style: %s",
                     str_compaction_style.c_str());

    const auto& it_compaction_pri =
        compaction_pri_to_string.find(compaction_pri);
    std::string str_compaction_pri;
    if (it_compaction_pri == compaction_pri_to_string.end()) {
      assert(false);
      str_compaction_pri = "unknown_" + std::to_string(compaction_pri);
    } else {
      str_compaction_pri = it_compaction_pri->second;
    }
    ROCKS_LOG_HEADER(log,
                     "                         Options.compaction_pri: %s",
                     str_compaction_pri.c_str());
    ROCKS_LOG_HEADER(log,
                     "Options.compaction_options_universal.size_ratio: %u",
                     compaction_options_universal.size_ratio);
    ROCKS_LOG_HEADER(log,
                     "Options.compaction_options_universal.min_merge_width: %u",
                     compaction_options_universal.min_merge_width);
    ROCKS_LOG_HEADER(log,
                     "Options.compaction_options_universal.max_merge_width: %u",
                     compaction_options_universal.max_merge_width);
    ROCKS_LOG_HEADER(
        log,
        "Options.compaction_options_universal."
        "max_size_amplification_percent: %u",
        compaction_options_universal.max_size_amplification_percent);
    ROCKS_LOG_HEADER(
        log,
        "Options.compaction_options_universal.compression_size_percent: %d",
        compaction_options_universal.compression_size_percent);
    const auto& it_compaction_stop_style = compaction_stop_style_to_string.find(
        compaction_options_universal.stop_style);
    std::string str_compaction_stop_style;
    if (it_compaction_stop_style == compaction_stop_style_to_string.end()) {
      assert(false);
      str_compaction_stop_style =
          "unknown_" + std::to_string(compaction_options_universal.stop_style);
    } else {
      str_compaction_stop_style = it_compaction_stop_style->second;
    }
    ROCKS_LOG_HEADER(log,
                     "Options.compaction_options_universal.stop_style: %s",
                     str_compaction_stop_style.c_str());
    ROCKS_LOG_HEADER(
        log, "Options.compaction_options_fifo.max_table_files_size: %" PRIu64,
        compaction_options_fifo.max_table_files_size);
    ROCKS_LOG_HEADER(log,
                     "Options.compaction_options_fifo.allow_compaction: %d",
                     compaction_options_fifo.allow_compaction);
    std::ostringstream collector_info;
    for (const auto& collector_factory : table_properties_collector_factories) {
      collector_info << collector_factory->ToString() << ';';
    }
    ROCKS_LOG_HEADER(
        log, "                  Options.table_properties_collectors: %s",
        collector_info.str().c_str());
    ROCKS_LOG_HEADER(log,
                     "                  Options.inplace_update_support: %d",
                     inplace_update_support);
    ROCKS_LOG_HEADER(
        log,
        "                Options.inplace_update_num_locks: %" ROCKSDB_PRIszt,
        inplace_update_num_locks);
    // TODO: easier config for bloom (maybe based on avg key/value size)
    ROCKS_LOG_HEADER(
        log, "              Options.memtable_prefix_bloom_size_ratio: %f",
        memtable_prefix_bloom_size_ratio);
    ROCKS_LOG_HEADER(log,
                     "              Options.memtable_whole_key_filtering: %d",
                     memtable_whole_key_filtering);

    ROCKS_LOG_HEADER(log, "  Options.memtable_huge_page_size: %" ROCKSDB_PRIszt,
                     memtable_huge_page_size);
    ROCKS_LOG_HEADER(log,
                     "                          Options.bloom_locality: %d",
                     bloom_locality);

    ROCKS_LOG_HEADER(
        log,
        "                   Options.max_successive_merges: %" ROCKSDB_PRIszt,
        max_successive_merges);
    ROCKS_LOG_HEADER(log,
                     "               Options.optimize_filters_for_hits: %d",
                     optimize_filters_for_hits);
    ROCKS_LOG_HEADER(log, "               Options.paranoid_file_checks: %d",
                     paranoid_file_checks);
    ROCKS_LOG_HEADER(log, "               Options.force_consistency_checks: %d",
                     force_consistency_checks);
    ROCKS_LOG_HEADER(log, "               Options.report_bg_io_stats: %d",
                     report_bg_io_stats);
    ROCKS_LOG_HEADER(log, "                              Options.ttl: %" PRIu64,
                     ttl);
    ROCKS_LOG_HEADER(log,
                     "         Options.periodic_compaction_seconds: %" PRIu64,
                     periodic_compaction_seconds);
}  // ColumnFamilyOptions::Dump

void Options::Dump(Logger* log) const {
  DBOptions::Dump(log);
  ColumnFamilyOptions::Dump(log);
}   // Options::Dump

void Options::DumpCFOptions(Logger* log) const {
  ColumnFamilyOptions::Dump(log);
}  // Options::DumpCFOptions

//
// The goal of this method is to create a configuration that
// allows an application to write all files into L0 and
// then do a single compaction to output all files into L1.
Options*
Options::PrepareForBulkLoad()
{
  // never slowdown ingest.
  level0_file_num_compaction_trigger = (1<<30);
  level0_slowdown_writes_trigger = (1<<30);
  level0_stop_writes_trigger = (1<<30);
  soft_pending_compaction_bytes_limit = 0;
  hard_pending_compaction_bytes_limit = 0;

  // no auto compactions please. The application should issue a
  // manual compaction after all data is loaded into L0.
  disable_auto_compactions = true;
  // A manual compaction run should pick all files in L0 in
  // a single compaction run.
  max_compaction_bytes = (static_cast<uint64_t>(1) << 60);

  // It is better to have only 2 levels, otherwise a manual
  // compaction would compact at every possible level, thereby
  // increasing the total time needed for compactions.
  num_levels = 2;

  // Need to allow more write buffers to allow more parallism
  // of flushes.
  max_write_buffer_number = 6;
  min_write_buffer_number_to_merge = 1;

  // When compaction is disabled, more parallel flush threads can
  // help with write throughput.
  max_background_flushes = 4;

  // Prevent a memtable flush to automatically promote files
  // to L1. This is helpful so that all files that are
  // input to the manual compaction are all at L0.
  max_background_compactions = 2;

  // The compaction would create large files in L1.
  target_file_size_base = 256 * 1024 * 1024;
  return this;
}

Options* Options::OptimizeForSmallDb() {
  // 16MB block cache
  std::shared_ptr<Cache> cache = NewLRUCache(16 << 20);

  ColumnFamilyOptions::OptimizeForSmallDb(&cache);
  DBOptions::OptimizeForSmallDb(&cache);
  return this;
}

Options* Options::OldDefaults(int rocksdb_major_version,
                              int rocksdb_minor_version) {
  ColumnFamilyOptions::OldDefaults(rocksdb_major_version,
                                   rocksdb_minor_version);
  DBOptions::OldDefaults(rocksdb_major_version, rocksdb_minor_version);
  return this;
}

DBOptions* DBOptions::OldDefaults(int rocksdb_major_version,
                                  int rocksdb_minor_version) {
  if (rocksdb_major_version < 4 ||
      (rocksdb_major_version == 4 && rocksdb_minor_version < 7)) {
    max_file_opening_threads = 1;
    table_cache_numshardbits = 4;
  }
  if (rocksdb_major_version < 5 ||
      (rocksdb_major_version == 5 && rocksdb_minor_version < 2)) {
    delayed_write_rate = 2 * 1024U * 1024U;
  } else if (rocksdb_major_version < 5 ||
             (rocksdb_major_version == 5 && rocksdb_minor_version < 6)) {
    delayed_write_rate = 16 * 1024U * 1024U;
  }
  max_open_files = 5000;
  wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OldDefaults(
    int rocksdb_major_version, int rocksdb_minor_version) {
  if (rocksdb_major_version < 5 ||
      (rocksdb_major_version == 5 && rocksdb_minor_version <= 18)) {
    compaction_pri = CompactionPri::kByCompensatedSize;
  }
  if (rocksdb_major_version < 4 ||
      (rocksdb_major_version == 4 && rocksdb_minor_version < 7)) {
    write_buffer_size = 4 << 20;
    target_file_size_base = 2 * 1048576;
    max_bytes_for_level_base = 10 * 1048576;
    soft_pending_compaction_bytes_limit = 0;
    hard_pending_compaction_bytes_limit = 0;
  }
  if (rocksdb_major_version < 5) {
    level0_stop_writes_trigger = 24;
  } else if (rocksdb_major_version == 5 && rocksdb_minor_version < 2) {
    level0_stop_writes_trigger = 30;
  }

  return this;
}

// Optimization functions
DBOptions* DBOptions::OptimizeForSmallDb(std::shared_ptr<Cache>* cache) {
  max_file_opening_threads = 1;
  max_open_files = 5000;

  // Cost memtable to block cache too.
  std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager> wbm =
      std::make_shared<ROCKSDB_NAMESPACE::WriteBufferManager>(
          0, (cache != nullptr) ? *cache : std::shared_ptr<Cache>());
  write_buffer_manager = wbm;

  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForSmallDb(
    std::shared_ptr<Cache>* cache) {
  write_buffer_size = 2 << 20;
  target_file_size_base = 2 * 1048576;
  max_bytes_for_level_base = 10 * 1048576;
  soft_pending_compaction_bytes_limit = 256 * 1048576;
  hard_pending_compaction_bytes_limit = 1073741824ul;

  BlockBasedTableOptions table_options;
  table_options.block_cache =
      (cache != nullptr) ? *cache : std::shared_ptr<Cache>();
  table_options.cache_index_and_filter_blocks = true;
  // Two level iterator to avoid LRU cache imbalance
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_factory.reset(new BlockBasedTableFactory(table_options));

  return this;
}

#ifndef ROCKSDB_LITE
ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForPointLookup(
    uint64_t block_cache_size_mb) {
  BlockBasedTableOptions block_based_options;
  block_based_options.data_block_index_type =
      BlockBasedTableOptions::kDataBlockBinaryAndHash;
  block_based_options.data_block_hash_table_util_ratio = 0.75;
  block_based_options.filter_policy.reset(NewBloomFilterPolicy(10));
  block_based_options.block_cache =
      NewLRUCache(static_cast<size_t>(block_cache_size_mb * 1024 * 1024));
  table_factory.reset(new BlockBasedTableFactory(block_based_options));
  memtable_prefix_bloom_size_ratio = 0.02;
  memtable_whole_key_filtering = true;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeLevelStyleCompaction(
    uint64_t memtable_memory_budget) {
  write_buffer_size = static_cast<size_t>(memtable_memory_budget / 4);
  // merge two memtables when flushing to L0
  min_write_buffer_number_to_merge = 2;
  // this means we'll use 50% extra memory in the worst case, but will reduce
  // write stalls.
  max_write_buffer_number = 6;
  // start flushing L0->L1 as soon as possible. each file on level0 is
  // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
  // memtable_memory_budget.
  level0_file_num_compaction_trigger = 2;
  // doesn't really matter much, but we don't want to create too many files
  target_file_size_base = memtable_memory_budget / 8;
  // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
  max_bytes_for_level_base = memtable_memory_budget;

  // level style compaction
  compaction_style = kCompactionStyleLevel;

  // only compress levels >= 2
  compression_per_level.resize(num_levels);
  for (int i = 0; i < num_levels; ++i) {
    if (i < 2) {
      compression_per_level[i] = kNoCompression;
    } else {
      compression_per_level[i] =
          LZ4_Supported()
              ? kLZ4Compression
              : (Snappy_Supported() ? kSnappyCompression : kNoCompression);
    }
  }
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeUniversalStyleCompaction(
    uint64_t memtable_memory_budget) {
  write_buffer_size = static_cast<size_t>(memtable_memory_budget / 4);
  // merge two memtables when flushing to L0
  min_write_buffer_number_to_merge = 2;
  // this means we'll use 50% extra memory in the worst case, but will reduce
  // write stalls.
  max_write_buffer_number = 6;
  // universal style compaction
  compaction_style = kCompactionStyleUniversal;
  compaction_options_universal.compression_size_percent = 80;
  return this;
}

DBOptions* DBOptions::IncreaseParallelism(int total_threads) {
  max_background_jobs = total_threads;
  env->SetBackgroundThreads(total_threads, Env::LOW);
  env->SetBackgroundThreads(1, Env::HIGH);
  return this;
}

#endif  // !ROCKSDB_LITE

ReadOptions::ReadOptions()
    : snapshot(nullptr),
      iterate_lower_bound(nullptr),
      iterate_upper_bound(nullptr),
      readahead_size(0),
      max_skippable_internal_keys(0),
      read_tier(kReadAllTier),
      verify_checksums(true),
      fill_cache(true),
      tailing(false),
      managed(false),
      total_order_seek(false),
      auto_prefix_mode(false),
      prefix_same_as_start(false),
      pin_data(false),
      background_purge_on_iterator_cleanup(false),
      ignore_range_deletions(false),
      iter_start_seqnum(0),
      timestamp(nullptr),
      iter_start_ts(nullptr),
      deadline(std::chrono::microseconds::zero()),
      value_size_soft_limit(std::numeric_limits<uint64_t>::max()) {}

ReadOptions::ReadOptions(bool cksum, bool cache)
    : snapshot(nullptr),
      iterate_lower_bound(nullptr),
      iterate_upper_bound(nullptr),
      readahead_size(0),
      max_skippable_internal_keys(0),
      read_tier(kReadAllTier),
      verify_checksums(cksum),
      fill_cache(cache),
      tailing(false),
      managed(false),
      total_order_seek(false),
      auto_prefix_mode(false),
      prefix_same_as_start(false),
      pin_data(false),
      background_purge_on_iterator_cleanup(false),
      ignore_range_deletions(false),
      iter_start_seqnum(0),
      timestamp(nullptr),
      iter_start_ts(nullptr),
      deadline(std::chrono::microseconds::zero()),
      value_size_soft_limit(std::numeric_limits<uint64_t>::max()) {}

}  // namespace ROCKSDB_NAMESPACE
