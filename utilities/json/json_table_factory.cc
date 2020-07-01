//
// Created by leipeng on 2020/7/1.
//
#include "table/block_based/block_based_table_factory.h"
#include "table/plain/plain_table_factory.h"
#include <memory>
#include <string>
#include "rocksdb/cache.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/flush_block_policy.h"
#include "util/json.h"

namespace ROCKSDB_NAMESPACE {

Status BlockBasedTableOptions::UpdateFromJson(const json& js) {
  try {
    ROCKSDB_JSON_OPT_FACT(js, flush_block_policy_factory);
    ROCKSDB_JSON_OPT_PROP(js, cache_index_and_filter_blocks);
    ROCKSDB_JSON_OPT_PROP(js, cache_index_and_filter_blocks_with_high_priority);
    ROCKSDB_JSON_OPT_PROP(js, pin_l0_filter_and_index_blocks_in_cache);
    ROCKSDB_JSON_OPT_PROP(js, pin_top_level_index_and_filter);
    ROCKSDB_JSON_OPT_PROP(js, pin_l0_filter_and_index_blocks_in_cache);
    ROCKSDB_JSON_OPT_ENUM(js, index_type);
    ROCKSDB_JSON_OPT_ENUM(js, data_block_index_type);
    ROCKSDB_JSON_OPT_ENUM(js, index_shortening);
    ROCKSDB_JSON_OPT_ENUM(js, data_block_index_type);
    ROCKSDB_JSON_OPT_PROP(js, data_block_hash_table_util_ratio);
    ROCKSDB_JSON_OPT_PROP(js, hash_index_allow_collision);
    ROCKSDB_JSON_OPT_ENUM(js, checksum);
    ROCKSDB_JSON_OPT_PROP(js, no_block_cache);
    ROCKSDB_JSON_OPT_PROP(js, block_size);
    ROCKSDB_JSON_OPT_PROP(js, block_size_deviation);
    ROCKSDB_JSON_OPT_PROP(js, block_restart_interval);
    ROCKSDB_JSON_OPT_PROP(js, index_block_restart_interval);
    ROCKSDB_JSON_OPT_PROP(js, metadata_block_size);
    ROCKSDB_JSON_OPT_PROP(js, partition_filters);
    ROCKSDB_JSON_OPT_PROP(js, use_delta_encoding);
    ROCKSDB_JSON_OPT_PROP(js, read_amp_bytes_per_bit);
    ROCKSDB_JSON_OPT_PROP(js, whole_key_filtering);
    ROCKSDB_JSON_OPT_PROP(js, verify_compression);
    ROCKSDB_JSON_OPT_PROP(js, format_version);
    ROCKSDB_JSON_OPT_PROP(js, enable_index_compression);
    ROCKSDB_JSON_OPT_PROP(js, block_align);
    ROCKSDB_JSON_OPT_FACT(js, block_cache);
    ROCKSDB_JSON_OPT_FACT(js, block_cache_compressed);
    ROCKSDB_JSON_OPT_FACT(js, persistent_cache);
    ROCKSDB_JSON_OPT_FACT(js, filter_policy);
    return Status::OK();
  }
  catch (const std::exception& ex) {
    return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
  }
}

#if 0
std::string BlockBasedTableFactory::GetOptionJson() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  ret.append("{");
  snprintf(buffer, kBufferSize, "  flush_block_policy_factory: %s, ",
           table_options_.flush_block_policy_factory->Name());
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  cache_index_and_filter_blocks: %d, ",
           table_options_.cache_index_and_filter_blocks);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  cache_index_and_filter_blocks_with_high_priority: %d, ",
           table_options_.cache_index_and_filter_blocks_with_high_priority);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  pin_l0_filter_and_index_blocks_in_cache: %d, ",
           table_options_.pin_l0_filter_and_index_blocks_in_cache);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  pin_top_level_index_and_filter: %d, ",
           table_options_.pin_top_level_index_and_filter);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_type: %d, ",
           table_options_.index_type);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  data_block_index_type: %d, ",
           table_options_.data_block_index_type);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_shortening: %d, ",
           static_cast<int>(table_options_.index_shortening));
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  data_block_hash_table_util_ratio: %lf, ",
           table_options_.data_block_hash_table_util_ratio);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  hash_index_allow_collision: %d, ",
           table_options_.hash_index_allow_collision);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  checksum: %d, ", table_options_.checksum);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  no_block_cache: %d, ",
           table_options_.no_block_cache);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_cache: %p, ",
           static_cast<void*>(table_options_.block_cache.get()));
  ret.append(buffer);
/*
  if (table_options_.block_cache) {
    const char* block_cache_name = table_options_.block_cache->Name();
    if (block_cache_name != nullptr) {
      snprintf(buffer, kBufferSize, "  block_cache_name: %s, ",
               block_cache_name);
      ret.append(buffer);
    }
    ret.append("  block_cache_options:, ");
    ret.append(table_options_.block_cache->GetOptionJson());
  }
  snprintf(buffer, kBufferSize, "  block_cache_compressed: %p, ",
           static_cast<void*>(table_options_.block_cache_compressed.get()));
  ret.append(buffer);
  if (table_options_.block_cache_compressed) {
    const char* block_cache_compressed_name =
        table_options_.block_cache_compressed->Name();
    if (block_cache_compressed_name != nullptr) {
      snprintf(buffer, kBufferSize, "  block_cache_name: %s, ",
               block_cache_compressed_name);
      ret.append(buffer);
    }
    ret.append("  block_cache_compressed_options:, ");
    ret.append(table_options_.block_cache_compressed->GetOptionJson());
  }
  snprintf(buffer, kBufferSize, "  persistent_cache: %p, ",
           static_cast<void*>(table_options_.persistent_cache.get()));
  ret.append(buffer);
  if (table_options_.persistent_cache) {
    snprintf(buffer, kBufferSize, "  persistent_cache_options:, ");
    ret.append(buffer);
    ret.append(table_options_.persistent_cache->GetOptionJson());
  }
*/
  snprintf(buffer, kBufferSize, "  block_size: %" ROCKSDB_PRIszt ", ",
           table_options_.block_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_size_deviation: %d, ",
           table_options_.block_size_deviation);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_restart_interval: %d, ",
           table_options_.block_restart_interval);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_block_restart_interval: %d, ",
           table_options_.index_block_restart_interval);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  metadata_block_size: %" PRIu64 ", ",
           table_options_.metadata_block_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  partition_filters: %d, ",
           table_options_.partition_filters);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  use_delta_encoding: %d, ",
           table_options_.use_delta_encoding);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  filter_policy: %s, ",
           table_options_.filter_policy == nullptr
           ? "nullptr"
           : table_options_.filter_policy->Name());
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  whole_key_filtering: %d, ",
           table_options_.whole_key_filtering);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  verify_compression: %d, ",
           table_options_.verify_compression);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  read_amp_bytes_per_bit: %d, ",
           table_options_.read_amp_bytes_per_bit);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  format_version: %d, ",
           table_options_.format_version);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  enable_index_compression: %d, ",
           table_options_.enable_index_compression);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_align: %d",
           table_options_.block_align);
  ret.append(buffer);
  ret.append("}");
  return ret;
}
#endif

static std::shared_ptr<TableFactory>
NewBlockBasedTableFactoryFromJson(const json& j, Status* s) {
  BlockBasedTableOptions _table_options;
  *s = _table_options.UpdateFromJson(j);
  if (s->ok())
    return std::make_shared<BlockBasedTableFactory>(_table_options);
  else
    return nullptr;
}

ROCKSDB_FACTORY_REG("BlockBasedTable", NewBlockBasedTableFactoryFromJson);

////////////////////////////////////////////////////////////////////////////
Status PlainTableOptions::UpdateFromJson(const json& js) {
  try {
    ROCKSDB_JSON_OPT_PROP(js, user_key_len);
    ROCKSDB_JSON_OPT_PROP(js, bloom_bits_per_key);
    ROCKSDB_JSON_OPT_PROP(js, hash_table_ratio);
    ROCKSDB_JSON_OPT_PROP(js, index_sparseness);
    ROCKSDB_JSON_OPT_ENUM(js, encoding_type);
    ROCKSDB_JSON_OPT_PROP(js, full_scan_mode);
    ROCKSDB_JSON_OPT_PROP(js, store_index_in_file);
    return Status::OK();
  }
  catch (const std::exception& ex) {
    return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
  }
}

static std::shared_ptr<TableFactory>
NewPlainTableFactoryFromJson(const json& j, Status* s) {
  PlainTableOptions options;
  *s = options.UpdateFromJson(j);
  if (s->ok())
    return std::make_shared<PlainTableFactory>(options);
  else
    return nullptr;
}

ROCKSDB_FACTORY_REG("PlainTable", NewPlainTableFactoryFromJson);


}