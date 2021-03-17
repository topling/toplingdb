//
// Created by leipeng on 2020/7/1.
//
#include "table/block_based/block_based_table_factory.h"
#include "table/plain/plain_table_factory.h"
#include "table/table_builder.h"
#include "json.h"
#include "json_plugin_factory.h"
#include "json_table_factory.h"
#include "internal_dispather_table.h"

namespace ROCKSDB_NAMESPACE {

static std::shared_ptr<const FilterPolicy>
NewBloomFilterPolicyJson(const json& js, const JsonPluginRepo&) {
  double bits_per_key = 10;
  bool use_block_based_builder = false;
  ROCKSDB_JSON_OPT_PROP(js, bits_per_key);
  ROCKSDB_JSON_OPT_PROP(js, use_block_based_builder);
  return std::shared_ptr<const FilterPolicy>(
      NewBloomFilterPolicy(bits_per_key, use_block_based_builder));
}
ROCKSDB_FACTORY_REG("BloomFilter", NewBloomFilterPolicyJson);

struct BlockBasedTableOptions_Json : BlockBasedTableOptions {
  BlockBasedTableOptions_Json(const json& js, const JsonPluginRepo& repo) {
    Update(js, repo);
  }
  void Update(const json& js, const JsonPluginRepo& repo) {
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
    ROCKSDB_JSON_OPT_SIZE(js, block_size);
    ROCKSDB_JSON_OPT_PROP(js, block_size_deviation);
    ROCKSDB_JSON_OPT_PROP(js, block_restart_interval);
    ROCKSDB_JSON_OPT_PROP(js, index_block_restart_interval);
    ROCKSDB_JSON_OPT_SIZE(js, metadata_block_size);
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
  }

  json ToJsonObj(const json& dump_options, const JsonPluginRepo& repo) const {
    bool html = JsonSmartBool(dump_options, "html");
    json js;
    ROCKSDB_JSON_SET_FACT(js, flush_block_policy_factory);
    ROCKSDB_JSON_SET_PROP(js, cache_index_and_filter_blocks);
    ROCKSDB_JSON_SET_PROP(js, cache_index_and_filter_blocks_with_high_priority);
    ROCKSDB_JSON_SET_PROP(js, pin_l0_filter_and_index_blocks_in_cache);
    ROCKSDB_JSON_SET_PROP(js, pin_top_level_index_and_filter);
    ROCKSDB_JSON_SET_PROP(js, pin_l0_filter_and_index_blocks_in_cache);
    ROCKSDB_JSON_SET_ENUM(js, index_type);
    ROCKSDB_JSON_SET_ENUM(js, data_block_index_type);
    ROCKSDB_JSON_SET_ENUM(js, index_shortening);
    ROCKSDB_JSON_SET_ENUM(js, data_block_index_type);
    ROCKSDB_JSON_SET_PROP(js, data_block_hash_table_util_ratio);
    ROCKSDB_JSON_SET_PROP(js, hash_index_allow_collision);
    ROCKSDB_JSON_SET_ENUM(js, checksum);
    ROCKSDB_JSON_SET_PROP(js, no_block_cache);
    ROCKSDB_JSON_SET_SIZE(js, block_size);
    ROCKSDB_JSON_SET_PROP(js, block_size_deviation);
    ROCKSDB_JSON_SET_PROP(js, block_restart_interval);
    ROCKSDB_JSON_SET_PROP(js, index_block_restart_interval);
    ROCKSDB_JSON_SET_SIZE(js, metadata_block_size);
    ROCKSDB_JSON_SET_PROP(js, partition_filters);
    ROCKSDB_JSON_SET_PROP(js, use_delta_encoding);
    ROCKSDB_JSON_SET_PROP(js, read_amp_bytes_per_bit);
    ROCKSDB_JSON_SET_PROP(js, whole_key_filtering);
    ROCKSDB_JSON_SET_PROP(js, verify_compression);
    ROCKSDB_JSON_SET_PROP(js, format_version);
    ROCKSDB_JSON_SET_PROP(js, enable_index_compression);
    ROCKSDB_JSON_SET_PROP(js, block_align);
    ROCKSDB_JSON_SET_FACX(js, block_cache, cache);
    ROCKSDB_JSON_SET_FACX(js, block_cache_compressed, cache);
    ROCKSDB_JSON_SET_FACT(js, persistent_cache);
    ROCKSDB_JSON_SET_FACT(js, filter_policy);
    return js;
  }
  std::string ToJsonStr(const json& dump_options,
                        const JsonPluginRepo& repo) const {
    auto js = ToJsonObj(dump_options, repo);
    return JsonToString(js, dump_options);
  }
};

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
NewBlockBasedTableFactoryFromJson(const json& js, const JsonPluginRepo& repo) {
  BlockBasedTableOptions_Json _table_options(js, repo);
  return std::make_shared<BlockBasedTableFactory>(_table_options);
}
ROCKSDB_FACTORY_REG("BlockBasedTable", NewBlockBasedTableFactoryFromJson);
ROCKSDB_FACTORY_REG("BlockBased", NewBlockBasedTableFactoryFromJson);

struct BlockBasedTableFactory_Manip : PluginManipFunc<TableFactory> {
  void Update(TableFactory* p, const json& js,
              const JsonPluginRepo& repo) const final {
    if (auto t = dynamic_cast<BlockBasedTableFactory*>(p)) {
      auto o = static_cast<const BlockBasedTableOptions_Json&>(t->table_options());
      auto mo = const_cast<BlockBasedTableOptions_Json&>(o);
      mo.Update(js, repo);
      return;
    }
    std::string name = p->Name();
    THROW_InvalidArgument("Is not DispatherTable, but is: " + name);
  }
  std::string ToString(const TableFactory& fac, const json& dump_options,
                       const JsonPluginRepo& repo) const final {
    if (auto t = dynamic_cast<const BlockBasedTableFactory*>(&fac)) {
      auto o = static_cast<const BlockBasedTableOptions_Json&>(t->table_options());
      return o.ToJsonStr(dump_options, repo);
    }
    std::string name = fac.Name();
    THROW_InvalidArgument("Is not TerarkZipTable, but is: " + name);
  }
};

static const PluginManipFunc<TableFactory>*
JS_BlockBasedTableFactoryManip(const json&, const JsonPluginRepo&) {
  static const BlockBasedTableFactory_Manip manip;
  return &manip;
}
ROCKSDB_FACTORY_REG("BlockBased", JS_BlockBasedTableFactoryManip);
ROCKSDB_FACTORY_REG("BlockBasedTable", JS_BlockBasedTableFactoryManip);

////////////////////////////////////////////////////////////////////////////
struct PlainTableOptions_Json : PlainTableOptions {
  PlainTableOptions_Json(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, user_key_len);
    ROCKSDB_JSON_OPT_PROP(js, bloom_bits_per_key);
    ROCKSDB_JSON_OPT_PROP(js, hash_table_ratio);
    ROCKSDB_JSON_OPT_PROP(js, index_sparseness);
    ROCKSDB_JSON_OPT_ENUM(js, encoding_type);
    ROCKSDB_JSON_OPT_PROP(js, full_scan_mode);
    ROCKSDB_JSON_OPT_PROP(js, store_index_in_file);
  }
};
static std::shared_ptr<TableFactory>
NewPlainTableFactoryFromJson(const json& js, const JsonPluginRepo&) {
  PlainTableOptions_Json options(js);
  return std::make_shared<PlainTableFactory>(options);
}
ROCKSDB_FACTORY_REG("PlainTable", NewPlainTableFactoryFromJson);

////////////////////////////////////////////////////////////////////////////
struct CuckooTableOptions_Json : CuckooTableOptions {
  CuckooTableOptions_Json(const json& js) {
    ROCKSDB_JSON_OPT_PROP(js, hash_table_ratio);
    ROCKSDB_JSON_OPT_PROP(js, max_search_depth);
    ROCKSDB_JSON_OPT_PROP(js, hash_table_ratio);
    ROCKSDB_JSON_OPT_PROP(js, cuckoo_block_size);
    ROCKSDB_JSON_OPT_PROP(js, identity_as_first_hash);
    ROCKSDB_JSON_OPT_PROP(js, use_module_hash);
  }
};
static std::shared_ptr<TableFactory>
NewCuckooTableFactoryJson(const json& js, const JsonPluginRepo&) {
  CuckooTableOptions_Json options(js);
  return std::shared_ptr<TableFactory>(NewCuckooTableFactory(options));
}
ROCKSDB_FACTORY_REG("CuckooTable", NewCuckooTableFactoryJson);

////////////////////////////////////////////////////////////////////////////

extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;
extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kCuckooTableMagicNumber;

// plugin TableFactory can using this function to register
// its TableMagicNumber
std::unordered_map<uint64_t, std::string>&
GetDispatherTableMagicNumberMap() {
  static std::unordered_map<uint64_t, std::string> map {
      {kPlainTableMagicNumber, "PlainTable"},
      {kLegacyPlainTableMagicNumber, "PlainTable"},
      {kBlockBasedTableMagicNumber, "BlockBasedTable"},
      {kLegacyBlockBasedTableMagicNumber, "BlockBasedTable"},
      {kCuckooTableMagicNumber, "CuckooTable"},
  };
  return map;
}
RegTableFactoryMagicNumber::
RegTableFactoryMagicNumber(uint64_t magic, const char* name) {
  auto ib = GetDispatherTableMagicNumberMap().emplace(magic, name);
  if (!ib.second) {
    fprintf(stderr,
        "ERROR: RegTableFactoryMagicNumber: dup: %016llX -> %s\n",
        (long long)magic, name);
    abort();
  }
}

////////////////////////////////////////////////////////////////////////////
static std::shared_ptr<SstPartitionerFactory>
NewFixedPrefixPartitionerFactoryJson(const json& js, const JsonPluginRepo&) {
  size_t prefix_len = 0;
  ROCKSDB_JSON_REQ_PROP(js, prefix_len);
  return NewSstPartitionerFixedPrefixFactory(prefix_len);
}
ROCKSDB_FACTORY_REG("SstPartitionerFixedPrefixFactory",
                    NewFixedPrefixPartitionerFactoryJson);
ROCKSDB_FACTORY_REG("SstPartitionerFixedPrefix",
                    NewFixedPrefixPartitionerFactoryJson);
ROCKSDB_FACTORY_REG("FixedPrefix", NewFixedPrefixPartitionerFactoryJson);

////////////////////////////////////////////////////////////////////////////

using namespace std::chrono;
class DispatherTableFactory;

// for hook TableBuilder::Add(key, value) to perform statistics
class DispatherTableBuilder : public TableBuilder {
 public:
  TableBuilder* tb = nullptr;
  const DispatherTableFactory* dispatcher = nullptr;
  using Stat = DispatherTableFactory::Stat;
  Stat st;
  Stat st_sum;
  int  level;
  DispatherTableBuilder(TableBuilder* tb1,
                        const DispatherTableFactory* dtf1,
                        int level1);
  ~DispatherTableBuilder();
  void UpdateStat();
  void Add(const Slice& key, const Slice& value) final;
  Status status() const final { return tb->status(); }
  IOStatus io_status() const final { return tb->io_status(); }
  Status Finish() final { return tb->Finish(); }
  void Abandon() final { tb->Abandon(); }
  uint64_t NumEntries() const final { return tb->NumEntries(); }
  bool IsEmpty() const { return tb->IsEmpty(); }
  uint64_t FileSize() const final { return tb->FileSize(); }
  uint64_t EstimatedFileSize() const { return tb->EstimatedFileSize(); }
  bool NeedCompact() const { return tb->NeedCompact(); }
  TableProperties GetTableProperties() const final { return tb->GetTableProperties(); }
  std::string GetFileChecksum() const final { return tb->GetFileChecksum(); }
  const char* GetFileChecksumFuncName() const final { return tb->GetFileChecksumFuncName(); }
};

DispatherTableFactory::~DispatherTableFactory() {}

DispatherTableFactory::
DispatherTableFactory(const json& js, const JsonPluginRepo& repo) {
  m_json_obj = js; // backup
  m_json_str = js.dump();
  m_is_back_patched = false;
}

const char* DispatherTableFactory::Name() const {
  return "DispatherTable";
}

Status DispatherTableFactory::NewTableReader(
    const ReadOptions& ro,
    const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table,
    bool prefetch_index_and_filter_in_cache) const {
  if (!m_is_back_patched) {
    return Status::InvalidArgument(
        ROCKSDB_FUNC, "BackPatch() was not called");
  }
  auto info_log = table_reader_options.ioptions.info_log;
  Footer footer;
  auto s = ReadFooterFromFile(IOOptions(),
                              file.get(), nullptr /* prefetch_buffer */,
                              file_size, &footer);
  if (!s.ok()) {
    return s;
  }
  const char* func = "Dispatch::NewTableReader"; // short func name
  auto magic = (unsigned long long)footer.table_magic_number();
  auto fp_iter = m_magic_to_factory.find(magic);
  if (m_magic_to_factory.end() != fp_iter) {
    const std::shared_ptr<TableFactory>& factory = fp_iter->second.factory;
    const std::string&                   varname = fp_iter->second.varname;
    Info(info_log, "%s: found factory: %016llX : %s: %s\n",
         func, magic, factory->Name(), varname.c_str());
    fp_iter->second.open_cnt++;
    fp_iter->second.sum_open_size += file_size;
    return factory->NewTableReader(ro, table_reader_options,
                                   std::move(file), file_size, table,
                                   prefetch_index_and_filter_in_cache);
  }
  auto& map = GetDispatherTableMagicNumberMap();
  auto iter = map.find(magic);
  if (map.end() != iter) {
    const std::string& facname = iter->second;
    if (PluginFactorySP<TableFactory>::HasPlugin(facname)) {
      try {
        Warn(info_log,
            "%s: not found factory: %016llX : %s, onfly create it.\n",
            func, magic, facname.c_str());
        json null_js;
        JsonPluginRepo empty_repo;
        auto factory = PluginFactorySP<TableFactory>::
                AcquirePlugin(facname, null_js, empty_repo);
        return factory->NewTableReader(ro, table_reader_options,
                                       std::move(file), file_size, table,
                                       prefetch_index_and_filter_in_cache);
      }
      catch (const std::exception& ex) {
        return Status::Corruption(ROCKSDB_FUNC,
          "onfly create factory=\"" + facname + "\" failed: " + ex.what());
      }
      catch (const Status& es) {
        return Status::Corruption(ROCKSDB_FUNC,
          "onfly create factory=\"" + facname + "\" failed: " + es.ToString());
      }
    } else {
      std::string msg;
      msg += "MagicNumber = ";
      msg += Slice((char*)&iter->first, 8).ToString(true);
      msg += " -> Factory class = " + facname;
      msg += " is defined in TableMagicNumberMap but not registered as a plugin.";
      return Status::Corruption(ROCKSDB_FUNC, msg);
    }
  }
  return Status::NotSupported("Unidentified table format");
}

TableBuilder* DispatherTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options,
    uint32_t column_family_id, WritableFileWriter* file)
const {
  auto info_log = table_builder_options.ioptions.info_log;
  if (!m_is_back_patched) {
    fprintf(stderr, "FATAL: %s:%d: %s: %s\n",
            __FILE__, __LINE__, ROCKSDB_FUNC,
            "BackPatch() was not called");
    abort();
  }
  int level = std::min(table_builder_options.level,
                       int(m_level_writers.size()-1));
  TableBuilder* builder;
  if (level >= 0) {
    if (JsonPluginRepo::DebugLevel() >= 3) {
      Info(info_log,
        "Dispatch::NewTableBuilder: level = %d, use level factory = %s\n",
        level, m_level_writers[level]->Name());
    }
    builder = m_level_writers[level]->NewTableBuilder(
        table_builder_options, column_family_id, file);
    if (!builder) {
      Warn(info_log,
        "Dispatch::NewTableBuilder: level = %d, use level factory = %s,"
        " returns null, try default: %s\n",
        level, m_level_writers[level]->Name(), m_default_writer->Name());
      builder = m_default_writer->NewTableBuilder(
          table_builder_options, column_family_id, file);
      m_writer_files[0]++;
    } else {
      m_writer_files[level+1]++;
    }
  }
  else {
    if (JsonPluginRepo::DebugLevel() >= 3) {
      Info(info_log,
        "Dispatch::NewTableBuilder: level = %d, use default factory = %s\n",
        level, m_default_writer->Name());
    }
    builder = m_default_writer->NewTableBuilder(
        table_builder_options, column_family_id, file);
    m_writer_files[0]++;
  }
  return new DispatherTableBuilder(builder, this, level);
}

// Sanitizes the specified DB Options.
Status DispatherTableFactory::ValidateOptions(const DBOptions&, const ColumnFamilyOptions&)
const {
  return Status::OK();
}

void DispatherTableBackPatch(TableFactory* f, const JsonPluginRepo& repo) {
  auto disptcher = dynamic_cast<DispatherTableFactory*>(f);
  assert(nullptr != disptcher);
  disptcher->BackPatch(repo);
}

extern bool IsCompactionWorker();
void DispatherTableFactory::BackPatch(const JsonPluginRepo& repo) {
  if (m_is_back_patched) {
    fprintf(stderr, "FATAL: %s:%d: %s: %s\n",
            __FILE__, __LINE__, ROCKSDB_FUNC,
            "BackPatch() was already called");
    abort();
  }
  assert(m_all.get() == nullptr);
  m_all = repo.m_impl->table_factory.name2p;
  if (!m_json_obj.is_object()) {
    THROW_InvalidArgument("DispatherTableFactory options must be object");
  }
  auto iter = m_json_obj.find("default");
  if (m_json_obj.end() != iter) {
    auto& subjs = iter.value();
    m_default_writer = PluginFactorySP<TableFactory>::
      ObtainPlugin("default", ROCKSDB_FUNC, subjs, repo);
    if (!m_default_writer) {
      THROW_InvalidArgument("fail get defined default writer = " + subjs.dump());
    }
  } else {
    auto iter2 = m_all->find("default");
    if (m_all->end() != iter2) {
      m_default_writer = iter2->second;
    } else {
     THROW_InvalidArgument("fail get global default Factory");
    }
  }
  iter = m_json_obj.find("level_writers");
  if (m_json_obj.end() != iter) {
    auto& level_writers_js = iter.value();
    if (!level_writers_js.is_array()) {
      THROW_InvalidArgument("level_writers must be a json array");
    }
    if (level_writers_js.empty()) {
      THROW_InvalidArgument("level_writers must be a non-empty json array");
    }
    for (auto& item : level_writers_js.items()) {
      auto& options = item.value();
      auto p = PluginFactorySP<TableFactory>::
        ObtainPlugin("default", ROCKSDB_FUNC, options, repo);
      if (!p) {
        THROW_InvalidArgument(
            "ObtainPlugin returns null, options = " + options.dump());
      }
      m_level_writers.push_back(p);
    }
  }
  for (auto& stv : m_stats) {
    stv.resize(m_level_writers.size() + 1);
  }
  m_writer_files.resize(m_level_writers.size() + 1);
  std::unordered_map<std::string, std::vector<uint64_t> > name2magic;
  for (auto& kv : GetDispatherTableMagicNumberMap()) {
    name2magic[kv.second].push_back(kv.first);
    //fprintf(stderr, "DEBG: %016llX : %s\n", (long long)kv.first, kv.second.c_str());
  }
  auto add_magic = [&](const std::shared_ptr<TableFactory>& factory,
                       const std::string& varname,
                       bool is_user_defined) {
    const char* facname = factory->Name();
    if (strcmp(facname, this->Name()) == 0) {
      if (is_user_defined) {
        THROW_InvalidArgument("Dispatch factory can not be defined as a reader");
      }
      return;
    }
    auto it = name2magic.find(facname);
    if (name2magic.end() == it) {
      THROW_InvalidArgument(
          std::string("not found magic of factory: ") + facname);
    }
    for (uint64_t magic : it->second) {
      ReaderFactory rf;
      rf.factory = factory;
      rf.varname = varname;
      rf.is_user_defined = is_user_defined;
      auto ib = m_magic_to_factory.emplace(magic, rf);
      if (!ib.second) { // emplace fail
        const char* varname1 = ib.first->second.varname.c_str(); // existed
        const char* type = ib.first->second.is_user_defined ? "user" : "auto";
        if (JsonPluginRepo::DebugLevel() >= 2)
          fprintf(stderr,
                "INFO: Dispatch::BackPatch: dup factory: %016llX : %-20s : %s(%s) %s(auto)\n",
                (long long)magic, facname, varname1, type, varname.c_str());
      } else if (JsonPluginRepo::DebugLevel() >= 2) {
        fprintf(stderr,
                "INFO: Dispatch::BackPatch: reg factory: %016llX : %-20s : %s\n",
                (long long)magic, facname, varname.c_str());
      }
    }
  };
  iter = m_json_obj.find("readers");
  if (m_json_obj.end() != iter) {
    auto& readers_js = iter.value();
    if (!readers_js.is_object()) {
      THROW_InvalidArgument("readers must be a json object");
    }
    for (auto& item : readers_js.items()) {
      auto& facname = item.key();
      if (!item.value().is_string()) {
        THROW_InvalidArgument(
            "readers[\"" + facname + "\"] must be a json string");
      }
      if (!PluginFactorySP<TableFactory>::HasPlugin(facname)) {
        THROW_InvalidArgument(
            "facname = \"" + facname + "\" is not a plugin");
      }
      const std::string& varname = item.value().get_ref<const std::string&>();
      // facname is the param 'varname' for GetPlugin
      auto p = PluginFactorySP<TableFactory>::GetPlugin(
          facname.c_str(), ROCKSDB_FUNC, item.value(), repo);
      if (!p) {
        THROW_InvalidArgument(
            "undefined varname = " + varname + "(factory = " + facname + ")");
      }
      if (!PluginFactorySP<TableFactory>::SamePlugin(p->Name(), facname)) {
        THROW_InvalidArgument(
            "facname = " + facname + " but factory->Name() = " + p->Name());
      }
      add_magic(p, varname, true);
    }
  }
  for (auto& kv : *m_all) {
    auto& varname = kv.first; // factory varname
    auto& factory = kv.second;
    add_magic(factory, varname, false);
  }
  for (auto& kv : *m_all) {
    auto& factory = kv.second;
    const json* cons_params = repo.GetConsParams(factory);
    m_cons_params.emplace_back(factory.get(), cons_params);
  }
  std::sort(m_cons_params.begin(), m_cons_params.end());
  m_json_obj = json{}; // reset
  m_is_back_patched = true;
}

std::string DispatherTableFactory::GetPrintableOptions() const {
  return m_json_str;
}

json DispatherTableFactory::ToJsonObj(const json& dump_options, const JsonPluginRepo& repo) const {
  const bool html = JsonSmartBool(dump_options, "html");
  const bool nozero = JsonSmartBool(dump_options, "nozero");
  auto& p2name = repo.m_impl->table_factory.p2name;
  const static std::string labels[] = {
       "1s-ops",  "1s-key",  "1s-val",
       "5s-ops",  "5s-key",  "5s-val",
      "30s-ops", "30s-key", "30s-val",
       "5m-ops",  "5m-key",  "5m-val",
      "30m-ops", "30m-key", "30m-val",
  };
  json lwjs;
  auto add_writer = [&](const std::shared_ptr<TableFactory>& tf, size_t level) {
    size_t file_num = m_writer_files[level];
    if (nozero && 0 == file_num) {
      return;
    }
    json wjs;
    char buf[64];
#define ToStr(...) json(std::string(buf, snprintf(buf, sizeof(buf), __VA_ARGS__)))
    wjs["level"] = 0 == level ? json("default") : json(level-1);
    ROCKSDB_JSON_SET_FACT_INNER(wjs["factory"], tf, table_factory);
    const auto& st = m_stats[0][level];
    wjs["files"] = file_num;
    if (html) {
      wjs["entry_cnt"] = ToStr("%.3f M", st.st.entry_cnt/1e6);
      wjs["key_size"] = ToStr("%.3f G", st.st.key_size/1e9);
      wjs["val_size"] = ToStr("%.3f G", st.st.val_size/1e9);
      wjs["avg_file"] = ToStr("%.3f M", st.st.file_size/1e6/file_num);
    } else {
      wjs["entry_cnt"] = st.st.entry_cnt;
      wjs["key_size"] = st.st.key_size;
      wjs["val_size"] = st.st.val_size;
      wjs["avg_file"] = double(st.st.file_size) / file_num;
    }
    for (size_t j = 0; j < 5; ++j) {
      double cnt = st.st.entry_cnt - m_stats[j+1][level].st.entry_cnt;
      double key = st.st.key_size - m_stats[j+1][level].st.key_size;
      double val = st.st.val_size - m_stats[j+1][level].st.val_size;
      double us = duration_cast<microseconds>(st.time - m_stats[j+1][level].time).count();
      wjs[labels[3*j+0]] = !cnt ? json(0) : ToStr("%.3f K/s", cnt/us*1e3);
      wjs[labels[3*j+1]] = !key ? json(0) : ToStr("%.3f M/s", key/us);
      wjs[labels[3*j+2]] = !val ? json(0) : ToStr("%.3f M/s", val/us);
    }
    lwjs.push_back(std::move(wjs));
  };
  json js;
  for (size_t i = 0, n = m_level_writers.size(); i < n; ++i) {
    auto& tf = m_level_writers[i];
    auto iter = p2name.find(tf.get());
    if (p2name.end() == iter) {
      THROW_Corruption("missing TableFactory of m_level_writer");
    }
    add_writer(tf, i+1);
  }
  if (html && !m_level_writers.empty()) {
    auto& cols = lwjs[0]["<htmltab:col>"];
    cols = json::array({
        "level", "factory", "files", "avg_file",
        "entry_cnt", "key_size", "val_size",
    });
    for (auto& lab : labels) {
      cols.push_back(lab);
    }
  }
  add_writer(m_default_writer, 0);
  if (lwjs.empty()) {
    lwjs = "Did Not Created Any TableBuilder, try nozero=0";
  }
  js["writers"] = std::move(lwjs);

  json readers_js;
  for (auto& kv : m_magic_to_factory) {
    size_t len = kv.second.sum_open_size;
    size_t cnt = kv.second.open_cnt;
    if (nozero && 0 == cnt) {
      continue;
    }
    json one_js;
    char buf[64];
    one_js["class"] = kv.second.factory->Name();
    one_js["magic_num"] = ToStr("%llX", (long long)kv.first);
    ROCKSDB_JSON_SET_FACT_INNER(one_js["factory"], kv.second.factory, table_factory);
    one_js["open_cnt"] = cnt;
    one_js["sum_open_size"] = ToStr("%.3f G", len/1e9);
    one_js["avg_open_size"] = ToStr("%.3f M", len/1e6/cnt);
    readers_js.push_back(std::move(one_js));
  }
  if (readers_js.empty()) {
    readers_js = "Did Not Created Any TableReader, try nozero=0";
  }
  else {
    readers_js[0]["<htmltab:col>"] = json::array({
      "class", "magic_num", "factory", "open_cnt", "sum_open_size",
      "avg_open_size"
    });
  }
  js["readers"] = std::move(readers_js);
  return js;
}
std::string DispatherTableFactory::ToJsonStr(const json& dump_options,
                                             const JsonPluginRepo& repo) const {
  auto js = ToJsonObj(dump_options, repo);
  try {
    return JsonToString(js, dump_options);
  }
  catch (const std::exception& ex) {
    THROW_InvalidArgument(std::string(ex.what()) + ", json:\n" + js.dump());
  }
}

void DispatherTableFactory::UpdateOptions(const json& js, const JsonPluginRepo& repo) {

}

static const seconds g_durations[6] = {
    seconds(0),
    seconds(1),
    seconds(5),
    seconds(30),
    seconds(300), // 5 minutes
    seconds(1800), // 30 minutes
};

DispatherTableBuilder::DispatherTableBuilder(TableBuilder* tb1,
                      const DispatherTableFactory* dtf1,
                      int level1) {
  tb = tb1;
  dispatcher = dtf1;
  if (size_t(level1) < dtf1->m_level_writers.size())
    level = level1 + 1;
  else
    level = 0;
}

template<class T>
inline
std::atomic<T>& as_atomic(T& x) {
    return reinterpret_cast<std::atomic<T>&>(x);
}

DispatherTableBuilder::~DispatherTableBuilder() {
  UpdateStat();
  as_atomic(dispatcher->m_stats[0][level].st.file_size)
    .fetch_add(tb->FileSize(), std::memory_order_relaxed);
  delete tb;
}

void DispatherTableBuilder::Add(const Slice& key, const Slice& value) {
  st.entry_cnt++;
  st.key_size += key.size();
  st.val_size += value.size();
  if (UNLIKELY(st.entry_cnt % 1024 == 0)) {
    UpdateStat();
  }
  tb->Add(key, value);
}
void DispatherTableBuilder::UpdateStat() {
  st_sum.Add(st);
  const_cast<DispatherTableFactory*>(dispatcher)->UpdateStat(level, st);
  if (JsonPluginRepo::DebugLevel() >= 4) {
    fprintf(stderr, "DBUG: entry_cnt = %zd\n", st_sum.entry_cnt);
  }
  st.Reset();
}

void DispatherTableFactory::UpdateStat(size_t lev, const Stat& st) {
  auto tp = steady_clock::now();
  m_mtx.lock();
  m_stats[0][lev].time = tp;
  m_stats[0][lev].st.Add(st);
  for (size_t i = 1; i < 6; ++i) {
    assert(m_stats[i].size() == m_level_writers.size() + 1);
    assert(lev <= m_level_writers.size());
    auto& ts = m_stats[i][lev];
    if (JsonPluginRepo::DebugLevel() >= 5) {
      fprintf(stderr, "DBUG: tp-ts.time = %zd, g_durations[i] = %zd, (tp - ts.time > g_durations[i]) = %d\n",
              (size_t)duration_cast<seconds>(tp - ts.time).count(),
              (size_t)duration_cast<seconds>(g_durations[i]).count(),
              tp - ts.time > g_durations[i]);
    }
    if (tp - ts.time > g_durations[i]) {
      auto& newer = m_stats[i-1][lev];
      ts = newer; // refresh
    }
  }
  m_mtx.unlock();
}

static std::shared_ptr<TableFactory>
NewDispatcherTableFactoryJson(const json& js, const JsonPluginRepo& repo) {
  return std::make_shared<DispatherTableFactory>(js, repo);
}

struct DispatcherTableFactory_Manip : PluginManipFunc<TableFactory> {
  void Update(TableFactory* p, const json& js,
              const JsonPluginRepo& repo) const final {
    if (auto t = dynamic_cast<DispatherTableFactory*>(p)) {
      t->UpdateOptions(js, repo);
      return;
    }
    std::string name = p->Name();
    THROW_InvalidArgument("Is not DispatherTable, but is: " + name);
  }
  std::string ToString(const TableFactory& fac, const json& dump_options,
                       const JsonPluginRepo& repo) const final {
    if (auto t = dynamic_cast<const DispatherTableFactory*>(&fac)) {
      return t->ToJsonStr(dump_options, repo);
    }
    std::string name = fac.Name();
    THROW_InvalidArgument("Is not TerarkZipTable, but is: " + name);
  }
};

ROCKSDB_REG_PluginManip("Dispath", DispatcherTableFactory_Manip);
ROCKSDB_REG_PluginManip("Dispather", DispatcherTableFactory_Manip);
ROCKSDB_REG_PluginManip("DispatherTable", DispatcherTableFactory_Manip);

ROCKSDB_FACTORY_REG("Dispath", NewDispatcherTableFactoryJson);
ROCKSDB_FACTORY_REG("Dispather", NewDispatcherTableFactoryJson);
ROCKSDB_FACTORY_REG("DispatherTable", NewDispatcherTableFactoryJson);

void TableFactoryDummyFuncToPreventGccDeleteSymbols() {}

}
