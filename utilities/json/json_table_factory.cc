//
// Created by leipeng on 2020/7/1.
//
#include "table/block_based/block_based_table_factory.h"
#include "table/plain/plain_table_factory.h"
#include "table/table_builder.h"
#include "json.h"
#include "json_plugin_factory.h"
#include "json_table_factory.h"

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

class DispatherTableFactory : public TableFactory {
 public:
  ~DispatherTableFactory() {}

  DispatherTableFactory(const json& js, const JsonPluginRepo& repo) {
    m_json_obj = js; // backup
    m_json_str = js.dump();
    m_repo.reset(new JsonPluginRepo(repo)); // backup
  }

  const char* Name() const final { return "DispatherTableFactory"; }

  using TableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& ro,
      const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table,
      bool prefetch_index_and_filter_in_cache) const override {
    assert(m_repo.get() == nullptr);
    if (m_repo) {
      return Status::InvalidArgument(
          ROCKSDB_FUNC, "SanitizeOptions() was not called");
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
                AcquirePlugin(
              facname, null_js, empty_repo);
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

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, WritableFileWriter* file)
  const override {
    assert(m_repo.get() == nullptr);
    auto info_log = table_builder_options.ioptions.info_log;
    if (m_repo) {
      fprintf(stderr, "FATAL: %s:%d: %s: %s\n",
              __FILE__, __LINE__, ROCKSDB_FUNC,
              "SanitizeOptions() was not called");
      abort();
    }
    int level = table_builder_options.level;
    if (size_t(level) < m_level_writers.size()) {
      if (JsonPluginRepo::DebugLevel() >= 3) {
        Info(info_log,
          "Dispatch::NewTableBuilder: level = %d, use level factory = %s\n",
          level, m_level_writers[level]->Name());
      }
      auto builder = m_level_writers[level]->NewTableBuilder(
          table_builder_options, column_family_id, file);
      if (builder) {
        return builder;
      }
      Warn(info_log,
        "Dispatch::NewTableBuilder: level = %d, use level factory = %s,"
        " returns null, try default: %s\n",
        level, m_level_writers[level]->Name(), m_default_writer->Name());
      return m_default_writer->NewTableBuilder(
          table_builder_options, column_family_id, file);
    }
    else {
      if (JsonPluginRepo::DebugLevel() >= 3) {
        Info(info_log,
          "Dispatch::NewTableBuilder: level = %d, use default factory = %s\n",
          level, m_default_writer->Name());
      }
      return m_default_writer->NewTableBuilder(
          table_builder_options, column_family_id, file);
    }
  }

  // Sanitizes the specified DB Options.
  Status SanitizeOptions(const DBOptions&, const ColumnFamilyOptions&)
  const override {
    Status s;
    if (m_repo) {
      // m_repo is non-null indicate BackPatch was not called
      s = BackPatch();
    }
    return s;
  }
  Status BackPatch() const try {
    m_all = m_repo->m_impl->table_factory.name2p;
    if (!m_json_obj.is_object()) {
      return Status::InvalidArgument(ROCKSDB_FUNC,
          "DispatherTableFactory options must be object");
    }
    auto iter = m_json_obj.find("default");
    if (m_json_obj.end() != iter) {
      auto& subjs = iter.value();
      m_default_writer = PluginFactorySP<TableFactory>::
        ObtainPlugin("default", ROCKSDB_FUNC, subjs, *m_repo);
      if (!m_default_writer) {
        return Status::InvalidArgument(ROCKSDB_FUNC,
            "fail get defined default writer = " + subjs.dump());
      }
    } else {
      auto iter2 = m_all->find("default");
      if (m_all->end() != iter2) {
        m_default_writer = iter2->second;
      } else {
        return Status::InvalidArgument(ROCKSDB_FUNC,
            "fail get global default Factory");
      }
    }
    iter = m_json_obj.find("level_writers");
    if (m_json_obj.end() != iter) {
      auto& level_writers_js = iter.value();
      if (!level_writers_js.is_array()) {
        return Status::InvalidArgument(ROCKSDB_FUNC,
            "level_writers must be a json array");
      }
      for (auto& item : level_writers_js.items()) {
        auto& options = item.value();
        auto p = PluginFactorySP<TableFactory>::
          ObtainPlugin("default", ROCKSDB_FUNC, options, *m_repo);
        if (!p) {
          return Status::InvalidArgument(ROCKSDB_FUNC,
              "ObtainPlugin returns null, options = " + options.dump());
        }
        m_level_writers.push_back(p);
      }
    }
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
          throw Status::InvalidArgument(ROCKSDB_FUNC,
                   "Dispatch factory can not be defined as a reader");
        }
        return;
      }
      auto it = name2magic.find(facname);
      if (name2magic.end() == it) {
        throw Status::InvalidArgument(ROCKSDB_FUNC,
            std::string("not found magic of factory: ") + facname);
      }
      for (uint64_t magic : it->second) {
        ReaderFactory rf{factory, varname, is_user_defined};
        auto ib = m_magic_to_factory.emplace(magic, rf);
        if (!ib.second) { // emplace fail
          const char* varname1 = ib.first->second.varname.c_str(); // existed
          const char* type = ib.first->second.is_user_defined ? "user" : "auto";
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
        return Status::InvalidArgument(ROCKSDB_FUNC,
                                       "readers must be a json object");
      }
      for (auto& item : readers_js.items()) {
        auto& facname = item.key();
        if (!item.value().is_string()) {
          return Status::InvalidArgument(ROCKSDB_FUNC,
              "readers[\"" + facname + "\"] must be a json string");
        }
        if (!PluginFactorySP<TableFactory>::HasPlugin(facname)) {
          return Status::InvalidArgument(ROCKSDB_FUNC,
              "facname = \"" + facname + "\" is not a plugin");
        }
        const std::string& varname = item.value().get<std::string>();
        // facname is the param 'varname' for GetPlugin
        auto p = PluginFactorySP<TableFactory>::GetPlugin(
            facname.c_str(), ROCKSDB_FUNC, item.value(), *m_repo);
        if (!p) {
          return Status::InvalidArgument(ROCKSDB_FUNC,
              "undefined varname = " + varname + "(factory = " + facname + ")");
        }
        if (!PluginFactorySP<TableFactory>::SamePlugin(p->Name(), facname)) {
          return Status::InvalidArgument(ROCKSDB_FUNC,
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
    m_repo.reset();
    m_json_obj = json{}; // reset
    return Status::OK();
  }
  catch (const std::exception& ex) {
    return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
  }
  catch (const Status& s) {
    return s;
  }

  std::string GetPrintableTableOptions() const override {
    return m_json_str;
  }

  Status GetOptionString(const ConfigOptions&,
                         std::string* opt_string) const override {
      *opt_string = m_json_str;
      return Status::OK();
  }

  mutable std::vector<std::shared_ptr<TableFactory> > m_level_writers;
  mutable std::shared_ptr<TableFactory> m_default_writer;
  mutable std::shared_ptr<std::unordered_map<std::string,
                                     std::shared_ptr<TableFactory>>> m_all;
  mutable std::string m_json_str;
  mutable json m_json_obj{}; // reset to null after back patched
  mutable std::unique_ptr<JsonPluginRepo> m_repo; // for back patch
  struct ReaderFactory {
    std::shared_ptr<TableFactory> factory;
    std::string varname;
    bool is_user_defined;
  };
  mutable std::unordered_map<uint64_t, ReaderFactory> m_magic_to_factory;
};

static std::shared_ptr<TableFactory>
NewDispatcherTableFactoryJson(const json& js, const JsonPluginRepo& repo) {
  return std::make_shared<DispatherTableFactory>(js, repo);
}
ROCKSDB_FACTORY_REG("Dispath", NewDispatcherTableFactoryJson);
ROCKSDB_FACTORY_REG("Dispather", NewDispatcherTableFactoryJson);
ROCKSDB_FACTORY_REG("DispatherTable", NewDispatcherTableFactoryJson);

void TableFactoryDummyFuncToPreventGccDeleteSymbols() {}

}
