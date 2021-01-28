//
// Created by leipeng on 2020/8/5.
//
#pragma once
#include "rocksdb/table.h"
#include <string>
#include <unordered_map>
#include <stdint.h>
#include "json_plugin_repo.h"
#include <chrono>

namespace ROCKSDB_NAMESPACE {

std::unordered_map<uint64_t, std::string>&
GetDispatherTableMagicNumberMap();

struct RegTableFactoryMagicNumber {
  RegTableFactoryMagicNumber(uint64_t, const char*);
};
#define ROCKSDB_RegTableFactoryMagicNumber(magic,name) \
  RegTableFactoryMagicNumber g_AutoRegTF_##magic(magic, name)

class DispatherTableFactory : public TableFactory {
public:
  ~DispatherTableFactory();
  DispatherTableFactory(const DispatherTableFactory&) = delete;
  DispatherTableFactory(const json& js, const JsonPluginRepo& repo);

  const char* Name() const final;

  using TableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& ro,
      const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table,
      bool prefetch_index_and_filter_in_cache) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, WritableFileWriter* file)
  const override;

  Status ValidateOptions(const DBOptions&, const ColumnFamilyOptions&) const override;
  std::string GetPrintableOptions() const override;

// non TableFactory methods:
  Status BackPatch(const JsonPluginRepo& repo);
  json ToJsonObj(const json& dump_options, const JsonPluginRepo& repo) const;
  std::string ToJsonStr(const json& dump_options,
                        const JsonPluginRepo& repo) const;
  void UpdateOptions(const json& js, const JsonPluginRepo& repo);

// should be protected, but use public for simple
  struct Stat {
    size_t entry_cnt = 0;
    size_t key_size = 0;
    size_t val_size = 0;
    unsigned long long file_size = 0;
    void Add(const Stat& y) {
      entry_cnt += y.entry_cnt;
      key_size += y.key_size;
      val_size += y.val_size;
    }
    void Reset() {
      entry_cnt = 0;
      key_size = 0;
      val_size = 0;
    }
  };
  struct TimeStat {
    Stat st;
    std::chrono::steady_clock::time_point time;
    TimeStat() { time = std::chrono::steady_clock::now(); }
  };
  struct ReaderFactory {
    std::shared_ptr<TableFactory> factory;
    std::string varname;
    size_t open_cnt = 0;
    unsigned long long sum_open_size = 0;
    bool is_user_defined;
  };
  void UpdateStat(size_t lev, const Stat& st);

protected:
  mutable std::mutex m_mtx;
  std::vector<std::shared_ptr<TableFactory> > m_level_writers;
  std::shared_ptr<TableFactory> m_default_writer;
  // 0s, 1s, 5s, 30s, 300s(5m), 1800(30m)
  mutable std::vector<TimeStat> m_stats[6];
  mutable std::vector<size_t> m_writer_files;
  std::shared_ptr<std::unordered_map<std::string,
      std::shared_ptr<TableFactory>>> m_all;
  std::vector<std::pair<const void*, const json*> > m_cons_params;
  std::string m_json_str;
  json m_json_obj{}; // reset to null after back patched
  mutable std::unordered_map<uint64_t, ReaderFactory> m_magic_to_factory;
  bool m_is_back_patched;
  friend class DispatherTableBuilder;
};

Status DispatherTableBackPatch(TableFactory* f, const JsonPluginRepo& repo);

} // ROCKSDB_NAMESPACE

