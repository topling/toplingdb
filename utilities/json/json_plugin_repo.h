//
// Created by leipeng on 2020/7/2.
//
// json_plugin_repo.h    is mostly for plugin users
// json_plugin_factory.h is mostly for plugin developers
//
#pragma once

#include "json_fwd.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct Options;
struct DBOptions;
struct ColumnFamilyDescriptor;
struct ColumnFamilyOptions;

class Cache;
class ColumnFamilyHandle;
class CompactionFilterFactory;
class Comparator;
class ConcurrentTaskLimiter;
class DB;
class Env;
class EventListener;
class FileChecksumGenFactory;
class FileSystem;
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

struct DB_MultiCF {
  DB_MultiCF();
  ~DB_MultiCF();
  DB* db = nullptr;
  std::vector<ColumnFamilyDescriptor> cf_descriptors;
  std::vector<ColumnFamilyHandle*> cf_handles;
};

class JsonOptionsRepo {
 public:
  JsonOptionsRepo() noexcept;
  ~JsonOptionsRepo();
  JsonOptionsRepo(const JsonOptionsRepo&) noexcept;
  JsonOptionsRepo(JsonOptionsRepo&&) noexcept;
  JsonOptionsRepo& operator=(const JsonOptionsRepo&) noexcept;
  JsonOptionsRepo& operator=(JsonOptionsRepo&&) noexcept;

  Status ImportJsonFile(const Slice& fname);
  Status Import(const std::string& json_str);
  Status Import(const nlohmann::json&);
  Status Export(nlohmann::json*) const;
  Status Export(std::string*, bool pretty = false) const;

  Status OpenDB(const std::string& js, DB**);
  Status OpenDB(const std::string& js, DB_MultiCF**);
  Status OpenDB(const nlohmann::json&, DB**);
  Status OpenDB(const nlohmann::json&, DB_MultiCF**);

  const std::vector<ColumnFamilyHandle*>& CFHandles() const;
  std::vector<ColumnFamilyHandle*>& CFHandles();

  const std::vector<ColumnFamilyDescriptor>& CFDescriptors() const;
  std::vector<ColumnFamilyDescriptor>& CFDescriptors();

  ///@{
  /// the semantic is overwrite
  void Add(const std::string& name, const std::shared_ptr<Options>&);
  void Add(const std::string& name, const std::shared_ptr<DBOptions>&);
  void Add(const std::string& name, const std::shared_ptr<ColumnFamilyOptions>&);

  void Add(const std::string& name, const std::shared_ptr<Cache>&);
  void Add(const std::string& name, const std::shared_ptr<CompactionFilterFactory>&);
  void Add(const std::string& name, const Comparator*);
  void Add(const std::string& name, const std::shared_ptr<ConcurrentTaskLimiter>&);
  void Add(const std::string& name, Env*);
  void Add(const std::string& name, const std::shared_ptr<EventListener>&);
  void Add(const std::string& name, const std::shared_ptr<FileChecksumGenFactory>&);
  void Add(const std::string& name, const std::shared_ptr<FileSystem>&);
  void Add(const std::string& name, const std::shared_ptr<const FilterPolicy>&);
  void Add(const std::string& name, const std::shared_ptr<FlushBlockPolicyFactory>&);
  void Add(const std::string& name, const std::shared_ptr<Logger>&);
  void Add(const std::string& name, const std::shared_ptr<MemTableRepFactory>&);
  void Add(const std::string& name, const std::shared_ptr<MergeOperator>&);
  void Add(const std::string& name, const std::shared_ptr<PersistentCache>&);
  void Add(const std::string& name, const std::shared_ptr<RateLimiter>&);
  void Add(const std::string& name, const std::shared_ptr<const SliceTransform>&);
  void Add(const std::string& name, const std::shared_ptr<SstFileManager>&);
  void Add(const std::string& name, const std::shared_ptr<Statistics>&);
  void Add(const std::string& name, const std::shared_ptr<TableFactory>&);
  void Add(const std::string& name, const std::shared_ptr<TablePropertiesCollectorFactory>&);
  ///@}

  bool Get(const std::string& name, std::shared_ptr<Options>*) const;
  bool Get(const std::string& name, std::shared_ptr<DBOptions>*) const;
  bool Get(const std::string& name, std::shared_ptr<ColumnFamilyOptions>*) const;

  bool Get(const std::string& name, std::shared_ptr<Cache>*) const;
  bool Get(const std::string& name, std::shared_ptr<CompactionFilterFactory>*) const;
  bool Get(const std::string& name, const Comparator**) const;
  bool Get(const std::string& name, std::shared_ptr<ConcurrentTaskLimiter>*) const;
  bool Get(const std::string& name, Env**) const;
  bool Get(const std::string& name, std::shared_ptr<EventListener>*) const;
  bool Get(const std::string& name, std::shared_ptr<FileChecksumGenFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<FileSystem>*) const;
  bool Get(const std::string& name, std::shared_ptr<const FilterPolicy>*) const;
  bool Get(const std::string& name, std::shared_ptr<FlushBlockPolicyFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<Logger>*) const;
  bool Get(const std::string& name, std::shared_ptr<MemTableRepFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<MergeOperator>*) const;
  bool Get(const std::string& name, std::shared_ptr<PersistentCache>*) const;
  bool Get(const std::string& name, std::shared_ptr<RateLimiter>*) const;
  bool Get(const std::string& name, std::shared_ptr<const SliceTransform>*) const;
  bool Get(const std::string& name, std::shared_ptr<SstFileManager>*) const;
  bool Get(const std::string& name, std::shared_ptr<Statistics>*) const;
  bool Get(const std::string& name, std::shared_ptr<TableFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<TablePropertiesCollectorFactory>*) const;

  void GetMap(std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<TableFactory>>>*) const;
  void GetMap(std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<MemTableRepFactory>>>*) const;

  static int DebugLevel();

// protected:
// also public
  struct Impl;
  std::shared_ptr<Impl> m_impl;
 private:
  template<class DBType>
  Status OpenDB_tpl(const nlohmann::json&, DBType**);
};

Status JS_Str_OpenDB(const std::string& js_str, DB**);
Status JS_Str_OpenDB(const std::string& js_str, DB_MultiCF**);

Status JS_File_OpenDB(const std::string& js_file, DB**);
Status JS_File_OpenDB(const std::string& js_file, DB_MultiCF**);

class ParseSizeXiB {
  long long m_val;
public:
  explicit ParseSizeXiB(const char* s);
  explicit ParseSizeXiB(const std::string& s);
  explicit ParseSizeXiB(const nlohmann::json&);
  explicit ParseSizeXiB(const nlohmann::json&, const char* key);
  operator int() const;
  operator long() const;
  operator long long() const;
  operator unsigned int() const;
  operator unsigned long() const;
  operator unsigned long long() const;
};


}  // namespace ROCKSDB_NAMESPACE
