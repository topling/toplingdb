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
#include <memory>

namespace ROCKSDB_NAMESPACE {

struct Options;
struct DBOptions;
struct ColumnFamilyDescriptor;
struct ColumnFamilyOptions;
struct DB_Ptr;

class Cache;
class ColumnFamilyHandle;

class CompactionExecutorFactory;
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
class MemoryAllocator;
class MemTableRepFactory;
class MergeOperator;
class PersistentCache;
class RateLimiter;
class SliceTransform;
class SstFileManager;
class SstPartitionerFactory;
class Statistics;
class TableFactory;
class TablePropertiesCollectorFactory;
class TransactionDBMutexFactory;

using nlohmann::json;

struct DB_MultiCF {
  DB_MultiCF();
  virtual ~DB_MultiCF();
  virtual ColumnFamilyHandle* Get(const std::string& cfname) const = 0;
  virtual Status CreateColumnFamily(const std::string& cfname, const std::string& json_str, ColumnFamilyHandle**) = 0;
  virtual Status DropColumnFamily(const std::string& cfname) = 0;
  virtual Status DropColumnFamily(ColumnFamilyHandle*) = 0;
  ColumnFamilyHandle* operator[](const std::string& cfname) const { return Get(cfname); }

  DB* db = nullptr;
  std::vector<ColumnFamilyHandle*> cf_handles;
};

class JsonPluginRepo;
struct AnyPlugin {
  AnyPlugin(const AnyPlugin&) = delete;
  AnyPlugin& operator=(const AnyPlugin&) = delete;
  AnyPlugin() {}
  virtual ~AnyPlugin();
  virtual void Update(const json&, const JsonPluginRepo&) = 0;
  virtual std::string ToString(const json&, const JsonPluginRepo&) const = 0;
};

class JsonPluginRepo {
 public:
  JsonPluginRepo() noexcept;
  ~JsonPluginRepo();
  JsonPluginRepo(const JsonPluginRepo&) noexcept;
  JsonPluginRepo(JsonPluginRepo&&) noexcept;
  JsonPluginRepo& operator=(const JsonPluginRepo&) noexcept;
  JsonPluginRepo& operator=(JsonPluginRepo&&) noexcept;

  Status ImportJsonFile(const Slice& fname);
  Status Import(const std::string& json_str);
  Status Import(const nlohmann::json&);
  Status Export(nlohmann::json*) const;
  Status Export(std::string*, bool pretty = false) const;

  Status OpenDB(const std::string& js, DB**);
  Status OpenDB(const std::string& js, DB_MultiCF**);
  Status OpenDB(const nlohmann::json&, DB**);
  Status OpenDB(const nlohmann::json&, DB_MultiCF**);

  ///@{ open the DB defined in js["open"]
  Status OpenDB(DB**);
  Status OpenDB(DB_MultiCF**);
  //@}

  // dbmap is held by m_impl internally, if dbmap is null, user can still
  // get db by Get(dbname) -- if user knows dbname
  Status OpenAllDB();
  std::shared_ptr<std::map<std::string, DB_Ptr>> GetAllDB() const;

  Status StartHttpServer(); // http server for inspection
  void   CloseHttpServer();

  // user must ensure all dbs are alive when calling this function
  void CloseAllDB(bool del_rocksdb_objs = true);

  ///@{
  /// the semantic is overwrite
  /// Put(name, PtrType(nullptr)) means delete
  void Put(const std::string& name, const std::shared_ptr<Options>&);
  void Put(const std::string& name, const std::shared_ptr<DBOptions>&);
  void Put(const std::string& name, const std::shared_ptr<ColumnFamilyOptions>&);

  // The caller should ensure DB handle's life time is longer than JsonPluginRepo
  void Put(const std::string& name, DB*);
  void Put(const std::string& name, DB_MultiCF*);

  void Put(const std::string& name, const std::shared_ptr<AnyPlugin>&);
  void Put(const std::string& name, const std::shared_ptr<Cache>&);
  void Put(const std::string& name, const std::shared_ptr<CompactionExecutorFactory>&);
  void Put(const std::string& name, const std::shared_ptr<CompactionFilterFactory>&);
  void Put(const std::string& name, const Comparator*);
  void Put(const std::string& name, const std::shared_ptr<ConcurrentTaskLimiter>&);
  void Put(const std::string& name, Env*);
  void Put(const std::string& name, const std::shared_ptr<EventListener>&);
  void Put(const std::string& name, const std::shared_ptr<FileChecksumGenFactory>&);
  void Put(const std::string& name, const std::shared_ptr<FileSystem>&);
  void Put(const std::string& name, const std::shared_ptr<const FilterPolicy>&);
  void Put(const std::string& name, const std::shared_ptr<FlushBlockPolicyFactory>&);
  void Put(const std::string& name, const std::shared_ptr<Logger>&);
  void Put(const std::string& name, const std::shared_ptr<MemoryAllocator>&);
  void Put(const std::string& name, const std::shared_ptr<MemTableRepFactory>&);
  void Put(const std::string& name, const std::shared_ptr<MergeOperator>&);
  void Put(const std::string& name, const std::shared_ptr<PersistentCache>&);
  void Put(const std::string& name, const std::shared_ptr<RateLimiter>&);
  void Put(const std::string& name, const std::shared_ptr<const SliceTransform>&);
  void Put(const std::string& name, const std::shared_ptr<SstFileManager>&);
  void Put(const std::string& name, const std::shared_ptr<SstPartitionerFactory>&);
  void Put(const std::string& name, const std::shared_ptr<Statistics>&);
  void Put(const std::string& name, const std::shared_ptr<TableFactory>&);
  void Put(const std::string& name, const std::shared_ptr<TablePropertiesCollectorFactory>&);
  void Put(const std::string& name, const std::shared_ptr<TransactionDBMutexFactory>&);
  ///@}

  bool Get(const std::string& name, std::shared_ptr<Options>*) const;
  bool Get(const std::string& name, std::shared_ptr<DBOptions>*) const;
  bool Get(const std::string& name, std::shared_ptr<ColumnFamilyOptions>*) const;

  // The caller should ensure DB handle is alive
  bool Get(const std::string& name, DB**, Status* = nullptr) const;
  bool Get(const std::string& name, DB_MultiCF**, Status* = nullptr) const;

  bool Get(const std::string& name, std::shared_ptr<AnyPlugin>*) const;
  bool Get(const std::string& name, std::shared_ptr<Cache>*) const;
  bool Get(const std::string& name, std::shared_ptr<CompactionExecutorFactory>*) const;
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
  bool Get(const std::string& name, std::shared_ptr<MemoryAllocator>*) const;
  bool Get(const std::string& name, std::shared_ptr<MemTableRepFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<MergeOperator>*) const;
  bool Get(const std::string& name, std::shared_ptr<PersistentCache>*) const;
  bool Get(const std::string& name, std::shared_ptr<RateLimiter>*) const;
  bool Get(const std::string& name, std::shared_ptr<const SliceTransform>*) const;
  bool Get(const std::string& name, std::shared_ptr<SstFileManager>*) const;
  bool Get(const std::string& name, std::shared_ptr<SstPartitionerFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<Statistics>*) const;
  bool Get(const std::string& name, std::shared_ptr<TableFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<TablePropertiesCollectorFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<TransactionDBMutexFactory>*) const;

  class Auto {
    friend class JsonPluginRepo;
    const JsonPluginRepo& m_repo;
    const std::string&    m_name;
    Auto(const JsonPluginRepo& repo, const std::string& name)
        : m_repo(repo), m_name(name) {}
    Auto(const Auto&) = default;
    Auto(Auto&&) = default;
   public:
    template<class Ptr>
    operator Ptr() && { Ptr p(nullptr); m_repo.Get(m_name, &p); return p; }
  };
  /// sample usage:
  /// std::shared_ptr<TableFactory> factory = repo["BlockBasedTable"];
  Auto Get(const std::string& name) const { return Auto(*this, name); }
  Auto operator[](const std::string& name) const { return Auto(*this, name); }

  const json* GetConsParams(const std::shared_ptr<Options>&) const;
  const json* GetConsParams(const std::shared_ptr<DBOptions>&) const;
  const json* GetConsParams(const std::shared_ptr<ColumnFamilyOptions>&) const;
  const json* GetConsParams(const std::shared_ptr<AnyPlugin>&) const;
  const json* GetConsParams(const std::shared_ptr<Cache>&) const;
  const json* GetConsParams(const std::shared_ptr<CompactionExecutorFactory>&) const;
  const json* GetConsParams(const std::shared_ptr<CompactionFilterFactory>&) const;
  const json* GetConsParams(const Comparator*) const;
  const json* GetConsParams(const std::shared_ptr<ConcurrentTaskLimiter>&) const;
  const json* GetConsParams(Env*) const;
  const json* GetConsParams(const std::shared_ptr<EventListener>&) const;
  const json* GetConsParams(const std::shared_ptr<FileChecksumGenFactory>&) const;
  const json* GetConsParams(const std::shared_ptr<FileSystem>&) const;
  const json* GetConsParams(const std::shared_ptr<const FilterPolicy>&) const;
  const json* GetConsParams(const std::shared_ptr<FlushBlockPolicyFactory>&) const;
  const json* GetConsParams(const std::shared_ptr<Logger>&) const;
  const json* GetConsParams(const std::shared_ptr<MemoryAllocator>&) const;
  const json* GetConsParams(const std::shared_ptr<MemTableRepFactory>&) const;
  const json* GetConsParams(const std::shared_ptr<MergeOperator>&) const;
  const json* GetConsParams(const std::shared_ptr<PersistentCache>&) const;
  const json* GetConsParams(const std::shared_ptr<RateLimiter>&) const;
  const json* GetConsParams(const std::shared_ptr<const SliceTransform>&) const;
  const json* GetConsParams(const std::shared_ptr<SstFileManager>&) const;
  const json* GetConsParams(const std::shared_ptr<SstPartitionerFactory>&) const;
  const json* GetConsParams(const std::shared_ptr<Statistics>&) const;
  const json* GetConsParams(const std::shared_ptr<TableFactory>&) const;
  const json* GetConsParams(const std::shared_ptr<TablePropertiesCollectorFactory>&) const;
  const json* GetConsParams(const std::shared_ptr<TransactionDBMutexFactory>&) const;

  const json* GetConsParams(const Options*) const;
  const json* GetConsParams(const DBOptions*) const;
  const json* GetConsParams(const ColumnFamilyOptions*) const;
  const json* GetConsParams(const AnyPlugin*) const;
  const json* GetConsParams(const Cache*) const;
  const json* GetConsParams(const CompactionExecutorFactory*) const;
  const json* GetConsParams(const CompactionFilterFactory*) const;
  const json* GetConsParams(const ConcurrentTaskLimiter*) const;
  const json* GetConsParams(const EventListener*) const;
  const json* GetConsParams(const FileChecksumGenFactory*) const;
  const json* GetConsParams(const FileSystem*) const;
  const json* GetConsParams(const FilterPolicy*) const;
  const json* GetConsParams(const FlushBlockPolicyFactory*) const;
  const json* GetConsParams(const Logger*) const;
  const json* GetConsParams(const MemoryAllocator*) const;
  const json* GetConsParams(const MemTableRepFactory*) const;
  const json* GetConsParams(const MergeOperator*) const;
  const json* GetConsParams(const PersistentCache*) const;
  const json* GetConsParams(const RateLimiter*) const;
  const json* GetConsParams(const SliceTransform*) const;
  const json* GetConsParams(const SstFileManager*) const;
  const json* GetConsParams(const SstPartitionerFactory*) const;
  const json* GetConsParams(const Statistics*) const;
  const json* GetConsParams(const TableFactory*) const;
  const json* GetConsParams(const TablePropertiesCollectorFactory*) const;
  const json* GetConsParams(const TransactionDBMutexFactory*) const;

  static int DebugLevel();

// protected:
// also public
  struct Impl;
  std::shared_ptr<Impl> m_impl;
 private:
  template<class DBType>
  Status OpenDB_tpl(const nlohmann::json&, DBType**);
};

///@param obj json object to be dumped
///@param options options for dump(pretty,indent)
std::string JsonToString(const json& obj, const json& options);

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

namespace ROCKSDB_NAMESPACE {
struct DB_Ptr {
  DB* db = nullptr;
  DB_MultiCF* dbm = nullptr;

  DB_Ptr(DB* db1) : db(db1), dbm(nullptr) {}
  DB_Ptr(DB_MultiCF* dbm1) : db(dbm1->db), dbm(dbm1) {}
  DB_Ptr(std::nullptr_t) : db(nullptr), dbm(nullptr) {}
  bool operator!() const { return nullptr == db; }
  operator bool () const { return nullptr != db; }
  const DB_Ptr& operator*() const { assert(nullptr != db); return *this; }
};
inline bool operator==(const DB_Ptr& x, const DB_Ptr& y) {
  assert(!((x.db == y.db) ^ (x.dbm == y.dbm)));
  return x.db == y.db;
}
inline bool operator!=(const DB_Ptr& x, const DB_Ptr& y) { return !(x == y); }

}  // namespace ROCKSDB_NAMESPACE
