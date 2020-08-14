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
struct DB_Ptr;

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
class MemoryAllocator;
class MemTableRepFactory;
class MergeOperator;
class PersistentCache;
class RateLimiter;
class SliceTransform;
class SstFileManager;
class Statistics;
class TableFactory;
class TablePropertiesCollectorFactory;
class TransactionDBMutexFactory;

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

  // dbmap is held by m_impl internally, if dbmap is null, user can still
  // get db by Get(dbname) -- if user knows dbname
  Status OpenAllDB(std::shared_ptr<std::unordered_map<std::string, DB_Ptr>>* dbmap = nullptr);

  // user must ensure all dbs are alive when calling this function
  void CloseAllDB();

  ///@{
  /// the semantic is overwrite
  /// Put(name, PtrType(nullptr)) means delete
  void Put(const std::string& name, const std::shared_ptr<Options>&);
  void Put(const std::string& name, const std::shared_ptr<DBOptions>&);
  void Put(const std::string& name, const std::shared_ptr<ColumnFamilyOptions>&);

  // The caller should ensure DB handle's life time is longer than JsonOptionsRepo
  void Put(const std::string& name, DB*);
  void Put(const std::string& name, DB_MultiCF*);

  void Put(const std::string& name, const std::shared_ptr<Cache>&);
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
  void Put(const std::string& name, const std::shared_ptr<Statistics>&);
  void Put(const std::string& name, const std::shared_ptr<TableFactory>&);
  void Put(const std::string& name, const std::shared_ptr<TablePropertiesCollectorFactory>&);
  void Put(const std::string& name, const std::shared_ptr<TransactionDBMutexFactory>&);
  ///@}

  bool Get(const std::string& name, std::shared_ptr<Options>*) const;
  bool Get(const std::string& name, std::shared_ptr<DBOptions>*) const;
  bool Get(const std::string& name, std::shared_ptr<ColumnFamilyOptions>*) const;

  // The caller should ensure DB handle's life time is longer than JsonOptionsRepo
  bool Get(const std::string& name, DB**, Status* = nullptr) const;
  bool Get(const std::string& name, DB_MultiCF**, Status* = nullptr) const;

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
  bool Get(const std::string& name, std::shared_ptr<MemoryAllocator>*) const;
  bool Get(const std::string& name, std::shared_ptr<MemTableRepFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<MergeOperator>*) const;
  bool Get(const std::string& name, std::shared_ptr<PersistentCache>*) const;
  bool Get(const std::string& name, std::shared_ptr<RateLimiter>*) const;
  bool Get(const std::string& name, std::shared_ptr<const SliceTransform>*) const;
  bool Get(const std::string& name, std::shared_ptr<SstFileManager>*) const;
  bool Get(const std::string& name, std::shared_ptr<Statistics>*) const;
  bool Get(const std::string& name, std::shared_ptr<TableFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<TablePropertiesCollectorFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<TransactionDBMutexFactory>*) const;

  class Auto {
    friend class JsonOptionsRepo;
    const JsonOptionsRepo& m_repo;
    const std::string&     m_name;
    Auto(const JsonOptionsRepo& repo, const std::string& name)
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

namespace ROCKSDB_NAMESPACE {
struct DB_Ptr {
  union {
    DB* db;
    DB_MultiCF* dbm;
  };
  enum ptr_type { kNull, kDB, kDB_MultiCF };
  ptr_type type;

  DB_Ptr(DB* db1) : db(db1), type(kDB) {}
  DB_Ptr(DB_MultiCF* dbm1) : dbm(dbm1), type(kDB_MultiCF) {}
  DB_Ptr(std::nullptr_t) : db(nullptr), type(kNull) {}
  bool operator!() const { return nullptr == db; }
  operator bool () const { return nullptr != db; }

  bool IsMultiCF() const { return kDB_MultiCF == type; }
};
inline bool operator==(const DB_Ptr& x, const DB_Ptr& y) {
  //assert(!((x.db == y.db) ^ (x.type == y.type)));
#if !defined(NDEBUG)
  if (x.db == y.db) {
    if (x.db) {
      assert(x.type == y.type);
    }
  }
#endif
  return x.db == y.db;
}
inline bool operator!=(const DB_Ptr& x, const DB_Ptr& y) { return !(x == y); }

}  // namespace ROCKSDB_NAMESPACE

namespace std {
  template<>
  struct hash<ROCKSDB_NAMESPACE::DB_Ptr> {
    size_t operator()(const ROCKSDB_NAMESPACE::DB_Ptr& x) const {
      assert(ROCKSDB_NAMESPACE::DB_Ptr::kNull != x.type);
      assert(nullptr != x.db);
      return size_t(x.db);
    }
  };
}
