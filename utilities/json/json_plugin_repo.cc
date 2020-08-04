//
// Created by leipeng on 2020/7/1.
//
#include <cinttypes>
#include <fstream>
#include <sstream>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "options/db_options.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/wal_filter.h"
#include "util/string_util.h"

#include "json.h"
#include "json_plugin_factory.h"

namespace ROCKSDB_NAMESPACE {

using std::shared_ptr;
using std::unordered_map;
using std::vector;
using std::string;

/////////////////////////////////////////////////////////////////////////////
template<class Ptr> // just for type deduction
static Ptr RepoPtrType(const JsonOptionsRepo::Impl::ObjMap<Ptr>&);
template<class T> // just for type deduction
static const shared_ptr<T>&
RepoPtrCref(const JsonOptionsRepo::Impl::ObjMap<shared_ptr<T> >&);

template<class T> // just for type deduction
static T* RepoPtrCref(const JsonOptionsRepo::Impl::ObjMap<T*>&);

template<class Ptr>
static void Impl_Import(JsonOptionsRepo::Impl::ObjMap<Ptr>& field,
                   const char* name,
                   const json& main_js, const JsonOptionsRepo& repo) {
  auto iter = main_js.find(name);
  if (main_js.end() != iter) {
    if (!iter.value().is_object()) {
      throw Status::InvalidArgument(ROCKSDB_FUNC,
          string(name) + " must be an object with class and options");
    }
    for (auto& item : iter.value().items()) {
      const string& inst_id = item.key();
      const json& value = item.value();
      // name and func are just for error report in this call
      Ptr p = PluginFactory<Ptr>::ObtainPlugin(
                name, ROCKSDB_FUNC, value, repo);
      if (!p) {
        throw Status::InvalidArgument(
            ROCKSDB_FUNC,
            string("fail to ObtainPlugin(name=") + name +
                ", value_js = " + value.dump() + ")");
      }
      field.name2p->emplace(inst_id, p);
      field.p2name.emplace(p, JsonOptionsRepo::Impl::ObjInfo{inst_id, value});
    }
  }
}

JsonOptionsRepo::JsonOptionsRepo() noexcept {
  m_impl.reset(new Impl);
}
JsonOptionsRepo::~JsonOptionsRepo() = default;
JsonOptionsRepo::JsonOptionsRepo(const JsonOptionsRepo&) noexcept = default;
JsonOptionsRepo::JsonOptionsRepo(JsonOptionsRepo&&) noexcept = default;
JsonOptionsRepo& JsonOptionsRepo::operator=(const JsonOptionsRepo&) noexcept = default;
JsonOptionsRepo& JsonOptionsRepo::operator=(JsonOptionsRepo&&) noexcept = default;

Status JsonOptionsRepo::ImportJsonFile(const Slice& fname) {
  std::string json_str;
  {
    std::fstream ifs(fname.data());
    if (!ifs.is_open()) {
      return Status::InvalidArgument("open json file fail", fname);
    }
    std::stringstream ss;
    ss << ifs.rdbuf();
    json_str = ss.str();
  }
  return Import(json_str);
}

Status JsonOptionsRepo::Import(const string& json_str) {
  json js = json::parse(json_str);
  return Import(js);
}

static
void MergeSubObject(json* target, const json& patch, const string& subname) {
  auto iter = patch.find(subname);
  if (patch.end() != iter) {
    auto& sub_js = iter.value();
    if (!sub_js.is_object()) {
      throw Status::InvalidArgument(
          ROCKSDB_FUNC,
          "\"" + subname + "\" must be an object");
    }
    if (!target->is_null() && !target->is_object()) {
      throw Status::Corruption(
          ROCKSDB_FUNC,
          "\"target\" must be an object or null, subname = " + subname);
    }
    target->merge_patch(sub_js);
  }
}

Status JsonOptionsRepo::Import(const nlohmann::json& main_js) try {
  MergeSubObject(&m_impl->db_js, main_js, "databases");
  const auto& repo = *this;
#define JSON_IMPORT_REPO(Clazz, field) \
  Impl_Import(m_impl->field, #Clazz, main_js, repo)
  JSON_IMPORT_REPO(Comparator              , comparator);
  JSON_IMPORT_REPO(Env                     , env);
  JSON_IMPORT_REPO(Logger                  , info_log);
  JSON_IMPORT_REPO(SliceTransform          , slice_transform);
  JSON_IMPORT_REPO(Cache                   , cache);
  JSON_IMPORT_REPO(PersistentCache         , persistent_cache);
  JSON_IMPORT_REPO(CompactionFilterFactory , compaction_filter_factory);
  JSON_IMPORT_REPO(ConcurrentTaskLimiter   , compaction_thread_limiter);
  JSON_IMPORT_REPO(EventListener           , event_listener);
  JSON_IMPORT_REPO(FileChecksumGenFactory  , file_checksum_gen_factory);
  JSON_IMPORT_REPO(FileSystem              , file_system);
  JSON_IMPORT_REPO(FilterPolicy            , filter_policy);
  JSON_IMPORT_REPO(FlushBlockPolicyFactory , flush_block_policy_factory);
  JSON_IMPORT_REPO(MergeOperator           , merge_operator);
  JSON_IMPORT_REPO(RateLimiter             , rate_limiter);
  JSON_IMPORT_REPO(SstFileManager          , sst_file_manager);
  JSON_IMPORT_REPO(Statistics              , statistics);
  JSON_IMPORT_REPO(TablePropertiesCollectorFactory,
                   table_properties_collector_factory);

  JSON_IMPORT_REPO(MemTableRepFactory      , mem_table_rep_factory);
  JSON_IMPORT_REPO(TableFactory            , table_factory);

  for (auto& kv : *m_impl->table_factory.name2p) {
    if (Slice(kv.second->Name()) == "DispatherTableFactory") {
      // db_options and cf_options will not be used in
      // DispatherTableFactory::SanitizeOptions()
      const DBOptions* db_options = nullptr;
      const ColumnFamilyOptions* cf_options = nullptr;
      Status s = kv.second->SanitizeOptions(*db_options, *cf_options);
      if (!s.ok()) return s;
    }
  }

  JSON_IMPORT_REPO(DBOptions            , db_options);
  JSON_IMPORT_REPO(ColumnFamilyOptions  , cf_options);

  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& s) {
  return s;
}

template<class Ptr>
static void Impl_Export(const JsonOptionsRepo::Impl::ObjMap<Ptr>& field,
                   const char* name, json& main_js) {
  auto& field_js = main_js[name];
  for (auto& kv: field.p2name) {
    auto& params_js = field_js[kv.second.name];
    params_js = kv.second.params;
  }
}
Status JsonOptionsRepo::Export(nlohmann::json* main_js) const try {
  assert(NULL != main_js);
#define JSON_EXPORT_REPO(Clazz, field) \
  Impl_Export(m_impl->field, #Clazz, *main_js)
  JSON_EXPORT_REPO(Comparator              , comparator);
  JSON_EXPORT_REPO(Env                     , env);
  JSON_EXPORT_REPO(Logger                  , info_log);
  JSON_EXPORT_REPO(SliceTransform          , slice_transform);
  JSON_EXPORT_REPO(Cache                   , cache);
  JSON_EXPORT_REPO(PersistentCache         , persistent_cache);
  JSON_EXPORT_REPO(CompactionFilterFactory , compaction_filter_factory);
  JSON_EXPORT_REPO(ConcurrentTaskLimiter   , compaction_thread_limiter);
  JSON_EXPORT_REPO(EventListener           , event_listener);
  JSON_EXPORT_REPO(FileChecksumGenFactory  , file_checksum_gen_factory);
  JSON_EXPORT_REPO(FileSystem              , file_system);
  JSON_EXPORT_REPO(FilterPolicy            , filter_policy);
  JSON_EXPORT_REPO(FlushBlockPolicyFactory , flush_block_policy_factory);
  JSON_EXPORT_REPO(MergeOperator           , merge_operator);
  JSON_EXPORT_REPO(RateLimiter             , rate_limiter);
  JSON_EXPORT_REPO(SstFileManager          , sst_file_manager);
  JSON_EXPORT_REPO(Statistics              , statistics);
  JSON_EXPORT_REPO(TablePropertiesCollectorFactory,
                   table_properties_collector_factory);

  JSON_EXPORT_REPO(MemTableRepFactory      , mem_table_rep_factory);
  JSON_EXPORT_REPO(TableFactory            , table_factory);
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}

Status JsonOptionsRepo::Export(string* json_str, bool pretty) const {
  assert(NULL != json_str);
  nlohmann::json js;
  Status s = Export(&js);
  if (s.ok()) {
    *json_str = js.dump(pretty ? 4 : -1);
  }
  return s;
}

#define JSON_REPO_TYPE_IMPL(field) \
void JsonOptionsRepo::Add(const string& name, \
                decltype((RepoPtrCref(((Impl*)0)->field))) p) { \
  auto ib = m_impl->field.name2p->emplace(name, p); \
  if (ib.second) \
    ib.first->second = p; \
} \
bool JsonOptionsRepo::Get(const string& name, \
                decltype(RepoPtrType(((Impl*)0)->field))* pp) const { \
  auto& __map = *m_impl->field.name2p; \
  auto iter = __map.find(name); \
  if (__map.end() != iter) { \
    *pp = iter->second; \
    return true; \
  } \
  return false; \
}

JSON_REPO_TYPE_IMPL(cache)
JSON_REPO_TYPE_IMPL(persistent_cache)
JSON_REPO_TYPE_IMPL(compaction_filter_factory)
JSON_REPO_TYPE_IMPL(comparator)
JSON_REPO_TYPE_IMPL(compaction_thread_limiter)
JSON_REPO_TYPE_IMPL(env)
JSON_REPO_TYPE_IMPL(event_listener)
JSON_REPO_TYPE_IMPL(file_checksum_gen_factory)
JSON_REPO_TYPE_IMPL(file_system)
JSON_REPO_TYPE_IMPL(filter_policy)
JSON_REPO_TYPE_IMPL(flush_block_policy_factory)
JSON_REPO_TYPE_IMPL(info_log)
JSON_REPO_TYPE_IMPL(mem_table_rep_factory)
JSON_REPO_TYPE_IMPL(merge_operator)
JSON_REPO_TYPE_IMPL(rate_limiter)
JSON_REPO_TYPE_IMPL(sst_file_manager)
JSON_REPO_TYPE_IMPL(statistics)
JSON_REPO_TYPE_IMPL(table_factory)
JSON_REPO_TYPE_IMPL(table_properties_collector_factory)
JSON_REPO_TYPE_IMPL(slice_transform)

JSON_REPO_TYPE_IMPL(options)
JSON_REPO_TYPE_IMPL(db_options)
JSON_REPO_TYPE_IMPL(cf_options)

void JsonOptionsRepo::GetMap(
    shared_ptr<unordered_map<string,
                             shared_ptr<TableFactory>>>* pp) const {
  *pp = m_impl->table_factory.name2p;
}

void JsonOptionsRepo::GetMap(
    shared_ptr<unordered_map<string,
                             shared_ptr<MemTableRepFactory>>>* pp) const {
  *pp = m_impl->mem_table_rep_factory.name2p;
}

/**
 * @param js may be:
 *  1. string name ref to a db defined in 'this' repo
 *  2. { "DB::Open": { options: {...} } }
 *  3. { "SomeStackableDB::Open": { } } }
 *  4. string name ref target in repo looks like:
 *     db : {
 *       dbname1 : {
 *         method : "DB::Open",
 *         params : {
 *           name : "some-name",
 *           options: { ... }
 *         }
 *       },
 *       dbname2 : {
 *         method : "SomeStackableDB::Open",
 *         params : {
 *           name : "some-name",
 *           options: { ... }
 *         }
 *       }
 *       dbname3 : {
 *         method : "DB::OpenReadOnly",
 *         params : {
 *           name : "some-name",
 *           options: { ... }
 *         }
 *       }
 *     }
 */
Status JsonOptionsRepo::OpenDB(const nlohmann::json& js, DB** dbp) {
  return OpenDB_tpl<DB>(js, dbp);
}
Status JsonOptionsRepo::OpenDB(const nlohmann::json& js, DB_MultiCF** dbp) {
  return OpenDB_tpl<DB_MultiCF>(js, dbp);
}

Status JsonOptionsRepo::OpenDB(const std::string& js, DB** dbp) try {
  return OpenDB_tpl<DB>(js, dbp);
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, "bad json object");
}
Status JsonOptionsRepo::OpenDB(const std::string& js, DB_MultiCF** dbp) try {
  return OpenDB_tpl<DB_MultiCF>(js, dbp);
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, "bad json object");
}

template<class DBT>
Status JsonOptionsRepo::OpenDB_tpl(const nlohmann::json& js, DBT** dbp) try {
  *dbp = nullptr;
  auto open_impl = [&](const std::string& dbname, const json& db_open_js) {
      auto iter = db_open_js.find("method");
      if (db_open_js.end() == iter) {
        throw Status::InvalidArgument(ROCKSDB_FUNC,
            "dbname = \"" + dbname + "\", param \"method\" is missing");
      }
      const std::string& method = iter.value().get<string>();
      iter = db_open_js.find("params");
      if (db_open_js.end() == iter) {
        throw Status::InvalidArgument(ROCKSDB_FUNC,
            "dbname = \"" + dbname + "\", param \"params\" is missing");
      }
      const auto& params_js = iter.value();
      *dbp = PluginFactory<DBT*>::AcquirePlugin(method, params_js, *this);
      assert(nullptr != *dbp);
  };
  auto open_defined_db = [&](const std::string& dbname) {
      auto iter = m_impl->db_js.find(dbname);
      if (m_impl->db_js.end() == iter) {
        throw Status::NotFound(ROCKSDB_FUNC,
            "dbname = \"" + dbname + "\" is not found");
      }
      open_impl(dbname, iter.value());
  };
  if (js.is_string()) {
    const std::string& str_val = js.get<std::string>();
    if (str_val.empty()) {
      return Status::InvalidArgument(ROCKSDB_FUNC,
        "open method = \"" + str_val + "\" is empty");
    }
    if ('$' == str_val[0]) {
      if (str_val.size() < 3) {
        return Status::InvalidArgument(ROCKSDB_FUNC,
            "dbname = \"" + str_val + "\" is too short");
      }
      open_defined_db(PluginParseInstID(str_val));
    } else {
      // string which does not like ${dbname} or $dbname
      open_defined_db(str_val); // str_val is dbname
    }
  } else {
    open_impl("<inline-defined>", js);
  }
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& s) {
  return s;
}

template<class DBT>
static Status JS_Str_OpenDB_tpl(const std::string& json_str, DBT** db) try {
  JsonOptionsRepo repo;
  nlohmann::json json_obj = json::parse(json_str);
  Status s = repo.Import(json_str);
  if (s.ok()) {
    auto iter = json_obj.find("open");
    if (json_obj.end() != iter) {
      return repo.OpenDB(iter.value(), db);
    }
    s = Status::InvalidArgument(ROCKSDB_FUNC, "sub obj \"open\" is required");
  }
  return s;
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& s) {
  return s;
}

template<class DBT>
static Status JS_File_OpenDB_tpl(const std::string& js_file, DBT** db) try {
  JsonOptionsRepo repo;
  std::string json_str;
  {
    std::fstream ifs(js_file.data());
    if (!ifs.is_open()) {
      return Status::InvalidArgument("open json file fail", js_file);
    }
    std::stringstream ss;
    ss << ifs.rdbuf();
    json_str = ss.str();
  }
  return JS_Str_OpenDB_tpl(json_str, db);
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}

/**
 * @param json_str sub object "open" is used as json_obj in
 *                 JsonOptionsRepo::OpenDB
 */
Status JS_Str_OpenDB(const std::string& json_str, DB** db) {
  return JS_Str_OpenDB_tpl(json_str, db);
}
Status JS_Str_OpenDB(const std::string& json_str, DB_MultiCF** db) {
  return JS_Str_OpenDB_tpl(json_str, db);
}
Status JS_File_OpenDB(const std::string& js_file, DB** db) {
  return JS_File_OpenDB_tpl(js_file, db);
}
Status JS_File_OpenDB(const std::string& js_file, DB_MultiCF** db) {
  return JS_File_OpenDB_tpl(js_file, db);
}

std::string PluginParseInstID(const std::string& str_val) {
  // ${inst_id} or $inst_id
  if ('{' == str_val[1])
    return str_val.substr(2, str_val.size() - 3);
  else
    return str_val.substr(1, str_val.size() - 1);
}

ParseSizeXiB::ParseSizeXiB(const char* s) {
  if ('-' == s[0])
    m_val = ParseInt64(s);
  else
    m_val = ParseUint64(s);
}
ParseSizeXiB::ParseSizeXiB(const std::string& s) {
  if ('-' == s[0])
    m_val = ParseInt64(s);
  else
    m_val = ParseUint64(s);
}
ParseSizeXiB::ParseSizeXiB(const nlohmann::json& js) {
  if (js.is_number_integer())
    m_val = js.get<long long>();
  else if (js.is_number_unsigned())
    m_val = js.get<unsigned long long>();
  else if (js.is_string())
    *this = ParseSizeXiB(js.get<std::string>());
  else
    throw std::invalid_argument("bad json = " + js.dump());
}
ParseSizeXiB::ParseSizeXiB(const nlohmann::json& js, const char* key) {
    auto iter = js.find(key);
    if (js.end() != iter) {
      *this = ParseSizeXiB(iter.value());
    }
    else {
      throw std::invalid_argument(
          std::string(ROCKSDB_FUNC) + ": not found key: " + key);
    }
}

ParseSizeXiB::operator int() const {
  if (m_val < INT_MIN || m_val > INT_MAX)
    throw std::domain_error(std::string(ROCKSDB_FUNC) + ": out of range<int>");
  return (int)m_val;
}

ParseSizeXiB::operator long() const {
  if (sizeof(long) != sizeof(long long) && (m_val < LONG_MIN || m_val > LONG_MAX))
    throw std::domain_error(std::string(ROCKSDB_FUNC) + ": out of range<long>");
  return (long)m_val;
}
ParseSizeXiB::operator long long() const {
  return m_val;
}
ParseSizeXiB::operator unsigned int() const {
  if (m_val > UINT_MAX)
    throw std::domain_error(std::string(ROCKSDB_FUNC) + ": out of range<uint>");
  return (unsigned int)m_val;
}
ParseSizeXiB::operator unsigned long() const {
  if (sizeof(long) != sizeof(long long) && (unsigned long long)m_val > ULONG_MAX)
    throw std::domain_error(std::string(ROCKSDB_FUNC) + ": out of range<ulong>");
  return (unsigned long)m_val;
}
ParseSizeXiB::operator unsigned long long() const {
  return (unsigned long long)m_val;
}

}
