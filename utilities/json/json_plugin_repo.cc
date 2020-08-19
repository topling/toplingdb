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
#include "rocksdb/utilities/transaction_db_mutex.h"
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
static Ptr RepoPtrType(const JsonPluginRepo::Impl::ObjMap<Ptr>&);
template<class T> // just for type deduction
static const shared_ptr<T>&
RepoPtrCref(const JsonPluginRepo::Impl::ObjMap<shared_ptr<T> >&);

template<class T> // just for type deduction
static T* RepoPtrCref(const JsonPluginRepo::Impl::ObjMap<T*>&);

std::string JsonGetClassName(const char* caller, const json& js) {
  if (js.is_string()) {
    return js.get<std::string>();
  }
  if (js.is_object()) {
    auto iter = js.find("class");
    if (js.end() != iter) {
      if (!iter.value().is_string())
        throw Status::InvalidArgument(caller,
          "json[\"class\"] must be string, but is: " + js.dump());
      return iter.value().get<std::string>();
    }
    throw Status::InvalidArgument(caller,
      "json missing sub obj \"class\": " + js.dump());
  }
  throw Status::InvalidArgument(caller,
    "json must be string or object, but is: " + js.dump());
}

template<class Ptr>
static void Impl_Import(JsonPluginRepo::Impl::ObjMap<Ptr>& field,
                   const char* name,
                   const json& main_js, const JsonPluginRepo& repo) {
  auto iter = main_js.find(name);
  if (main_js.end() == iter) {
      return;
  }
  if (!iter.value().is_object()) {
    throw Status::InvalidArgument(ROCKSDB_FUNC,
        string(name) + " must be an object with class and options");
  }
  for (auto& item : iter.value().items()) {
    const string& inst_id = item.key();
    json value = item.value();
    auto ib = field.name2p->emplace(inst_id, Ptr(nullptr));
    auto& existing = ib.first->second;
    if (!ib.second) { // existed
      assert(Ptr(nullptr) != existing);
      auto oi_iter = field.p2name.find(existing);
      if (field.p2name.end() == oi_iter) {
        throw Status::Corruption(ROCKSDB_FUNC,
            "p2name[ptr_of(\"" + inst_id + "\")] is missing");
      }
      auto old_clazz = JsonGetClassName(ROCKSDB_FUNC, oi_iter->second.params);
      auto new_clazz = JsonGetClassName(ROCKSDB_FUNC, value);
      if (new_clazz == old_clazz) {
        try {
          PluginUpdate(&*existing, old_clazz, value, repo);
          oi_iter->second.params.merge_patch(value);
          continue; // done for current item
        }
        catch (const Status& st) {
          // not found updater, overwrite with merged json
          oi_iter->second.params.merge_patch(value);
          value.swap(oi_iter->second.params);
        }
      }
      field.p2name.erase(existing);
    }
    // do not use ObtainPlugin, to disallow define var2 = var1
    Ptr p = PluginFactory<Ptr>::AcquirePlugin(value, repo);
    if (!p) {
      throw Status::InvalidArgument(
          ROCKSDB_FUNC,
            "fail to AcquirePlugin: inst_id = " + inst_id +
              ", value_js = " + value.dump());
    }
    existing = p;
    field.p2name.emplace(p, JsonPluginRepo::Impl::ObjInfo{inst_id, std::move(value)});
  }
}

template<class Ptr>
static void Impl_ImportOptions(JsonPluginRepo::Impl::ObjMap<Ptr>& field,
                   const char* option_class_name,
                   const json& main_js, const JsonPluginRepo& repo) {
  auto iter = main_js.find(option_class_name);
  if (main_js.end() == iter) {
    return;
  }
  if (!iter.value().is_object()) {
    throw Status::InvalidArgument(
      ROCKSDB_FUNC,
      string(option_class_name) + " must be a json object");
  }
  for (auto& item : iter.value().items()) {
    const string& option_name = item.key();
    json params_js = item.value();
    auto ib = field.name2p->emplace(option_name, Ptr(nullptr));
    auto& existing = ib.first->second;
    if (!ib.second) { // existed
      assert(Ptr(nullptr) != existing);
      auto oi_iter = field.p2name.find(existing);
      if (field.p2name.end() == oi_iter) {
        throw Status::Corruption(ROCKSDB_FUNC,
            "p2name[ptr_of(\"" + option_name + "\")] is missing");
      }
      PluginUpdate(&*existing, option_class_name, params_js, repo);
      oi_iter->second.params.merge_patch(params_js);
    }
    else {
      Ptr p = PluginFactory<Ptr>::AcquirePlugin(option_class_name, params_js, repo);
      assert(Ptr(nullptr) != p);
      existing = p;
      field.p2name.emplace(p, JsonPluginRepo::Impl::ObjInfo{option_name, params_js});
    }
  }
}

JsonPluginRepo::JsonPluginRepo() noexcept {
  m_impl.reset(new Impl);
}
JsonPluginRepo::~JsonPluginRepo() = default;
JsonPluginRepo::JsonPluginRepo(const JsonPluginRepo&) noexcept = default;
JsonPluginRepo::JsonPluginRepo(JsonPluginRepo&&) noexcept = default;
JsonPluginRepo& JsonPluginRepo::operator=(const JsonPluginRepo&) noexcept = default;
JsonPluginRepo& JsonPluginRepo::operator=(JsonPluginRepo&&) noexcept = default;

Status JsonPluginRepo::ImportJsonFile(const Slice& fname) {
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

Status JsonPluginRepo::Import(const string& json_str) try {
  json js = json::parse(json_str);
  return Import(js);
}
catch (const std::exception& ex) {
  // just parse error
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
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

static void JS_setenv(const nlohmann::json& main_js) {
  auto iter = main_js.find("setenv");
  if (main_js.end() == iter) {
    return;
  }
  auto& envmap = iter.value();
  if (!envmap.is_object()) {
    throw Status::InvalidArgument(
        ROCKSDB_FUNC, "main_js[\"setenv\"] must be a json object");
  }
  for (auto& item : envmap.items()) {
    const std::string& name = item.key();
    const json& val = item.value();
    if (val.is_object() || val.is_array()) {
      throw Status::InvalidArgument(
          ROCKSDB_FUNC, "main_js[\"setenv\"] must not be object or array");
    }
    if (JsonPluginRepo::DebugLevel() >= 3) {
      const std::string& valstr = val.dump();
      fprintf(stderr, "JS_setenv: %s = %s\n", name.c_str(), valstr.c_str());
    }
    if (val.is_string()) {
      ::setenv(name.c_str(), val.get<std::string>().c_str(), true);
    }
    else if (val.is_boolean()) {
      ::setenv(name.c_str(), val.get<bool>() ? "1" : "0", true);
    }
    else {
      const std::string& valstr = val.dump();
      ::setenv(name.c_str(), valstr.c_str(), true);
    }
  }
}

Status JsonPluginRepo::Import(const nlohmann::json& main_js) try {
  JS_setenv(main_js);
  MergeSubObject(&m_impl->db_js, main_js, "databases");
  MergeSubObject(&m_impl->http_js, main_js, "http");
  MergeSubObject(&m_impl->open_js, main_js, "open");
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

  JSON_IMPORT_REPO(MemoryAllocator         , memory_allocator);
  JSON_IMPORT_REPO(MemTableRepFactory      , mem_table_rep_factory);
  JSON_IMPORT_REPO(TableFactory            , table_factory);
  JSON_IMPORT_REPO(TransactionDBMutexFactory, txn_db_mutex_factory);

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

  Impl_ImportOptions(m_impl->db_options, "DBOptions", main_js, repo);
  Impl_ImportOptions(m_impl->cf_options, "CFOptions", main_js, repo);

  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& s) {
  return s;
}

template<class Ptr>
static void Impl_Export(const JsonPluginRepo::Impl::ObjMap<Ptr>& field,
                   const char* name, json& main_js) {
  auto& field_js = main_js[name];
  for (auto& kv: field.p2name) {
    auto& params_js = field_js[kv.second.name];
    params_js = kv.second.params;
  }
}
Status JsonPluginRepo::Export(nlohmann::json* main_js) const try {
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

  JSON_EXPORT_REPO(MemoryAllocator         , memory_allocator);
  JSON_EXPORT_REPO(MemTableRepFactory      , mem_table_rep_factory);
  JSON_EXPORT_REPO(TableFactory            , table_factory);
  JSON_EXPORT_REPO(TransactionDBMutexFactory, txn_db_mutex_factory);

  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}

Status JsonPluginRepo::Export(string* json_str, bool pretty) const {
  assert(NULL != json_str);
  nlohmann::json js;
  Status s = Export(&js);
  if (s.ok()) {
    *json_str = js.dump(pretty ? 4 : -1);
  }
  return s;
}

template<class Map, class Ptr>
static void
Impl_Put(const std::string& name, Map& map, const Ptr& p) {
  auto& name2p = *map.name2p;
  if (p) { // put
    auto ib = name2p.emplace(name, p);
    if (!ib.second)
      ib.first->second = p; // overwrite
  }
  else { // p is null, do delete
    auto iter = name2p.find(name);
    if (name2p.end() == iter) {
      return;
    }
    map.p2name.erase(iter->second);
    name2p.erase(iter);
  }
}

template<class Map, class Ptr>
static bool
Impl_Get(const std::string& name, const Map& map, Ptr* pp) {
  auto& name2p = *map.name2p;
  auto iter = name2p.find(name);
  if (name2p.end() != iter) {
    *pp = iter->second;
    return true;
  }
  else {
    *pp = Ptr(nullptr);
    return false;
  }
}

#define JSON_REPO_TYPE_IMPL(field) \
void JsonPluginRepo::Put(const string& name, \
                decltype((RepoPtrCref(((Impl*)0)->field))) p) { \
  Impl_Put(name, m_impl->field, p); \
} \
bool JsonPluginRepo::Get(const string& name, \
                decltype(RepoPtrType(((Impl*)0)->field))* pp) const { \
  return Impl_Get(name, m_impl->field, pp); \
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
JSON_REPO_TYPE_IMPL(memory_allocator)
JSON_REPO_TYPE_IMPL(mem_table_rep_factory)
JSON_REPO_TYPE_IMPL(merge_operator)
JSON_REPO_TYPE_IMPL(rate_limiter)
JSON_REPO_TYPE_IMPL(sst_file_manager)
JSON_REPO_TYPE_IMPL(statistics)
JSON_REPO_TYPE_IMPL(table_factory)
JSON_REPO_TYPE_IMPL(table_properties_collector_factory)
JSON_REPO_TYPE_IMPL(txn_db_mutex_factory)
JSON_REPO_TYPE_IMPL(slice_transform)

JSON_REPO_TYPE_IMPL(options)
JSON_REPO_TYPE_IMPL(db_options)
JSON_REPO_TYPE_IMPL(cf_options)

void JsonPluginRepo::Put(const std::string& name, DB* db) {
  Impl_Put(name, m_impl->db, DB_Ptr(db));
}
void JsonPluginRepo::Put(const std::string& name, DB_MultiCF* db) {
  Impl_Put(name, m_impl->db, DB_Ptr(db));
}
bool JsonPluginRepo::Get(const std::string& name, DB** db, Status* s) const {
  DB_Ptr dbp(nullptr);
  if (Impl_Get(name, m_impl->db, &dbp)) {
    if (!dbp.dbm) {
      *db = dbp.db;
      return true;
    }
    Status ss = Status::InvalidArgument(ROCKSDB_FUNC,
        "database \"" + name + "\" mubst be DB, but is DB_MultiCF");
    if (s)
      *s = ss;
    else
      throw ss;
  }
  return false;
}
bool JsonPluginRepo::Get(const std::string& name, DB_MultiCF** db, Status* s) const {
  DB_Ptr dbp(nullptr);
  if (Impl_Get(name, m_impl->db, &dbp)) {
    if (dbp.dbm) {
      *db = dbp.dbm;
      return true;
    }
    Status ss = Status::InvalidArgument(ROCKSDB_FUNC,
        "database \"" + name + "\" mubst be DB_MultiCF, but is DB");
    if (s)
      *s = ss;
    else
      throw ss;
  }
  return false;
}

void JsonPluginRepo::GetMap(
    shared_ptr<unordered_map<string,
                             shared_ptr<TableFactory>>>* pp) const {
  *pp = m_impl->table_factory.name2p;
}

void JsonPluginRepo::GetMap(
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
Status JsonPluginRepo::OpenDB(const nlohmann::json& js, DB** dbp) {
  return OpenDB_tpl<DB>(js, dbp);
}
Status JsonPluginRepo::OpenDB(const nlohmann::json& js, DB_MultiCF** dbp) {
  return OpenDB_tpl<DB_MultiCF>(js, dbp);
}

Status JsonPluginRepo::OpenDB(const std::string& js, DB** dbp) try {
  return OpenDB_tpl<DB>(js, dbp);
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, "bad json object");
}
Status JsonPluginRepo::OpenDB(const std::string& js, DB_MultiCF** dbp) try {
  return OpenDB_tpl<DB_MultiCF>(js, dbp);
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, "bad json object");
}

template<class DBT>
static void Impl_OpenDB_tpl(const std::string& dbname,
                            const json& db_open_js,
                            JsonPluginRepo& repo,
                            DBT** dbp) {
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
  auto params_js = iter.value();
  if (!params_js.is_object()) {
    throw Status::InvalidArgument(ROCKSDB_FUNC,
        "dbname = \"" + dbname + "\", \"params\" must be a json object");
  }
  if (!dbname.empty()) {
    params_js["name"] = dbname;
  }
  auto db = PluginFactory<DBT*>::AcquirePlugin(method, params_js, repo);
  assert(nullptr != db);
  repo.Put(dbname, db);
  *dbp = db;
  const auto& http_js = repo.m_impl->http_js;
  if (JsonPluginRepo::DebugLevel() >= 2) {
    fprintf(stderr, "INFO: http_js = %s\n", http_js.dump().c_str());
  }
  if (http_js.is_object()) {
    repo.m_impl->http.Init(http_js, &repo);
  }
  else if (!http_js.is_null()) {
    fprintf(stderr, "ERROR: bad http_js = %s\n", http_js.dump().c_str());
  }
}

template<class DBT>
Status JsonPluginRepo::OpenDB_tpl(const nlohmann::json& js, DBT** dbp) try {
  *dbp = nullptr;
  auto open_defined_db = [&](const std::string& dbname) {
      auto iter = m_impl->db_js.find(dbname);
      if (m_impl->db_js.end() == iter) {
        throw Status::NotFound(ROCKSDB_FUNC,
            "dbname = \"" + dbname + "\" is not found");
      }
      Impl_OpenDB_tpl(dbname, iter.value(), *this, dbp);
  };
  if (js.is_string()) {
    const std::string& str_val = js.get<std::string>();
    if (str_val.empty()) {
      return Status::InvalidArgument(ROCKSDB_FUNC,
        "open js:string = \"" + str_val + "\" is empty");
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
  } else if (js.is_object()) {
    // when name is empty, js["params"]["name"] must be defined
    std::string empty_name = "";
    Impl_OpenDB_tpl(empty_name, js, *this, dbp);
  }
  else {
    throw Status::InvalidArgument(ROCKSDB_FUNC, "bad js = " + js.dump());
  }
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& s) {
  return s;
}

Status JsonPluginRepo::OpenAllDB() try {
  size_t num = 0;
  for (auto& item : m_impl->db_js.items()) {
    const std::string& dbname = item.key();
    const json& db_open_js = item.value();
    auto iter = db_open_js.find("params");
    if (db_open_js.end() == iter) {
      return Status::InvalidArgument(ROCKSDB_FUNC,
          "dbname = \"" + dbname + "\", param \"params\" is missing");
    }
    const json& params_js = iter.value();
    if (!params_js.is_object()) {
      return Status::InvalidArgument(ROCKSDB_FUNC,
          "dbname = \"" + dbname + "\", \"params\" must be a json object");
    }
    iter = params_js.find("column_families");
    if (params_js.end() == iter) {
      DB* db = Get(dbname);
      if (db) {
        return Status::InvalidArgument(ROCKSDB_FUNC,
            "DB \"" + dbname + "\" have been opened, can not open same db twice");
      }
      Impl_OpenDB_tpl(dbname, db_open_js, *this, &db);
    }
    else {
      DB_MultiCF* db = Get(dbname);
      if (db) {
        return Status::InvalidArgument(ROCKSDB_FUNC,
            "DB_MultiCF \"" + dbname + "\" have been opened, can not open same db twice");
      }
      Impl_OpenDB_tpl(dbname, db_open_js, *this, &db);
    }
    num++;
  }
  if (0 == num) {
    return Status::InvalidArgument(ROCKSDB_FUNC, "databases are empty");
  }
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& s) {
  // nested Status
  return Status::InvalidArgument(ROCKSDB_FUNC, s.ToString());
}

std::shared_ptr<std::unordered_map<std::string, DB_Ptr> > JsonPluginRepo::GetAllDB() const {
  return m_impl->db.name2p;
}

/**
 * @param json_str sub object "open" is used as json_obj in
 *                 JsonPluginRepo::OpenDB
 */
Status JsonPluginRepo::OpenDB(DB** db) {
  if (m_impl->open_js.is_string() || m_impl->open_js.is_object())
    return OpenDB(m_impl->open_js, db);
  else
    return Status::InvalidArgument(ROCKSDB_FUNC, "bad json[\"open\"] = " + js.dump());
}
Status JsonPluginRepo::OpenDB(DB_MultiCF** db) {
  if (m_impl->open_js.is_string() || m_impl->open_js.is_object())
    return OpenDB(m_impl->open_js, db);
  else
    return Status::InvalidArgument(ROCKSDB_FUNC, "bad json[\"open\"] = " + js.dump());
}

Status JsonPluginRepo::StartHttpServer() try {
  m_impl->http.Init(m_impl->http_js, this);
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& s) {
  // nested Status
  return Status::InvalidArgument(ROCKSDB_FUNC, s.ToString());
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
    if (!js.is_object()) {
      throw std::invalid_argument(
          std::string(ROCKSDB_FUNC) + ": js is not an object, key = " + key);
    }
    auto iter = js.find(key);
    if (js.end() != iter) {
      auto& sub_js = iter.value();
      if (sub_js.is_number_integer())
        m_val = sub_js.get<long long>();
      else if (sub_js.is_number_unsigned())
        m_val = sub_js.get<unsigned long long>();
      else if (sub_js.is_string())
        *this = ParseSizeXiB(sub_js.get<std::string>());
      else
        throw std::invalid_argument(
                "bad sub_js = " + sub_js.dump() + ", key = \"" + key + "\"");
    }
    else {
      throw std::invalid_argument(
          std::string("ParseSizeXiB : not found key: \"") +
            key + "\" in js = " + js.dump());
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

void TableFactoryDummyFuncToPreventGccDeleteSymbols();
static int InitOnceDebugLevel() {
  const char* env = getenv("JsonOptionsRepo_DebugLevel");
  if (env) {
    return atoi(env);
  }
  TableFactoryDummyFuncToPreventGccDeleteSymbols();
  return 0;
}

int JsonPluginRepo::DebugLevel() {
  static int d = InitOnceDebugLevel();
  return d;
}

bool JsonWeakBool(const json& js) {
  if (js.is_string()) {
    const std::string& s = js.get<std::string>();
    if (strcasecmp(s.c_str(), "true") == 0) return true;
    if (strcasecmp(s.c_str(), "false") == 0) return false;
    if (strcasecmp(s.c_str(), "on") == 0) return true;
    if (strcasecmp(s.c_str(), "off") == 0) return false;
    if (strcasecmp(s.c_str(), "yes") == 0) return true;
    if (strcasecmp(s.c_str(), "no") == 0) return false;
    if (isdigit((unsigned char)s[0])) {
      return atoi(s.c_str()) != 0;
    }
    throw std::invalid_argument("JsonWeakBool: bad js = " + s);
  }
  if (js.is_boolean()) return js.get<bool>();
  if (js.is_number_integer()) return js.get<long long>() != 0;
  throw std::invalid_argument("JsonWeakBool: bad js = " + js.dump());
}

bool JsonWeakInt(const json& js) {
  if (js.is_string()) {
    const std::string& s = js.get<std::string>();
    if (isdigit((unsigned char)s[0])) {
      return atoi(s.c_str());
    }
    throw std::invalid_argument("JsonWeakInt: bad js = " + s);
  }
  if (js.is_number_integer()) return js.get<int>();
  throw std::invalid_argument("JsonWeakBool: bad js = " + js.dump());
}

std::string JsonToHtml(const json& obj) {
  std::string html;
  html.append("<table><tbody>\n");
  html.append("  <tr><th>name</th><th>value</th></tr>\n");
  for (const auto& kv : obj.items()) {
    const std::string& key = kv.key();
    html.append("  <tr><td>");
    html.append(key);
    html.append("</td><td>");
    html.append(kv.value().dump());
    html.append("</td></tr>\n");
  }
  html.append("</tbody></table>\n");
  return html;
}

std::string JsonToString(const json& obj, const json& options) {
  int indent = -1;
  auto iter = options.find("pretty");
  if (options.end() != iter) {
    if (JsonWeakBool(iter.value())) {
      indent = 4;
    }
  }
  iter = options.find("indent");
  if (options.end() != iter) {
    indent = JsonWeakInt(iter.value());
  }
  iter = options.find("html");
  bool html = false;
  if (options.end() != iter) {
    html = JsonWeakBool(iter.value());
  }
  if (html)
    return JsonToHtml(obj);
  else
    return obj.dump(indent);
}

std::string
PluginToString(const DB_Ptr& dbp, const std::string& clazz,
               const json& js, const JsonPluginRepo& repo) {
  if (dbp.dbm)
    return PluginToString(*dbp.dbm, clazz, js, repo);
  else
    return PluginToString(*dbp.db, clazz, js, repo);
}

}
