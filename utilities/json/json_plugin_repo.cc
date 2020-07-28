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
      Status s;
      // name and func are just for error report in this call
      Ptr p = PluginFactory<Ptr>::ObtainPlugin(
                name, ROCKSDB_FUNC, value, repo, &s);
      if (!s.ok()) throw s;
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
  json js(json_str);
  return Import(js);
}

Status JsonOptionsRepo::Import(const nlohmann::json& main_js) try {
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
Status JsonOptionsRepo::Export(nlohmann::json* main_js) const {
  assert(NULL != js);
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

template<class DBT>
Status JsonOptionsRepo::OpenDB_tpl(const nlohmann::json& js, DBT** dbp) try {
  *dbp = nullptr;
  auto open_impl = [&](const std::string& dbname, const json& db_open_js) {
      auto iter = db_open_js.find("method");
      if (db_open_js.end() == iter) {
        return Status::InvalidArgument(ROCKSDB_FUNC,
            "dbname = \"" + dbname + "\", param \"method\" is missing");
      }
      const std::string& method = iter.value().get<string>();
      iter = db_open_js.find("params");
      if (db_open_js.end() == iter) {
        return Status::InvalidArgument(ROCKSDB_FUNC,
            "dbname = \"" + dbname + "\", param \"params\" is missing");
      }
      const auto& params_js = iter.value();
      Status s;
      *dbp = PluginFactory<DBT*>::AcquirePlugin(method, params_js, *this, &s);
      return s;
  };
  auto open_db = [&](const std::string& dbname) {
      auto iter = m_impl->db_js.find(dbname);
      if (m_impl->db_js.end() == iter) {
        return Status::NotFound(ROCKSDB_FUNC,
            "dbname = \"" + dbname + "\" is not found");
      }
      return open_impl(dbname, iter.value());
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
      return open_db(PluginParseInstID(str_val));
    } else {
      // string which does not like ${dbname} or $dbname
      return open_db(str_val); // str_val is dbname
    }
  } else {
    return open_impl("<inline-defined>", js);
  }
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}

std::string PluginParseInstID(const std::string& str_val) {
  // ${inst_id} or $inst_id
  if ('{' == str_val[1])
    return str_val.substr(2, str_val.size() - 3);
  else
    return str_val.substr(1, str_val.size() - 1);
}

}
