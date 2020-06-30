//
// Created by leipeng on 2020-06-30.
//

#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <rocksdb/cache.h>
#include <rocksdb/persistent_cache.h>
#include <rocksdb/filter_policy.h>


#include <util/json.h>

namespace ROCKSDB_NAMESPACE {

template<class T>
static Status InitRepo(const json& repo_js, const char* name) {
  auto iter = repo_js.find(name);
  if (repo_js.end() != iter) {
    for (auto& one : iter.value().items()) {
      const std::string& inst_id = one.key();
      const json& js = one.value();
      std::shared_ptr<T> obj;
      ROCKSDB_JSON_GET_NEST_IMPL(js, obj, T);
      T::InsertRepoInstance(inst_id, obj);
    }
  }
  return Status::OK();
}


Status DB::InitConfigRepo(const std::string& json_text) try {
  json main_js(json_text);
  Status s;
#define INIT_REPO(js, T, name) \
  s = InitRepo<T>(js, name); if (!s.ok()) return s
  auto iter = main_js.find("global");
  if (main_js.end() != iter) {
    auto& global_js = iter.value();
    iter = global_js.find("DBOptions");
    if (global_js.end() != iter) {
      // TODO:
    }
    INIT_REPO(global_js, Cache, "cache");
    INIT_REPO(global_js, PersistentCache, "persistent_cache");
    INIT_REPO(global_js, FilterPolicy, "filter_policy");
    INIT_REPO(global_js, TableFactory, "table_factory");
  }
/* TODO:
  iter = main_js.find("column_family");
  if (main_js.end() != iter) {
    auto& cfset_js = iter.value();
    for (auto& cf : cfset_js.items()) {
      const std::string& cf_name = cf.key();
      // TODO:
    }
  }
*/
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}


} // ROCKSDB_NAMESPACE
