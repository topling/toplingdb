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
static Status InitRepo(const json& main_js, const char* name) {
  auto iter = main_js.find(name);
  if (main_js.end() != iter) {
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
#define INIT_REPO(T, name) \
  s = InitRepo<T>(main_js, name); if (!s.ok()) return s
  INIT_REPO(Cache, "cache");
  INIT_REPO(PersistentCache, "persistent_cache");
  INIT_REPO(FilterPolicy, "filter_policy");
  INIT_REPO(TableFactory, "table_factory");
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}


} // ROCKSDB_NAMESPACE
