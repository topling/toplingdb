//
// Created by leipeng on 2020/7/28.
//
#include "rocksdb/env.h"
#include "rocksdb/db.h"
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
using std::vector;
using std::string;

/*
{
  "DB::Open": {
    "cf_options" : {
      "default": {},
      "user-cf1": {}
    },
    "db_options" : {

    }
  },
  "db": "DB::Open"
}

*/

DB_MultiCF_Impl::DB_MultiCF_Impl() = default;
DB_MultiCF_Impl::~DB_MultiCF_Impl() = default;

ColumnFamilyHandle* DB_MultiCF_Impl::Get(const std::string& cfname) const {
  auto iter = m_cfhs.name2p->find(cfname);
  if (m_cfhs.name2p->end() != iter) {
    return iter->second;
  }
  return nullptr;
}

Status DB_MultiCF_Impl::CreateColumnFamily(const std::string& cfname,
                                           const std::string& json_str,
                                           ColumnFamilyHandle** cfh)
try {
  auto js = json::parse(json_str);
  auto cfo = ObtainOPT(m_repo.m_impl->cf_options, "CFOptions", js, m_repo);
  if (!m_create_cf) {
    return Status::InvalidArgument(ROCKSDB_FUNC, "DataBase is read only");
  }
  *cfh = m_create_cf(db, cfname, *cfo, js);
  cf_handles.push_back(*cfh);
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& st) {
  return st;
}

Status DB_MultiCF_Impl::DropColumnFamily(const std::string& cfname) try {
  auto iter = m_cfhs.name2p->find(cfname);
  if (m_cfhs.name2p->end() == iter) {
    return Status::NotFound(ROCKSDB_FUNC, "cfname = " + cfname);
  }
  ColumnFamilyHandle* cfh = iter->second;
  m_cfhs.name2p->erase(iter);
  m_cfhs.p2name.erase(cfh);
  db->DropColumnFamily(cfh);
  db->DestroyColumnFamilyHandle(cfh);
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::InvalidArgument(ROCKSDB_FUNC, ex.what());
}
catch (const Status& st) {
  return st;
}

Status DB_MultiCF_Impl::DropColumnFamily(ColumnFamilyHandle* cfh) {
  return DropColumnFamily(cfh->GetName());
}

void DB_MultiCF_Impl::AddOneCF_ToMap(const std::string& cfname,
                                     ColumnFamilyHandle* cfh,
                                     const json& js) {
  m_cfhs.name2p->emplace(cfname, cfh);
  m_cfhs.p2name.emplace(cfh, JsonPluginRepo::Impl::ObjInfo{cfname, js});
}

void DB_MultiCF_Impl::InitAddCF_ToMap(const json& js_cf_desc) {
  size_t i = 0;
  assert(js_cf_desc.size() == cf_handles.size());
  for (auto& item : js_cf_desc.items()) {
    auto& cfname = item.key();
    auto& js_cf = item.value();
    ColumnFamilyHandle* cfh = cf_handles[i];
    assert(cfname == cfh->GetName());
    AddOneCF_ToMap(cfname, cfh, js_cf);
    i++;
  }
}

} // namespace ROCKSDB_NAMESPACE
