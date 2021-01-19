//
// Created by leipeng on 2021/1/19.
//

#pragma once

#include "json_plugin_repo.h"
#include "json_plugin_factory.h"
#include <rocksdb/merge_operator.h>
#include <db/compaction/compaction_executor.h>

namespace ROCKSDB_NAMESPACE {


class RemoteCompactionExecutor : public CompactionExecutor {
  std::string m_cmd;
 public:
  explicit RemoteCompactionExecutor(const std::string& cmd) : m_cmd(cmd) {}
  void SetParams(CompactionParams*,
                 const ImmutableCFOptions&,
                 const MutableCFOptions&) override;
  Status Execute(const CompactionParams&, CompactionResults*) override;
};

//----------------------------------------------------------------------------
// SerDe example for TablePropertiesCollector
struct MySerDe : SerDeFunc<TablePropertiesCollector> {
  Status Serialize(const TablePropertiesCollector&, std::string* /*output*/)
  const override {
    // do serialize
    return Status::OK();
  }
  Status DeSerialize(TablePropertiesCollector*, const Slice& /*input*/)
  const override {
    // do deserialize
    return Status::OK();
  }
};
static const SerDeFunc<TablePropertiesCollector>*
CreateMySerDe(const json&,const JsonPluginRepo&) {
  static MySerDe serde;
  return &serde;
}
//ROCKSDB_FACTORY_REG("CompactOnDeletionCollector", CreateMySerDe);
ROCKSDB_FACTORY_REG("MySerDe", CreateMySerDe);

void ExampleUseMySerDe(const std::string& clazz) {
  // SerDe should have same class name(clazz) with PluginFactory
  Status s;
  const SerDeFunc<TablePropertiesCollector>* serde =
      SerDeFactory<TablePropertiesCollector>::AcquirePlugin(
          clazz, json{}, JsonPluginRepo{});
  auto factory =
      PluginFactorySP<TablePropertiesCollectorFactory>::AcquirePlugin(
          clazz, json{}, JsonPluginRepo{});
  auto instance = factory->CreateTablePropertiesCollector({});
  std::string bytes;
  s = serde->Serialize(*instance, &bytes);
  // send bytes ...
  // recv bytes ...
  s = serde->DeSerialize(&*instance, bytes);
}
// end. SerDe example for TablePropertiesCollector

template<class ObjectPtr>
void SetObjectRpcParamTpl(ObjectRpcParam& p, const ObjectPtr& obj) {
  if (auto p_obj = GetRawPtr(obj)) {
    p.clazz = obj->Name();
    p.content = SerDe_Serialize(p.clazz, p_obj);
  }
}
void RemoteCompactionExecutor::SetParams(CompactionParams* params,
                                         const ImmutableCFOptions& imm_cfo,
                                         const MutableCFOptions& mut_cfo) {
  //uint32_t cf_id = params->cf_id;
#define SetObjectRpcParam(cfo, field) \
  SetObjectRpcParamTpl(params->field, cfo.field)
  SetObjectRpcParam(imm_cfo, compaction_filter_factory);
  SetObjectRpcParam(imm_cfo, sst_partitioner_factory);
  SetObjectRpcParam(mut_cfo, prefix_extractor);
  SetObjectRpcParam(imm_cfo, table_factory);
  SetObjectRpcParam(imm_cfo, merge_operator);
  SetObjectRpcParam(imm_cfo, user_comparator);
//  params->event_listner.reserve(imm_cfo.listeners.size());
//  for (auto& x : imm_cfo.listeners) {
//    params->event_listner.push_back(x->Name()); // no ->Name()
//  }
  params->int_tbl_prop_collector_factories.reserve(
      imm_cfo.table_properties_collector_factories.size());
  for (auto& tpc : imm_cfo.table_properties_collector_factories) {
    ObjectRpcParam p;
    SetObjectRpcParamTpl(p, tpc);
    params->int_tbl_prop_collector_factories.push_back(std::move(p));
  }
  params->allow_ingest_behind = imm_cfo.allow_ingest_behind;

  params->cf_paths = imm_cfo.cf_paths;
  //params->output_path = ??; // TODO: use remote hostname?
}

Status RemoteCompactionExecutor::Execute(const CompactionParams& params,
                                         CompactionResults* results)
{

  return Status::OK();
}

class RemoteCompactionExecutorFactory : public CompactionExecutorFactory {
  std::string m_cmd;
 public:
  RemoteCompactionExecutorFactory(const std::string& cmd) : m_cmd(cmd) {}

  CompactionExecutor* NewExecutor(const Compaction*) const override {
    return new RemoteCompactionExecutor(m_cmd);
  }
  const char* Name() const override {
    return "RemoteCompaction";
  }
};

static std::shared_ptr<CompactionExecutorFactory>
JS_NewRemoteCompactionFactory(const json& js, const JsonPluginRepo&) {
  std::string cmd;
  ROCKSDB_JSON_REQ_PROP(js, cmd);
  return std::make_shared<RemoteCompactionExecutorFactory>(cmd);
}
ROCKSDB_FACTORY_REG("RemoteCompaction", JS_NewRemoteCompactionFactory);


}
