//
// Created by leipeng on 2021/1/19.
//

#pragma once

#include "json_plugin_repo.h"
#include "json_plugin_factory.h"
#include <rocksdb/merge_operator.h>
#include <db/compaction/compaction_executor.h>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/util/process.hpp>

namespace ROCKSDB_NAMESPACE {

using terark::ExplicitSerDePointer;

template<class DataIO>
void DataIO_saveObject(DataIO& dio, const Slice& x) {
  dio << terark::var_size_t(x.size_);
  dio.ensureWrite(x.data_, x.size_);
}
template<class DataIO>
void DataIO_loadObject(DataIO& dio, InternalKey& x) {
  dio >> *x.rep();
}
template<class DataIO>
void DataIO_saveObject(DataIO& dio, const InternalKey& x) {
  dio << x.Encode();
}
struct Status_SerDe : Status {
  Status_SerDe(uint8_t _code, uint8_t _subcode, const Slice& msg)
    : Status(Code(_code), SubCode(_subcode), msg, Slice("")) {}
};
template<class DataIO>
void DataIO_loadObject(DataIO& dio, Status& x) {
  uint8_t code, subcode;
  std::string msg;
  dio >> code >> subcode >> msg;
  if (Status::Code(code) == Status::kOk)
    x = Status::OK();
  else
    x = Status_SerDe(code, subcode, msg);
}
template<class DataIO>
void DataIO_saveObject(DataIO& dio, const Status& x) {
  dio << uint8_t(x.code());
  dio << uint8_t(x.subcode());
  if (x.ok()) {
    dio << Slice(x.getState());
  }
  else {
    dio.writeByte(0);
  }
}


DATA_IO_LOAD_SAVE_E(FileDescriptor, & packed_number_and_path_id & file_size
                  & smallest_seqno & largest_seqno)
DATA_IO_DUMP_RAW_MEM_E(FileSampledStats)
DATA_IO_LOAD_SAVE_E(FileMetaData, & fd & smallest & largest & stats
                  & compensated_file_size
                  & num_entries & num_deletions
                  & raw_key_size & raw_value_size
                  & being_compacted & init_stats_from_file
                  & marked_for_compaction & oldest_blob_file_number
                  & oldest_ancester_time & file_creation_time
                  & file_checksum & file_checksum_func_name
                  )
template<class DataIO>
void DataIO_loadObject(DataIO& dio, std::vector<FileMetaData*>& vec) {
  size_t num = dio.template load_as<terark::var_size_t>().t;
  vec.clear();
  vec.reserve(num);
  for (size_t i = 0; i < num; ++i) {
    std::unique_ptr<FileMetaData> meta(new FileMetaData());
    dio >> *meta;
    vec.push_back(meta.release());
  }
}
template<class DataIO>
void DataIO_saveObject(DataIO& dio, const std::vector<FileMetaData*>& vec) {
  size_t num = vec.size();
  dio << terark::var_size_t(num);
  for (FileMetaData* p : vec) {
    dio << *p;
  }
}

template<class DataIO>
void DataIO_loadObject(DataIO& dio, AtomicCompactionUnitBoundary& x) {
  std::unique_ptr<InternalKey> smallest(new InternalKey());
  std::unique_ptr<InternalKey> largest(new InternalKey());
  dio >> *smallest;
  dio >> *largest;
  x.smallest = smallest.release();
  x.largest = largest.release();
}
template<class DataIO>
void DataIO_saveObject(DataIO& dio, const AtomicCompactionUnitBoundary& x) {
  dio << *x.smallest;
  dio << *x.largest;
}

DATA_IO_LOAD_SAVE_E(CompactionInputFiles,
                  & level & files & atomic_compaction_unit_boundaries)
/*
DATA_IO_LOAD_SAVE_E(VersionStorageInfo, & num_levels_ & num_non_empty_levels_
                  & level_max_bytes_
                  & level_files_brief_
                  )
*/
/*
DATA_IO_LOAD_SAVE_E(VersionSet, & next_file_number_ & last_sequence_
                  // below are not necessary fields, but we serialize it for
                  // for completeness & debugging
                  & last_allocated_sequence_
                  & last_published_sequence_
                  & min_log_number_to_keep_2pc_
                  & manifest_file_number_
                  & options_file_number_
                  & pending_manifest_file_number_
                  & prev_log_number_
                  & current_version_number_
                  )
*/
DATA_IO_DUMP_RAW_MEM_E(VersionSetSerDe)
DATA_IO_LOAD_SAVE_E(CompactionParams,
                  & job_id & num_levels & output_level & cf_id
                  & version_set
                  & target_file_size
                  & max_compaction_bytes & cf_paths & max_subcompactions
                  & compression & compression_opts & score
                  & manual_compaction & deletion_compaction
                  & compaction_reason
                  & preserve_deletes_seqnum
                  & earliest_write_conflict_snapshot & paranoid_file_checks
                  & dbname & db_id & db_session_id & full_history_ts_low
                  & compaction_filter_factory & merge_operator
                  & user_comparator & table_factory & prefix_extractor
                  & sst_partitioner_factory & allow_ingest_behind
                  & preserve_deletes & bottommost_level
                  & smallest_user_key & largest_user_key
                  & int_tbl_prop_collector_factories
                  & ExplicitSerDePointer(inputs)
                  & ExplicitSerDePointer(grandparents)
                  //& ExplicitSerDePointer(version_set)
                  & ExplicitSerDePointer(existing_snapshots)
                  & ExplicitSerDePointer(compaction_job_stats)
                  )

using FileMinMeta = CompactionResults::FileMinMeta;
using ObjectRpcRetVal = CompactionResults::ObjectRpcRetVal;
using StatisticsResult = CompactionResults::StatisticsResult;

DATA_IO_LOAD_SAVE_E(FileMinMeta, &fname &fsize &smallest_seqno &largest_seqno)
DATA_IO_LOAD_SAVE_E(StatisticsResult, & tickers & histograms)
DATA_IO_DUMP_RAW_MEM_E(HistogramStat)
DATA_IO_LOAD_SAVE_E(CompactionJobStats, & elapsed_micros & cpu_micros
                  & num_input_records & num_input_files
                  & num_input_files_at_output_level
                  & num_output_records & num_output_files
                  & is_full_compaction & is_manual_compaction
                  & total_input_bytes & total_output_bytes
                  & num_records_replaced
                  & total_input_raw_key_bytes
                  & total_input_raw_value_bytes
                  & num_input_deletion_records
                  & num_expired_deletion_records
                  & num_corrupt_keys & file_write_nanos
                  & file_range_sync_nanos & file_fsync_nanos
                  & file_prepare_write_nanos
                  & smallest_output_key_prefix & largest_output_key_prefix
                  & num_single_del_fallthru & num_single_del_mismatch
                  )
DATA_IO_LOAD_SAVE_E(ObjectRpcRetVal, & compaction_filter & merge_operator
                  & user_comparator & table_builder & prefix_extractor
                  & int_tbl_prop_collector & event_listner
                  & output_files & job_stats & num_output_records
                  )
DATA_IO_LOAD_SAVE_E(CompactionResults, & sub_compacts & stat_result & status)

void SerDeRead(FILE* fp, CompactionParams* p) {
  using namespace terark;
  LittleEndianDataInput<NonOwnerFileStream> dio(fp);
  dio >> *p;
}
void SerDeWrite(FILE* fp, CompactionResults* res) {
  using namespace terark;
  LittleEndianDataOutput<NonOwnerFileStream> dio(fp);
  dio << *res;
}

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
void SetObjectRpcParamReqTpl(ObjectRpcParam& p, const ObjectPtr& obj) {
  if (auto p_obj = GetRawPtr(obj)) {
    p.clazz = obj->Name();
    p.content = SerDe_SerializeReq(p.clazz, p_obj);
  }
}
template<class ObjectPtr>
void SetObjectRpcParamOptTpl(ObjectRpcParam& p, const ObjectPtr& obj) {
  if (auto p_obj = GetRawPtr(obj)) {
    p.clazz = obj->Name();
    p.content = SerDe_SerializeOpt(p.clazz, p_obj);
  }
}

class RemoteCompactionExecutor : public CompactionExecutor {
  std::string m_cmd;
 public:
  explicit RemoteCompactionExecutor(std::string cmd) : m_cmd(std::move(cmd)) {}
  void SetParams(CompactionParams*,
                 const ImmutableCFOptions&,
                 const MutableCFOptions&) override;
  Status Execute(const CompactionParams&, CompactionResults*) override;
};

void RemoteCompactionExecutor::SetParams(CompactionParams* params,
                                         const ImmutableCFOptions& imm_cfo,
                                         const MutableCFOptions& mut_cfo) {
  //uint32_t cf_id = params->cf_id;
#define SetObjectRpcParamReq(cfo, field) \
  SetObjectRpcParamReqTpl(params->field, cfo.field)
#define SetObjectRpcParamOpt(cfo, field) \
  SetObjectRpcParamOptTpl(params->field, cfo.field)

  SetObjectRpcParamReq(imm_cfo, compaction_filter_factory);
  SetObjectRpcParamOpt(imm_cfo, sst_partitioner_factory);
  SetObjectRpcParamOpt(mut_cfo, prefix_extractor);
  SetObjectRpcParamReq(imm_cfo, table_factory);
  SetObjectRpcParamOpt(imm_cfo, merge_operator);
  SetObjectRpcParamOpt(imm_cfo, user_comparator);
//  params->event_listner.reserve(imm_cfo.listeners.size());
//  for (auto& x : imm_cfo.listeners) {
//    params->event_listner.push_back(x->Name()); // no ->Name()
//  }
  params->int_tbl_prop_collector_factories.reserve(
      imm_cfo.table_properties_collector_factories.size());
  for (auto& tpc : imm_cfo.table_properties_collector_factories) {
    ObjectRpcParam p;
    SetObjectRpcParamOptTpl(p, tpc);
    params->int_tbl_prop_collector_factories.push_back(std::move(p));
  }
  params->allow_ingest_behind = imm_cfo.allow_ingest_behind;

  params->cf_paths = imm_cfo.cf_paths;
  //params->output_path = ??; // TODO: use remote hostname?
}

Status RemoteCompactionExecutor::Execute(const CompactionParams& params,
                                         CompactionResults* results)
try {
  using namespace terark;
  auto write_pipe = [&](ProcPipeStream& pipe) {
    auto& dio = static_cast<LittleEndianDataOutput<ProcPipeStream>&>(pipe);
    dio << params;
  };
  auto future = vfork_cmd(m_cmd, ref(write_pipe));
  auto output = future.get();
  LittleEndianDataInput<MemIO> mem(output.data(), output.size());
  mem >> *results;
  return Status::OK();
}
catch (const std::exception& ex) {
  return Status::Corruption(ROCKSDB_FUNC, ex.what());
}

class RemoteCompactionExecutorFactory : public CompactionExecutorFactory {
  std::string m_cmd;
 public:
  explicit
  RemoteCompactionExecutorFactory(std::string cmd) : m_cmd(std::move(cmd)) {}

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
