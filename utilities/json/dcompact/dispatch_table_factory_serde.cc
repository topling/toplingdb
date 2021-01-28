//
// Created by leipeng on 2021-01-27.
//
#include <utilities/json/json_table_factory.h>
#include <utilities/json/json_plugin_factory.h>
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/io/access_byid.hpp>
#include <terark/util/atomic.hpp>

namespace ROCKSDB_NAMESPACE {

extern bool IsCompactionWorker();

using namespace terark;

using DioWriter = LittleEndianDataOutput<AutoGrownMemIO>;
using DioReader = LittleEndianDataInput<MemIO>;

/*
template<class T>
void write_obj(DioWriter& dio, std::map<const T*, size_t>& ptr2id,
               const T* obj) {
  size_t id = ptr2id.size() + 1;
  auto ib = ptr2id.emplace(obj, id);
  if (ib.second) {
    dio.write_var_uint64(id);
    dio << *obj;
  } else {
    size_t existed_id = ib.first->second;
    dio.write_var_uint64(existed_id);
  }
}
*/

//using ReaderFactory = DispatherTableFactory::ReaderFactory;
using Stat = DispatherTableFactory::Stat;
//using TimeStat = DispatherTableFactory::TimeStat;
//using TimePoint = std::chrono::steady_clock::time_point;
//DATA_IO_DUMP_RAW_MEM_E(TimePoint)
//DATA_IO_LOAD_SAVE_E(ReaderFactory, & varname & open_cnt
//                                   & sum_open_size & is_user_defined)
//DATA_IO_LOAD_SAVE_E(Stat, & entry_cnt & key_size & val_size)
//DATA_IO_LOAD_SAVE_E(TimeStat, & st & time)
//DATA_IO_DUMP_RAW_MEM_E(TimeStat)
DATA_IO_DUMP_RAW_MEM_E(Stat)

class DispatherTableFactoryEx : public DispatherTableFactory {
public:
  void HosterSide_Serialize(std::string* output) const {
    std::map<const TableFactory*, std::string> ptr2name;
    for (auto& kv : *m_all) {
      ptr2name.emplace(kv.second.get(), kv.first);
    }
    DioWriter dio(64*1024);
    //dio << m_json_str; // has been set by cons
    dio.write_var_uint64(ptr2name.size());
    for (auto& kv : ptr2name) {
      const TableFactory* tf = kv.first;
      const std::string& name = kv.second;
      const std::string clazz = tf->Name();
      const std::string serde = SerDe_SerializeOpt(clazz, tf);
      auto ith = lower_bound_ex_a(m_cons_params, tf, TERARK_GET(.first));
      TERARK_VERIFY_LT(ith, m_cons_params.size());
      std::string cons_jstr = m_cons_params[ith].second->dump();
      dio << name << clazz << cons_jstr << serde;
    }
    output->assign((const char*)dio.begin(), dio.tell());
  }
  void WorkerSide_DeSerialize(Slice input) {
    DioReader dio(input.data_, input.size_);
    size_t num = dio.read_var_uint64();
    JsonPluginRepo repo;
    for (size_t i = 0; i < num; ++i) {
      std::string name, clazz, cons_jstr, serde;
      dio >> name >> clazz >> cons_jstr >> serde;
      json js = json::parse(cons_jstr);
      auto tf = PluginFactory<std::shared_ptr<TableFactory> >::
      AcquirePlugin(clazz, js, repo);
      SerDe_DeSerialize(clazz, serde, tf.get());
      repo.Put(name, tf);
      repo.m_impl->table_factory.p2name[tf.get()].params = std::move(js);
    }
    BackPatch(repo); // NOLINT
  }
  void WorkerSide_Serialize(std::string* output) const {
    DioWriter dio(128);
    size_t level = 0, num_levels = m_stats[0].size();
    for (level = 0; level < num_levels; ++level) {
      if (m_stats[0][level].st.entry_cnt) {
        dio.writeByte(1);
        dio.write_var_uint64(level);
        dio << m_stats[0][level].st;
        dio << m_writer_files[level+1];
        output->assign((const char*)dio.begin(), dio.tell());
        return;
      }
    }
    output->assign("", 1); // '\0'
  }
  void HosterSide_DeSerialize(Slice input) {
    DioReader dio(input.data_, input.size_);
    const auto hasData = dio.readByte();
    if (hasData) {
      Stat st;
      size_t writer_files = 0;
      size_t level = (size_t)dio.read_var_uint64();
      dio >> st;
      dio >> writer_files;
      as_atomic(m_writer_files[level]).fetch_add(writer_files);
      this->UpdateStat(level, st); // NOLINT
    }
  }
};

struct DispatherTableFactory_SerDe : SerDeFunc<TableFactory> {
  Status Serialize(const TableFactory& object, std::string* output)
  const override try {
    auto& f1 = dynamic_cast<const DispatherTableFactory&>(object);
    auto& factory = static_cast<const DispatherTableFactoryEx&>(f1);
    if (IsCompactionWorker()) {
      factory.WorkerSide_Serialize(output);
    } else {
      factory.HosterSide_Serialize(output);
    }
    return Status::OK();
  }
  catch (const std::exception& ex) {
    return Status::Corruption(ROCKSDB_FUNC, ex.what());
  }
  Status DeSerialize(TableFactory* object, const Slice& input)
  const override try {
    auto* f1 = dynamic_cast<DispatherTableFactory*>(object);
    auto* factory = static_cast<DispatherTableFactoryEx*>(f1);
    if (IsCompactionWorker()) {
      factory->WorkerSide_DeSerialize(input);
    } else {
      factory->HosterSide_DeSerialize(input);
    }
    return Status::OK();
  }
  catch (const std::exception& ex) {
    return Status::Corruption(ROCKSDB_FUNC, ex.what());
  }
};
ROCKSDB_REG_PluginSerDe("DispatherTable", DispatherTableFactory_SerDe);
ROCKSDB_REG_PluginSerDe("Dispather", DispatherTableFactory_SerDe);
ROCKSDB_REG_PluginSerDe("Dispath", DispatherTableFactory_SerDe);


} // ROCKSDB_NAMESPACE
