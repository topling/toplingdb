//
// Created by leipeng on 2021/1/21.
//

#include <utilities/json/json_plugin_repo.h>
#include <utilities/json/json_plugin_factory.h>
#include <db/compaction/compaction_executor.h>
#include <env/composite_env_wrapper.h>
#include <terark/fstring.hpp>
//#include <terark/io/FileStream.hpp>
//#include <terark/io/DataIO.hpp>

//using namespace terark;
using namespace ROCKSDB_NAMESPACE;
using namespace std;

string MakeOutputPath(const CompactionParams& params) {
  string path = params.cf_paths[0].path;
  char buf[128];
  path.append(buf, snprintf(buf, sizeof(buf), "/job-%08d", params.job_id));
  return path;
}

template<class Ptr>
void CreatePluginTpl(Ptr& ptr, const ObjectRpcParam& param) {
  if (param.clazz.empty()) {
    return; // not defined
  }
  ptr = PluginFactory<Ptr>::AcquirePlugin(param.clazz, json{}, JsonPluginRepo());
  if (!param.content.empty())
    SerDe_DeSerialize(param.clazz, param.content, &*ptr);
}

int main(int argc, char* argv[]) try {
/*
  string json_file;
  if (auto env = getenv("JSON_DB_CONFIG")) {
    json_file = env;
  }
  else {
    fprintf(stderr, "");
    return 1;
  }

  JsonPluginRepo repo;
  {
    Status s = repo.ImportJsonFile(json_file);
    if (!s.ok()) {
      fprintf(stderr, "ERROR: %s\n", s.ToString().c_str());
      return 1;
    }
  }
*/
  CompactionParams  params;
  SerDeRead(stdin, &params);
  EnvOptions env_options; env_options.use_mmap_reads = true;
  Env* env = Env::Default();
  shared_ptr<FileSystem> fs(make_shared<LegacyFileSystemWrapper>(env));
  shared_ptr<Cache> table_cache = NewLRUCache(50000, 16);
  WriteController write_controller;
  WriteBufferManager write_buffer_manager(params.db_write_buffer_size);
  ImmutableDBOptions imm_dbo;
  MutableDBOptions   mut_dbo;
  ColumnFamilyOptions cfo;
#define MyCreatePlugin2(obj, field1, field2) \
  CreatePluginTpl(obj.field1, params.field2)
#define MyCreatePlugin1(obj, field) MyCreatePlugin2(obj, field, field)
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  MyCreatePlugin1(cfo, compaction_filter_factory);
  MyCreatePlugin2(cfo, comparator, user_comparator);
  MyCreatePlugin1(cfo, merge_operator);
  MyCreatePlugin1(cfo, table_factory);
  MyCreatePlugin1(cfo, prefix_extractor);
  MyCreatePlugin1(cfo, sst_partitioner_factory);

  string output_dir = MakeOutputPath(params);
  imm_dbo.env = env;
  imm_dbo.fs = fs;
  imm_dbo.db_paths = params.cf_paths;
  imm_dbo.db_paths.emplace_back(output_dir, UINT64_MAX);
  cfo.cf_paths = imm_dbo.db_paths;
  string fake_dbname = output_dir;
  env->CreateDirIfMissing(fake_dbname);
  unique_ptr<VersionSet> versions(
      new VersionSet(fake_dbname, &imm_dbo, env_options, table_cache.get(),
                       &write_buffer_manager, &write_controller,
                       /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr));
  params.version_set.To(versions.get());

  uint64_t log_number = 0;
  VersionEdit new_db;
  new_db.SetLogNumber(log_number);
  new_db.SetNextFile(params.version_set.next_file_number);
  new_db.SetLastSequence(params.version_set.last_sequence);
  auto manifest_fnum = params.version_set.manifest_file_number;
  const string manifest = DescriptorFileName(fake_dbname, manifest_fnum);
  {
    unique_ptr<WritableFile> file;
    Status s1 = env->NewWritableFile(
            manifest, &file, env->OptimizeForManifestWrite(env_options));
    TERARK_VERIFY_F(s1.ok(), "%s", s1.ToString().c_str());
    unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
     NewLegacyWritableFileWrapper(std::move(file)), manifest, env_options));
    log::Writer log(std::move(file_writer), log_number, false);
    string record;
    new_db.EncodeTo(&record);
    auto s2 = log.AddRecord(record);
    TERARK_VERIFY_F(s2.ok(), "%s", s2.ToString().c_str());
  // Make "CURRENT" file that points to the new manifest file.
    auto s3 = SetCurrentFile(fs.get(), fake_dbname, manifest_fnum, nullptr);
    TERARK_VERIFY_F(s3.ok(), "%s", s3.ToString().c_str());
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(params.cf_name, cfo);
    auto s4 = versions->Recover(column_families, false);
    TERARK_VERIFY_F(s4.ok(), "%s", s4.ToString().c_str());
    auto cfd = versions->GetColumnFamilySet()->GetColumnFamily();
  }

  size_t output_path_id = params.cf_paths.size();
  Compaction compaction();
  //----------------------------------------------------------------------------

  unique_ptr<CompactionResults> results(new CompactionResults());
  return 0;
}
catch (const std::exception& ex) {
  fprintf(stderr, "%s:%d: %s: caught exception: %s\n",
          __FILE__, __LINE__, ROCKSDB_FUNC, ex.what());
  return 1;
}
catch (const ROCKSDB_NAMESPACE::Status& s) {
  fprintf(stderr, "%s:%d: %s: caught Status: %s\n",
          __FILE__, __LINE__, ROCKSDB_FUNC, s.ToString().c_str());
  return 1;
}

