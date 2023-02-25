//
// Created by leipeng on 2021/1/11.
//
#pragma once
#include "compaction_job.h"

namespace ROCKSDB_NAMESPACE {

struct ObjectRpcParam {
  std::string clazz;
  std::string params; // construction json params
  typedef std::function<void(FILE*, const ObjectRpcParam&)> serde_fn_t;
  serde_fn_t serde;
};
struct VersionSetSerDe {
  uint64_t last_sequence;
  uint64_t last_allocated_sequence;
  uint64_t last_published_sequence;
  uint64_t next_file_number;
 #if ROCKSDB_MAJOR < 7
  uint64_t min_log_number_to_keep_2pc;
 #else
  uint64_t min_log_number_to_keep;
 #endif
  uint64_t manifest_file_number;
  uint64_t options_file_number;
  //uint64_t pending_manifest_file_number;
  uint64_t prev_log_number;
  uint64_t current_version_number;
  void From(const VersionSet*);
  void To(VersionSet*) const;
};
struct CompactionParams {
  CompactionParams(const CompactionParams&) = delete;
  CompactionParams& operator=(const CompactionParams&) = delete;
  CompactionParams();
  ~CompactionParams();
  int job_id;
  int num_levels;
  int output_level;
  uint32_t cf_id;
  std::string cf_name;
  const std::vector<CompactionInputFiles>* inputs = nullptr;
  VersionSetSerDe version_set;
  uint64_t target_file_size;
  uint64_t max_compaction_bytes;

  // we add a dedicated path to compaction worker's cf_path as
  // output path, thus reduce changes to the existing rocksdb code.
  // the output_path_id should be the last elem of cf_paths, so it
  // needs not the field output_path_id.
  //uint32_t output_path_id; // point to the extra cf_path
  //std::string output_path; // will append to cfopt.cf_paths on remote node?
  std::vector<DbPath> cf_paths;

  uint32_t max_subcompactions; // num_threads
  CompressionType compression;
  CompressionOptions compression_opts;
  const std::vector<FileMetaData*>* grandparents = nullptr;
  double score;
  bool manual_compaction;
  bool deletion_compaction;
  InfoLogLevel compaction_log_level;
  CompactionReason compaction_reason;

  //VersionSet* version_set;
  SequenceNumber preserve_deletes_seqnum;
  const std::vector<SequenceNumber>* existing_snapshots = nullptr;
  SequenceNumber smallest_seqno;
  SequenceNumber earliest_write_conflict_snapshot;
  bool paranoid_file_checks;
  uint32_t    code_version;
  std::string code_githash;
  std::string hoster_root;
  std::string instance_name;
  std::string dbname;
  std::string db_id;
  std::string db_session_id;
  std::string full_history_ts_low;
  //CompactionJobStats* compaction_job_stats = nullptr; // this is out param
  //SnapshotChecker* snapshot_checker; // not used
  //FSDirectory* db_directory;
  //FSDirectory* output_directory;
  //FSDirectory* blob_output_directory;

  std::string smallest_user_key; // serialization must before
  std::string largest_user_key;  // ObjectRpcParam fields
  //ObjectRpcParam compaction_filter; // don't use compaction_filter
  ObjectRpcParam compaction_filter_factory; // always use
  ObjectRpcParam merge_operator;
  ObjectRpcParam user_comparator;
  ObjectRpcParam table_factory;
  ObjectRpcParam prefix_extractor;
  ObjectRpcParam sst_partitioner_factory;
  ObjectRpcParam html_user_key_coder;

  //bool skip_filters;
  bool allow_ingest_behind;
  bool preserve_deletes;
  bool bottommost_level;
  bool is_deserialized;
  CompactionStyle compaction_style;
  CompactionPri   compaction_pri;
  std::vector<ObjectRpcParam> listeners;
  std::vector<ObjectRpcParam> table_properties_collector_factories;
  std::string extensible_js_data;

  // CompactionFilterFactory ... can have individual serde files
  mutable std::vector<std::string> extra_serde_files;
  Logger* info_log = nullptr; // do not serialize, just for running process
  mutable class UserKeyCoder* p_html_user_key_coder = nullptr;
  const std::atomic<bool>* shutting_down = nullptr; // do not serialize

  std::string DebugString() const;
  void InputBytes(size_t* res) const;
};

struct CompactionResults {
  CompactionResults(const CompactionResults&) = delete;
  CompactionResults& operator=(const CompactionResults&) = delete;
  CompactionResults();
  ~CompactionResults();
  struct FileMinMeta {
    uint64_t    file_number;
    uint64_t    file_size;
    uint64_t    smallest_seqno;
    uint64_t    largest_seqno;
    InternalKey smallest_ikey;
    InternalKey largest_ikey;
    bool marked_for_compaction;
  };
  // collect remote statistics
  struct RawStatistics {
    uint64_t tickers[INTERNAL_TICKER_ENUM_MAX] = {0};
    HistogramStat histograms[INTERNAL_HISTOGRAM_ENUM_MAX];
  };

  std::string output_dir;
  std::vector<std::vector<FileMinMeta> > output_files;
  InternalStats::CompactionStats compaction_stats;
  CompactionJobStats job_stats;
  RawStatistics statistics;
  Status status;
  size_t curl_time_usec; // set by CompactionExecutor, not worker
  size_t work_time_usec;
  size_t mount_time_usec; // mount nfs
  size_t prepare_time_usec; // open nfs params/results
  size_t waiting_time_usec; // wait in work queue

  uint64_t output_index_size; // not serialized, just for DB side convenient
  uint64_t output_data_size; // not serialized, just for DB side convenient

  size_t all_time_usec() const {
    return curl_time_usec + mount_time_usec + prepare_time_usec + work_time_usec;
  }
};

class CompactionExecutor {
 public:
  virtual ~CompactionExecutor();
  virtual void SetParams(CompactionParams*, const Compaction*) = 0;
  virtual Status Execute(const CompactionParams&, CompactionResults*) = 0;
  virtual void CleanFiles(const CompactionParams&, const CompactionResults&) = 0;
};

class CompactionExecutorFactory {
 public:
  virtual ~CompactionExecutorFactory();
  virtual bool ShouldRunLocal(const Compaction*) const = 0;
  virtual bool AllowFallbackToLocal() const = 0;
  virtual CompactionExecutor* NewExecutor(const Compaction*) const = 0;
  virtual const char* Name() const = 0;
};

/////////////////////////////////////////////////////////////////////////////

std::string GetDirFromEnv(const char* name, const char* Default = nullptr);
bool ReplacePrefix(Slice Old, Slice New, Slice str, std::string* res);
std::string ReplacePrefix(Slice Old, Slice New, Slice str);
void ReplaceAll(std::string& str, Slice from, Slice to);
std::string ReplaceAll(Slice str, Slice from, Slice to);
std::string MakePath(std::string dir, Slice sub);
std::string& AppendJobID(std::string& path, int job_id);
std::string CatJobID(const std::string& path, int job_id);
std::string& AppendAttempt(std::string& path, int attempt);
std::string CatAttempt(const std::string& path, int attempt);

} // namespace ROCKSDB_NAMESPACE
