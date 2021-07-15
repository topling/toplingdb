//
// Created by leipeng on 2021/1/11.
//

#include "compaction_executor.h"

namespace ROCKSDB_NAMESPACE {

CompactionParams::CompactionParams() {
  is_deserialized = false;
}
CompactionParams::~CompactionParams() {
  if (is_deserialized) {
    ROCKSDB_VERIFY(IsCompactionWorker());
    /*
    for (auto& x : *inputs) {
      for (auto& e : x.atomic_compaction_unit_boundaries) {
        delete e.smallest;
        delete e.largest;
      }
    }
    */
    if (grandparents) {
      for (auto meta : *grandparents) {
        delete meta;
      }
      delete grandparents;
    }
    if (inputs) {
      for (auto& level_files : *inputs) {
        for (auto meta : level_files.files)
          delete meta;
      }
      delete inputs;
    }
    delete existing_snapshots;
    //delete compaction_job_stats;
  }
  else {
    //ROCKSDB_VERIFY(!IsCompactionWorker());
  }
}

static void PrintVersionSetSerDe(FILE* fp, const VersionSetSerDe& v) {
  fprintf(fp, "VersionSetSerDe\n");
  fprintf(fp, "  last_sequence = %zd, "
              "last_allocated_sequence = %zd, "
              "last_published_sequence = %zd\n",
              size_t(v.last_sequence),
              size_t(v.last_allocated_sequence),
              size_t(v.last_published_sequence));
  fprintf(fp, "  next_file_number = %zd, "
              "min_log_number_to_keep_2pc = %zd, "
              "manifest_file_number = %zd, "
              "options_file_number = %zd, "
              "prev_log_number = %zd, "
              "current_version_number = %zd\n",
              size_t(v.next_file_number),
              size_t(v.min_log_number_to_keep_2pc),
              size_t(v.manifest_file_number),
              size_t(v.options_file_number),
              size_t(v.prev_log_number),
              size_t(v.current_version_number));
}
static void PrintFileMetaData(FILE* fp, const FileMetaData* f) {
  Slice temperature = enum_name(f->temperature);
  Slice lo = f->smallest.user_key();
  Slice hi = f->largest.user_key();
  fprintf(fp,
    "  %06zd.sst : entries = %zd, del = %zd, rks = %zd, rvs = %zd, "
    "fsize = %zd : %zd, temp = %.*s, seq = %zd : %zd, rng = %.*s : %.*s\n",
    size_t(f->fd.GetNumber()),
    size_t(f->num_entries), size_t(f->num_deletions),
    size_t(f->raw_key_size), size_t(f->raw_value_size),
    size_t(f->fd.file_size), size_t(f->compensated_file_size),
    int(temperature.size_), temperature.data_,
    size_t(f->fd.smallest_seqno), size_t(f->fd.largest_seqno),
    int(lo.size_), lo.data_, int(hi.size_), hi.data_);
}
void CompactionParams::DebugPrint(FILE* fout) const {
#if defined(_GNU_SOURCE)
  size_t mem_len = 0;
  char*  mem_buf = nullptr;
  FILE*  fp = open_memstream(&mem_buf, &mem_len);
#else
  FILE*  fp = fout;
#endif
  fprintf(fp, "job_id = %d, output_level = %d, dbname = %s, cfname = %s\n",
          job_id, output_level, dbname.c_str(), cf_name.c_str());
  fprintf(fp, "bottommost_level = %d, compaction_reason = %s\n",
               bottommost_level, enum_cstr(compaction_reason));
  fprintf(fp, "smallest_user_key = %s\n", smallest_user_key.c_str());
  fprintf(fp, "llargest_user_key = %s\n",  largest_user_key.c_str());
  for (size_t i = 0; i < inputs->size(); ++i) {
    auto& l = inputs->at(i);
    fprintf(fp, "inputs.size = %zd : %zd : level = %d, size = %3zd\n",
            inputs->size(), i, l.level, l.size());
    for (auto fmd : l.files) {
      PrintFileMetaData(fp, fmd);
    }
  }
  if (grandparents) {
    fprintf(fp, "grandparents.size = %zd\n", grandparents->size());
    for (size_t i = 0; i < grandparents->size(); ++i) {
      FileMetaData* fmd = grandparents->at(i);
      PrintFileMetaData(fp, fmd);
    }
  }
  else {
    fprintf(fp, "grandparents = nullptr\n");
  }
  if (existing_snapshots) {
    fprintf(fp, "existing_snapshots.size = %zd\n", existing_snapshots->size());
  }
  else {
    fprintf(fp, "existing_snapshots = nullptr\n");
  }
  PrintVersionSetSerDe(fp, version_set);
#if defined(_GNU_SOURCE)
  fclose(fp);
  fwrite(mem_buf, 1, mem_len, fout);
  free(mem_buf);
#endif
}

// res[0] : raw
// res[1] : zip
void CompactionParams::InputBytes(size_t* res) const {
  size_t raw = 0, zip = 0;
  for (auto& eachlevel : *inputs) {
    for (auto& eachfile : eachlevel.files) {
      zip += eachfile->fd.file_size;
      raw += eachfile->raw_key_size + eachfile->raw_value_size;
    }
  }
  res[0] = raw;
  res[1] = zip;
}

CompactionResults::CompactionResults() {
  curl_time_usec = 0;
  work_time_usec = 0;
  mount_time_usec = 0;
  prepare_time_usec = 0;
}
CompactionResults::~CompactionResults() {}

struct MyVersionSet : VersionSet {
  void From(const VersionSetSerDe& version_set) {
    next_file_number_ = version_set.next_file_number;
    last_sequence_ = version_set.last_sequence;
    // below are not necessary fields, but we serialize it for
    // for completeness debugging
    last_allocated_sequence_ = version_set.last_allocated_sequence;
    last_published_sequence_ = version_set.last_published_sequence;
    min_log_number_to_keep_2pc_ = version_set.min_log_number_to_keep_2pc;
    manifest_file_number_ = version_set.manifest_file_number;
    options_file_number_ = version_set.options_file_number;
    //pending_manifest_file_number_ is temporal on running, do NOT serilize!
    //pending_manifest_file_number_ = version_set.pending_manifest_file_number;
    prev_log_number_ = version_set.prev_log_number;
    current_version_number_ = version_set.current_version_number;
  }
  void To(VersionSetSerDe& version_set) const {
    version_set.next_file_number = next_file_number_;
    version_set.last_sequence = last_sequence_;
    // below are not necessary fields, but we serialize it for
    // for completeness debugging
    version_set.last_allocated_sequence = last_allocated_sequence_;
    version_set.last_published_sequence = last_published_sequence_;
    version_set.min_log_number_to_keep_2pc = min_log_number_to_keep_2pc_;
    version_set.manifest_file_number = manifest_file_number_;
    version_set.options_file_number = options_file_number_;
    //pending_manifest_file_number_ is temporal on running, do NOT serilize!
    //version_set.pending_manifest_file_number = pending_manifest_file_number_;
    version_set.prev_log_number = prev_log_number_;
    version_set.current_version_number = current_version_number_;
  }
};
void VersionSetSerDe::From(const VersionSet* vs) {
  static_cast<const MyVersionSet*>(vs)->To(*this); // NOLINT
}
void VersionSetSerDe::To(VersionSet* vs) const {
  static_cast<MyVersionSet*>(vs)->From(*this); // NOLINT
}

CompactionExecutor::~CompactionExecutor() = default;
CompactionExecutorFactory::~CompactionExecutorFactory() = default;

static bool g_is_compaction_worker = false;
bool IsCompactionWorker() {
  return g_is_compaction_worker;
}
void SetAsCompactionWorker() {
  g_is_compaction_worker = true;
}

} // namespace ROCKSDB_NAMESPACE
