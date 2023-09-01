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

#if defined(_MSC_VER)
static std::string html_user_key_decode(const CompactionParams&, Slice uk) {
  return uk.ToString(true);
}
#else
std::string __attribute__((weak))
CompactionParams_html_user_key_decode(const CompactionParams&, Slice);
static std::string html_user_key_decode(const CompactionParams& cp, Slice uk) {
  if (CompactionParams_html_user_key_decode)
    return CompactionParams_html_user_key_decode(cp, uk);
  else
    return uk.ToString(true);
}
#endif

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
            #if ROCKSDB_MAJOR < 7
              size_t(v.min_log_number_to_keep_2pc),
            #else
              size_t(v.min_log_number_to_keep),
            #endif
              size_t(v.manifest_file_number),
              size_t(v.options_file_number),
              size_t(v.prev_log_number),
              size_t(v.current_version_number));
}
static void PrintFileMetaData(const CompactionParams& cp,
                              FILE* fp, const FileMetaData* f) {
  Slice temperature = enum_name(f->temperature);
  std::string lo = html_user_key_decode(cp, f->smallest.user_key());
  std::string hi = html_user_key_decode(cp, f->largest.user_key());
  fprintf(fp,
    "  %06zd.sst : entries = %zd, del = %zd, rks = %zd, rvs = %zd, "
    "fsize = %zd : %zd, temp = %.*s, seq = %zd : %zd, rng = %.*s : %.*s\n",
    size_t(f->fd.GetNumber()),
    size_t(f->num_entries), size_t(f->num_deletions),
    size_t(f->raw_key_size), size_t(f->raw_value_size),
    size_t(f->fd.file_size), size_t(f->compensated_file_size),
    int(temperature.size_), temperature.data_,
    size_t(f->fd.smallest_seqno), size_t(f->fd.largest_seqno),
    int(lo.size()), lo.data(), int(hi.size()), hi.data());
}

std::string CompactionParams::DebugString() const {
  size_t mem_len = 0;
  char*  mem_buf = nullptr;
  FILE*  fp = open_memstream(&mem_buf, &mem_len);
  fprintf(fp, "job_id = %d, output_level = %d, dbname = %s, cfname = %s\n",
          job_id, output_level, dbname.c_str(), cf_name.c_str());
  fprintf(fp, "bottommost_level = %d, compaction_reason = %s\n",
               bottommost_level, enum_cstr(compaction_reason));
  fprintf(fp, "smallest_user_key = %s\n", html_user_key_decode(*this, smallest_user_key).c_str());
  fprintf(fp, "llargest_user_key = %s\n", html_user_key_decode(*this,  largest_user_key).c_str());
  for (size_t i = 0; i < inputs->size(); ++i) {
    auto& l = inputs->at(i);
    fprintf(fp, "inputs.size = %zd : %zd : level = %d, size = %3zd\n",
            inputs->size(), i, l.level, l.size());
    for (auto fmd : l.files) {
      PrintFileMetaData(*this, fp, fmd);
    }
  }
  if (grandparents) {
    fprintf(fp, "grandparents.size = %zd\n", grandparents->size());
    for (size_t i = 0; i < grandparents->size(); ++i) {
      FileMetaData* fmd = grandparents->at(i);
      PrintFileMetaData(*this, fp, fmd);
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
  fprintf(fp, "level_compaction_dynamic_file_size = %s",
               level_compaction_dynamic_file_size ? "true" : "false");
  PrintVersionSetSerDe(fp, version_set);
  fclose(fp);
  std::string result(mem_buf, mem_len);
  free(mem_buf);
  return result;
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
  waiting_time_usec = 0;
  output_index_size = 0;
  output_data_size = 0;
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
   #if ROCKSDB_MAJOR < 7
    min_log_number_to_keep_2pc_ = version_set.min_log_number_to_keep_2pc;
   #else
    min_log_number_to_keep_ = version_set.min_log_number_to_keep;
   #endif
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
   #if ROCKSDB_MAJOR < 7
    version_set.min_log_number_to_keep_2pc = min_log_number_to_keep_2pc_;
   #else
    version_set.min_log_number_to_keep = min_log_number_to_keep_;
   #endif
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

/////////////////////////////////////////////////////////////////////////////
std::string GetDirFromEnv(const char* name, const char* Default) {
  const char* dir = getenv(name);
  if (nullptr == dir) {
    ROCKSDB_VERIFY(nullptr != Default);
    dir = Default;
  }
  size_t dir_name_len = strlen(dir);
  ROCKSDB_VERIFY(dir_name_len > 0);
  while (dir_name_len && '/' == dir[dir_name_len-1]) {
    dir_name_len--;
  }
  ROCKSDB_VERIFY(dir_name_len > 0);
  return std::string(dir, dir_name_len);
}

bool ReplacePrefix(Slice Old, Slice New, Slice str, std::string* res) {
  ROCKSDB_VERIFY(Old.size_ > 0);
  ROCKSDB_VERIFY(New.size_ > 0);
  while (Old.size_ && Old.data_[Old.size_-1] == '/') {
    --Old.size_;
  }
  while (New.size_ && New.data_[New.size_-1] == '/') {
    --New.size_;
  }
  ROCKSDB_VERIFY(Old.size_ > 0);
  ROCKSDB_VERIFY(New.size_ > 0);
  if (str.starts_with(Old)) {
    size_t suffixLen = str.size_ - Old.size_;
    res->reserve(New.size_ + suffixLen);
    res->assign(New.data_, New.size_);
    res->append(str.data_ + Old.size_, suffixLen);
    return true;
  }
  return false;
}

std::string ReplacePrefix(Slice Old, Slice New, Slice str) {
  std::string res;
  if (ReplacePrefix(Old, New, str, &res)) {
    return res;
  }
  ROCKSDB_DIE("str = '%.*s' does not start with Old='%.*s'",
              int(str.size()), str.data(), int(Old.size()), Old.data());
}

void ReplaceAll(std::string& str, Slice from, Slice to) {
  if (from.empty()) return;
  size_t start_pos = 0;
  while ((start_pos = str.find(from.data(), start_pos)) != std::string::npos) {
    str.replace(start_pos, from.size(), to.data(), to.size());
    start_pos += to.size();
  }
}
std::string ReplaceAll(Slice str, Slice from, Slice to) {
  std::string tmp(str.data(), str.size());
  ReplaceAll(tmp, from, to);
  return tmp;
}
std::string MakePath(std::string dir, Slice sub) {
  while (!dir.empty() && '/' == dir.back()) {
    dir.pop_back();
  }
  dir.reserve(dir.size() + 1 + sub.size());
  ROCKSDB_VERIFY(!sub.empty());
  while (!sub.empty() && '/' == sub[0]) {
    sub.remove_prefix(1);
  }
  ROCKSDB_VERIFY(!sub.empty());
  dir.push_back('/');
  dir.append(sub.data(), sub.size());
  return dir;
}

std::string& AppendJobID(std::string& dir, int job_id) {
  while (!dir.empty() && '/' == dir.back()) {
    dir.pop_back();
  }
  char buf[32];
  dir.append(buf, snprintf(buf, sizeof(buf), "/job-%05d", job_id));
  return dir;
}
std::string CatJobID(const std::string& dir, int job_id) {
  std::string output_path = dir;
  AppendJobID(output_path, job_id);
  return output_path;
}
std::string& AppendAttempt(std::string& dir, int attempt) {
  while (!dir.empty() && '/' == dir.back()) {
    dir.pop_back();
  }
  char buf[32];
  dir.append(buf, snprintf(buf, sizeof(buf), "/att-%02d", attempt));
  return dir;
}
std::string CatAttempt(const std::string& dir, int attempt) {
  std::string output_path = dir;
  AppendAttempt(output_path, attempt);
  return output_path;
}

} // namespace ROCKSDB_NAMESPACE
