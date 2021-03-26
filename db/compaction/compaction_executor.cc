//
// Created by leipeng on 2021/1/11.
//

#include "compaction_executor.h"
#include "terark/io/DataIO.hpp"

namespace ROCKSDB_NAMESPACE {

CompactionParams::CompactionParams() {
  is_deserialized = false;
}
CompactionParams::~CompactionParams() {
  if (is_deserialized) {
    for (auto& x : *inputs) {
      for (auto& e : x.atomic_compaction_unit_boundaries) {
        delete e.smallest;
        delete e.largest;
      }
    }
    for (auto meta : *grandparents) {
      delete meta;
    }
    delete grandparents;
    for (auto& level_files : *inputs) {
      for (auto meta : level_files.files)
        delete meta;
    }
    delete inputs;
    delete existing_snapshots;
    delete compaction_job_stats;
  }
}

CompactionResults::CompactionResults() {}
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
    pending_manifest_file_number_ = version_set.pending_manifest_file_number;
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
    version_set.pending_manifest_file_number = pending_manifest_file_number_;
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
