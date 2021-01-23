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
    delete inputs;
    delete existing_snapshots;
    delete compaction_job_stats;
  }
}

void CompactionResults::ObjectRpcRetVal::resize(size_t n) {
  compaction_filter_factory.resize(n); // for each compaction filter
  merge_operator.resize(n);
  user_comparator.resize(n);
  table_factory.resize(n); // table builder
  prefix_extractor.resize(n);
  sst_partitioner_factory.resize(n);
  int_tbl_prop_collector.resize(n);
  event_listner.resize(n);
  output_files.resize(n);
  job_stats.resize(n);
  num_output_records.resize(n);
}
size_t CompactionResults::ObjectRpcRetVal::size() const {
  size_t n_size = compaction_filter_factory.size();
  //assert(n_size == compaction_filter_factory.size());
  assert(n_size == merge_operator.size());
  assert(n_size == user_comparator.size());
  assert(n_size == table_factory.size()); // table builder
  assert(n_size == prefix_extractor.size());
  assert(n_size == sst_partitioner_factory.size());
  assert(n_size == int_tbl_prop_collector.size());
  assert(n_size == event_listner.size());
  assert(n_size == output_files.size());
  assert(n_size == job_stats.size());
  assert(n_size == num_output_records.size());
  return n_size;
}

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

class LocalCompactionExecutor : public CompactionExecutor {
 public:
  Status Execute(const CompactionParams&, CompactionResults*) override;
};

Status LocalCompactionExecutor::Execute(const CompactionParams& params,
                                        CompactionResults* results)
{
}

class LocalCompactionExecutorFactory : public CompactionExecutorFactory {
 public:
  CompactionExecutor* NewExecutor(const Compaction*) const override {

  }
};

} // namespace ROCKSDB_NAMESPACE
