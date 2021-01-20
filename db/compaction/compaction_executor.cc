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
    delete version_set;
    delete existing_snapshots;
    delete compaction_job_stats;
  }
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
