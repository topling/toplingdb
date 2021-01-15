//
// Created by leipeng on 2021/1/11.
//

#include "compaction_executor.h"
#include "terark/io/DataIO.hpp"

namespace ROCKSDB_NAMESPACE {

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
