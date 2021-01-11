//
// Created by leipeng on 2021/1/11.
//
#pragma once
#include "compaction_job.h"

namespace ROCKSDB_NAMESPACE {

class CompactionExecutor {
 public:
  virtual ~CompactionExecutor();
  virtual void Execute(CompactionJob*) = 0;
};

class CompactionExecutorFactory {
 public:
  virtual ~CompactionExecutorFactory();
  virtual CompactionExecutor* NewExecutor(const Compaction*) const = 0;
};

CompactionExecutorFactory* GetLocalCompactionExecutorFactory();

} // namespace ROCKSDB_NAMESPACE
