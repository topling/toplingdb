//
// Created by leipeng on 2021/1/11.
//

#include "compaction_executor.h"

namespace ROCKSDB_NAMESPACE {

CompactionExecutor::~CompactionExecutor() = default;
CompactionExecutorFactory::~CompactionExecutorFactory() = default;

class LocalCompactionExecutor : public CompactionExecutor {
 public:
  void Execute(CompactionJob* job) override {
  const size_t num_threads = job->NumSubCompacts();
  assert(num_threads > 0);
  // Launch a thread for each of subcompactions 1...num_threads-1
  std::vector<port::Thread> thread_pool;
  thread_pool.reserve(num_threads - 1);
  for (size_t i = 1; i < num_threads; i++) {
    thread_pool.emplace_back(&CompactionJob::ProcessKeyValueCompaction, this,
                             &compact_->sub_compact_states[i]);
  }

  // Always schedule the first subcompaction (whether or not there are also
  // others) in the current thread to be efficient with resources
  ProcessKeyValueCompaction(&compact_->sub_compact_states[0]);

  // Wait for all other threads (if there are any) to finish execution
  for (auto& thread : thread_pool) {
    thread.join();
  }

  }
};

class LocalCompactionExecutorFactory : public CompactionExecutorFactory {
 public:
  CompactionExecutor* NewExecutor(const Compaction*) const override {

  }
};

} // namespace ROCKSDB_NAMESPACE
