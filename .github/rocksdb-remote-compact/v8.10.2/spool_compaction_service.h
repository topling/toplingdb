#pragma once

#include <memory>
#include <string>

#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

// Persists remote-compaction requests for a broker already running outside the
// db_bench cgroup. The broker owns process creation; this class never forks.
class SpoolCompactionService : public CompactionService {
 public:
  explicit SpoolCompactionService(std::string spool_dir);

  const char* Name() const override { return "SpoolCompactionService"; }

  CompactionServiceJobStatus StartV2(
      const CompactionServiceJobInfo& info,
      const std::string& compaction_service_input) override;
  CompactionServiceJobStatus WaitForCompleteV2(
      const CompactionServiceJobInfo& info,
      std::string* compaction_service_result) override;

 private:
  std::string JobDir(const CompactionServiceJobInfo& info) const;

  const std::string spool_dir_;
};

}  // namespace ROCKSDB_NAMESPACE
