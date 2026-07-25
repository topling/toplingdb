#pragma once

#include <memory>
#include <string>

#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

class SpoolCompactionService : public CompactionService {
 public:
  explicit SpoolCompactionService(std::string spool_dir);

  const char* Name() const override { return "SpoolCompactionService"; }

  CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& info,
      const std::string& compaction_service_input) override;
  CompactionServiceJobStatus Wait(const std::string& scheduled_job_id,
                                  std::string* result) override;

 private:
  std::string JobDir(const std::string& scheduled_job_id) const;

  const std::string spool_dir_;
};

}  // namespace ROCKSDB_NAMESPACE
