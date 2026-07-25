#include "tools/remote_compact/spool_compaction_service.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

namespace ROCKSDB_NAMESPACE {
namespace {

namespace fs = std::filesystem;

bool WriteFileAtomically(const fs::path& path, const std::string& value) {
  const fs::path temporary = path.string() + ".tmp";
  std::ofstream file(temporary, std::ios::binary | std::ios::trunc);
  if (!file.write(value.data(), static_cast<std::streamsize>(value.size()))) {
    return false;
  }
  file.close();
  std::error_code error;
  fs::rename(temporary, path, error);
  return !error;
}

bool ReadFile(const fs::path& path, std::string* value) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    return false;
  }
  value->assign(std::istreambuf_iterator<char>(file),
                std::istreambuf_iterator<char>());
  return !file.bad();
}

}  // namespace

SpoolCompactionService::SpoolCompactionService(std::string spool_dir)
    : spool_dir_(std::move(spool_dir)) {}

std::string SpoolCompactionService::JobDir(
    const std::string& scheduled_job_id) const {
  const std::size_t separator = scheduled_job_id.find('/');
  if (separator == std::string::npos) {
    return {};
  }
  return (fs::path(spool_dir_) / scheduled_job_id.substr(0, separator) /
          scheduled_job_id.substr(separator + 1))
      .string();
}

CompactionServiceScheduleResponse SpoolCompactionService::Schedule(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_service_input) {
  try {
    const std::string job_id =
        info.db_id + "/" + info.db_session_id + "-" + std::to_string(info.job_id);
    const fs::path job_dir(JobDir(job_id));
    std::error_code error;
    if (!fs::create_directories(job_dir / "output", error) && error) {
      return CompactionServiceScheduleResponse(
          CompactionServiceJobStatus::kFailure);
    }
    if (!WriteFileAtomically(job_dir / "db_name", info.db_name) ||
        !WriteFileAtomically(job_dir / "input.bin", compaction_service_input) ||
        !WriteFileAtomically(job_dir / "state", "PENDING")) {
      return CompactionServiceScheduleResponse(
          CompactionServiceJobStatus::kFailure);
    }
    return CompactionServiceScheduleResponse(job_id,
                                             CompactionServiceJobStatus::kSuccess);
  } catch (...) {
    return CompactionServiceScheduleResponse(CompactionServiceJobStatus::kFailure);
  }
}

CompactionServiceJobStatus SpoolCompactionService::Wait(
    const std::string& scheduled_job_id, std::string* result) {
  try {
    const fs::path job_dir(JobDir(scheduled_job_id));
    if (job_dir.empty()) {
      return CompactionServiceJobStatus::kFailure;
    }
    for (;;) {
      std::string state;
      if (!ReadFile(job_dir / "state", &state)) {
        return CompactionServiceJobStatus::kFailure;
      }
      if (state == "DONE") {
        return ReadFile(job_dir / "result.bin", result)
                   ? CompactionServiceJobStatus::kSuccess
                   : CompactionServiceJobStatus::kFailure;
      }
      if (state == "FAILED") {
        return CompactionServiceJobStatus::kFailure;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  } catch (...) {
    return CompactionServiceJobStatus::kFailure;
  }
}

}  // namespace ROCKSDB_NAMESPACE
