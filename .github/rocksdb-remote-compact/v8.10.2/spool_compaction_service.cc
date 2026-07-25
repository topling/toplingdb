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

bool ReadState(const fs::path& job_dir, std::string* state) {
  return ReadFile(job_dir / "state", state);
}

}  // namespace

SpoolCompactionService::SpoolCompactionService(std::string spool_dir)
    : spool_dir_(std::move(spool_dir)) {}

std::string SpoolCompactionService::JobDir(
    const CompactionServiceJobInfo& info) const {
  return (fs::path(spool_dir_) / info.db_id / std::to_string(info.job_id))
      .string();
}

CompactionServiceJobStatus SpoolCompactionService::StartV2(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_service_input) {
  try {
    const fs::path job_dir(JobDir(info));
    std::error_code error;
    if (!fs::create_directories(job_dir, error) && error) {
      return CompactionServiceJobStatus::kFailure;
    }
    if (!fs::create_directories(job_dir / "output", error) && error) {
      return CompactionServiceJobStatus::kFailure;
    }

    // db_name is broker metadata, not part of the OpenAndCompact wire input.
    // It lets an out-of-cgroup worker open the DB that issued this request.
    if (!WriteFileAtomically(job_dir / "db_name", info.db_name) ||
        !WriteFileAtomically(job_dir / "input.bin", compaction_service_input) ||
        !WriteFileAtomically(job_dir / "state", "PENDING")) {
      return CompactionServiceJobStatus::kFailure;
    }
    return CompactionServiceJobStatus::kSuccess;
  } catch (...) {
    return CompactionServiceJobStatus::kFailure;
  }
}

CompactionServiceJobStatus SpoolCompactionService::WaitForCompleteV2(
    const CompactionServiceJobInfo& info,
    std::string* compaction_service_result) {
  try {
    const fs::path job_dir(JobDir(info));
    for (;;) {
      std::string state;
      if (!ReadState(job_dir, &state)) {
        return CompactionServiceJobStatus::kFailure;
      }
      if (state == "DONE") {
        return ReadFile(job_dir / "result.bin", compaction_service_result)
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
