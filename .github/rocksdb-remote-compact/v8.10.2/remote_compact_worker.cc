#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace {

namespace fs = std::filesystem;

bool ReadFile(const fs::path& path, std::string* value) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    return false;
  }
  value->assign(std::istreambuf_iterator<char>(file),
                std::istreambuf_iterator<char>());
  return !file.bad();
}

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

rocksdb::CompactionServiceOptionsOverride MakeOptionsOverride() {
  rocksdb::CompactionServiceOptionsOverride override;
  // db_bench's default options use this exact table factory and comparator.
  // Non-default pointer options need a worker built for the same application.
  override.comparator = rocksdb::BytewiseComparator();
  override.table_factory.reset(rocksdb::NewBlockBasedTableFactory());
  return override;
}

int Fail(const fs::path& job_dir, const std::string& message) {
  WriteFileAtomically(job_dir / "error.txt", message);
  WriteFileAtomically(job_dir / "state", "FAILED");
  return 1;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: remote_compact_worker <job-dir>\n";
    return 2;
  }

  const fs::path job_dir(argv[1]);
  std::string db_name;
  std::string input;
  if (!ReadFile(job_dir / "db_name", &db_name) ||
      !ReadFile(job_dir / "input.bin", &input)) {
    return Fail(job_dir, "missing db_name or input.bin");
  }

  std::string result;
  rocksdb::OpenAndCompactOptions options;
  const rocksdb::Status status = rocksdb::DB::OpenAndCompact(
      options, db_name, (job_dir / "output").string(), input, &result,
      MakeOptionsOverride());
  if (!status.ok()) {
    return Fail(job_dir, status.ToString());
  }
  if (!WriteFileAtomically(job_dir / "result.bin", result) ||
      !WriteFileAtomically(job_dir / "state", "DONE")) {
    return Fail(job_dir, "cannot publish result");
  }
  return 0;
}
