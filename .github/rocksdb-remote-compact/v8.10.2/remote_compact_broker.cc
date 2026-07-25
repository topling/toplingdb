#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

namespace fs = std::filesystem;

bool ReadFile(const fs::path& path, std::string* value) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    return false;
  }
  std::getline(file, *value, '\0');
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

void StartPendingJob(const fs::path& job_dir, const std::string& worker) {
  std::string state;
  if (!ReadFile(job_dir / "state", &state) || state != "PENDING") {
    return;
  }
  if (!WriteFileAtomically(job_dir / "state", "RUNNING")) {
    return;
  }

  const pid_t pid = fork();
  if (pid == 0) {
    execl(worker.c_str(), worker.c_str(), job_dir.c_str(), nullptr);
    WriteFileAtomically(job_dir / "error.txt", "cannot exec worker");
    WriteFileAtomically(job_dir / "state", "FAILED");
    _exit(127);
  }
  if (pid < 0) {
    WriteFileAtomically(job_dir / "error.txt", "cannot fork worker");
    WriteFileAtomically(job_dir / "state", "FAILED");
  }
}

void Scan(const fs::path& spool_dir, const std::string& worker) {
  std::error_code error;
  for (const fs::directory_entry& db_dir :
       fs::directory_iterator(spool_dir, error)) {
    if (error || !db_dir.is_directory()) {
      continue;
    }
    for (const fs::directory_entry& job_dir :
         fs::directory_iterator(db_dir.path(), error)) {
      if (error) {
        break;
      }
      if (job_dir.is_directory()) {
        StartPendingJob(job_dir.path(), worker);
      }
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 3 || argc > 4) {
    std::cerr << "Usage: remote_compact_broker <spool-dir> <worker> [poll-ms]\n";
    return 2;
  }
  const fs::path spool_dir(argv[1]);
  const std::string worker(argv[2]);
  const int poll_ms = argc == 4 ? std::stoi(argv[3]) : 100;
  std::error_code error;
  fs::create_directories(spool_dir, error);
  if (error) {
    std::cerr << "cannot create spool directory: " << error.message() << '\n';
    return 1;
  }

  for (;;) {
    Scan(spool_dir, worker);
    while (waitpid(-1, nullptr, WNOHANG) > 0) {
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
  }
}
