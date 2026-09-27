//  Copyright (c) 2026-present, Topling Inc.
//  Abnormal-exit recovery benchmark.
//  Same RocksDB API. Topling options come from TOPLINGDB_EASY_MIGRATE_CONF.
//
//  Fill one memtable, _exit without Close, copy that directory aside, then
//  time DB::Open on a fresh copy of the backup. Repeat and print the average.

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace fs = std::filesystem;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteOptions;

namespace {

constexpr size_t kWriteBufferBytes = 2ull << 30;
constexpr int kFillExit = 42;

struct Args {
  std::string root = "/tmp/crash_recover_bench";
  int runs = 5;
  int key_size = 16;
  int value_size = 128;
  // Stop filling once the active memtable reaches this fraction of 2GB,
  // so the engine does not flush it before the abnormal exit.
  double fill_frac = 0.75;
  bool reuse_backup = false;
};

void Die(const std::string& msg) {
  fprintf(stderr, "crash_recover_bench: %s\n", msg.c_str());
  std::_Exit(1);
}

Args ParseArgs(int argc, char** argv) {
  Args a;
  for (int i = 1; i < argc; i++) {
    std::string s = argv[i];
    auto need = [&](const char* name) -> std::string {
      auto eq = s.find('=');
      if (eq == std::string::npos || s.substr(0, eq) != name) {
        return "";
      }
      return s.substr(eq + 1);
    };
    if (auto v = need("--root"); !v.empty()) {
      a.root = v;
    } else if (auto v = need("--runs"); !v.empty()) {
      a.runs = std::atoi(v.c_str());
    } else if (auto v = need("--key_size"); !v.empty()) {
      a.key_size = std::atoi(v.c_str());
    } else if (auto v = need("--value_size"); !v.empty()) {
      a.value_size = std::atoi(v.c_str());
    } else if (auto v = need("--fill_frac"); !v.empty()) {
      a.fill_frac = std::atof(v.c_str());
    } else if (s == "--reuse_backup") {
      a.reuse_backup = true;
    } else if (s == "--help") {
      fprintf(stderr,
              "Usage: crash_recover_bench [--root=DIR] [--runs=N]\n"
              "       [--key_size=N] [--value_size=N] [--fill_frac=0.75]\n"
              "       [--reuse_backup]\n"
              "ToplingDB: TOPLINGDB_EASY_MIGRATE_CONF=tools/crash_recover_bench.yaml\n"
              "RocksDB:   leave that variable unset\n"
              "write_buffer_size is 2GB either way.\n");
      std::exit(0);
    } else {
      Die("unknown arg " + s);
    }
  }
  if (a.runs < 1 || a.key_size < 1 || a.value_size < 1 || a.fill_frac <= 0 ||
      a.fill_frac >= 1) {
    Die("bad --runs/--key_size/--value_size/--fill_frac");
  }
  return a;
}

Options MakeOptions() {
  Options opt;
  opt.create_if_missing = true;
  opt.write_buffer_size = kWriteBufferBytes;
  opt.max_write_buffer_number = 4;
  opt.disable_auto_compactions = true;
  opt.level0_file_num_compaction_trigger = 1 << 20;
  opt.avoid_flush_during_recovery = true;
  return opt;
}

std::string KeyAt(int key_size, uint64_t i) {
  std::string k(static_cast<size_t>(key_size), '0');
  for (int p = key_size - 1; p >= 0 && i > 0; --p) {
    k[static_cast<size_t>(p)] = static_cast<char>('0' + (i % 10));
    i /= 10;
  }
  return k;
}

uint64_t ActiveMemBytes(DB* db) {
  std::string v;
  if (!db->GetProperty("rocksdb.cur-size-active-mem-table", &v)) {
    return 0;
  }
  return std::strtoull(v.c_str(), nullptr, 10);
}

// Child only. Writes until the live memtable reaches the target, then _exit
// without Close so the directory is a process-crash image.
void FillAndAbort(const Args& args, const fs::path& dbpath,
                 const fs::path& nkeys_path) {
  fs::remove_all(dbpath);
  fs::create_directories(dbpath);
  DB* db = nullptr;
  Status s = DB::Open(MakeOptions(), dbpath.string(), &db);
  if (!s.ok()) {
    Die("fill open: " + s.ToString());
  }
  const uint64_t target =
      static_cast<uint64_t>(kWriteBufferBytes * args.fill_frac);
  WriteOptions wo;
  wo.disableWAL = false;
  std::string value(static_cast<size_t>(args.value_size), 'v');
  uint64_t n = 0;
  uint64_t mem = 0;
  while (mem < target) {
    std::string key = KeyAt(args.key_size, n);
    s = db->Put(wo, key, value);
    if (!s.ok()) {
      Die("put: " + s.ToString());
    }
    n++;
    if ((n & 1023) == 0) {
      mem = ActiveMemBytes(db);
    }
  }
  fprintf(stderr, "filled keys=%llu active_mem=%llu target=%llu\n",
          static_cast<unsigned long long>(n),
          static_cast<unsigned long long>(mem),
          static_cast<unsigned long long>(target));
  FILE* nf = fopen(nkeys_path.c_str(), "w");
  if (nf == nullptr || fprintf(nf, "%llu\n", static_cast<unsigned long long>(n)) < 0) {
    Die("write nkeys");
  }
  fclose(nf);
  // Leave the DB open. _exit skips destructors, same as kill -9.
  std::_Exit(kFillExit);
}

void CopyTree(const fs::path& src, const fs::path& dst) {
  std::error_code ec;
  fs::remove_all(dst, ec);
  fs::create_directories(dst.parent_path(), ec);
  if (ec) {
    Die("mkdir " + dst.parent_path().string() + ": " + ec.message());
  }
  // Keep holes. A plain copy fills the memtab tail and makes ftruncate drop
  // real pages.
  const pid_t child = ::fork();
  if (child < 0) {
    Die("fork cp: " + std::string(std::strerror(errno)));
  }
  if (child == 0) {
    ::execlp("cp", "cp", "-a", "--sparse=always", "--", src.c_str(),
             dst.c_str(), static_cast<char*>(nullptr));
    std::_Exit(127);
  }
  int status = 0;
  pid_t waited;
  do {
    waited = ::waitpid(child, &status, 0);
  } while (waited < 0 && errno == EINTR);
  if (waited < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    Die("cp --sparse=always " + src.string() + " -> " + dst.string());
  }
}

double OpenMillis(const fs::path& dbpath, const Args& args, uint64_t nkeys) {
  DB* db = nullptr;
  auto t0 = std::chrono::steady_clock::now();
  Status s = DB::Open(MakeOptions(), dbpath.string(), &db);
  auto t1 = std::chrono::steady_clock::now();
  if (!s.ok()) {
    Die("recover open: " + s.ToString());
  }
  std::string got;
  s = db->Get(ReadOptions(), KeyAt(args.key_size, 0), &got);
  if (!s.ok() || got.size() != static_cast<size_t>(args.value_size)) {
    Die("spot check key 0: " + s.ToString());
  }
  if (nkeys > 1) {
    s = db->Get(ReadOptions(), KeyAt(args.key_size, nkeys - 1), &got);
    if (!s.ok()) {
      Die("spot check last key: " + s.ToString());
    }
  }
  s = db->Close();
  delete db;
  if (!s.ok()) {
    Die("close: " + s.ToString());
  }
  return std::chrono::duration<double, std::milli>(t1 - t0).count();
}

uint64_t ReadNKeys(const fs::path& path) {
  FILE* nf = fopen(path.c_str(), "r");
  if (nf == nullptr) {
    Die("open " + path.string());
  }
  unsigned long long n = 0;
  if (fscanf(nf, "%llu", &n) != 1 || n == 0) {
    fclose(nf);
    Die("bad nkeys file");
  }
  fclose(nf);
  return n;
}

}  // namespace

int main(int argc, char** argv) {
  setenv("ROCKSDB_KICK_OUT_OPTIONS_FILE", "1", 1);
  Args args = ParseArgs(argc, argv);
  const fs::path root = args.root;
  const fs::path crashed = root / "crashed";
  const fs::path backup = root / "backup";
  const fs::path nkeys_path = root / "nkeys.txt";
  fs::create_directories(root);

  const bool have_backup = args.reuse_backup && fs::exists(backup / "CURRENT");
  if (!have_backup) {
    const pid_t pid = ::fork();
    if (pid < 0) {
      Die("fork");
    }
    if (pid == 0) {
      FillAndAbort(args, crashed, nkeys_path);
    }
    int st = 0;
    if (::waitpid(pid, &st, 0) != pid || !WIFEXITED(st) ||
        WEXITSTATUS(st) != kFillExit) {
      Die("fill child failed");
    }
    CopyTree(crashed, backup);
    fprintf(stderr, "backup %s\n", backup.c_str());
  }

  const uint64_t nkeys = ReadNKeys(nkeys_path);
  fprintf(stderr, "keys=%llu runs=%d write_buffer=%zuMB\n",
          static_cast<unsigned long long>(nkeys), args.runs,
          kWriteBufferBytes >> 20);

  double sum = 0;
  for (int i = 0; i < args.runs; i++) {
    const fs::path run = root / ("run-" + std::to_string(i));
    CopyTree(backup, run);
    const double ms = OpenMillis(run, args, nkeys);
    sum += ms;
    printf("run %d open_ms %.3f\n", i, ms);
    fflush(stdout);
    fs::remove_all(run);
  }
  printf("avg_open_ms %.3f runs %d\n", sum / args.runs, args.runs);
  return 0;
}
