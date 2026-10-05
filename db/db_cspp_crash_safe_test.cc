//  Copyright (c) 2026-present, Topling Inc.
//  Crash-safe leftover recover: Convert + WAL tail, sync-point injection.

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <typeinfo>
#include <vector>

#include <topling/side_plugin_factory.h>

#include <terark/fsa/cspptrie.inl>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/offset_skiplist.hpp>
#include <terark/util/crc.hpp>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/pre_release_callback.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/file_util.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/io_status.h"
#include "rocksdb/convenience.h"
#include "rocksdb/statistics.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/wal_filter.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/sst_file_dumper.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "table/top_table_reader.h"
#include "utilities/merge_operators.h"
#include "utilities/fault_injection_fs.h"
#include "test_util/sync_point.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

extern MemTableRepFactory* NewCSPPMemTabForPlain(const std::string&);
std::shared_ptr<MemTableRepFactory> EasyNewMemTableRep(Slice cls, Slice js);

namespace {

struct PublishedSeqOnDisk {
  uint64_t magic = 0;
  uint32_t version = 0;
  uint32_t header_size = 0;
  uint32_t wal_offset_kind = 0;
  uint32_t kind_since_wal = 0;
  uint64_t generation = 0;
  uint64_t pubseq = 0;
  uint64_t wal_number = 0;
  uint64_t wal_offset = 0;
  uint64_t padding = 0;
};

void SetupCspp(Options* options, bool file_mmap) {
  const char* js = file_mmap
                       ? R"({"mem_cap":16777216,"convert_to_sst":"kFileMmap"})"
                       : R"({"mem_cap":16777216,"convert_to_sst":"kDontConvert"})";
  options->memtable_factory.reset(NewCSPPMemTabForPlain(js));
  const SidePluginRepo repo;
  options->table_factory = PluginFactorySP<TableFactory>::AcquirePlugin(
      "CSPPMemTabTable", json::parse(js), repo);
}

void SetupOsl(Options* options, bool file_mmap) {
  const char* js = file_mmap
                       ? R"({"mem_cap":16777216,"convert_to_sst":"kFileMmap"})"
                       : R"({"mem_cap":16777216,"convert_to_sst":"kDontConvert"})";
  options->memtable_factory = EasyNewMemTableRep("OffsetSkipList", js);
  const SidePluginRepo repo;
  options->table_factory = PluginFactorySP<TableFactory>::AcquirePlugin(
      "OffsetSkipListTable", json::parse(js), repo);
}

Options BaseCrashSafeOptions(const std::string& dbname, bool recover,
                             bool log_index) {
  Options options;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.memtable_crash_safe_recover = recover;
  options.memtable_as_log_index = log_index;
  options.avoid_flush_during_shutdown = false;
  options.avoid_flush_during_recovery = true;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 64 << 20;
  options.max_write_buffer_number = 8;
  options.level0_file_num_compaction_trigger = 1 << 20;
  options.env = Env::Default();
  options.wal_dir = dbname;
  SetupCspp(&options, true);
  return options;
}

std::vector<std::string> ListLeftovers(const Options& options,
                                       const std::string& dir) {
  std::vector<std::string> leftovers;
  Env* env = options.env ? options.env : Env::Default();
  std::string current;
  Status s = env->FileExists(CurrentFileName(dir));
  if (s.IsNotFound()) return leftovers;
  EXPECT_OK(s);
  if (!s.ok()) return leftovers;
  s = ReadFileToString(env, CurrentFileName(dir), &current);
  if (!s.ok()) {
    ADD_FAILURE() << s.ToString();
    return leftovers;
  }
  EXPECT_FALSE(current.empty());
  if (current.empty()) return leftovers;
  if (current.back() == '\n') current.pop_back();
  const std::string manifest = dir + "/" + current;
  std::unique_ptr<FSSequentialFile> file;
  s = env->GetFileSystem()->NewSequentialFile(manifest, FileOptions(), &file,
                                             nullptr);
  EXPECT_OK(s);
  if (!s.ok()) return leftovers;
  struct Reporter : log::Reader::Reporter {
    void Corruption(size_t, const Status& status) override {
      ADD_FAILURE() << status.ToString();
    }
  } reporter;
  auto input = std::make_unique<SequentialFileReader>(std::move(file), manifest);
  log::Reader reader(nullptr, std::move(input), &reporter, true, 0);
  std::map<uint32_t, std::set<uint64_t>> registered;
  auto apply = [&](const VersionEdit& edit) {
    const uint32_t cf = edit.GetColumnFamily();
    if (edit.IsColumnFamilyDrop()) {
      registered.erase(cf);
      return;
    }
    for (uint64_t number : edit.GetMemTableFileDeletions()) {
      registered[cf].erase(number);
    }
    for (uint64_t number : edit.GetMemTableFileAdditions()) {
      registered[cf].insert(number);
    }
  };
  AtomicGroupReadBuffer group;
  Slice record;
  std::string scratch;
  while (reader.ReadRecord(&record, &scratch)) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    EXPECT_OK(s);
    if (!s.ok()) break;
    s = group.AddEdit(&edit);
    EXPECT_OK(s);
    if (!s.ok()) break;
    if (!edit.IsInAtomicGroup()) {
      apply(edit);
    } else if (group.IsFull()) {
      for (const auto& member : group.replay_buffer()) apply(member);
      group.Clear();
    }
  }
  const std::string path = !options.cf_paths.empty()
                               ? options.cf_paths[0].path
                               : !options.db_paths.empty()
                                     ? options.db_paths[0].path
                                     : dir;
  for (const auto& cf : registered) {
    for (uint64_t number : cf.second) {
      leftovers.push_back(MakeTableFileName(path, number));
    }
  }
  return leftovers;
}

bool ReadPublishedSeqFile(const std::string& dbname, PublishedSeqOnDisk* out) {
  const std::string path = CrashSafePubSeqFileName(dbname);
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return false;
  }
  const ssize_t n = ::pread(fd, out, sizeof(*out), 0);
  ::close(fd);
  return n == static_cast<ssize_t>(sizeof(*out));
}

bool SetPublishedSeqGeneration(const std::string& dbname, uint64_t g) {
  const std::string path = CrashSafePubSeqFileName(dbname);
  int fd = ::open(path.c_str(), O_WRONLY);
  if (fd < 0) {
    return false;
  }
  const ssize_t n =
      ::pwrite(fd, &g, sizeof(g), offsetof(PublishedSeqOnDisk, generation));
  ::close(fd);
  return n == static_cast<ssize_t>(sizeof(g));
}

bool SetPublishedSeqWalOffsetKind(const std::string& dbname, uint32_t kind) {
  const std::string path = CrashSafePubSeqFileName(dbname);
  int fd = ::open(path.c_str(), O_WRONLY);
  if (fd < 0) {
    return false;
  }
  const ssize_t n =
      ::pwrite(fd, &kind, sizeof(kind),
               offsetof(PublishedSeqOnDisk, wal_offset_kind));
  ::close(fd);
  return n == static_cast<ssize_t>(sizeof(kind));
}

bool ZeroPublishedSeqFile(const std::string& dbname) {
  const std::string path = CrashSafePubSeqFileName(dbname);
  const std::string zeros(4096, '\0');
  int fd = ::open(path.c_str(), O_WRONLY);
  if (fd < 0) {
    return false;
  }
  const ssize_t n = ::pwrite(fd, zeros.data(), zeros.size(), 0);
  ::close(fd);
  return n == static_cast<ssize_t>(zeros.size());
}

int CountL0(DB* db, const std::string& cf_name = "") {
  std::vector<LiveFileMetaData> files;
  db->GetLiveFilesMetaData(&files);
  int n = 0;
  for (const auto& f : files) {
    if (f.level == 0 && (cf_name.empty() || f.column_family_name == cf_name)) {
      n++;
    }
  }
  return n;
}

#if !defined(OS_WIN)
// Only exec/_exit run between fork and exec; DB and SyncPoint state are
// initialized in the new process, including Env's background worker threads.
int RunCrashChild(const std::string& dbname, const char* scenario,
                  const std::string& arg = "") {
  char exe[] = "/proc/self/exe";
  char flag[] = "--crash-child";
  char* argv[] = {exe, flag, const_cast<char*>(scenario),
                  const_cast<char*>(dbname.c_str()),
                  const_cast<char*>(arg.c_str()), nullptr};
  const pid_t pid = ::fork();
  if (pid == 0) {
    ::execv(exe, argv);
    ::_exit(127);
  }
  if (pid < 0) {
    return -1;
  }
  int st = 0;
  pid_t waited;
  do {
    waited = ::waitpid(pid, &st, 0);
  } while (waited < 0 && errno == EINTR);
  return waited == pid && WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

const char* crash_child_db = nullptr;
const char* crash_child_arg = nullptr;

class CrashChild : public ::testing::Test {
 protected:
  const std::string dbname_ = crash_child_db ? crash_child_db : "";
  const std::string arg_ = crash_child_arg ? crash_child_arg : "";
};

const int kKindPrepChildCrashed = 42;

TEST_F(CrashChild, DISABLED_KindPrep) {
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  log_index.avoid_flush_during_shutdown = true;
  SyncPoint::GetInstance()->SetCallBack(
      arg_, [](void*) { ::_exit(kKindPrepChildCrashed); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* db = nullptr;
  Status s = DB::Open(log_index, dbname_, &db);
  ::_exit(s.ok() ? 1 : 2);
}
#endif

}  // namespace

class DBCsppCrashSafeTest : public DBTestBase {
 public:
  DBCsppCrashSafeTest()
      : DBTestBase("db_cspp_crash_safe_test", /*env_do_fsync=*/false) {}
};

#if !defined(OS_WIN)
TEST_F(CrashChild, DISABLED_RegisteredMemTables) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  if (arg_ == "osl") SetupOsl(&options, true);
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "first", "1"));
  ASSERT_OK(static_cast<DBImpl*>(child_db)->TEST_SwitchMemtable());
  ASSERT_OK(child_db->Put(WriteOptions(), "second", "2"));
  ASSERT_OK(static_cast<DBImpl*>(child_db)->TEST_SwitchMemtable());
  // The registered empty active file must also exist for prefix recovery.
  ::_exit(42);
}

TEST_F(DBCsppCrashSafeTest, MissingRegisteredMemTableUsesFullWal) {
  Close();
  for (bool osl : {false, true}) {
    for (int missing : {0, 1, 2, 3}) {
      SCOPED_TRACE(osl);
      SCOPED_TRACE(missing);
      Options options = BaseCrashSafeOptions(dbname_, true, false);
      if (osl) SetupOsl(&options, true);
      Destroy(options);
      ASSERT_EQ(RunCrashChild(dbname_, "RegisteredMemTables",
                              osl ? "osl" : "cspp"), 42);
      const auto registered = ListLeftovers(options, dbname_);
      ASSERT_EQ(registered.size(), 3U);
      if (missing == 3) {
        for (const auto& path : registered) ASSERT_OK(env_->DeleteFile(path));
      } else {
        ASSERT_OK(env_->DeleteFile(registered[missing]));
      }
      std::atomic<int> converted{0};
      SyncPoint::GetInstance()->SetCallBack(
          "MemTableRep::ConvertToSST:After",
          [&](void*) { ++converted; });
      SyncPoint::GetInstance()->EnableProcessing();
      ASSERT_OK(TryReopen(options));
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      // Recovery may convert an intact prefix before discovering the missing
      // registered file, but must discard that prefix and replay the full WAL.
      ASSERT_EQ(converted.load(), missing == 3 ? 0 : missing);
      ASSERT_EQ(Get("first"), "1");
      ASSERT_EQ(Get("second"), "2");
      ASSERT_EQ(NumTableFilesAtLevel(0), 0);
      ASSERT_OK(Flush());
      ASSERT_EQ(NumTableFilesAtLevel(0), 1);
      Close();
      ASSERT_OK(TryReopen(options));
      ASSERT_EQ(Get("first"), "1");
      ASSERT_EQ(Get("second"), "2");
      ASSERT_EQ(NumTableFilesAtLevel(0), 1);
      Close();
    }
  }
}

TEST_F(DBCsppCrashSafeTest, FailedRegistrationCannotAcceptWrites) {
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("before", "safe"));
    ASSERT_OK(dbfull()->TEST_SwitchMemtable());
    const auto caller = std::this_thread::get_id();
    std::atomic<int> failed{0};
    std::atomic<bool> registering_cache{false};
    SyncPoint::GetInstance()->SetCallBack(
        "FlushJob::BeforeManifest", [&](void*) { registering_cache.store(true); });
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* p) {
          if (!registering_cache.load()) return;
          EXPECT_NE(std::this_thread::get_id(), caller);
          ++failed;
          *static_cast<IOStatus*>(p) = IOStatus::IOError("register injection");
        });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_NOK(Flush());
    ASSERT_GT(failed.load(), 0);
    auto* pending = dbfull()->GetVersionSet()->GetColumnFamilySet()
                        ->GetDefault()->PeekPrecreatedMemtable();
    ASSERT_NE(pending, nullptr);
    ASSERT_FALSE(pending->IsFileRegistered());
    ASSERT_NOK(dbfull()->TEST_SwitchMemtable());
    ASSERT_NOK(Put("unregistered", "must-not-commit"));
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_EQ(Get("before"), "safe");
    Close();
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("before"), "safe");
    ASSERT_EQ(Get("unregistered"), "NOT_FOUND");
    Close();
  }
}

TEST_F(DBCsppCrashSafeTest, FailedInitialRegistrationClearsDbPointer) {
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    std::atomic<int> failed{0};
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::RegisterMemTableFile:AfterLogAndApply", [&](void* p) {
          ++failed;
          *static_cast<Status*>(p) = Status::IOError("initial register injection");
        });
    SyncPoint::GetInstance()->EnableProcessing();
    DB* opened = nullptr;
    const Status status = DB::Open(options, dbname_, &opened);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_NOK(status);
    ASSERT_GT(failed.load(), 0);
    ASSERT_EQ(opened, nullptr);
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("after-failure", "safe"));
    Close();
  }
}

TEST_F(CrashChild, DISABLED_RegistrationCommitWindow) {
  ASSERT_EQ(arg_.size(), 3U);
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  if (arg_[0] == '1') SetupOsl(&options, true);
  const char* point = arg_[2] == '0'
      ? (arg_[1] == '0' ? "DBImpl::RegisterMemTableFile:AfterLogAndApply"
                        : "DBImpl::RegisterMemTableFile:BeforeInstall")
      : (arg_[1] == '0' ? "FlushJob::MemTableCache:BeforePublish"
                        : "FlushJob::MemTableCache:AfterPublish");
  const auto arm = [&] {
    SyncPoint::GetInstance()->SetCallBack(point, [](void*) { ::_exit(42); });
    SyncPoint::GetInstance()->EnableProcessing();
  };
  if (arg_[2] == '0') arm();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "before-switch", "preserved"));
  if (arg_[2] == '1') arm();
  ASSERT_OK(child_db->Flush(FlushOptions()));
  ::_exit(1);
}

TEST_F(DBCsppCrashSafeTest, RegistrationCommitCrashKeepsManifestInventory) {
  Close();
  for (bool osl : {false, true}) {
    for (bool marked : {false, true}) {
      for (bool switching : {false, true}) {
        SCOPED_TRACE(osl);
        SCOPED_TRACE(marked);
        SCOPED_TRACE(switching);
        Options options = BaseCrashSafeOptions(dbname_, true, false);
        if (osl) SetupOsl(&options, true);
        Destroy(options);
        const std::string arg = std::to_string(osl) + std::to_string(marked) +
                                std::to_string(switching);
        ASSERT_EQ(RunCrashChild(dbname_, "RegistrationCommitWindow", arg), 42);
        const auto registered = ListLeftovers(options, dbname_);
        ASSERT_EQ(registered.size(), 2U);
        for (const auto& path : registered) ASSERT_OK(env_->FileExists(path));
        ASSERT_OK(TryReopen(options));
        ASSERT_TRUE(dbfull()->GetVersionSet()->HasMemTableFileTracking());
        ASSERT_EQ(Get("before-switch"), switching ? "preserved" : "NOT_FOUND");
        ASSERT_OK(Put("after-crash", "committed"));
        Close();
        for (const auto& path : ListLeftovers(options, dbname_))
          ASSERT_OK(env_->FileExists(path));
        ASSERT_OK(TryReopen(options));
        ASSERT_EQ(Get("before-switch"), switching ? "preserved" : "NOT_FOUND");
        ASSERT_EQ(Get("after-crash"), "committed");
        Close();
      }
    }
  }
}

TEST_F(DBCsppCrashSafeTest, FailedNewColumnFamilyRegistrationRemainsReopenable) {
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("existing", "preserved"));
    std::atomic<int> failed{0};
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::RegisterMemTableFile:AfterLogAndApply", [&](void* p) {
          ++failed;
          *static_cast<Status*>(p) = Status::IOError("new CF register injection");
        });
    SyncPoint::GetInstance()->EnableProcessing();
    ColumnFamilyHandle* handle = nullptr;
    const Status created = db_->CreateColumnFamily(options, "failed", &handle);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_NOK(created);
    ASSERT_GT(failed.load(), 0);
    ASSERT_EQ(handle, nullptr);
    ASSERT_EQ(Get("existing"), "preserved");
    // Closing and reopening also exercises manifest snapshots over this CF.
    Close();
    ASSERT_OK(TryReopenWithColumnFamilies({kDefaultColumnFamilyName, "failed"},
                                        options));
    ASSERT_EQ(Get(0, "existing"), "preserved");
    ASSERT_EQ(Get(1, "existing"), "NOT_FOUND");
    std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions(), handles_[1]));
    iterator->SeekToFirst();
    ASSERT_FALSE(iterator->Valid());
    ASSERT_OK(iterator->status());
    iterator.reset();
    Close();
  }
}

TEST_F(DBCsppCrashSafeTest, FileMmapRejectsReadOnlyAndSecondary) {
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    ASSERT_EQ(RunCrashChild(dbname_, "WalFilterFallsBackToWal",
                            osl ? "10" : "00"), 0);
    std::vector<std::string> before;
    ASSERT_OK(env_->GetChildren(dbname_, &before));
    std::sort(before.begin(), before.end());
    for (bool secondary : {false, true}) {
      SCOPED_TRACE(osl);
      SCOPED_TRACE(secondary);
      DB* rejected = nullptr;
      const Status s = secondary
          ? DB::OpenAsSecondary(options, dbname_, dbname_ + "_secondary",
                                &rejected)
          : DB::OpenForReadOnly(options, dbname_, &rejected);
      ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
      ASSERT_EQ(rejected, nullptr);
      std::vector<std::string> after;
      ASSERT_OK(env_->GetChildren(dbname_, &after));
      std::sort(after.begin(), after.end());
      ASSERT_EQ(after, before);
    }
  }
}

TEST_F(DBCsppCrashSafeTest, ReadWriteWalRecoveryFailureDoesNotAbort) {
  class CorruptSecondRecord final : public WalFilter {
   public:
    int calls = 0;
    const char* Name() const override { return "CorruptSecondRecord"; }
    WalProcessingOption LogRecordFound(unsigned long long, const std::string&,
                                       const WriteBatch&, WriteBatch*,
                                       bool*) override {
      return ++calls == 1 ? WalProcessingOption::kContinueProcessing
                          : WalProcessingOption::kCorruptedRecord;
    }
  };
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    ASSERT_EQ(RunCrashChild(dbname_, "WalFilterFallsBackToWal",
                            osl ? "10" : "00"), 0);
    auto list_sst = [&] {
      std::vector<std::string> children;
      EXPECT_OK(env_->GetChildren(dbname_, &children));
      std::vector<std::string> files;
      for (const auto& child : children) {
        if (child.size() >= 4 && child.compare(child.size() - 4, 4, ".sst") == 0)
          files.push_back(child);
      }
      std::sort(files.begin(), files.end());
      return files;
    };
    const auto before = list_sst();
    ASSERT_FALSE(before.empty());
    std::map<std::string, std::string> original_files;
    for (const auto& name : before) {
      ASSERT_OK(ReadFileToString(env_, dbname_ + "/" + name, &original_files[name]));
    }
    CorruptSecondRecord filter;
    options.memtable_crash_safe_recover = false;
    options.wal_filter = &filter;
    options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
    DB* failed = nullptr;
    const Status status = DB::Open(options, dbname_, &failed);
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    ASSERT_EQ(failed, nullptr);
    ASSERT_EQ(filter.calls, 2);
    for (const auto& file : original_files) {
      std::string after;
      ASSERT_OK(ReadFileToString(env_, dbname_ + "/" + file.first, &after));
      ASSERT_EQ(after, file.second);
    }
    options.wal_filter = nullptr;
    options.memtable_crash_safe_recover = true;
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("a"), std::string(128, 'a'));
    ASSERT_EQ(Get("b"), std::string(128, 'b'));
    Close();
  }
}

TEST_F(DBCsppCrashSafeTest, ManifestRolloverPreservesMemTableRegistry) {
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    options.max_manifest_file_size = 1;
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("first", "1"));
    std::string before;
    ASSERT_OK(ReadFileToString(env_, CurrentFileName(dbname_), &before));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("second", "2"));
    std::string after;
    ASSERT_OK(ReadFileToString(env_, CurrentFileName(dbname_), &after));
    ASSERT_NE(before, after);
    const auto registered = dbfull()->GetVersionSet()->GetColumnFamilySet()
                                ->GetDefault()->GetMemTableFiles();
    ASSERT_EQ(registered.size(), 2U);
    const auto disk = ListLeftovers(options, dbname_);
    ASSERT_EQ(disk.size(), 2U);
    for (uint64_t number : registered) {
      const auto path = MakeTableFileName(dbname_, number);
      ASSERT_EQ(std::count(disk.begin(), disk.end(), path), 1);
      ASSERT_OK(env_->FileExists(path));
    }
    Close();
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("first"), "1");
    ASSERT_EQ(Get("second"), "2");
    Close();
  }
}

TEST_F(CrashChild, DISABLED_LegacyManifestWal) {
  Options options = BaseCrashSafeOptions(dbname_, false, false);
  options.memtable_factory = std::make_shared<SkipListFactory>();
  options.table_factory = Options().table_factory;
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  WriteOptions write;
  write.sync = true;
  ASSERT_OK(child_db->Put(write, "legacy-first", "one"));
  ASSERT_OK(child_db->Put(write, "legacy-second", "two"));
  ::_exit(42);
}

TEST_F(DBCsppCrashSafeTest, LegacyManifestWithoutTrackingUsesFullWal) {
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    ASSERT_EQ(RunCrashChild(dbname_, "LegacyManifestWal"), 42);
    ASSERT_TRUE(ListLeftovers(options, dbname_).empty());
    std::vector<std::string> children;
    ASSERT_OK(env_->GetChildren(dbname_, &children));
    uint64_t wal_number = 0;
    for (const auto& child : children) {
      uint64_t number;
      FileType type;
      if (ParseFileName(child, &number, &type) && type == kWalFile)
        wal_number = std::max(wal_number, number);
    }
    ASSERT_NE(wal_number, 0U);
    uint64_t wal_size = 0;
    ASSERT_OK(env_->GetFileSize(LogFileName(dbname_, wal_number), &wal_size));
    ASSERT_GT(wal_size, 0U);
    // Valid classic sidecar deliberately points past both keys. An inventory
    // without tracking cannot justify skipping this WAL prefix.
    PublishedSeqOnDisk record;
    record.magic = 0x5145534255505343ULL;
    record.version = 1;
    record.header_size = sizeof(record);
    record.wal_offset_kind = 1;
    record.kind_since_wal = static_cast<uint32_t>(wal_number);
    record.generation = 2;
    record.pubseq = 2;
    record.wal_number = wal_number;
    record.wal_offset = wal_size;
    std::string sidecar(4096, '\0');
    std::memcpy(&sidecar[0], &record, sizeof(record));
    ASSERT_OK(WriteStringToFile(env_, sidecar, CrashSafePubSeqFileName(dbname_)));
    std::atomic<int> converted{0};
    std::atomic<int> reads{0};
    SyncPoint::GetInstance()->SetCallBack(
        "MemTableRep::ConvertToSST:After", [&](void*) { ++converted; });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::RecoverLogFiles:BeforeReadWal", [&](void*) { ++reads; });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(TryReopen(options));
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_EQ(converted.load(), 0);
    ASSERT_GT(reads.load(), 0);
    ASSERT_EQ(Get("legacy-first"), "one");
    ASSERT_EQ(Get("legacy-second"), "two");
    ASSERT_TRUE(dbfull()->GetVersionSet()->HasMemTableFileTracking());
    Close();
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("legacy-first"), "one");
    ASSERT_EQ(Get("legacy-second"), "two");
    Close();
  }
}

TEST_F(CrashChild, DISABLED_FlushManifestWindow) {
  ASSERT_EQ(arg_.size(), 2U);
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  if (arg_[0] == '1') SetupOsl(&options, true);
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "converted", "durable"));
  SyncPoint::GetInstance()->SetCallBack(
      arg_[1] == '0' ? "FlushJob::BeforeManifest"
                     : "FlushJob::AfterManifest",
      [](void*) { ::_exit(42); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(child_db->Flush(FlushOptions()));
  ::_exit(1);
}

TEST_F(DBCsppCrashSafeTest, ConversionCrashAcrossManifestCommit) {
  Close();
  for (bool osl : {false, true}) {
    for (bool committed : {false, true}) {
      SCOPED_TRACE(osl);
      SCOPED_TRACE(committed);
      Options options = BaseCrashSafeOptions(dbname_, true, false);
      if (osl) SetupOsl(&options, true);
      Destroy(options);
      const std::string arg = std::string(osl ? "1" : "0") +
                              (committed ? "1" : "0");
      ASSERT_EQ(RunCrashChild(dbname_, "FlushManifestWindow", arg), 42);
      const auto registered = ListLeftovers(options, dbname_);
      ASSERT_FALSE(registered.empty());
      for (const auto& path : registered) ASSERT_OK(env_->FileExists(path));
      std::atomic<int> converts{0};
      SyncPoint::GetInstance()->SetCallBack(
          "MemTableRep::ConvertToSST:After",
          [&](void*) { ++converts; });
      SyncPoint::GetInstance()->EnableProcessing();
      ASSERT_OK(TryReopen(options));
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      ASSERT_EQ(converts.load(), committed ? 0 : 1);
      ASSERT_EQ(Get("converted"), "durable");
      ASSERT_EQ(CountL0(db_), 1);
      std::vector<LiveFileMetaData> files;
      db_->GetLiveFilesMetaData(&files);
      ASSERT_EQ(files.size(), 1U);
      if (!committed) {
        const std::string path = MakeTableFileName(dbname_, files[0].file_number);
        ASSERT_EQ(std::count(registered.begin(), registered.end(), path), 1);
        ASSERT_OK(env_->FileExists(path));
      }
      Close();
      ASSERT_OK(TryReopen(options));
      ASSERT_EQ(Get("converted"), "durable");
      ASSERT_EQ(CountL0(db_), 1);
      Close();
    }
  }
}

TEST_F(DBCsppCrashSafeTest, GarbageCollectionKeepsActiveAndCachedMemTables) {
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(TryReopen(options));
    std::atomic<int> published{0};
    SyncPoint::GetInstance()->SetCallBack(
        "FlushJob::MemTableCache:BeforePublish", [&](void*) {
          std::vector<std::string> expected;
          for (auto* cfd : *dbfull()->GetVersionSet()->GetColumnFamilySet()) {
            for (uint64_t number : cfd->GetMemTableFiles()) {
              expected.push_back(MakeTableFileName(dbname_, number));
            }
          }
          ASSERT_EQ(ListLeftovers(options, dbname_), expected);
          for (const auto& path : expected) ASSERT_OK(env_->FileExists(path));
        });
    SyncPoint::GetInstance()->SetCallBack(
        "FlushJob::MemTableCache:AfterPublish", [&](void*) { ++published; });
    ASSERT_OK(Put("first", "1"));
    ASSERT_OK(Flush());
    ASSERT_GT(published.load(), 0);
    ASSERT_OK(Put("active", "2"));
    const auto registered = dbfull()->GetVersionSet()->GetColumnFamilySet()
                                ->GetDefault()->GetMemTableFiles();
    ASSERT_GE(registered.size(), 2U);
    ASSERT_OK(db_->DisableFileDeletions());
    ASSERT_OK(db_->EnableFileDeletions(true));
    for (uint64_t number : registered) {
      ASSERT_OK(env_->FileExists(MakeTableFileName(dbname_, number)));
    }
    ASSERT_EQ(Get("first"), "1");
    ASSERT_EQ(Get("active"), "2");
    Close();
    const auto kept = ListLeftovers(options, dbname_);
    ASSERT_FALSE(kept.empty());
    for (const auto& path : kept) ASSERT_OK(env_->FileExists(path));
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("first"), "1");
    ASSERT_EQ(Get("active"), "2");
    Close();
  }
}

TEST_F(DBCsppCrashSafeTest, EmptyFlushRetiresSourceFileWithoutFullScan) {
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    options.delete_obsolete_files_period_micros = UINT64_MAX;
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    ASSERT_OK(TryReopen(options));
    auto* cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    const std::string source =
        MakeTableFileName(dbname_, cfd->mem()->GetFileNumber());
    ASSERT_OK(env_->FileExists(source));
    ASSERT_OK(dbfull()->TEST_SwitchMemtable());
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
    ASSERT_OK(dbfull()->TEST_WaitForPurge());
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    ASSERT_TRUE(files.empty());
    ASSERT_TRUE(env_->FileExists(source).IsNotFound());
    const auto active = ListLeftovers(options, dbname_);
    ASSERT_EQ(active.size(), 2U);
    for (const auto& path : active) ASSERT_OK(env_->FileExists(path));
    const auto converted = MakeTableFileName(dbname_, cfd->mem()->GetFileNumber());
    ASSERT_OK(Put("converted", "preserved"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
    ASSERT_OK(dbfull()->TEST_WaitForPurge());
    db_->GetLiveFilesMetaData(&files);
    ASSERT_EQ(files.size(), 1U);
    ASSERT_EQ(MakeTableFileName(dbname_, files[0].file_number), converted);
    ASSERT_OK(env_->FileExists(converted));
    for (const auto& path : ListLeftovers(options, dbname_))
      ASSERT_OK(env_->FileExists(path));
    ASSERT_EQ(Get("converted"), "preserved");
    Close();
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("converted"), "preserved");
    Close();
  }
}

TEST_F(DBCsppCrashSafeTest, DroppedCfRetiresSourceFilesWithoutFullScan) {
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    options.delete_obsolete_files_period_micros = UINT64_MAX;
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    ASSERT_OK(TryReopen(options));
    CreateAndReopenWithCF({"retire"}, options);
    ASSERT_OK(Put(0, "keep", "one"));
    ASSERT_OK(Put(1, "drop", "two"));
    const auto registered = dbfull()->GetVersionSet()->GetColumnFamilySet()
                                ->GetColumnFamily(1)->GetMemTableFiles();
    ASSERT_EQ(registered.size(), 2U);
    for (uint64_t number : registered)
      ASSERT_OK(env_->FileExists(MakeTableFileName(dbname_, number)));
    ASSERT_OK(db_->DropColumnFamily(handles_[1]));
    ASSERT_OK(db_->DestroyColumnFamilyHandle(handles_[1]));
    handles_.pop_back();
    // An ordinary flush provides normal obsolete-file GC, without a scan.
    ASSERT_OK(Flush(0));
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
    ASSERT_OK(dbfull()->TEST_WaitForPurge());
    for (uint64_t number : registered)
      ASSERT_TRUE(env_->FileExists(MakeTableFileName(dbname_, number)).IsNotFound());
    ASSERT_EQ(dbfull()->GetVersionSet()->GetColumnFamilySet()
                  ->GetColumnFamily(1), nullptr);
    for (const auto& path : ListLeftovers(options, dbname_))
      ASSERT_OK(env_->FileExists(path));
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    ASSERT_EQ(files.size(), 1U);
    ASSERT_OK(env_->FileExists(MakeTableFileName(dbname_, files[0].file_number)));
    ASSERT_EQ(Get(0, "keep"), "one");
    Close();
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("keep"), "one");
    Close();
  }
}

TEST_F(DBCsppCrashSafeTest, CheckpointDoesNotHardLinkWritableMemTable) {
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("checkpoint", "original"));
    const auto registered = dbfull()->GetVersionSet()->GetColumnFamilySet()
                                ->GetDefault()->GetMemTableFiles();
    ASSERT_EQ(registered.size(), 2U);
    const uint64_t number = dbfull()->GetVersionSet()->GetColumnFamilySet()
                                ->GetDefault()->mem()->GetFileNumber();
    ASSERT_EQ(registered.count(number), 1U);
    const std::string checkpoint_dir = dbname_ + ".checkpoint";
    Options copy_options = options;
    copy_options.wal_dir = checkpoint_dir;
    ASSERT_OK(DestroyDB(checkpoint_dir, copy_options));
    Checkpoint* raw = nullptr;
    ASSERT_OK(Checkpoint::Create(db_, &raw));
    std::unique_ptr<Checkpoint> checkpoint(raw);
    ASSERT_OK(checkpoint->CreateCheckpoint(checkpoint_dir, UINT64_MAX));
    // This memtable is still mutable, and hence must not be a live SST.
    const auto& after = dbfull()->GetVersionSet()->GetColumnFamilySet()
                            ->GetDefault()->GetMemTableFiles();
    if (after.count(number)) {
      ASSERT_TRUE(env_->FileExists(MakeTableFileName(checkpoint_dir, number))
                      .IsNotFound());
    }
    ASSERT_OK(Put("checkpoint", "source-changed"));
    DB* copy_raw = nullptr;
    ASSERT_OK(DB::Open(copy_options, checkpoint_dir, &copy_raw));
    std::unique_ptr<DB> copy(copy_raw);
    std::string value;
    ASSERT_OK(copy->Get(ReadOptions(), "checkpoint", &value));
    ASSERT_EQ(value, "original");
    copy.reset();
    checkpoint.reset();
    ASSERT_OK(DestroyDB(checkpoint_dir, copy_options));
    Close();
  }
}



TEST_F(DBCsppCrashSafeTest, OpenAndCreateColumnFamilyRegisterNextMemTable) {
  Close();
  for (bool osl : {false, true}) {
    for (bool atomic : {false, true}) {
      SCOPED_TRACE(osl);
      SCOPED_TRACE(atomic);
      Options options = BaseCrashSafeOptions(dbname_, true, false);
      options.atomic_flush = atomic;
      options.avoid_flush_during_shutdown = true;
      if (osl) SetupOsl(&options, true);
      Destroy(options);
      SyncPoint::GetInstance()->EnableProcessing();
      ASSERT_OK(TryReopen(options));
      auto* default_cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()
                              ->GetColumnFamily(0);
      dbfull()->TEST_LockMutex();
      auto* default_head = default_cfd->PeekPrecreatedMemtable();
      const uint64_t default_cache = default_head ? default_head->GetFileNumber() : 0;
      dbfull()->TEST_UnlockMutex();
      ASSERT_NE(default_cache, 0U);
      ASSERT_EQ(default_cfd->GetMemTableFiles()
                    .count(default_cache), 1U);

      ColumnFamilyHandle* handle = nullptr;
      ASSERT_OK(db_->CreateColumnFamily(options, "bootstrap", &handle));
      auto* created_cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()
                              ->GetColumnFamily(handle->GetID());
      dbfull()->TEST_LockMutex();
      auto* created_head = created_cfd->PeekPrecreatedMemtable();
      const uint64_t created_cache = created_head ? created_head->GetFileNumber() : 0;
      dbfull()->TEST_UnlockMutex();
      ASSERT_NE(created_cache, 0U);
      ASSERT_EQ(created_cfd->GetMemTableFiles()
                    .count(created_cache), 1U);
      std::atomic<int> front_registrations{0};
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::RegisterMemTableFile:BeforeLogAndApply",
          [&](void*) { ++front_registrations; });
      ASSERT_OK(Put("default", "1"));
      ASSERT_OK(Flush());
      ASSERT_EQ(default_cfd->mem()->GetFileNumber(), default_cache);
      ASSERT_TRUE(default_cfd->mem()->IsFileRegistered());
      ASSERT_OK(db_->Put(WriteOptions(), handle, "created", "2"));
      ASSERT_OK(db_->Flush(FlushOptions(), handle));
      ASSERT_EQ(created_cfd->mem()->GetFileNumber(), created_cache);
      ASSERT_TRUE(created_cfd->mem()->IsFileRegistered());
      ASSERT_EQ(front_registrations.load(), 0);
      ASSERT_OK(db_->DestroyColumnFamilyHandle(handle));
      Close();
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
    }
  }
}

TEST_F(DBCsppCrashSafeTest, SwitchWaitsForPendingCacheRegistration) {
  Close();
  for (bool osl : {false, true}) {
    for (bool atomic : {false, true}) {
      for (int mode : {0, 1, 2, 3}) {  // Success, failure, shutdown, already committed.
        const bool fail = mode == 1;
        const bool shutdown = mode == 2;
        SCOPED_TRACE(osl);
        SCOPED_TRACE(atomic);
        SCOPED_TRACE(mode);
        Options options = BaseCrashSafeOptions(dbname_, true, false);
        options.atomic_flush = atomic;
        options.max_bgerror_resume_count = 0;
        options.avoid_flush_during_shutdown = true;
        if (osl) SetupOsl(&options, true);
        Destroy(options);
        SyncPoint::GetInstance()->EnableProcessing();
        ASSERT_OK(TryReopen(options));
        auto* cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()
                        ->GetColumnFamily(0);
        ASSERT_OK(Put("first", "1"));

        std::mutex mu;
        std::condition_variable cv;
        bool paused = false, release = false, waiting = false;
        bool flush_done = false, switch_done = false, pause_timeout = false;
        std::atomic<int> frontend_registrations{0};
        const auto caller = std::this_thread::get_id();
        std::atomic<bool> arm{false}, inject{false};
        SyncPoint::GetInstance()->SetCallBack(
            "FlushJob::BeforeManifest", [&](void*) { arm.store(true); });
        SyncPoint::GetInstance()->SetCallBack(
            "VersionSet::LogAndApply:WriteManifestStart", [&](void*) {
              if (!arm.exchange(false)) return;
              std::unique_lock<std::mutex> lk(mu);
              paused = true;
              inject.store(fail);
              cv.notify_all();
              // Do not strand the flush thread if an assertion misses the hook.
              pause_timeout = !cv.wait_for(lk, std::chrono::seconds(30),
                                          [&] { return release; });
            });
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::SwitchMemtable:BeforeInstallMemTable", [&](void*) {
              if (mode != 3) return;
              std::unique_lock<std::mutex> lk(mu);
              if (!paused) return;
              waiting = true;
              cv.notify_all();
              if (!cv.wait_for(lk, std::chrono::seconds(30),
                               [&] { return release && flush_done; }))
                std::abort();
            });
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::SwitchMemtable:MemTableCacheMiss", [&](void* p) {
              EXPECT_NE(static_cast<MemTable*>(p)->GetFileNumber(), 0U);
              if (mode == 3) return;
              std::lock_guard<std::mutex> lk(mu);
              waiting = true;
              cv.notify_all();
            });
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::RegisterMemTableFile:BeforeLogAndApply", [&](void*) {
              EXPECT_NE(std::this_thread::get_id(), caller);
              ++frontend_registrations;
            });
        SyncPoint::GetInstance()->SetCallBack(
            "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* p) {
              if (inject.exchange(false)) {
                *static_cast<IOStatus*>(p) = IOStatus::IOError("cache wait injection");
              }
            });
        Status flush_status, switch_status;
        std::thread flush_thread([&] {
          flush_status = Flush();
          std::lock_guard<std::mutex> lk(mu);
          flush_done = true;
          cv.notify_all();
        });
        bool reached_pause;
        {
          std::unique_lock<std::mutex> lk(mu);
          reached_pause = cv.wait_for(lk, std::chrono::seconds(10),
                                      [&] { return paused || flush_done; }) && paused;
        }
        std::vector<uint64_t> cache_files;
        std::vector<std::string> before_switch, after_switch;
        uint64_t active_file = 0;
        if (reached_pause) {
          dbfull()->TEST_LockMutex();
          if (auto* head = cfd->PeekPrecreatedMemtable()) {
            cache_files.push_back(head->GetFileNumber());
          }
          active_file = cfd->mem()->GetFileNumber();
          dbfull()->TEST_UnlockMutex();
          EXPECT_OK(env_->GetChildren(dbname_, &before_switch));
          EXPECT_OK(Put("active", "2"));
        }
        std::thread switch_thread([&] {
          switch_status = dbfull()->TEST_SwitchMemtable();
          std::lock_guard<std::mutex> lk(mu);
          switch_done = true;
          cv.notify_all();
        });
        bool reached_wait;
        {
          std::unique_lock<std::mutex> lk(mu);
          reached_wait = cv.wait_for(lk, std::chrono::seconds(10),
                                     [&] { return waiting || switch_done; }) && waiting;
        }
        if (reached_pause && reached_wait) {
          // The popped head remains protected by the switch's pending output.
          std::vector<uint64_t> still_cached;
          dbfull()->TEST_LockMutex();
          if (auto* head = cfd->PeekPrecreatedMemtable()) {
            still_cached.push_back(head->GetFileNumber());
          }
          dbfull()->TEST_UnlockMutex();
          EXPECT_TRUE(still_cached.empty());
          EXPECT_OK(db_->DisableFileDeletions());
          EXPECT_OK(db_->EnableFileDeletions(true));
          for (uint64_t number : cache_files) {
            EXPECT_OK(env_->FileExists(MakeTableFileName(dbname_, number)));
          }
          EXPECT_OK(env_->GetChildren(dbname_, &after_switch));
          auto only_ssts = [](const std::vector<std::string>& children) {
            std::set<std::string> files;
            for (const auto& child : children) {
              if (child.size() >= 4 && child.compare(child.size() - 4, 4, ".sst") == 0)
                files.insert(child);
            }
            return files;
          };
          EXPECT_EQ(only_ssts(before_switch), only_ssts(after_switch));
          std::lock_guard<std::mutex> lk(mu);
          EXPECT_FALSE(switch_done);
        }
        if (shutdown && reached_wait) {
          CancelAllBackgroundWork(db_, false);
        }
        {
          std::unique_lock<std::mutex> lk(mu);
          release = true;
          cv.notify_all();
          if (!cv.wait_for(lk, std::chrono::seconds(30),
                           [&] { return flush_done && switch_done; })) {
            // A missed wakeup must fail this test instead of hanging in join.
            std::abort();
          }
        }
        flush_thread.join();
        switch_thread.join();
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        ASSERT_TRUE(reached_pause);
        ASSERT_TRUE(reached_wait);
        ASSERT_FALSE(pause_timeout);
        ASSERT_EQ(cache_files.size(), 1U);
        ASSERT_EQ(frontend_registrations.load(), mode == 3 ? 0 : 1);
        if (shutdown) {
          ASSERT_TRUE(switch_status.IsShutdownInProgress());
          ASSERT_TRUE(flush_status.ok() || flush_status.IsShutdownInProgress());
          ASSERT_EQ(cfd->mem()->GetFileNumber(), active_file);
        } else if (fail) {
          ASSERT_TRUE(flush_status.IsIOError());
          ASSERT_TRUE(switch_status.IsIOError());
          ASSERT_TRUE(dbfull()->TEST_GetBGError().IsIOError());
          ASSERT_EQ(cfd->mem()->GetFileNumber(), active_file);
          ASSERT_OK(db_->DisableFileDeletions());
          ASSERT_OK(db_->EnableFileDeletions(true));
          ASSERT_OK(env_->FileExists(MakeTableFileName(dbname_, active_file)));
        } else {
          ASSERT_OK(flush_status);
          ASSERT_OK(switch_status);
          ASSERT_EQ(cfd->mem()->GetFileNumber(), cache_files.front());
          ASSERT_TRUE(cfd->mem()->IsFileRegistered());
          ASSERT_OK(Put("switched", "3"));
        }
        Close();
        ASSERT_OK(TryReopen(options));
        ASSERT_EQ(Get("first"), "1");
        ASSERT_EQ(Get("active"), "2");
        if (!fail && !shutdown) {
          ASSERT_EQ(Get("switched"), "3");
        }
        Close();
      }
    }
  }
}

TEST_F(DBCsppCrashSafeTest, CacheMissRegistersWhileFlushIsPaused) {
  Close();
  for (bool osl : {false, true}) {
    for (bool atomic : {false, true}) {
      for (bool fail : {false, true}) {
        SCOPED_TRACE(osl);
        SCOPED_TRACE(atomic);
        SCOPED_TRACE(fail);
        Options options = BaseCrashSafeOptions(dbname_, true, false);
        options.atomic_flush = atomic;
        options.paranoid_checks = !fail;
        options.avoid_flush_during_shutdown = true;
        if (osl) SetupOsl(&options, true);
        Destroy(options);
        SyncPoint::GetInstance()->EnableProcessing();
        ASSERT_OK(TryReopen(options));
        auto* cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()
                        ->GetColumnFamily(0);
        ASSERT_OK(Put("first", "1"));
        if (atomic) {
          CreateColumnFamilies({"aux"}, options);
          ASSERT_EQ(handles_.size(), 1U);
        }
        auto* aux = atomic ? handles_.back() : nullptr;
        if (atomic) {
          ASSERT_OK(db_->Put(WriteOptions(), aux, "aux-first", "3"));
        }
        std::mutex mu;
        std::condition_variable cv;
        bool paused = false, release = false, done = false, waiting = false;
        bool pause_timeout = false;
        std::atomic<bool> first_convert{true};
        std::atomic<int> conversions{0};
        const auto caller = std::this_thread::get_id();
        std::thread::id switch_id;
        std::atomic<int> registrations{0};
        std::atomic<int> foreground_registrations{0};
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::SwitchMemtable:MemTableCacheMiss", [&](void*) {
              std::lock_guard<std::mutex> lk(mu);
              waiting = true;
              cv.notify_all();
            });
        SyncPoint::GetInstance()->SetCallBack(
            "MemTableRep::ConvertToSST:Before", [&](void*) {
              if (!first_convert.exchange(false)) return;
              std::unique_lock<std::mutex> lk(mu);
              paused = true;
              cv.notify_all();
              pause_timeout = !cv.wait_for(lk, std::chrono::seconds(30),
                                          [&] { return release; });
            });
        SyncPoint::GetInstance()->SetCallBack(
            "FlushJob::ConvertToSST:Status", [&](void* p) {
              EXPECT_TRUE(static_cast<Status*>(p)->ok());
              // Atomic flush runs aux first, then default; fail only the last.
              if (++conversions == (atomic ? 2 : 1) && fail)
                *static_cast<Status*>(p) = Status::IOError("ignored table flush injection");
            });
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::RegisterMemTableFile:BeforeLogAndApply", [&](void*) {
              std::lock_guard<std::mutex> lk(mu);
              EXPECT_NE(std::this_thread::get_id(), caller);
              if (std::this_thread::get_id() == switch_id)
                ++foreground_registrations;
              else
                ++registrations;
            });
        FlushOptions flush_options;
        flush_options.wait = false;
        const Status flush_status = atomic
            ? db_->Flush(flush_options, {db_->DefaultColumnFamily(), aux})
            : db_->Flush(flush_options);
        bool reached_pause;
        {
          std::unique_lock<std::mutex> lk(mu);
          reached_pause = cv.wait_for(lk, std::chrono::seconds(10),
                                      [&] { return paused; });
        }
        dbfull()->TEST_LockMutex();
        const bool cache_empty = cfd->PeekPrecreatedMemtable() == nullptr;
        dbfull()->TEST_UnlockMutex();
        EXPECT_TRUE(cache_empty);
        EXPECT_OK(Put("second", "2"));
        Status switch_status;
        std::thread switch_thread([&] {
          {
            std::lock_guard<std::mutex> lk(mu);
            switch_id = std::this_thread::get_id();
          }
          switch_status = dbfull()->TEST_SwitchMemtable();
          std::lock_guard<std::mutex> lk(mu);
          done = true;
          cv.notify_all();
        });
        bool reached_wait;
        bool completed_before_release;
        {
          std::unique_lock<std::mutex> lk(mu);
          reached_wait = cv.wait_for(
              lk, std::chrono::seconds(10), [&] { return waiting || done; }) &&
                         waiting;
        }
        {
          std::unique_lock<std::mutex> lk(mu);
          completed_before_release = cv.wait_for(
              lk, std::chrono::seconds(10), [&] { return done; });
          release = true;
          cv.notify_all();
          if (!cv.wait_for(lk, std::chrono::seconds(30), [&] { return done; }))
            std::abort();
        }
        switch_thread.join();
        ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        ASSERT_TRUE(reached_pause);
        ASSERT_TRUE(reached_wait);
        ASSERT_TRUE(completed_before_release);
        ASSERT_FALSE(pause_timeout);
        ASSERT_OK(flush_status);
        ASSERT_OK(switch_status);
        ASSERT_OK(dbfull()->TEST_GetBGError());
        ASSERT_TRUE(cfd->mem()->IsFileRegistered());
        ASSERT_EQ(foreground_registrations.load(), 1);
        ASSERT_EQ(conversions.load(), atomic ? 2 : 1);
        ASSERT_EQ(registrations.load(), 0);
        if (atomic) {
          auto* aux_cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()
                              ->GetColumnFamily(aux->GetID());
          dbfull()->TEST_LockMutex();
          auto* aux_head = aux_cfd->PeekPrecreatedMemtable();
          const uint64_t aux_cache_number = aux_head ? aux_head->GetFileNumber() : 0;
          dbfull()->TEST_UnlockMutex();
          ASSERT_NE(aux_cache_number, 0U);
          ASSERT_EQ(dbfull()->GetVersionSet()->GetColumnFamilySet()
                        ->GetColumnFamily(aux->GetID())->GetMemTableFiles()
                        .count(aux_cache_number), fail ? 0U : 1U);
          if (fail) {
            ASSERT_OK(dbfull()->TEST_SwitchMemtable(aux_cfd));
            ASSERT_EQ(aux_cfd->mem()->GetFileNumber(), aux_cache_number);
            ASSERT_TRUE(aux_cfd->mem()->IsFileRegistered());
            ASSERT_EQ(aux_cfd->GetMemTableFiles().count(aux_cache_number), 1U);
          }
        }
        Close();
        if (atomic) {
          ASSERT_OK(TryReopenWithColumnFamilies({"default", "aux"}, options));
          ASSERT_EQ(Get(0, "first"), "1");
          ASSERT_EQ(Get(0, "second"), "2");
          ASSERT_EQ(Get(1, "aux-first"), "3");
        } else {
          ASSERT_OK(TryReopen(options));
          ASSERT_EQ(Get("first"), "1");
          ASSERT_EQ(Get("second"), "2");
        }
        Close();
      }
    }
  }
}

TEST_F(DBCsppCrashSafeTest, FrontendCacheMissRegistrationFailure) {
  Close();
  for (bool osl : {false, true}) {
    for (bool after_sync : {false, true}) {
      SCOPED_TRACE(osl);
      SCOPED_TRACE(after_sync);
      Options options = BaseCrashSafeOptions(dbname_, true, false);
      options.max_bgerror_resume_count = 0;
      options.avoid_flush_during_shutdown = true;
      if (osl) SetupOsl(&options, true);
      Destroy(options);
      ASSERT_OK(TryReopen(options));
      ASSERT_OK(Put("first", "1"));
      ASSERT_OK(dbfull()->TEST_SwitchMemtable());
      ASSERT_OK(Put("active", "2"));
      auto* cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
      const uint64_t active = cfd->mem()->GetFileNumber();
      ASSERT_EQ(cfd->PeekPrecreatedMemtable(), nullptr);
      const auto caller = std::this_thread::get_id();
      int injected = 0;
      uint64_t candidate = 0;
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::SwitchMemtable:MemTableCacheMiss", [&](void* p) {
            candidate = static_cast<MemTable*>(p)->GetFileNumber();
            EXPECT_NE(candidate, active);
            EXPECT_EQ(cfd->PeekPrecreatedMemtable(), nullptr);
          });
      SyncPoint::GetInstance()->SetCallBack(
          after_sync ? "VersionSet::ProcessManifestWrites:AfterSyncManifest"
                     : "DBImpl::RegisterMemTableFile:AfterLogAndApply",
          [&](void* p) {
            EXPECT_EQ(std::this_thread::get_id(), caller);
            ++injected;
            if (after_sync) {
              *static_cast<IOStatus*>(p) = IOStatus::IOError("frontend register injection");
            } else {
              *static_cast<Status*>(p) = Status::IOError("frontend register injection");
            }
          });
      SyncPoint::GetInstance()->EnableProcessing();
      ASSERT_TRUE(dbfull()->TEST_SwitchMemtable().IsIOError());
      ASSERT_EQ(injected, 1);
      ASSERT_TRUE(dbfull()->TEST_GetBGError().IsIOError());
      ASSERT_EQ(cfd->mem()->GetFileNumber(), active);
      ASSERT_NE(candidate, 0U);
      ASSERT_EQ(cfd->GetMemTableFiles().count(candidate), after_sync ? 0U : 1U);
      ASSERT_EQ(cfd->PeekPrecreatedMemtable(), nullptr);
      ASSERT_OK(env_->FileExists(MakeTableFileName(dbname_, candidate)));
      ASSERT_NOK(Put("rejected", "3"));
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      ASSERT_OK(db_->DisableFileDeletions());
      ASSERT_OK(db_->EnableFileDeletions(true));
      ASSERT_OK(env_->FileExists(MakeTableFileName(dbname_, active)));
      ASSERT_EQ(Get("first"), "1");
      ASSERT_EQ(Get("active"), "2");
      Close();
      ASSERT_OK(TryReopen(options));
      ASSERT_EQ(Get("first"), "1");
      ASSERT_EQ(Get("active"), "2");
      ASSERT_EQ(Get("rejected"), "NOT_FOUND");
      Close();
    }
  }
}

TEST_F(DBCsppCrashSafeTest, FailedCacheRegistrationSurvivesGcAndReopen) {
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    options.max_bgerror_resume_count = 0;
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("retry", "preserved"));
    std::vector<std::string> pending;
    std::atomic<bool> pending_commit{false};
    SyncPoint::GetInstance()->SetCallBack("FlushJob::BeforeManifest", [&](void*) {
      std::vector<std::string> children;
      ASSERT_OK(env_->GetChildren(dbname_, &children));
      for (const auto& child : children) {
        if (child.size() >= 4 && child.compare(child.size() - 4, 4, ".sst") == 0)
          pending.push_back(dbname_ + "/" + child);
      }
      pending_commit.store(true);
    });
    std::atomic<bool> injected{false};
    std::atomic<int> published{0};
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* p) {
          if (pending_commit.load() && !injected.exchange(true))
            *static_cast<IOStatus*>(p) = IOStatus::IOError("cache register injection");
        });
    SyncPoint::GetInstance()->SetCallBack(
        "FlushJob::MemTableCache:AfterPublish", [&](void*) { ++published; });
    ASSERT_NOK(Flush());
    ASSERT_TRUE(injected.load());
    ASSERT_EQ(published.load(), 0);
    ASSERT_GE(pending.size(), 3U);  // converting input, active, pending cache
    ASSERT_OK(db_->DisableFileDeletions());
    ASSERT_OK(db_->EnableFileDeletions(true));
    for (const auto& path : pending) ASSERT_OK(env_->FileExists(path));
    SyncPoint::GetInstance()->ClearCallBack("FlushJob::BeforeManifest");
    // Plain MANIFEST IOError is fatal under the existing error policy.
    // Resume preserves that error; reopening is the supported recovery path.
    const Status resumed = db_->Resume();
    ASSERT_TRUE(resumed.IsIOError());
    ASSERT_EQ(Get("retry"), "preserved");
    Close();
    SyncPoint::GetInstance()->ClearCallBack(
        "VersionSet::ProcessManifestWrites:AfterSyncManifest");
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("retry"), "preserved");
    published.store(0);
    ASSERT_OK(Put("after-reopen", "committed"));
    ASSERT_OK(Flush());
    ASSERT_GT(published.load(), 0);
    ASSERT_EQ(Get("retry"), "preserved");
    Close();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("retry"), "preserved");
    ASSERT_EQ(Get("after-reopen"), "committed");
    Close();
  }
}
#endif

TEST_F(DBCsppCrashSafeTest, CrashSafeRequiresFileMmapFactories) {
  Close();
  for (bool osl : {false, true}) {
    for (const char* mode : {"kDontConvert", "kDumpMem", "SkipList"}) {
      SCOPED_TRACE(osl);
      SCOPED_TRACE(mode);
      Options options = BaseCrashSafeOptions(dbname_, true, false);
      if (osl) SetupOsl(&options, true);
      Options unsupported = options;
      unsupported.memtable_factory = std::string(mode) == "SkipList"
          ? std::shared_ptr<MemTableRepFactory>(new SkipListFactory)
          : EasyNewMemTableRep(osl ? "OffsetSkipList" : "CSPPMemTab",
                json({{"mem_cap", 16777216}, {"convert_to_sst", mode}}).dump());
      Destroy(options);
      DB* rejected = nullptr;
      ASSERT_TRUE(DB::Open(unsupported, dbname_, &rejected).IsInvalidArgument());
      ASSERT_EQ(rejected, nullptr);
      ASSERT_OK(TryReopen(options));
      ColumnFamilyHandle* handle = nullptr;
      ASSERT_TRUE(db_->CreateColumnFamily(unsupported, "mixed", &handle)
                      .IsInvalidArgument());
      ASSERT_EQ(handle, nullptr);
      Close();
      options.memtable_crash_safe_recover = false;
      options.avoid_flush_during_shutdown = true;
      unsupported.memtable_crash_safe_recover = false;
      ASSERT_OK(TryReopen(options));
      ASSERT_OK(db_->CreateColumnFamily(unsupported, "mixed", &handle));
      handles_.push_back(handle);
      ASSERT_OK(Put("file", "value"));
      ASSERT_OK(db_->Put(WriteOptions(), handle, "mixed", "value"));
      if (unsupported.memtable_factory->SupportConvertToSST()) {
        ASSERT_OK(db_->Flush(FlushOptions(), handle));
      }
      for (auto* cfd : *dbfull()->GetVersionSet()->GetColumnFamilySet()) {
        ASSERT_TRUE(cfd->GetMemTableFiles().empty());
      }
      Close();
      ASSERT_OK(TryReopenWithColumnFamilies(
          {"default", "mixed"}, std::vector<Options>{options, unsupported}));
      ASSERT_EQ(Get(0, "file"), "value");
      ASSERT_EQ(Get(1, "mixed"), "value");
      Close();
    }
  }
}

TEST_F(DBCsppCrashSafeTest, RecoverOffKeepsUnregisteredMemTableFiles) {
  Close();
  for (bool osl : {false, true}) {
    SCOPED_TRACE(osl);
    Options options = BaseCrashSafeOptions(dbname_, false, false);
    options.experimental_mempurge_threshold = 2.0;
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    int registrations = 0, waits = 0, inspected = 0;
    std::atomic<int> converts{0}, purges{0};
    SyncPoint::GetInstance()->SetCallBack(
        "MemTableRep::ConvertToSST:After", [&](void*) { ++converts; });
    for (const char* point : {"DBImpl::FlushJob:MemPurgeSuccessful",
                              "DBImpl::FlushJob:MemPurgeUnsuccessful"}) {
      SyncPoint::GetInstance()->SetCallBack(point, [&](void*) { ++purges; });
    }
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::RegisterMemTableFile:BeforeLogAndApply",
        [&](void*) { ++registrations; });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::SwitchMemtable:MemTableCacheMiss",
        [&](void*) { ++waits; });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(TryReopen(options));
    // Open reuses this hook for ordinary recovery MANIFEST writes.
    registrations = 0;
    ASSERT_OK(Put("sst", "1"));
    ASSERT_OK(Flush());
    ASSERT_GT(converts.load(), 0);
    ASSERT_EQ(purges.load(), 0);
    auto* cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    ASSERT_NE(cfd->PeekPrecreatedMemtable(), nullptr);
    const uint64_t cached = cfd->PeekPrecreatedMemtable()->GetFileNumber();
    ASSERT_OK(Put("imm", "2"));
    const uint64_t immutable = cfd->mem()->GetFileNumber();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::SwitchMemtable:BeforeInstallMemTable", [&](void*) {
          ASSERT_OK(db_->DisableFileDeletions());
          ASSERT_OK(db_->EnableFileDeletions(true));
          ASSERT_OK(env_->FileExists(MakeTableFileName(dbname_, cached)));
          ++inspected;
        });
    ASSERT_OK(dbfull()->TEST_SwitchMemtable());
    SyncPoint::GetInstance()->ClearCallBack(
        "DBImpl::SwitchMemtable:BeforeInstallMemTable");
    ASSERT_EQ(inspected, 1);
    ASSERT_EQ(cfd->mem()->GetFileNumber(), cached);
    ASSERT_OK(Put("active", "3"));
    // Refill through the ordinary cache producer to check all three live roles.
    cfd->PrepareNewMemtableInBackground(*cfd->GetLatestMutableCFOptions());
    auto* precreated = cfd->PeekPrecreatedMemtable();
    ASSERT_NE(precreated, nullptr);
    const uint64_t next = precreated->GetFileNumber();
    for (auto* cfd : *dbfull()->GetVersionSet()->GetColumnFamilySet()) {
      ASSERT_TRUE(cfd->GetMemTableFiles().empty());
    }
    ASSERT_OK(db_->DisableFileDeletions());
    ASSERT_OK(db_->EnableFileDeletions(true));
    for (uint64_t number : {immutable, cached, next}) {
      ASSERT_OK(env_->FileExists(MakeTableFileName(dbname_, number)));
    }
    ASSERT_EQ(registrations, 0);
    ASSERT_EQ(waits, 0);
    ASSERT_EQ(Get("imm"), "2");
    ASSERT_EQ(Get("active"), "3");
    Close();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("sst"), "1");
    ASSERT_EQ(Get("imm"), "2");
    ASSERT_EQ(Get("active"), "3");
    Close();
  }
}

TEST_F(DBCsppCrashSafeTest, ImmutableFactoryConvertMode) {
  Close();
  const SidePluginRepo repo;
  const json query = {{"html", false}};
  auto check = [&](const auto& factory, const auto* manip, const char* mode) {
    auto state = [&] {
      return json::parse(manip->ToString(*factory, query, repo));
    };
    auto update = [&](const json& body) {
      try {
        manip->HandleUpdate(factory.get(), query, body, repo);
        return Status::OK();
      } catch (const Status& s) {
        return s;
      }
    };
    ASSERT_EQ(state()["convert_to_sst"], mode);
    ASSERT_OK(update({{"token_use_idle", false}}));
    ASSERT_EQ(state()["token_use_idle"], false);
    ASSERT_OK(update({{"convert_to_sst", mode}}));
    const json before = state();
    for (const char* other : {"kDontConvert", "kDumpMem", "kFileMmap", "invalid"}) {
      if (std::string(other) == mode) continue;
      SCOPED_TRACE(other);
      Status s = update({{"convert_to_sst", other},
                         {"token_use_idle", true}, {"populate_read", false}});
      ASSERT_TRUE(s.IsInvalidArgument());
      ASSERT_EQ(state(), before);
    }
  };
  for (const char* mode : {"kDontConvert", "kDumpMem", "kFileMmap"}) {
    SCOPED_TRACE(mode);
    const json params = {{"mem_cap", 16777216}, {"convert_to_sst", mode}};
    for (const char* cls : {"CSPPMemTab", "OffsetSkipList"}) {
      SCOPED_TRACE(cls);
      auto factory = PluginFactorySP<MemTableRepFactory>::AcquirePlugin(
          cls, params, repo);
      ASSERT_EQ(factory->SupportCrashSafe(), std::string(mode) == "kFileMmap");
      auto* manip = PluginManip<MemTableRepFactory>::AcquirePlugin(cls, {}, repo);
      check(factory, manip, mode);
      InternalKeyComparator icmp(BytewiseComparator());
      MemTable::KeyComparator cmp(icmp);
      Arena arena;
      MutableCFOptions moptions(Options{});
      const std::string path = MakeTableFileName(dbname_, 900000);
      if (std::string(mode) == "kFileMmap") {
        ASSERT_DEATH(factory->CreateMemTableRep(
            "", moptions, cmp, &arena, nullptr, nullptr, 0), "memtable_file_path");
      }
      std::unique_ptr<MemTableRep> rep(factory->CreateMemTableRep(
          path, moptions, cmp, &arena, nullptr, nullptr, 0));
      ASSERT_EQ(enum_stdstr(rep->GetConvertKind()), mode);
      ASSERT_EQ(rep->SupportConvertToSST(), std::string(mode) != "kDontConvert");
      ASSERT_EQ(rep->SupportCrashSafe(), std::string(mode) == "kFileMmap");
      rep.reset();
      if (std::string(mode) == "kFileMmap") {
        ASSERT_OK(env_->DeleteFile(path));
      }
    }
    for (const char* cls : {"CSPPMemTabTable", "OffsetSkipListTable"}) {
      SCOPED_TRACE(cls);
      auto factory = PluginFactorySP<TableFactory>::AcquirePlugin(cls, params, repo);
      auto* manip = PluginManip<TableFactory>::AcquirePlugin(cls, {}, repo);
      check(factory, manip, mode);
    }
  }
}

TEST_F(DBCsppCrashSafeTest, RecoveredTableUsesFileSequenceBound) {
  Close();
  for (bool osl : {false, true}) {
    SCOPED_TRACE(osl ? "OSL" : "CSPP");
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    if (osl) {
      SetupOsl(&options, true);
    }
    Destroy(options);
    ASSERT_OK(env_->CreateDirIfMissing(dbname_));
    options.cf_paths = {{dbname_, 0}};
    InternalKeyComparator icmp(options.comparator);
    ImmutableOptions ioptions(options);
    MutableCFOptions moptions(options);
    WriteBufferManager wb(options.db_write_buffer_size);
    std::unique_ptr<MemTable> mem(new MemTable(
        icmp, ioptions, moptions, &wb, kMaxSequenceNumber, 0, 1));
    ASSERT_OK(mem->Add(1, kTypeValue, "key", "old", nullptr));
    ASSERT_OK(mem->Add(3, kTypeValue, "key", "new", nullptr));
    ASSERT_OK(mem->Add(4, kTypeValue, "ghost", "unpublished", nullptr));
    mem->MarkImmutable();
    const std::string leftover = MakeTableFileName(dbname_, 2);
    CopyFile(MakeTableFileName(dbname_, 1), leftover);
    mem.reset();

    IntTblPropCollectorFactories collectors;
    TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                            options.compression, options.compression_opts, 0,
                            kDefaultColumnFamilyName, 0);
    FileMetaData meta;
    meta.fd = FileDescriptor(2, 0, 0);
    meta.fd.smallest_seqno = 0;
    // The published bound need not be the sequence of any physical entry.
    meta.fd.largest_seqno = 2;
    ASSERT_OK(options.memtable_factory->RecoverCrashSafeMemTableToSST(
        leftover, &meta, tbo));
    ASSERT_GT(meta.fd.GetFileSize(), 0U);
    ASSERT_EQ(meta.fd.smallest_seqno, 0U);
    ASSERT_EQ(meta.fd.largest_seqno, 2U);

    const std::string fname = TableFileName(options.cf_paths, 2, 0);
    for (SequenceNumber limit : {meta.fd.largest_seqno, SequenceNumber(4),
                                 SequenceNumber(0), kMaxSequenceNumber}) {
      SCOPED_TRACE(limit);
      std::unique_ptr<FSRandomAccessFile> file;
      ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(
          fname, FileOptions(), &file, nullptr));
      std::unique_ptr<RandomAccessFileReader> reader(
          new RandomAccessFileReader(std::move(file), fname));
      EnvOptions env_options;
      TableReaderOptions tro(ioptions, options.prefix_extractor, env_options,
                             icmp, 0);
      // The file header owns visibility, independently of the caller's bound.
      FileDescriptor fd = meta.fd;
      fd.largest_seqno = limit;
      tro.largest_seqno = fd.largest_seqno;
      if (osl) {
        const auto block = ReadMetaBlockE(
            reader.get(), fd.GetFileSize(), 0x62546d654d4c534fULL, ioptions,
            "OffsetSkipList");
        ASSERT_EQ(block.data.size(), 48U);
        // Packed metadata has a 40-byte prefix followed by uint64_t pubseq.
        uint64_t pubseq;
        memcpy(&pubseq, block.data.data() + 40, sizeof(pubseq));
        ASSERT_EQ(pubseq, 2U);
      }
      std::unique_ptr<TableReader> table;
      ASSERT_OK(options.table_factory->NewTableReader(
          ReadOptions(), tro, std::move(reader), fd.GetFileSize(), &table,
          true));
      const auto& compression = table->GetTableProperties()->compression_options;
      ASSERT_EQ(compression.substr(0, compression.find(';', 1)), ";pubseq:2");
      const auto* view = dynamic_cast<const TopTableReaderBase*>(table.get());
      ASSERT_NE(view, nullptr);
      ASSERT_EQ(json::parse(view->ToWebViewString({{"html", false}}))["pubseq"], 2);
      for (const char* key : {"key", "ghost"}) {
        PinnableSlice value;
        GetContext get_context(
            options.comparator, nullptr, nullptr, nullptr, GetContext::kNotFound,
            key, &value, nullptr, nullptr, nullptr, true, nullptr, nullptr);
        InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
        ASSERT_OK(table->Get(ReadOptions(), ikey.Encode(), &get_context,
                             nullptr));
        const bool visible = key[0] == 'k';
        ASSERT_EQ(get_context.State(),
                  visible ? GetContext::kFound : GetContext::kNotFound);
        if (visible) {
          ASSERT_EQ(value.ToString(), "old");
        }
      }
    }
    SstFileReader standalone(options);
    ASSERT_OK(standalone.Open(fname));
    std::unique_ptr<Iterator> standalone_it(standalone.NewIterator(ReadOptions()));
    standalone_it->SeekToFirst();
    ASSERT_TRUE(standalone_it->Valid());
    ASSERT_EQ(standalone_it->key().ToString(), "key");
    ASSERT_EQ(standalone_it->value().ToString(), "old");
    standalone_it->Next();
    ASSERT_FALSE(standalone_it->Valid());
    ASSERT_OK(standalone_it->status());
    SstFileDumper dumper(options, fname, Temperature::kUnknown, 0,
                         true, false, false, EnvOptions(), true);
    ASSERT_OK(dumper.getStatus());
    ASSERT_OK(dumper.ReadSequential(false, 0, false, "", false, ""));
    ASSERT_EQ(dumper.GetReadNumber(), 1U);
  }
}

TEST_F(DBCsppCrashSafeTest, RecoveredTableIteratorUsesFileSequenceBound) {
  Close();
  for (bool osl : {false, true}) {
    for (bool reverse : {false, true}) {
      SCOPED_TRACE(osl ? "OSL" : "CSPP");
      SCOPED_TRACE(reverse);
      Options options = BaseCrashSafeOptions(dbname_, true, false);
      if (osl) SetupOsl(&options, true);
      if (reverse) options.comparator = ReverseBytewiseComparator();
      Destroy(options);
      ASSERT_OK(env_->CreateDirIfMissing(dbname_));
      options.cf_paths = {{dbname_, 0}};
      InternalKeyComparator icmp(options.comparator);
      ImmutableOptions ioptions(options);
      MutableCFOptions moptions(options);
      WriteBufferManager wb(options.db_write_buffer_size);
      auto mem = std::make_unique<MemTable>(
          icmp, ioptions, moptions, &wb, kMaxSequenceNumber, 0, 1);
      std::vector<std::string> physical;
      for (const auto& entry : {std::make_pair("b", 1), {"d", 2}, {"b", 3},
                                {"d", 5}, {"c", 6}, {"b", 7}, {"a", 8},
                                {"z", 9}}) {
        ASSERT_OK(mem->Add(entry.second, kTypeValue, entry.first,
                           std::to_string(entry.second), nullptr));
        physical.push_back(
            InternalKey(entry.first, entry.second, kTypeValue).Encode().ToString());
      }
      auto less = [&](const std::string& a, const std::string& b) {
        return icmp.Compare(a, b) < 0;
      };
      std::sort(physical.begin(), physical.end(), less);
      mem->MarkImmutable();
      const std::string leftover = MakeTableFileName(dbname_, 2);
      CopyFile(MakeTableFileName(dbname_, 1), leftover);
      mem.reset();
      IntTblPropCollectorFactories collectors;
      TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                              options.compression, options.compression_opts, 0,
                              kDefaultColumnFamilyName, 0);
      FileMetaData meta;
      meta.fd = FileDescriptor(2, 0, 0);
      meta.fd.smallest_seqno = 0;
      meta.fd.largest_seqno = 4;
      ASSERT_OK(options.memtable_factory->RecoverCrashSafeMemTableToSST(
          leftover, &meta, tbo));
      const std::string fname = TableFileName(options.cf_paths, 2, 0);
      for (SequenceNumber limit : {SequenceNumber(0), SequenceNumber(4),
                                   SequenceNumber(9), kMaxSequenceNumber}) {
        SCOPED_TRACE(limit);
        std::unique_ptr<FSRandomAccessFile> file;
        ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(
            fname, FileOptions(), &file, nullptr));
        auto reader = std::make_unique<RandomAccessFileReader>(std::move(file), fname);
        EnvOptions env_options;
        TableReaderOptions tro(ioptions, options.prefix_extractor, env_options,
                               icmp, 0);
        tro.largest_seqno = limit;
        if (osl) {
          const auto block = ReadMetaBlockE(
              reader.get(), meta.fd.GetFileSize(), 0x62546d654d4c534fULL,
              ioptions, "OffsetSkipList");
          ASSERT_EQ(block.data.size(), 48U);
          uint64_t pubseq;
          memcpy(&pubseq, block.data.data() + 40, sizeof(pubseq));
          ASSERT_EQ(pubseq, 4U);
        }
        std::unique_ptr<TableReader> table;
        ASSERT_OK(options.table_factory->NewTableReader(
            ReadOptions(), tro, std::move(reader), meta.fd.GetFileSize(),
            &table, true));
        const auto& compression = table->GetTableProperties()->compression_options;
        ASSERT_EQ(compression.substr(0, compression.find(';', 1)), ";pubseq:4");
        const auto* view = dynamic_cast<const TopTableReaderBase*>(table.get());
        ASSERT_NE(view, nullptr);
        ASSERT_EQ(json::parse(view->ToWebViewString({{"html", false}}))["pubseq"], 4);
        std::vector<std::string> expected;
        for (const auto& key : physical) {
          if (GetInternalKeySeqno(key) <= 4) expected.push_back(key);
        }
        for (bool use_arena : {false, true}) {
          SCOPED_TRACE(use_arena);
          Arena arena;
          auto destroy = [&](InternalIterator* p) {
            if (use_arena) p->~InternalIterator();
            else delete p;
          };
          std::unique_ptr<InternalIterator, decltype(destroy)> it(
              table->NewIterator(ReadOptions(), nullptr,
                                 use_arena ? &arena : nullptr, false,
                                 TableReaderCaller::kUserIterator), destroy);
          auto check = [&](size_t pos) {
            ASSERT_EQ(it->Valid(), pos < expected.size());
            if (it->Valid()) {
              ASSERT_EQ(it->key().ToString(), expected[pos]);
              ASSERT_EQ(it->value().ToString(),
                        std::to_string(GetInternalKeySeqno(expected[pos])));
            }
          };
          // Exercise every forward entry point, including the fast result API.
          for (int advance = 0; advance < 3; ++advance) {
            it->SeekToFirst();
            for (size_t i = 0; i < expected.size(); ++i) {
              check(i);
              ASSERT_TRUE(it->Valid());
              if (advance == 0) {
                it->Next();
              } else if (advance == 1) {
                ASSERT_EQ(it->NextAndCheckValid(), i + 1 < expected.size());
              } else {
                IterateResult result;
                ASSERT_EQ(it->NextAndGetResult(&result), i + 1 < expected.size());
                ASSERT_EQ(result.is_valid, i + 1 < expected.size());
                if (result.is_valid) {
                  ASSERT_EQ(result.key().ToString(), expected[i + 1]);
                }
              }
            }
            check(expected.size());
          }
          for (bool fast : {false, true}) {
            it->SeekToLast();
            for (size_t i = expected.size(); i > 0; --i) {
              check(i - 1);
              ASSERT_TRUE(it->Valid());
              if (fast) {
                ASSERT_EQ(it->PrevAndCheckValid(), i > 1);
              } else {
                it->Prev();
              }
            }
            check(expected.size());
          }
          auto* mem_it = static_cast<MemTableRep::Iterator*>(it.get());
          std::vector<std::string> targets = physical;
          for (const char* key : {"", "a", "b", "bb", "c", "d", "zz"}) {
            for (SequenceNumber seq : {SequenceNumber(0), SequenceNumber(4),
                                        kMaxSequenceNumber}) {
              targets.push_back(InternalKey(key, seq, kTypeValue).Encode().ToString());
            }
          }
          for (const auto& target : targets) {
            const size_t next = std::lower_bound(
                expected.begin(), expected.end(), target, less) - expected.begin();
            const size_t upper = std::upper_bound(
                expected.begin(), expected.end(), target, less) - expected.begin();
            const size_t prev = upper ? upper - 1 : expected.size();
            it->Seek(target);
            check(next);
            it->SeekForPrev(target);
            check(prev);
            std::string encoded;
            const char* memkey = EncodeKey(&encoded, target);
            mem_it->Seek(target, memkey);
            check(next);
            mem_it->SeekForPrev(target, memkey);
            check(prev);
          }
          if (osl) {
            for (int i = 0; i < 20; ++i) {
              mem_it->RandomSeek();
              if (it->Valid()) {
                ASSERT_LE(GetInternalKeySeqno(it->key()), 4U);
              }
            }
          }
        }
      }
    }
  }
}

TEST_F(DBCsppCrashSafeTest, ConvertedTableUsesUnboundedHeaderSequence) {
  Close();
  for (bool osl : {false, true}) {
    for (bool file_mmap : {false, true}) {
      SCOPED_TRACE(osl ? "OSL" : "CSPP");
      SCOPED_TRACE(file_mmap);
      Options options = BaseCrashSafeOptions(dbname_, true, false);
      const char* js = file_mmap
          ? R"({"mem_cap":16777216,"convert_to_sst":"kFileMmap"})"
          : R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem"})";
      if (osl) {
        options.memtable_factory = EasyNewMemTableRep("OffsetSkipList", js);
      } else {
        options.memtable_factory.reset(NewCSPPMemTabForPlain(js));
      }
      const SidePluginRepo repo;
      options.table_factory = PluginFactorySP<TableFactory>::AcquirePlugin(
          osl ? "OffsetSkipListTable" : "CSPPMemTabTable", json::parse(js), repo);
      Destroy(options);
      ASSERT_OK(env_->CreateDirIfMissing(dbname_));
      options.cf_paths = {{dbname_, 0}};
      InternalKeyComparator icmp(options.comparator);
      ImmutableOptions ioptions(options);
      MutableCFOptions moptions(options);
      WriteBufferManager wb(options.db_write_buffer_size);
      auto mem = std::make_unique<MemTable>(
          icmp, ioptions, moptions, &wb, kMaxSequenceNumber, 0, 1);
      ASSERT_OK(mem->Add(1, kTypeValue, "a", "1", nullptr));
      ASSERT_OK(mem->Add(2, kTypeValue, "b", "2", nullptr));
      ASSERT_OK(mem->Add(3, kTypeValue, "b", "3", nullptr));
      mem->MarkImmutable();
      IntTblPropCollectorFactories collectors;
      TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                              options.compression, options.compression_opts, 0,
                              kDefaultColumnFamilyName, 0);
      FileMetaData meta;
      meta.fd = FileDescriptor(1, 0, 0);
      meta.fd.smallest_seqno = 1;
      meta.fd.largest_seqno = 3;
      ASSERT_OK(mem->ConvertToSST(&meta, tbo));
      ASSERT_GT(meta.fd.GetFileSize(), 0U);
      mem.reset();

      const std::string fname = TableFileName(options.cf_paths, 1, 0);
      if (!osl && !file_mmap) {
        const int fd = ::open(fname.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        terark::DFA_MmapHeader header{};
        const ssize_t read = ::pread(fd, &header, sizeof(header), 0);
        ::close(fd);
        ASSERT_EQ(read, static_cast<ssize_t>(sizeof(header)));
        uint32_t prefix[3];
        memcpy(prefix, header.reserved, sizeof(prefix));
        ASSERT_EQ(prefix[0], 0x50505343U);  // CSPP crash-safe magic.
        ASSERT_EQ(prefix[1], 0U);  // No WAL references in this conversion.
        ASSERT_EQ(prefix[2], 0U);
        // pubseq follows the 16-byte prefix and sixteen 24-byte WAL slots.
        uint64_t pubseq;
        memcpy(&pubseq, header.reserved + 400, sizeof(pubseq));
        ASSERT_EQ(pubseq, 0U);  // Unbounded ordinary conversion.
        ASSERT_GE(header.crc32cLevel, 1U);
        ASSERT_EQ(header.header_crc32,
                  terark::Crc32c_update(0, &header, sizeof(header) - 4));
      }
      const std::type_info* unfiltered_type = nullptr;
      for (SequenceNumber limit : {kMaxSequenceNumber, SequenceNumber(0),
                                   SequenceNumber(2), meta.fd.largest_seqno}) {
        std::unique_ptr<FSRandomAccessFile> file;
        ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(
            fname, FileOptions(), &file, nullptr));
        auto reader = std::make_unique<RandomAccessFileReader>(std::move(file), fname);
        EnvOptions env_options;
        TableReaderOptions tro(ioptions, options.prefix_extractor, env_options,
                               icmp, 0);
        tro.largest_seqno = limit;
        if (osl) {
          const auto block = ReadMetaBlockE(
              reader.get(), meta.fd.GetFileSize(), 0x62546d654d4c534fULL,
              ioptions, "OffsetSkipList");
          ASSERT_EQ(block.data.size(), 48U);
          uint64_t pubseq;
          memcpy(&pubseq, block.data.data() + 40, sizeof(pubseq));
          ASSERT_EQ(pubseq, 0U);
        }
        std::unique_ptr<TableReader> table;
        ASSERT_OK(options.table_factory->NewTableReader(
            ReadOptions(), tro, std::move(reader), meta.fd.GetFileSize(),
            &table, true));
        const auto& compression = table->GetTableProperties()->compression_options;
        ASSERT_EQ(compression.find("pubseq:"), std::string::npos);
        ASSERT_EQ(compression.find("VisFilter:"), std::string::npos);
        const auto* view = dynamic_cast<const TopTableReaderBase*>(table.get());
        ASSERT_NE(view, nullptr);
        ASSERT_EQ(json::parse(view->ToWebViewString({{"html", false}}))["pubseq"],
                  "kMaxSequenceNumber");
        std::unique_ptr<InternalIterator> it(table->NewIterator(
            ReadOptions(), nullptr, nullptr, false,
            TableReaderCaller::kUserIterator));
        if (limit == kMaxSequenceNumber) {
          unfiltered_type = &typeid(*it);
        } else {
          ASSERT_NE(unfiltered_type, nullptr);
          // A caller's finite maximum must not select VisibleIter.
          ASSERT_EQ(typeid(*it), *unfiltered_type);
        }
        size_t count = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()) ++count;
        ASSERT_EQ(count, 3U);
      }
    }
  }
}

TEST_F(DBCsppCrashSafeTest, CsppSelfMmapUnmapsWholeFile) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_OK(env_->CreateDirIfMissing(dbname_));
  options.cf_paths = {{dbname_, 0}};
  InternalKeyComparator icmp(options.comparator);
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(new MemTable(
      icmp, ioptions, moptions, &wb, kMaxSequenceNumber, 0, 1));
  ASSERT_OK(mem->Add(1, kTypeValue, "key", "value", nullptr));
  const std::string path = MakeTableFileName(dbname_, 2);
  CopyFile(MakeTableFileName(dbname_, 1), path);
  mem.reset();
  const int fd = ::open(path.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  terark::DFA_MmapHeader hdr{};
  ASSERT_EQ(::pread(fd, &hdr, sizeof(hdr), 0),
            static_cast<ssize_t>(sizeof(hdr)));
  ASSERT_EQ(::ftruncate(fd, hdr.file_size + 2 * ::sysconf(_SC_PAGESIZE)), 0);
  const size_t physical_size = hdr.file_size + 2 * ::sysconf(_SC_PAGESIZE);
  const auto original = hdr;
  std::vector<uint64_t> buffer((physical_size + 7) / 8);
  ASSERT_EQ(::pread(fd, buffer.data(), physical_size, 0),
            static_cast<ssize_t>(physical_size));
  auto check_unmapped = [&] {
    std::ifstream maps("/proc/self/maps");
    ASSERT_TRUE(maps.good());
    for (std::string line; std::getline(maps, line);) {
      EXPECT_EQ(line.find(path), std::string::npos) << "leaked mapping: " << line;
    }
  };
  for (int entry = 0; entry < 4; ++entry) {
    // self(path), self(fd), load(path), load(fd).
    for (int damage = 0; damage < 7; ++damage) {
      SCOPED_TRACE(entry);
      SCOPED_TRACE(damage);
      if (damage == 6 && entry < 2) continue;  // load-only format check.
      hdr = original;
      size_t length = physical_size;
      if (damage == 1) hdr.num_blocks = 0;  // finish_load_mmap failure.
      if (damage == 2) length = 0;
      if (damage == 3) length = sizeof(hdr) - 1;
      if (damage == 4) hdr.file_size = sizeof(hdr) - 1;
      if (damage == 5) hdr.file_size = physical_size + 1;
      if (damage == 6) hdr.magic[0] = '!';
      ASSERT_EQ(::ftruncate(fd, physical_size), 0);
      ASSERT_EQ(::pwrite(fd, buffer.data(), physical_size, 0),
                static_cast<ssize_t>(physical_size));
      ASSERT_EQ(::pwrite(fd, &hdr, sizeof(hdr), 0),
                static_cast<ssize_t>(sizeof(hdr)));
      ASSERT_EQ(::ftruncate(fd, length), 0);
      auto open = [&] {
        if (entry < 2) {
          terark::MainPatricia trie(0, 16 << 20,
                                    terark::Patricia::NoWriteReadOnly);
          if (entry == 0) trie.self_mmap(path);
          else trie.self_mmap(fd, false);
          EXPECT_EQ(trie.get_mmap().size(), original.file_size);
        } else {
          std::unique_ptr<terark::BaseDFA> trie(entry == 2
              ? terark::BaseDFA::load_mmap(path, false)
              : terark::BaseDFA::load_mmap(fd));
          EXPECT_EQ(trie->get_mmap().size(), original.file_size);
        }
      };
      if (damage == 0) {
        ASSERT_NO_THROW(open());
      } else {
        try {
          open();
          FAIL() << "expected invalid_argument";
        } catch (const std::invalid_argument& ex) {
          if ((entry == 0 || entry == 2) && damage >= 2 && damage <= 5) {
            EXPECT_NE(std::string(ex.what()).find(path), std::string::npos);
          }
        }
      }
      ASSERT_NE(::fcntl(fd, F_GETFD), -1);  // Caller retains its descriptor.
      check_unmapped();
    }
  }
  for (bool load : {false, true}) {
    for (int damage = 0; damage < 5; ++damage) {
      SCOPED_TRACE(load);
      SCOPED_TRACE(damage);
      auto* header = reinterpret_cast<terark::DFA_MmapHeader*>(buffer.data());
      *header = original;
      const void* data = buffer.data();
      size_t length = physical_size;
      if (damage == 1) { data = nullptr; length = 0; }
      if (damage == 2) length = sizeof(original) - 1;
      if (damage == 3) header->file_size = sizeof(original) - 1;
      if (damage == 4) header->file_size = physical_size + 1;
      buffer.back() = 0x87654321;
      auto borrow = [&] {
        if (load) {
          std::unique_ptr<terark::BaseDFA> trie(
              terark::BaseDFA::load_mmap_user_mem(data, length));
          ASSERT_NE(trie, nullptr);
          EXPECT_EQ(trie->get_mmap().size(), original.file_size);
        } else {
          terark::MainPatricia trie(0, 16 << 20,
                                    terark::Patricia::NoWriteReadOnly);
          trie.self_mmap_user_mem(data, length);
          EXPECT_EQ(trie.get_mmap().size(), original.file_size);
        }
      };
      if (damage == 0) {
        ASSERT_NO_THROW(borrow());
      } else {
        ASSERT_THROW(borrow(), std::invalid_argument);
      }
      // Borrowers neither free the buffer nor alter its logical or extra tail.
      ASSERT_EQ(header->file_size, damage == 3 ? sizeof(original) - 1
                                  : damage == 4 ? physical_size + 1
                                                : original.file_size);
      ASSERT_EQ(buffer.back(), 0x87654321U);
      buffer.back() = 0x12345678;
      ASSERT_EQ(buffer.back(), 0x12345678U);
    }
  }
  ::close(fd);
}

TEST_F(DBCsppCrashSafeTest, OslRecoveryUnmapsWholeFile) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SetupOsl(&options, true);
  Destroy(options);
  ASSERT_OK(env_->CreateDirIfMissing(dbname_));
  options.cf_paths = {{dbname_, 0}};
  InternalKeyComparator icmp(options.comparator);
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(new MemTable(
      icmp, ioptions, moptions, &wb, kMaxSequenceNumber, 0, 1));
  ASSERT_OK(mem->Add(1, kTypeValue, "key", "value", nullptr));
  const std::string leftover = MakeTableFileName(dbname_, 2);
  CopyFile(MakeTableFileName(dbname_, 1), leftover);
  mem.reset();
  IntTblPropCollectorFactories collectors;
  TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                          options.compression, options.compression_opts, 0,
                          kDefaultColumnFamilyName, 0);
  FileMetaData meta;
  meta.fd = FileDescriptor(2, 0, 0);
  meta.fd.smallest_seqno = 0;
  meta.fd.largest_seqno = 1;
  ASSERT_OK(options.memtable_factory->RecoverCrashSafeMemTableToSST(
      leftover, &meta, tbo));
  std::ifstream maps("/proc/self/maps");
  ASSERT_TRUE(maps.good());
  const auto fname = TableFileName(options.cf_paths, 2, 0);
  for (std::string line; std::getline(maps, line);) {
    EXPECT_EQ(line.find(fname), std::string::npos) << "leaked mapping: " << line;
  }
}

#if !defined(OS_WIN)
TEST_F(CrashChild, DISABLED_SecondCrashSeed) {
  ASSERT_EQ(arg_.size(), 3U);
  Options options = BaseCrashSafeOptions(dbname_, true, arg_[1] == '1');
  if (arg_[0] == '1') SetupOsl(&options, true);
  DB* db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  ASSERT_OK(db->Put(WriteOptions(), "a", "1"));
  ASSERT_OK(static_cast<DBImpl*>(db)->TEST_SwitchMemtable());
  ASSERT_OK(db->Put(WriteOptions(), "b", "2"));
  ASSERT_OK(static_cast<DBImpl*>(db)->TEST_SwitchMemtable());
  ASSERT_OK(db->Put(WriteOptions(), "c", "3"));
  ::_exit(42);
}

TEST_F(CrashChild, DISABLED_SecondCrashRecover) {
  ASSERT_EQ(arg_.size(), 3U);
  Options options = BaseCrashSafeOptions(dbname_, true, arg_[1] == '1');
  if (arg_[0] == '1') SetupOsl(&options, true);
  SyncPoint::GetInstance()->SetCallBack(
      "CrashSafeRecover::ConvertToSST:InjectStatus", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("review: failed conversion");
      });
  SyncPoint::GetInstance()->SetCallBack(
      arg_[2] == '1' ? "DBImpl::Open:Opened"
                     : "DBImpl::RecoverLogFiles:BeforeReadWal",
      [](void*) { ::_exit(43); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* db = nullptr;
  DB::Open(options, dbname_, &db).PermitUncheckedError();
  ::_exit(17);
}
#endif

TEST_F(DBCsppCrashSafeTest, SecondCrashAfterConvertFailure) {
  Close();
  for (bool osl : {false, true}) {
    for (bool log_index : {false, true}) {
      for (bool after_open : {false, true}) {
        SCOPED_TRACE(osl ? "OSL" : "CSPP");
        SCOPED_TRACE(log_index);
        SCOPED_TRACE(after_open);
        Options options = BaseCrashSafeOptions(dbname_, true, log_index);
        if (osl) SetupOsl(&options, true);
        Destroy(options);
        const std::string child_options =
            std::to_string(osl) + std::to_string(log_index) +
            std::to_string(after_open);
        ASSERT_EQ(RunCrashChild(dbname_, "SecondCrashSeed", child_options), 42);
        ASSERT_GE(ListLeftovers(options, dbname_).size(), 3U);
        ASSERT_EQ(RunCrashChild(dbname_, "SecondCrashRecover", child_options), 43);
        PublishedSeqOnDisk rec;
        ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
        // Recovery does not invalidate a valid published cursor: registered
        // source files survive conversion failure and can be retried.
        ASSERT_EQ(rec.generation & 1, 0U);
        ASSERT_OK(TryReopen(options));
        EXPECT_EQ(Get("a"), "1");
        EXPECT_EQ(Get("b"), "2");
        EXPECT_EQ(Get("c"), "3");
        ASSERT_OK(Put("after", "recovery"));
        ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
        ASSERT_EQ(rec.generation & 1, 0U);
        Reopen(options);
        EXPECT_EQ(Get("a"), "1");
        EXPECT_EQ(Get("b"), "2");
        EXPECT_EQ(Get("c"), "3");
        EXPECT_EQ(Get("after"), "recovery");
        Close();
      }
    }
  }
}

#if !defined(OS_WIN)
TEST_F(CrashChild, DISABLED_PatriciaSingleWriterMmapConstructor) {
  const auto mode = static_cast<decltype(terark::Patricia::SingleThreadStrict)>(
      std::stoi(arg_));
  const std::string path = dbname_ + "/review-patricia-" + arg_;
  alignas(terark::MainPatricia) unsigned char storage[sizeof(terark::MainPatricia)];
  std::memset(storage, 0xa5, sizeof(storage));
  auto* trie = new (storage) terark::MainPatricia(
      4, 16 << 20, mode, terark::fstring(path));
  trie->~MainPatricia();
  ::_exit(0);
}
#endif

TEST_F(DBCsppCrashSafeTest, PatriciaSingleWriterMmapConstructor) {
  Close();
  ASSERT_OK(env_->CreateDirIfMissing(dbname_));
  for (auto mode : {terark::Patricia::SingleThreadStrict,
                    terark::Patricia::SingleThreadShared,
                    terark::Patricia::OneWriteMultiRead}) {
    SCOPED_TRACE(int(mode));
    ASSERT_EQ(RunCrashChild(dbname_, "PatriciaSingleWriterMmapConstructor",
                            std::to_string(int(mode))), 0);
  }
}

TEST_F(DBCsppCrashSafeTest, RecoverOffDoesNotCreatePublishedSeq) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, false, false);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  ASSERT_TRUE(env_->FileExists(CrashSafePubSeqFileName(dbname_)).IsNotFound());
}

TEST_F(DBCsppCrashSafeTest, RecoverOnCreatesPublishedSeqAndFlushWalBuffer) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.manual_wal_flush = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_FALSE(dbfull()->GetDBOptions().manual_wal_flush);
  ASSERT_OK(Put("k", "v"));
  ASSERT_TRUE(dbfull()->WALBufferIsEmpty());
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.generation % 2, 0U);
  ASSERT_GT(rec.pubseq, 0U);
  ASSERT_GT(rec.wal_offset, 0U);
  Close();
  ASSERT_OK(env_->FileExists(CrashSafePubSeqFileName(dbname_)));
}

TEST_F(DBCsppCrashSafeTest, SkipListRejectsRecoverAndUsesWalWhenDisabled) {
  Close();
  Options options = CurrentOptions();
  options.memtable_factory = std::make_shared<SkipListFactory>();
  options.memtable_crash_safe_recover = true;
  options.create_if_missing = true;
  Destroy(options);
  ASSERT_TRUE(TryReopen(options).IsInvalidArgument());
  ASSERT_EQ(db_, nullptr);
  options.memtable_crash_safe_recover = false;
  options.avoid_flush_during_shutdown = true;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  Close();
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("a"), "1");
}

#if !defined(OS_WIN)
static Options LogRefCrashOptions(const std::string& dbname,
                                  const std::string& config) {
  Options options = BaseCrashSafeOptions(dbname, true, true);
  const bool osl = config[0] == 'O';
  const json params = {
      {"mem_cap", 16777216}, {"convert_to_sst", "kFileMmap"},
      {"log_ref_format", config[1] == 'P' ? "kPlainLogRef" : "kShortLogRef"}};
  options.memtable_factory = EasyNewMemTableRep(
      osl ? "OffsetSkipList" : "CSPPMemTab", params.dump());
  const SidePluginRepo repo;
  options.table_factory = PluginFactorySP<TableFactory>::AcquirePlugin(
      osl ? "OffsetSkipListTable" : "CSPPMemTabTable", params, repo);
  return options;
}

TEST_F(CrashChild, DISABLED_LogRefRecoveryIgnoresCounters) {
  Options options = LogRefCrashOptions(dbname_, arg_);
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  const size_t count = arg_[2] == 'L' ? 5000 : 3;
  for (size_t i = 0; i < count; ++i) {
    ASSERT_OK(child_db->Put(WriteOptions(), std::to_string(i % 2),
                            std::string(128, 'v')));
  }
  ASSERT_OK(child_db->Put(WriteOptions(), "inline", "v"));
  auto* cfd = static_cast<DBImpl*>(child_db)->GetVersionSet()
                  ->GetColumnFamilySet()->GetDefault();
  ASSERT_OK(WriteStringToFile(
      options.env, MakeTableFileName(dbname_, cfd->mem()->GetFileNumber()),
      dbname_ + "/active-file"));
  ::_exit(42);
}

TEST_F(DBCsppCrashSafeTest, LogRefRecoveryIgnoresCounters) {
  for (const char* config : {"CPS", "CPL", "CSS", "CSL",
                             "OPS", "OPL", "OSS", "OSL"}) {
    SCOPED_TRACE(config);
    Close();
    Options options = LogRefCrashOptions(dbname_, config);
    Destroy(options);
    ASSERT_EQ(RunCrashChild(dbname_, "LogRefRecoveryIgnoresCounters", config), 42);
    const auto leftovers = ListLeftovers(options, dbname_);
    ASSERT_EQ(leftovers.size(), 2U);
    std::string active;
    ASSERT_OK(ReadFileToString(env_, dbname_ + "/active-file", &active));
    ASSERT_EQ(std::count(leftovers.begin(), leftovers.end(), active), 1);
    const int fd = ::open(active.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    // Header statistics are not a source of truth for WAL references.
    const size_t offset = config[0] == 'O'
        ? offsetof(terark::OSL_MmapHeader, reserved) + 2 * sizeof(uint32_t)
        : offsetof(terark::DFA_MmapHeader, reserved) + 4 * sizeof(uint32_t);
    uint64_t wal[3];  // fileno, cnt, bytes
    const ssize_t n = ::pread(fd, wal, sizeof(wal), offset);
    ASSERT_EQ(n, static_cast<ssize_t>(sizeof(wal)));
    if (config[2] == 'S') {
      // Simulate a crash before TLS statistics were flushed to the mapped header.
      // A zero approximate count must not discard the real WAL references.
      wal[1] = 0;
      wal[2] = 0;
      ASSERT_EQ(::pwrite(fd, wal, sizeof(wal), offset),
                static_cast<ssize_t>(sizeof(wal)));
    }
    ::close(fd);
    ASSERT_NE(wal[0], 0U);
    for (int reopen = 0; reopen < 2; ++reopen) {
      ASSERT_OK(TryReopen(options));
      ASSERT_GT(CountL0(db_), 0);
      ColumnFamilyMetaData cf_meta;
      db_->GetColumnFamilyMetaData(&cf_meta);
      ASSERT_EQ(cf_meta.blob_files.size(), 1U);
      ASSERT_EQ(cf_meta.blob_files[0].total_blob_count,
                std::max<uint64_t>(wal[1], 1));
      ASSERT_EQ(cf_meta.blob_files[0].total_blob_bytes,
                std::max<uint64_t>(wal[2], 1));
      ASSERT_EQ(Get("0"), std::string(128, 'v'));
      ASSERT_EQ(Get("1"), std::string(128, 'v'));
      ASSERT_EQ(Get("inline"), "v");
      Close();
    }
  }
}

TEST_F(CrashChild, DISABLED_LogRefRecoveryMultipleWals) {
  Options options = LogRefCrashOptions(dbname_, arg_);
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  Options auxiliary = options;
  ColumnFamilyHandle* handle = nullptr;
  ASSERT_OK(child_db->CreateColumnFamily(auxiliary, "rotate", &handle));
  auto* impl = static_cast<DBImpl*>(child_db);
  auto* cfd = impl->GetVersionSet()->GetColumnFamilySet()->GetColumnFamily(
      handle->GetID());
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(child_db->Put(WriteOptions(), std::to_string(i),
                            std::string(128, 'a' + i)));
    if (i != 2) {
      ASSERT_OK(impl->TEST_SwitchMemtable(cfd));
    }
  }
  // Only the auxiliary CF switches: all three WAL slots belong to one primary
  // memtable, exercising the parallel mapping array rather than three memtables.
  ::_exit(42);
}

TEST_F(DBCsppCrashSafeTest, LogRefRecoveryMultipleWals) {
  for (const char* config : {"CPS", "CSS", "OPS", "OSS"}) {
    SCOPED_TRACE(config);
    Close();
    Options options = LogRefCrashOptions(dbname_, config);
    Destroy(options);
    ASSERT_EQ(RunCrashChild(dbname_, "LogRefRecoveryMultipleWals", config), 42);
    const auto leftovers = ListLeftovers(options, dbname_);
    // The empty rotating CF is now FileMmap too; its registered sources follow
    // the primary CF's earlier file number in the complete inventory.
    ASSERT_GE(leftovers.size(), 2U);
    const int fd = ::open(leftovers.front().c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    uint32_t num_wals = 0;
    const size_t num_wals_offset = config[0] == 'O'
        ? offsetof(terark::OSL_MmapHeader, reserved) + sizeof(uint32_t)
        : offsetof(terark::DFA_MmapHeader, reserved) + 2 * sizeof(uint32_t);
    ASSERT_EQ(::pread(fd, &num_wals, sizeof(num_wals),
                      num_wals_offset),
              static_cast<ssize_t>(sizeof(num_wals)));
    ::close(fd);
    ASSERT_EQ(num_wals, 3U);
    for (int reopen = 0; reopen < 2; ++reopen) {
      ASSERT_OK(TryReopenWithColumnFamilies({"default", "rotate"}, options));
      ASSERT_EQ(CountL0(db_), 1);
      ColumnFamilyMetaData meta;
      db_->GetColumnFamilyMetaData(&meta);
      ASSERT_EQ(meta.blob_files.size(), 3U);
      std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
      it->SeekToFirst();
      for (int i = 0; i < 3; ++i) {
        const std::string value(128, 'a' + i);
        ASSERT_EQ(Get(0, std::to_string(i)), value);
        ASSERT_TRUE(it->Valid());
        ASSERT_EQ(it->key().ToString(), std::to_string(i));
        ASSERT_EQ(it->value().ToString(), value);
        it->Next();
      }
      ASSERT_FALSE(it->Valid());
      ASSERT_OK(it->status());
      it.reset();
      Close();
    }
  }
}

TEST_F(CrashChild, DISABLED_ChangedFactoryWithOtherCfLeftover) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Options other = options;
  if (arg_ == "OSL") {
    SetupOsl(&options, true);
  } else {
    SetupOsl(&other, true);
  }
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ColumnFamilyHandle* handle = nullptr;
  ASSERT_OK(child_db->CreateColumnFamily(other, "other", &handle));
  ASSERT_OK(child_db->Put(WriteOptions(), "key", "primary"));
  ASSERT_OK(child_db->Put(WriteOptions(), handle, "key", "other"));
  ::_exit(1);
}

TEST_F(DBCsppCrashSafeTest, ChangedFactoryWithOtherCfLeftoverUsesFullWal) {
  for (bool was_osl : {false, true}) {
    SCOPED_TRACE(was_osl);
    Close();
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    if (!was_osl) SetupOsl(&options, true);
    Destroy(options);
    ASSERT_EQ(RunCrashChild(dbname_, "ChangedFactoryWithOtherCfLeftover",
                            was_osl ? "OSL" : "CSPP"), 1);
    // Both CFs now use the other's factory, so default's old leftover is absent.
    ASSERT_OK(TryReopenWithColumnFamilies({"default", "other"}, options));
    ASSERT_EQ(Get(0, "key"), "primary");
    ASSERT_EQ(Get(1, "key"), "other");
  }
}

TEST_F(CrashChild, DISABLED_OslLeftoverWithWriterLock) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SetupOsl(&options, true);
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  const std::string key = "locked-osl-key";
  ASSERT_OK(child_db->Put(WriteOptions(), key, "value"));
  auto* cfd = static_cast<DBImpl*>(child_db)->GetVersionSet()
                  ->GetColumnFamilySet()->GetDefault();
  const auto active = MakeTableFileName(dbname_, cfd->mem()->GetFileNumber());
  int fd = ::open(active.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  char data[4096];
  const ssize_t n = ::pread(fd, data, sizeof(data), 0);
  ASSERT_GT(n, 0);
  const char* p = std::search(data, data + n, key.begin(), key.end());
  ASSERT_NE(p, data + n);
  ASSERT_GE(p - data, 12);
  ASSERT_EQ(DecodeFixed32(p - 4), key.size());
  // ValueVec precedes the length-prefixed key. Simulate InsertDup holding its
  // lock while the published COW array is still available to readers.
  ASSERT_EQ(DecodeFixed32(p - 12), 1U);
  char locked_num[4];
  EncodeFixed32(locked_num, 0x80000001U);
  ASSERT_EQ(::pwrite(fd, locked_num, sizeof(locked_num), p - 12 - data), 4);
  ::close(fd);
  std::string value;
  ASSERT_OK(child_db->Get(ReadOptions(), key, &value));
  ASSERT_EQ(value, "value");
  std::unique_ptr<Iterator> it(child_db->NewIterator(ReadOptions()));
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(it->key(), key);
  ASSERT_EQ(it->value(), "value");
  ::_exit(1);
}

TEST_F(DBCsppCrashSafeTest, OslLeftoverWithWriterLock) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SetupOsl(&options, true);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "OslLeftoverWithWriterLock"), 1);
  for (int i = 0; i != 2; ++i) {
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("locked-osl-key"), "value");
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(it->key(), "locked-osl-key");
    ASSERT_EQ(it->value(), "value");
    it.reset();
    Close();
  }
}

TEST_F(CrashChild, DISABLED_MixedDumpMemUsesFullWal) {
  Options options = BaseCrashSafeOptions(dbname_, false, false);
  const bool osl = arg_ == "OSL";
  if (osl) SetupOsl(&options, true);
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  Options dump = options;
  dump.memtable_factory = EasyNewMemTableRep(
      osl ? "OffsetSkipList" : "CSPPMemTab",
      R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem"})");
  ColumnFamilyHandle* handle = nullptr;
  ASSERT_OK(child_db->CreateColumnFamily(dump, "dump", &handle));
  ASSERT_OK(child_db->Put(WriteOptions(), "mmap", "1"));
  ASSERT_OK(child_db->Put(WriteOptions(), handle, "dump", "2"));
  ::_exit(1);
}

TEST_F(DBCsppCrashSafeTest, MixedDumpMemUsesFullWal) {
  for (bool osl : {false, true}) {
    SCOPED_TRACE(osl);
    Close();
    Options options = BaseCrashSafeOptions(dbname_, false, false);
    if (osl) SetupOsl(&options, true);
    Destroy(options);
    ASSERT_EQ(RunCrashChild(dbname_, "MixedDumpMemUsesFullWal",
                            osl ? "OSL" : "CSPP"), 1);
    Options dump = options;
    dump.memtable_factory = EasyNewMemTableRep(
        osl ? "OffsetSkipList" : "CSPPMemTab",
        R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem"})");
    ASSERT_OK(TryReopenWithColumnFamilies(
        {"default", "dump"}, std::vector<Options>{options, dump}));
    ASSERT_EQ(Get(0, "mmap"), "1");
    ASSERT_EQ(Get(1, "dump"), "2");
  }
}

TEST_F(CrashChild, DISABLED_CloseConvertsLeftovers) {
  ASSERT_EQ(arg_.size(), 2U);
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  if (arg_[0] == '1') SetupOsl(&options, true);
  options.atomic_flush = arg_[1] == '1';
  options.min_write_buffer_number_to_merge = 3;
  DB* db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  const auto closing_thread = std::this_thread::get_id();
  std::atomic<int> converts{0};
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::ConvertToSST:Status", [&](void*) {
        EXPECT_NE(std::this_thread::get_id(), closing_thread);
        converts.fetch_add(1);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db->Put(WriteOptions(), "k1", "v1"));
  ASSERT_OK(static_cast<DBImpl*>(db)->TEST_SwitchMemtable());
  ASSERT_OK(db->Put(WriteOptions(), "k2", "v2"));
  ASSERT_OK(db->Close());
  delete db;
  ASSERT_GE(converts.load(), 2);
  ASSERT_FALSE(HasFailure());
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, CloseConvertsLeftovers) {
  Close();
  for (bool osl : {false, true}) {
    for (bool atomic : {false, true}) {
      SCOPED_TRACE(osl);
      SCOPED_TRACE(atomic);
      Options options = BaseCrashSafeOptions(dbname_, true, false);
      if (osl) SetupOsl(&options, true);
      options.atomic_flush = atomic;
      Destroy(options);
      ASSERT_EQ(RunCrashChild(dbname_, "CloseConvertsLeftovers",
                              std::to_string(osl) + std::to_string(atomic)), 0);
      ASSERT_EQ(ListLeftovers(options, dbname_).size(), 3U);
      for (const auto& path : ListLeftovers(options, dbname_))
        ASSERT_OK(env_->FileExists(path));
      ASSERT_OK(env_->FileExists(CrashSafePubSeqFileName(dbname_)));
      ASSERT_OK(TryReopen(options));
      ASSERT_EQ(Get("k1"), "v1");
      ASSERT_EQ(Get("k2"), "v2");
      ASSERT_GE(CountL0(db_), 1);
      Close();
      ASSERT_EQ(ListLeftovers(options, dbname_).size(), 2U);
      for (const auto& path : ListLeftovers(options, dbname_))
        ASSERT_OK(env_->FileExists(path));
    }
  }
}
#endif

TEST_F(DBCsppCrashSafeTest, AvoidFlushDuringShutdownKeepsRegisteredMemTable) {
  Close();
  for (bool osl : {false, true}) {
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    if (osl) SetupOsl(&options, true);
    options.avoid_flush_during_shutdown = true;
    Destroy(options);
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("k", "v"));
    const auto registered = ListLeftovers(options, dbname_);
    ASSERT_EQ(registered.size(), 2U);
    auto* cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    for (auto* mem : {cfd->mem(), cfd->PeekPrecreatedMemtable()}) {
      ASSERT_NE(mem, nullptr);
      ASSERT_TRUE(mem->IsFileRegistered());
      const auto path = MakeTableFileName(dbname_, mem->GetFileNumber());
      ASSERT_EQ(std::count(registered.begin(), registered.end(), path), 1);
    }
    Close();
    ASSERT_EQ(ListLeftovers(options, dbname_), registered);
    for (const auto& path : registered) ASSERT_OK(env_->FileExists(path));
    ASSERT_OK(env_->FileExists(CrashSafePubSeqFileName(dbname_)));
    options.avoid_flush_during_shutdown = false;
    std::atomic<int> converted{0};
    SyncPoint::GetInstance()->SetCallBack(
        "MemTableRep::ConvertToSST:After", [&](void*) { ++converted; });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(TryReopen(options));
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_EQ(converted.load(), 1);
    ASSERT_EQ(Get("k"), "v");
    ASSERT_EQ(CountL0(db_), 1);
    Close();
    for (const auto& path : ListLeftovers(options, dbname_))
      ASSERT_OK(env_->FileExists(path));
  }
}

TEST_F(DBCsppCrashSafeTest, AvoidFlushCloseReopenDoesNotProbeWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  int probes = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RecoverLogFiles:ProbeWalFormat",
      [&probes](void*) { probes++; });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(TryReopen(options));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(probes, 0);
  ASSERT_EQ(Get("k"), "v");
}

TEST_F(DBCsppCrashSafeTest, FreshSidecarProbesOnlyOlderWal) {
  Close();
  Options off = BaseCrashSafeOptions(dbname_, false, false);
  off.avoid_flush_during_shutdown = true;
  Destroy(off);
  ASSERT_OK(TryReopen(off));
  ASSERT_OK(Put("a", "1"));
  Close();
  Options on = BaseCrashSafeOptions(dbname_, true, false);
  on.avoid_flush_during_shutdown = true;
  std::vector<uint64_t> probed;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RecoverLogFiles:ProbeWalFormat", [&probed](void* arg) {
        probed.push_back(*static_cast<uint64_t*>(arg));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(TryReopen(on));
  ASSERT_EQ(probed.size(), 1U);
  ASSERT_OK(Put("b", "2"));
  Close();
  // A normal close retains registered sources, enabling prefix conversion.
  // Force full WAL replay here to exercise mixed old/new WAL format probing:
  // only WALs older than the fresh sidecar's kind boundary need inspection.
  const auto registered = ListLeftovers(on, dbname_);
  ASSERT_FALSE(registered.empty());
  ASSERT_OK(env_->DeleteFile(registered.front()));
  probed.clear();
  ASSERT_OK(TryReopen(on));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_GT(rec.kind_since_wal, 0U);
  ASSERT_EQ(probed.size(), 1U);
  ASSERT_LT(probed[0], rec.kind_since_wal);
  ASSERT_EQ(Get("a"), "1");
  ASSERT_EQ(Get("b"), "2");
}

TEST_F(DBCsppCrashSafeTest, ClassicWalToLogIndexWithoutSidecarIsNotSupported) {
  Close();
  std::string value(100000, '\0');
  for (size_t i = 0; i < value.size(); ++i) {
    value[i] = static_cast<char>('a' + i % 23);
  }
  for (bool osl : {false, true}) {
    SCOPED_TRACE(osl ? "OSL" : "CSPP");
    for (bool paranoid : {false, true}) {
      SCOPED_TRACE(paranoid);
      Options classic = BaseCrashSafeOptions(dbname_, false, false);
      if (osl) SetupOsl(&classic, true);
      classic.avoid_flush_during_shutdown = true;
      classic.paranoid_checks = paranoid;
      Destroy(classic);
      ASSERT_OK(TryReopen(classic));
      ASSERT_OK(Put("large", value));
      Close();
      ASSERT_TRUE(env_->FileExists(CrashSafePubSeqFileName(dbname_)).IsNotFound());

      Options log_index = classic;
      log_index.memtable_crash_safe_recover = true;
      log_index.memtable_as_log_index = true;
      for (int attempt = 0; attempt < 2; ++attempt) {
        const Status s = TryReopen(log_index);
        ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
        PublishedSeqOnDisk rec;
        ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
        ASSERT_EQ(rec.wal_offset_kind, 2U);
        ASSERT_EQ(rec.kind_since_wal, 0U);
        ASSERT_EQ(rec.generation & 1, 0U);
      }

      // Failed Open must not force KindPrep with the unestablished log-index
      // kind. Reopen in the original format without deleting the sidecar.
      classic.memtable_crash_safe_recover = true;
      ASSERT_OK(TryReopen(classic));
      ASSERT_EQ(Get("large"), value);
      PublishedSeqOnDisk rec;
      ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
      ASSERT_EQ(rec.wal_offset_kind, 1U);
      ASSERT_GT(rec.kind_since_wal, 0U);
      Close();

      // An established sidecar still permits the existing KindPrep switch.
      ASSERT_OK(TryReopen(log_index));
      ASSERT_EQ(Get("large"), value);
      Close();
    }
  }
}

TEST_F(DBCsppCrashSafeTest, LogIndexWalWithoutSidecarRecoversAsClassic) {
  Close();
  const std::string value(100000, 'v');
  for (bool osl : {false, true}) {
    SCOPED_TRACE(osl ? "OSL" : "CSPP");
    Options options = BaseCrashSafeOptions(dbname_, false, true);
    if (osl) SetupOsl(&options, true);
    options.avoid_flush_during_shutdown = true;
    Destroy(options);
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("large", value));
    Close();
    ASSERT_TRUE(env_->FileExists(CrashSafePubSeqFileName(dbname_)).IsNotFound());

    options.memtable_crash_safe_recover = true;
    options.memtable_as_log_index = false;
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("large"), value);
    Close();
  }
}

TEST_F(DBCsppCrashSafeTest, RecoverOffDeletesStaleSidecar) {
  Close();
  for (bool osl : {false, true}) {
    SCOPED_TRACE(osl);
    Options on = BaseCrashSafeOptions(dbname_, true, false);
    if (osl) SetupOsl(&on, true);
    on.avoid_flush_during_shutdown = true;
    Destroy(on);
    ASSERT_OK(TryReopen(on));
    ASSERT_OK(Put("k", "v"));
    Close();
    ASSERT_OK(env_->FileExists(CrashSafePubSeqFileName(dbname_)));
    ASSERT_FALSE(ListLeftovers(on, dbname_).empty());
    Options off = on;
    off.memtable_crash_safe_recover = false;
    ASSERT_OK(TryReopen(off));
    ASSERT_TRUE(env_->FileExists(CrashSafePubSeqFileName(dbname_)).IsNotFound());
    for (auto* cfd : *dbfull()->GetVersionSet()->GetColumnFamilySet()) {
      ASSERT_TRUE(cfd->GetMemTableFiles().empty());
    }
    ASSERT_TRUE(ListLeftovers(off, dbname_).empty());
    ASSERT_EQ(Get("k"), "v");
    Close();
    ASSERT_OK(TryReopen(off));
    for (auto* cfd : *dbfull()->GetVersionSet()->GetColumnFamilySet()) {
      ASSERT_TRUE(cfd->GetMemTableFiles().empty());
    }
    ASSERT_EQ(Get("k"), "v");
    Close();
  }
}

TEST_F(DBCsppCrashSafeTest, OddGenerationKindSwitchUsesKindPrep) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_TRUE(SetPublishedSeqGeneration(dbname_, rec.generation | 1));
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  log_index.avoid_flush_during_shutdown = true;
  int prep = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::Open::KindPrep:AfterOpenBeforeFlush",
      [&prep](void*) { prep++; });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(TryReopen(log_index));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(prep, 1);
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 2U);
  ASSERT_EQ(Get("k"), "v");
}

TEST_F(DBCsppCrashSafeTest, DirectDBImplOpenKindMismatchFails) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  std::vector<ColumnFamilyDescriptor> cfs = {
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, log_index)};
  std::vector<ColumnFamilyHandle*> handles;
  DB* db = nullptr;
  const Status s = DBImpl::Open(DBOptions(log_index), dbname_, cfs, &handles,
                                &db, false /*seq_per_batch*/,
                                true /*batch_per_txn*/);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_EQ(db, nullptr);
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 1U);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "v");
}

TEST_F(DBCsppCrashSafeTest, OddGenerationPublishesEvenAfterWalRecovery) {
  for (bool log_index : {false, true}) {
    SCOPED_TRACE(log_index);
    Close();
    Options options = BaseCrashSafeOptions(dbname_, true, log_index);
    options.avoid_flush_during_shutdown = true;
    Destroy(options);
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("before", "recovery"));
    Close();
    PublishedSeqOnDisk rec;
    ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
    ASSERT_TRUE(SetPublishedSeqGeneration(dbname_, rec.generation | 1));
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("before"), "recovery");
    ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
    ASSERT_EQ(rec.generation % 2, 0U);
    ASSERT_EQ(rec.pubseq, 0U);
#if !defined(__AVX__)
    int odd = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::PersistPublishedSequence:AfterOddGeneration", [&](void*) {
          PublishedSeqOnDisk in_progress;
          ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &in_progress));
          ASSERT_EQ(in_progress.generation % 2, 1U);
          ++odd;
        });
    SyncPoint::GetInstance()->EnableProcessing();
#endif
    ASSERT_OK(Put("after", "recovery"));
#if !defined(__AVX__)
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_EQ(odd, 1);
#endif
    ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
    ASSERT_EQ(rec.generation % 2, 0U);
    ASSERT_EQ(rec.pubseq, dbfull()->GetLatestSequenceNumber());
    Close();
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("before"), "recovery");
    ASSERT_EQ(Get("after"), "recovery");
  }
}

TEST_F(DBCsppCrashSafeTest, LogIndexOffRecoverStillConverts) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("only", "recover"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("tail", "wal"));
  Close();
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("only"), "recover");
  ASSERT_EQ(Get("tail"), "wal");
}

TEST_F(DBCsppCrashSafeTest, AtomicFlushCloseConverts) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.atomic_flush = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  ASSERT_EQ(ListLeftovers(options, dbname_).size(), 2U);
  for (const auto& path : ListLeftovers(options, dbname_))
    ASSERT_OK(env_->FileExists(path));
  ASSERT_OK(env_->FileExists(CrashSafePubSeqFileName(dbname_)));
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "v");
  ASSERT_GE(CountL0(db_), 1);
}

#if !defined(OS_WIN)
TEST_F(CrashChild, DISABLED_AtomicFlushAfterCommitExitConverts) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.atomic_flush = true;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void* /*arg*/) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "pub", "yes"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, AtomicFlushAfterCommitExitConverts) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.atomic_flush = true;
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "AtomicFlushAfterCommitExitConverts"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("pub"), "yes");
  ASSERT_GE(CountL0(db_), 1);
}

TEST_F(CrashChild, DISABLED_AfterCommitExitConvertsAndReopens) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void* /*arg*/) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "pub", "yes"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, AfterCommitExitConvertsAndReopens) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "AfterCommitExitConvertsAndReopens"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("pub"), "yes");
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1U);
  ASSERT_TRUE(metadata.levels[0].files[0].marked_for_compaction);
}

// Non-AVX builds publish through the odd generation.
#if !defined(__AVX__)
TEST_F(CrashChild, DISABLED_AfterOddGenerationFallsBackToWal) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterOddGeneration",
      [](void* /*arg*/) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "odd", "wal"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, AfterOddGenerationFallsBackToWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "AfterOddGenerationFallsBackToWal"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("odd"), "wal");
}
#endif  // !__AVX__

TEST_F(CrashChild, DISABLED_AfterWriteToWALBeforePublishKeepsWalTail) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:AfterWriteToWALBeforePublish",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(child_db->Put(WriteOptions(), "tail", "2"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, AfterWriteToWALBeforePublishKeepsWalTail) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  {
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("first", "1"));
    Close();
  }
  ASSERT_EQ(RunCrashChild(dbname_, "AfterWriteToWALBeforePublishKeepsWalTail"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("first"), "1");
  ASSERT_EQ(Get("tail"), "2");
}

TEST_F(CrashChild, DISABLED_ConvertInjectFailureFallsBackToWal) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "inj", "ok"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, ConvertInjectFailureFallsBackToWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "ConvertInjectFailureFallsBackToWal"), 1);
  SyncPoint::GetInstance()->EnableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "CrashSafeRecover::ConvertToSST:InjectStatus", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("inject convert");
      });
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("inj"), "ok");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CrashChild, DISABLED_SeekInjectFailureStillConverts) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "seek", "ok"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, SeekInjectFailureStillConverts) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "SeekInjectFailureStillConverts"), 1);
  SyncPoint::GetInstance()->EnableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "CrashSafeRecover::SeekToFileOffset:InjectStatus", [](void* arg) {
        *static_cast<IOStatus*>(arg) = IOStatus::IOError("inject seek");
      });
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("seek"), "ok");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class SeekFailureFS : public FileSystemWrapper {
 public:
  SeekFailureFS(const std::shared_ptr<FileSystem>& fs, std::string path,
                bool fail_read)
      : FileSystemWrapper(fs), path_(std::move(path)), fail_read_(fail_read) {}
  const char* Name() const override { return "SeekFailureFS"; }
  bool armed = false;
  int failures = 0;
  int wal_opens = 0;

  class File : public FSSequentialFileOwnerWrapper {
   public:
    File(std::unique_ptr<FSSequentialFile>&& file, SeekFailureFS* fs)
        : FSSequentialFileOwnerWrapper(std::move(file)), fs_(fs) {}
    IOStatus Read(size_t n, const IOOptions& opts, Slice* result, char* scratch,
                  IODebugContext* dbg) override {
      if (fs_->armed && fs_->fail_read_) {
        fs_->armed = false;
        IOStatus s = FSSequentialFileOwnerWrapper::Read(
            std::min<size_t>(n, 4), opts, result, scratch, dbg);
        if (!s.ok()) return s;
        fs_->failures++;
        return IOStatus::IOError("seek read failed after consuming bytes");
      }
      return FSSequentialFileOwnerWrapper::Read(n, opts, result, scratch, dbg);
    }
    IOStatus Skip(uint64_t n) override {
      IOStatus s = FSSequentialFileOwnerWrapper::Skip(n);
      if (s.ok() && n != 0 && fs_->armed && !fs_->fail_read_) {
        fs_->armed = false;
        fs_->failures++;
        return IOStatus::IOError("seek skip failed after advancing file");
      }
      return s;
    }
   private:
    SeekFailureFS* fs_;
  };

  IOStatus NewSequentialFile(const std::string& fname, const FileOptions& opts,
                            std::unique_ptr<FSSequentialFile>* file,
                            IODebugContext* dbg) override {
    IOStatus s = FileSystemWrapper::NewSequentialFile(fname, opts, file, dbg);
    if (s.ok() && fname == path_) {
      wal_opens++;
      *file = std::make_unique<File>(std::move(*file), this);
    }
    return s;
  }
 private:
  const std::string path_;
  const bool fail_read_;
};

TEST_F(CrashChild, DISABLED_SeekIOFailureReopensWal) {
  Options options = BaseCrashSafeOptions(dbname_, true, arg_ == "log-index");
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "prefix",
                          std::string(log::kBlockSize * 2, 'p')));
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:AfterWriteToWALBeforePublish",
      [](void*) { ::_exit(42); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(child_db->Put(WriteOptions(), "tail", "unpublished"));
  ::_exit(1);
}

TEST_F(DBCsppCrashSafeTest, SeekIOFailureReopensWal) {
  Close();
  for (const std::string mode : {"read", "skip", "log-index"}) {
    SCOPED_TRACE(mode);
    Options options = BaseCrashSafeOptions(dbname_, true, mode == "log-index");
    Destroy(options);
    ASSERT_EQ(RunCrashChild(dbname_, "SeekIOFailureReopensWal", mode), 42);
    PublishedSeqOnDisk rec;
    ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
    auto fs = std::make_shared<SeekFailureFS>(
        options.env->GetFileSystem(), LogFileName(options.wal_dir, rec.wal_number),
        mode == "read");
    auto fault_env = NewCompositeEnv(fs);
    options.env = fault_env.get();
    options.log_readahead_size = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "CrashSafeRecover::SeekToFileOffset:Before",
        [&](void*) { fs->armed = true; });
    SyncPoint::GetInstance()->EnableProcessing();
    const Status s = TryReopen(options);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    if (s.ok()) {
      EXPECT_EQ(Get("prefix"), std::string(log::kBlockSize * 2, 'p'));
      EXPECT_EQ(Get("tail"), "unpublished");
    }
    Close();
    ASSERT_OK(s);
    ASSERT_EQ(fs->failures, 1);
    ASSERT_GE(fs->wal_opens, 2);
  }
}

TEST_F(DBCsppCrashSafeTest, TornStampKindZeroRestampsAndProbesWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.magic, 0x5145534255505343ULL);
  ASSERT_EQ(rec.wal_offset_kind, 1U);
  ASSERT_TRUE(SetPublishedSeqWalOffsetKind(dbname_, 0));
  int probes = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RecoverLogFiles:ProbeWalFormat",
      [&probes](void*) { probes++; });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(TryReopen(options));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_GT(probes, 0);
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 1U);
  ASSERT_EQ(Get("k"), "v");
}

TEST_F(CrashChild, DISABLED_UninitializedPublishedSeqStillOpens) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MapPublishedSeqFile:AfterMmapBeforeStamp",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, UninitializedPublishedSeqStillOpens) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "UninitializedPublishedSeqStillOpens"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("after", "stamp"));
  ASSERT_EQ(Get("after"), "stamp");
}

TEST_F(CrashChild, DISABLED_ZeroPublishedSeqWithLeftoverFallsBackToWal) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "k", "v"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, ZeroPublishedSeqWithLeftoverFallsBackToWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "ZeroPublishedSeqWithLeftoverFallsBackToWal"), 1);
  const auto before = ListLeftovers(options, dbname_);
  ASSERT_FALSE(before.empty());
  ASSERT_TRUE(ZeroPublishedSeqFile(dbname_));
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "v");
  for (const auto& path : before) {
    ASSERT_TRUE(env_->FileExists(path).IsNotFound());
  }
}

TEST_F(CrashChild, DISABLED_LogIndexSeekInjectKeepsUnpublishedWalTail) {
  Options options = BaseCrashSafeOptions(dbname_, true, true);
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:AfterWriteToWALBeforePublish",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(child_db->Put(WriteOptions(), "b", "2"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, LogIndexSeekInjectKeepsUnpublishedWalTail) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, true);
  Destroy(options);
  {
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("a", "1"));
    Close();
  }
  ASSERT_EQ(RunCrashChild(dbname_, "LogIndexSeekInjectKeepsUnpublishedWalTail"), 1);
  SyncPoint::GetInstance()->EnableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "CrashSafeRecover::SeekToFileOffset:InjectStatus", [](void* arg) {
        *static_cast<IOStatus*>(arg) = IOStatus::IOError("inject seek");
      });
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("a"), "1");
  ASSERT_EQ(Get("b"), "2");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif  // !OS_WIN

TEST_F(DBCsppCrashSafeTest, TwoWriteQueuesWalTail) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.two_write_queues = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("q", "1"));
  Close();
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("q"), "1");
}

TEST_F(DBCsppCrashSafeTest, WriteCommittedSkipPrepare) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  Transaction* txn = txn_db->BeginTransaction(WriteOptions());
  ASSERT_OK(txn->Put("wc", "1"));
  ASSERT_OK(txn->Commit());
  delete txn;
  delete txn_db;
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("wc"), "1");
}

TEST_F(DBCsppCrashSafeTest, OslFileMmapRecoverRoundTrip) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SetupOsl(&options, true);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("osl", "v"));
  Close();
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("osl"), "v");
}

TEST_F(DBCsppCrashSafeTest, LogIndexHeaderHasWalRefWithoutRecover) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, false, true);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", std::string(64, 'v')));
  ASSERT_TRUE(ListLeftovers(options, dbname_).empty());
  const uint64_t number = dbfull()->GetVersionSet()->GetColumnFamilySet()
                              ->GetDefault()->mem()->GetFileNumber();
  const int fd = ::open(MakeTableFileName(dbname_, number).c_str(), O_RDONLY);
  ASSERT_GE(fd, 0);
  terark::DFA_MmapHeader hdr{};
  ASSERT_EQ(::pread(fd, &hdr, sizeof(hdr), 0),
            static_cast<ssize_t>(sizeof(hdr)));
  ::close(fd);
  const auto* cs =
      reinterpret_cast<const uint32_t*>(hdr.reserved);
  ASSERT_EQ(cs[0], 0x50505343U);
  ASSERT_NE(cs[1], 0U);
  ASSERT_GT(cs[2], 0U);
}

#if !defined(OS_WIN)
TEST_F(CrashChild, DISABLED_DisableWalIsForcedOff) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  std::atomic<int> hits{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:AfterWriteToWALBeforePublish", [&hits](void*) {
        if (hits.fetch_add(1) >= 1) {
          ::_exit(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "pad", "p"));
  WriteOptions child_wo;
  child_wo.disableWAL = true;
  ASSERT_OK(child_db->Put(child_wo, "d", "4"));
  ::_exit(0);
}
#endif

TEST_F(DBCsppCrashSafeTest, DisableWalIsForcedOff) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  const uint64_t wal_number = rec.wal_number;
  const uint64_t wal_offset = rec.wal_offset;
  const uint64_t pubseq = rec.pubseq;
  ASSERT_GT(wal_number, 0U);
  ASSERT_GT(wal_offset, 0U);
  WriteOptions wo;
  wo.disableWAL = true;
  ASSERT_OK(Put("b", "2", wo));
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_number, wal_number);
  ASSERT_GT(rec.wal_offset, wal_offset);
  ASSERT_GT(rec.pubseq, pubseq);
  ASSERT_EQ(Get("a"), "1");
  ASSERT_EQ(Get("b"), "2");
#if !defined(OS_WIN)
  Close();
  Destroy(options);
  {
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("c", "3"));
    Close();
  }
  ASSERT_EQ(RunCrashChild(dbname_, "DisableWalIsForcedOff"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("c"), "3");
  ASSERT_EQ(Get("pad"), "p");
  ASSERT_EQ(Get("d"), "4");
#endif
}

#if !defined(OS_WIN)
TEST_F(CrashChild, DISABLED_UnorderedWritePublishesPreviousWalCursor) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.unordered_write = true;
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "pad", "p"));
  ASSERT_OK(child_db->Put(WriteOptions(), "d", "4"));
  ::_exit(1);
}
#endif

TEST_F(DBCsppCrashSafeTest, UnorderedWritePublishesPreviousWalCursor) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.unordered_write = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.pubseq, 0U);
  ASSERT_OK(Put("b", "2"));
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_GT(rec.pubseq, 0U);
  ASSERT_GT(rec.wal_offset, 0U);
  const uint64_t after_b_seq = rec.pubseq;
  const uint64_t after_b_off = rec.wal_offset;
  ASSERT_OK(Put("c", "3"));
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_GT(rec.pubseq, after_b_seq);
  ASSERT_GT(rec.wal_offset, after_b_off);
  ASSERT_EQ(Get("a"), "1");
  ASSERT_EQ(Get("b"), "2");
  ASSERT_EQ(Get("c"), "3");
#if !defined(OS_WIN)
  Close();
  Destroy(options);
  {
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("k", "0"));
    Close();
  }
  ASSERT_EQ(RunCrashChild(dbname_, "UnorderedWritePublishesPreviousWalCursor"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "0");
  ASSERT_EQ(Get("pad"), "p");
  ASSERT_EQ(Get("d"), "4");
#endif
}

TEST_F(DBCsppCrashSafeTest, UnorderedWriteSyncFailureDoesNotBlockFlushOrClose) {
  Close();
  for (bool recover : {false, true}) {
    for (bool osl : {false, true}) {
      SCOPED_TRACE(recover);
      SCOPED_TRACE(osl);
      Options options = BaseCrashSafeOptions(dbname_, recover, false);
      if (osl) SetupOsl(&options, true);
      options.unordered_write = true;
      Destroy(options);
      auto fs = std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
      auto fault_env = NewCompositeEnv(fs);
      options.env = fault_env.get();
      DB* raw_db = nullptr;
      ASSERT_OK(DB::Open(options, dbname_, &raw_db));
      std::unique_ptr<DB> db(raw_db);
      ASSERT_OK(db->Put(WriteOptions(), "seed", "value"));
      SyncPoint::GetInstance()->SetCallBack("DBImpl::SyncWAL:Begin", [&](void*) {
        fs->SetFilesystemActive(false, IOStatus::IOError("WAL sync failure"));
      });
      SyncPoint::GetInstance()->EnableProcessing();
      WriteOptions wo;
      wo.sync = true;
      Status s = db->Put(wo, "failed", "value");
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      fs->SetFilesystemActive(true);
      ASSERT_TRUE(s.IsIOError());
      // A WAL sync error stops the DB, but Flush and Close must still return.
      ASSERT_TRUE(db->Flush(FlushOptions()).IsIOError());
      ASSERT_TRUE(db->Close().IsIOError());
    }
  }
}

TEST_F(DBCsppCrashSafeTest, UnorderedWritePreReleaseFailureAccountsWholeGroup) {
  class FailPreRelease : public PreReleaseCallback {
   public:
    size_t calls = 0;
    Status Callback(SequenceNumber, bool, uint64_t, size_t, size_t total) override {
      EXPECT_EQ(total, 2U);
      ++calls;
      return Status::Busy("pre-release failure");
    }
  };
  for (bool recover : {false, true}) {
    for (bool osl : {false, true}) {
      SCOPED_TRACE(recover);
      SCOPED_TRACE(osl);
      Close();
      Options options = BaseCrashSafeOptions(dbname_, recover, false);
      if (osl) SetupOsl(&options, true);
      options.unordered_write = true;
      Destroy(options);
      ASSERT_OK(TryReopen(options));
      ASSERT_OK(Put("seed", "value"));
      // Hold the leader until the other writer has joined the same group.
      FailPreRelease callback;
      SyncPoint::GetInstance()->LoadDependency({
          {"UnorderedWriteFailure:FollowerJoined", "UnorderedWriteFailure:Leader"}});
      SyncPoint::GetInstance()->SetCallBack(
          "WriteThread::JoinBatchGroup:Wait", [&callback](void* arg) {
            auto* w = static_cast<WriteThread::Writer*>(arg);
            w->pre_release_callback = &callback;
            if (w->state == WriteThread::STATE_GROUP_LEADER) {
              TEST_SYNC_POINT("UnorderedWriteFailure:Leader");
            } else {
              TEST_SYNC_POINT("UnorderedWriteFailure:FollowerJoined");
            }
          });
      SyncPoint::GetInstance()->EnableProcessing();
      Status status[2];
      std::thread writers[2];
      for (size_t i = 0; i != 2; ++i) {
        writers[i] = std::thread([&, i] {
          WriteBatch batch;
          ASSERT_OK(batch.Put("failed" + std::to_string(i), "value"));
          status[i] = db_->Write(WriteOptions(), &batch);
        });
      }
      for (auto& writer : writers) writer.join();
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      SyncPoint::GetInstance()->LoadDependency({});
      ASSERT_EQ(callback.calls, 1U);
      for (const auto& s : status) ASSERT_TRUE(s.IsBusy());
      ASSERT_EQ(Get("failed0"), "NOT_FOUND");
      ASSERT_EQ(Get("failed1"), "NOT_FOUND");
      ASSERT_OK(Put("after", "value"));
      ASSERT_OK(Put("tail", "value"));
      if (recover) {
        PublishedSeqOnDisk rec;
        ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
        ASSERT_EQ(rec.pubseq, db_->GetLatestSequenceNumber() - 1);
      }
      ASSERT_OK(Flush());
      ASSERT_OK(db_->Close());
      ASSERT_OK(TryReopen(options));
      ASSERT_EQ(Get("seed"), "value");
      ASSERT_EQ(Get("after"), "value");
      ASSERT_EQ(Get("tail"), "value");
    }
  }
}

#if !defined(OS_WIN)
TEST_F(CrashChild, DISABLED_PipelinedWriteUsesStagedWalCursor) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.enable_pipelined_write = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "pad", "p"));
  ASSERT_OK(child_db->Merge(WriteOptions(), "m2", "y"));
  ASSERT_OK(child_db->Put(WriteOptions(), "d", "4"));
  ::_exit(1);
}
#endif

TEST_F(DBCsppCrashSafeTest, PipelinedWriteUsesStagedWalCursor) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.enable_pipelined_write = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.pubseq, 0U);
  {
    std::thread t1([&] { ASSERT_OK(Merge("m", "x")); });
    std::thread t2([&] { ASSERT_OK(Put("b", "2")); });
    t1.join();
    t2.join();
  }
  ASSERT_OK(Put("c", "3"));
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_GT(rec.pubseq, 0U);
  ASSERT_GT(rec.wal_number, 0U);
  ASSERT_GT(rec.wal_offset, 0U);
  ASSERT_EQ(Get("a"), "1");
  ASSERT_EQ(Get("b"), "2");
  ASSERT_EQ(Get("c"), "3");
  ASSERT_EQ(Get("m"), "x");
#if !defined(OS_WIN)
  Close();
  Destroy(options);
  {
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("k", "0"));
    Close();
  }
  ASSERT_EQ(RunCrashChild(dbname_, "PipelinedWriteUsesStagedWalCursor"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "0");
  ASSERT_EQ(Get("pad"), "p");
  ASSERT_EQ(Get("m2"), "y");
  ASSERT_EQ(Get("d"), "4");
#endif
}

#if !defined(OS_WIN)
TEST_F(CrashChild, DISABLED_LeftoverOnDbPathNotCfPaths0) {
  const std::string l0 = dbname_ + "/l0data";
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.db_paths = {{l0, 1ULL << 30}};
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "pad", "p"));
  ASSERT_OK(child_db->Put(WriteOptions(), "x", "1"));
  auto* cfd = static_cast<DBImpl*>(child_db)->GetVersionSet()
                  ->GetColumnFamilySet()->GetDefault();
  ASSERT_OK(WriteStringToFile(
      options.env, TableFileName(cfd->ioptions()->cf_paths,
                                cfd->mem()->GetFileNumber(), 0),
      dbname_ + "/active-file"));
  ::_exit(1);
}
#endif

TEST_F(DBCsppCrashSafeTest, LeftoverOnDbPathNotCfPaths0) {
  Close();
  const std::string l0 = dbname_ + "/l0data";
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.db_paths = {{l0, 1ULL << 30}};
  Destroy(options);
  ASSERT_OK(env_->CreateDirIfMissing(dbname_));
  ASSERT_OK(env_->CreateDirIfMissing(l0));
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "0"));
  Close();
  ASSERT_EQ(ListLeftovers(options, dbname_).size(), 2U);
  for (const auto& path : ListLeftovers(options, dbname_))
    ASSERT_OK(env_->FileExists(path));
#if !defined(OS_WIN)
  ASSERT_EQ(RunCrashChild(dbname_, "LeftoverOnDbPathNotCfPaths0"), 1);
  auto leftovers_l0 = ListLeftovers(options, dbname_);
  ASSERT_EQ(leftovers_l0.size(), 2U);
  std::string active;
  ASSERT_OK(ReadFileToString(env_, dbname_ + "/active-file", &active));
  ASSERT_EQ(std::count(leftovers_l0.begin(), leftovers_l0.end(), active), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "0");
  ASSERT_EQ(Get("pad"), "p");
  ASSERT_EQ(Get("x"), "1");
  ASSERT_OK(env_->FileExists(active));
  std::vector<LiveFileMetaData> files;
  db_->GetLiveFilesMetaData(&files);
  const auto matching = std::count_if(files.begin(), files.end(), [&](const auto& file) {
    return MakeTableFileName(file.db_path, file.file_number) == active;
  });
  ASSERT_EQ(matching, 1);
#endif
}

TEST_F(DBCsppCrashSafeTest, EmptyWriteUpdatesWalCursor) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  const uint64_t pubseq = rec.pubseq;
  const uint64_t wal_number = rec.wal_number;
  const uint64_t wal_offset = rec.wal_offset;
  ASSERT_GT(pubseq, 0U);
  WriteBatch empty;
  ASSERT_OK(db_->Write(WriteOptions(), &empty));
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.pubseq, pubseq);
  ASSERT_EQ(rec.wal_number, wal_number);
  ASSERT_GT(rec.wal_offset, wal_offset);
}

TEST_F(DBCsppCrashSafeTest, PublishedSeqFieldsAfterPut) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 1U);  // kPhysical, stamped on create
  ASSERT_EQ(rec.pubseq, 0U);
  ASSERT_OK(Put("k", "v"));
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.generation % 2, 0U);
  ASSERT_EQ(rec.pubseq, dbfull()->GetLatestSequenceNumber());
  ASSERT_GT(rec.wal_number, 0U);
  ASSERT_GT(rec.wal_offset, 0U);
  ASSERT_EQ(rec.wal_offset_kind, 1U);  // Persist does not rewrite kind
}

TEST_F(DBCsppCrashSafeTest, ExistingPublishedSeqKindPrepLogIndexAfterClassic) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 1U);
  Close();
  ASSERT_OK(env_->FileExists(CrashSafePubSeqFileName(dbname_)));
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  log_index.avoid_flush_during_shutdown = true;
  ASSERT_OK(TryReopen(log_index));
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 2U);
  ASSERT_EQ(Get("k"), "v");
  ASSERT_GE(CountL0(db_), 1);
}

TEST_F(DBCsppCrashSafeTest, ExistingPublishedSeqKindPrepClassicAfterLogIndex) {
  Close();
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  log_index.avoid_flush_during_shutdown = true;
  Destroy(log_index);
  ASSERT_OK(TryReopen(log_index));
  ASSERT_OK(Put("k", "v"));
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 2U);
  Close();
  ASSERT_OK(env_->FileExists(CrashSafePubSeqFileName(dbname_)));
  Options classic = BaseCrashSafeOptions(dbname_, true, false);
  classic.avoid_flush_during_shutdown = true;
  ASSERT_OK(TryReopen(classic));
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 1U);
  ASSERT_EQ(Get("k"), "v");
  ASSERT_GE(CountL0(db_), 1);
}

TEST_F(DBCsppCrashSafeTest, KindPrepSkippedWhenKindMatches) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  int prep = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::Open::KindPrep:AfterOpenBeforeFlush",
      [&](void*) { prep++; });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(TryReopen(options));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(prep, 0);
  ASSERT_EQ(Get("k"), "v");
}

TEST_F(DBCsppCrashSafeTest, KindPrepFailureClearsDbPointer) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  options.memtable_as_log_index = true;
  options.error_if_exists = true;
  DB* db = reinterpret_cast<DB*>(uintptr_t(1));
  Status s = DB::Open(options, dbname_, &db);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_EQ(db, nullptr);
}

#if !defined(OS_WIN)
TEST_F(CrashChild, DISABLED_EasyMigrateKindPrep) {
  ASSERT_EQ(arg_.size(), 3U);
  const bool log_index = arg_[0] == '1';
  const bool txn = arg_[1] == '1';
  const bool recover = arg_[2] == '1';
  // EasyMigrate caches its config, so load it only in this fresh exec child.
  const std::string config = dbname_ + ".easy-migrate.json";
  ASSERT_EQ(::setenv("TOPLINGDB_EASY_MIGRATE_CONF", config.c_str(), 1), 0);
  Options options = BaseCrashSafeOptions(dbname_, recover, log_index);
  int prep = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::Open::KindPrep:AfterOpenBeforeFlush", [&](void*) { ++prep; });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* db = nullptr;
  if (txn) {
    TransactionDB* txn_db = nullptr;
    ASSERT_OK(TransactionDB::Open(options, TransactionDBOptions(), dbname_,
                                  &txn_db));
    db = txn_db;
  } else {
    ASSERT_OK(DB::Open(options, dbname_, &db));
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(prep, 1);
  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "k", &value));
  ASSERT_EQ(value, "v");
  ASSERT_OK(db->Put(WriteOptions(), "after", "switch"));
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, log_index ? 2U : 1U);
  ASSERT_OK(db->Close());
  delete db;
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, EasyMigrateKindPrep) {
  for (bool log_index : {false, true}) {
    for (bool txn : {false, true}) {
      for (bool recover : {false, true}) {
        const std::string mode = {char('0' + log_index), char('0' + txn),
                                  char('0' + recover)};
        SCOPED_TRACE(mode);
        Close();
        Options options = BaseCrashSafeOptions(dbname_, true, !log_index);
        options.avoid_flush_during_shutdown = true;
        Destroy(options);
        ASSERT_OK(TryReopen(options));
        ASSERT_OK(Put("k", "v"));
        // This convert-only table factory cannot BuildTable during 2PC replay.
        ASSERT_OK(Flush());
        Close();
        const std::string config = dbname_ + ".easy-migrate.json";
        const json conf = {
            {"DBOptions", {{"default", {{"memtable_crash_safe_recover", true},
                                        {"memtable_as_log_index", log_index}}}}},
            {"http", {{"auto_start_http", false}}}};
        ASSERT_OK(WriteStringToFile(env_, conf.dump(), config));
        ASSERT_EQ(RunCrashChild(dbname_, "EasyMigrateKindPrep", mode), 0);
        ASSERT_OK(env_->DeleteFile(config));
        options.memtable_as_log_index = log_index;
        ASSERT_OK(TryReopen(options));
        ASSERT_EQ(Get("k"), "v");
        ASSERT_EQ(Get("after"), "switch");
      }
    }
  }
}

TEST_F(CrashChild, DISABLED_KindPrepKeepsUnpublishedWalTail) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:AfterWriteToWALBeforePublish",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "b", "2"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, KindPrepKeepsUnpublishedWalTail) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  {
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("a", "1"));
    Close();
  }
  ASSERT_EQ(RunCrashChild(dbname_, "KindPrepKeepsUnpublishedWalTail"), 1);
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  log_index.avoid_flush_during_shutdown = true;
  ASSERT_OK(TryReopen(log_index));
  ASSERT_EQ(Get("a"), "1");
  ASSERT_EQ(Get("b"), "2");
}

TEST_F(DBCsppCrashSafeTest, KindPrepAfterFlushBeforeClose) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  ASSERT_EQ(RunCrashChild(
                dbname_, "KindPrep", "DBImpl::Open::KindPrep:AfterFlushBeforeClose"),
            kKindPrepChildCrashed);
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 1U);
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  log_index.avoid_flush_during_shutdown = true;
  ASSERT_OK(TryReopen(log_index));
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 2U);
  ASSERT_EQ(Get("k"), "v");
}

TEST_F(DBCsppCrashSafeTest, KindPrepAfterCloseBeforeDeleteSidecar) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  ASSERT_EQ(RunCrashChild(
                dbname_, "KindPrep", "DBImpl::Open::KindPrep:AfterCloseBeforeDeleteSidecar"),
            kKindPrepChildCrashed);
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 1U);
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  log_index.avoid_flush_during_shutdown = true;
  ASSERT_OK(TryReopen(log_index));
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 2U);
  ASSERT_EQ(Get("k"), "v");
}

TEST_F(CrashChild, DISABLED_TransactionDBKindPrepClassicToLogIndex) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  Transaction* txn = txn_db->BeginTransaction(WriteOptions());
  ASSERT_OK(txn->Put("t", "1"));
  ASSERT_OK(txn->Commit());
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, TransactionDBKindPrepClassicToLogIndex) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  TransactionDB* txn_db = nullptr;
  ASSERT_EQ(RunCrashChild(dbname_, "TransactionDBKindPrepClassicToLogIndex"), 0);
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 1U);
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  log_index.avoid_flush_during_shutdown = true;
  int prep = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::Open::KindPrep:AfterOpenBeforeFlush",
      [&prep](void*) { prep++; });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(TransactionDB::Open(log_index, txn_opts, dbname_, &txn_db));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(prep, 1);
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 2U);
  std::string v;
  ASSERT_OK(txn_db->Get(ReadOptions(), "t", &v));
  ASSERT_EQ(v, "1");
  delete txn_db;
}

TEST_F(CrashChild, DISABLED_TransactionDBKindPrepPreparedNotSupported) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  TransactionOptions to;
  to.skip_prepare = false;
  Transaction* txn = txn_db->BeginTransaction(WriteOptions(), to);
  ASSERT_OK(txn->SetName("xid1"));
  ASSERT_OK(txn->Put("prep", "v"));
  ASSERT_OK(txn->Prepare());
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, TransactionDBKindPrepPreparedNotSupported) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  TransactionDB* txn_db = nullptr;
  ASSERT_EQ(RunCrashChild(dbname_, "TransactionDBKindPrepPreparedNotSupported"), 0);
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  log_index.avoid_flush_during_shutdown = true;
  ASSERT_TRUE(TransactionDB::Open(log_index, txn_opts, dbname_, &txn_db)
                  .IsNotSupported());
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_EQ(rec.wal_offset_kind, 1U);
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  std::vector<Transaction*> prepared;
  txn_db->GetAllPreparedTransactions(&prepared);
  ASSERT_EQ(prepared.size(), 1U);
  ASSERT_OK(prepared[0]->Rollback());
  delete prepared[0];
  delete txn_db;
}

TEST_F(DBCsppCrashSafeTest, TransactionDBPreparedCloseThenSwitchKind) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  TransactionOptions to;
  to.skip_prepare = false;
  Transaction* txn = txn_db->BeginTransaction(WriteOptions(), to);
  ASSERT_OK(txn->SetName("xid1"));
  ASSERT_OK(txn->Put("prep", "v"));
  ASSERT_OK(txn->Prepare());
  delete txn;
  delete txn_db;  // default avoid_flush_during_shutdown=false: Close converts
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  ASSERT_TRUE(TransactionDB::Open(log_index, txn_opts, dbname_, &txn_db)
                  .IsNotSupported());
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  std::vector<Transaction*> prepared;
  txn_db->GetAllPreparedTransactions(&prepared);
  ASSERT_EQ(prepared.size(), 1U);
  ASSERT_OK(prepared[0]->Rollback());
  delete prepared[0];
  delete txn_db;
}

TEST_F(CrashChild, DISABLED_TransactionDBRollbackCloseThenSwitchKind) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  TransactionOptions to;
  to.skip_prepare = false;
  Transaction* txn = txn_db->BeginTransaction(WriteOptions(), to);
  ASSERT_OK(txn->SetName("xid1"));
  ASSERT_OK(txn->Put("prep", "v"));
  ASSERT_OK(txn->Prepare());
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, TransactionDBRollbackCloseThenSwitchKind) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  ASSERT_OK(txn_db->Put(WriteOptions(), "k", "v"));
  delete txn_db;
  ASSERT_EQ(RunCrashChild(dbname_, "TransactionDBRollbackCloseThenSwitchKind"), 0);
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  std::vector<Transaction*> prepared;
  txn_db->GetAllPreparedTransactions(&prepared);
  ASSERT_EQ(prepared.size(), 1U);
  ASSERT_OK(prepared[0]->Rollback());
  delete prepared[0];
  delete txn_db;  // default avoid_flush_during_shutdown=false: Close converts
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  ASSERT_OK(TransactionDB::Open(log_index, txn_opts, dbname_, &txn_db));
  std::string v;
  ASSERT_OK(txn_db->Get(ReadOptions(), "k", &v));
  ASSERT_EQ(v, "v");
  ASSERT_TRUE(txn_db->Get(ReadOptions(), "prep", &v).IsNotFound());
  delete txn_db;
}

TEST_F(DBCsppCrashSafeTest, KindPrepConvertFailFallsBackToWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  Options log_index = BaseCrashSafeOptions(dbname_, true, true);
  log_index.avoid_flush_during_shutdown = true;
  ASSERT_FALSE(log_index.check_wal_format);
  ASSERT_OK(TryReopen(log_index));
  ASSERT_EQ(Get("k"), "v");
  Close();
  SyncPoint::GetInstance()->SetCallBack(
      "CrashSafeRecover::ConvertToSST:InjectStatus", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("inject convert");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(TryReopen(log_index));
  ASSERT_EQ(Get("k"), "v");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif

TEST_F(DBCsppCrashSafeTest, DontConvertUsesWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, false, false);
  SetupCspp(&options, false);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "v");
}

TEST_F(DBCsppCrashSafeTest, BestEffortsRecoveryFallsBackToWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.best_efforts_recovery = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "v");
}

TEST_F(DBCsppCrashSafeTest, LogIndexOnRecoverOffUsesWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, false, true);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  ASSERT_TRUE(env_->FileExists(CrashSafePubSeqFileName(dbname_)).IsNotFound());
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "v");
}

TEST_F(DBCsppCrashSafeTest, ManifestRegistryIgnoresUnregisteredFiles) {
  for (bool osl : {false, true}) {
    SCOPED_TRACE(osl ? "OSL" : "CSPP");
    Close();
    Options options = BaseCrashSafeOptions(dbname_, true, false);
    if (osl) {
      SetupOsl(&options, true);
    }
    Destroy(options);
    ASSERT_OK(env_->CreateDirIfMissing(dbname_));
    const std::string high = MakeTableFileName(dbname_, 100);
    const std::string low = MakeTableFileName(dbname_, 10);
    ASSERT_OK(WriteStringToFile(env_, "", high));
    ASSERT_OK(WriteStringToFile(env_, "", low));
    ASSERT_TRUE(ListLeftovers(options, dbname_).empty());
    // Remove both so collision checks cannot hide a stale counter.
    ASSERT_OK(env_->DeleteFile(high));
    ASSERT_OK(env_->DeleteFile(low));
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(ListLeftovers(options, dbname_).size(), 2U);
    for (const auto& path : ListLeftovers(options, dbname_))
      ASSERT_OK(env_->FileExists(path));
    Close();
    const auto before = ListLeftovers(options, dbname_);
    // The manifest remains authoritative when unrelated files appear.
    ASSERT_OK(WriteStringToFile(env_, "", low));
    ASSERT_EQ(ListLeftovers(options, dbname_), before);
    ASSERT_OK(env_->DeleteFile(low));
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(ListLeftovers(options, dbname_).size(), 2U);
    for (const auto& path : ListLeftovers(options, dbname_))
      ASSERT_OK(env_->FileExists(path));
    Close();
    Destroy(options);
  }
}

TEST_F(DBCsppCrashSafeTest, CreateMemTableRepDoesNotOverwriteLeftover) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("keep", "me"));
  auto first = ListLeftovers(options, dbname_);
  ASSERT_EQ(first.size(), 2U);
  auto* cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto active = MakeTableFileName(dbname_, cfd->mem()->GetFileNumber());
  ASSERT_EQ(std::count(first.begin(), first.end(), active), 1);
  std::string before;
  ASSERT_OK(ReadFileToString(env_, active, &before));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("other", "x"));
  auto after_list = ListLeftovers(options, dbname_);
  ASSERT_EQ(after_list, first);
  ASSERT_NE(MakeTableFileName(dbname_, cfd->mem()->GetFileNumber()), active);
  std::string after;
  ASSERT_OK(ReadFileToString(env_, active, &after));
  ASSERT_EQ(before, after);
}

TEST_F(DBCsppCrashSafeTest, Allow2pcAloneStillConverts) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.allow_2pc = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  Close();
  ASSERT_EQ(ListLeftovers(options, dbname_).size(), 2U);
  for (const auto& path : ListLeftovers(options, dbname_))
    ASSERT_OK(env_->FileExists(path));
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "v");
}

#if !defined(OS_WIN)
TEST_F(CrashChild, DISABLED_WalFilterFallsBackToWal) {
  Options options = BaseCrashSafeOptions(dbname_, true, arg_[1] == '1');
  if (arg_[0] == '1') SetupOsl(&options, true);
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "a", std::string(128, 'a')));
  ASSERT_OK(child_db->Put(WriteOptions(), "b", std::string(128, 'b')));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, WalFilterFallsBackToWal) {
  class IgnoreRecords final : public WalFilter {
   public:
    int calls = 0;
    const char* Name() const override { return "IgnoreRecords"; }
    WalProcessingOption LogRecordFound(unsigned long long, const std::string&,
                                       const WriteBatch&, WriteBatch*,
                                       bool*) override {
      ++calls;
      return WalProcessingOption::kIgnoreCurrentRecord;
    }
  };
  for (bool osl : {false, true}) {
    for (bool log_index : {false, true}) {
      SCOPED_TRACE(osl);
      SCOPED_TRACE(log_index);
      Close();
      Options options = BaseCrashSafeOptions(dbname_, true, log_index);
      if (osl) SetupOsl(&options, true);
      Destroy(options);
      ASSERT_EQ(RunCrashChild(dbname_, "WalFilterFallsBackToWal",
                              std::string(osl ? "1" : "0") +
                                  (log_index ? "1" : "0")), 0);
      ASSERT_FALSE(ListLeftovers(options, dbname_).empty());
      IgnoreRecords filter;
      options.wal_filter = &filter;
      ASSERT_OK(TryReopen(options));
      ASSERT_EQ(filter.calls, 2);
      ASSERT_EQ(Get("a"), "NOT_FOUND");
      ASSERT_EQ(Get("b"), "NOT_FOUND");
      ASSERT_EQ(CountL0(db_), 0);
      Close();
    }
  }
}

TEST_F(CrashChild, DISABLED_LeftoverNoMagicFallsBackToWal) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "bad", "hdr"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, LeftoverNoMagicFallsBackToWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  for (const std::string damage : {"empty", "short-header", "missing-magic"}) {
    SCOPED_TRACE(damage);
    Destroy(options);
    ASSERT_EQ(RunCrashChild(dbname_, "LeftoverNoMagicFallsBackToWal"), 1);
    auto leftovers = ListLeftovers(options, dbname_);
    ASSERT_FALSE(leftovers.empty());
    const int fd = ::open(leftovers[0].c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    terark::DFA_MmapHeader hdr{};
    if (damage == "missing-magic") {
      ASSERT_EQ(::pread(fd, &hdr, sizeof(hdr), 0),
                static_cast<ssize_t>(sizeof(hdr)));
      std::memset(hdr.reserved, 0, sizeof(hdr.reserved));
      ASSERT_EQ(::pwrite(fd, &hdr, sizeof(hdr), 0),
                static_cast<ssize_t>(sizeof(hdr)));
    } else {
      ASSERT_EQ(::ftruncate(fd, damage == "empty" ? 0 : sizeof(hdr) - 1), 0);
    }
    ::close(fd);
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("bad"), "hdr");
    Close();
  }
}

TEST_F(CrashChild, DISABLED_DualLeftoverSecondConvertFails) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  std::atomic<int> pubs{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit", [&pubs](void*) {
        if (pubs.fetch_add(1) >= 1) {
          ::_exit(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "a", "1"));
  ASSERT_OK(static_cast<DBImpl*>(child_db)->TEST_SwitchMemtable());
  ASSERT_OK(child_db->Put(WriteOptions(), "b", "2"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, DualLeftoverSecondConvertFails) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "DualLeftoverSecondConvertFails"), 1);
  SyncPoint::GetInstance()->EnableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  std::atomic<int> converts{0};
  SyncPoint::GetInstance()->SetCallBack(
      "CrashSafeRecover::ConvertToSST:InjectStatus", [&converts](void* arg) {
        if (converts.fetch_add(1) >= 1) {
          *static_cast<Status*>(arg) = Status::IOError("second leftover");
        }
      });
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("a"), "1");
  ASSERT_EQ(Get("b"), "2");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CrashChild, DISABLED_TruncateInjectFailureFallsBackToWal) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "tr", "ok"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, TruncateInjectFailureFallsBackToWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "TruncateInjectFailureFallsBackToWal"), 1);
  SyncPoint::GetInstance()->EnableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  bool truncate_called = false;
  SyncPoint::GetInstance()->SetCallBack(
      "MemTableRep::ConvertToSST:Truncate", [&truncate_called](void* arg) {
        truncate_called = true;
        *static_cast<IOStatus*>(arg) = IOStatus::IOError("inject truncate");
      });
  ASSERT_OK(TryReopen(options));
  ASSERT_TRUE(truncate_called);
  ASSERT_EQ(Get("tr"), "ok");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CrashChild, DISABLED_AfterConvertBeforeAddFileKeepsRegisteredFile) {
  ASSERT_EQ(arg_.size(), 3U);
  Options options = BaseCrashSafeOptions(dbname_, true, arg_[1] == '1');
  if (arg_[0] == '1') SetupOsl(&options, true);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  auto* cfd = static_cast<DBImpl*>(child_db)->GetVersionSet()
                  ->GetColumnFamilySet()->GetDefault();
  ASSERT_OK(WriteStringToFile(
      options.env, MakeTableFileName(dbname_, cfd->mem()->GetFileNumber()),
      dbname_ + "/active-file"));
  ASSERT_OK(child_db->Put(WriteOptions(), "or", std::string(128, 'v')));
  ::_exit(0);
}

TEST_F(CrashChild, DISABLED_AfterConvertBeforeAddFileKeepsRegisteredFileRecover) {
  ASSERT_EQ(arg_.size(), 3U);
  Options options = BaseCrashSafeOptions(dbname_, true, arg_[1] == '1');
  if (arg_[0] == '1') SetupOsl(&options, true);
  const char* points[] = {
      "MemTableRep::ConvertToSST:Truncate",
      "CrashSafeRecover::AfterConvertBeforeAddFile",
      "CrashSafeRecover::AfterConvertBeforeAddFile",
      "DBImpl::RegisterMemTableFile:AfterLogAndApply"};
  ASSERT_LT(arg_[2] - '0', 4);
  SyncPoint::GetInstance()->SetCallBack(
      points[arg_[2] - '0'],
      [&](void*) {
        if (arg_[2] == '1') {
          // Model an incomplete SST tail, without claiming this callback runs
          // in the middle of a write. The persisted trie remains intact.
          const auto files = ListLeftovers(options, dbname_);
          ASSERT_EQ(files.size(), 2U);
          std::string active;
          ASSERT_OK(ReadFileToString(options.env, dbname_ + "/active-file", &active));
          ASSERT_EQ(std::count(files.begin(), files.end(), active), 1);
          const int fd = ::open(active.c_str(), O_RDWR);
          ASSERT_GE(fd, 0);
          uint64_t structure_size = 0;
          if (arg_[0] == '1') {
            terark::OSL_MmapHeader hdr{};
            ASSERT_EQ(::pread(fd, &hdr, sizeof(hdr), 0),
                      static_cast<ssize_t>(sizeof(hdr)));
            structure_size = hdr.mem_used;
          } else {
            terark::DFA_MmapHeader hdr{};
            ASSERT_EQ(::pread(fd, &hdr, sizeof(hdr), 0),
                      static_cast<ssize_t>(sizeof(hdr)));
            structure_size = hdr.file_size;
          }
          uint64_t size = 0;
          ASSERT_OK(options.env->GetFileSize(active, &size));
          ASSERT_GT(size, structure_size);
          ASSERT_EQ(::ftruncate(fd, size - 1), 0);
          ::close(fd);
        }
        ::_exit(1);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* recover_db = nullptr;
  DB::Open(options, dbname_, &recover_db);
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, AfterConvertBeforeAddFileKeepsRegisteredFile) {
  Close();
  for (bool osl : {false, true}) {
    for (bool log_index : {false, true}) {
      for (int window = 0; window < 4; ++window) {
        const std::string config = std::to_string(osl) +
            std::to_string(log_index) + std::to_string(window);
        SCOPED_TRACE(config);
        Options options = BaseCrashSafeOptions(dbname_, true, log_index);
        if (osl) SetupOsl(&options, true);
        Destroy(options);
        ASSERT_EQ(RunCrashChild(dbname_,
            "AfterConvertBeforeAddFileKeepsRegisteredFile", config), 1);
        const auto before = ListLeftovers(options, dbname_);
        ASSERT_EQ(before.size(), 2U);
        std::string active;
        ASSERT_OK(ReadFileToString(env_, dbname_ + "/active-file", &active));
        ASSERT_EQ(std::count(before.begin(), before.end(), active), 1);
        for (const auto& path : before) ASSERT_OK(env_->FileExists(path));
        // Before commit, interrupt the same file twice to exercise footer
        // replacement, not just a one-time conversion of the original source.
        for (int crash = 0; crash < (window == 3 ? 1 : 2); ++crash) {
          ASSERT_EQ(RunCrashChild(dbname_,
              "AfterConvertBeforeAddFileKeepsRegisteredFileRecover", config), 1);
          ASSERT_OK(env_->FileExists(active));
          if (window != 3) {
            ASSERT_EQ(ListLeftovers(options, dbname_), before);
            for (const auto& path : before) ASSERT_OK(env_->FileExists(path));
          }
          PublishedSeqOnDisk rec;
          ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
          ASSERT_EQ(rec.generation & 1, 0U);
        }
        int converted = 0;
        SyncPoint::GetInstance()->SetCallBack(
            "MemTableRep::ConvertToSST:After", [&](void*) { ++converted; });
        SyncPoint::GetInstance()->EnableProcessing();
        ASSERT_OK(TryReopen(options));
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        ASSERT_EQ(converted, window == 3 ? 0 : 1);
        ASSERT_EQ(Get("or"), std::string(128, 'v'));
        std::vector<LiveFileMetaData> files;
        db_->GetLiveFilesMetaData(&files);
        ASSERT_EQ(files.size(), 1U);
        ASSERT_EQ(files.front().level, 0);
        ASSERT_EQ(MakeTableFileName(dbname_, files.front().file_number),
                  active);
        Close();
        ASSERT_OK(TryReopen(options));
        ASSERT_EQ(Get("or"), std::string(128, 'v'));
        ASSERT_EQ(CountL0(db_), 1);
        Close();
      }
    }
  }
}

TEST_F(DBCsppCrashSafeTest, CrashSafeOrLogIndexDisablesWalCompression) {
  for (bool crash_safe : {false, true}) {
    for (bool log_index : {false, true}) {
      SCOPED_TRACE(crash_safe);
      SCOPED_TRACE(log_index);
      Close();
      Options options = BaseCrashSafeOptions(dbname_, crash_safe, log_index);
      options.wal_compression = kZSTD;
      Destroy(options);
      ASSERT_OK(TryReopen(options));
      ASSERT_EQ(db_->GetDBOptions().wal_compression,
                crash_safe || log_index ? kNoCompression : kZSTD);
      ASSERT_OK(Put("k", "v"));
      Reopen(options);
      ASSERT_EQ(Get("k"), "v");
    }
  }
}

TEST_F(CrashChild, DISABLED_AfterWriteToWALGhostCopyHidden) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:AfterWriteToWALBeforePublish",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(child_db->Put(WriteOptions(), "tail", "2"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, AfterWriteToWALGhostCopyHidden) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  {
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("first", "1"));
    Close();
  }
  ASSERT_EQ(RunCrashChild(dbname_, "AfterWriteToWALGhostCopyHidden"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("first"), "1");
  ASSERT_EQ(Get("tail"), "2");
  ASSERT_OK(Put("bump", "seq"));
  ASSERT_EQ(Get("tail"), "2");
  int n = 0;
  std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
  for (it->Seek("tail"); it->Valid() && it->key() == "tail"; it->Next()) {
    n++;
  }
  ASSERT_OK(it->status());
  ASSERT_EQ(n, 1);
}

TEST_F(CrashChild, DISABLED_WriteCommittedPrepareInDoubt) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  TransactionOptions to;
  to.skip_prepare = false;
  Transaction* txn = txn_db->BeginTransaction(WriteOptions(), to);
  ASSERT_OK(txn->SetName("xid1"));
  ASSERT_OK(txn->Put("prep", "v"));
  ASSERT_OK(txn->Prepare());
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, WriteCommittedPrepareInDoubt) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  ASSERT_EQ(RunCrashChild(dbname_, "WriteCommittedPrepareInDoubt"), 1);
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  std::vector<Transaction*> prepared;
  txn_db->GetAllPreparedTransactions(&prepared);
  ASSERT_EQ(prepared.size(), 1U);
  ASSERT_OK(prepared[0]->Rollback());
  delete prepared[0];
  delete txn_db;
}

TEST_F(CrashChild, DISABLED_WriteCommittedPrepareAndCommit) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  std::atomic<bool> prepare_done{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit", [&prepare_done](void*) {
        if (prepare_done.load(std::memory_order_acquire)) {
          ::_exit(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  TransactionOptions to;
  to.skip_prepare = false;
  Transaction* txn = txn_db->BeginTransaction(WriteOptions(), to);
  ASSERT_OK(txn->SetName("xid2"));
  ASSERT_OK(txn->Put("pc", "v"));
  ASSERT_OK(txn->Prepare());
  prepare_done.store(true, std::memory_order_release);
  ASSERT_OK(txn->Commit());
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, WriteCommittedPrepareAndCommit) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  ASSERT_EQ(RunCrashChild(dbname_, "WriteCommittedPrepareAndCommit"), 1);
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  std::vector<Transaction*> prepared;
  txn_db->GetAllPreparedTransactions(&prepared);
  ASSERT_TRUE(prepared.empty());
  std::string v;
  ASSERT_OK(txn_db->Get(ReadOptions(), "pc", &v));
  ASSERT_EQ(v, "v");
  delete txn_db;
}

TEST_F(DBCsppCrashSafeTest, WriteCommittedPrepareTwoQueuesPublishes) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.two_write_queues = true;
  Destroy(options);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  TransactionOptions to;
  to.skip_prepare = false;
  Transaction* txn = txn_db->BeginTransaction(WriteOptions(), to);
  ASSERT_OK(txn->SetName("xidq"));
  ASSERT_OK(txn->Put("pq", "1"));
  ASSERT_OK(txn->Prepare());
  PublishedSeqOnDisk rec;
  ASSERT_TRUE(ReadPublishedSeqFile(dbname_, &rec));
  ASSERT_GT(rec.wal_number, 0U);
  ASSERT_GT(rec.wal_offset, 0U);
  ASSERT_OK(txn->Rollback());
  delete txn;
  delete txn_db;
}

#if !defined(OS_WIN)
TEST_F(CrashChild, DISABLED_WriteCommittedPrepareTwoQueuesInDoubt) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.two_write_queues = true;
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  TransactionOptions to;
  to.skip_prepare = false;
  Transaction* txn = txn_db->BeginTransaction(WriteOptions(), to);
  ASSERT_OK(txn->SetName("xidq1"));
  ASSERT_OK(txn->Put("prepq", "v"));
  ASSERT_OK(txn->Prepare());
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, WriteCommittedPrepareTwoQueuesInDoubt) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.two_write_queues = true;
  Destroy(options);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  ASSERT_EQ(RunCrashChild(dbname_, "WriteCommittedPrepareTwoQueuesInDoubt"), 1);
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  std::vector<Transaction*> prepared;
  txn_db->GetAllPreparedTransactions(&prepared);
  ASSERT_EQ(prepared.size(), 1U);
  ASSERT_OK(prepared[0]->Rollback());
  delete prepared[0];
  delete txn_db;
}

TEST_F(CrashChild, DISABLED_WriteCommittedPrepareTwoQueuesAndCommit) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.two_write_queues = true;
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  std::atomic<bool> prepare_done{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit", [&prepare_done](void*) {
        if (prepare_done.load(std::memory_order_acquire)) {
          ::_exit(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  TransactionOptions to;
  to.skip_prepare = false;
  Transaction* txn = txn_db->BeginTransaction(WriteOptions(), to);
  ASSERT_OK(txn->SetName("xidq2"));
  ASSERT_OK(txn->Put("pcq", "v"));
  ASSERT_OK(txn->Prepare());
  prepare_done.store(true, std::memory_order_release);
  ASSERT_OK(txn->Commit());
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, WriteCommittedPrepareTwoQueuesAndCommit) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.two_write_queues = true;
  Destroy(options);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  ASSERT_EQ(RunCrashChild(dbname_, "WriteCommittedPrepareTwoQueuesAndCommit"), 1);
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  std::vector<Transaction*> prepared;
  txn_db->GetAllPreparedTransactions(&prepared);
  ASSERT_TRUE(prepared.empty());
  std::string v;
  ASSERT_OK(txn_db->Get(ReadOptions(), "pcq", &v));
  ASSERT_EQ(v, "v");
  delete txn_db;
}
#endif

TEST_F(DBCsppCrashSafeTest, WritePreparedFallsBackToWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.two_write_queues = true;
  Destroy(options);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_PREPARED;
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  Transaction* txn = txn_db->BeginTransaction(WriteOptions());
  ASSERT_OK(txn->Put("wp", "1"));
  ASSERT_OK(txn->Commit());
  delete txn;
  delete txn_db;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  std::string v;
  ASSERT_OK(txn_db->Get(ReadOptions(), "wp", &v));
  ASSERT_EQ(v, "1");
  delete txn_db;
}

TEST_F(DBCsppCrashSafeTest, AfterConvertCloseSecondFlushInject) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("c", "3"));
  std::atomic<int> converts{0};
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::ConvertToSST:Status", [&converts](void* arg) {
        if (converts.fetch_add(1) >= 1) {
          *static_cast<Status*>(arg) = Status::IOError("close second");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Close();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(env_->FileExists(CrashSafePubSeqFileName(dbname_)));
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("a"), "1");
  ASSERT_EQ(Get("b"), "2");
  ASSERT_EQ(Get("c"), "3");
}

TEST_F(CrashChild, DISABLED_DualCfLeftoverConvertsBoth) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  std::atomic<int> pubs{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit", [&pubs](void*) {
        if (pubs.fetch_add(1) >= 1) {
          ::_exit(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  std::vector<ColumnFamilyHandle*> hs;
  std::vector<ColumnFamilyDescriptor> cfs = {
      {kDefaultColumnFamilyName, options}, {"one", options}};
  ASSERT_OK(DB::Open(options, dbname_, cfs, &hs, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), hs[0], "d", "1"));
  ASSERT_OK(child_db->Put(WriteOptions(), hs[1], "c", "2"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, DualCfLeftoverConvertsBoth) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  CreateAndReopenWithCF({"one"}, options);
  Close();
  ASSERT_EQ(RunCrashChild(dbname_, "DualCfLeftoverConvertsBoth"), 1);
  ASSERT_OK(TryReopenWithColumnFamilies({kDefaultColumnFamilyName, "one"},
                                        options));
  ASSERT_EQ(Get(0, "d"), "1");
  ASSERT_EQ(Get(1, "c"), "2");
  ASSERT_GE(CountL0(db_, kDefaultColumnFamilyName), 1);
  ASSERT_GE(CountL0(db_, "one"), 1);
}

TEST_F(CrashChild, DISABLED_AtomicFlushDualCfLeftoverConvertsBoth) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.atomic_flush = true;
  std::atomic<int> pubs{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit", [&pubs](void*) {
        if (pubs.fetch_add(1) >= 1) {
          ::_exit(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  std::vector<ColumnFamilyHandle*> hs;
  std::vector<ColumnFamilyDescriptor> cfs = {
      {kDefaultColumnFamilyName, options}, {"one", options}};
  ASSERT_OK(DB::Open(options, dbname_, cfs, &hs, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), hs[0], "d", "1"));
  ASSERT_OK(child_db->Put(WriteOptions(), hs[1], "c", "2"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, AtomicFlushDualCfLeftoverConvertsBoth) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.atomic_flush = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  CreateAndReopenWithCF({"one"}, options);
  Close();
  ASSERT_EQ(RunCrashChild(dbname_, "AtomicFlushDualCfLeftoverConvertsBoth"), 1);
  ASSERT_OK(TryReopenWithColumnFamilies({kDefaultColumnFamilyName, "one"},
                                        options));
  ASSERT_EQ(Get(0, "d"), "1");
  ASSERT_EQ(Get(1, "c"), "2");
  ASSERT_GE(CountL0(db_, kDefaultColumnFamilyName), 1);
  ASSERT_GE(CountL0(db_, "one"), 1);
}

TEST_F(CrashChild, DISABLED_AtomicFlushManifestWindow) {
  ASSERT_EQ(arg_.size(), 2U);
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.atomic_flush = true;
  if (arg_[0] == '1') SetupOsl(&options, true);
  DB* child_db = nullptr;
  std::vector<ColumnFamilyHandle*> handles;
  const std::vector<ColumnFamilyDescriptor> cfs = {
      {kDefaultColumnFamilyName, options}, {"one", options}};
  ASSERT_OK(DB::Open(options, dbname_, cfs, &handles, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), handles[0], "default-key", "one"));
  ASSERT_OK(child_db->Put(WriteOptions(), handles[1], "other-key", "two"));
  SyncPoint::GetInstance()->SetCallBack(
      arg_[1] == '0' ? "FlushJob::BeforeManifest" : "FlushJob::AfterManifest",
      [](void*) { ::_exit(42); });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(child_db->Flush(FlushOptions(), handles));
  ::_exit(1);
}

TEST_F(DBCsppCrashSafeTest, AtomicFlushCrashAcrossManifestCommit) {
  Close();
  for (bool osl : {false, true}) {
    for (bool committed : {false, true}) {
      SCOPED_TRACE(osl);
      SCOPED_TRACE(committed);
      Options options = BaseCrashSafeOptions(dbname_, true, false);
      options.atomic_flush = true;
      if (osl) SetupOsl(&options, true);
      Destroy(options);
      ASSERT_OK(TryReopen(options));
      CreateAndReopenWithCF({"one"}, options);
      Close();
      const std::string arg = std::string(osl ? "1" : "0") +
                              (committed ? "1" : "0");
      ASSERT_EQ(RunCrashChild(dbname_, "AtomicFlushManifestWindow", arg), 42);
      const auto registered = ListLeftovers(options, dbname_);
      ASSERT_GE(registered.size(), 2U);
      for (const auto& path : registered) ASSERT_OK(env_->FileExists(path));
      std::atomic<int> converted{0};
      SyncPoint::GetInstance()->SetCallBack(
          "MemTableRep::ConvertToSST:After",
          [&](void*) { ++converted; });
      SyncPoint::GetInstance()->EnableProcessing();
      ASSERT_OK(TryReopenWithColumnFamilies(
          {kDefaultColumnFamilyName, "one"}, options));
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      ASSERT_EQ(converted.load(), committed ? 0 : 2);
      ASSERT_EQ(Get(0, "default-key"), "one");
      ASSERT_EQ(Get(1, "other-key"), "two");
      ASSERT_EQ(CountL0(db_, kDefaultColumnFamilyName), 1);
      ASSERT_EQ(CountL0(db_, "one"), 1);
      std::vector<LiveFileMetaData> files;
      db_->GetLiveFilesMetaData(&files);
      ASSERT_EQ(files.size(), 2U);
      for (const auto& file : files) {
        const std::string path = MakeTableFileName(dbname_, file.file_number);
        ASSERT_OK(env_->FileExists(path));
        ASSERT_EQ(std::count(registered.begin(), registered.end(), path),
                  committed ? 0 : 1);
      }
      Close();
      ASSERT_OK(TryReopenWithColumnFamilies(
          {kDefaultColumnFamilyName, "one"}, options));
      ASSERT_EQ(Get(0, "default-key"), "one");
      ASSERT_EQ(Get(1, "other-key"), "two");
      ASSERT_EQ(CountL0(db_, kDefaultColumnFamilyName), 1);
      ASSERT_EQ(CountL0(db_, "one"), 1);
      Close();
    }
  }
}

TEST_F(CrashChild, DISABLED_DroppedCfLeftoverSkipped) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  std::atomic<int> pubs{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit", [&pubs](void*) {
        if (pubs.fetch_add(1) >= 1) {
          ::_exit(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  std::vector<ColumnFamilyHandle*> hs;
  std::vector<ColumnFamilyDescriptor> cfs = {
      {kDefaultColumnFamilyName, options}, {"one", options}};
  ASSERT_OK(DB::Open(options, dbname_, cfs, &hs, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), hs[0], "keep", "1"));
  ASSERT_OK(child_db->Put(WriteOptions(), hs[1], "drop", "2"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, DroppedCfLeftoverSkipped) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  CreateAndReopenWithCF({"one"}, options);
  Close();
  ASSERT_EQ(RunCrashChild(dbname_, "DroppedCfLeftoverSkipped"), 1);
  ASSERT_OK(TryReopenWithColumnFamilies({kDefaultColumnFamilyName, "one"},
                                        options));
  ASSERT_EQ(Get(0, "keep"), "1");
  ASSERT_EQ(Get(1, "drop"), "2");
  ASSERT_OK(Flush(0));
  const auto registered = dbfull()->GetVersionSet()->GetColumnFamilySet()
                              ->GetColumnFamily(1)->GetMemTableFiles();
  ASSERT_FALSE(registered.empty());
  const std::string left1 = MakeTableFileName(dbname_, *registered.begin());
  ASSERT_FALSE(left1.empty());
  const std::string bak = left1 + ".bak";
  CopyFile(left1, bak);
  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  Close();
  if (::access(left1.c_str(), F_OK) != 0) {
    ASSERT_EQ(::rename(bak.c_str(), left1.c_str()), 0);
  } else {
    ::unlink(bak.c_str());
  }
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("keep"), "1");
  ASSERT_EQ(dbfull()->GetVersionSet()->GetColumnFamilySet()
                ->GetColumnFamily(1), nullptr);
  const auto leftovers = ListLeftovers(options, dbname_);
  ASSERT_EQ(std::count(leftovers.begin(), leftovers.end(), left1), 0);
}

TEST_F(CrashChild, DISABLED_MultiChunkAfterCommitStillReadable) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.memtable_factory.reset(NewCSPPMemTabForPlain(
      R"({"mem_cap":16777216,"convert_to_sst":"kFileMmap","chunk_size":4096})"));
  std::atomic<int> pubs{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit", [&pubs](void*) {
        if (pubs.fetch_add(1) >= 199) {
          ::_exit(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  for (int i = 0; i < 200; ++i) {
    ASSERT_OK(child_db->Put(WriteOptions(), "k" + std::to_string(i),
                            std::string(80, 'v')));
  }
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, MultiChunkAfterCommitStillReadable) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.memtable_factory.reset(NewCSPPMemTabForPlain(
      R"({"mem_cap":16777216,"convert_to_sst":"kFileMmap","chunk_size":4096})"));
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "MultiChunkAfterCommitStillReadable"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k0").size(), 80U);
  ASSERT_EQ(Get("k199").size(), 80U);
}

TEST_F(CrashChild, DISABLED_OslMultiChunkAfterCommitStillReadable) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SetupOsl(&options, true);
  std::atomic<int> pubs{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit", [&pubs](void*) {
        if (pubs.fetch_add(1) >= 399) {
          ::_exit(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  for (int i = 0; i < 400; ++i) {
    ASSERT_OK(child_db->Put(WriteOptions(), "ok" + std::to_string(i),
                            std::string(8192, 'o')));
  }
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, OslMultiChunkAfterCommitStillReadable) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SetupOsl(&options, true);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "OslMultiChunkAfterCommitStillReadable"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("ok0").size(), 8192U);
  ASSERT_EQ(Get("ok399").size(), 8192U);
}

TEST_F(CrashChild, DISABLED_OslFirstChunkAfterCommitReadable) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SetupOsl(&options, true);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "osl1", "v"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, OslFirstChunkAfterCommitReadable) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SetupOsl(&options, true);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "OslFirstChunkAfterCommitReadable"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("osl1"), "v");
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1U);
  ASSERT_TRUE(metadata.levels[0].files[0].marked_for_compaction);
}

TEST_F(CrashChild, DISABLED_WalSwitchKeepsTail) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  std::atomic<int> pubs{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit", [&pubs](void*) {
        if (pubs.fetch_add(1) >= 1) {
          ::_exit(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "old", "1"));
  ASSERT_OK(static_cast<DBImpl*>(child_db)->TEST_SwitchWAL());
  ASSERT_OK(child_db->Put(WriteOptions(), "neu", "2"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, WalSwitchKeepsTail) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "WalSwitchKeepsTail"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("old"), "1");
  ASSERT_EQ(Get("neu"), "2");
}

TEST_F(CrashChild, DISABLED_LeftoverLogRefUnbindFallsBackToWal) {
  Options options = BaseCrashSafeOptions(dbname_, true, true);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "rb", "ok"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, LeftoverLogRefUnbindFallsBackToWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, true);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "LeftoverLogRefUnbindFallsBackToWal"), 1);
  auto leftovers = ListLeftovers(options, dbname_);
  ASSERT_FALSE(leftovers.empty());
  const int fd = ::open(leftovers[0].c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  terark::DFA_MmapHeader hdr{};
  ASSERT_EQ(::pread(fd, &hdr, sizeof(hdr), 0),
            static_cast<ssize_t>(sizeof(hdr)));
  auto* cs = reinterpret_cast<uint32_t*>(hdr.reserved);
  ASSERT_EQ(cs[0], 0x50505343U);
  uint64_t fake_fileno = 999999;
  const size_t wal0 = offsetof(terark::DFA_MmapHeader, reserved) + 16;
  ASSERT_EQ(::pwrite(fd, &fake_fileno, sizeof(fake_fileno),
                     static_cast<off_t>(wal0)),
            static_cast<ssize_t>(sizeof(fake_fileno)));
  ::close(fd);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("rb"), "ok");
}

// Non-AVX builds hit AfterOddGeneration.
#if !defined(__AVX__)
TEST_F(DBCsppCrashSafeTest, CloseWaitsForStatsPublication) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.stats_dump_period_sec = 0;
  options.stats_persist_period_sec = 1;
  options.persist_stats_to_disk = true;
  options.statistics = CreateDBStatistics();
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  SyncPoint::GetInstance()->LoadDependency({
      {"CloseWaitsForStatsPublication:Publishing",
       "CloseWaitsForStatsPublication:BeginClose"},
      {"Timer::WaitForTaskCompleteIfNecessary:TaskExecuting",
       "CloseWaitsForStatsPublication:Resume"},
  });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterOddGeneration", [](void*) {
        TEST_SYNC_POINT("CloseWaitsForStatsPublication:Publishing");
        TEST_SYNC_POINT("CloseWaitsForStatsPublication:Resume");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  TEST_SYNC_POINT("CloseWaitsForStatsPublication:BeginClose");
  ASSERT_OK(db_->Close());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency({});
  Close();
}
#endif  // !__AVX__

TEST_F(DBCsppCrashSafeTest, InFlightFlushThenClose) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.max_background_flushes = 1;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("if", "1"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("tail", "2"));
  test::SleepingBackgroundTask sleeping_task;
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table", [&](void*) { sleeping_task.DoSleep(); });
  SyncPoint::GetInstance()->EnableProcessing();
  FlushOptions fo;
  fo.wait = false;
  const Status flush_status = db_->Flush(fo);
  const bool paused = !sleeping_task.TimedWaitUntilSleeping(10 * 1000000);
  if (!flush_status.ok() || !paused) sleeping_task.WakeUp();
  ASSERT_OK(flush_status);
  ASSERT_TRUE(paused);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTable:AfterScheduleFlush",
      [&](void*) { sleeping_task.WakeUp(); });
  std::thread closer([&] { Close(); });
  sleeping_task.WaitUntilDone();
  closer.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("if"), "1");
  ASSERT_EQ(Get("tail"), "2");
}
#endif  // !OS_WIN

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
#if !defined(OS_WIN)
  if (argc == 5 && std::strcmp(argv[1], "--crash-child") == 0) {
    ROCKSDB_NAMESPACE::crash_child_db = argv[3];
    ROCKSDB_NAMESPACE::crash_child_arg = argv[4];
    ::testing::GTEST_FLAG(filter) = std::string("CrashChild.DISABLED_") + argv[2];
    ::testing::GTEST_FLAG(also_run_disabled_tests) = true;
    ::alarm(30);
    const int result = RUN_ALL_TESTS();
    // Every child action must reach its explicit _exit, not just finish a test.
    return result == 0 ? 126 : 125;
  }
#endif
  return RUN_ALL_TESTS();
}
