// Copyright (c) 2026-present, Topling Inc.
// Flush/Close conversion, independently of crash-safe recovery.

#include <atomic>
#include <stdexcept>
#include <thread>
#include <tuple>

#include <topling/side_plugin_factory.h>
#include <topling/builtin_table_factory.h>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/memtable.h"
#include "db/version_set.h"
#include "env/env_chroot.h"
#include "file/filename.h"
#include "memory/arena.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"
#include "table/table_builder.h"

namespace ROCKSDB_NAMESPACE {

class DBMemtableConvertTest
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, const char*, bool>> {
 public:
  DBMemtableConvertTest()
      : DBTestBase("db_memtable_convert_test", /*env_do_fsync=*/false) {}
  ~DBMemtableConvertTest() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

 protected:
  Options ConvertOptions() {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.avoid_flush_during_recovery = true;
    options.write_buffer_size = 64 << 20;
    options.max_write_buffer_number = 8;
    options.min_write_buffer_number_to_merge = 4;
    options.atomic_flush = std::get<2>(GetParam());
    const bool osl = std::get<0>(GetParam());
    const json params = {{"mem_cap", 16777216},
                         {"convert_to_sst", std::get<1>(GetParam())}};
    auto& repo = repo_;
    options.memtable_factory = PluginFactorySP<MemTableRepFactory>::AcquirePlugin(
        osl ? "OffsetSkipList" : "CSPPMemTab", params, repo);
    repo.Put("default", options.table_factory);
    repo.Put("converted", PluginFactorySP<TableFactory>::AcquirePlugin(
        osl ? "OffsetSkipListTable" : "CSPPMemTabTable", params, repo));
    options.table_factory = PluginFactorySP<TableFactory>::AcquirePlugin(
        "Dispatch", {{"default", "$default"}}, repo);
    DispatcherTableBackPatch(options.table_factory.get(), repo);
    return options;
  }

  void ObserveConversion(bool fail = false) {
    const auto caller = std::this_thread::get_id();
    SyncPoint::GetInstance()->SetCallBack(
        "FlushJob::ConvertToSST:Status", [this, caller, fail](void* arg) {
          EXPECT_NE(std::this_thread::get_id(), caller);
          EXPECT_TRUE(static_cast<Status*>(arg)->ok());
          ++converts_;
          if (fail) {
            *static_cast<Status*>(arg) = Status::IOError("injected convert");
          }
        });
    SyncPoint::GetInstance()->SetCallBack(
        "FlushJob::WriteLevel0Table:s", [this](void*) { ++builds_; });
    SyncPoint::GetInstance()->EnableProcessing();
  }

  void CheckClose(bool wait_for_compact) {
    Options options = ConvertOptions();
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "1"));
    ASSERT_OK(dbfull()->TEST_SwitchMemtable());
    ASSERT_OK(Put("b", "2"));
    ASSERT_OK(dbfull()->TEST_SwitchMemtable());
    ASSERT_OK(Put("c", "3"));
    ObserveConversion();
    if (wait_for_compact) {
      WaitForCompactOptions wait;
      wait.close_db = true;
      ASSERT_OK(db_->WaitForCompact(wait));
    } else {
      ASSERT_OK(db_->Close());
    }
    Close();
    ASSERT_EQ(converts_.load(), 3);
    ASSERT_EQ(builds_.load(), 0);
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(Get("a"), "1");
    ASSERT_EQ(Get("b"), "2");
    ASSERT_EQ(Get("c"), "3");
    Close();
  }

  std::atomic<int> converts_{0};
  std::atomic<int> builds_{0};
  SidePluginRepo repo_;
};

TEST_P(DBMemtableConvertTest, ManualFlushConverts) {
  Options options = ConvertOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ObserveConversion();
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(converts_.load(), 1);
  ASSERT_EQ(builds_.load(), 0);
  ASSERT_EQ(Get("k"), "v");
  Close();
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "v");
  Close();
}

TEST_P(DBMemtableConvertTest, FileMmapConversionKeepsFileNumberAndPath) {
  if (std::string(std::get<1>(GetParam())) != "kFileMmap") return;
  Options options = ConvertOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("same-file", "value"));
  const auto registered = dbfull()->GetVersionSet()->GetMemTableFiles();
  ASSERT_EQ(registered.count(0), 1U);
  ASSERT_EQ(registered.at(0).size(), 1U);
  const uint64_t number = *registered.at(0).begin();
  const std::string path = MakeTableFileName(dbname_, number);
  ASSERT_OK(env_->FileExists(path));
  ObserveConversion();
  ASSERT_OK(db_->Flush(FlushOptions()));
  std::vector<LiveFileMetaData> files;
  db_->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 1U);
  ASSERT_EQ(files[0].file_number, number);
  ASSERT_OK(env_->FileExists(path));
  const auto& after = dbfull()->GetVersionSet()->GetMemTableFiles();
  auto cf = after.find(0);
  ASSERT_TRUE(cf == after.end() || cf->second.count(number) == 0);
  Close();
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("same-file"), "value");
}

#if !defined(OS_WIN)
TEST_P(DBMemtableConvertTest, FileMmapExclusiveCreationAndChrootConversion) {
  if (std::string(std::get<1>(GetParam())) != "kFileMmap") return;
  Options options = ConvertOptions();
  DestroyAndReopen(options);
  Close();
  const std::string logical_path = MakeTableFileName("", 900000);
  const std::string physical_path = dbname_ + logical_path;
  auto factory = PluginFactorySP<MemTableRepFactory>::AcquirePlugin(
      std::get<0>(GetParam()) ? "OffsetSkipList" : "CSPPMemTab",
      {{"mem_cap", 16777216}, {"convert_to_sst", "kFileMmap"},
       {"chroot_dir", dbname_}}, repo_);
  InternalKeyComparator icmp(options.comparator);
  MemTable::KeyComparator cmp(icmp);
  MutableCFOptions moptions(options);
  moptions.write_buffer_size = 1 << 20;
  Arena arena;
  if (std::get<0>(GetParam())) {
    const std::string sentinel = "existing SST must remain intact";
    ASSERT_OK(WriteStringToFile(env_, sentinel, physical_path));
    ASSERT_THROW({
      std::unique_ptr<MemTableRep> collision(factory->CreateMemTableRep(
          logical_path, moptions, cmp, &arena, nullptr, nullptr, 0));
    }, std::runtime_error);
    std::string after;
    ASSERT_OK(ReadFileToString(env_, physical_path, &after));
    ASSERT_EQ(after, sentinel);
    ASSERT_OK(env_->DeleteFile(physical_path));
  }

  std::unique_ptr<Env> chroot_env(NewChrootEnv(env_, dbname_));
  options.env = chroot_env.get();
  options.cf_paths = {{"", 0}};
  ImmutableOptions ioptions(options);
  IntTblPropCollectorFactories collectors;
  const std::string cf_name = "default";
  TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                          options.compression, options.compression_opts, 0,
                          cf_name, 0);
  std::unique_ptr<MemTableRep> rep(factory->CreateMemTableRep(
      logical_path, moptions, cmp, &arena, nullptr, nullptr, 0));
  ASSERT_TRUE(rep->IsFileMmap());
  rep->InitSetMemTableAsLogIndex(false);
  rep->MarkFileRegistered();
  ASSERT_TRUE(rep->InsertKeyValue(PackSequenceAndType(1, kTypeValue), "k", "v"));
  rep->MarkReadOnly();
  FileMetaData meta;
  meta.fd = FileDescriptor(900001, 0, 0);
  meta.num_entries = 1;
  ASSERT_TRUE(rep->ConvertToSST(&meta, tbo).IsInvalidArgument());
  rep.reset();
  ASSERT_OK(env_->FileExists(physical_path));
  meta.fd = FileDescriptor(900000, 0, 0);
  meta.fd.largest_seqno = kMaxSequenceNumber;
  ASSERT_OK(factory->RecoverCrashSafeMemTableToSST(logical_path, &meta, tbo));
  ASSERT_GT(meta.fd.GetFileSize(), 0U);
  ASSERT_OK(env_->FileExists(physical_path));
  ASSERT_OK(env_->DeleteFile(physical_path));

  // Retry conversion on the same live rep after a completed footer exists.
  rep.reset(factory->CreateMemTableRep(
      logical_path, moptions, cmp, &arena, nullptr, nullptr, 0));
  rep->InitSetMemTableAsLogIndex(false);
  rep->MarkFileRegistered();
  ASSERT_TRUE(rep->InsertKeyValue(PackSequenceAndType(1, kTypeValue), "k", "v"));
  rep->MarkReadOnly();
  meta.fd = FileDescriptor(900000, 0, 0);
  ASSERT_OK(rep->ConvertToSST(&meta, tbo));
  const uint64_t first_size = meta.fd.GetFileSize();
  ASSERT_GT(first_size, 0U);
  ASSERT_OK(rep->ConvertToSST(&meta, tbo));
  ASSERT_EQ(meta.fd.GetFileSize(), first_size);
  uint64_t actual_size = 0;
  ASSERT_OK(env_->GetFileSize(physical_path, &actual_size));
  ASSERT_EQ(actual_size, first_size);
  std::string value;
  rep->GetPIK(ReadOptions(), ParsedInternalKey("k", 1, kTypeValue), &value,
              [](void* arg, const MemTableRep::KeyValuePair& kv) {
                *static_cast<std::string*>(arg) = kv.value.ToString();
                return false;
              });
  ASSERT_EQ(value, "v");
  rep.reset();
  meta.fd = FileDescriptor(900000, 0, 0);
  meta.fd.largest_seqno = kMaxSequenceNumber;
  ASSERT_OK(factory->RecoverCrashSafeMemTableToSST(logical_path, &meta, tbo));
  ASSERT_OK(env_->DeleteFile(physical_path));
}
#endif

TEST_P(DBMemtableConvertTest, FileMmapCloseKeepsEveryFileNumber) {
  if (std::string(std::get<1>(GetParam())) != "kFileMmap") return;
  Options options = ConvertOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("head", "1"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("tail", "2"));
  const auto registered = dbfull()->GetVersionSet()->GetMemTableFiles();
  ASSERT_EQ(registered.count(0), 1U);
  ASSERT_EQ(registered.at(0).size(), 2U);
  ObserveConversion();
  ASSERT_OK(db_->Close());
  Close();
  ASSERT_EQ(converts_.load(), 2);
  for (uint64_t number : registered.at(0)) {
    ASSERT_OK(env_->FileExists(MakeTableFileName(dbname_, number)));
  }
  ASSERT_OK(TryReopen(options));
  std::vector<LiveFileMetaData> files;
  db_->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 2U);
  for (const auto& file : files) {
    ASSERT_EQ(registered.at(0).count(file.file_number), 1U);
  }
  ASSERT_EQ(Get("head"), "1");
  ASSERT_EQ(Get("tail"), "2");
}

TEST_P(DBMemtableConvertTest, CloseConvertsAllMemtables) {
  CheckClose(false);
}

TEST_P(DBMemtableConvertTest, WaitForCompactConvertsBeforeClose) {
  CheckClose(true);
}

TEST_P(DBMemtableConvertTest, AvoidCloseFlush) {
  Options options = ConvertOptions();
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ObserveConversion();
  ASSERT_OK(db_->Close());
  Close();
  ASSERT_EQ(converts_.load(), 0);
  ASSERT_EQ(builds_.load(), 0);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "v");
  Close();
}

TEST_P(DBMemtableConvertTest, MixedColumnFamilies) {
  Options options = ConvertOptions();
  Options plain = CurrentOptions();
  const std::vector<Options> cf_options = {options, plain};
  DestroyAndReopen(options);
  CreateColumnFamilies({"plain"}, plain);
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "plain"}, cf_options));
  ASSERT_OK(Put(0, "convert", "v1"));
  ASSERT_OK(Put(1, "plain", "v2"));
  ObserveConversion();
  Close();
  ASSERT_EQ(converts_.load(), 0);
  ASSERT_EQ(builds_.load(), 0);
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "plain"}, cf_options));
  ASSERT_EQ(Get(0, "convert"), "v1");
  ASSERT_EQ(Get(1, "plain"), "v2");
  Close();
}

TEST_P(DBMemtableConvertTest, UnpersistedDataStillFlushes) {
  Options options = ConvertOptions();
  options.memtable_factory = CurrentOptions().memtable_factory;
  DestroyAndReopen(options);
  WriteOptions write;
  write.disableWAL = true;
  ASSERT_OK(db_->Put(write, "k", "v"));
  ObserveConversion();
  ASSERT_OK(db_->Close());
  Close();
  ASSERT_EQ(converts_.load(), 0);
  ASSERT_EQ(builds_.load(), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "v");
  Close();
}

TEST_P(DBMemtableConvertTest, CloseConvertFailureDoesNotBuildTable) {
  Options options = ConvertOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ObserveConversion(true);
  Close();
  ASSERT_EQ(converts_.load(), 1);
  ASSERT_EQ(builds_.load(), 0);
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "v");
  Close();
}

TEST_P(DBMemtableConvertTest, InFlightFlushThenClose) {
  Options options = ConvertOptions();
  options.max_background_flushes = 1;
  DestroyAndReopen(options);
  ASSERT_OK(Put("head", "1"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("tail", "2"));
  test::SleepingBackgroundTask sleeping;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping,
                 Env::Priority::HIGH);
  sleeping.WaitUntilSleeping();
  ObserveConversion();
  FlushOptions flush;
  flush.wait = false;
  ASSERT_OK(db_->Flush(flush));
  SyncPoint::GetInstance()->SetCallBack(
      options.atomic_flush ? "DBImpl::AtomicFlushMemTables:AfterScheduleFlush"
                           : "DBImpl::FlushMemTable:AfterScheduleFlush",
      [&](void*) { sleeping.WakeUp(); });
  std::thread closer([&] { Close(); });
  sleeping.WaitUntilDone();
  closer.join();
  ASSERT_EQ(converts_.load(), 2);
  ASSERT_EQ(builds_.load(), 0);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("head"), "1");
  ASSERT_EQ(Get("tail"), "2");
  Close();
}

INSTANTIATE_TEST_CASE_P(
    Formats, DBMemtableConvertTest,
    ::testing::Combine(::testing::Bool(),
                       ::testing::Values("kDumpMem", "kFileMmap"),
                       ::testing::Bool()));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
