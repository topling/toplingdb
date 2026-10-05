// Copyright (c) 2026-present, Topling Inc.
// Flush/Close conversion, independently of crash-safe recovery.

#include <atomic>
#include <thread>
#include <tuple>

#include <topling/side_plugin_factory.h>
#include <topling/builtin_table_factory.h>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"

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

TEST_P(DBMemtableConvertTest, ConversionSanitizesMergeThreshold) {
  // Avoid the existing atomic-flush sanitizer masking the conversion rule.
  if (std::get<2>(GetParam())) return;
  Options options = ConvertOptions();
  ASSERT_GT(options.min_write_buffer_number_to_merge, 1);
  DestroyAndReopen(options);
  ASSERT_EQ(db_->GetOptions().min_write_buffer_number_to_merge, 1);
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(options, "converted", &cf));
  ASSERT_EQ(db_->GetOptions(cf).min_write_buffer_number_to_merge, 1);
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf));
  Close();
}

TEST_P(DBMemtableConvertTest, NonConversionKeepsMergeThreshold) {
  // One run per plugin suffices for the disabled conversion and default
  // factory controls; the converting modes are covered independently above.
  if (std::get<2>(GetParam()) ||
      std::string(std::get<1>(GetParam())) != "kDumpMem") return;
  for (bool skip_list : {false, true}) {
    SCOPED_TRACE(skip_list);
    Options options = CurrentOptions();
    options.max_write_buffer_number = 8;
    options.min_write_buffer_number_to_merge = 2;
    options.atomic_flush = false;
    if (skip_list) {
      options.memtable_factory = std::make_shared<SkipListFactory>();
    } else {
      options.memtable_factory = PluginFactorySP<MemTableRepFactory>::AcquirePlugin(
          std::get<0>(GetParam()) ? "OffsetSkipList" : "CSPPMemTab",
          {{"mem_cap", 16777216}, {"convert_to_sst", "kDontConvert"}}, repo_);
    }
    DestroyAndReopen(options);
    ASSERT_EQ(db_->GetOptions().min_write_buffer_number_to_merge, 2);
    ColumnFamilyHandle* cf = nullptr;
    ASSERT_OK(db_->CreateColumnFamily(options, "plain", &cf));
    ASSERT_EQ(db_->GetOptions(cf).min_write_buffer_number_to_merge, 2);
    ASSERT_OK(db_->DestroyColumnFamilyHandle(cf));
    Close();
  }
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
