// Copyright (c) 2026-present, Topling Inc.
// WAL-backed memtable indexes preserve offsets when recovering prepared transactions.

#include <cerrno>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <topling/side_plugin_factory.h>
#include <topling/builtin_table_factory.h>

#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/transaction_db.h"

#if !defined(OS_WIN)
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace ROCKSDB_NAMESPACE {

extern MemTableRepFactory* NewCSPPMemTabForPlain(const std::string&);
std::shared_ptr<MemTableRepFactory> EasyNewMemTableRep(Slice cls, Slice js);

namespace {

#if !defined(OS_WIN)
Options LogIndexOptions(bool two_queues, bool osl) {
  Options options;
  options.create_if_missing = true;
  options.memtable_as_log_index = true;
  options.memtable_crash_safe_recover = false;
  options.disable_auto_compactions = true;
  options.two_write_queues = two_queues;
  const char* js = R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem"})";
  if (osl) {
    options.memtable_factory = EasyNewMemTableRep("OffsetSkipList", js);
  } else {
    options.memtable_factory.reset(NewCSPPMemTabForPlain(js));
  }
  SidePluginRepo repo;
  repo.Put("default", options.table_factory);
  repo.Put("converted", PluginFactorySP<TableFactory>::AcquirePlugin(
      osl ? "OffsetSkipListTable" : "CSPPMemTabTable", json::parse(js), repo));
  options.table_factory = PluginFactorySP<TableFactory>::AcquirePlugin(
      "Dispatch", {{"default", "$default"}}, repo);
  DispatcherTableBackPatch(options.table_factory.get(), repo);
  return options;
}

// Initialize DB and background threads after exec, never in the forked child.
int RunCrashChild(const std::string& dbname, const char* scenario, const std::string& arg) {
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
#endif

}  // namespace

class DBMemTableAsLogIndexTest : public DBTestBase {
 public:
  DBMemTableAsLogIndexTest()
      : DBTestBase("db_memtable_as_log_index_test", /*env_do_fsync=*/false) {}
};

#if !defined(OS_WIN)
TEST_F(CrashChild, DISABLED_WriteCommittedPreparedLogDataRecovery) {
  const bool two_queues = arg_.find("two_queues") != std::string::npos;
  const bool osl = arg_.find("osl") != std::string::npos;
  Options options = LogIndexOptions(two_queues, osl);
  TransactionDBOptions txn_opts;
  txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
  ASSERT_OK(txn_db->Put(WriteOptions(), "seed", "seed-value"));
  TransactionOptions to;
  to.skip_prepare = false;
  Transaction* txn = txn_db->BeginTransaction(WriteOptions(), to);
  ASSERT_OK(txn->SetName("logdata-in-doubt"));
  const std::string before(128, 'a');
  const std::string after(193, 'b');
  const std::string last(257, 'c');
  const std::string log1(17, 'L');
  const std::string log2(41, 'M');
  ASSERT_OK(txn->Put("before_log", before));
  txn->PutLogData(Slice(log1));
  ASSERT_OK(txn->Put("after_log", after));
  txn->PutLogData(Slice(log2));
  ASSERT_OK(txn->Put("last", last));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn_db->Put(WriteOptions(), "after_prepare", "advanced"));
  ::_exit(0);
}

TEST_F(DBMemTableAsLogIndexTest, WriteCommittedPreparedLogDataRecovery) {
  Close();
  const std::string before(128, 'a');
  const std::string after(193, 'b');
  const std::string last(257, 'c');
  for (bool two_queues : {false, true}) {
    for (bool osl : {false, true}) {
      SCOPED_TRACE(two_queues);
      SCOPED_TRACE(osl);
      Options options = LogIndexOptions(two_queues, osl);
      Destroy(options);
      std::string arg = two_queues ? "two_queues" : "one_queue";
      arg += osl ? "_osl" : "_cspp";
      ASSERT_EQ(RunCrashChild(dbname_, "WriteCommittedPreparedLogDataRecovery", arg), 0);
      TransactionDBOptions txn_opts;
      txn_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
      TransactionDB* txn_db = nullptr;
      ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
      std::string value;
      ASSERT_OK(txn_db->Get(ReadOptions(), "seed", &value));
      ASSERT_EQ(value, "seed-value");
      ASSERT_OK(txn_db->Get(ReadOptions(), "after_prepare", &value));
      ASSERT_EQ(value, "advanced");
      ASSERT_TRUE(txn_db->Get(ReadOptions(), "before_log", &value).IsNotFound());
      ASSERT_TRUE(txn_db->Get(ReadOptions(), "after_log", &value).IsNotFound());
      ASSERT_TRUE(txn_db->Get(ReadOptions(), "last", &value).IsNotFound());
      std::vector<Transaction*> prepared;
      txn_db->GetAllPreparedTransactions(&prepared);
      ASSERT_EQ(prepared.size(), 1U);
      ASSERT_EQ(prepared[0]->GetName(), "logdata-in-doubt");
      ASSERT_OK(prepared[0]->Commit());
      ASSERT_OK(txn_db->Get(ReadOptions(), "before_log", &value));
      ASSERT_EQ(value, before);
      ASSERT_OK(txn_db->Get(ReadOptions(), "after_log", &value));
      ASSERT_EQ(value, after);
      ASSERT_OK(txn_db->Get(ReadOptions(), "last", &value));
      ASSERT_EQ(value, last);
      delete prepared[0];
      delete txn_db;
      ASSERT_OK(TransactionDB::Open(options, txn_opts, dbname_, &txn_db));
      ASSERT_OK(txn_db->Get(ReadOptions(), "before_log", &value));
      ASSERT_EQ(value, before);
      ASSERT_OK(txn_db->Get(ReadOptions(), "after_log", &value));
      ASSERT_EQ(value, after);
      ASSERT_OK(txn_db->Get(ReadOptions(), "last", &value));
      ASSERT_EQ(value, last);
      prepared.clear();
      txn_db->GetAllPreparedTransactions(&prepared);
      ASSERT_TRUE(prepared.empty());
      delete txn_db;
    }
  }
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
    return result == 0 ? 126 : 125;
  }
#endif
  return RUN_ALL_TESTS();
}
