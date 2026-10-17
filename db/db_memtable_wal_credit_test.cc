// Concurrent writers of one user key make one of them lose the memtable insert
// race. The loser has already accounted its first entry to the WAL slot when
// the winning entry takes the node, so the duplicate insert must consume that
// accounting rather than count the same slot, fileno and value size again.
//
// The race window is internal to the memtable rep, so this test can not pause
// in it; it runs the writers against a barrier and checks the invariant the
// memtable file exposes after a flush: the per-slot blob reference count equals
// the number of puts. A double count shows up as a count above the number of
// puts.

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "port/stack_trace.h"
#include "rocksdb/metadata.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "test_util/testharness.h"

#include <topling/side_plugin_repo.h>
#include <topling/builtin_table_factory.h>
#include <topling/side_plugin_factory.h>

namespace ROCKSDB_NAMESPACE {

extern MemTableRepFactory* NewCSPPMemTabForPlain(const std::string&);
std::shared_ptr<MemTableRepFactory> EasyNewMemTableRep(Slice cls, Slice js);

namespace {

constexpr size_t kThreads = 100;
constexpr size_t kRounds = 100;
constexpr size_t kValueSize = 128;

class DBMemtableWalCreditTest : public testing::Test {
 protected:
  // Writes one fresh key from every thread at once, round after round, so the
  // inserts of a round race on the same absent key.
  void Run(bool cspp) {
    const std::string dir =
        test::TmpDir() + "/wal_credit_" + (cspp ? "cspp" : "osl");
    Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;
    options.memtable_crash_safe_recover = true;
    options.memtable_as_log_index = true;
    options.avoid_flush_during_shutdown = false;
    options.avoid_flush_during_recovery = true;
    options.disable_auto_compactions = true;
    options.write_buffer_size = 64 << 20;
    options.max_write_buffer_number = 8;
    options.level0_file_num_compaction_trigger = 1 << 20;
    options.wal_dir = dir;
    // 100 writers each take a mempool chunk from the shared pool, so the
    // chunks must be small enough for all of them to fit in mem_cap.
    const json p = {{"mem_cap", 16777216},
                    {"chunk_size", 65536},
                    {"convert_to_sst", "kFileMmap"}};
    if (cspp) {
      options.memtable_factory.reset(NewCSPPMemTabForPlain(p.dump()));
    } else {
      options.memtable_factory = EasyNewMemTableRep("OffsetSkipList", p.dump());
    }
    const SidePluginRepo repo;
    options.table_factory = PluginFactorySP<TableFactory>::AcquirePlugin(
        cspp ? "CSPPMemTabTable" : "OffsetSkipListTable", p, repo);
    DestroyDB(dir, options);
    Env::Default()->DeleteDir(dir);

    DB* db = nullptr;
    ASSERT_OK(DB::Open(options, dir, &db));
    std::unique_ptr<DB> db_guard(db);

    // A spin barrier, not a condition variable: the writers must reach
    // `Put()` within the same few hundred nanoseconds, or the write thread's
    // group has no followers and nothing races.
    std::atomic<size_t> arrived{0};
    std::atomic<size_t> sense{0};
    auto barrier = [&] {
      const size_t s = sense.load(std::memory_order_acquire);
      if (arrived.fetch_add(1, std::memory_order_acq_rel) + 1 == kThreads) {
        arrived.store(0, std::memory_order_relaxed);
        sense.fetch_add(1, std::memory_order_release);
      } else {
        while (sense.load(std::memory_order_acquire) == s) {
          std::this_thread::yield();
        }
      }
    };

    const std::string value(kValueSize, 'v');
    std::vector<Status> statuses(kThreads);
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (size_t t = 0; t < kThreads; ++t) {
      threads.emplace_back([&, t] {
        for (size_t r = 0; r < kRounds; ++r) {
          const std::string key = "key-" + std::to_string(r);
          barrier();
          Status s = db->Put(WriteOptions(), key, value);
          if (!s.ok()) {
            statuses[t] = s;
          }
        }
      });
    }
    for (auto& t : threads) {
      t.join();
    }
    for (size_t t = 0; t < kThreads; ++t) {
      ASSERT_OK(statuses[t]);
    }

    ASSERT_OK(db->Flush(FlushOptions()));
    ColumnFamilyMetaData meta;
    db->GetColumnFamilyMetaData(&meta);
    const uint64_t puts = kThreads * kRounds;
    ASSERT_EQ(meta.blob_files.size(), 1U);
    ASSERT_EQ(meta.blob_files[0].total_blob_count, puts);
    ASSERT_EQ(meta.blob_files[0].total_blob_bytes, puts * kValueSize);
    std::string got;
    ASSERT_OK(db->Get(ReadOptions(), "key-" + std::to_string(kRounds - 1), &got));
    ASSERT_EQ(got, value);

    db_guard.reset();
    DestroyDB(dir, options);
    Env::Default()->DeleteDir(dir);
  }
};

TEST_F(DBMemtableWalCreditTest, CsppConcurrentSameKey) { Run(true); }
TEST_F(DBMemtableWalCreditTest, OffsetSkipListConcurrentSameKey) { Run(false); }

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
