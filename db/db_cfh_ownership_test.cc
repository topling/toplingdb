//  Copyright (c) 2026-present, Topling Inc.
//  ColumnFamilyHandle ownership: the handles the caller gets from the core
//  API, the clones the Easy Migrate registry keeps, and the handles the repo
//  interface owns.

#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <topling/side_plugin_factory.h>
#include <topling/side_plugin_repo.h>

#include "file/file_util.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
namespace {

const char* child_dbname = nullptr;  // set by main() in the exec child

// Easy Migrate caches TOPLINGDB_EASY_MIGRATE_CONF and keeps one global
// SidePluginRepo per process, so each scenario runs in a fresh exec child, the
// same way db_cspp_crash_safe_test.cc runs its crash arms.  A child that does
// not reach its _exit(0) is reported as a failure.
int RunChild(const char* scenario, const std::string& dbname,
             const std::string& easy_conf) {
  if (easy_conf.empty()) {
    ::unsetenv("TOPLINGDB_EASY_MIGRATE_CONF");
  } else {
    ::setenv("TOPLINGDB_EASY_MIGRATE_CONF", easy_conf.c_str(), 1);
  }
  char exe[] = "/proc/self/exe";
  char flag[] = "--cfh-child";
  char* argv[] = {exe, flag, const_cast<char*>(scenario),
                  const_cast<char*>(dbname.c_str()), nullptr};
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

// Only turns the mechanism on: the scenarios do not depend on which factories
// are configured.
const char* kEasyConf = R"({
  "DBOptions": {"default": {"create_if_missing": true,
                            "create_missing_column_families": true,
                            "max_background_jobs": 2}},
  "CFOptions": {"default": {"write_buffer_size": 4194304,
                            "max_write_buffer_number": 2}},
  "http": {"auto_start_http": false}
})";

std::string FreshDir(const std::string& name) {
  const std::string path = test::TmpDir() + "/" + name;
  DestroyDir(Env::Default(), path).PermitUncheckedError();
  return path;
}

std::string WriteEasyConf(const std::string& path) {
  const std::string conf = path + ".easy-migrate.json";
  std::ofstream(conf, std::ios::trunc) << kEasyConf;
  return conf;
}

std::string WritePluginConf(const std::string& path) {
  std::string conf = R"({
  "DBOptions": {"default": {"create_if_missing": true,
                            "create_missing_column_families": true}},
  "CFOptions": {"default": {"write_buffer_size": 4194304}},
  "databases": {
    "cfh": {"method": "DB::Open",
            "params": {"db_options": "$default",
                       "column_families": {"default": "$default"},
                       "path": ")";
  conf += path;
  conf += R"("}}
  },
  "open": "cfh"
})";
  const std::string fname = path + ".plugin.json";
  std::ofstream fout(fname, std::ios::trunc);
  fout << conf;
  return fname;
}

class CfhChild : public ::testing::Test {
 protected:
  const std::string dbname_ = child_dbname ? child_dbname : "";
};

const std::shared_ptr<SidePluginRepo>& ERepo() {
  return GetEasyMigrateSidePluginRepo();
}

// Property views the easy registry holds: one per CF handle it owns.
size_t Views() {
  auto& repo = ERepo();
  return repo ? repo->m_impl->props.name2p->size() : 0;
}

Status OpenDB(const std::string& path, DB** db) {
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  return DB::Open(options, path, db);
}

DB_MultiCF* GetDbm(DB* db) {
  DB_MultiCF* dbm = nullptr;
  Status s;
  bool found = ERepo()->Get(db->GetName(), &dbm, &s);
  return found ? dbm : nullptr;
}

// ---------------------------------------------------------------------------
// easy migrate world: core API only
// ---------------------------------------------------------------------------

// The registry keeps a clone, so destroying the caller's handle is the death of
// a handle, not of a CF: the CF stays registered and CloseOneDB() frees the
// clone, never the address the caller already freed.
TEST_F(CfhChild, DISABLED_UserDestroyThenClose) {
  DB* db = nullptr;
  ASSERT_OK(OpenDB(dbname_, &db));
  const size_t v0 = Views();
  // The core deletes the handle it made for itself during DB::Open; the
  // registry's clone of the default CF must survive that.
  ASSERT_EQ(v0, 1U);
  ColumnFamilyHandle* h = nullptr;
  ASSERT_OK(db->CreateColumnFamily(ColumnFamilyOptions(), "c0", &h));
  ASSERT_EQ(Views(), v0 + 1);
  ASSERT_OK(db->Put(WriteOptions(), h, "k", "v"));
  delete h;  // rocksdb_column_family_handle_destroy() does exactly this
  ASSERT_EQ(Views(), v0 + 1);  // the CF is still in the database
  ASSERT_NE(GetDbm(db)->Get("c0"), nullptr);
  ASSERT_OK(ERepo()->CloseOneDB(db, false));
  ASSERT_EQ(Views(), 0U);
  delete db;  // CloseOneDB(false) keeps it alive
  ::_exit(0);
}

// CloseOneDB(db, true) deletes the database as well; the destructor hooks it
// triggers must not deadlock on the repo lock.
TEST_F(CfhChild, DISABLED_CloseOneTrue) {
  DB* db = nullptr;
  ASSERT_OK(OpenDB(dbname_, &db));
  ColumnFamilyHandle* h = nullptr;
  ASSERT_OK(db->CreateColumnFamily(ColumnFamilyOptions(), "c0", &h));
  delete h;
  ASSERT_OK(ERepo()->CloseOneDB(db, true));
  ASSERT_EQ(Views(), 0U);
  ::_exit(0);
}

// Core-created handle dropped through the core API: the registry holds a clone,
// which must be unregistered; the caller's own handle stays theirs.
TEST_F(CfhChild, DISABLED_UserHandleDropViaCore) {
  DB* db = nullptr;
  ASSERT_OK(OpenDB(dbname_, &db));
  DB_MultiCF* dbm = GetDbm(db);
  ASSERT_NE(dbm, nullptr);
  ColumnFamilyHandle* h = nullptr;
  ASSERT_OK(db->CreateColumnFamily(ColumnFamilyOptions(), "c0", &h));
  ASSERT_NE(dbm->Get("c0"), h);
  ASSERT_OK(db->DropColumnFamily(h));
  ASSERT_EQ(dbm->Get("c0"), nullptr);
  delete h;  // still the caller's handle
  ASSERT_OK(ERepo()->CloseOneDB(db, false));
  ASSERT_EQ(Views(), 0U);
  delete db;
  ::_exit(0);
}

// CloseAllDB(true): no iterator invalidation, no double free.
TEST_F(CfhChild, DISABLED_CloseAllTrue) {
  DB* db = nullptr;
  ASSERT_OK(OpenDB(dbname_, &db));
  ColumnFamilyHandle* h0 = nullptr;
  ColumnFamilyHandle* h1 = nullptr;
  ASSERT_OK(db->CreateColumnFamily(ColumnFamilyOptions(), "c0", &h0));
  ASSERT_OK(db->CreateColumnFamily(ColumnFamilyOptions(), "c1", &h1));
  delete h0;
  delete h1;
  ASSERT_EQ(Views(), 3U);  // handle deaths leave the CFs registered
  ERepo()->CloseAllDB(true);  // deletes the database too
  ASSERT_EQ(Views(), 0U);
  ::_exit(0);
}

// The caller destroys its handle after the repo closed the database: the
// registry entry is already gone, and a handle death alone changes nothing.
TEST_F(CfhChild, DISABLED_UserDestroyAfterClose) {
  DB* db = nullptr;
  ASSERT_OK(OpenDB(dbname_, &db));
  ColumnFamilyHandle* h = nullptr;
  ASSERT_OK(db->CreateColumnFamily(ColumnFamilyOptions(), "c0", &h));
  ASSERT_OK(ERepo()->CloseOneDB(db, false));
  ASSERT_EQ(Views(), 0U);
  delete h;  // the database is alive, its registry entry is already gone
  delete db;
  ::_exit(0);
}

// The original defect: TDengine destroys the handles of a database and then
// closes it, from several threads that also open other databases.  The
// registry used to keep the caller's handles, so a close could walk an address
// that another thread had already freed.
TEST_F(CfhChild, DISABLED_ConcurrentOpenClose) {
  constexpr int kThreads = 8;
  constexpr int kRounds = 20;
  ASSERT_OK(Env::Default()->CreateDirIfMissing(dbname_));
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      const std::string path = dbname_ + "/db" + std::to_string(t);
      // A fresh database: create the CF the way TDengine does, destroying the
      // handle while the database is still open.
      {
        DB* db = nullptr;
        Status s = OpenDB(path, &db);
        if (!s.ok()) {
          fprintf(stderr, "open %s: %s\n", path.c_str(), s.ToString().c_str());
          ++failures;
          return;
        }
        ColumnFamilyHandle* h = nullptr;
        s = db->CreateColumnFamily(ColumnFamilyOptions(), "tsdb.blk", &h);
        delete h;  // null when the create failed
        delete db;
        if (!s.ok()) {
          ++failures;
          return;
        }
      }
      for (int r = 0; r < kRounds; ++r) {
        std::vector<std::string> names;
        if (!DB::ListColumnFamilies(DBOptions(), path, &names).ok()) {
          ++failures;
          return;
        }
        std::vector<ColumnFamilyDescriptor> descs;
        descs.reserve(names.size());
        for (const auto& name : names) {
          descs.emplace_back(name, ColumnFamilyOptions());
        }
        std::vector<ColumnFamilyHandle*> handles;
        DB* db = nullptr;
        Options options;
        options.create_if_missing = true;
        options.create_missing_column_families = true;
        if (!DB::Open(options, path, descs, &handles, &db).ok()) {
          ++failures;
          return;
        }
        // TDengine's close order: the handles first, the database second.
        for (auto* h : handles) {
          delete h;
        }
        delete db;
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  ASSERT_EQ(failures.load(), 0);
  ::_exit(0);
}

// ---------------------------------------------------------------------------
// repo interface: no easy migrate
// ---------------------------------------------------------------------------

// Put() must clone: the handles passed in stay the caller's, CloseAllDB() must
// not delete them.
TEST_F(CfhChild, DISABLED_RepoPutDoesNotOwn) {
  ASSERT_EQ(ERepo().get(), nullptr);
  std::vector<ColumnFamilyDescriptor> descs = {
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions())};
  std::vector<ColumnFamilyHandle*> handles;
  DB* db = nullptr;
  Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  ASSERT_OK(DB::Open(options, dbname_, descs, &handles, &db));
  ASSERT_EQ(handles.size(), 1U);
  ASSERT_EQ(handles[0]->GetID(), 0U);

  SidePluginRepo repo;
  repo.Put("mydb", db, handles);
  repo.CloseAllDB(false);  // the repo's table goes away, the database stays
  ASSERT_EQ(handles[0]->GetID(), 0U);  // still the caller's handle
  ASSERT_OK(db->Put(WriteOptions(), handles[0], "k", "v"));
  ASSERT_OK(db->DestroyColumnFamilyHandle(handles[0]));
  delete db;
  ::_exit(0);
}

// Put(name, DB*) registers a plain database: the view of its default CF and the
// keep_default_cf[] entry are what CloseOneDB()/CloseAllDB() look up.
TEST_F(CfhChild, DISABLED_RepoPutPlainDB) {
  ASSERT_EQ(ERepo().get(), nullptr);
  ASSERT_OK(Env::Default()->CreateDirIfMissing(dbname_));
  DB* db1 = nullptr;
  DB* db2 = nullptr;
  ASSERT_OK(OpenDB(dbname_ + "/one", &db1));
  ASSERT_OK(OpenDB(dbname_ + "/two", &db2));
  SidePluginRepo repo;
  repo.Put("one", db1);
  repo.Put("two", db2);
  ASSERT_OK(repo.CloseOneDB(db1, false));
  repo.CloseAllDB(false);  // db2 leaves through keep_default_cf.at()
  ASSERT_OK(db1->Put(WriteOptions(), "k", "v"));  // neither database was closed
  ASSERT_OK(db2->Put(WriteOptions(), "k", "v"));
  delete db1;
  delete db2;
  ::_exit(0);
}

// The config opens the database through the repo: the constructor clones the
// handles DB::Open returned, and every handle the repo hands out is its own.
TEST_F(CfhChild, DISABLED_RepoOpenFromConfig) {
  ASSERT_EQ(ERepo().get(), nullptr);
  SidePluginRepo repo;
  ASSERT_OK(repo.ImportAutoFile(dbname_ + ".plugin.json"));
  DB_MultiCF* dbm = nullptr;
  ASSERT_OK(repo.OpenDB(&dbm));
  ASSERT_NE(dbm, nullptr);
  const size_t n0 = dbm->get_cf_handles_view().size();
  const size_t v0 = repo.m_impl->props.name2p->size();
  ASSERT_NE(dbm->Get("default"), dbm->db->DefaultColumnFamily());

  ColumnFamilyHandle* h = nullptr;
  ASSERT_OK(dbm->CreateColumnFamily("c0", "{}", &h));
  ASSERT_NE(dbm->Get("c0"), h);  // the repo keeps a clone, h is the caller's
  ASSERT_EQ(dbm->get_cf_handles_view().size(), n0 + 1);
  ASSERT_OK(dbm->db->Put(WriteOptions(), h, "k", "v"));
  // The caller owns h: destroying it must leave the registry intact.
  ASSERT_OK(dbm->db->DestroyColumnFamilyHandle(h));
  ASSERT_NE(dbm->Get("c0"), nullptr);

  ColumnFamilyHandle* h1 = nullptr;
  ASSERT_OK(dbm->CreateColumnFamily("c1", "{}", &h1));
  ASSERT_OK(dbm->DropColumnFamily("c0"));  // the repo destroys its clone
  ASSERT_OK(dbm->db->DestroyColumnFamilyHandle(h1));
  ASSERT_OK(dbm->DropColumnFamily("c1"));
  ASSERT_EQ(dbm->Get("c0"), nullptr);
  ASSERT_EQ(dbm->Get("c1"), nullptr);
  ASSERT_EQ(dbm->get_cf_handles_view().size(), n0);
  ASSERT_EQ(repo.m_impl->props.name2p->size(), v0);  // the views went with them

  // JNI dropCF() hands the name: the registry's own handle still has to be
  // destroyed, and the caller keeps the handle it was given.
  ColumnFamilyHandle* h2 = nullptr;
  ASSERT_OK(dbm->CreateColumnFamily("c2", "{}", &h2));
  ASSERT_EQ(repo.m_impl->props.name2p->size(), v0 + 1);
  ASSERT_OK(dbm->DropColumnFamily("c2"));
  ASSERT_EQ(dbm->get_cf_handles_view().size(), n0);
  ASSERT_EQ(repo.m_impl->props.name2p->size(), v0);
  ASSERT_OK(dbm->db->DestroyColumnFamilyHandle(h2));

  ColumnFamilyHandle* h3 = nullptr;
  ASSERT_OK(dbm->CreateColumnFamily("c3", "{}", &h3));
  ASSERT_OK(dbm->DropColumnFamily(h3));  // handle overload
  ASSERT_EQ(dbm->get_cf_handles_view().size(), n0);
  ASSERT_EQ(repo.m_impl->props.name2p->size(), v0);
  ASSERT_OK(dbm->db->DestroyColumnFamilyHandle(h3));

  // drop through the registry's own handle: the repo must still destroy it,
  // otherwise the leak trips ~ColumnFamilySet()'s assert(last_ref) below.
  ColumnFamilyHandle* h4 = nullptr;
  ASSERT_OK(dbm->CreateColumnFamily("c4", "{}", &h4));
  ASSERT_OK(dbm->DropColumnFamily(dbm->Get("c4")));
  ASSERT_EQ(dbm->Get("c4"), nullptr);
  ASSERT_EQ(repo.m_impl->props.name2p->size(), v0);
  ASSERT_OK(dbm->db->DestroyColumnFamilyHandle(h4));

  repo.CloseAllDB(true);  // destroys the remaining handles, then the database
  ::_exit(0);
}

// ---------------------------------------------------------------------------
// parent: one fresh child process per scenario
// ---------------------------------------------------------------------------

TEST(DBCfhOwnershipTest, UserDestroyThenClose) {
  const std::string path = FreshDir("cfh_user_destroy_then_close");
  ASSERT_EQ(RunChild("UserDestroyThenClose", path, WriteEasyConf(path)), 0);
}

TEST(DBCfhOwnershipTest, CloseOneTrue) {
  const std::string path = FreshDir("cfh_close_one_true");
  ASSERT_EQ(RunChild("CloseOneTrue", path, WriteEasyConf(path)), 0);
}

TEST(DBCfhOwnershipTest, UserHandleDropViaCore) {
  const std::string path = FreshDir("cfh_user_handle_drop_via_core");
  ASSERT_EQ(RunChild("UserHandleDropViaCore", path, WriteEasyConf(path)), 0);
}

TEST(DBCfhOwnershipTest, CloseAllTrue) {
  const std::string path = FreshDir("cfh_close_all_true");
  ASSERT_EQ(RunChild("CloseAllTrue", path, WriteEasyConf(path)), 0);
}

TEST(DBCfhOwnershipTest, UserDestroyAfterClose) {
  const std::string path = FreshDir("cfh_user_destroy_after_close");
  ASSERT_EQ(RunChild("UserDestroyAfterClose", path, WriteEasyConf(path)), 0);
}

TEST(DBCfhOwnershipTest, ConcurrentOpenClose) {
  const std::string path = FreshDir("cfh_concurrent_open_close");
  ASSERT_EQ(RunChild("ConcurrentOpenClose", path, WriteEasyConf(path)), 0);
  ASSERT_OK(DestroyDir(Env::Default(), path));
}

TEST(DBCfhOwnershipTest, RepoPutDoesNotOwn) {
  const std::string path = FreshDir("cfh_repo_put_does_not_own");
  ASSERT_EQ(RunChild("RepoPutDoesNotOwn", path, ""), 0);
}

TEST(DBCfhOwnershipTest, RepoPutPlainDB) {
  const std::string path = FreshDir("cfh_repo_put_plain_db");
  ASSERT_EQ(RunChild("RepoPutPlainDB", path, ""), 0);
}

TEST(DBCfhOwnershipTest, RepoOpenFromConfig) {
  const std::string path = FreshDir("cfh_repo_open_from_config");
  WritePluginConf(path);
  ASSERT_EQ(RunChild("RepoOpenFromConfig", path, ""), 0);
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
#if !defined(OS_WIN)
  if (argc == 4 && std::strcmp(argv[1], "--cfh-child") == 0) {
    ROCKSDB_NAMESPACE::child_dbname = argv[3];
    ::testing::GTEST_FLAG(filter) = std::string("CfhChild.DISABLED_") + argv[2];
    ::testing::GTEST_FLAG(also_run_disabled_tests) = true;
    ::alarm(300);
    const int result = RUN_ALL_TESTS();
    // every scenario must reach its explicit _exit(0)
    return result == 0 ? 126 : 125;
  }
#endif
  return RUN_ALL_TESTS();
}
