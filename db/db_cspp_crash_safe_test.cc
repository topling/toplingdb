//  Copyright (c) 2026-present, Topling Inc.
//  Crash-safe leftover recover: Convert + WAL tail, sync-point injection.

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <typeinfo>
#include <vector>

#include <topling/side_plugin_factory.h>

#include <terark/fsa/cspptrie.inl>
#include <terark/fsa/dfa_mmap_header.hpp>
#include <terark/offset_skiplist.hpp>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/pre_release_callback.h"
#include "file/filename.h"
#include "file/file_util.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/io_status.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/wal_filter.h"
#include "table/get_context.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
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
  if (options.memtable_factory) {
    options.memtable_factory->ListCrashSafeLeftovers(dir, &leftovers);
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

TEST_F(DBCsppCrashSafeTest, CrashSafeRequiresFileMmap) {
  for (const char* cls : {"CSPPMemTab", "OffsetSkipList"}) {
    for (const char* mode : {"kDontConvert", "kDumpMem", "kFileMmap"}) {
      SCOPED_TRACE(cls);
      SCOPED_TRACE(mode);
      const SidePluginRepo repo;
      auto factory = PluginFactorySP<MemTableRepFactory>::AcquirePlugin(
          cls, {{"convert_to_sst", mode}}, repo);
      ASSERT_EQ(factory->SupportCrashSafe(), std::string(mode) == "kFileMmap");
    }
  }
}

TEST_F(DBCsppCrashSafeTest, DangerousFactoryUpdate) {
  Close();
  const SidePluginRepo repo;
  const json query = {{"html", false}};
  auto check = [&](const auto& factory, const auto* manip, bool allowed) {
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
    ASSERT_EQ(state()["convert_to_sst"], "kFileMmap");
    ASSERT_EQ(state()["allow_dangerous_update"], allowed);
    ASSERT_OK(update({{"token_use_idle", false}}));
    ASSERT_EQ(state()["token_use_idle"], false);
    ASSERT_OK(update({{"convert_to_sst", "kFileMmap"}}));

    const json before = state();
    Status s = update({{"allow_dangerous_update", !allowed},
                       {"token_use_idle", true}, {"populate_read", false}});
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_NE(s.ToString().find("cannot be changed online"), std::string::npos);
    ASSERT_EQ(state(), before);
    s = update({{"allow_dangerous_update", !allowed},
                {"convert_to_sst", "kDontConvert"}});
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_EQ(state(), before);
    s = update({{"convert_to_sst", "invalid"}, {"token_use_idle", true}});
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_EQ(state(), before);
    ASSERT_OK(update({{"allow_dangerous_update", allowed},
                      {"convert_to_sst", "kFileMmap"}}));
    if (!allowed) {
      s = update({{"convert_to_sst", "kDontConvert"},
                  {"token_use_idle", true}, {"populate_read", false}});
      ASSERT_TRUE(s.IsInvalidArgument());
      ASSERT_NE(s.ToString().find("allow_dangerous_update=true"), std::string::npos);
      ASSERT_EQ(state(), before);
      return;
    }
    ASSERT_OK(update({{"convert_to_sst", "kDumpMem"}}));
    ASSERT_EQ(state()["convert_to_sst"], "kDumpMem");
    ASSERT_OK(update({{"allow_dangerous_update", true},
                      {"convert_to_sst", "kDontConvert"}}));
    ASSERT_EQ(state()["convert_to_sst"], "kDontConvert");
    ASSERT_OK(update({{"convert_to_sst", "kFileMmap"}}));
    ASSERT_EQ(state()["convert_to_sst"], "kFileMmap");
    ASSERT_EQ(state()["allow_dangerous_update"], true);
  };
  for (bool allowed : {false, true}) {
    SCOPED_TRACE(allowed);
    json params = {{"mem_cap", 16777216}, {"convert_to_sst", "kFileMmap"}};
    if (allowed) params["allow_dangerous_update"] = true;
    for (const char* cls : {"CSPPMemTab", "OffsetSkipList"}) {
      SCOPED_TRACE(cls);
      auto factory = PluginFactorySP<MemTableRepFactory>::AcquirePlugin(
          cls, params, repo);
      auto* manip = PluginManip<MemTableRepFactory>::AcquirePlugin(cls, {}, repo);
      check(factory, manip, allowed);
    }
    for (const char* cls : {"CSPPMemTabTable", "OffsetSkipListTable"}) {
      SCOPED_TRACE(cls);
      auto factory = PluginFactorySP<TableFactory>::AcquirePlugin(cls, params, repo);
      auto* manip = PluginManip<TableFactory>::AcquirePlugin(cls, {}, repo);
      check(factory, manip, allowed);
    }
  }
}

TEST_F(DBCsppCrashSafeTest, DangerousUpdateAffectsOnlyNewMemtables) {
  Close();
  const SidePluginRepo repo;
  InternalKeyComparator icmp(BytewiseComparator());
  MemTable::KeyComparator cmp(icmp);
  for (const char* cls : {"CSPPMemTab", "OffsetSkipList"}) {
    SCOPED_TRACE(cls);
    Arena arena;
    auto factory = PluginFactorySP<MemTableRepFactory>::AcquirePlugin(
        cls, {{"mem_cap", 16777216}, {"allow_dangerous_update", true}}, repo);
    auto* manip = PluginManip<MemTableRepFactory>::AcquirePlugin(cls, {}, repo);
    std::unique_ptr<MemTableRep> before(
        factory->CreateMemTableRep(cmp, &arena, nullptr, nullptr));
    ASSERT_FALSE(before->SupportConvertToSST());
    manip->Update(factory.get(), {}, {{"convert_to_sst", "kDumpMem"}}, repo);
    std::unique_ptr<MemTableRep> after(
        factory->CreateMemTableRep(cmp, &arena, nullptr, nullptr));
    ASSERT_FALSE(before->SupportConvertToSST());
    ASSERT_TRUE(after->SupportConvertToSST());
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
        icmp, ioptions, moptions, &wb, kMaxSequenceNumber, 0));
    ASSERT_OK(mem->Add(1, kTypeValue, "key", "old", nullptr));
    ASSERT_OK(mem->Add(3, kTypeValue, "key", "new", nullptr));
    ASSERT_OK(mem->Add(4, kTypeValue, "ghost", "unpublished", nullptr));
    mem->MarkImmutable();
    const auto leftovers = ListLeftovers(options, dbname_);
    ASSERT_EQ(leftovers.size(), 1U);
    const std::string leftover = leftovers[0] + ".recovery";
    CopyFile(leftovers[0], leftover);
    mem.reset();

    IntTblPropCollectorFactories collectors;
    TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                            options.compression, options.compression_opts, 0,
                            "default", 0);
    FileMetaData meta;
    meta.fd = FileDescriptor(1, 0, 0);
    meta.fd.smallest_seqno = 0;
    // The published bound need not be the sequence of any physical entry.
    meta.fd.largest_seqno = 2;
    ASSERT_OK(options.memtable_factory->RecoverCrashSafeMemTableToSST(
        leftover, &meta, tbo));
    ASSERT_GT(meta.fd.GetFileSize(), 0U);
    ASSERT_EQ(meta.fd.smallest_seqno, 0U);
    ASSERT_EQ(meta.fd.largest_seqno, 2U);

    const std::string fname = TableFileName(options.cf_paths, 1, 0);
    for (SequenceNumber limit : {meta.fd.largest_seqno, SequenceNumber(4),
                                 SequenceNumber(0)}) {
      SCOPED_TRACE(limit);
      std::unique_ptr<FSRandomAccessFile> file;
      ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(
          fname, FileOptions(), &file, nullptr));
      std::unique_ptr<RandomAccessFileReader> reader(
          new RandomAccessFileReader(std::move(file), fname));
      EnvOptions env_options;
      TableReaderOptions tro(ioptions, options.prefix_extractor, env_options,
                             icmp, 0);
      // Reopen the same bytes with a different FileDescriptor visibility bound.
      FileDescriptor fd = meta.fd;
      fd.largest_seqno = limit;
      tro.largest_seqno = fd.largest_seqno;
      std::unique_ptr<TableReader> table;
      ASSERT_OK(options.table_factory->NewTableReader(
          ReadOptions(), tro, std::move(reader), fd.GetFileSize(), &table,
          true));
      for (const char* key : {"key", "ghost"}) {
        PinnableSlice value;
        GetContext get_context(
            options.comparator, nullptr, nullptr, nullptr, GetContext::kNotFound,
            key, &value, nullptr, nullptr, nullptr, true, nullptr, nullptr);
        InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
        ASSERT_OK(table->Get(ReadOptions(), ikey.Encode(), &get_context,
                             nullptr));
        const bool visible = limit == 4 || (limit == 2 && key[0] == 'k');
        ASSERT_EQ(get_context.State(),
                  visible ? GetContext::kFound : GetContext::kNotFound);
        if (visible) {
          ASSERT_EQ(value.ToString(), key[0] == 'g' ? "unpublished"
                                     : limit == 2 ? "old" : "new");
        }
      }
    }
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
          icmp, ioptions, moptions, &wb, kMaxSequenceNumber, 0);
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
      const auto leftovers = ListLeftovers(options, dbname_);
      ASSERT_EQ(leftovers.size(), 1U);
      const std::string leftover = leftovers[0] + ".iterator";
      CopyFile(leftovers[0], leftover);
      mem.reset();
      IntTblPropCollectorFactories collectors;
      TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                              options.compression, options.compression_opts, 0,
                              "default", 0);
      FileMetaData meta;
      meta.fd = FileDescriptor(1, 0, 0);
      meta.fd.smallest_seqno = 0;
      meta.fd.largest_seqno = 4;
      ASSERT_OK(options.memtable_factory->RecoverCrashSafeMemTableToSST(
          leftover, &meta, tbo));
      const std::string fname = TableFileName(options.cf_paths, 1, 0);
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
        std::unique_ptr<TableReader> table;
        ASSERT_OK(options.table_factory->NewTableReader(
            ReadOptions(), tro, std::move(reader), meta.fd.GetFileSize(),
            &table, true));
        std::vector<std::string> expected;
        for (const auto& key : physical) {
          if (GetInternalKeySeqno(key) <= limit) expected.push_back(key);
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
                ASSERT_LE(GetInternalKeySeqno(it->key()), limit);
              }
            }
          }
        }
      }
    }
  }
}

TEST_F(DBCsppCrashSafeTest, ConvertedTableVisibilityFilter) {
  Close();
  for (bool osl : {false, true}) {
    for (bool file_mmap : {false, true}) {
      for (const auto& marker : {
               std::make_pair("", false), {"VisFilter:0", false},
               {"VisFilter:10", false}, {"XVisFilter:1", false},
               {"Other:VisFilter:1", false}, {"VisFilter:1x", false},
               {"VisFilter:1", true}, {"Other:0;VisFilter:1", true},
               {"Other:0;VisFilter:1;Tail:0", true}}) {
        SCOPED_TRACE(osl ? "OSL" : "CSPP");
        SCOPED_TRACE(file_mmap);
        SCOPED_TRACE(marker.first);
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
            icmp, ioptions, moptions, &wb, kMaxSequenceNumber, 0);
        ASSERT_OK(mem->Add(1, kTypeValue, "a", "1", nullptr));
        ASSERT_OK(mem->Add(2, kTypeValue, "b", "2", nullptr));
        ASSERT_OK(mem->Add(3, kTypeValue, "b", "3", nullptr));
        mem->MarkImmutable();
        IntTblPropCollectorFactories collectors;
        TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                                options.compression, options.compression_opts, 0,
                                "default", 0);
        FileMetaData meta;
        meta.fd = FileDescriptor(1, 0, 0);
        meta.fd.smallest_seqno = 1;
        meta.fd.largest_seqno = 3;
        SyncPoint::GetInstance()->SetCallBack(
            "PropertyBlockBuilder::AddTableProperty:Start", [marker](void* p) {
              auto& opts = static_cast<TableProperties*>(p)->compression_options;
              ASSERT_EQ(opts.find("VisFilter:"), std::string::npos);
              opts += marker.first;
            });
        SyncPoint::GetInstance()->EnableProcessing();
        const Status converted = mem->ConvertToSST(&meta, tbo);
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        ASSERT_OK(converted);
        ASSERT_GT(meta.fd.GetFileSize(), 0U);
        mem.reset();

        const std::string fname = TableFileName(options.cf_paths, 1, 0);
        const std::type_info* unfiltered_type = nullptr;
        for (SequenceNumber limit : {kMaxSequenceNumber, meta.fd.largest_seqno}) {
          std::unique_ptr<FSRandomAccessFile> file;
          ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(
              fname, FileOptions(), &file, nullptr));
          auto reader = std::make_unique<RandomAccessFileReader>(std::move(file), fname);
          EnvOptions env_options;
          TableReaderOptions tro(ioptions, options.prefix_extractor, env_options,
                                 icmp, 0);
          tro.largest_seqno = limit;
          std::unique_ptr<TableReader> table;
          ASSERT_OK(options.table_factory->NewTableReader(
              ReadOptions(), tro, std::move(reader), meta.fd.GetFileSize(),
              &table, true));
          std::unique_ptr<InternalIterator> it(table->NewIterator(
              ReadOptions(), nullptr, nullptr, false,
              TableReaderCaller::kUserIterator));
          if (limit == kMaxSequenceNumber) {
            unfiltered_type = &typeid(*it);
          } else {
            ASSERT_NE(unfiltered_type, nullptr);
            // A finite file maximum alone must not select VisibleIter.
            ASSERT_EQ(typeid(*it) == *unfiltered_type, !marker.second);
          }
          size_t count = 0;
          for (it->SeekToFirst(); it->Valid(); it->Next()) ++count;
          ASSERT_EQ(count, 3U);
        }
      }
    }
  }
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
      icmp, ioptions, moptions, &wb, kMaxSequenceNumber, 0));
  ASSERT_OK(mem->Add(1, kTypeValue, "key", "value", nullptr));
  const auto leftovers = ListLeftovers(options, dbname_);
  ASSERT_EQ(leftovers.size(), 1U);
  const std::string leftover = leftovers[0] + ".review-leak";
  CopyFile(leftovers[0], leftover);
  mem.reset();
  IntTblPropCollectorFactories collectors;
  TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                          options.compression, options.compression_opts, 0,
                          "default", 0);
  FileMetaData meta;
  meta.fd = FileDescriptor(1, 0, 0);
  meta.fd.smallest_seqno = 0;
  meta.fd.largest_seqno = 1;
  ASSERT_OK(options.memtable_factory->RecoverCrashSafeMemTableToSST(
      leftover, &meta, tbo));
  std::ifstream maps("/proc/self/maps");
  ASSERT_TRUE(maps.good());
  const auto fname = TableFileName(options.cf_paths, 1, 0);
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
        ASSERT_EQ(rec.generation & 1, after_open ? 0U : 1U);
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

TEST_F(DBCsppCrashSafeTest, SkipListPlusRecoverOpensAndUsesFullWal) {
  Close();
  Options options = CurrentOptions();
  options.memtable_crash_safe_recover = true;
  options.create_if_missing = true;
  Destroy(options);
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
    ASSERT_EQ(leftovers.size(), 1U);
    const int fd = ::open(leftovers[0].c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    // Header statistics are not a source of truth for WAL references.
    const size_t offset = config[0] == 'O'
        ? offsetof(terark::OSL_MmapHeader, reserved) + 2 * sizeof(uint32_t)
        : offsetof(terark::DFA_MmapHeader, reserved) + 4 * sizeof(uint32_t);
    uint64_t wal[3];  // fileno, cnt, bytes
    const ssize_t n = ::pread(fd, wal, sizeof(wal), offset);
    ::close(fd);
    ASSERT_EQ(n, static_cast<ssize_t>(sizeof(wal)));
    ASSERT_NE(wal[0], 0U);
    ASSERT_EQ(wal[1], 0U);
    ASSERT_EQ(wal[2], 0U);
    uint64_t wal_size = 0;
    ASSERT_OK(env_->GetFileSize(LogFileName(options.wal_dir, wal[0]), &wal_size));
    for (int reopen = 0; reopen < 2; ++reopen) {
      ASSERT_OK(TryReopen(options));
      ASSERT_GT(CountL0(db_), 0);
      ColumnFamilyMetaData cf_meta;
      db_->GetColumnFamilyMetaData(&cf_meta);
      ASSERT_EQ(cf_meta.blob_files.size(), 1U);
      ASSERT_EQ(cf_meta.blob_files[0].total_blob_count, 1U);
      ASSERT_EQ(cf_meta.blob_files[0].total_blob_bytes, wal_size);
      ASSERT_EQ(Get("0"), std::string(128, 'v'));
      ASSERT_EQ(Get("1"), std::string(128, 'v'));
      ASSERT_EQ(Get("inline"), "v");
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
  const auto leftovers = ListLeftovers(options, dbname_);
  ASSERT_EQ(leftovers.size(), 1U);
  int fd = ::open(leftovers[0].c_str(), O_RDWR);
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
  Options options = BaseCrashSafeOptions(dbname_, true, false);
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
    Options options = BaseCrashSafeOptions(dbname_, true, false);
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
      ASSERT_TRUE(ListLeftovers(options, dbname_).empty());
      ASSERT_OK(env_->FileExists(CrashSafePubSeqFileName(dbname_)));
      ASSERT_OK(TryReopen(options));
      ASSERT_EQ(Get("k1"), "v1");
      ASSERT_EQ(Get("k2"), "v2");
      ASSERT_GE(CountL0(db_), 1);
      Close();
      ASSERT_TRUE(ListLeftovers(options, dbname_).empty());
    }
  }
}
#endif

TEST_F(DBCsppCrashSafeTest, AvoidFlushDuringShutdownLeavesNoLeftoverThenWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  options.avoid_flush_during_shutdown = true;
  Destroy(options);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("k", "v"));
  Close();
  ASSERT_TRUE(ListLeftovers(options, dbname_).empty());
  ASSERT_OK(env_->FileExists(CrashSafePubSeqFileName(dbname_)));
  options.avoid_flush_during_shutdown = false;
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "v");
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
        ASSERT_EQ(rec.generation & 1, 1U);
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
  Options on = BaseCrashSafeOptions(dbname_, true, false);
  on.avoid_flush_during_shutdown = true;
  Destroy(on);
  ASSERT_OK(TryReopen(on));
  ASSERT_OK(Put("k", "v"));
  Close();
  ASSERT_OK(env_->FileExists(CrashSafePubSeqFileName(dbname_)));
  Options off = BaseCrashSafeOptions(dbname_, false, false);
  ASSERT_OK(TryReopen(off));
  ASSERT_TRUE(env_->FileExists(CrashSafePubSeqFileName(dbname_)).IsNotFound());
  ASSERT_EQ(Get("k"), "v");
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
#if !defined(__AVX__) || defined(__clang__)
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
#if !defined(__AVX__) || defined(__clang__)
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
  ASSERT_TRUE(ListLeftovers(options, dbname_).empty());
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

// Clang is excluded from the AVX store and still publishes through the odd
// generation, so it runs the same crash-injection tests as a non-AVX build.
#if !defined(__AVX__) || defined(__clang__)
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
#endif  // !__AVX__ || __clang__

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
        "CrashSafeRecover::SeekToFileOffset:InjectStatus",
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
  auto leftovers = ListLeftovers(options, dbname_);
  ASSERT_FALSE(leftovers.empty());
  const int fd = ::open(leftovers[0].c_str(), O_RDONLY);
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
  ASSERT_TRUE(ListLeftovers(options, dbname_).empty());
#if !defined(OS_WIN)
  ASSERT_EQ(RunCrashChild(dbname_, "LeftoverOnDbPathNotCfPaths0"), 1);
  auto leftovers_l0 = ListLeftovers(options, l0);
  ASSERT_FALSE(leftovers_l0.empty());
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("k"), "0");
  ASSERT_EQ(Get("pad"), "p");
  ASSERT_EQ(Get("x"), "1");
  for (const auto& leftover_path : leftovers_l0) {
    ASSERT_TRUE(env_->FileExists(leftover_path).IsNotFound());
  }
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

TEST_F(DBCsppCrashSafeTest, DontConvertFallsBackToWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
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

TEST_F(DBCsppCrashSafeTest, ListLeftoversAdvancesFileNumber) {
  for (bool osl : {false, true}) {
    SCOPED_TRACE(osl ? "OSL" : "CSPP");
    Close();
    Options options = BaseCrashSafeOptions(dbname_, false, false);
    if (osl) {
      SetupOsl(&options, true);
    }
    Destroy(options);
    ASSERT_OK(env_->CreateDirIfMissing(dbname_));
    const std::string prefix = dbname_ + (osl ? "/OffsetSkipList-" : "/cspp-");
    const std::string high = prefix + "000100.memtab-0";
    const std::string low = prefix + "000010.memtab-0";
    ASSERT_OK(WriteStringToFile(env_, "", high));
    ASSERT_OK(WriteStringToFile(env_, "", low));
    ASSERT_EQ(ListLeftovers(options, dbname_).size(), 2U);
    // Remove both so collision checks cannot hide a stale counter.
    ASSERT_OK(env_->DeleteFile(high));
    ASSERT_OK(env_->DeleteFile(low));
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(ListLeftovers(options, dbname_),
              std::vector<std::string>{prefix + "000101.memtab-0"});
    Close();
    ASSERT_TRUE(ListLeftovers(options, dbname_).empty());
    // Empty and lower-number scans must not move the counter backwards.
    ASSERT_OK(WriteStringToFile(env_, "", low));
    ASSERT_EQ(ListLeftovers(options, dbname_).size(), 1U);
    ASSERT_OK(env_->DeleteFile(low));
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(ListLeftovers(options, dbname_),
              std::vector<std::string>{prefix + "000102.memtab-0"});
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
  ASSERT_EQ(first.size(), 1U);
  std::string before;
  ASSERT_OK(ReadFileToString(env_, first[0], &before));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("other", "x"));
  auto after_list = ListLeftovers(options, dbname_);
  ASSERT_GE(after_list.size(), 2U);
  std::string after;
  ASSERT_OK(ReadFileToString(env_, first[0], &after));
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
  ASSERT_TRUE(ListLeftovers(options, dbname_).empty());
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
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "LeftoverNoMagicFallsBackToWal"), 1);
  auto leftovers = ListLeftovers(options, dbname_);
  ASSERT_FALSE(leftovers.empty());
  const int fd = ::open(leftovers[0].c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  terark::DFA_MmapHeader hdr{};
  ASSERT_EQ(::pread(fd, &hdr, sizeof(hdr), 0),
            static_cast<ssize_t>(sizeof(hdr)));
  std::memset(hdr.reserved, 0, sizeof(hdr.reserved));
  ASSERT_EQ(::pwrite(fd, &hdr, sizeof(hdr), 0),
            static_cast<ssize_t>(sizeof(hdr)));
  ::close(fd);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("bad"), "hdr");
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
  SyncPoint::GetInstance()->SetCallBack(
      "CrashSafeRecover::Truncate:InjectStatus", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("inject truncate");
      });
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("tr"), "ok");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CrashChild, DISABLED_LinkFileInjectFailureFallsBackToWal) {
  Options options = BaseCrashSafeOptions(dbname_, true, true);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "lk", "ok"));
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, LinkFileInjectFailureFallsBackToWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, true);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "LinkFileInjectFailureFallsBackToWal"), 1);
  SyncPoint::GetInstance()->EnableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "CrashSafeRecover::LinkFile:InjectStatus", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("inject link");
      });
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("lk"), "ok");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CrashChild, DISABLED_AfterRenameBeforeAddFileFallsBackToWal) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PersistPublishedSequence:AfterCommit",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* child_db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &child_db));
  ASSERT_OK(child_db->Put(WriteOptions(), "or", "phan"));
  ::_exit(0);
}

TEST_F(CrashChild, DISABLED_AfterRenameBeforeAddFileFallsBackToWalRecover) {
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  SyncPoint::GetInstance()->SetCallBack(
      "CrashSafeRecover::AfterRenameBeforeAddFile",
      [](void*) { ::_exit(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  DB* recover_db = nullptr;
  DB::Open(options, dbname_, &recover_db);
  ::_exit(0);
}

TEST_F(DBCsppCrashSafeTest, AfterRenameBeforeAddFileFallsBackToWal) {
  Close();
  Options options = BaseCrashSafeOptions(dbname_, true, false);
  Destroy(options);
  ASSERT_EQ(RunCrashChild(dbname_, "AfterRenameBeforeAddFileFallsBackToWal"), 1);
  SyncPoint::GetInstance()->EnableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(RunCrashChild(dbname_, "AfterRenameBeforeAddFileFallsBackToWalRecover"), 1);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("or"), "phan");
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

TEST_F(DBCsppCrashSafeTest, AfterRenameCloseSecondFlushInject) {
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
  std::string left1;
  for (const auto& p : ListLeftovers(options, dbname_)) {
    if (p.find(".memtab-1") != std::string::npos) {
      left1 = p;
      break;
    }
  }
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
  bool dropped_left = false;
  for (const auto& p : ListLeftovers(options, dbname_)) {
    if (p.find(".memtab-1") != std::string::npos) {
      dropped_left = true;
    }
  }
  ASSERT_TRUE(dropped_left);
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

// Clang is excluded from the AVX store and still hits AfterOddGeneration.
#if !defined(__AVX__) || defined(__clang__)
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
#endif  // !__AVX__ || __clang__

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
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::HIGH);
  sleeping_task.WaitUntilSleeping();
  FlushOptions fo;
  fo.wait = false;
  ASSERT_OK(db_->Flush(fo));
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTable:AfterScheduleFlush",
      [&](void*) { sleeping_task.WakeUp(); });
  SyncPoint::GetInstance()->EnableProcessing();
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
