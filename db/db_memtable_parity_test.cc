// Copyright (c) 2026-present, Topling Inc.
//
// CSPP memtable vs OffsetSkipList memtable: the same sequence of operations
// must be externally observable in the same way. Everything the user can see is
// recorded into one log per run, and the two logs are compared line by line.
//
// Compared (`x.C(...)` below, the `cmp` channel):
//   - statuses of Put/Delete/SingleDelete/DeleteRange/Flush/CompactRange,
//   - Get results, iteration order and values (forward, backward, Seek),
//   - how many files exist, on which level, and with which user key bounds,
//   - per file: TableProperties::num_entries/num_deletions/num_range_deletions/
//     num_merge_operands/raw_key_size/raw_value_size, and the file level's
//     num_entries/num_deletions,
//   - which flush path ran (converted by the memtable vs built by the table
//     factory),
//   - the crash-safe recovery result, for the memtable_as_log_index both off
//     and on.
//
// Not compared (`x.I(...)`, the `info` channel: printed and diffed, so a
// by-design difference is visible, but never asserted):
//   - the byte layout of the two formats: file/block/index/filter sizes,
//     data_size/index_size/tag_size/gdic_size, num_data_blocks, format_version,
//     index flags, compression/filter/comparator/collector names, file numbers,
//     and user_collected_properties,
//   - identity and clocks: db_id/db_session_id/db_host_id, creation times,
//     file sequence numbers,
//   - estimates and anything derived from memory usage or timing.
//
// Each sequence runs twice per memtable first: a difference between two runs of
// one memtable is a flaky test, and would make the cross-memtable comparison
// meaningless, so it is reported separately.

#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "file/file_util.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "utilities/merge_operators.h"

// After the rocksdb headers, and repo before factory: builtin_table_factory.h
// needs both TableFactory and SidePluginRepo.
#include <topling/side_plugin_repo.h>
#include <topling/builtin_table_factory.h>
#include <topling/side_plugin_factory.h>

namespace ROCKSDB_NAMESPACE {

extern MemTableRepFactory* NewCSPPMemTabForPlain(const std::string&);
std::shared_ptr<MemTableRepFactory> EasyNewMemTableRep(Slice cls, Slice js);

namespace {

std::string Str(const char* fmt, ...) __attribute__((format(printf, 1, 2)));
std::string Str(const char* fmt, ...) {
  va_list ap, ap2;
  va_start(ap, fmt);
  va_copy(ap2, ap);
  // Sized by the real length: a sequence holds keys and values of any size, and
  // a silently truncated one would drop the steps after it.
  const int n = vsnprintf(nullptr, 0, fmt, ap);
  va_end(ap);
  std::string r(n > 0 ? static_cast<size_t>(n) : 0, '\0');
  if (n > 0) vsnprintf(r.data(), r.size() + 1, fmt, ap2);
  va_end(ap2);
  return r;
}

std::string Esc(Slice s) {
  std::string r = "\"";
  for (unsigned char c : s.ToString()) {
    if (c >= 32 && c < 127) {
      r += static_cast<char>(c);
    } else {
      r += Str("\\x%02x", c);
    }
  }
  return r + "\"";
}

std::vector<std::string> Split(const std::string& s) {
  std::vector<std::string> v;
  std::istringstream in(s);
  std::string line;
  while (std::getline(in, line)) v.push_back(line);
  return v;
}

// Points at the line that diverged, with a little context from both sides: a
// parity failure has to name the operation, not dump two logs.
std::string Diff(const std::string& a, const std::string& b) {
  const std::vector<std::string> la = Split(a), lb = Split(b);
  size_t i = 0;
  while (i < la.size() && i < lb.size() && la[i] == lb[i]) ++i;
  std::string r = Str("  first difference at line %zu (a: %zu lines, b: %zu)\n",
                      i + 1, la.size(), lb.size());
  for (size_t j = i; j < la.size() && j < i + 6; ++j) {
    r += Str("  a%-3zu | %s\n", j + 1, la[j].c_str());
  }
  r += "  ----\n";
  for (size_t j = i; j < lb.size() && j < i + 6; ++j) {
    r += Str("  b%-3zu | %s\n", j + 1, lb[j].c_str());
  }
  return r;
}

// The info channel: reported, never asserted.
void NoteInfoDiffs(const char* scenario, const std::string& a,
                   const std::string& b) {
  std::vector<std::string> la = Split(a), lb = Split(b);
  std::sort(la.begin(), la.end());
  std::sort(lb.begin(), lb.end());
  std::vector<std::string> only_a, only_b;
  std::set_difference(la.begin(), la.end(), lb.begin(), lb.end(),
                      std::back_inserter(only_a));
  std::set_difference(lb.begin(), lb.end(), la.begin(), la.end(),
                      std::back_inserter(only_b));
  if (only_a.empty() && only_b.empty()) return;
  fprintf(stdout, "[info-only] %s: %zu + %zu line(s) differ (not asserted)\n",
          scenario, only_a.size(), only_b.size());
  for (size_t i = 0; i < only_a.size() && i < 4; ++i) {
    fprintf(stdout, "  only-cspp: %s\n", only_a[i].c_str());
  }
  for (size_t i = 0; i < only_b.size() && i < 4; ++i) {
    fprintf(stdout, "  only-osl : %s\n", only_b[i].c_str());
  }
  fflush(stdout);
}

// ---------------------------------------------------------------------------
// One observation log
// ---------------------------------------------------------------------------

struct Obs {
  std::string cmp;  // must be identical between the two memtables
  std::string info; // may differ, by design
  int bad = 0;      // failing write statuses, for a crash child's exit code

  void C(const std::string& s) { cmp += s; cmp += '\n'; }
  void I(const std::string& s) { info += s; info += '\n'; }
};

// Every run has its own directory, by construction: it must not reach the log,
// or nothing would ever be deterministic. A failing status is the one thing
// that names it.
std::string CleanRun(std::string s, const std::string& dir) {
  for (size_t p = s.find(dir); !dir.empty() && p != std::string::npos;
       p = s.find(dir, p + 5)) {
    s.replace(p, dir.size(), "<db>");
  }
  return s;
}

// ---------------------------------------------------------------------------
// A run: options, DB, and the step interpreter
// ---------------------------------------------------------------------------

//   kCspp / kOsll:      one memtable, one rep.
//   kPair:              PairCheckMemTableRep holding cspp and osl at once, so
//                       every result the DB observes is cross checked in
//                       process and a divergence dies at the call that made
//                       it (sideplugin/topling-sst/src/table/
//                       pair_check_memtablerep.cc).
//   kCsppNoConvert:     cspp with convert_to_sst forced to kDontConvert: the
//                       baseline a pair run is compared against. The pair's
//                       primary rep is cspp and a pair never converts, so the
//                       two runs see the same configuration.
enum class Kind : int { kCspp = 0, kOsll = 1, kPair = 2, kCsppNoConvert = 3 };

const char* KindName(Kind k) {
  switch (k) {
    case Kind::kCspp: return "cspp";
    case Kind::kOsll: return "osl";
    case Kind::kPair: return "pair";
    case Kind::kCsppNoConvert: return "cspp-nc";
  }
  return "?";
}
Kind KindOf(const char* s) {
  if (strcmp(s, "cspp") == 0) return Kind::kCspp;
  if (strcmp(s, "osl") == 0) return Kind::kOsll;
  if (strcmp(s, "pair") == 0) return Kind::kPair;
  if (strcmp(s, "cspp-nc") == 0) return Kind::kCsppNoConvert;
  // Only ever called with a name KindName() produced.
  EXPECT_TRUE(false) << "bad kind: " << s;
  return Kind::kCspp;
}

// A pair of reps can not convert into one memtable file, so the pair never
// converts and is never crash safe: it always takes the plain flush path,
// whatever convert_to_sst the scenario asks for. Its baseline runs the same
// way, so the two are comparable line by line.
std::string ConvertOf(Kind k, const std::string& convert) {
  return (k == Kind::kPair || k == Kind::kCsppNoConvert) ? "kDontConvert"
                                                         : convert;
}

struct Scenario {
  std::string name;
  std::string convert;      // convert_to_sst
  std::string before;       // the writer's steps
  std::string after;        // the reader's steps, run after the writer's DB is
                            // reopened (only when crash)
  bool crash_safe = false;  // memtable_crash_safe_recover
  bool log_index = false;   // memtable_as_log_index
  bool crash = false;       // `before` runs in a child that _exit()s
  int expect_convert = -1;  // -1: don't check, 1: at least once
};

// The steps of the interpreter:
//   put <k> <v> | del <k> | sdel <k> | merge <k> <v> | dr <start> <end>
//   | get <k> | seek <k> | scan | rscan | flush | reopen | compact | props
//   | snap | sget <k> | sscan | rsnap
// A key given as - is the empty key, so `dr - m` covers everything below m.
//
// What a sequence is for is written above it: they are the sequences where the
// two memtables' implementations actually differ.

// A DB that is opened, driven, and closed in one process.
Scenario Plain(const char* name, const char* convert, const std::string& steps,
               int expect_convert = 1) {
  Scenario s;
  s.name = name;
  s.convert = convert;
  s.before = steps;
  s.expect_convert = expect_convert;
  return s;
}

// The writer runs in a child that _exit()s with the DB open, so the parent's
// Open() has to recover the range deletions.
Scenario Crash(const char* name, const char* convert, const std::string& before,
               const std::string& after, bool crash_safe, bool log_index = false) {
  Scenario s;
  s.name = name;
  s.convert = convert;
  s.before = before;
  s.after = after;
  s.crash = true;
  s.crash_safe = crash_safe;
  s.log_index = log_index;
  return s;
}

// Counters of the two flush paths, so a scenario can prove which one ran.
std::atomic<int> g_converted{0};
std::atomic<int> g_built{0};
std::atomic<bool> g_armed{false};

void ArmFlushPathCounters() {
  SyncPoint* sp = SyncPoint::GetInstance();
  sp->SetCallBack("FlushJob::ConvertToSST:Status",
                  [](void*) { ++g_converted; });
  sp->SetCallBack("FlushJob::WriteLevel0Table:s", [](void*) { ++g_built; });
  sp->EnableProcessing();
  g_armed = true;
  g_converted = 0;
  g_built = 0;
}

void DisarmFlushPathCounters() {
  SyncPoint* sp = SyncPoint::GetInstance();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  g_armed = false;
}

class Runner {
 public:
  Runner(Kind kind, const Scenario& sc, const std::string& dir, Obs* x)
      : kind_(kind), sc_(sc), dir_(dir), x_(x) {
    const std::string convert = ConvertOf(kind, sc.convert);
    const json p = {{"mem_cap", 16777216}, {"convert_to_sst", convert}};
    o_.create_if_missing = true;
    o_.error_if_exists = false;
    o_.env = Env::Default();
    o_.wal_dir = dir;
    // Same operator for both, so a merge operand read back from a file is
    // merged the same way (and shows up in num_merge_operands).
    o_.merge_operator = MergeOperators::CreateStringAppendOperator();
    // Recovery from a leftover memtable file needs a file backed factory: the
    // DB refuses memtable_crash_safe_recover for anything else (db/db_impl/
    // db_impl.cc), and the pair is never file backed. So a pair run and its
    // baseline recover from the WAL, like any non-file-backed memtable, and
    // the scenario's steps still run.
    o_.memtable_crash_safe_recover = sc.crash_safe && convert == "kFileMmap";
    o_.memtable_as_log_index = sc.log_index;
    o_.avoid_flush_during_shutdown = false;
    o_.avoid_flush_during_recovery = true;
    o_.write_buffer_size = 64 << 20;
    o_.max_write_buffer_number = 8;
    // A converted file is marked for compaction (db/flush_job.cc, the
    // ConvertToSST branch), so a background compaction would race the
    // observations below. The explicit `compact` steps drive compaction.
    o_.disable_auto_compactions = true;
    o_.level0_file_num_compaction_trigger = 1 << 20;
    // The CF's table factory is a Dispatch: the plain BlockBasedTable writes
    // the files a normal flush makes, and the memtable's own factory (the
    // "converted" entry) reads back the files the memtable wrote. This is the
    // shape the two sibling test files use, and the only shape in which
    // DeleteRange is not refused by the table factory's own flag.
    // The pair holds cspp as its primary rep and osl as the checked one, both
    // with the same params, so a pair run is cspp's observable behavior with
    // every result verified against osl as it is produced.
    const json pair = {{"a", {{"class", "CSPPMemTab"}, {"params", p}}},
                       {"b", {{"class", "OffsetSkipList"}, {"params", p}}}};
    const bool cspp = kind == Kind::kCspp || kind == Kind::kCsppNoConvert;
    o_.memtable_factory = kind == Kind::kPair
        ? EasyNewMemTableRep("PairCheckMemTableRep", pair.dump())
        : cspp ? PluginFactorySP<MemTableRepFactory>::AcquirePlugin(
                     "CSPPMemTab", p, repo_)
               : EasyNewMemTableRep("OffsetSkipList", p.dump());
    o_.table_factory.reset(NewBlockBasedTableFactory());
    repo_.Put("default", o_.table_factory);
    // A pair never converts, so its "converted" entry is never looked up.
    repo_.Put("converted",
              PluginFactorySP<TableFactory>::AcquirePlugin(
                  cspp ? "CSPPMemTabTable" : "OffsetSkipListTable", p, repo_));
    o_.table_factory = PluginFactorySP<TableFactory>::AcquirePlugin(
        "Dispatch", {{"default", "$default"}}, repo_);
    DispatcherTableBackPatch(o_.table_factory.get(), repo_);
    // A fresh dir, whatever a previous run left behind.
    DestroyDB(dir_, o_);
    Env::Default()->DeleteDir(dir_);
    ArmFlushPathCounters();
  }

  ~Runner() {
    if (db_ != nullptr) Close();
    DisarmFlushPathCounters();
    DestroyDB(dir_, o_);
    Env::Default()->DeleteDir(dir_);
  }

  Status Open() {
    const Status s = DB::Open(o_, dir_, &db_);
    x_->C("open -> " + S(s));
    if (!s.ok()) db_ = nullptr;
    return s;
  }

  void Close() {
    if (db_ == nullptr) return;
    if (snap_ != nullptr) {
      db_->ReleaseSnapshot(snap_);
      snap_ = nullptr;
      x_->C("close: released a live snapshot");
    }
    x_->C("close -> " + S(db_->Close()) +
          Str(" convert=%d build=%d", g_converted.load(), g_built.load()));
    delete db_;
    db_ = nullptr;
  }

  // The status of a call, with this run's directory out of it.
  std::string S(const Status& s) {
    return s.ok() ? std::string("OK") : CleanRun(s.ToString(), dir_);
  }

  // A failing write status is also the crash child's exit code.
  std::string W(const Status& s) {
    if (!s.ok()) ++x_->bad;
    return S(s);
  }

  std::string G(const std::string& k) {
    std::string v;
    const Status s = db_->Get(ReadOptions(), k, &v);
    if (s.IsNotFound()) return "NOT_FOUND";
    if (!s.ok()) return "ERR " + S(s);
    return Esc(v);
  }

  // The sequence is the test's input: the same text drives both memtables, so
  // a case is added by adding a line, not by adding code.
  void Run(const char* steps) {
    std::istringstream in(steps == nullptr ? "" : steps);
    std::string line;
    while (std::getline(in, line)) {
      if (line.empty() || line[0] == '#') continue;
      std::istringstream ls(line);
      std::string verb, a1, a2;
      ls >> verb >> a1 >> a2;
      if (a1 == "-") a1.clear();  // the empty key
      if (a2 == "-") a2.clear();
      Step(verb, a1, a2);
    }
  }

  int converted() const { return g_converted.load(); }
  int built() const { return g_built.load(); }

 private:
  void Step(const std::string& verb, const std::string& a1,
            const std::string& a2) {
    if (db_ == nullptr) {
      x_->C(verb + " " + a1 + " skipped: no db");
      return;
    }
    if (verb == "put") {
      x_->C("put " + a1 + " " + a2 + " -> " + W(db_->Put(WriteOptions(), a1, a2)));
    } else if (verb == "del") {
      x_->C("del " + a1 + " -> " + W(db_->Delete(WriteOptions(), a1)));
    } else if (verb == "sdel") {
      x_->C("sdel " + a1 + " -> " + W(db_->SingleDelete(WriteOptions(), a1)));
    } else if (verb == "dr") {
      x_->C("dr [" + a1 + "," + a2 + ") -> " +
            W(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), a1,
                               a2)));
    } else if (verb == "merge") {
      x_->C("merge " + a1 + " " + a2 + " -> " +
            W(db_->Merge(WriteOptions(), a1, a2)));
    } else if (verb == "snap") {
      if (snap_ == nullptr) snap_ = db_->GetSnapshot();
      x_->C(std::string("snap -> ") + (snap_ == nullptr ? "NULL" : "OK"));
    } else if (verb == "rsnap") {
      if (snap_ != nullptr) {
        db_->ReleaseSnapshot(snap_);
        snap_ = nullptr;
      }
      x_->C("rsnap -> OK");
    } else if (verb == "sget") {
      ReadOptions ro;
      ro.snapshot = snap_;
      std::string v;
      const Status s = db_->Get(ro, a1, &v);
      x_->C("sget " + a1 + " -> " +
            (s.IsNotFound() ? "NOT_FOUND" : s.ok() ? Esc(v) : "ERR " + S(s)));
    } else if (verb == "get") {
      x_->C("get " + a1 + " -> " + G(a1));
    } else if (verb == "flush") {
      x_->C(Str("flush -> %s convert=%d build=%d",
                W(db_->Flush(FlushOptions())).c_str(), g_converted.load(),
                g_built.load()));
    } else if (verb == "compact") {
      x_->C(Str("compact -> %s",
                W(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr))
                    .c_str()));
      Files("after-compact");
    } else if (verb == "reopen") {
      const Status cs = db_->Close();
      x_->C(Str("reopen: close -> %s convert=%d build=%d", S(cs).c_str(),
                g_converted.load(), g_built.load()));
      if (!cs.ok()) return;
      delete db_;
      db_ = nullptr;
      Open();
    } else if (verb == "scan" || verb == "rscan" || verb == "sscan") {
      ReadOptions ro;
      if (verb == "sscan") ro.snapshot = snap_;
      Scan(ro, verb.c_str());
    } else if (verb == "seek") {
      std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
      it->Seek(a1);
      x_->C("seek " + a1 + " -> " +
            (it->Valid() ? it->key().ToString() + " = " + Esc(it->value())
                         : std::string("END")));
    } else if (verb == "props") {
      Files("props");
    } else {
      x_->C("unknown verb " + verb);
    }
  }

  void Scan(const ReadOptions& ro, const char* tag) {
    const bool forward = strcmp(tag, "rscan") != 0;
    std::unique_ptr<Iterator> it(db_->NewIterator(ro));
    int n = 0;
    for (forward ? it->SeekToFirst() : it->SeekToLast(); it->Valid();
         forward ? it->Next() : it->Prev()) {
      x_->C(Str("%s %s = %s", tag, it->key().ToString().c_str(),
                Esc(it->value()).c_str()));
      ++n;
    }
    x_->C(Str("%s -> end n=%d %s", tag, n, S(it->status()).c_str()));
  }

  // Everything the user can learn about the files: how many, on which level,
  // their user key bounds, their logical counts, and (info only) their layout.
  void Files(const char* tag) {
    TablePropertiesCollection props;
    const Status s = db_->GetPropertiesOfAllTables(&props);
    if (!s.ok()) {
      x_->C(Str("%s props -> %s", tag, S(s).c_str()));
      return;
    }
    std::vector<std::string> cmp, info;
    for (const auto& kv : props) {
      const TableProperties& p = *kv.second;
      cmp.push_back(Str("%s f entries=%llu dels=%llu rds=%llu merges=%llu "
                        "keysz=%llu valsz=%llu",
                        tag, (unsigned long long)p.num_entries,
                        (unsigned long long)p.num_deletions,
                        (unsigned long long)p.num_range_deletions,
                        (unsigned long long)p.num_merge_operands,
                        (unsigned long long)p.raw_key_size,
                        (unsigned long long)p.raw_value_size));
      info.push_back(Str("%s i data=%llu index=%llu filter=%llu tag=%llu "
                         "gdic=%llu blocks=%llu fmt=%llu fkl=%llu "
                         "comp=%s fp=%s",
                         tag, (unsigned long long)p.data_size,
                         (unsigned long long)p.index_size,
                         (unsigned long long)p.filter_size,
                         (unsigned long long)p.tag_size,
                         (unsigned long long)p.gdic_size,
                         (unsigned long long)p.num_data_blocks,
                         (unsigned long long)p.format_version,
                         (unsigned long long)p.fixed_key_len,
                         p.compression_name.c_str(),
                         p.filter_policy_name.c_str()));
    }
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    std::string levels;
    for (const auto& f : files) {
      cmp.push_back(Str("%s fL%d entries=%llu dels=%llu [%s,%s]", tag, f.level,
                        (unsigned long long)f.num_entries,
                        (unsigned long long)f.num_deletions,
                        Esc(f.smallestkey).c_str(),
                        Esc(f.largestkey).c_str()));
      info.push_back(Str("%s iL%d size=%llu seq=%llu..%llu created=%llu",
                         tag, f.level, (unsigned long long)f.size,
                         (unsigned long long)f.smallest_seqno,
                         (unsigned long long)f.largest_seqno,
                         (unsigned long long)f.file_creation_time));
      levels += Str(" L%d", f.level);
    }
    std::sort(cmp.begin(), cmp.end());
    std::sort(info.begin(), info.end());
    std::sort(levels.begin(), levels.end());
    for (const auto& l : cmp) x_->C(l);
    for (const auto& l : info) x_->I(l);
    x_->C(Str("%s files=%zu levels=%s", tag, files.size(),
              levels.empty() ? "-" : levels.c_str() + 1));
    std::string v;
    // An estimate: on the info channel, like memory usage.
    if (db_->GetProperty("rocksdb.estimate-num-keys", &v)) {
      x_->I(Str("%s estimate-num-keys=%s", tag, v.c_str()));
    }
    if (db_->GetProperty("rocksdb.num-immutable-mem-table", &v)) {
      x_->C(Str("%s immutable-mem-tables=%s", tag, v.c_str()));
    }
  }

  Kind kind_;
  const Scenario& sc_;
  std::string dir_;
  Obs* x_;
  Options o_;
  SidePluginRepo repo_;
  DB* db_ = nullptr;
  const Snapshot* snap_ = nullptr;
};

// ---------------------------------------------------------------------------
// The scenarios
// ---------------------------------------------------------------------------

const char* kAfterCrash = R"(
props
get a
get d
get e
get z
scan
rscan
compact
scan
props
reopen
get d
scan
props
)";

// Keys with a shared prefix: the offset skiplist compresses common prefixes,
// so a few thousand of these reach code that unique short keys never do.
std::string CommonPrefixSteps() {
  std::string s = "# ks0000..ks0599, deletes, and three range deletions\n";
  for (int i = 0; i < 600; ++i) s += Str("put ks%04d v%03d\n", i, i % 11);
  for (int i = 0; i < 600; i += 7) s += Str("del ks%04d\n", i);
  s += "get ks0000\nget ks0150\nget ks0599\n";
  s += "dr ks0100 ks0200\ndr ks0300 ks0350\ndr ks0180 ks0220\n";
  s += "get ks0150\nget ks0250\nscan\nrscan\nflush\n";
  s += "get ks0100\nget ks0500\nseek ks0300\nreopen\nscan\nprops\n";
  s += "compact\nscan\nrscan\nprops\n";
  return s;
}

// Two memtables, so a range deletion written in the first file has to be seen
// by a reader of the second one.
std::string ManyKeysSteps() {
  std::string s = "# k0000..k1399 over two flushes\n";
  for (int i = 0; i < 700; ++i) s += Str("put k%04d v%d\n", i, i % 13);
  for (int i = 0; i < 700; i += 5) s += Str("del k%04d\n", i);
  s += "flush\n";
  for (int i = 700; i < 1400; ++i) s += Str("put k%04d v%d\n", i, i % 13);
  s += "dr k0100 k0130\ndr k0650 k0750\ndr k1300 k1400\n";
  s += "flush\nget k0100\nget k0120\nget k0700\nget k1399\n";
  s += "scan\nrscan\nreopen\nget k0000\nget k0699\nscan\nprops\n";
  s += "compact\nscan\nprops\n";
  return s;
}

const std::vector<Scenario>& Scenarios() {
  static const std::vector<Scenario> v = [] {
    std::vector<Scenario> v;
    // Point writes and point deletes around a range deletion, then everything
    // again from the files. sdel is on f: SingleDelete with a range deletion
    // over the same key is undefined in RocksDB itself, not a memtable issue.
    v.push_back(Plain("keys-and-deletes", "kDumpMem", R"(
put a 1
put d 4
put e 5
put f 6
put z 9
get d
sdel f
put f 6
put b 2
del b
get b
dr c e
get c
get d
get e
get f
get z
scan
flush
get c
get d
reopen
get a
get b
get c
get d
get e
get z
rscan
seek c
seek d
seek y
compact
scan
props
)"));

    // The last memtable holds nothing but a range deletion: the probe, the
    // empty-memtable check and the rd-only file are all on this path.
    v.push_back(Plain("rd-only-memtable", "kDumpMem", R"(
put a 1
flush
put d 4
flush
get d
dr c e
get d
flush
get a
get d
scan
reopen
get d
scan
compact
scan
props
)"));

    // The very first write of a fresh memtable is a range deletion, and an
    // unbounded start: the list, its head and its probe are created by the
    // range deletion path alone.
    v.push_back(Plain("rd-first-op", "kDumpMem", R"(
dr c e
get d
put d 4
get d
put a 1
get b
scan
flush
dr - m
get d
get e
scan
reopen
get a
get d
scan
compact
scan
props
)"));

    // Overlapping range deletions in one memtable, and writes that must win
    // over a range deletion written before them.
    v.push_back(Plain("overlapping-rd", "kDumpMem", R"(
put a 1
put d 4
put f 6
put y 7
put z 9
dr c e
dr c e
dr c z
get d
get f
get e
put f 6
get f
flush
reopen
scan
get z
compact
scan
props
)"));

    v.push_back(Plain("common-prefix-keys", "kDumpMem", CommonPrefixSteps()));

    v.push_back(Plain("many-keys-two-memtables", "kDumpMem", ManyKeysSteps()));

    // A snapshot taken before a range deletion must still see what the range
    // deletion hides -- in the memtable, and again from the converted file
    // once it is flushed, where the tombstone's sequence number is what
    // decides.
    v.push_back(Plain("snapshot-rd-visibility", "kDumpMem", R"(
put a 1
put d 4
put e 5
snap
dr c e
del a
get a
sget a
get d
sget d
scan
sscan
flush
sget d
sget e
sscan
rsnap
get d
scan
reopen
scan
props
compact
scan
props
)"));

    // Merge operands in the memtable, under a range deletion, and read back
    // from the converted file: num_merge_operands is compared too.
    v.push_back(Plain("merge-and-rd", "kFileMmap", R"(
merge a x1
merge a x2
merge b y1
get a
dr c e
merge d z1
get d
get a
flush
get a
get d
merge a x3
get a
reopen
get a
get d
scan
compact
scan
get a
props
)"));

    // The edges: an empty key, an empty value, a long value, a range deletion
    // of exactly one key, one that runs backwards, and one for equal keys.
    std::string e = R"(
get a
del a
get a
seek a
rscan
put e1
put e2
put -
get -
get e1
dr e1 e1x
get e1
dr c c
get e1
scan
put long )";
    e += std::string(1000, 'x');
    e += R"(
get long
flush
get -
get long
scan
reopen
get -
get long
scan
props
compact
scan
props
dr z a
)";
    v.push_back(Plain("edge-cases", "kDumpMem", e));

    // kFileMmap: the memtable writes its own file even without a crash, so the
    // reader of that file is on this path.
    v.push_back(Plain("filemmap-close-and-reopen", "kFileMmap", R"(
put a 1
put d 4
put e 5
dr c e
flush
get a
get d
get e
scan
reopen
get d
scan
props
compact
scan
props
)"));

    // A range deletion in the second memtable covers keys living in the file
    // the first memtable converted.
    v.push_back(Plain("rd-across-flush-boundary", "kFileMmap", R"(
put a 1
put b 2
put c 3
flush
put d 4
put e 5
dr b d
get a
get b
get c
get d
get e
flush
get b
get c
scan
reopen
scan
compact
scan
props
)"));

    // No own file format: the memtable still goes through ConvertToSST, which
    // then writes an ordinary table, so the convert counter is not a signal
    // here. What the memtable must still get right is the LSM itself.
    v.push_back(Plain("no-convert-many-flushes", "kDontConvert", R"(
put a 1
flush
put b 2
del a
flush
put c 3
put d 4
dr b d
flush
put e 5
get a
get b
get c
get d
get e
scan
reopen
get a
get c
get d
scan
compact
scan
props
)",
                         -1));

    v.push_back(Crash("crash-recover", "kFileMmap", R"(
put a 1
put d 4
put e 5
put z 9
dr c e
get d
)",
                      kAfterCrash, /*crash_safe=*/true));

    // Overlapping range deletions of the recovered memtable: the tombstone list
    // is rebuilt from the memtable's own tree, so an entry that is lost or
    // comes back in the wrong order changes what a later get sees. The puts
    // after the range deletions are newer, so those keys must come back.
    v.push_back(Crash("crash-recover-overlapping-rd", "kFileMmap", R"(
put a 1
put d 4
put f 6
put y 7
put z 9
dr c e
dr c e
dr c z
get d
get f
put e 5
put f 6
get e
get f
)",
                      kAfterCrash, /*crash_safe=*/true));

    // Without a memtable file, the recovery of the range deletion is a WAL
    // replay into a fresh memtable. crash_safe_recover is off: the DB refuses
    // it for a factory that is not file backed, so with it on every memtable
    // would fail to open and nothing would be replayed.
    v.push_back(Crash("crash-wal-replay", "kDontConvert", R"(
put a 1
put d 4
put e 5
put z 9
dr c e
get d
)",
                      kAfterCrash, /*crash_safe=*/false));

    v.push_back(Crash("crash-recover-log-index", "kFileMmap", R"(
put a 1
put d 4
put e 5
put z 9
dr c e
get d
)",
                      kAfterCrash, /*crash_safe=*/true, /*log_index=*/true));
    return v;
  }();
  return v;
}

const Scenario* FindScenario(const char* name) {
  for (const Scenario& sc : Scenarios()) {
    if (sc.name == name) return &sc;
  }
  return nullptr;
}

// ---------------------------------------------------------------------------
// The crash child
// ---------------------------------------------------------------------------

// Runs `before` and _exit()s, leaving the DB open: the parent's next Open() has
// to recover. Same idiom as db_cspp_crash_safe_test.cc -- the child writes its
// own log, which the parent prepends to its own, so the writer's observations
// are compared too.
std::string ChildLogPath(const std::string& dir) { return dir + ".childlog"; }

void RunParityChild(const char* scenario, const char* dir, const char* kind) {
  const Scenario* sc = FindScenario(scenario);
  if (sc == nullptr) _exit(123);
  Obs x;
  Runner r(KindOf(kind), *sc, dir, &x);
  alarm(30);
  r.Open();
  r.Run(sc->before.c_str());
  const Status s = WriteStringToFile(Env::Default(), x.cmp, ChildLogPath(dir));
  if (!s.ok()) _exit(122);
  _exit(x.bad != 0 ? 1 : 0);
}

int RunCrashChild(const char* scenario, const std::string& dir, Kind kind) {
  std::vector<std::string> args = {"/proc/self/exe", "--parity-child", scenario,
                                   dir, KindName(kind)};
  std::vector<char*> argv;
  for (std::string& a : args) argv.push_back(const_cast<char*>(a.c_str()));
  argv.push_back(nullptr);
  const pid_t pid = ::fork();
  if (pid == 0) {
    ::execv(argv[0], argv.data());
    ::_exit(127);
  }
  int status = 0;
  if (::waitpid(pid, &status, 0) < 0) return 126;
  if (WIFEXITED(status)) return WEXITSTATUS(status);
  return 128 + (WIFSIGNALED(status) ? WTERMSIG(status) : 0);
}

// ---------------------------------------------------------------------------
// The driver
// ---------------------------------------------------------------------------

Obs RunOnce(const Scenario& sc, Kind kind, const std::string& dir) {
  Obs x;
  Runner r(kind, sc, dir, &x);
  if (sc.crash) {
    const int rc = RunCrashChild(sc.name.c_str(), dir, kind);
    EXPECT_EQ(rc, 0) << "crash child failed for " << sc.name;
    std::string child;
    const Status s = ReadFileToString(Env::Default(), ChildLogPath(dir), &child);
    if (s.ok()) x.cmp = child + x.cmp;
    Env::Default()->DeleteFile(ChildLogPath(dir));
    r.Open();
    r.Run(sc.after.c_str());
  } else {
    r.Open();
    r.Run(sc.before.c_str());
  }
  r.Close();
  // A pair never converts (there is no one file to convert two reps into), and
  // neither does the baseline it is compared against.
  const bool converts = kind != Kind::kPair && kind != Kind::kCsppNoConvert;
  if (converts && sc.expect_convert == 0) {
    EXPECT_EQ(r.converted(), 0) << sc.name << ": converted a memtable";
  } else if (converts && sc.expect_convert == 1) {
    EXPECT_GT(r.converted(), 0)
        << sc.name << ": the memtable never converted its own file";
  }
  return x;
}

void ExpectParity(const Scenario& sc) {
  const std::string root =
      test::PerThreadDBPath(std::string("db_memtable_parity/") + sc.name);
  // DB::Open only makes the DB directory itself, not the ones above it.
  ASSERT_OK(Env::Default()->CreateDirIfMissing(
      root.substr(0, root.find_last_of('/'))));
  ASSERT_OK(Env::Default()->CreateDirIfMissing(root));
  const Obs c1 = RunOnce(sc, Kind::kCspp, root + "/cspp1");
  const Obs c2 = RunOnce(sc, Kind::kCspp, root + "/cspp2");
  const Obs o1 = RunOnce(sc, Kind::kOsll, root + "/osl1");
  const Obs o2 = RunOnce(sc, Kind::kOsll, root + "/osl2");
  // The pair does not need a log to be compared: it checks every result in
  // process, and a divergence kills the run at the call that made it. What its
  // log is good for is the DB level guarantee that holding both reps changes
  // nothing, so it runs against cspp with convert_to_sst forced off: same
  // primary rep, same flush path.
  const Obs p1 = RunOnce(sc, Kind::kPair, root + "/pair");
  const Obs b1 = RunOnce(sc, Kind::kCsppNoConvert, root + "/cspp-nc");
  // A difference between two runs of one memtable is a flaky test, and would
  // make the comparison below meaningless, so it is checked first.
  const bool same = c1.cmp == c2.cmp && o1.cmp == o2.cmp && c1.cmp == o1.cmp &&
                    p1.cmp == b1.cmp;
  if (!same) {
    const char* runs[] = {"cspp1", "cspp2", "osl1", "osl2", "pair", "cspp-nc"};
    const Obs* logs[] = {&c1, &c2, &o1, &o2, &p1, &b1};
    for (size_t i = 0; i < std::size(runs); i++) {
      const std::string p = root + "/" + runs[i];
      WriteStringToFile(Env::Default(), logs[i]->cmp, p + ".cmp.log");
      WriteStringToFile(Env::Default(), logs[i]->info, p + ".info.log");
    }
    fprintf(stdout, "\nlogs written to %s/<kind><n>.{cmp,info}.log\n",
            root.c_str());
    fflush(stdout);
  }
  // EXPECT_TRUE, not EXPECT_EQ: on failure gtest would print both whole logs.
  EXPECT_TRUE(c1.cmp == c2.cmp) << "cspp is not deterministic on " << sc.name
                                << "\n"
                                << Diff(c1.cmp, c2.cmp);
  EXPECT_TRUE(o1.cmp == o2.cmp) << "osl is not deterministic on " << sc.name
                                << "\n"
                                << Diff(o1.cmp, o2.cmp);
  EXPECT_TRUE(c1.cmp == o1.cmp) << "cspp and osl differ on " << sc.name << "\n"
                                << Diff(c1.cmp, o1.cmp);
  // Holding both reps at once must not be observable: the pair's log is the
  // log of its primary rep (cspp) with convert_to_sst off, which is the
  // configuration a pair always runs in.
  EXPECT_TRUE(p1.cmp == b1.cmp)
      << "the pair and its primary rep (cspp, no convert) differ on " << sc.name
      << "\n"
      << Diff(p1.cmp, b1.cmp);
  NoteInfoDiffs(sc.name.c_str(), c1.info, o1.info);
  // The whole log on demand: for reading what a sequence actually did.
  if (getenv("PARITY_VERBOSE") != nullptr) {
    fprintf(stdout, "=== %s: cspp1 cmp ===\n%s=== %s: cspp1 info ===\n%s",
            sc.name.c_str(), c1.cmp.c_str(), sc.name.c_str(), c1.info.c_str());
    fprintf(stdout, "=== %s: osl1 cmp ===\n%s=== %s: osl1 info ===\n%s",
            sc.name.c_str(), o1.cmp.c_str(), sc.name.c_str(), o1.info.c_str());
    fflush(stdout);
  }
  // Kept when something failed: the logs are the whole point of the dump.
  if (same) Env::Default()->DeleteDir(root);
}

}  // namespace

TEST(DBMemtableParityTest, KeysAndDeletes) {
  ExpectParity(*FindScenario("keys-and-deletes"));
}
TEST(DBMemtableParityTest, RangeDelOnlyMemtable) {
  ExpectParity(*FindScenario("rd-only-memtable"));
}
TEST(DBMemtableParityTest, RangeDelFirstOp) {
  ExpectParity(*FindScenario("rd-first-op"));
}
TEST(DBMemtableParityTest, OverlappingRangeDels) {
  ExpectParity(*FindScenario("overlapping-rd"));
}
TEST(DBMemtableParityTest, CommonPrefixKeys) {
  ExpectParity(*FindScenario("common-prefix-keys"));
}
TEST(DBMemtableParityTest, ManyKeysTwoMemtables) {
  ExpectParity(*FindScenario("many-keys-two-memtables"));
}
TEST(DBMemtableParityTest, SnapshotRangeDelVisibility) {
  ExpectParity(*FindScenario("snapshot-rd-visibility"));
}
TEST(DBMemtableParityTest, MergeAndRangeDel) {
  ExpectParity(*FindScenario("merge-and-rd"));
}
TEST(DBMemtableParityTest, EdgeCases) {
  ExpectParity(*FindScenario("edge-cases"));
}
TEST(DBMemtableParityTest, FileMmapCloseAndReopen) {
  ExpectParity(*FindScenario("filemmap-close-and-reopen"));
}
TEST(DBMemtableParityTest, RangeDelAcrossFlushBoundary) {
  ExpectParity(*FindScenario("rd-across-flush-boundary"));
}
TEST(DBMemtableParityTest, NoConvertManyFlushes) {
  ExpectParity(*FindScenario("no-convert-many-flushes"));
}
TEST(DBMemtableParityTest, CrashRecover) {
  ExpectParity(*FindScenario("crash-recover"));
}
TEST(DBMemtableParityTest, CrashRecoverOverlappingRangeDels) {
  ExpectParity(*FindScenario("crash-recover-overlapping-rd"));
}
TEST(DBMemtableParityTest, CrashRecoverLogIndex) {
  ExpectParity(*FindScenario("crash-recover-log-index"));
}
TEST(DBMemtableParityTest, CrashWalReplay) {
  ExpectParity(*FindScenario("crash-wal-replay"));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--parity-child") == 0 && i + 3 < argc) {
      ROCKSDB_NAMESPACE::RunParityChild(argv[i + 1], argv[i + 2], argv[i + 3]);
      return 125;  // RunParityChild never returns
    }
  }
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
