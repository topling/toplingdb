//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "table/iterator_wrapper.h"
#include "table/merging_iterator.h"
#include "table/scoped_arena_iterator.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

class MergerTest : public testing::Test {
 public:
  MergerTest()
      : icomp_(BytewiseComparator()),
        rnd_(3),
        merging_iterator_(nullptr),
        single_iterator_(nullptr) {}
  ~MergerTest() override = default;
  std::vector<std::string> GenerateStrings(size_t len, int string_len) {
    std::vector<std::string> ret;

    for (size_t i = 0; i < len; ++i) {
      InternalKey ik(rnd_.HumanReadableString(string_len), 0,
                     ValueType::kTypeValue);
      ret.push_back(ik.Encode().ToString(false));
    }
    return ret;
  }

  void AssertEquivalence() {
    auto a = merging_iterator_.get();
    auto b = single_iterator_.get();
    if (!a->Valid()) {
      ASSERT_TRUE(!b->Valid());
    } else {
      ASSERT_TRUE(b->Valid());
      ASSERT_EQ(b->key().ToString(), a->key().ToString());
      ASSERT_EQ(b->value().ToString(), a->value().ToString());
    }
  }

  void SeekToRandom() {
    InternalKey ik(rnd_.HumanReadableString(5), 0, ValueType::kTypeValue);
    Seek(ik.Encode().ToString(false));
  }

  void Seek(std::string target) {
    merging_iterator_->Seek(target);
    single_iterator_->Seek(target);
  }

  void SeekToFirst() {
    merging_iterator_->SeekToFirst();
    single_iterator_->SeekToFirst();
  }

  void SeekToLast() {
    merging_iterator_->SeekToLast();
    single_iterator_->SeekToLast();
  }

  void Next(int times) {
    for (int i = 0; i < times && merging_iterator_->Valid(); ++i) {
      AssertEquivalence();
      merging_iterator_->Next();
      single_iterator_->Next();
    }
    AssertEquivalence();
  }

  void Prev(int times) {
    for (int i = 0; i < times && merging_iterator_->Valid(); ++i) {
      AssertEquivalence();
      merging_iterator_->Prev();
      single_iterator_->Prev();
    }
    AssertEquivalence();
  }

  void NextAndPrev(int times) {
    for (int i = 0; i < times && merging_iterator_->Valid(); ++i) {
      AssertEquivalence();
      if (rnd_.OneIn(2)) {
        merging_iterator_->Prev();
        single_iterator_->Prev();
      } else {
        merging_iterator_->Next();
        single_iterator_->Next();
      }
    }
    AssertEquivalence();
  }

  void Generate(size_t num_iterators, size_t strings_per_iterator,
                int letters_per_string) {
    std::vector<InternalIterator*> small_iterators;
    for (size_t i = 0; i < num_iterators; ++i) {
      auto strings = GenerateStrings(strings_per_iterator, letters_per_string);
      small_iterators.push_back(new VectorIterator(strings, strings, &icomp_));
      all_keys_.insert(all_keys_.end(), strings.begin(), strings.end());
    }

    merging_iterator_.reset(
        NewMergingIterator(&icomp_, small_iterators.data(),
                           static_cast<int>(small_iterators.size())));
    single_iterator_.reset(new VectorIterator(all_keys_, all_keys_, &icomp_));
  }

  InternalKeyComparator icomp_;
  Random rnd_;
  std::unique_ptr<InternalIterator> merging_iterator_;
  std::unique_ptr<InternalIterator> single_iterator_;
  std::vector<std::string> all_keys_;
};

TEST_F(MergerTest, SeekToRandomNextTest) {
  Generate(1000, 50, 50);
  for (int i = 0; i < 10; ++i) {
    SeekToRandom();
    AssertEquivalence();
    Next(50000);
  }
}

TEST_F(MergerTest, SeekToRandomNextSmallStringsTest) {
  Generate(1000, 50, 2);
  for (int i = 0; i < 10; ++i) {
    SeekToRandom();
    AssertEquivalence();
    Next(50000);
  }
}

TEST_F(MergerTest, SeekToRandomPrevTest) {
  Generate(1000, 50, 50);
  for (int i = 0; i < 10; ++i) {
    SeekToRandom();
    AssertEquivalence();
    Prev(50000);
  }
}

TEST_F(MergerTest, SeekToRandomRandomTest) {
  Generate(200, 50, 50);
  for (int i = 0; i < 3; ++i) {
    SeekToRandom();
    AssertEquivalence();
    NextAndPrev(5000);
  }
}

TEST_F(MergerTest, SeekToFirstTest) {
  Generate(1000, 50, 50);
  for (int i = 0; i < 10; ++i) {
    SeekToFirst();
    AssertEquivalence();
    Next(50000);
  }
}

TEST_F(MergerTest, SeekToLastTest) {
  Generate(1000, 50, 50);
  for (int i = 0; i < 10; ++i) {
    SeekToLast();
    AssertEquivalence();
    Prev(50000);
  }
}

// The tests below drive the real LevelIterator and MergingIterator over a real
// database. A level with more than one file is scanned while its LevelIterator
// is the child the merging iterator reads every value from, so that child
// switches files -- and deletes the iterator it had bound the wrapper to --
// while staying that child. LevelIterator reports each rebinding through a sync
// point, which lets the tests check what no value check alone can see: that the
// wrapper the merging iterator copied the binding into still names the file
// iterator the level iterator just bound itself to. Scanning a level of a
// single file would pass without exercising any of this, so the tests also
// assert, through the same sync point, that a file boundary was really crossed.
// One of the tests writes a range tombstone over a file boundary of the level,
// so a scan that applies range tombstones is covered as well.

const int kNumKeys = 4000;  // several files at 128KB per file below
const int kNumHighKeys = 100;
const int kNumAllKeys = kNumKeys + kNumHighKeys;

// The keys the range tombstone of the test below hides, [kDelFirst, kDelLast):
// a range of its own, so the level that still holds them physically holds
// nothing else, and the internal iterator -- which does not merge copies of one
// user key in two levels -- sees each of them once.
const int kDelFirst = 2600;
const int kDelLast = 2800;

// Long enough that a few thousand keys make a level hold more than one file,
// and it names its own key, so a value served from the wrong place is visible.
std::string MergerLevelValue(int i) {
  return std::string("v") + std::to_string(i) + std::string(80, '.');
}

class MergerLevelIterTest : public DBTestBase {
 public:
  MergerLevelIterTest()
      : DBTestBase("merger_level_iter_test", /*env_do_fsync=*/false) {}

  // L1 holds several files over keys [0, kNumKeys), and L0 holds one file over
  // keys [kNumKeys, kNumAllKeys), which are all larger. A forward scan
  // therefore keeps the L1 LevelIterator -- one iterator lazily walking every
  // file of the level -- at the top of the merging heap while it crosses its
  // own file boundaries. That is the case where the value funcs the merging
  // iterator cached for the wrapper it was given are left pointing at a file
  // iterator LevelIterator has already deleted.
  void BuildMultiFileLevelAndHighL0File() {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.level_compaction_dynamic_level_bytes = false;
    // The level must really take the size below.
    options.compression = kNoCompression;
    options.target_file_size_base = 128 * 1024;
    options.write_buffer_size = 8 * 1024 * 1024;
    DestroyAndReopen(options);

    PutKeys(0, kNumKeys);
    // A trivial move would carry the whole level over as a single file, so make
    // this a real compaction: its output is split by target_file_size_base.
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                          /*disallow_trivial_move=*/true));
    ASSERT_GE(NumTableFilesAtLevel(1), 2) << FilesPerLevel();

    PutKeys(kNumKeys, kNumHighKeys);
    ASSERT_EQ(1, NumTableFilesAtLevel(0));
  }

  // The level alone: no file of level 0 and nothing in the memtable, so the
  // merging iterator has the one child, which stays the top of its heap while
  // it crosses its own file boundaries.
  void BuildMultiFileLevelOnly() {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.level_compaction_dynamic_level_bytes = false;
    options.compression = kNoCompression;
    options.target_file_size_base = 128 * 1024;
    options.write_buffer_size = 8 * 1024 * 1024;
    DestroyAndReopen(options);

    PutKeys(0, kNumKeys);
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                          /*disallow_trivial_move=*/true));
    ASSERT_GE(NumTableFilesAtLevel(1), 2) << FilesPerLevel();
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
  }

  // L2 holds the lower half of the keys and L1 the upper half, each in several
  // files, so a scan is served by one level iterator at a time and crosses the
  // file boundaries of both. The level that is not the one being read crosses
  // its own boundaries as well, and the rebinding of a wrapper that is not the
  // current child is a rebinding the merging iterator must not take for itself.
  void BuildTwoMultiFileLevels() {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.level_compaction_dynamic_level_bytes = false;
    options.compression = kNoCompression;
    options.target_file_size_base = 128 * 1024;
    options.write_buffer_size = 8 * 1024 * 1024;
    DestroyAndReopen(options);

    // The lower half down to L2, where a later compaction cannot take it away.
    PutKeys(0, kNumKeys / 2);
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                          /*disallow_trivial_move=*/true));
    ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, nullptr,
                                          /*disallow_trivial_move=*/true));
    ASSERT_GE(NumTableFilesAtLevel(2), 2) << FilesPerLevel();

    // The upper half into L1, and one file of higher keys above both.
    PutKeys(kNumKeys / 2, kNumKeys / 2);
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                          /*disallow_trivial_move=*/true));
    ASSERT_GE(NumTableFilesAtLevel(1), 2) << FilesPerLevel();
    PutKeys(kNumKeys, kNumHighKeys);
    ASSERT_EQ(1, NumTableFilesAtLevel(0));
  }

  // The range tombstone inside the level itself: the keys it covers are written
  // alone, pushed down to L2, then written again together with the tombstone
  // and compacted into the files of L1, which drops those keys from the level
  // and keeps the tombstone because the L2 file below still holds copies of
  // them. The level is left with several files and with a range tombstone
  // iterator -- the read's, since it applies them -- so it reports the file
  // boundaries it crosses as sentinel keys as well as by rebinding the wrapper
  // it was given.
  void BuildLevelWithRangeTombstoneInIt() {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.level_compaction_dynamic_level_bytes = false;
    options.compression = kNoCompression;
    options.target_file_size_base = 128 * 1024;
    options.write_buffer_size = 8 * 1024 * 1024;
    DestroyAndReopen(options);

    PutKeys(kDelFirst, kDelLast - kDelFirst);
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                          /*disallow_trivial_move=*/true));
    ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, nullptr,
                                          /*disallow_trivial_move=*/true));
    ASSERT_EQ(1, NumTableFilesAtLevel(2)) << FilesPerLevel();

    PutKeys(0, kNumKeys);
    ASSERT_OK(dbfull()->DeleteRange(WriteOptions(),
                                    dbfull()->DefaultColumnFamily(),
                                    Key(kDelFirst), Key(kDelLast)));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                          /*disallow_trivial_move=*/true));
    ASSERT_GE(NumTableFilesAtLevel(1), 2) << FilesPerLevel();
    ASSERT_EQ(1, NumTableFilesAtLevel(2)) << FilesPerLevel();
    PutKeys(kNumKeys, kNumHighKeys);
    ASSERT_EQ(1, NumTableFilesAtLevel(0));

    del_first_ = kDelFirst;
    del_last_ = kDelLast;
    range_tombstone_ = true;
  }

  // The same database, with a range tombstone in a newer file of L0, which is
  // where the range tombstone of a real workload starts out. The tombstone
  // hides the keys around a file boundary of the level, so a read that applies
  // range tombstones has to hide them while it walks the files of that level.
  void BuildMultiFileLevelWithRangeTombstone() {
    BuildMultiFileLevelAndHighL0File();

    int boundary = LevelBoundary(1);
    del_first_ = boundary - 50;
    del_last_ = boundary + 50;

    ASSERT_OK(dbfull()->DeleteRange(WriteOptions(),
                                    dbfull()->DefaultColumnFamily(),
                                    Key(del_first_), Key(del_last_)));
    ASSERT_OK(Flush());
    ASSERT_EQ(2, NumTableFilesAtLevel(0));
    range_tombstone_ = true;
  }

  // The first file boundary of a level, taken from the files themselves, so
  // that a deleted range around it lands on one whatever the compaction made
  // the level's split.
  int LevelBoundary(int level) {
    std::vector<LiveFileMetaData> files;
    dbfull()->GetLiveFilesMetaData(&files);
    int boundary = kNumKeys;
    for (const auto& file : files) {
      if (file.level == level) {
        boundary = std::min(boundary, KeyIndex(file.largestkey));
      }
    }
    EXPECT_GT(boundary, 100);
    EXPECT_LT(boundary, kNumKeys);
    return boundary;
  }

  // "key%06d" back to the number it was built from.
  static int KeyIndex(const std::string& key) {
    return std::stoi(key.substr(std::string("key").size()));
  }

  void PutKeys(int first, int n) {
    for (int i = first; i < first + n; ++i) {
      ASSERT_OK(Put(Key(i), MergerLevelValue(i)));
    }
    ASSERT_OK(Flush());
  }

  // A target internal key, the way DBIter::Seek() builds one.
  static std::string EncodedKey(int i) {
    return InternalKey(Key(i), kMaxSequenceNumber, kTypeValue)
        .Encode()
        .ToString(false);
  }

  // A target internal key, the way DBIter::SeekForPrev() builds one: sequence
  // number 0 makes it the largest internal key of the user key, which is what a
  // reverse seek has to be given to land on the key itself.
  static std::string EncodedKeyForPrev(int i) {
    return InternalKey(Key(i), 0, kValueTypeForSeekForPrev)
        .Encode()
        .ToString(false);
  }

  // Follow what LevelIterator binds the wrapper it was given to, count how
  // often it rebinds it while a scan runs, and count how often the merging
  // iterator was told about one.
  void InstallLevelBindingCallbacks() {
    SyncPoint::GetInstance()->SetCallBack(
        "LevelIterator::UpdateScanFunc:Rebound", [this](void* arg) {
          auto* iw = static_cast<IteratorWrapper*>(arg);
          void* binding = static_cast<void*>(iw->value_iter_);
          void* before = level_binding_.exchange(binding);
          ++level_rebindings_;
          ++rebinds_by_wrapper_[arg];
          if (before != nullptr && before != binding) {
            ++moved_bindings_;
          }
        });
    SyncPoint::GetInstance()->SetCallBack(
        "MergingIterator::UpdateWrapper", [this](void* arg) {
          ++told_by_wrapper_[arg];
        });
    SyncPoint::GetInstance()->SetCallBack(
        "MergingIterator::UpdateWrapper:RefreshCurrent",
        [this](void*) { ++refreshed_current_; });
    SyncPoint::GetInstance()->SetCallBack(
        "LevelIterator::TrySetDeleteRangeSentinel",
        [this](void*) { ++sentinels_; });
    SyncPoint::GetInstance()->EnableProcessing();
  }

  // Every rebinding the merging iterator was told about has to be a rebinding
  // of a wrapper a level iterator really did rebind, and the rebindings of a
  // level iterator that was not the current child must not have produced one:
  // the binding the merging iterator copies for itself is the current child's,
  // so taking another child's would point the wrapper it was given at the wrong
  // iterator. The counts also say that there was such a rebinding to leave out.
  // A build without TOPLINGDB_WITH_WIDE_COLUMNS leaves the value of a key for
  // the reader to fetch, so a read of it goes through the funcs the internal
  // iterators copied -- which is the read the server aborted on. This build
  // prepares the value while positioning on the key, so asking for the other
  // shape takes both hooks of db_iter.{h,cc}, and both of them are inert until
  // a test sets them.
  void LeavePublicValuesUnprepared() {
    SyncPoint::GetInstance()->SetCallBack(
        "DBIter::ValueMayBeUnprepared", [](void* arg) {
          *static_cast<bool*>(arg) = true;
        });
    SyncPoint::GetInstance()->SetCallBack(
        "DBIter::ValuePrepared", [this](void* arg) {
          *static_cast<bool*>(arg) = false;
          ++values_left_unprepared_;
        });
  }

  // Says that the values really were read the way the server reads them, which
  // is what makes the scans below the server's own read path.
  void ExpectPublicValuesReadTheCopiedFuncs() {
    EXPECT_GT(values_left_unprepared_.load(), 0)
        << "no value was left unprepared, so no read went through the value funcs "
           "the internal iterators copied";
  }

  void CheckWrapperNotifications() {
    int told = 0;
    int rebinds = 0;
    for (const auto& entry : told_by_wrapper_) {
      told += entry.second;
      auto it = rebinds_by_wrapper_.find(entry.first);
      ASSERT_NE(rebinds_by_wrapper_.end(), it)
          << "the merging iterator was told about a wrapper that no level "
             "iterator rebound";
      EXPECT_LE(entry.second, it->second)
          << "the merging iterator was told about more rebindings than there "
             "were";
    }
    for (const auto& entry : rebinds_by_wrapper_) {
      rebinds += entry.second;
    }
    EXPECT_GE(told, 1) << "the merging iterator was never told about a rebinding";
    EXPECT_LT(told, rebinds)
        << "no rebinding of a level that was not the current child was left "
           "untold, so the test never exercised that check";
  }

  void UninstallLevelBindingCallbacks() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  // Read the current value the way DBIter::value() does -- db_iter.h calls
  // IteratorWrapper::PrepareAndGetValue(), whose funcs are the ones the merging
  // iterator cached -- and check it, the key, and that the accessor which does
  // not use that cache agrees.
  void CheckCurrentKey(IteratorWrapper* outer, int i) {
    ParsedInternalKey ikey;
    ASSERT_OK(ParseInternalKey(outer->key(), &ikey, /*log_err_key=*/true));
    ASSERT_EQ(Key(i), ikey.user_key.ToString());
    Slice value;
    ASSERT_TRUE(outer->PrepareAndGetValue(&value));
    ASSERT_EQ(MergerLevelValue(i), value.ToString());
    ASSERT_EQ(MergerLevelValue(i), outer->value().ToString());
  }

  // The key and the value of a public iterator, the value read through value(),
  // which is the call that goes through the funcs the merging iterator cached
  // for the wrapper DBIter handed it.
  void CheckPublicKeyValue(Iterator* iter, int i) {
    ASSERT_EQ(Key(i), iter->key().ToString());
    ASSERT_EQ(MergerLevelValue(i), iter->value().ToString());
  }

  // True once the database has the range tombstone, which hides the keys of
  // [del_first_, del_last_).
  bool range_tombstone_ = false;
  int del_first_ = 0;
  int del_last_ = 0;

  bool Hidden(int i) const {
    return range_tombstone_ && i >= del_first_ && i < del_last_;
  }

  // The keys a forward scan from `first` yields, in order. A read that ignores
  // range tombstones leaves every key visible, since it does not hide any.
  std::vector<int> ForwardKeys(int first, bool ignore_range_deletions) const {
    std::vector<int> keys;
    for (int i = first; i < kNumAllKeys; ++i) {
      if (ignore_range_deletions || !Hidden(i)) {
        keys.push_back(i);
      }
    }
    return keys;
  }

  // The keys a backward scan from `last` yields, in order.
  std::vector<int> BackwardKeys(int last, bool ignore_range_deletions) const {
    std::vector<int> keys;
    for (int i = 0; i <= last; ++i) {
      if (ignore_range_deletions || !Hidden(i)) {
        keys.push_back(i);
      }
    }
    return keys;
  }

  // Require the scan to yield exactly `keys`, in order, each with its own
  // value. For the keys of the level, the value funcs of the wrapper must also
  // name the iterator LevelIterator last bound itself to: those keys are the
  // ones the level iterator is the current child for, so the wrapper's copy of
  // the binding is the one that would be used to read them.
  void CheckForward(IteratorWrapper* outer, const std::vector<int>& keys) {
    for (int i : keys) {
      ASSERT_TRUE(outer->Valid()) << "key " << Key(i) << " is missing";
      CheckCurrentKey(outer, i);
      if (check_binding_ && i < kNumKeys) {
        ASSERT_EQ(level_binding_.load(), static_cast<void*>(outer->value_iter_))
            << "the value funcs of the wrapper do not point at the file "
               "iterator of "
            << Key(i);
      }
      outer->Next();
    }
    ASSERT_FALSE(outer->Valid());
    ASSERT_OK(outer->status());
  }

  // The same backward, where the merging iterator reads values through the
  // child rather than through its cached copy, so only the keys and values
  // apply here.
  void CheckBackward(IteratorWrapper* outer, const std::vector<int>& keys) {
    for (auto it = keys.rbegin(); it != keys.rend(); ++it) {
      ASSERT_TRUE(outer->Valid()) << "key " << Key(*it) << " is missing";
      CheckCurrentKey(outer, *it);
      outer->Prev();
    }
    ASSERT_FALSE(outer->Valid());
    ASSERT_OK(outer->status());
  }

  // Whether CheckForward also requires the wrapper it is given to be bound to
  // what the level iterator last bound itself to. That can only be asked of a
  // database whose other multi-file levels are never the child being read,
  // since a second one rebinds its own wrapper too; the tests with two of them
  // read that invariant from the notification counts instead.
  bool check_binding_ = true;

  // The binding of the wrapper as LevelIterator last set it, and the counts
  // collected by InstallLevelBindingCallbacks().
  std::atomic<void*> level_binding_{nullptr};
  std::atomic<int> level_rebindings_{0};
  std::atomic<int> moved_bindings_{0};
  std::atomic<int> refreshed_current_{0};
  std::atomic<int> sentinels_{0};
  // Values that were left unprepared for their reader, the way a build without
  // wide columns leaves them.
  std::atomic<int> values_left_unprepared_{0};
  std::map<void*, int> rebinds_by_wrapper_;
  std::map<void*, int> told_by_wrapper_;
};

TEST_F(MergerLevelIterTest, ValueFuncsAcrossLevelFileBoundaries) {
  BuildMultiFileLevelAndHighL0File();
  InstallLevelBindingCallbacks();

  // ignore_range_deletions decides whether the level iterator gets a range
  // tombstone iterator, and with it whether a file boundary is reported to the
  // merging iterator as a sentinel key or only by the level iterator rebinding
  // the wrapper itself. Both shapes have to keep the wrapper's binding right.
  for (bool ignore_range_deletions : {false, true}) {
    for (bool allow_unprepared_value : {false, true}) {
      ReadOptions read_options;
      read_options.ignore_range_deletions = ignore_range_deletions;
      Arena arena;
      IteratorWrapper outer;  // stands in for DBIter::iter_
      ScopedArenaIterator iter(dbfull()->NewInternalIterator(
          read_options, &arena, kMaxSequenceNumber, nullptr,
          allow_unprepared_value));
      ASSERT_NE(nullptr, iter.get());
      outer.Set(iter.get());

      outer.SeekToFirst();
      CheckForward(&outer, ForwardKeys(0, ignore_range_deletions));
      outer.SeekToLast();
      CheckBackward(&outer, BackwardKeys(kNumAllKeys - 1, ignore_range_deletions));
      // Forward again, this time with every level iterator left with no file at
      // all: the pass has to reopen the level from its first file.
      outer.SeekToFirst();
      CheckForward(&outer, ForwardKeys(0, ignore_range_deletions));
    }
  }

  UninstallLevelBindingCallbacks();

  // Coverage: the checks above mean nothing unless the level iterator really
  // did rebind its wrapper, and really did move to another file iterator while
  // doing so.
  EXPECT_GT(level_rebindings_.load(), 0)
      << "the level iterator never rebound the wrapper it was given";
  EXPECT_GT(moved_bindings_.load(), 0)
      << "the level iterator never moved to another file";
  // Only a build that tells the parent about the rebinding can pass this, which
  // is the point of the fix these tests cover.
  EXPECT_GT(refreshed_current_.load(), 0)
      << "the merging iterator never refreshed the binding it had copied";
}

TEST_F(MergerLevelIterTest, ValueFuncsAfterSeekAcrossLevelFileBoundaries) {
  BuildMultiFileLevelAndHighL0File();
  InstallLevelBindingCallbacks();

  for (bool ignore_range_deletions : {false, true}) {
    for (bool allow_unprepared_value : {false, true}) {
      ReadOptions read_options;
      read_options.ignore_range_deletions = ignore_range_deletions;
      Arena arena;
      IteratorWrapper outer;
      ScopedArenaIterator iter(dbfull()->NewInternalIterator(
          read_options, &arena, kMaxSequenceNumber, nullptr,
          allow_unprepared_value));
      ASSERT_NE(nullptr, iter.get());
      outer.Set(iter.get());

      // Every target either sits in another file of the level or at its end, so
      // each scan crosses the boundaries after it, and seeking to a key of the
      // level makes the level iterator itself seek its files.
      for (int target : {0, kNumKeys / 4, kNumKeys / 2, kNumKeys - 1, kNumKeys,
                         kNumAllKeys - 1}) {
        outer.Seek(EncodedKey(target));
        CheckForward(&outer, ForwardKeys(target, ignore_range_deletions));
        outer.Seek(EncodedKey(target));
        CheckBackward(&outer, BackwardKeys(target, ignore_range_deletions));
      }
    }
  }

  UninstallLevelBindingCallbacks();
  EXPECT_GT(level_rebindings_.load(), 0)
      << "the level iterator never rebound the wrapper it was given";
  EXPECT_GT(moved_bindings_.load(), 0)
      << "the level iterator never moved to another file";
  EXPECT_GT(refreshed_current_.load(), 0)
      << "the merging iterator never refreshed the binding it had copied";
  CheckWrapperNotifications();
}

// The tombstone straddles a file boundary of the level, so hiding the keys it
// covers makes the level iterator cross that boundary in the middle of a skip,
// and the merging iterator has to consult the tombstone while it walks the
// files of the level. The keys around the hidden range still have to be read
// from their own place, through the value funcs the merging iterator cached for
// the wrapper it was given.
TEST_F(MergerLevelIterTest, ValueFuncsWithRangeTombstoneAcrossLevelFileBoundaries) {
  BuildMultiFileLevelWithRangeTombstone();
  InstallLevelBindingCallbacks();

  int sentinels_with_tombstones = 0;
  for (bool ignore_range_deletions : {false, true}) {
    for (bool allow_unprepared_value : {false, true}) {
      ReadOptions read_options;
      read_options.ignore_range_deletions = ignore_range_deletions;
      Arena arena;
      IteratorWrapper outer;
      ScopedArenaIterator iter(dbfull()->NewInternalIterator(
          read_options, &arena, kMaxSequenceNumber, nullptr,
          allow_unprepared_value));
      ASSERT_NE(nullptr, iter.get());
      outer.Set(iter.get());

      outer.SeekToFirst();
      CheckForward(&outer, ForwardKeys(0, ignore_range_deletions));
      outer.SeekToLast();
      CheckBackward(&outer, BackwardKeys(kNumAllKeys - 1, ignore_range_deletions));
      // A forward seek into the hidden range lands on the first key the
      // tombstone does not hide, a backward seek on the last one, and the scan
      // on from either stays right across the file boundary the tombstone
      // covers.
      for (int target :
           {0, del_first_ - 1, del_first_, del_first_ + 1, del_last_ - 1,
            del_last_, kNumKeys - 1, kNumKeys, kNumAllKeys - 1}) {
        outer.Seek(EncodedKey(target));
        CheckForward(&outer, ForwardKeys(target, ignore_range_deletions));
        outer.SeekForPrev(EncodedKeyForPrev(target));
        CheckBackward(&outer, BackwardKeys(target, ignore_range_deletions));
      }
    }
    if (!ignore_range_deletions) {
      sentinels_with_tombstones = sentinels_.load();
    }
  }

  UninstallLevelBindingCallbacks();
  EXPECT_GT(level_rebindings_.load(), 0)
      << "the level iterator never rebound the wrapper it was given";
  EXPECT_GT(moved_bindings_.load(), 0)
      << "the level iterator never moved to another file";
  EXPECT_GT(refreshed_current_.load(), 0)
      << "the merging iterator never refreshed the binding it had copied";
  // Coverage: the read that applies range tombstones gives the level iterator
  // one of the read's own range tombstones to hold over a file boundary, so the
  // boundary really was reported as a sentinel key -- the branch of the update
  // that binds the wrapper it was given to the level iterator itself rather
  // than to a file iterator. The read that ignores them gives it none, so the
  // count has to stand still for that one.
  EXPECT_GT(sentinels_with_tombstones, 0)
      << "no file boundary was reported as a sentinel key";
  EXPECT_EQ(sentinels_with_tombstones, sentinels_.load())
      << "the read that ignores range tombstones built a sentinel key";
  CheckWrapperNotifications();
}

// Two levels of the database each hold several files, so a scan is served by
// one of them at a time and crosses the file boundaries of both. Every key
// still has to come from where it lives, and the rebinding of the level that is
// not the one being read must not be taken for the binding of the one that is.
TEST_F(MergerLevelIterTest, ValueFuncsWithTwoMultiFileLevels) {
  BuildTwoMultiFileLevels();
  InstallLevelBindingCallbacks();
  // One wrapper per level, so there is no single binding to compare the value
  // funcs of a key against; CheckWrapperNotifications() asks the same question.
  check_binding_ = false;

  for (bool ignore_range_deletions : {false, true}) {
    for (bool allow_unprepared_value : {false, true}) {
      ReadOptions read_options;
      read_options.ignore_range_deletions = ignore_range_deletions;
      Arena arena;
      IteratorWrapper outer;
      ScopedArenaIterator iter(dbfull()->NewInternalIterator(
          read_options, &arena, kMaxSequenceNumber, nullptr,
          allow_unprepared_value));
      ASSERT_NE(nullptr, iter.get());
      outer.Set(iter.get());

      outer.SeekToFirst();
      CheckForward(&outer, ForwardKeys(0, ignore_range_deletions));
      outer.SeekToLast();
      CheckBackward(&outer, BackwardKeys(kNumAllKeys - 1, ignore_range_deletions));
      // The keys where the two levels meet are the ones where the child the
      // merging iterator reads from changes.
      for (int target : {0, kNumKeys / 2 - 1, kNumKeys / 2, kNumKeys / 2 + 1,
                         kNumKeys - 1, kNumKeys, kNumAllKeys - 1}) {
        outer.Seek(EncodedKey(target));
        CheckForward(&outer, ForwardKeys(target, ignore_range_deletions));
        outer.SeekForPrev(EncodedKeyForPrev(target));
        CheckBackward(&outer, BackwardKeys(target, ignore_range_deletions));
      }
    }
  }

  UninstallLevelBindingCallbacks();
  EXPECT_GT(level_rebindings_.load(), 0)
      << "the level iterators never rebound a wrapper";
  EXPECT_GT(moved_bindings_.load(), 0)
      << "no level iterator moved to another file";
  EXPECT_GT(refreshed_current_.load(), 0)
      << "the merging iterator never refreshed the binding it had copied";
  CheckWrapperNotifications();
}

// Here the range tombstone is inside the level itself, carried into its files
// by the compaction that wrote the keys it covers, so the level iterator has
// range tombstones of its own: it reports the file boundaries it crosses as
// sentinel keys as well as by rebinding the wrapper it was given, and the keys
// the tombstone covers are hidden from the level that still holds them.
TEST_F(MergerLevelIterTest, ValueFuncsWithTombstoneInsideLevelFileBoundaries) {
  BuildLevelWithRangeTombstoneInIt();
  InstallLevelBindingCallbacks();
  check_binding_ = false;

  int sentinels_with_tombstones = 0;
  for (bool ignore_range_deletions : {false, true}) {
    for (bool allow_unprepared_value : {false, true}) {
      ReadOptions read_options;
      read_options.ignore_range_deletions = ignore_range_deletions;
      Arena arena;
      IteratorWrapper outer;
      ScopedArenaIterator iter(dbfull()->NewInternalIterator(
          read_options, &arena, kMaxSequenceNumber, nullptr,
          allow_unprepared_value));
      ASSERT_NE(nullptr, iter.get());
      outer.Set(iter.get());

      outer.SeekToFirst();
      CheckForward(&outer, ForwardKeys(0, ignore_range_deletions));
      outer.SeekToLast();
      CheckBackward(&outer, BackwardKeys(kNumAllKeys - 1, ignore_range_deletions));
      for (int target : {0, del_first_ - 1, del_first_, del_last_ - 1, del_last_,
                         kNumKeys - 1, kNumKeys, kNumAllKeys - 1}) {
        outer.Seek(EncodedKey(target));
        CheckForward(&outer, ForwardKeys(target, ignore_range_deletions));
        outer.SeekForPrev(EncodedKeyForPrev(target));
        CheckBackward(&outer, BackwardKeys(target, ignore_range_deletions));
      }
    }
    if (!ignore_range_deletions) {
      // The read that applies the tombstones is the one that gives the level
      // iterator tombstones of its own, so by the end of it the sentinels have
      // to be there, and the read that follows must not add any.
      sentinels_with_tombstones = sentinels_.load();
    }
  }

  UninstallLevelBindingCallbacks();
  EXPECT_GT(level_rebindings_.load(), 0)
      << "the level iterator never rebound the wrapper it was given";
  EXPECT_GT(moved_bindings_.load(), 0)
      << "the level iterator never moved to another file";
  EXPECT_GT(refreshed_current_.load(), 0)
      << "the merging iterator never refreshed the binding it had copied";
  // Coverage: a file boundary of the level really was reported as a sentinel
  // key, which is the branch of the update that binds the wrapper to the level
  // iterator itself rather than to a file iterator.
  EXPECT_GT(sentinels_with_tombstones, 0)
      << "no file boundary was reported as a sentinel key";
  EXPECT_EQ(sentinels_with_tombstones, sentinels_.load())
      << "the read that ignores range tombstones built a sentinel key";
  CheckWrapperNotifications();
}

// The public iterator's own chain over the level's file boundaries: keys,
// values, order, and the iterators' seeks and directions.  What these can show
// depends on a build option. With TOPLINGDB_WITH_WIDE_COLUMNS -- which
// MAKE_UNIT_TEST=1 turns on, so this test binary has it -- DBIter prepares a
// value while it positions on a key, so a public read never calls the value
// funcs the merging iterator copied into the wrapper it was given, and this
// scan cannot abort here even on a library without the fix. Built without that
// option, as db_bench and the server are, the read is lazy: that is where the
// "pure virtual method called ... mysqld got signal 6" of a SELECT came from
// (BaseDeltaIterator::UpdateCurrent() -> ArenaWrappedDBIter::value() ->
// InternalIteratorBase::PrepareAndGetValue), and the tests above cover that
// read where it lives, in the merging iterator's copy of a child's value funcs.
TEST_F(MergerLevelIterTest, PublicIteratorOverMultiFileLevel) {
  BuildMultiFileLevelAndHighL0File();
  InstallLevelBindingCallbacks();
  LeavePublicValuesUnprepared();

  for (bool ignore_range_deletions : {false, true}) {
    ReadOptions read_options;
    read_options.ignore_range_deletions = ignore_range_deletions;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_NE(nullptr, iter.get());

    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      CheckPublicKeyValue(iter.get(), i);
      ++i;
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ(kNumAllKeys, i) << "the forward scan did not cover every key";

    i = kNumAllKeys - 1;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      CheckPublicKeyValue(iter.get(), i);
      --i;
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ(-1, i) << "the backward scan did not cover every key";

    // Both directions of seek, from inside each file of the level and from its
    // end, each followed by the scan on from there.
    for (int target : {0, kNumKeys / 4, kNumKeys / 2, kNumKeys - 1, kNumKeys,
                       kNumAllKeys - 1}) {
      i = target;
      for (iter->Seek(Key(target)); iter->Valid(); iter->Next()) {
        CheckPublicKeyValue(iter.get(), i);
        ++i;
      }
      ASSERT_OK(iter->status());
      EXPECT_EQ(kNumAllKeys, i) << "the scan from " << Key(target) << " ended early";

      i = target;
      for (iter->SeekForPrev(Key(target)); iter->Valid(); iter->Prev()) {
        CheckPublicKeyValue(iter.get(), i);
        --i;
      }
      ASSERT_OK(iter->status());
      EXPECT_EQ(-1, i) << "the backward scan from " << Key(target) << " ended early";
    }
  }
  UninstallLevelBindingCallbacks();
  ExpectPublicValuesReadTheCopiedFuncs();
}

// The public iterator over a database that holds nothing but the level: with
// one iterator to merge, the builder hands that iterator over as it is
// (MergeIteratorBuilder::Finish), so DBIter reads straight from the level
// iterator and there is no copy of a child's value funcs to go stale. This is
// the shape a freshly compacted database has, and the scans above are the ones
// with a merging iterator in the middle.
TEST_F(MergerLevelIterTest, PublicIteratorOverLevelAlone) {
  BuildMultiFileLevelOnly();
  InstallLevelBindingCallbacks();
  LeavePublicValuesUnprepared();

  for (bool ignore_range_deletions : {false, true}) {
    ReadOptions read_options;
    read_options.ignore_range_deletions = ignore_range_deletions;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_NE(nullptr, iter.get());

    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      CheckPublicKeyValue(iter.get(), i);
      ++i;
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ(kNumKeys, i) << "the forward scan did not cover every key";

    i = kNumKeys - 1;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      CheckPublicKeyValue(iter.get(), i);
      --i;
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ(-1, i) << "the backward scan did not cover every key";

    for (int target : {0, kNumKeys / 4, kNumKeys / 2, kNumKeys - 1}) {
      i = target;
      for (iter->Seek(Key(target)); iter->Valid(); iter->Next()) {
        CheckPublicKeyValue(iter.get(), i);
        ++i;
      }
      ASSERT_OK(iter->status());
      EXPECT_EQ(kNumKeys, i) << "the scan from " << Key(target) << " ended early";

      i = target;
      for (iter->SeekForPrev(Key(target)); iter->Valid(); iter->Prev()) {
        CheckPublicKeyValue(iter.get(), i);
        --i;
      }
      ASSERT_OK(iter->status());
      EXPECT_EQ(-1, i) << "the backward scan from " << Key(target) << " ended early";
    }
  }
  UninstallLevelBindingCallbacks();
  ExpectPublicValuesReadTheCopiedFuncs();
}

// The path MyTopling died on. MyRocks scans a table through the read-your-own-
// writes iterator, which wraps the database iterator as its base and reads the
// value of that base for every key the transaction did not write itself
// (BaseDeltaIterator::UpdateCurrent() -> SetValueAndColumnsFromBase() ->
// base_iterator_->value()). Every death of the server was this, on a plain
// table scan right after a bulk load (SELECT * FROM t1 FORCE INDEX (PRIMARY)),
// so this drives the same chain over the level's file boundaries. See the note
// above for what it can show with and without TOPLINGDB_WITH_WIDE_COLUMNS.
TEST_F(MergerLevelIterTest, ReadYourOwnWritesOverMultiFileLevel) {
  BuildMultiFileLevelAndHighL0File();
  InstallLevelBindingCallbacks();
  LeavePublicValuesUnprepared();
  int boundary = LevelBoundary(1);

  for (bool ignore_range_deletions : {false, true}) {
    ReadOptions read_options;
    read_options.ignore_range_deletions = ignore_range_deletions;

    // A few keys of the transaction's own writes, on both sides of the boundary
    // the level switches files at; every other key is one the base iterator has
    // to answer for.
    WriteBatchWithIndex batch(BytewiseComparator(), /*overwrite_key=*/true);
    for (int i = boundary - 4; i <= boundary + 4; ++i) {
      batch.Put(Key(i), MergerLevelValue(i));
    }

    // The returned iterator owns the base iterator.
    std::unique_ptr<Iterator> iter(batch.NewIteratorWithBase(
        db_->DefaultColumnFamily(), db_->NewIterator(read_options),
        &read_options));
    ASSERT_NE(nullptr, iter.get());

    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      CheckPublicKeyValue(iter.get(), i);
      ++i;
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ(kNumAllKeys, i) << "the forward scan did not cover every key";

    i = kNumAllKeys - 1;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      CheckPublicKeyValue(iter.get(), i);
      --i;
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ(-1, i) << "the backward scan did not cover every key";
  }

  UninstallLevelBindingCallbacks();
  ExpectPublicValuesReadTheCopiedFuncs();
}

// Two ingested files that overlap: the second has to shadow the first, whichever
// level each ends up in. This is the shape a run of MyTopling's bulk load leaves
// behind (its ingest options allow a global seqno), and it is the one that gave
// duplicate primary keys in the server.
TEST_F(MergerLevelIterTest, IngestedOverlappingFiles) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  auto* const cfh = db_->DefaultColumnFamily();

  auto write_and_ingest = [&](const std::string& tag,
                              const std::vector<std::pair<int, std::string>>& rows) {
    const std::string path = dbname_ + "/ingest-" + tag + ".sst";
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.Open(path));
    for (const auto& row : rows) {
      ASSERT_OK(writer.Put(Key(row.first), row.second));
    }
    ASSERT_OK(writer.Finish());
    IngestExternalFileOptions ifo;
    ifo.allow_global_seqno = true;   // what MyTopling sets
    ifo.write_global_seqno = false;  // what MyTopling sets
    ASSERT_OK(db_->IngestExternalFile(cfh, {path}, ifo));
  };

  write_and_ingest("first", {{1, "a"}, {2, "b"}});
  write_and_ingest("second", {{1, "A2"}, {2, "B2"}});

  // The shape that matters: what level each file landed in, and the seqno range
  // the DB recorded for it (0 means the file kept the seqnos inside it).
  {
    std::vector<LiveFileMetaData> files;
    dbfull()->GetLiveFilesMetaData(&files);
    for (const auto& file : files) {
      fprintf(stderr, "DIAG file L%d seqno=[%llu,%llu] size=%zu\n", file.level,
              static_cast<unsigned long long>(file.smallest_seqno),
              static_cast<unsigned long long>(file.largest_seqno), file.size);
    }
  }

  for (bool ignore_range_deletions : {false, true}) {
    ReadOptions read_options;
    read_options.ignore_range_deletions = ignore_range_deletions;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    std::vector<std::string> rows;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      rows.push_back(iter->key().ToString() + "=" + iter->value().ToString());
    }
    ASSERT_OK(iter->status());
    fprintf(stderr, "DIAG ingest ignore_range_deletions=%d rows=%zu:",
            static_cast<int>(ignore_range_deletions), rows.size());
    for (const auto& row : rows) fprintf(stderr, " %s", row.c_str());
    fprintf(stderr, "\n");
    EXPECT_EQ(2u, rows.size())
        << "a key was returned more than once, so the ingested file that "
           "shadowed it was not the only version read";

    // Now the same scan through the read-your-own-writes layer, which is the one
    // MyRocks's own scans go through.
    WriteBatchWithIndex batch(BytewiseComparator(), /*overwrite_key=*/true);
    std::unique_ptr<Iterator> wrapped(batch.NewIteratorWithBase(
        db_->DefaultColumnFamily(), db_->NewIterator(read_options),
        &read_options));
    std::vector<std::string> via_delta;
    for (wrapped->SeekToFirst(); wrapped->Valid(); wrapped->Next()) {
      via_delta.push_back(wrapped->key().ToString() + "=" +
                          wrapped->value().ToString());
    }
    ASSERT_OK(wrapped->status());
    fprintf(stderr, "DIAG ingest via delta, ignore_range_deletions=%d rows=%zu:",
            static_cast<int>(ignore_range_deletions), via_delta.size());
    for (const auto& row : via_delta) fprintf(stderr, " %s", row.c_str());
    fprintf(stderr, "\n");
    EXPECT_EQ(2u, via_delta.size())
        << "the read-your-own-writes layer returned a key more than once";
  }
}

}  // namespace ROCKSDB_NAMESPACE


int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
