//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <functional>

#include "db/db_test_util.h"
#include <table/terark_zip_table.h>
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"


std::string localTempDir = "/tmp";

namespace rocksdb {


class Rdb_pk_comparator : public Comparator {
public:
  Rdb_pk_comparator(const Rdb_pk_comparator &) = delete;
  Rdb_pk_comparator &operator=(const Rdb_pk_comparator &) = delete;
  Rdb_pk_comparator() = default;

  static int bytewise_compare(const rocksdb::Slice &a,
    const rocksdb::Slice &b) {
    const size_t a_size = a.size();
    const size_t b_size = b.size();
    const size_t len = (a_size < b_size) ? a_size : b_size;
    int res;

    if ((res = memcmp(a.data(), b.data(), len)))
      return res;

    /* Ok, res== 0 */
    if (a_size != b_size) {
      return a_size < b_size ? -1 : 1;
    }
    return 0;
  }

  /* Override virtual methods of interest */

  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
    return bytewise_compare(a, b);
  }

  const char *Name() const override { return "RocksDB_SE_v3.10"; }

  // TODO: advanced funcs:
  // - FindShortestSeparator
  // - FindShortSuccessor

  // for now, do-nothing implementations:
  void FindShortestSeparator(std::string *start,
    const rocksdb::Slice &limit) const override {}
  void FindShortSuccessor(std::string *key) const override {}
};

class Rdb_rev_comparator : public Comparator {
public:
  Rdb_rev_comparator(const Rdb_rev_comparator &) = delete;
  Rdb_rev_comparator &operator=(const Rdb_rev_comparator &) = delete;
  Rdb_rev_comparator() = default;

  static int bytewise_compare(const rocksdb::Slice &a,
    const rocksdb::Slice &b) {
    return -Rdb_pk_comparator::bytewise_compare(a, b);
  }

  int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
    return -Rdb_pk_comparator::bytewise_compare(a, b);
  }
  const char *Name() const override { return "rev:RocksDB_SE_v3.10"; }
  void FindShortestSeparator(std::string *start,
    const rocksdb::Slice &limit) const override {}
  void FindShortSuccessor(std::string *key) const override {}
};

std::string get_key(size_t i)
{
  char buffer[32];
  snprintf(buffer, sizeof buffer, "%04zd", i);
  return buffer;
}
std::string get_value(size_t i)
{
  char const *str = "0123456789QWERTYUIOPASDFGHJKLZXCVBNM";
  return get_key(i) + (str + (i % (strlen(str) - 1)));
}

Rdb_pk_comparator pk_c;
Rdb_rev_comparator rev_c;

class TerarkZipReaderTest : public DBTestBase {
public:
  TerarkZipReaderTest() : DBTestBase("/terark_zip_reader_test") {}

  void BasicTest(bool rev, size_t count, size_t prefix, size_t blockUnits, size_t minValue) {
    Options options = CurrentOptions();
    TerarkZipTableOptions tzto;
    tzto.disableSecondPassIter = true;
    tzto.keyPrefixLen = prefix;
    tzto.offsetArrayBlockUnits = (uint16_t)blockUnits;
    tzto.minDictZipValueSize = minValue;
    tzto.entropyAlgo = TerarkZipTableOptions::kFSE;
    tzto.localTempDir = localTempDir;
    options.allow_mmap_reads = true;
    if (rev) {
      options.comparator = &rev_c;
    }
    else {
      options.comparator = &pk_c;
    }
    options.table_factory.reset(NewTerarkZipTableFactory(tzto,
      NewBlockBasedTableFactory(BlockBasedTableOptions())));
    DestroyAndReopen(options);
    rocksdb::ReadOptions ro;
    rocksdb::WriteOptions wo;
    rocksdb::FlushOptions fo;
    std::string value;

    auto db = db_;

    for (size_t i = 0; i < count; ++i)
    {
      ASSERT_OK(db->Put(wo, get_key(i), get_value(i)));
    }
    ASSERT_OK(db->Flush(fo));
    ASSERT_OK(db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr));

    for (size_t i = 0; i < count; ++i)
    {
      ASSERT_OK(db->Get(ro, get_key(i), &value));
      ASSERT_EQ(value, get_value(i));
    }
    auto it = db->NewIterator(ro);
    auto forward = [&](size_t i, int d, size_t e) {
      for (it->SeekToFirst(); it->Valid(); it->Next())
      {
        ASSERT_EQ(it->key().ToString(), get_key(i));
        ASSERT_EQ(it->value().ToString(), get_value(i));
        i += d;
      }
      ASSERT_EQ(i, e);
    };
    auto backward = [&](size_t i, int d, size_t e) {
      for (it->SeekToLast(); it->Valid(); it->Prev())
      {
        ASSERT_EQ(it->key().ToString(), get_key(i));
        ASSERT_EQ(it->value().ToString(), get_value(i));
        i += d;
      }
      ASSERT_EQ(i, e);
    };
    if (rev) {
      forward(count - 1, -1, -1);
      backward(0, 1, count);
    }
    else {
      forward(0, 1, count);
      backward(count - 1, -1, -1);
    }
    delete it;
  }
  void HardZipTest(bool rev, size_t count, size_t prefix) {
    Options options = CurrentOptions();
    TerarkZipTableOptions tzto;
    tzto.disableSecondPassIter = true;
    tzto.keyPrefixLen = prefix;
    tzto.localTempDir = localTempDir;
    tzto.minDictZipValueSize = 0;
    tzto.singleIndexMemLimit = 512;
    options.allow_mmap_reads = true;
    if (rev) {
      options.comparator = &rev_c;
    }
    else {
      options.comparator = &pk_c;
    }
    options.table_factory.reset(NewTerarkZipTableFactory(tzto,
      NewBlockBasedTableFactory(BlockBasedTableOptions())));
    DestroyAndReopen(options);
    rocksdb::ReadOptions ro;
    rocksdb::WriteOptions wo;
    rocksdb::FlushOptions fo;
    std::string value;

    auto db = db_;
    auto seed = *(uint64_t*)"__Terark";
    std::mt19937_64 mt;
    mt.seed(seed);
    std::uniform_int_distribution<size_t> uid(16, 64);
    count = (count + 7) / 8;
    for (size_t sst = 0; sst < 8; ++sst)
    {
      for (size_t i = sst, e = i + 8 * count; i < e; i += 8)
      {
        std::string value;
        value.resize(uid(mt) * 8);
        for (size_t l = 0; l < value.size(); l += 8)
        {
          *(uint64_t *)(&value.front() + l) = mt();
        }
        ASSERT_OK(db->Put(wo, get_key(i), value));
      }
      ASSERT_OK(db->Flush(fo));
    }
    ASSERT_OK(db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr));
    mt.seed(seed);
    for (size_t sst = 0; sst < 8; ++sst)
    {
      for (size_t i = sst, e = i + 8 * count; i < e; i += 8)
      {
        std::string value, value_get;
        value.resize(uid(mt) * 8);
        for (size_t l = 0; l < value.size(); l += 8)
        {
          *(uint64_t *)(&value.front() + l) = mt();
        }
        ASSERT_OK(db->Get(ro, get_key(i), &value_get));
        ASSERT_EQ(value, value_get);
      }
    }
  }
  void ZeroStoreTest(bool rev, size_t count, size_t prefix) {
    Options options = CurrentOptions();
    TerarkZipTableOptions tzto;
    tzto.disableSecondPassIter = true;
    tzto.keyPrefixLen = prefix;
    tzto.entropyAlgo = TerarkZipTableOptions::kFSE;
    tzto.localTempDir = localTempDir;
    options.allow_mmap_reads = true;
    if (rev) {
      options.comparator = &rev_c;
    }
    else {
      options.comparator = &pk_c;
    }
    options.table_factory.reset(NewTerarkZipTableFactory(tzto,
      NewBlockBasedTableFactory(BlockBasedTableOptions())));
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);
    rocksdb::ReadOptions ro;
    rocksdb::WriteOptions wo;
    rocksdb::FlushOptions fo;
    std::string value;

    auto db = db_;

    count = (count + 3) / 4;
    for (size_t sst = 0; sst < 2; ++sst)
    {
      for (size_t i = sst * count, e = i + count; i < e; ++i)
      {
        ASSERT_OK(db->Put(wo, get_key(i), ""));
      }
      ASSERT_OK(db->Flush(fo));
    }
    ASSERT_OK(db->Put(wo, get_key(count * 2), ""));
    ASSERT_OK(db->Flush(fo));
    rocksdb::ColumnFamilyMetaData meta;
    db->GetColumnFamilyMetaData(&meta);
    ASSERT_EQ(meta.levels[0].files.size(), 3);
    db->CompactFiles(CompactionOptions(), {
        meta.levels[0].files[0].name,
        meta.levels[0].files[1].name,
        meta.levels[0].files[2].name,
    }, 1);
    ASSERT_OK(db->Delete(wo, get_key(count * 2)));
    ASSERT_OK(db->Flush(fo));
    db->GetColumnFamilyMetaData(&meta);
    ASSERT_EQ(meta.levels[0].files.size(), 1);
    ASSERT_EQ(meta.levels[1].files.size(), 1);
    db->CompactFiles(CompactionOptions(), {
        meta.levels[0].files[0].name,
        meta.levels[1].files[0].name,
    }, 2);

    count *= 2;
    for (size_t i = 0; i < count; ++i)
    {
      ASSERT_OK(db->Get(ro, get_key(i), &value));
      ASSERT_EQ(value, "");
    }
    auto it = db->NewIterator(ro);
    auto forward = [&](size_t i, int d, size_t e) {
      for (it->SeekToFirst(); it->Valid(); it->Next())
      {
        ASSERT_EQ(it->key().ToString(), get_key(i));
        ASSERT_EQ(it->value().ToString(), "");
        i += d;
      }
      ASSERT_EQ(i, e);
    };
    auto backward = [&](size_t i, int d, size_t e) {
      for (it->SeekToLast(); it->Valid(); it->Prev())
      {
        ASSERT_EQ(it->key().ToString(), get_key(i));
        ASSERT_EQ(it->value().ToString(), "");
        i += d;
      }
      ASSERT_EQ(i, e);
    };
    if (rev) {
      forward(count - 1, -1, -1);
      backward(0, 1, count);
    }
    else {
      forward(0, 1, count);
      backward(count - 1, -1, -1);
    }
    delete it;
  }
  void IterTest(std::initializer_list<const char*> data_list,
    std::initializer_list<const char*> test_list,
    bool rev, size_t prefix, size_t blockUnits, size_t minValue, size_t indexMem = 2ULL << 30) {
    Options options = CurrentOptions();
    TerarkZipTableOptions tzto;
    tzto.disableSecondPassIter = true;
    tzto.keyPrefixLen = prefix;
    tzto.offsetArrayBlockUnits = (uint16_t)blockUnits;
    tzto.minDictZipValueSize = minValue;
    tzto.singleIndexMemLimit = indexMem;
    tzto.entropyAlgo = TerarkZipTableOptions::kFSE;
    tzto.localTempDir = localTempDir;
    options.allow_mmap_reads = true;
    if (rev) {
      options.comparator = &rev_c;
    }
    else {
      options.comparator = &pk_c;
    }
    options.table_factory.reset(NewTerarkZipTableFactory(tzto,
      NewBlockBasedTableFactory(BlockBasedTableOptions())));
    DestroyAndReopen(options);
    rocksdb::ReadOptions ro1, ro2, ro3;
    rocksdb::WriteOptions wo;
    rocksdb::FlushOptions fo;

    struct comp_t
    {
      bool greater = 0;
      bool operator()(const std::string& l, const std::string& r) const
      {
        if (greater)
        {
          return l > r;
        }
        else
        {
          return l < r;
        }
      }
    } comp;
    comp.greater = rev;
    std::set<std::string, comp_t> key_set(comp);
    auto db = db_;
    auto put = [&](std::string&& k, const std::string& s)
    {
      ASSERT_OK(db->Put(wo, k, k + s));
    };
    for (auto d : data_list)
    {
      key_set.emplace(d);
      put(d, "-1");
    }
    auto s1 = db->GetSnapshot();
    ro1.snapshot = s1;
    for (auto d : data_list)
    {
      put(d, "-2");
    }
    auto s2 = db->GetSnapshot();
    ro2.snapshot = s2;
    for (auto d : data_list)
    {
      put(d, "-3");
    }

    ASSERT_OK(db->Flush(fo));

    auto i1 = db->NewIterator(ro1);
    auto i2 = db->NewIterator(ro2);
    auto i3 = db->NewIterator(ro3);

    auto basic_test = [&](Iterator *i, std::string s)
    {
      i->SeekToFirst();
      for (auto it = key_set.begin(); it != key_set.end(); ++it, i->Next()) {
        ASSERT_EQ(i->value().ToString(), *it + s);
      }
      ASSERT_FALSE(i->Valid());
      i->SeekToLast();
      for (auto it = key_set.rbegin(); it != key_set.rend(); ++it, i->Prev()) {
        ASSERT_EQ(i->value().ToString(), *it + s);
      }
      ASSERT_FALSE(i->Valid());
    };
    basic_test(i1, "-1");
    basic_test(i2, "-2");
    basic_test(i3, "-3");

    auto test = [&](const std::string& s)
    {
      auto lb = key_set.lower_bound(s);
      i1->Seek(s);
      i2->Seek(s);
      i3->Seek(s);
      if (lb == key_set.end())
      {
        ASSERT_FALSE(i1->Valid());
        ASSERT_FALSE(i2->Valid());
        ASSERT_FALSE(i3->Valid());
      }
      else
      {
        ASSERT_EQ(i1->value().ToString(), *lb + "-1");
        ASSERT_EQ(i2->value().ToString(), *lb + "-2");
        ASSERT_EQ(i3->value().ToString(), *lb + "-3");
      }
      auto up = key_set.upper_bound(s);
      i1->SeekForPrev(s);
      i2->SeekForPrev(s);
      i3->SeekForPrev(s);
      if (up == key_set.begin())
      {
        ASSERT_FALSE(i1->Valid());
        ASSERT_FALSE(i2->Valid());
        ASSERT_FALSE(i3->Valid());
      }
      else
      {
        --up;
        ASSERT_EQ(i1->value().ToString(), *up + "-1");
        ASSERT_EQ(i2->value().ToString(), *up + "-2");
        ASSERT_EQ(i3->value().ToString(), *up + "-3");
      }
    };
    for (auto t : test_list)
    {
      test(t);
    }

    db->ReleaseSnapshot(s1);
    db->ReleaseSnapshot(s2);
    delete i1;
    delete i2;
    delete i3;
  }
};


TEST_F(TerarkZipReaderTest, BasicTest                        ) { BasicTest(false, 1000, 0,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestRev                     ) { BasicTest(true , 1000, 0,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMulti                   ) { BasicTest(false, 1000, 1,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiRev                ) { BasicTest(true , 1000, 1,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiUint               ) { BasicTest(false, 1000, 2,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiUintRev            ) { BasicTest(true , 1000, 2,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiMany               ) { BasicTest(false, 1000, 3,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiManyRev            ) { BasicTest(true , 1000, 3,   0,    0); }
TEST_F(TerarkZipReaderTest, BasicTestDictZipZO64             ) { BasicTest(false,  100, 0,  64,    0); }
TEST_F(TerarkZipReaderTest, BasicTestDictZipZO64Rev          ) { BasicTest(true ,  100, 0,  64,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiDictZipZO64        ) { BasicTest(false,  100, 3,  64,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiDictZipZO64Rev     ) { BasicTest(true ,  100, 3,  64,    0); }
TEST_F(TerarkZipReaderTest, BasicTestDictZipZO128            ) { BasicTest(false,  100, 0, 128,    0); }
TEST_F(TerarkZipReaderTest, BasicTestDictZipZO128Rev         ) { BasicTest(true ,  100, 0, 128,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiDictZipZO128       ) { BasicTest(false,  100, 3, 128,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiDictZipZO128Rev    ) { BasicTest(true ,  100, 3, 128,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiManyDictZipZO128   ) { BasicTest(false,  100, 4, 128,    0); }
TEST_F(TerarkZipReaderTest, BasicTestMultiManyDictZipZO128Rev) { BasicTest(true ,  100, 4, 128,    0); }
TEST_F(TerarkZipReaderTest, BasicTestZipOffset64             ) { BasicTest(false,  100, 0,  64, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestZipOffset64Rev          ) { BasicTest(true ,  100, 0,  64, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestMultiZipOffset64        ) { BasicTest(false,  100, 3,  64, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestMultiZipOffset64Rev     ) { BasicTest(true ,  100, 3,  64, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestZipOffset128            ) { BasicTest(false,  100, 0, 128, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestZipOffset128Rev         ) { BasicTest(true ,  100, 0, 128, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestMultiZipOffset128       ) { BasicTest(false,  100, 3, 128, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestMultiZipOffset128Rev    ) { BasicTest(true ,  100, 3, 128, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestMultiManyZipOffset128   ) { BasicTest(false,  100, 4, 128, 1024); }
TEST_F(TerarkZipReaderTest, BasicTestMultiManyZipOffset128Rev) { BasicTest(true ,  100, 4, 128, 1024); }


TEST_F(TerarkZipReaderTest, HardZipTest            ) { HardZipTest(false, 1000, 0); }
TEST_F(TerarkZipReaderTest, HardZipTestRev         ) { HardZipTest(true , 1000, 0); }
TEST_F(TerarkZipReaderTest, HardZipTestMulti       ) { HardZipTest(false, 1000, 1); }
TEST_F(TerarkZipReaderTest, HardZipTestMultiRev    ) { HardZipTest(true , 1000, 1); }
TEST_F(TerarkZipReaderTest, HardZipTestMultiMany   ) { HardZipTest(false, 1000, 2); }
TEST_F(TerarkZipReaderTest, HardZipTestMultiManyRev) { HardZipTest(true , 1000, 2); }

TEST_F(TerarkZipReaderTest, ZeroStoreTest            ) { ZeroStoreTest(false, 1000, 0); }
TEST_F(TerarkZipReaderTest, ZeroStoreTestRev         ) { ZeroStoreTest(true , 1000, 0); }
TEST_F(TerarkZipReaderTest, ZeroStoreTestMulti       ) { ZeroStoreTest(false, 1000, 1); }
TEST_F(TerarkZipReaderTest, ZeroStoreTestMultiRev    ) { ZeroStoreTest(true , 1000, 1); }
TEST_F(TerarkZipReaderTest, ZeroStoreTestMultiMany   ) { ZeroStoreTest(false, 1000, 2); }
TEST_F(TerarkZipReaderTest, ZeroStoreTestMultiManyRev) { ZeroStoreTest(true , 1000, 2); }


TEST_F(TerarkZipReaderTest, SingleRecordTest) {
  auto data_list =
  {
    "0000AAAAXXXX",
  };
  IterTest(data_list, {}, false, 0, 0, 1024);
  IterTest(data_list, {}, true , 0, 0, 1024);
  IterTest(data_list, {}, false, 4, 0, 1024);
  IterTest(data_list, {}, true , 4, 0, 1024);
}


TEST_F(TerarkZipReaderTest, UIntIteratorTest) {
  auto data_list =
  {
    "0000AAAAXXXX",
    "0000AAAAYYYY",
    "0000AAAAZZZZ",
  };
  auto test_list =
  {
    "",
    "#",
    "0",
    "0000",
    "00000",
    "0000A",
    "0A",
    "0AAA",
    "0AAAA",
    "0000B",
    "0000AB",
    "0000AAAA",
    "0000AAAAA",
    "0000ABBB",
    "0000ABBBB",
    "0000AAAAX",
    "0000AAAAXY",
    "0000AAAAXXXX",
    "0000AAAAXXXXX",
    "1",
    "1111",
    "11111",
  };
  IterTest(data_list, test_list, false, 0, 0, 1024);
  IterTest(data_list, test_list, true , 0, 0, 1024);
  IterTest(data_list, test_list, false, 1, 0, 1024);
  IterTest(data_list, test_list, true , 1, 0, 1024);
  IterTest(data_list, test_list, false, 2, 0, 1024);
  IterTest(data_list, test_list, true , 2, 0, 1024);
  IterTest(data_list, test_list, false, 3, 0, 1024);
  IterTest(data_list, test_list, true , 3, 0, 1024);
  IterTest(data_list, test_list, false, 4, 0, 1024);
  IterTest(data_list, test_list, true , 4, 0, 1024);
}

TEST_F(TerarkZipReaderTest, PrefixTest) {
  auto data_list =
  {
    "0000AAAAXXXX",
    "1111BBBBYYYY",
    "2222CCCCZZZZ",
  };
  auto test_list =
  {
    "",
    "#",
    "0",
    "0000",
    "00000",
    "0000A",
    "0A",
    "0AAA",
    "0AAAA",
    "0000B",
    "0000AB",
    "0000AAAA",
    "0000AAAAA",
    "0000ABBB",
    "0000ABBBB",
    "0000AAAAX",
    "0000AAAAXY",
    "0000AAAAXXXX",
    "0000AAAAXXXXX",
    "1",
    "1111",
    "11111",
  };
  IterTest(data_list, test_list, false, 0, 0, 1024);
  IterTest(data_list, test_list, true , 0, 0, 1024);
  IterTest(data_list, test_list, false, 1, 0, 1024);
  IterTest(data_list, test_list, true , 1, 0, 1024);
  IterTest(data_list, test_list, false, 2, 0, 1024);
  IterTest(data_list, test_list, true , 2, 0, 1024);
  IterTest(data_list, test_list, false, 3, 0, 1024);
  IterTest(data_list, test_list, true , 3, 0, 1024);
  IterTest(data_list, test_list, false, 4, 0, 1024);
  IterTest(data_list, test_list, true , 4, 0, 1024);
  IterTest(data_list, test_list, false, 7, 0, 1024);
  IterTest(data_list, test_list, true , 7, 0, 1024);
  IterTest(data_list, test_list, false, 8, 0, 1024);
  IterTest(data_list, test_list, true , 8, 0, 1024);
  IterTest(data_list, test_list, false, 9, 0, 1024);
  IterTest(data_list, test_list, true , 9, 0, 1024);
}

TEST_F(TerarkZipReaderTest, PrefixMoreTest) {
  auto data_list =
  {
    "####0000",
    "####0001",
    "####0002",
    "####0009",
    "0000AAAAXXXX",
    "0000AAAAYYYY",
    "0000AAAAZZZZ",
    "1111AAAA",
    "1111BBBB",
    "1111CCCC",
    "AAAA",
    "BBBB",
    "CCCC",
  };
  auto test_list =
  {
    "",
    "#",
    "####",
    "#####",
    "####0",
    "####000",
    "####0000",
    "####00000",
    "####0001",
    "####0003",
    "####1000",
    "####A",
    "0",
    "0000",
    "00000",
    "0000A",
    "0A",
    "0AAA",
    "0AAAA",
    "0000B",
    "0000AB",
    "0000AAAA",
    "0000AAAAA",
    "0000ABBB",
    "0000ABBBB",
    "0000AAAAX",
    "0000AAAAXY",
    "0000AAAAXXXX",
    "0000AAAAXXXXX",
    "1",
    "1111",
    "11111",
    "1111A",
    "1A",
    "1AAA",
    "1AAAA",
    "1111AAAA",
    "1111AAAAA",
    "1111AB",
    "1111ABBB",
    "1111ABBBB",
    "2",
    "2222",
    "22222",
    "2222A",
    "A",
    "AAAA",
    "AAAAA",
    "AB",
    "ABBB",
    "ABBBB",
    "D",
    "DDDD",
    "DDDDD",
  };
  IterTest(data_list, test_list, false, 0, 0, 1024);
  IterTest(data_list, test_list, true , 0, 0, 1024);
  IterTest(data_list, test_list, false, 1, 0, 1024);
  IterTest(data_list, test_list, true , 1, 0, 1024);
  IterTest(data_list, test_list, false, 2, 0, 1024);
  IterTest(data_list, test_list, true , 2, 0, 1024);
  IterTest(data_list, test_list, false, 3, 0, 1024);
  IterTest(data_list, test_list, true , 3, 0, 1024);
  IterTest(data_list, test_list, false, 4, 0, 1024);
  IterTest(data_list, test_list, true , 4, 0, 1024);
  IterTest(data_list, test_list, false, 0, 0, 1024, 1);
  IterTest(data_list, test_list, true , 0, 0, 1024, 1);
  IterTest(data_list, test_list, false, 1, 0, 1024, 1);
  IterTest(data_list, test_list, true , 1, 0, 1024, 1);
  IterTest(data_list, test_list, false, 2, 0, 1024, 1);
  IterTest(data_list, test_list, true , 2, 0, 1024, 1);
  IterTest(data_list, test_list, false, 3, 0, 1024, 1);
  IterTest(data_list, test_list, true , 3, 0, 1024, 1);
  IterTest(data_list, test_list, false, 4, 0, 1024, 1);
  IterTest(data_list, test_list, true , 4, 0, 1024, 1);
}

TEST_F(TerarkZipReaderTest, StoreBuildTest) {
  auto data_list =
  {
    "0000AAAAXXXX",
    "1111BBBBYYYY",
    "2222CCCCZZZZ",
  };
  auto test_list =
  {
    "",
    "0",
    "2222",
  };
  IterTest(data_list, test_list, false, 0,  0,    0);
  IterTest(data_list, test_list, true , 0,  0,    0);
  IterTest(data_list, test_list, false, 0, 64,    0);
  IterTest(data_list, test_list, true , 0, 64,    0);
  IterTest(data_list, test_list, false, 0,  0, 1024);
  IterTest(data_list, test_list, true , 0,  0, 1024);
  IterTest(data_list, test_list, false, 0, 64, 1024);
  IterTest(data_list, test_list, true , 0, 64, 1024);
  IterTest(data_list, test_list, false, 4,  0,    0);
  IterTest(data_list, test_list, true , 4,  0,    0);
  IterTest(data_list, test_list, false, 4, 64,    0);
  IterTest(data_list, test_list, true , 4, 64,    0);
  IterTest(data_list, test_list, false, 4,  0, 1024);
  IterTest(data_list, test_list, true , 4,  0, 1024);
  IterTest(data_list, test_list, false, 4, 64, 1024);
  IterTest(data_list, test_list, true , 4, 64, 1024);
}

TEST_F(TerarkZipReaderTest, IndexBuildTest) {
  auto data_list =
  {
    "0000AAAAWWWW",
    "0000AAAAXXX",
    "0000AAAAYY",
    "0000AAAAZ",
    "1111BBBB",
    "1111CCC",
    "1111DD",
    "1111E",
    "2222",
  };
  auto test_list =
  {
    "",
    "3",
    "0000AAAAZZZZ",
  };
  IterTest(data_list, test_list, false, 0,  0, 1024);
  IterTest(data_list, test_list, true , 0,  0, 1024);
  IterTest(data_list, test_list, false, 0, 64, 1024);
  IterTest(data_list, test_list, true , 0, 64, 1024);
  IterTest(data_list, test_list, false, 4,  0, 1024);
  IterTest(data_list, test_list, true , 4,  0, 1024);
  IterTest(data_list, test_list, false, 4, 64, 1024);
  IterTest(data_list, test_list, true , 4, 64, 1024);
  IterTest(data_list, test_list, false, 0,  0, 1024, 1);
  IterTest(data_list, test_list, true , 0,  0, 1024, 1);
  IterTest(data_list, test_list, false, 0, 64, 1024, 1);
  IterTest(data_list, test_list, true , 0, 64, 1024, 1);
  IterTest(data_list, test_list, false, 4,  0, 1024, 1);
  IterTest(data_list, test_list, true , 4,  0, 1024, 1);
  IterTest(data_list, test_list, false, 4, 64, 1024, 1);
  IterTest(data_list, test_list, true , 4, 64, 1024, 1);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  fprintf(stderr, "exit in 5 seconds\n");
  std::this_thread::sleep_for(std::chrono::seconds(5));
  return ret;
}
