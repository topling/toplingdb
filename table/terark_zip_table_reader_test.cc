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
#include <table/terark_zip_weak_function.h>
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"

namespace rocksdb {

class TerarkZipReaderTest : public DBTestBase {
public:
  TerarkZipReaderTest() : DBTestBase("/terark_zip_reader_test") {}
};

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

TEST_F(TerarkZipReaderTest, BasicTest) {

  auto run_test = [&](bool rev, size_t prefix) {
    Options options = CurrentOptions();
    TerarkZipTableOptions tzto;
    Rdb_pk_comparator pk_c;
    Rdb_rev_comparator rev_c;
    tzto.disableSecondPassIter = true;
    options.allow_mmap_reads = true;
    tzto.keyPrefixLen = prefix;
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

    const size_t count = 1000;

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
  };
  run_test(false, 0);
  run_test(true , 0);
  run_test(false, 1);
  run_test(true , 1);
  run_test(false, 2);
  run_test(true , 2);
  run_test(false, 3);
  run_test(true , 3);
}

TEST_F(TerarkZipReaderTest, IterTest) {

  auto run_test = [&](bool rev, size_t prefix
    , std::initializer_list<const char*> data_list
    , std::initializer_list<const char*> test_list
    ) {
    Options options = CurrentOptions();
    TerarkZipTableOptions tzto;
    Rdb_pk_comparator pk_c;
    Rdb_rev_comparator rev_c;
    tzto.disableSecondPassIter = true;
    options.allow_mmap_reads = true;
    options.compaction_style = kCompactionStyleNone;
    tzto.keyPrefixLen = prefix;
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
    int c = 0;
    auto put = [&](std::string&& s)
    {
      ASSERT_OK(db->Put(wo, s, s));
      key_set.emplace(std::move(s));
    };
    for (auto d : data_list)
    {
      put(d);
    }

    ASSERT_OK(db->Flush(fo));

    auto i = db->NewIterator(ro);

    i->SeekToFirst();
    for (auto it = key_set.begin(); it != key_set.end(); ++it, i->Next()) {
      ASSERT_EQ(i->value().ToString(), *it);
    }
    ASSERT_FALSE(i->Valid());
    i->SeekToLast();
    for (auto it = key_set.rbegin(); it != key_set.rend(); ++it, i->Prev()) {
      ASSERT_EQ(i->value().ToString(), *it);
    }
    ASSERT_FALSE(i->Valid());

    auto test = [&](const std::string& s)
    {
      auto lb = key_set.lower_bound(s);
      i->Seek(s);
      if (lb == key_set.end())
      {
        ASSERT_FALSE(i->Valid());
      }
      else
      {
        ASSERT_EQ(i->value().ToString(), *lb);
      }
      auto up = key_set.upper_bound(s);
      i->SeekForPrev(s);
      if (up == key_set.begin())
      {
        ASSERT_FALSE(i->Valid());
      }
      else
      {
        ASSERT_EQ(i->value().ToString(), *(--up));
      }
    };
    for (auto t : test_list)
    {
      test(t);
    }

    delete i;
  };

  std::initializer_list<const char*> data_list, test_list;

  data_list =
  {
    "0000AAAAXXXX",
    "0000AAAAYYYY",
    "0000AAAAZZZZ",
  };
  test_list =
  {
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
  run_test(false, 0, data_list, test_list);
  run_test(true , 0, data_list, test_list);
  run_test(false, 1, data_list, test_list);
  run_test(true , 1, data_list, test_list);
  run_test(false, 2, data_list, test_list);
  run_test(true , 2, data_list, test_list);
  run_test(false, 3, data_list, test_list);
  run_test(true , 3, data_list, test_list);
  run_test(false, 4, data_list, test_list);
  run_test(true , 4, data_list, test_list);

  data_list =
  {
    "0000AAAAXXXX",
    "1111BBBBYYYY",
    "2222CCCCZZZZ",
  };
  test_list =
  {
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
  run_test(false, 0, data_list, test_list);
  run_test(true , 0, data_list, test_list);
  run_test(false, 1, data_list, test_list);
  run_test(true , 1, data_list, test_list);
  run_test(false, 2, data_list, test_list);
  run_test(true , 2, data_list, test_list);
  run_test(false, 3, data_list, test_list);
  run_test(true , 3, data_list, test_list);
  run_test(false, 4, data_list, test_list);
  run_test(true , 4, data_list, test_list);
  run_test(false, 7, data_list, test_list);
  run_test(true , 7, data_list, test_list);
  run_test(false, 8, data_list, test_list);
  run_test(true,  8, data_list, test_list);
  run_test(false, 9, data_list, test_list);
  run_test(true,  9, data_list, test_list);

  data_list =
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
  test_list =
  {
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
  run_test(false, 0, data_list, test_list);
  run_test(true , 0, data_list, test_list);
  run_test(false, 1, data_list, test_list);
  run_test(true , 1, data_list, test_list);
  run_test(false, 2, data_list, test_list);
  run_test(true , 2, data_list, test_list);
  run_test(false, 3, data_list, test_list);
  run_test(true , 3, data_list, test_list);
  run_test(false, 4, data_list, test_list);
  run_test(true , 4, data_list, test_list);
}


}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
