// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run this test... Skipping...\n");
  return 0;
}
#else

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <gflags/gflags.h>
#include <vector>
#include <string>
#include <map>

#include "table/meta_blocks.h"
#include <table/terark_zip_table.h>
#include "table/get_context.h"
#include "util/arena.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

using GFLAGS::ParseCommandLineFlags;
using GFLAGS::SetUsageMessage;

DEFINE_string(file_dir, "", "Directory where the files will be created"
    " for benchmark. Added for using tmpfs.");
DEFINE_bool(enable_perf, false, "Run Benchmark Tests too.");
DEFINE_bool(write, false,
    "Should write new values to file in performance tests?");
DEFINE_bool(identity_as_first_hash, true, "use identity as first hash");

namespace rocksdb {

namespace {
const uint32_t kNumTest = 10;
}

class TerarkZipReaderTest : public testing::Test {
 public:
  using testing::Test::SetUp;

  TerarkZipReaderTest() {
    options.allow_mmap_reads = true;
    env = options.env;
    env_options = EnvOptions(options);
  }

  void SetUp(int num) {
    num_items = num;
    keys.clear();
    keys.resize(num_items);
    user_keys.clear();
    user_keys.resize(num_items);
    values.clear();
    values.resize(num_items);
  }

  std::string NumToStr(int64_t i) {
    return std::string(reinterpret_cast<char*>(&i), sizeof(i));
  }

  void CreateTerarkZipFileAndCheckReader(
      const Comparator* ucomp = BytewiseComparator()) {
    std::unique_ptr<WritableFile> writable_file;
    ASSERT_OK(env->NewWritableFile(fname, &writable_file, env_options));
    unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::move(writable_file), env_options));

    std::vector<std::unique_ptr<IntTblPropCollectorFactory>> int_tbl_prop_collector_factories;
    const ImmutableCFOptions ioptions(options);

    std::unique_ptr<TableFactory> table_factory_ptr;
    table_factory_ptr.reset(NewTerarkZipTableFactory(TerarkZipTableOptions(), NewBlockBasedTableFactory(BlockBasedTableOptions())));
    unique_ptr<TableBuilder> table_builder;
    unique_ptr<TableReader> table_reader;
    InternalKeyComparator ic(ucomp);

    table_builder.reset(table_factory_ptr->NewTableBuilder(TableBuilderOptions(ioptions,
                                                                               ic,
                                                                               &int_tbl_prop_collector_factories,
                                                                               CompressionType::kNoCompression,
                                                                               CompressionOptions(),
                                                                               nullptr,
                                                                               false,
                                                                               kDefaultColumnFamilyName,
                                                                               -1), 0, file_writer.get()));

    TableBuilder &builder = *table_builder.get();
    ASSERT_OK(builder.status());
    for (uint32_t key_idx = 0; key_idx < num_items; ++key_idx) {
      builder.Add(Slice(keys[key_idx]), Slice(values[key_idx]));
      ASSERT_OK(builder.status());
      ASSERT_EQ(builder.NumEntries(), key_idx + 1);
    }
    ASSERT_OK(builder.Finish());
    ASSERT_EQ(num_items, builder.NumEntries());
    file_size = builder.FileSize();
    ASSERT_OK(file_writer->Close());

    // Check reader now.
    std::unique_ptr<RandomAccessFile> read_file;
    ASSERT_OK(env->NewRandomAccessFile(fname, &read_file, env_options));
    unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(read_file)));

    table_factory_ptr->NewTableReader(TableReaderOptions(ioptions, env_options, ic), std::move(file_reader), file_size, &table_reader);

    TableReader &reader = *table_reader.get();

    // Assume no merge/deletion
    for (uint32_t i = 0; i < num_items; ++i) {
      std::string value;
      GetContext get_context(ucomp, nullptr, nullptr, nullptr,
                             GetContext::kNotFound, Slice(user_keys[i]), &value,
                             nullptr, nullptr, nullptr, nullptr);
      ASSERT_OK(reader.Get(ReadOptions(), Slice(keys[i]), &get_context));
      ASSERT_EQ(values[i], value);
    }
  }
  void UpdateKeys(bool with_zero_seqno) {
    for (uint32_t i = 0; i < num_items; i++) {
      ParsedInternalKey ikey(user_keys[i],
          with_zero_seqno ? 0 : i + 1000, kTypeValue);
      keys[i].clear();
      AppendInternalKey(&keys[i], ikey);
    }
  }

  void CheckIterator() {
    std::unique_ptr<RandomAccessFile> read_file;
    ASSERT_OK(env->NewRandomAccessFile(fname, &read_file, env_options));
    unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(read_file)));
    const ImmutableCFOptions ioptions(options);

    std::unique_ptr<TableFactory> table_factory_ptr;
    table_factory_ptr.reset(NewTerarkZipTableFactory(TerarkZipTableOptions(), NewBlockBasedTableFactory(BlockBasedTableOptions())));
    unique_ptr<TableReader> table_reader;
    InternalKeyComparator ic(BytewiseComparator());
    table_factory_ptr->NewTableReader(TableReaderOptions(ioptions, env_options, ic), std::move(file_reader), file_size, &table_reader);

    TableReader &reader = *table_reader.get();

    InternalIterator* it = reader.NewIterator(ReadOptions(), nullptr);
    ASSERT_OK(it->status());
    ASSERT_TRUE(!it->Valid());
    it->SeekToFirst();
    int cnt = 0;
    while (it->Valid()) {
      ASSERT_OK(it->status());
      ASSERT_TRUE(Slice(keys[cnt]) == it->key());
      ASSERT_TRUE(Slice(values[cnt]) == it->value());
      ++cnt;
      it->Next();
    }
    ASSERT_EQ(static_cast<uint32_t>(cnt), num_items);

    it->SeekToLast();
    cnt = static_cast<int>(num_items) - 1;
    ASSERT_TRUE(it->Valid());
    while (it->Valid()) {
      ASSERT_OK(it->status());
      ASSERT_TRUE(Slice(keys[cnt]) == it->key());
      ASSERT_TRUE(Slice(values[cnt]) == it->value());
      --cnt;
      it->Prev();
    }
    ASSERT_EQ(cnt, -1);

    cnt = static_cast<int>(num_items) / 2;
    it->Seek(keys[cnt]);
    while (it->Valid()) {
      ASSERT_OK(it->status());
      ASSERT_TRUE(Slice(keys[cnt]) == it->key());
      ASSERT_TRUE(Slice(values[cnt]) == it->value());
      ++cnt;
      it->Next();
    }
    ASSERT_EQ(static_cast<uint32_t>(cnt), num_items);
    delete it;

    Arena arena;
    it = reader.NewIterator(ReadOptions(), &arena);
    ASSERT_OK(it->status());
    ASSERT_TRUE(!it->Valid());
    it->Seek(keys[num_items/2]);
    ASSERT_TRUE(it->Valid());
    ASSERT_OK(it->status());
    ASSERT_TRUE(keys[num_items/2] == it->key());
    ASSERT_TRUE(values[num_items/2] == it->value());
    ASSERT_OK(it->status());
    it->~InternalIterator();
  }

  std::vector<std::string> keys;
  std::vector<std::string> user_keys;
  std::vector<std::string> values;
  uint64_t num_items;
  std::string fname;
  uint64_t file_size;
  Options options;
  Env* env;
  EnvOptions env_options;
};

TEST_F(TerarkZipReaderTest, WhenKeyExists) {
  SetUp(kNumTest);
  fname = test::TmpDir() + "/TerarkZipReader_WhenKeyExists";
  for (uint64_t i = 0; i < num_items; i++) {
    user_keys[i] = "key" + NumToStr(i);
    ParsedInternalKey ikey(user_keys[i], i + 1000, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + NumToStr(i);
  }
  CreateTerarkZipFileAndCheckReader();
  // Last level file.
  UpdateKeys(true);
  CreateTerarkZipFileAndCheckReader();
  UpdateKeys(false);
  CreateTerarkZipFileAndCheckReader();
  // Last level file.
  UpdateKeys(true);
  CreateTerarkZipFileAndCheckReader();
}

TEST_F(TerarkZipReaderTest, CheckIterator) {
  SetUp(2*kNumTest);
  fname = test::TmpDir() + "/TerarkZipReader_CheckIterator";
  for (uint64_t i = 0; i < num_items; i++) {
    user_keys[i] = "key" + NumToStr(i);
    ParsedInternalKey ikey(user_keys[i], 1000, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + NumToStr(i);
  }
  CreateTerarkZipFileAndCheckReader();
  CheckIterator();
  // Last level file.
  UpdateKeys(true);
  CreateTerarkZipFileAndCheckReader();
  CheckIterator();
}

TEST_F(TerarkZipReaderTest, WhenKeyNotFound) {
  SetUp(kNumTest);
  fname = test::TmpDir() + "/TerarkZipReader_WhenKeyNotFound";
  for (uint64_t i = 0; i < num_items; i++) {
    user_keys[i] = "key" + NumToStr(i);
    ParsedInternalKey ikey(user_keys[i], i + 1000, kTypeValue);
    AppendInternalKey(&keys[i], ikey);
    values[i] = "value" + NumToStr(i);
  }
  auto* ucmp = BytewiseComparator();
  CreateTerarkZipFileAndCheckReader();
  std::unique_ptr<RandomAccessFile> read_file;
  ASSERT_OK(env->NewRandomAccessFile(fname, &read_file, env_options));
  unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(read_file)));
  const ImmutableCFOptions ioptions(options);

  std::unique_ptr<TableFactory> table_factory_ptr;
  table_factory_ptr.reset(NewTerarkZipTableFactory(TerarkZipTableOptions(), NewBlockBasedTableFactory(BlockBasedTableOptions())));
  unique_ptr<TableReader> table_reader;
  InternalKeyComparator ic(ucmp);
  table_factory_ptr->NewTableReader(TableReaderOptions(ioptions, env_options, ic), std::move(file_reader), file_size, &table_reader);

  TableReader &reader = *table_reader.get();

  // Search for a key with colliding hash values.
  std::string not_found_user_key = "key" + NumToStr(num_items);
  std::string not_found_key;
  ParsedInternalKey ikey(not_found_user_key, 1000, kTypeValue);
  AppendInternalKey(&not_found_key, ikey);
  std::string value;
  GetContext get_context(ucmp, nullptr, nullptr, nullptr, GetContext::kNotFound,
                         Slice(not_found_key), &value, nullptr, nullptr,
                         nullptr, nullptr);
  ASSERT_OK(reader.Get(ReadOptions(), Slice(not_found_key), &get_context));
  ASSERT_TRUE(value.empty());

  // Search for a key with an independent hash value.
  std::string not_found_user_key2 = "key" + NumToStr(num_items + 1);
  ParsedInternalKey ikey2(not_found_user_key2, 1000, kTypeValue);
  std::string not_found_key2;
  AppendInternalKey(&not_found_key2, ikey2);
  GetContext get_context2(ucmp, nullptr, nullptr, nullptr,
                          GetContext::kNotFound, Slice(not_found_key2), &value,
                          nullptr, nullptr, nullptr, nullptr);
}

// Performance tests
namespace {
void GetKeys(uint64_t num, std::vector<std::string>* keys) {
  keys->clear();
  IterKey k;
  k.SetInternalKey("", 0, kTypeValue);
  std::string internal_key_suffix = k.GetKey().ToString();
  ASSERT_EQ(static_cast<size_t>(8), internal_key_suffix.size());
  for (uint64_t key_idx = 0; key_idx < num; ++key_idx) {
    uint64_t value = 2 * key_idx;
    std::string new_key(reinterpret_cast<char*>(&value), sizeof(value));
    new_key += internal_key_suffix;
    keys->push_back(new_key);
  }
}

std::string GetFileName(uint64_t num) {
  if (FLAGS_file_dir.empty()) {
    FLAGS_file_dir = test::TmpDir();
  }
  return FLAGS_file_dir + "/TerarkZip_read_benchmark" +
    ToString(num/1000000) + "Mkeys";
}

// Create last level file as we are interested in measuring performance of
// last level file only.
void WriteFile(const std::vector<std::string>& keys,
    const uint64_t num) {
  Options options;
  options.allow_mmap_reads = true;
  Env* env = options.env;
  EnvOptions env_options = EnvOptions(options);
  std::string fname = GetFileName(num);

  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env->NewWritableFile(fname, &writable_file, env_options));
  unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(writable_file), env_options));

  const ImmutableCFOptions ioptions(options);
    std::vector<std::unique_ptr<IntTblPropCollectorFactory>> int_tbl_prop_collector_factories;

  std::unique_ptr<TableFactory> table_factory_ptr;
  table_factory_ptr.reset(NewTerarkZipTableFactory(TerarkZipTableOptions(), NewBlockBasedTableFactory(BlockBasedTableOptions())));
  unique_ptr<TableBuilder> table_builder;
  unique_ptr<TableReader> table_reader;
  InternalKeyComparator ic(BytewiseComparator());

  table_builder.reset(table_factory_ptr->NewTableBuilder(TableBuilderOptions(ioptions,
                                                                             ic,
                                                                             &int_tbl_prop_collector_factories,
                                                                             CompressionType::kNoCompression,
                                                                             CompressionOptions(),
                                                                             nullptr,
                                                                             false,
                                                                             kDefaultColumnFamilyName,
                                                                             -1), 0, file_writer.get()));

  TableBuilder &builder = *table_builder.get();

  ASSERT_OK(builder.status());
  for (uint64_t key_idx = 0; key_idx < num; ++key_idx) {
    // Value is just a part of key.
    builder.Add(Slice(keys[key_idx]), Slice(&keys[key_idx][0], 4));
    ASSERT_EQ(builder.NumEntries(), key_idx + 1);
    ASSERT_OK(builder.status());
  }
  ASSERT_OK(builder.Finish());
  ASSERT_EQ(num, builder.NumEntries());
  ASSERT_OK(file_writer->Close());

  uint64_t file_size;
  env->GetFileSize(fname, &file_size);
  std::unique_ptr<RandomAccessFile> read_file;
  ASSERT_OK(env->NewRandomAccessFile(fname, &read_file, env_options));
  unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(read_file)));

  table_factory_ptr->NewTableReader(TableReaderOptions(ioptions, env_options, ic), std::move(file_reader), file_size, &table_reader);

  TableReader &reader = *table_reader.get();
  ReadOptions r_options;
  std::string value;
  // Assume only the fast path is triggered
  GetContext get_context(nullptr, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, Slice(), &value, nullptr,
                         nullptr, nullptr, nullptr);
  for (uint64_t i = 0; i < num; ++i) {
    value.clear();
    ASSERT_OK(reader.Get(r_options, Slice(keys[i]), &get_context));
    ASSERT_TRUE(Slice(keys[i]) == Slice(&keys[i][0], 4));
  }
}

void ReadKeys(uint64_t num, uint32_t batch_size) {
  Options options;
  options.allow_mmap_reads = true;
  Env* env = options.env;
  EnvOptions env_options = EnvOptions(options);
  std::string fname = GetFileName(num);

  uint64_t file_size;
  env->GetFileSize(fname, &file_size);
  std::unique_ptr<RandomAccessFile> read_file;
  ASSERT_OK(env->NewRandomAccessFile(fname, &read_file, env_options));
  unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(read_file)));

  const ImmutableCFOptions ioptions(options);

  InternalKeyComparator ic(BytewiseComparator());
  std::unique_ptr<TableFactory> table_factory_ptr;
  unique_ptr<TableReader> table_reader;
  table_factory_ptr.reset(NewTerarkZipTableFactory(TerarkZipTableOptions(), NewBlockBasedTableFactory(BlockBasedTableOptions())));
  table_factory_ptr->NewTableReader(TableReaderOptions(ioptions, env_options, ic), std::move(file_reader), file_size, &table_reader);

  TableReader &reader = *table_reader.get();
  const UserCollectedProperties user_props =
    reader.GetTableProperties()->user_collected_properties;
  ReadOptions r_options;

  std::vector<uint64_t> keys;
  keys.reserve(num);
  for (uint64_t i = 0; i < num; ++i) {
    keys.push_back(2 * i);
  }
  std::random_shuffle(keys.begin(), keys.end());

  std::string value;
  // Assume only the fast path is triggered
  GetContext get_context(nullptr, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, Slice(), &value, nullptr,
                         nullptr, nullptr, nullptr);
  uint64_t start_time = env->NowMicros();
  if (batch_size > 0) {
    for (uint64_t i = 0; i < num; i += batch_size) {
      for (uint64_t j = i; j < i+batch_size && j < num; ++j) {
        reader.Prepare(Slice(reinterpret_cast<char*>(&keys[j]), 16));
      }
      for (uint64_t j = i; j < i+batch_size && j < num; ++j) {
        reader.Get(r_options, Slice(reinterpret_cast<char*>(&keys[j]), 16),
                   &get_context);
      }
    }
  } else {
    for (uint64_t i = 0; i < num; i++) {
      reader.Get(r_options, Slice(reinterpret_cast<char*>(&keys[i]), 16),
                 &get_context);
    }
  }
  float time_per_op = (env->NowMicros() - start_time) * 1.0f / num;
  fprintf(stderr,
      "Time taken per op is %.3fus (%.1f Mqps) with batch size of %u\n",
      time_per_op, 1.0 / time_per_op, batch_size);
}
}  // namespace.

TEST_F(TerarkZipReaderTest, TestReadPerformance) {
  if (!FLAGS_enable_perf) {
    return;
  }
  // These numbers are chosen to have a hash utilizaiton % close to
  // 0.9, 0.75, 0.6 and 0.5 respectively.
  // They all create 128 M buckets.
  std::vector<uint64_t> nums = {120*1024*1024, 100*1024*1024, 80*1024*1024,
    70*1024*1024};
#ifndef NDEBUG
  fprintf(stdout,
      "WARNING: Not compiled with DNDEBUG. Performance tests may be slow.\n");
#endif
  for (uint64_t num : nums) {
    if (FLAGS_write ||
        Env::Default()->FileExists(GetFileName(num)).IsNotFound()) {
      std::vector<std::string> all_keys;
      GetKeys(num, &all_keys);
      WriteFile(all_keys, num);
    }
    ReadKeys(num, 0);
    ReadKeys(num, 10);
    ReadKeys(num, 25);
    ReadKeys(num, 50);
    ReadKeys(num, 100);
    fprintf(stderr, "\n");
  }
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

#endif  // GFLAGS.

