//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/udt_util.h"
#include <terark/fstring.hpp>

namespace ROCKSDB_NAMESPACE {
namespace log {

class Writer::WriterKeyCompare {
public:
  using is_transparent = void;
  using LookupKey = std::pair<FileSystem*, const std::string*>;
  bool LessThan(const LookupKey& x, const LookupKey& y) const {
    assert(x.first != nullptr);
    assert(y.first != nullptr);
    if (x.first != y.first)
      return x.first < y.first;
    else
      return *x.second < *y.second;
  }
  bool operator()(const Writer* x, const Writer* y) const {
    return LessThan({x->fs_, &x->fname_}, {y->fs_, &y->fname_});
  }
  bool operator()(const Writer* x, const LookupKey& y) const {
    return LessThan({x->fs_, &x->fname_}, y);
  }
  bool operator()(const LookupKey& x, const Writer* y) const {
    return LessThan(x, {y->fs_, &y->fname_});
  }
};
static std::set<const Writer*, Writer::WriterKeyCompare> g_reg;
static std::mutex g_mtx;

std::shared_ptr<uint64_t>
GetLogWriterFileOffset(FileSystem& fs, const std::string& fname) {
  std::lock_guard<std::mutex> lk(g_mtx);
  auto iter = g_reg.find(Writer::WriterKeyCompare::LookupKey(&fs, &fname));
  if (iter != g_reg.end())
    return (*iter)->get_log_offset_ptr();
  else
    return nullptr;
}

// when use writev, skip buffer in WritableFileWriter
const static bool g_WAL_USE_WRITEV = terark::getEnvBool
                  ("WAL_USE_WRITEV", TERARK_IF_DEBUG(false, true));

Writer::Writer(std::unique_ptr<WritableFileWriter>&& dest, uint64_t log_number,
               bool recycle_log_files, bool manual_flush,
               CompressionType compression_type)
    : dest_(std::move(dest)),
      log_number_(log_number),
      recycle_log_files_(recycle_log_files),
      manual_flush_(manual_flush),
      compression_type_(compression_type),
      block_offset_(0),
      compress_(nullptr) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
  fs_ = nullptr;
  memtable_as_log_index_ = false;
  use_writev_ = g_WAL_USE_WRITEV;
}

Writer::~Writer() {
  if (dest_) {
    //WriteBuffer().PermitUncheckedError();
    Close().PermitUncheckedError();
  }
  if (compress_) {
    delete compress_;
  }
  if (memtable_as_log_index_) {
    std::lock_guard<std::mutex> lk(g_mtx);
    auto iter = g_reg.find(this);
    TERARK_VERIFY_S(g_reg.end() != iter, "%s is not registered", fname_);
    g_reg.erase(iter);
  }
}

IOStatus Writer::WriteBuffer() {
  if (dest_->seen_error()) {
    return IOStatus::IOError("Seen error. Skip writing buffer.");
  }
  return dest_->Flush();
}

IOStatus Writer::Close() {
  IOStatus s;
  if (dest_) {
    s = dest_->Close();
    dest_.reset();
  }
  return s;
}

void Writer::InitReaderMmap(FileSystem& fs, size_t mmap_size) {
  auto& fname = dest_->file_name();
  if (mmap_size > (8ull<<40)) {
    fprintf(stderr,
      "WARN: Writer::InitReaderMmap: mmap_size %.6f TiB, reset to 32 GiB\n",
      mmap_size / double(1ull<<40));
    mmap_size = 32ull << 30; // 32G
  }
  if (0 == mmap_size) {
    mmap_size = size_t(1) << 30; // 1G
  }
  auto s = ReadonlyFileMmap::New(&mmap_reader_, fs, log_number_, fname, mmap_size);
  TERARK_VERIFY_S(s.ok(), "mmap size %zd for %s, %s", mmap_size, fname, s.ToString());
  TERARK_VERIFY_GE(mmap_reader_->size_, mmap_size);
  fname_ = fname;
  fs_ = &fs;
  log_offset_ = std::make_shared<uint64_t>(0);
  mmap_reader_->tail_pos = log_offset_;
  memtable_as_log_index_ = true;
  std::lock_guard<std::mutex> lk(g_mtx);
  auto [iter, insert_ok] = g_reg.insert(this);
  TERARK_VERIFY_S(insert_ok, "%s is registered", fname_);
}

IOStatus Writer::AddRecord(const Slice& slice,
                           Env::IOPriority rate_limiter_priority) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  if (LIKELY(memtable_as_log_index_)) {
    IOStatus s = EmitPhysicalRecord(kFullType, ptr, left, rate_limiter_priority);
    if (UNLIKELY(!use_writev_ && !manual_flush_ && s.ok())) {
      s = dest_->Flush();
    }
    return s;
  }

  // Header size varies depending on whether we are recycling or not.
  const int header_size =
      recycle_log_files_ ? kRecyclableHeaderSize : kHeaderSize;

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  bool begin = true;
  int compress_remaining = 0;
  bool compress_start = false;
  if (compress_) {
    compress_->Reset();
    compress_start = true;
  }

  IOStatus s;
  do {
    const int64_t leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < header_size) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize and
        // kRecyclableHeaderSize being <= 11)
        assert(header_size <= 11);
        s = dest_->Append(Slice("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                                static_cast<size_t>(leftover)),
                          0 /* crc32c_checksum */, rate_limiter_priority);
        if (!s.ok()) {
          break;
        }
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < header_size bytes in a block.
    assert(static_cast<int64_t>(kBlockSize - block_offset_) >= header_size);

    const size_t avail = kBlockSize - block_offset_ - header_size;

    // Compress the record if compression is enabled.
    // Compress() is called at least once (compress_start=true) and after the
    // previous generated compressed chunk is written out as one or more
    // physical records (left=0).
    if (compress_ && (compress_start || left == 0)) {
      compress_remaining = compress_->Compress(slice.data(), slice.size(),
                                               compressed_buffer_.get(), &left);

      if (compress_remaining < 0) {
        // Set failure status
        s = IOStatus::IOError("Unexpected WAL compression error");
        s.SetDataLoss(true);
        break;
      } else if (left == 0) {
        // Nothing left to compress
        if (!compress_start) {
          break;
        }
      }
      compress_start = false;
      ptr = compressed_buffer_.get();
    }

    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length && compress_remaining == 0);
    if (begin && end) {
      type = recycle_log_files_ ? kRecyclableFullType : kFullType;
    } else if (begin) {
      type = recycle_log_files_ ? kRecyclableFirstType : kFirstType;
    } else if (end) {
      type = recycle_log_files_ ? kRecyclableLastType : kLastType;
    } else {
      type = recycle_log_files_ ? kRecyclableMiddleType : kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length, rate_limiter_priority);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && (left > 0 || compress_remaining > 0));

  if (s.ok()) {
    if (!manual_flush_) {
      s = dest_->Flush(rate_limiter_priority);
    }
  }

  return s;
}

IOStatus Writer::AddCompressionTypeRecord() {
  // Should be the first record
  assert(block_offset_ == 0);

  if (memtable_as_log_index_) {
    return IOStatus::OK();
  }

  if (compression_type_ == kNoCompression) {
    // No need to add a record
    return IOStatus::OK();
  }

  CompressionTypeRecord record(compression_type_);
  std::string encode;
  record.EncodeTo(&encode);
  IOStatus s =
      EmitPhysicalRecord(kSetCompressionType, encode.data(), encode.size());
  if (s.ok()) {
    if (!manual_flush_) {
      s = dest_->Flush();
    }
    // Initialize fields required for compression
    const size_t max_output_buffer_len =
        kBlockSize - (recycle_log_files_ ? kRecyclableHeaderSize : kHeaderSize);
    CompressionOptions opts;
    constexpr uint32_t compression_format_version = 2;
    compress_ = StreamingCompress::Create(compression_type_, opts,
                                          compression_format_version,
                                          max_output_buffer_len);
    assert(compress_ != nullptr);
    compressed_buffer_ =
        std::unique_ptr<char[]>(new char[max_output_buffer_len]);
    assert(compressed_buffer_);
  } else {
    // Disable compression if the record could not be added.
    compression_type_ = kNoCompression;
  }
  return s;
}

IOStatus Writer::MaybeAddUserDefinedTimestampSizeRecord(
    const UnorderedMap<uint32_t, size_t>& cf_to_ts_sz,
    Env::IOPriority rate_limiter_priority) {
  std::vector<std::pair<uint32_t, size_t>> ts_sz_to_record;
  for (const auto& [cf_id, ts_sz] : cf_to_ts_sz) {
    if (recorded_cf_to_ts_sz_.count(cf_id) != 0) {
      // A column family's user-defined timestamp size should not be
      // updated while DB is running.
      assert(recorded_cf_to_ts_sz_[cf_id] == ts_sz);
    } else if (ts_sz != 0) {
      ts_sz_to_record.emplace_back(cf_id, ts_sz);
      recorded_cf_to_ts_sz_.insert(std::make_pair(cf_id, ts_sz));
    }
  }
  if (ts_sz_to_record.empty()) {
    return IOStatus::OK();
  }

  UserDefinedTimestampSizeRecord record(std::move(ts_sz_to_record));
  std::string encoded;
  record.EncodeTo(&encoded);
  RecordType type = recycle_log_files_ ? kRecyclableUserDefinedTimestampSizeType
                                       : kUserDefinedTimestampSizeType;
  return EmitPhysicalRecord(type, encoded.data(), encoded.size(),
                            rate_limiter_priority);
}

bool Writer::BufferIsEmpty() { return dest_->BufferIsEmpty(); }

IOStatus Writer::AddRecordv(Slice* parts, size_t num_parts, size_t sum_len,
                            const uint32_t* part_crc32c_checksums,
                            Env::IOPriority rate_limiter_priority) {
  assert(memtable_as_log_index_);
  assert(num_parts > 0);
  assert(parts != nullptr);
  size_t len = sizeof(RawRecHeader) + sum_len;
  size_t beg = *log_offset_, end = beg + len;
  size_t mmap_size = mmap_reader_->size_;
  if (UNLIKELY(end > mmap_size)) {
    dest_->Sync(false).PermitUncheckedError(); // Sync before return error
    char msg1[192];
    sprintf(msg1, "memtable_as_log_index log::Writer::AddRecordv: "
                  "write offset %zd : %zd, len %zd, exceeds mmap size %zd "
                  "by %zd bytes",
            beg, end, len, mmap_size, end - mmap_size);
    return IOStatus::IOFenced(msg1, fname_);
  }
  TERARK_IF_DEBUG(size_t computed_sum_len = 0,);
  uint32_t payload_crc = 0;
  for (size_t i = 1; i < num_parts; ++i) {
    Slice part = parts[i];
    if (part_crc32c_checksums) {
      uint32_t crc2 = part_crc32c_checksums[i];
      if (payload_crc == 0) {
        payload_crc = crc2;
      } else {
        payload_crc = crc32c::Crc32cCombine(payload_crc, crc2, part.size());
      }
    } else {
      payload_crc = crc32c::Extend(payload_crc, part.data(), part.size());
    }
    TERARK_IF_DEBUG(computed_sum_len += part.size(),);
  }
  ROCKSDB_ASSERT_EQ(computed_sum_len, sum_len);
  RawRecHeader header;
  header.length = sum_len;
  header.rec_type = kFullType;
  header.checksum = payload_crc;
  header.header_checksum = crc32c::Value(header.hbytes, sizeof(header.hbytes));
  parts[0] = Slice((char*)&header, sizeof(RawRecHeader));
  if (LIKELY(use_writev_)) {
    IOStatus s = dest_->Appendv(parts, num_parts, len, rate_limiter_priority);
    if (LIKELY(s.ok())) {
      *log_offset_ = end;
    }
    return s;
  }
  // just for unit test
  IOStatus s = dest_->Append(Slice((char*)&header, sizeof(RawRecHeader)),
                             0 /* crc32c_checksum */, rate_limiter_priority);
  if (!s.ok()) {
    return s;
  }
  for (size_t i = 1; i < num_parts; ++i) {
    Slice part = parts[i];
    uint32_t part_crc;
    if (part_crc32c_checksums) {
      part_crc = part_crc32c_checksums[i];
    } else {
     #if defined(ROCKSDB_UNIT_TEST)
      part_crc = crc32c::Value(part.data(), part.size());
     #else
      part_crc = 0;
     #endif
    }
    s = dest_->Append(part, part_crc, rate_limiter_priority);
    if (!s.ok())
      return s;
  }
  if (UNLIKELY(!manual_flush_)) {
    s = dest_->Flush();
  }
  *log_offset_ = end;
  return s;
}

IOStatus Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n,
                                    Env::IOPriority rate_limiter_priority) {
  if (LIKELY(memtable_as_log_index_)) {
    ROCKSDB_VERIFY(!recycle_log_files_);
    ROCKSDB_VERIFY(!dest_->use_direct_io()); // must not use direct io
    size_t need = *log_offset_ + sizeof(RawRecHeader) + n;
    if (UNLIKELY(need > mmap_reader_->size_)) {
      dest_->Sync(false).PermitUncheckedError(); // Sync before return error
      char msg1[128];
      sprintf(msg1, "memtable_as_log_index log::Writer::AddRecord: "
                    "write offset %zd, size %zd, exceeds mmap size %zd",
              size_t(*log_offset_), n, mmap_reader_->size_);
      return IOStatus::IOFenced(msg1, fname_);
    }
    RawRecHeader header;
    header.checksum = crc32c::Value(ptr, n);
    header.length = n;
    header.rec_type = t;
    header.header_checksum = crc32c::Value(header.hbytes, sizeof(header.hbytes));
    if (LIKELY(use_writev_)) {
      IOStatus s = dest_->Appendv(
          {Slice((char*)&header, sizeof(RawRecHeader)), Slice(ptr, n)},
          sizeof(RawRecHeader) + n, rate_limiter_priority);
      if (LIKELY(s.ok())) {
        *log_offset_ += sizeof(RawRecHeader) + n;
      }
      return s;
    }
    IOStatus s = dest_->Append(Slice((char*)&header, sizeof(RawRecHeader)),
                               0 /* crc32c_checksum */, rate_limiter_priority);
    if (s.ok()) {
      s = dest_->Append(Slice(ptr, n), header.checksum, rate_limiter_priority);
      *log_offset_ += sizeof(RawRecHeader) + n;
    }
    return s;
  }
  assert(n <= 0xffff);  // Must fit in two bytes

  size_t header_size;
  char buf[kRecyclableHeaderSize];

  // Format the header
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  uint32_t crc = type_crc_[t];
  if (t < kRecyclableFullType || t == kSetCompressionType ||
      t == kUserDefinedTimestampSizeType) {
    // Legacy record format
    assert(block_offset_ + kHeaderSize + n <= kBlockSize);
    header_size = kHeaderSize;
  } else {
    // Recyclable record format
    assert(block_offset_ + kRecyclableHeaderSize + n <= kBlockSize);
    header_size = kRecyclableHeaderSize;

    // Only encode low 32-bits of the 64-bit log number.  This means
    // we will fail to detect an old record if we recycled a log from
    // ~4 billion logs ago, but that is effectively impossible, and
    // even if it were we'dbe far more likely to see a false positive
    // on the 32-bit CRC.
    EncodeFixed32(buf + 7, static_cast<uint32_t>(log_number_));
    crc = crc32c::Extend(crc, buf + 7, 4);
  }

  // Compute the crc of the record type and the payload.
  uint32_t payload_crc = crc32c::Value(ptr, n);
  crc = crc32c::Crc32cCombine(crc, payload_crc, n);
  crc = crc32c::Mask(crc);  // Adjust for storage
  TEST_SYNC_POINT_CALLBACK("LogWriter::EmitPhysicalRecord:BeforeEncodeChecksum",
                           &crc);
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  IOStatus s = dest_->Append(Slice(buf, header_size), 0 /* crc32c_checksum */,
                             rate_limiter_priority);
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, n), payload_crc, rate_limiter_priority);
  }
  block_offset_ += header_size + n;
  return s;
}

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE
