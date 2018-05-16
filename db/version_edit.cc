//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/event_logger.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {

void RangeEraseSet::push(const InternalKey& smallest, const InternalKey& largest,
                    bool smallest_open, bool largest_open) {
  erase.emplace_back(smallest);
  erase.emplace_back(largest);
  open.emplace_back(smallest_open);
  open.emplace_back(largest_open);
}

void MergeRangeSet(const std::vector<InternalKey>& range_set,
                   const RangeEraseSet& erase_set,
                   std::vector<InternalKey>& output,
                   const InternalKeyComparator& ic,
                   InternalIterator* iter) {
  output.clear();
  assert(!range_set.empty());
  assert(!erase_set.erase.empty());
  assert(range_set.size() % 2 == 0);
  assert(erase_set.erase.size() % 2 == 0);
  auto put_left_bound = [&](const InternalKey& left, bool include) {
    output.emplace_back();
    iter->Seek(left.Encode());
    if (iter->Valid()) {
      if (include || iter->key() != left.Encode()) {
        output.back().DecodeFrom(iter->key());
      } else {
        iter->Next();
        if (iter->Valid()) {
          output.back().DecodeFrom(iter->key());
        }
      }
    }
  };
  auto put_right_bound = [&](const InternalKey& right, bool include) {
    if (output.back().size() == 0) {
      // left bound invalid
      output.pop_back();
      return;
    }
    output.emplace_back();
    iter->SeekForPrev(right.Encode());
    if (iter->Valid()) {
      if (include || iter->key() != right.Encode()) {
        output.back().DecodeFrom(iter->key());
      } else {
        iter->Prev();
        if (iter->Valid()) {
          output.back().DecodeFrom(iter->key());
        }
      }
    }
    if (output.back().size() == 0 ||
        ic.Compare(output.end()[-2], output.back()) > 0) {
      // right bound invalid or right bound less than left bound
      output.pop_back();
      output.pop_back();
    }
  };
  size_t ri = 0, ei = 0;  // index
  size_t rc, ec;          // change
  do {
    int c;
    if (ri < range_set.size() && ei < erase_set.erase.size()) {
      c = ic.Compare(range_set[ri], erase_set.erase[ei]);
    } else {
      c = ri < range_set.size() ? -1 : 1;
    }
    rc = c <= 0;
    ec = c >= 0;
#define MergeRangeSet_CASE(a,b,c,d) ((a) | ((b) << 1) | ((c) << 2) | ((d) << 3))
    switch (MergeRangeSet_CASE(ri % 2, ei % 2, rc, ec)) {
    // out range , out erase , begin range
    case MergeRangeSet_CASE(0, 0, 1, 0):
      put_left_bound(range_set[ri], true);
      break;
    // in range , out erase , end range
    case MergeRangeSet_CASE(1, 0, 1, 0):
      put_right_bound(range_set[ri], true);
      break;
    // in range , out erase , begin erase
    case MergeRangeSet_CASE(1, 0, 0, 1):
    // in range , out erase , end range & begin erase
    case MergeRangeSet_CASE(1, 0, 1, 1):
      put_right_bound(erase_set.erase[ei], erase_set.open[ei]);
      break;
    // in range , in erase , end erase
    case MergeRangeSet_CASE(1, 1, 0, 1):
    // out range , in erase , end erase & begin range
    case MergeRangeSet_CASE(0, 1, 1, 1):
      put_left_bound(erase_set.erase[ei], erase_set.open[ei]);
      break;
    }
#undef MergeRangeSet_CASE
    ri += rc;
    ei += ec;
  } while (ri != range_set.size() || ei != erase_set.erase.size());
  assert(output.size() % 2 == 0);
}

bool PartialRemovedMetaData::InitFrom(FileMetaData* file,
                                      const RangeEraseSet& erase_set,
                                      uint8_t output_level,
                                      ColumnFamilyData* cfd,
                                      const EnvOptions& env_opt) {
  meta = file;
  partial_removed = file->partial_removed;
  compact_to_level = output_level;
  if (erase_set.erase.empty()) {
    range_set = file->range_set;
    return false;
  }
  const InternalKeyComparator& ic = cfd->ioptions()->internal_comparator;
  TableReader* table_reader = nullptr;
  auto table_cache = cfd->table_cache();
  std::unique_ptr<InternalIterator> iter(
      table_cache->NewIterator(ReadOptions(), env_opt, ic, *file, nullptr,
                               &table_reader, nullptr, false, nullptr,
                               false, -1, true));
  assert(table_reader);
  MergeRangeSet(file->range_set, erase_set, range_set, ic, iter.get());
  if (range_set.empty()) {
    partial_removed = kPartialRemovedMax;
    return true;
  }
  if (range_set.size() == file->range_set.size() &&
      std::equal(range_set.begin(), range_set.end(), file->range_set.begin(),
      [](const InternalKey& l, const InternalKey& r) {
    return l.Encode() == r.Encode();
  })) {
    return false;
  }
  size_t sst_size = file->fd.GetFileSize();
  size_t alive_size = 0;
  for (size_t i = 0; i < range_set.size(); i += 2) {
    uint64_t left_offset =
        table_reader->ApproximateOffsetOf(range_set[i].Encode());
    uint64_t right_offset =
        table_reader->ApproximateOffsetOf(range_set[i + 1].Encode());
    alive_size += right_offset - left_offset;
  }
  partial_removed =
      (uint8_t)std::min<uint64_t>(
          kPartialRemovedMax - 1, std::max<uint64_t>(
                  1, (std::max(alive_size, sst_size) - alive_size) *
                          kPartialRemovedMax / sst_size));
  return true;
}

FileMetaData PartialRemovedMetaData::Get() {
  assert(partial_removed < kPartialRemovedMax);
  FileMetaData f;
  f.fd = FileDescriptor(meta->fd.GetNumber(), meta->fd.GetPathId(),
                        meta->fd.GetFileSize());
  f.range_set = std::move(range_set);
  f.smallest_seqno = meta->smallest_seqno;
  f.largest_seqno = meta->largest_seqno;
  f.marked_for_compaction = meta->marked_for_compaction;
  f.partial_removed = partial_removed;
  f.compact_to_level = compact_to_level;
  return f;
}

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9,

  // these are new formats divergent from open source leveldb
  kNewFile2 = 100,
  kNewFile3 = 102,
  kNewFile4 = 103,      // 4th (the latest) format version of adding files
  kColumnFamily = 200,  // specify column family for version edit
  kColumnFamilyAdd = 201,
  kColumnFamilyDrop = 202,
  kMaxColumnFamily = 203,
};

enum CustomTag {
  kTerminate = 1,  // The end of customized fields
  kNeedCompaction = 2,
  kPartialRemoved = 3,
  kCompactToLevel = 4,
  kExpandRangeSet = 5,
  kPathId = 65,
};
// If this bit for the custom tag is set, opening DB should fail if
// we don't know this field.
uint32_t kCustomTagNonSafeIgnoreMask = 1 << 6;

uint64_t PackFileNumberAndPathId(uint64_t number, uint64_t path_id) {
  assert(number <= kFileNumberMask);
  return number | (path_id * (kFileNumberMask + 1));
}

void VersionEdit::Clear() {
  comparator_.clear();
  max_level_ = 0;
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  max_column_family_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  has_max_column_family_ = false;
  deleted_files_.clear();
  new_files_.clear();
  column_family_ = 0;
  is_column_family_add_ = 0;
  is_column_family_drop_ = 0;
  column_family_name_.clear();
}

bool VersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32Varint64(dst, kLogNumber, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32Varint64(dst, kPrevLogNumber, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32Varint64(dst, kNextFileNumber, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32Varint64(dst, kLastSequence, last_sequence_);
  }
  if (has_max_column_family_) {
    PutVarint32Varint32(dst, kMaxColumnFamily, max_column_family_);
  }

  for (const auto& deleted : deleted_files_) {
    PutVarint32Varint32Varint64(dst, kDeletedFile, deleted.first /* level */,
                                deleted.second /* file number */);
  }

  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    if (!f.smallest().Valid() || !f.largest().Valid()) {
      return false;
    }
    bool has_customized_fields = false;
    if (f.marked_for_compaction || f.partial_removed ||
        f.compact_to_level) {
      PutVarint32(dst, kNewFile4);
      has_customized_fields = true;
    } else if (f.fd.GetPathId() == 0) {
      // Use older format to make sure user can roll back the build if they
      // don't config multiple DB paths.
      PutVarint32(dst, kNewFile2);
    } else {
      PutVarint32(dst, kNewFile3);
    }
    PutVarint32Varint64(dst, new_files_[i].first /* level */, f.fd.GetNumber());
    if (f.fd.GetPathId() != 0 && !has_customized_fields) {
      // kNewFile3
      PutVarint32(dst, f.fd.GetPathId());
    }
    PutVarint64(dst, f.fd.GetFileSize());
    PutLengthPrefixedSlice(dst, f.smallest().Encode());
    PutLengthPrefixedSlice(dst, f.largest().Encode());
    PutVarint64Varint64(dst, f.smallest_seqno, f.largest_seqno);
    if (has_customized_fields) {
      // Customized fields' format:
      // +-----------------------------+
      // | 1st field's tag (varint32)  |
      // +-----------------------------+
      // | 1st field's size (varint32) |
      // +-----------------------------+
      // |    bytes for 1st field      |
      // |  (based on size decoded)    |
      // +-----------------------------+
      // |                             |
      // |          ......             |
      // |                             |
      // +-----------------------------+
      // | last field's size (varint32)|
      // +-----------------------------+
      // |    bytes for last field     |
      // |  (based on size decoded)    |
      // +-----------------------------+
      // | terminating tag (varint32)  |
      // +-----------------------------+
      //
      // Customized encoding for fields:
      //   tag kPathId: 1 byte as path_id
      //   tag kNeedCompaction:
      //        now only can take one char value 1 indicating need-compaction
      //
      if (f.fd.GetPathId() != 0) {
        PutVarint32(dst, CustomTag::kPathId);
        char p = static_cast<char>(f.fd.GetPathId());
        PutLengthPrefixedSlice(dst, Slice(&p, 1));
      }
      if (f.marked_for_compaction) {
        PutVarint32(dst, CustomTag::kNeedCompaction);
        char p = static_cast<char>(1);
        PutLengthPrefixedSlice(dst, Slice(&p, 1));
      }
      if (f.partial_removed) {
        PutVarint32(dst, CustomTag::kPartialRemoved);
        char p = static_cast<char>(f.partial_removed);
        PutLengthPrefixedSlice(dst, Slice(&p, 1));
      }
      if (f.compact_to_level) {
        PutVarint32(dst, CustomTag::kCompactToLevel);
        char p = static_cast<char>(f.compact_to_level);
        PutLengthPrefixedSlice(dst, Slice(&p, 1));
      }
      for (size_t j = 1; j < f.range_set.size() - 1; ++j) {
        PutVarint32(dst, CustomTag::kExpandRangeSet);
        PutLengthPrefixedSlice(dst, f.range_set[j].Encode());
      }
      TEST_SYNC_POINT_CALLBACK("VersionEdit::EncodeTo:NewFile4:CustomizeFields",
                               dst);

      PutVarint32(dst, CustomTag::kTerminate);
    }
  }

  // 0 is default and does not need to be explicitly written
  if (column_family_ != 0) {
    PutVarint32Varint32(dst, kColumnFamily, column_family_);
  }

  if (is_column_family_add_) {
    PutVarint32(dst, kColumnFamilyAdd);
    PutLengthPrefixedSlice(dst, Slice(column_family_name_));
  }

  if (is_column_family_drop_) {
    PutVarint32(dst, kColumnFamilyDrop);
  }
  return true;
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return dst->Valid();
  } else {
    return false;
  }
}

bool VersionEdit::GetLevel(Slice* input, int* level, const char** msg) {
  uint32_t v;
  if (GetVarint32(input, &v)) {
    *level = v;
    if (max_level_ < *level) {
      max_level_ = *level;
    }
    return true;
  } else {
    return false;
  }
}

const char* VersionEdit::DecodeNewFile4From(Slice* input) {
  const char* msg = nullptr;
  int level;
  FileMetaData f;
  uint64_t number;
  uint32_t path_id = 0;
  uint64_t file_size;
  InternalKey largest;
  f.range_set.resize(1);
  if (GetLevel(input, &level, &msg) && GetVarint64(input, &number) &&
      GetVarint64(input, &file_size) &&
      GetInternalKey(input, &f.smallest()) &&
      GetInternalKey(input, &largest) &&
      GetVarint64(input, &f.smallest_seqno) &&
      GetVarint64(input, &f.largest_seqno)) {
    // See comments in VersionEdit::EncodeTo() for format of customized fields
    while (true) {
      uint32_t custom_tag;
      Slice field;
      if (!GetVarint32(input, &custom_tag)) {
        return "new-file4 custom field";
      }
      if (custom_tag == kTerminate) {
        break;
      }
      if (!GetLengthPrefixedSlice(input, &field)) {
        return "new-file4 custom field lenth prefixed slice error";
      }
      switch (custom_tag) {
        case kPathId:
          if (field.size() != 1) {
            return "path_id field wrong size";
          }
          path_id = field[0];
          if (path_id > 3) {
            return "path_id wrong vaue";
          }
          break;
        case kNeedCompaction:
          if (field.size() != 1) {
            return "need_compaction field wrong size";
          }
          f.marked_for_compaction = (field[0] == 1);
          break;
        case kPartialRemoved:
          if (field.size() != 1) {
            return "partial_removed field wrong size";
          }
          f.partial_removed = static_cast<uint8_t>(field[0]);
          break;
        case kCompactToLevel:
          if (field.size() != 1) {
            return "compact_to_level field wrong size";
          }
          f.compact_to_level = static_cast<uint8_t>(field[0]);
          break;
        case kExpandRangeSet:
          f.range_set.emplace_back();
          f.range_set.back().DecodeFrom(field);
          if (!f.range_set.back().Valid()) {
            return "range_set field invalid internal key";
          }
        default:
          if ((custom_tag & kCustomTagNonSafeIgnoreMask) != 0) {
            // Should not proceed if cannot understand it
            return "new-file4 custom field not supported";
          }
          break;
      }
    }
  } else {
    return "new-file4 entry";
  }
  f.range_set.emplace_back(std::move(largest));
  if (f.range_set.size() % 2 != 0) {
    return "range_set field wrong element count";
  }
  f.fd = FileDescriptor(number, path_id, file_size);
  new_files_.push_back(std::make_pair(level, f));
  return nullptr;
}

Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear();
  Slice input = src;
  const char* msg = nullptr;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  FileMetaData f;
  Slice str;
  InternalKey key;

  while (msg == nullptr && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kMaxColumnFamily:
        if (GetVarint32(&input, &max_column_family_)) {
          has_max_column_family_ = true;
        } else {
          msg = "max column family";
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level, &msg) &&
            GetInternalKey(&input, &key)) {
          // we don't use compact pointers anymore,
          // but we should not fail if they are still
          // in manifest
        } else {
          if (!msg) {
            msg = "compaction pointer";
          }
        }
        break;

      case kDeletedFile: {
        uint64_t number;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));
        } else {
          if (!msg) {
            msg = "deleted file";
          }
        }
        break;
      }

      case kNewFile: {
        uint64_t number;
        uint64_t file_size;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number) &&
            GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest()) &&
            GetInternalKey(&input, &f.largest())) {
          f.fd = FileDescriptor(number, 0, file_size);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file entry";
          }
        }
        break;
      }
      case kNewFile2: {
        uint64_t number;
        uint64_t file_size;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number) &&
            GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest()) &&
            GetInternalKey(&input, &f.largest()) &&
            GetVarint64(&input, &f.smallest_seqno) &&
            GetVarint64(&input, &f.largest_seqno)) {
          f.fd = FileDescriptor(number, 0, file_size);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file2 entry";
          }
        }
        break;
      }

      case kNewFile3: {
        uint64_t number;
        uint32_t path_id;
        uint64_t file_size;
        if (GetLevel(&input, &level, &msg) && GetVarint64(&input, &number) &&
            GetVarint32(&input, &path_id) && GetVarint64(&input, &file_size) &&
            GetInternalKey(&input, &f.smallest()) &&
            GetInternalKey(&input, &f.largest()) &&
            GetVarint64(&input, &f.smallest_seqno) &&
            GetVarint64(&input, &f.largest_seqno)) {
          f.fd = FileDescriptor(number, path_id, file_size);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          if (!msg) {
            msg = "new-file3 entry";
          }
        }
        break;
      }

      case kNewFile4: {
        msg = DecodeNewFile4From(&input);
        break;
      }

      case kColumnFamily:
        if (!GetVarint32(&input, &column_family_)) {
          if (!msg) {
            msg = "set column family id";
          }
        }
        break;

      case kColumnFamilyAdd:
        if (GetLengthPrefixedSlice(&input, &str)) {
          is_column_family_add_ = true;
          column_family_name_ = str.ToString();
        } else {
          if (!msg) {
            msg = "column family add";
          }
        }
        break;

      case kColumnFamilyDrop:
        is_column_family_drop_ = true;
        break;

      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != nullptr) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString(bool hex_key) const {
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFileNumber: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
       iter != deleted_files_.end();
       ++iter) {
    r.append("\n  DeleteFile: ");
    AppendNumberTo(&r, iter->first);
    r.append(" ");
    AppendNumberTo(&r, iter->second);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.fd.GetNumber());
    r.append(" ");
    AppendNumberTo(&r, f.fd.GetFileSize());
    for (size_t j = 0; j < f.range_set.size(); j += 2) {
      r.append(" ");
      r.append(f.range_set[j].DebugString(hex_key));
      r.append(" .. ");
      r.append(f.range_set[j + 1].DebugString(hex_key));
    }
  }
  r.append("\n  ColumnFamily: ");
  AppendNumberTo(&r, column_family_);
  if (is_column_family_add_) {
    r.append("\n  ColumnFamilyAdd: ");
    r.append(column_family_name_);
  }
  if (is_column_family_drop_) {
    r.append("\n  ColumnFamilyDrop");
  }
  if (has_max_column_family_) {
    r.append("\n  MaxColumnFamily: ");
    AppendNumberTo(&r, max_column_family_);
  }
  r.append("\n}\n");
  return r;
}

std::string VersionEdit::DebugJSON(int edit_num, bool hex_key) const {
  JSONWriter jw;
  jw << "EditNumber" << edit_num;

  if (has_comparator_) {
    jw << "Comparator" << comparator_;
  }
  if (has_log_number_) {
    jw << "LogNumber" << log_number_;
  }
  if (has_prev_log_number_) {
    jw << "PrevLogNumber" << prev_log_number_;
  }
  if (has_next_file_number_) {
    jw << "NextFileNumber" << next_file_number_;
  }
  if (has_last_sequence_) {
    jw << "LastSeq" << last_sequence_;
  }

  if (!deleted_files_.empty()) {
    jw << "DeletedFiles";
    jw.StartArray();

    for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
         iter != deleted_files_.end();
         ++iter) {
      jw.StartArrayedObject();
      jw << "Level" << iter->first;
      jw << "FileNumber" << iter->second;
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  if (!new_files_.empty()) {
    jw << "AddedFiles";
    jw.StartArray();

    for (size_t i = 0; i < new_files_.size(); i++) {
      jw.StartArrayedObject();
      jw << "Level" << new_files_[i].first;
      const FileMetaData& f = new_files_[i].second;
      jw << "FileNumber" << f.fd.GetNumber();
      jw << "FileSize" << f.fd.GetFileSize();
      for (size_t j = 0; j < f.range_set.size(); j += 2) {
        jw << "SmallestIKey" << f.range_set[j].DebugString(hex_key);
        jw << "LargestIKey" << f.range_set[j + 1].DebugString(hex_key);
      }
      jw.EndArrayedObject();
    }

    jw.EndArray();
  }

  jw << "ColumnFamily" << column_family_;

  if (is_column_family_add_) {
    jw << "ColumnFamilyAdd" << column_family_name_;
  }
  if (is_column_family_drop_) {
    jw << "ColumnFamilyDrop" << column_family_name_;
  }
  if (has_max_column_family_) {
    jw << "MaxColumnFamily" << max_column_family_;
  }

  jw.EndObject();

  return jw.Get();
}

bool VersionEdit::is_has_comparator() {
  return has_comparator_;
}
bool VersionEdit::is_has_log_number() {
  return has_log_number_;
}
bool VersionEdit::is_has_prev_log_number() {
  return has_prev_log_number_;
}
bool VersionEdit::is_has_next_file_number() {
  return has_next_file_number_;
}
bool VersionEdit::is_has_last_sequence() {
  return has_last_sequence_;
}
bool VersionEdit::is_has_max_column_family() {
  return has_max_column_family_;
}
bool VersionEdit::is_column_family_drop() {
  return is_column_family_drop_;
}
bool VersionEdit::is_column_family_add() {
  return is_column_family_add_;
}
std::string VersionEdit::get_comparator() {
  return comparator_;
}
uint64_t VersionEdit::get_log_number() {
  return log_number_;
}
uint64_t VersionEdit::get_prev_log_number() {
  return prev_log_number_;
}
uint64_t VersionEdit::get_next_file_number() {
  return next_file_number_;
}
SequenceNumber VersionEdit::get_last_sequence() {
  return last_sequence_;
}
uint32_t VersionEdit::get_max_column_family() {
  return max_column_family_;
}
std::string VersionEdit::get_column_family_name() {
  return column_family_name_;
}
uint32_t VersionEdit::get_column_family() {
  return column_family_;
}
VersionEdit::DeletedFileSet VersionEdit::get_deleted_files() {
  return deleted_files_;
}
std::vector<std::pair<int, FileMetaData>> VersionEdit::get_new_files() {
  return new_files_;
}

}  // namespace rocksdb
