//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_universal.h"
#ifndef ROCKSDB_LITE

#include <cinttypes>
#include <limits>
#include <queue>
#include <string>
#include <utility>
#include "db/column_family.h"
#include "file/filename.h"
#include "logging/log_buffer.h"
#include "monitoring/statistics.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
// A helper class that form universal compactions. The class is used by
// UniversalCompactionPicker::PickCompaction().
// The usage is to create the class, and get the compaction object by calling
// PickCompaction().
class UniversalCompactionBuilder {
 public:
  UniversalCompactionBuilder(const ImmutableCFOptions& ioptions,
                             const InternalKeyComparator* icmp,
                             const std::string& cf_name,
                             const MutableCFOptions& mutable_cf_options,
                             VersionStorageInfo* vstorage,
                             UniversalCompactionPicker* picker,
                             LogBuffer* log_buffer)
      : ioptions_(ioptions),
        icmp_(icmp),
        cf_name_(cf_name),
        mutable_cf_options_(mutable_cf_options),
        vstorage_(vstorage),
        picker_(picker),
        log_buffer_(log_buffer) {}

  // Form and return the compaction object. The caller owns return object.
  Compaction* PickCompaction();

 private:
  struct SortedRun {
    SortedRun(int _level, FileMetaData* _file, uint64_t _size,
              uint64_t _compensated_file_size, bool _being_compacted)
        : level(_level),
          file(_file),
          size(_size),
          compensated_file_size(_compensated_file_size),
          being_compacted(_being_compacted) {
      assert(compensated_file_size > 0);
      assert(level != 0 || file != nullptr);
    }

    void Dump(char* out_buf, size_t out_buf_size,
              bool print_path = false) const;

    // sorted_run_count is added into the string to print
    void DumpSizeInfo(char* out_buf, size_t out_buf_size,
                      size_t sorted_run_count) const;

    int level;
    // `file` Will be null for level > 0. For level = 0, the sorted run is
    // for this file.
    FileMetaData* file;
    // For level > 0, `size` and `compensated_file_size` are sum of sizes all
    // files in the level. `being_compacted` should be the same for all files
    // in a non-zero level. Use the value here.
    uint64_t size;
    uint64_t compensated_file_size;
    bool being_compacted;
  };

  // Pick Universal compaction to limit read amplification
  Compaction* PickCompactionToReduceSortedRuns(
      unsigned int ratio, unsigned int max_number_of_files_to_compact);

  // Pick Universal compaction to limit space amplification.
  Compaction* PickCompactionToReduceSizeAmp();

  Compaction* PickDeleteTriggeredCompaction();

  // Form a compaction from the sorted run indicated by start_index to the
  // oldest sorted run.
  // The caller is responsible for making sure that those files are not in
  // compaction.
  Compaction* PickCompactionToOldest(size_t start_index,
                                     CompactionReason compaction_reason);

  // Try to pick periodic compaction. The caller should only call it
  // if there is at least one file marked for periodic compaction.
  // null will be returned if no such a compaction can be formed
  // because some files are being compacted.
  Compaction* PickPeriodicCompaction();

  // Used in universal compaction when the enabled_trivial_move
  // option is set. Checks whether there are any overlapping files
  // in the input. Returns true if the input files are non
  // overlapping.
  bool IsInputFilesNonOverlapping(Compaction* c);

  const ImmutableCFOptions& ioptions_;
  const InternalKeyComparator* icmp_;
  double score_;
  std::vector<SortedRun> sorted_runs_;
  const std::string& cf_name_;
  const MutableCFOptions& mutable_cf_options_;
  VersionStorageInfo* vstorage_;
  UniversalCompactionPicker* picker_;
  LogBuffer* log_buffer_;

  static std::vector<SortedRun> CalculateSortedRuns(
      const VersionStorageInfo& vstorage);

  // Pick a path ID to place a newly generated file, with its estimated file
  // size.
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            uint64_t file_size);
};

// Used in universal compaction when trivial move is enabled.
// This structure is used for the construction of min heap
// that contains the file meta data, the level of the file
// and the index of the file in that level

struct InputFileInfo {
  InputFileInfo() : f(nullptr), level(0), index(0) {}

  FileMetaData* f;
  size_t level;
  size_t index;
};

// Used in universal compaction when trivial move is enabled.
// This comparator is used for the construction of min heap
// based on the smallest key of the file.
struct SmallestKeyHeapComparator {
  explicit SmallestKeyHeapComparator(const Comparator* ucmp) { ucmp_ = ucmp; }

  bool operator()(InputFileInfo i1, InputFileInfo i2) const {
    return (ucmp_->Compare(i1.f->smallest().user_key(),
                           i2.f->smallest().user_key()) > 0);
  }

 private:
  const Comparator* ucmp_;
};

typedef std::priority_queue<InputFileInfo, std::vector<InputFileInfo>,
                            SmallestKeyHeapComparator>
    SmallestKeyHeap;

// This function creates the heap that is used to find if the files are
// overlapping during universal compaction when the allow_trivial_move
// is set.
SmallestKeyHeap create_level_heap(Compaction* c, const Comparator* ucmp) {
  SmallestKeyHeap smallest_key_priority_q =
      SmallestKeyHeap(SmallestKeyHeapComparator(ucmp));

  InputFileInfo input_file;

  for (size_t l = 0; l < c->num_input_levels(); l++) {
    if (c->num_input_files(l) != 0) {
      if (l == 0 && c->start_level() == 0) {
        for (size_t i = 0; i < c->num_input_files(0); i++) {
          input_file.f = c->input(0, i);
          input_file.level = 0;
          input_file.index = i;
          smallest_key_priority_q.push(std::move(input_file));
        }
      } else {
        input_file.f = c->input(l, 0);
        input_file.level = l;
        input_file.index = 0;
        smallest_key_priority_q.push(std::move(input_file));
      }
    }
  }
  return smallest_key_priority_q;
}

#ifndef NDEBUG
// smallest_seqno and largest_seqno are set iff. `files` is not empty.
void GetSmallestLargestSeqno(const std::vector<FileMetaData*>& files,
                             SequenceNumber* smallest_seqno,
                             SequenceNumber* largest_seqno) {
  bool is_first = true;
  for (FileMetaData* f : files) {
    assert(f->fd.smallest_seqno <= f->fd.largest_seqno);
    if (is_first) {
      is_first = false;
      *smallest_seqno = f->fd.smallest_seqno;
      *largest_seqno = f->fd.largest_seqno;
    } else {
      if (f->fd.smallest_seqno < *smallest_seqno) {
        *smallest_seqno = f->fd.smallest_seqno;
      }
      if (f->fd.largest_seqno > *largest_seqno) {
        *largest_seqno = f->fd.largest_seqno;
      }
    }
  }
}
#endif
}  // namespace

// Algorithm that checks to see if there are any overlapping
// files in the input
bool UniversalCompactionBuilder::IsInputFilesNonOverlapping(Compaction* c) {
  auto comparator = icmp_->user_comparator();
  int first_iter = 1;

  InputFileInfo prev, curr, next;

  SmallestKeyHeap smallest_key_priority_q =
      create_level_heap(c, icmp_->user_comparator());

  while (!smallest_key_priority_q.empty()) {
    curr = smallest_key_priority_q.top();
    smallest_key_priority_q.pop();

    if (first_iter) {
      prev = curr;
      first_iter = 0;
    } else {
      if (comparator->Compare(prev.f->largest().user_key(),
                              curr.f->smallest().user_key()) >= 0) {
        // found overlapping files, return false
        return false;
      }
      assert(comparator->Compare(curr.f->largest().user_key(),
                                 prev.f->largest().user_key()) > 0);
      prev = curr;
    }

    next.f = nullptr;

    if (c->level(curr.level) != 0 &&
        curr.index < c->num_input_files(curr.level) - 1) {
      next.f = c->input(curr.level, curr.index + 1);
      next.level = curr.level;
      next.index = curr.index + 1;
    }

    if (next.f) {
      smallest_key_priority_q.push(std::move(next));
    }
  }
  return true;
}

bool UniversalCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  const int kLevel0 = 0;
  if (vstorage->CompactionScore(kLevel0) >= 1) {
    return true;
  }
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  return false;
}

Compaction* UniversalCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, LogBuffer* log_buffer,
    SequenceNumber /* earliest_memtable_seqno */) {
  UniversalCompactionBuilder builder(ioptions_, icmp_, cf_name,
                                     mutable_cf_options, vstorage, this,
                                     log_buffer);
  return builder.PickCompaction();
}

void UniversalCompactionBuilder::SortedRun::Dump(char* out_buf,
                                                 size_t out_buf_size,
                                                 bool print_path) const {
  if (level == 0) {
    assert(file != nullptr);
    if (file->fd.GetPathId() == 0 || !print_path) {
      snprintf(out_buf, out_buf_size, "file %" PRIu64, file->fd.GetNumber());
    } else {
      snprintf(out_buf, out_buf_size, "file %" PRIu64
                                      "(path "
                                      "%" PRIu32 ")",
               file->fd.GetNumber(), file->fd.GetPathId());
    }
  } else {
    snprintf(out_buf, out_buf_size, "level %d", level);
  }
}

void UniversalCompactionBuilder::SortedRun::DumpSizeInfo(
    char* out_buf, size_t out_buf_size, size_t sorted_run_count) const {
  if (level == 0) {
    assert(file != nullptr);
    snprintf(out_buf, out_buf_size,
             "file %" PRIu64 "[%" ROCKSDB_PRIszt
             "] "
             "with size %" PRIu64 " (compensated size %" PRIu64 ")",
             file->fd.GetNumber(), sorted_run_count, file->fd.GetFileSize(),
             file->compensated_file_size);
  } else {
    snprintf(out_buf, out_buf_size,
             "level %d[%" ROCKSDB_PRIszt
             "] "
             "with size %" PRIu64 " (compensated size %" PRIu64 ")",
             level, sorted_run_count, size, compensated_file_size);
  }
}

std::vector<UniversalCompactionBuilder::SortedRun>
UniversalCompactionBuilder::CalculateSortedRuns(
    const VersionStorageInfo& vstorage) {
  std::vector<UniversalCompactionBuilder::SortedRun> ret;
  for (FileMetaData* f : vstorage.LevelFiles(0)) {
    ret.emplace_back(0, f, f->fd.GetFileSize(), f->compensated_file_size,
                     f->being_compacted);
  }
  for (int level = 1; level < vstorage.num_levels(); level++) {
    uint64_t total_compensated_size = 0U;
    uint64_t total_size = 0U;
    bool being_compacted = false;
    for (FileMetaData* f : vstorage.LevelFiles(level)) {
      total_compensated_size += f->compensated_file_size;
      total_size += f->fd.GetFileSize();
      // Size amp, read amp and periodic compactions always include all files
      // for a non-zero level. However, a delete triggered compaction and
      // a trivial move might pick a subset of files in a sorted run. So
      // always check all files in a sorted run and mark the entire run as
      // being compacted if one or more files are being compacted
      if (f->being_compacted) {
        being_compacted = f->being_compacted;
      }
    }
    if (total_compensated_size > 0) {
      ret.emplace_back(level, nullptr, total_size, total_compensated_size,
                       being_compacted);
    }
  }
  return ret;
}

// Universal style of compaction. Pick files that are contiguous in
// time-range to compact.
Compaction* UniversalCompactionBuilder::PickCompaction() {
  const int kLevel0 = 0;
  score_ = vstorage_->CompactionScore(kLevel0);
  sorted_runs_ = CalculateSortedRuns(*vstorage_);

  if (sorted_runs_.size() == 0 ||
      (vstorage_->FilesMarkedForPeriodicCompaction().empty() &&
       vstorage_->FilesMarkedForCompaction().empty() &&
       sorted_runs_.size() < (unsigned int)mutable_cf_options_
                                 .level0_file_num_compaction_trigger)) {
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: nothing to do\n",
                     cf_name_.c_str());
    TEST_SYNC_POINT_CALLBACK(
        "UniversalCompactionBuilder::PickCompaction:Return", nullptr);
    return nullptr;
  }
  VersionStorageInfo::LevelSummaryStorage tmp;
  ROCKS_LOG_BUFFER_MAX_SZ(
      log_buffer_, 3072,
      "[%s] Universal: sorted runs: %" ROCKSDB_PRIszt " files: %s\n",
      cf_name_.c_str(), sorted_runs_.size(), vstorage_->LevelSummary(&tmp));

  Compaction* c = nullptr;
  // Periodic compaction has higher priority than other type of compaction
  // because it's a hard requirement.
  if (!vstorage_->FilesMarkedForPeriodicCompaction().empty()) {
    // Always need to do a full compaction for periodic compaction.
    c = PickPeriodicCompaction();
  }

  // Check for size amplification.
  if (c == nullptr &&
      sorted_runs_.size() >=
          static_cast<size_t>(
              mutable_cf_options_.level0_file_num_compaction_trigger)) {
    if ((c = PickCompactionToReduceSizeAmp()) != nullptr) {
      ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: compacting for size amp\n",
                       cf_name_.c_str());
    } else {
      // Size amplification is within limits. Try reducing read
      // amplification while maintaining file size ratios.
      unsigned int ratio =
          mutable_cf_options_.compaction_options_universal.size_ratio;

      if ((c = PickCompactionToReduceSortedRuns(ratio, UINT_MAX)) != nullptr) {
        ROCKS_LOG_BUFFER(log_buffer_,
                         "[%s] Universal: compacting for size ratio\n",
                         cf_name_.c_str());
      } else {
        // Size amplification and file size ratios are within configured limits.
        // If max read amplification is exceeding configured limits, then force
        // compaction without looking at filesize ratios and try to reduce
        // the number of files to fewer than level0_file_num_compaction_trigger.
        // This is guaranteed by NeedsCompaction()
        assert(sorted_runs_.size() >=
               static_cast<size_t>(
                   mutable_cf_options_.level0_file_num_compaction_trigger));
        // Get the total number of sorted runs that are not being compacted
        int num_sr_not_compacted = 0;
        for (size_t i = 0; i < sorted_runs_.size(); i++) {
          if (sorted_runs_[i].being_compacted == false) {
            num_sr_not_compacted++;
          }
        }

        // The number of sorted runs that are not being compacted is greater
        // than the maximum allowed number of sorted runs
        if (num_sr_not_compacted >
            mutable_cf_options_.level0_file_num_compaction_trigger) {
          unsigned int num_files =
              num_sr_not_compacted -
              mutable_cf_options_.level0_file_num_compaction_trigger + 1;
          if ((c = PickCompactionToReduceSortedRuns(UINT_MAX, num_files)) !=
              nullptr) {
            ROCKS_LOG_BUFFER(log_buffer_,
                             "[%s] Universal: compacting for file num -- %u\n",
                             cf_name_.c_str(), num_files);
          }
        }
      }
    }
  }

  if (c == nullptr) {
    if ((c = PickDeleteTriggeredCompaction()) != nullptr) {
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Universal: delete triggered compaction\n",
                       cf_name_.c_str());
    }
  }

  if (c == nullptr) {
    TEST_SYNC_POINT_CALLBACK(
        "UniversalCompactionBuilder::PickCompaction:Return", nullptr);
    return nullptr;
  }

  if (mutable_cf_options_.compaction_options_universal.allow_trivial_move ==
          true &&
      c->compaction_reason() != CompactionReason::kPeriodicCompaction) {
    c->set_is_trivial_move(IsInputFilesNonOverlapping(c));
  }

// validate that all the chosen files of L0 are non overlapping in time
#ifndef NDEBUG
  SequenceNumber prev_smallest_seqno = 0U;
  bool is_first = true;

  size_t level_index = 0U;
  if (c->start_level() == 0) {
    for (auto f : *c->inputs(0)) {
      assert(f->fd.smallest_seqno <= f->fd.largest_seqno);
      if (is_first) {
        is_first = false;
      }
      prev_smallest_seqno = f->fd.smallest_seqno;
    }
    level_index = 1U;
  }
  for (; level_index < c->num_input_levels(); level_index++) {
    if (c->num_input_files(level_index) != 0) {
      SequenceNumber smallest_seqno = 0U;
      SequenceNumber largest_seqno = 0U;
      GetSmallestLargestSeqno(*(c->inputs(level_index)), &smallest_seqno,
                              &largest_seqno);
      if (is_first) {
        is_first = false;
      } else if (prev_smallest_seqno > 0) {
        // A level is considered as the bottommost level if there are
        // no files in higher levels or if files in higher levels do
        // not overlap with the files being compacted. Sequence numbers
        // of files in bottommost level can be set to 0 to help
        // compression. As a result, the following assert may not hold
        // if the prev_smallest_seqno is 0.
        assert(prev_smallest_seqno > largest_seqno);
      }
      prev_smallest_seqno = smallest_seqno;
    }
  }
#endif
  // update statistics
  RecordInHistogram(ioptions_.statistics, NUM_FILES_IN_SINGLE_COMPACTION,
                    c->inputs(0)->size());

  picker_->RegisterCompaction(c);
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);

  TEST_SYNC_POINT_CALLBACK("UniversalCompactionBuilder::PickCompaction:Return",
                           c);
  return c;
}

uint32_t UniversalCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, uint64_t file_size) {
  // Two conditions need to be satisfied:
  // (1) the target path needs to be able to hold the file's size
  // (2) Total size left in this and previous paths need to be not
  //     smaller than expected future file size before this new file is
  //     compacted, which is estimated based on size_ratio.
  // For example, if now we are compacting files of size (1, 1, 2, 4, 8),
  // we will make sure the target file, probably with size of 16, will be
  // placed in a path so that eventually when new files are generated and
  // compacted to (1, 1, 2, 4, 8, 16), all those files can be stored in or
  // before the path we chose.
  //
  // TODO(sdong): now the case of multiple column families is not
  // considered in this algorithm. So the target size can be violated in
  // that case. We need to improve it.

/*
  uint64_t accumulated_size = 0;
  uint64_t future_size =
      file_size *
      (100 - mutable_cf_options.compaction_options_universal.size_ratio) / 100;
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());
  for (; p < ioptions.cf_paths.size() - 1; p++) {
    uint64_t target_size = ioptions.cf_paths[p].target_size;
    if (target_size > file_size &&
        accumulated_size + (target_size - file_size) > future_size) {
      return p;
    }
    accumulated_size += target_size;
  }
  return p;
*/
}

Compaction* UniversalCompactionPicker::PickCompactionConitnue(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, LogBuffer* log_buffer,
    int continue_output_level) {
  int start_level = 0;
  int output_level = 0;
  // found some sst has compact_to_level
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    for (auto f : vstorage->LevelFiles(level)) {
      if (f->compact_to_level) {
        output_level = f->compact_to_level;
        assert(output_level < vstorage->num_levels());
        break;
      }
    }
    if (output_level != 0) {
      if (continue_output_level != 0 && output_level != continue_output_level) {
        // ignore non continue_output_level
        level = output_level;
        output_level = 0;
        continue;
      }
      for (int i = level; i <= output_level; ++i) {
        if (i == 0) {
          for (auto f : vstorage->LevelFiles(0)) {
            if (f->compact_to_level == output_level && f->being_compacted) {
              level = output_level;
              output_level = 0;
              break;
            }
          }
          if (output_level == 0) {
            break;
          }
        } else if (AreFilesInCompaction(vstorage->LevelFiles(i))) {
          level = output_level;
          output_level = 0;
          break;
        }
      }
    }
    if (output_level != 0) {
      start_level = level;
      break;
    }
  }
  if (output_level == 0) {
    return nullptr;
  }
  std::vector<FileMetaData*> level0_files;
  // fill inputs
  std::vector<CompactionInputFiles> inputs;
  std::vector<CompactionInputFilesRange> input_range;
  for (int level = start_level; level <= output_level; ++level) {
    auto& files = vstorage->LevelFiles(level);
    CompactionInputFiles input_level;
    input_level.level = level;
    if (level == 0) {
      for (auto f : files) {
        if (f->compact_to_level == output_level) {
          level0_files.emplace_back(f);
        }
      }
      input_level.files = level0_files;
    } else {
      input_level.files = files;
    }
    if (!input_level.files.empty()) {
      inputs.emplace_back(std::move(input_level));
    }
  }
  std::vector<FileMetaData*> overlap_files;
  // return true if this sst need compact
  auto need_compact_file = [&, this](FileMetaData* meta) {
    if (meta->compact_to_level == output_level) {
      return true;
    }
    Slice smallest = meta->smallest().user_key();
    Slice largest = meta->largest().user_key();
    auto ucmp = icmp_->user_comparator();
    const std::vector<FileMetaData*>* files_ptr;
    for (auto level = start_level; level < output_level; ++level) {
      if (level == 0) {
        files_ptr = &level0_files;
      } else {
        vstorage->GetOverlappingInputsRangeBinarySearch(
                      level, smallest, largest, &overlap_files, -1, nullptr);
        files_ptr = &overlap_files;
      }
      for (auto f : *files_ptr) {
        for (size_t i = 0; i < f->range_set.size(); i += 2) {
          if (ucmp->Compare(largest, f->range_set[i].user_key()) >= 0 &&
              ucmp->Compare(smallest, f->range_set[i + 1].user_key()) <= 0) {
            return true;
          }
        }
      }
    }
    return false;
  };
  // return true if exists data between these tow sst
  auto need_compact_between = [&, this](FileMetaData* left, FileMetaData* right) {
    const InternalKey* smallest = left ? &left->largest() : nullptr;
    const InternalKey* largest = right ? &right->smallest() : nullptr;
    auto ucmp = icmp_->user_comparator();
    const std::vector<FileMetaData*>* files_ptr;
    for (auto level = start_level; level < output_level; ++level) {
      if (level == 0) {
        files_ptr = &level0_files;
      } else {
        vstorage->GetOverlappingInputs(level, smallest, largest,
                                       &overlap_files, -1, nullptr);
        files_ptr = &overlap_files;
      }
      for (auto f : *files_ptr) {
        for (size_t i = 0; i < f->range_set.size(); i += 2) {
          if ((largest != nullptr &&
               ucmp->Compare(largest->user_key(),
                             f->range_set[i].user_key()) >= 0) &&
              (smallest != nullptr &&
               ucmp->Compare(smallest->user_key(),
                             f->range_set[i + 1].user_key()) <= 0)) {
            return true;
          }
        }
      }
    }
    return false;
  };
  // skip sst & calc input range
  if (inputs.back().level == output_level) {
    auto& files = inputs.back().files;
    assert(!files.empty());
    auto push_range = [&](int start, int end) {
      if (start != -1) {
        // range
        CompactionInputFilesRange range;
        if (start > 0) {
          range.smallest = &files[start - 1]->largest();
          range.flags = CompactionInputFilesRange::kSmallestOpen;
        }
        if (end < (int)files.size()) {
          range.largest = &files[end]->smallest();
          range.flags |= CompactionInputFilesRange::kLargestOpen;
        }
        input_range.emplace_back(range);
      } else if (end == 0) {
        if (need_compact_between(nullptr, files.front())) {
          // this case should be rare
          // head range
          CompactionInputFilesRange range;
          range.largest = &files.front()->largest();
          range.flags |= CompactionInputFilesRange::kLargestOpen;
          input_range.emplace_back(range);
        }
      } else if (end == (int)files.size()) {
        if (need_compact_between(files.back(), nullptr)) {
          // tail range
          CompactionInputFilesRange range;
          range.smallest = &files[start]->largest();
          range.flags |= CompactionInputFilesRange::kSmallestOpen;
          input_range.emplace_back(range);
        }
      } else if (need_compact_between(files[end - 1], files[end])) {
        // insert break
        CompactionInputFilesRange range;
        range.smallest = &files[end - 1]->largest();
        range.largest = &files[end]->smallest();
        range.flags = CompactionInputFilesRange::kSmallestOpen |
                      CompactionInputFilesRange::kLargestOpen;
        input_range.emplace_back(range);
      }
    };
    std::vector<FileMetaData*> new_files;
    int start = -1;
    for (int i = 0; i < (int)files.size(); ++i) {
      if (need_compact_file(files[i])) {
        new_files.emplace_back(files[i]);
        if (start == -1) {
          start = i;
        }
      } else {
        push_range(start, i);
        start = -1;
      }
    }
    if (start != 0) {
      push_range(start, (int)files.size());
    }
    files.swap(new_files);
  }
  uint64_t estimated_total_size = 0;
  for (auto& input : inputs) {
    for (auto f : input.files) {
      assert(!f->being_compacted);
      estimated_total_size += f->fd.GetFileSize() *
                              (kPartialRemovedMax - f->partial_removed) /
                              kPartialRemovedMax;
    }
  }
  auto path_id = GetPathId(ioptions_, estimated_total_size);
  auto c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, inputs, output_level,
      mutable_cf_options.MaxFileSizeForLevel(output_level),
      LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, start_level,
                         1), /* grandparents */ {}, /* is manual */ false, 0,
      false /* deletion_compaction */, false, ioptions_.enable_partial_remove,
      input_range, CompactionReason::kUniversalContinue);
  return c;
}

Compaction* UniversalCompactionPicker::TrivialMovePickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, LogBuffer* log_buffer) {
  if (!ioptions_.compaction_options_universal.allow_trivial_move) {
    return nullptr;
  }
  int output_level = vstorage->num_levels() - 1;
  // last level is reserved for the files ingested behind
  if (ioptions_.allow_ingest_behind) {
    --output_level;
  }
  int start_level = 0;
  while (true) {
    // found an empty level
    for (; output_level >= 1; --output_level) {
      if (!vstorage->LevelFiles(output_level).empty()) {
        continue;
      }
      bool invalid = false;
      for (auto c : compactions_in_progress_) {
        if (c->output_level() == output_level) {
          invalid = true;
          break;
        }
      }
      if (invalid) {
        continue;
      }
      break;
    }
    if (output_level < 1) {
      return nullptr;
    }
    bool invalid = false;
    // found an non empty level
    for (start_level = output_level - 1; start_level > 0; --start_level) {
      invalid = false;
      for (auto c : compactions_in_progress_) {
        if (c->output_level() == start_level) {
          invalid = true;
          break;
        }
      }
      if (!invalid && vstorage->LevelFiles(start_level).empty()) {
        continue;
      }
      break;
    }
    if (!invalid) {
      // will move lv0 last sst
      if (start_level == 0) {
        break;
      }
      auto& files = vstorage->LevelFiles(start_level);
      for (size_t i = 0; i < files.size(); i++) {
        if (files[i]->being_compacted || files[i]->compact_to_level) {
          invalid = true;
        }
      }
      if (!invalid) {
        break;
      }
    }
    output_level = start_level - 1;
  }
  CompactionInputFiles inputs;
  inputs.level = start_level;
  uint32_t path_id = 0;
  if (start_level == 0) {
    auto& level0_files = vstorage->LevelFiles(0);
    if (level0_files.empty() || level0_files.back()->being_compacted ||
        level0_files.back()->compact_to_level) {
      return nullptr;
    }
    FileMetaData* meta = level0_files.back();
    inputs.files = { meta };
    path_id = meta->fd.GetPathId();
  } else {
    inputs.files = vstorage->LevelFiles(start_level);
    path_id = inputs.files.front()->fd.GetPathId();
  }
  assert(!AreFilesInCompaction(inputs.files));
  auto c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, { std::move(inputs) },
      output_level, mutable_cf_options.MaxFileSizeForLevel(output_level),
      LLONG_MAX, path_id, kNoCompression, /* grandparents */ {},
      /* is manual */ false, 0, false /* deletion_compaction */,
      false, false, {}, CompactionReason::kUniversalTrivialMove);
  c->set_is_trivial_move(true);
  return c;
}

namespace {
  struct RankingElem {
    double   cur_sr_ratio; // = sr->size / size_sum
    double   max_sr_ratio;
    uint64_t size_sum;
    uint64_t size_max_val; // most cases is sr->size
    size_t   size_max_idx;
    size_t   real_idx;
  };
  struct Candidate {
    size_t   start;
    size_t   count;
    uint64_t max_sr_size;
    double   max_sr_ratio;
  };
}  // namespace

//
// Consider compaction files based on their size differences with
// the next file in time order.
//
Compaction* UniversalCompactionBuilder::PickCompactionToReduceSortedRuns(
    unsigned int ratio, unsigned int max_number_of_files_to_compact) {
  unsigned int min_merge_width =
      mutable_cf_options_.compaction_options_universal.min_merge_width;
  unsigned int max_merge_width =
      mutable_cf_options_.compaction_options_universal.max_merge_width;

  bool done = false;
  size_t start_index = 0;
  size_t candidate_count = 0;

  size_t write_buffer_size = mutable_cf_options.write_buffer_size;
  double qlev; // a dynamic level_size multiplier
  double slev; // multiplier for small sst
  double xlev; // multiplier for size reversed levels
  double skip_min_ratio = max_merge_width * std::log2(max_merge_width);
  {
    // assume sorted run sizes are a geometric sequence, and seq len is n.
    // compute an approximate q, then compute qlev
    //
    // treat a[i] is ith sorted run, then a[i] = a[0]*pow(q,i)
    // then q = pow(a[n]/a[0], 1/n)
    //   -- to make the approximation simple and stable, we let a[n]=sum
    //
    // given 'm' adjacent sorted runs, in the geometric sequence, a[m] is
    // the largest one. the ratio=a[m]/sum(am) dominate WriteAmp, so it
    // should be limited, we compute qlev as the threshold, qlev means: we
    // assume the 'm' is infinite, then sum of 'm' sorted runs is:
    //   sum(am) = a[m] / (1 - 1/q) = a[m]*q/(q-1), so
    //   qlev = a[m]/sum(am) = (q-1)/q
    //
    uint64_t sum = 0;
    for (auto& sr : sorted_runs) sum += sr.compensated_file_size;
    size_t n = mutable_cf_options.level0_file_num_compaction_trigger;
    if (compactions_in_progress_.empty()) {
      n += (ioptions_.num_levels - 1) / 2; // reduce n to increase q
    } else {
      n += (ioptions_.num_levels - 1);
    }
    sum = std::max<uint64_t>(sum, n * write_buffer_size);
    double q = std::pow(double(sum) / write_buffer_size, 1.0/n);
    qlev = (q - 1) / q;
    qlev = std::max(qlev, 0.51);
    slev = std::sqrt(qlev);     // relax
    xlev = std::pow(qlev, 0.4); // relax more
  }
  unsigned int max_files_to_compact =
       std::max(2U, std::min(max_merge_width, max_number_of_files_to_compact));
  min_merge_width = std::max(min_merge_width, 2U);
  min_merge_width = std::min(min_merge_width, max_files_to_compact);

  ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: "
      "ratio = %u, max_files_to_compact = %u, "
      "score = %f, qlev = %f, slev = %f, xlev = %f, "
      "min_merge_width = %u, max_merge_width = %u"
      , cf_name.c_str()
      , ratio, max_files_to_compact
      , score, qlev, slev, xlev
      , min_merge_width, max_merge_width
      );

  // Caller checks the size before executing this function. This invariant is
  // important because otherwise we may have a possible integer underflow when
  // dealing with unsigned types.
  assert(sorted_runs_.size() > 0);

  std::vector<RankingElem> rankingVec(sorted_runs.size());
  auto computeRanking = [&](size_t start_idx, size_t count) {
    auto& rv = rankingVec;
    rv.resize(count);
    rv[0].cur_sr_ratio = 1.0;
    rv[0].max_sr_ratio = 1.0;
    rv[0].size_max_idx = 0;
    rv[0].size_max_val = sorted_runs[start_idx].compensated_file_size;
    rv[0].size_sum = sorted_runs[start_idx].compensated_file_size;
    rv[0].real_idx = start_idx;
    for (size_t i = 1; i < count; i++) {
      auto& sr1 = sorted_runs[start_idx + i];
      if (sr1.compensated_file_size > rv[i-1].size_max_val) {
        rv[i].size_max_val = sr1.compensated_file_size;
        rv[i].size_max_idx = i;
      } else {
        rv[i].size_max_val = rv[i-1].size_max_val;
        rv[i].size_max_idx = rv[i-1].size_max_idx;
      }
      rv[i].size_sum = rv[i-1].size_sum + sr1.compensated_file_size;
      rv[i].cur_sr_ratio = double(sr1.compensated_file_size) / rv[i].size_sum;
      rv[i].max_sr_ratio = double(rv[i].size_max_val) / rv[i].size_sum;
      rv[i].real_idx = i;
    }
    // a best candidate is which max_sr_ratio is the smallest
    std::sort(rv.begin(), rv.begin() + count,
        [](const RankingElem& x, const RankingElem& y) {
          return x.max_sr_ratio < y.max_sr_ratio;
        });
  };
  std::vector<const SortedRun*> sr_bysize(sorted_runs.size());
  for (size_t i = 0; i < sr_bysize.size(); i++) {
    sr_bysize[i] = &sorted_runs[i];
  }
  std::stable_sort(sr_bysize.begin(), sr_bysize.end(),
      [write_buffer_size](const SortedRun* x, const SortedRun* y) {
        size_t x_rough = x->compensated_file_size / write_buffer_size;
        size_t y_rough = y->compensated_file_size / write_buffer_size;
        return x_rough < y_rough;
      });

  std::vector<Candidate> candidate_vec;
  candidate_vec.reserve(sorted_runs.size());

  auto discard_small_sr = [&](uint64_t max_sr_size) {
    while (candidate_count > min_merge_width &&
           sorted_runs[start_index].size * skip_min_ratio < max_sr_size) {
      char file_num_buf[kFormatFileNumberBufSize];
      sorted_runs[start_index].Dump(file_num_buf, sizeof(file_num_buf),
                                    true);
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Universal: min/max = %7.5f too small, "
                       "Skipping %s", cf_name.c_str(),
                       sorted_runs[start_index].size / double(max_sr_size),
                       file_num_buf);
      ++start_index;
      --candidate_count;
    }
  };

  // Considers a candidate file only if it is smaller than the
  // total size accumulated so far.
  for (size_t loop = 0; loop < sorted_runs_.size(); loop++) {
    candidate_count = 0;

    // Skip files that are already being compacted
    for (sr = nullptr; loop < sorted_runs_.size(); loop++) {
      sr = &sorted_runs_[loop];

      if (!sr->being_compacted) {
        candidate_count = 1;
        break;
      }
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf));
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Universal: %s"
                       "[%d] being compacted, skipping",
                       cf_name_.c_str(), file_num_buf, loop);

      sr = nullptr;
    }

    // This file is not being compacted. Consider it as the
    // first candidate to be compacted.
    uint64_t candidate_size = sr != nullptr ? sr->compensated_file_size : 0;
    if (sr != nullptr) {
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf), true);
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Universal: Possible candidate %s[%d].",
                       cf_name_.c_str(), file_num_buf, loop);
    }

    // Check if the succeeding files need compaction.
    for (size_t i = loop + 1;
         candidate_count < max_files_to_compact && i < sorted_runs_.size();
         i++) {
      const SortedRun* succeeding_sr = &sorted_runs_[i];
      if (succeeding_sr->being_compacted) {
        break;
      }
      // Pick files if the total/last candidate file size (increased by the
      // specified ratio) is still larger than the next candidate file.
      // candidate_size is the total size of files picked so far with the
      // default kCompactionStopStyleTotalSize; with
      // kCompactionStopStyleSimilarSize, it's simply the size of the last
      // picked file.
      double sz = candidate_size * (100.0 + ratio) / 100.0;
      if (sz < static_cast<double>(succeeding_sr->size)) {
        break;
      }
      if (mutable_cf_options_.compaction_options_universal.stop_style ==
          kCompactionStopStyleSimilarSize) {
        // Similar-size stopping rule: also check the last picked file isn't
        // far larger than the next candidate file.
        sz = (succeeding_sr->size * (100.0 + ratio)) / 100.0;
        if (sz < static_cast<double>(candidate_size)) {
          // If the small file we've encountered begins a run of similar-size
          // files, we'll pick them up on a future iteration of the outer
          // loop. If it's some lonely straggler, it'll eventually get picked
          // by the last-resort read amp strategy which disregards size ratios.
          break;
        }
        candidate_size = succeeding_sr->compensated_file_size;
      } else {  // default kCompactionStopStyleTotalSize
        candidate_size += succeeding_sr->compensated_file_size;
      }
      candidate_count++;
    }

    auto logSkipping = [&](const char* reason) {
      size_t limit2 = std::min(loop + candidate_count, sorted_runs.size());
      for (size_t i = loop; i < limit2; i++) {
        sorted_runs[i].DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
        ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal:%s Skipping %s",
                         cf_name.c_str(), reason, file_num_buf);
      }
    };

    // Found a series of consecutive files that need compaction.
    if (candidate_count >= min_merge_width) {
      computeRanking(loop, candidate_count);
      double max_sr_ratio = double(max_sr_size) / sum_sr_size;
      for (size_t rank = 0; rank < candidate_count - min_merge_width; rank++) {
        size_t merge_width = rankingVec[rank].real_idx + 1;
        if (merge_width >= min_merge_width) {
          max_sr_ratio = rankingVec[rank].max_sr_ratio;
          max_sr_size = rankingVec[rank].size_max_val;
          sum_sr_size = rankingVec[rank].size_sum;
          candidate_count = merge_width;
          // found a best picker which start from loop and has
          // at least min_merge_width sorted runs
          break;
        }
      }
      bool has_small_bottom = false;
      for (size_t i = 1; i < candidate_count; ++i) {
        auto& prev = sorted_runs[loop + i - 1];
        auto& curr = sorted_runs[loop + i + 0];
        if (curr.compensated_file_size < prev.compensated_file_size) {
          has_small_bottom = true;
          break;
        }
      }
      auto& o = mutable_cf_options;
      auto small_sum = o.write_buffer_size * o.max_write_buffer_number;
      // in worst case, all sorted runs are picked, and the size condition
      // is still not satisfied, in this case, we must pick the compaction
      if ( // candidate_count >= max_sr_num ||
           max_sr_ratio < qlev ||
          (max_sr_ratio < xlev && has_small_bottom) ||
          (max_sr_ratio < slev && sum_sr_size < small_sum)) {
        // not so bad, pick the compaction
      start_index = loop;
        discard_small_sr(max_sr_size);
      done = true;
      break;
    } else {
      for (size_t i = loop;
           i < loop + candidate_count && i < sorted_runs_.size(); i++) {
        const SortedRun* skipping_sr = &sorted_runs_[i];
        char file_num_buf[256];
        skipping_sr->DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
        ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: Skipping %s",
                         cf_name_.c_str(), file_num_buf);
      }
    } else if (candidate_count > 1) { // do not print if candidate_count == 1
      char buf[80];
      sprintf(buf, " candidate_count(%zd) < min_merge_width(%d),"
                 , candidate_count, min_merge_width);
      logSkipping(buf);
    }
  }
  if (!done || candidate_count <= 1) {
    if (candidate_vec.size() < 1) {
    return nullptr;
  }
    auto best = candidate_vec.begin();
    for (auto iter = best + 1; iter < candidate_vec.end(); ++iter) {
      if (iter->max_sr_ratio < best->max_sr_ratio) {
        best = iter;
      }
    }
    if (best->max_sr_ratio > slev) {
      return nullptr;
    }
    char file_num_buf[kFormatFileNumberBufSize];
    sorted_runs[best->start].Dump(file_num_buf, sizeof(file_num_buf), true);
    ROCKS_LOG_BUFFER(log_buffer,
        "[%s] Universal: repicked candidate %s[%d], "
        "count = %zd, max/sum = %7.5f",
        cf_name.c_str(), file_num_buf, best->start,
        best->count, best->max_sr_ratio);
    start_index = best->start;
    candidate_count = best->count;
    discard_small_sr(best->max_sr_size);
    done = true;
  }
  size_t first_index_after = start_index + candidate_count;
  // Compression is enabled if files compacted earlier already reached
  // size ratio of compression.
  bool enable_compression = true;
  int ratio_to_compress =
      mutable_cf_options_.compaction_options_universal.compression_size_percent;
  if (ratio_to_compress >= 0) {
    uint64_t total_size = 0;
    for (auto& sorted_run : sorted_runs_) {
      total_size += sorted_run.compensated_file_size;
    }

    uint64_t older_file_size = 0;
    for (size_t i = sorted_runs_.size() - 1; i >= first_index_after; i--) {
      older_file_size += sorted_runs_[i].size;
      if (older_file_size * 100L >= total_size * (long)ratio_to_compress) {
        enable_compression = false;
        break;
      }
    }
  }

  uint64_t estimated_total_size = 0;
  for (unsigned int i = 0; i < first_index_after; i++) {
    estimated_total_size += sorted_runs_[i].size;
  }
  uint32_t path_id =
      GetPathId(ioptions_, mutable_cf_options_, estimated_total_size);
  int start_level = sorted_runs_[start_index].level;
  int output_level;
  if (first_index_after == sorted_runs_.size()) {
    output_level = vstorage_->num_levels() - 1;
  } else if (sorted_runs_[first_index_after].level == 0) {
    output_level = 0;
  } else {
    output_level = sorted_runs_[first_index_after].level - 1;
  }

  // last level is reserved for the files ingested behind
  if (ioptions_.allow_ingest_behind &&
      (output_level == vstorage_->num_levels() - 1)) {
    assert(output_level > 1);
    output_level--;
  }

  std::vector<CompactionInputFiles> inputs(vstorage_->num_levels());
  for (size_t i = 0; i < inputs.size(); ++i) {
    inputs[i].level = start_level + static_cast<int>(i);
  }
  for (size_t i = start_index; i < first_index_after; i++) {
    auto& picking_sr = sorted_runs_[i];
    if (picking_sr.level == 0) {
      FileMetaData* picking_file = picking_sr.file;
      inputs[0].files.push_back(picking_file);
    } else {
      auto& files = inputs[picking_sr.level - start_level].files;
      for (auto* f : vstorage_->LevelFiles(picking_sr.level)) {
        files.push_back(f);
      }
    }
    char file_num_buf[256];
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), i);
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: Picking %s",
                     cf_name_.c_str(), file_num_buf);
  }

  CompactionReason compaction_reason;
  if (max_number_of_files_to_compact == UINT_MAX) {
    compaction_reason = CompactionReason::kUniversalSizeRatio;
  } else {
    compaction_reason = CompactionReason::kUniversalSortedRunNum;
  }
  return new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, std::move(inputs),
      output_level,
      MaxFileSizeForLevel(mutable_cf_options_, output_level,
                          kCompactionStyleUniversal),
      LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_, start_level,
                         1, enable_compression),
      GetCompressionOptions(mutable_cf_options_, vstorage_, start_level,
                            enable_compression),
      /* max_subcompactions */ 0, /* grandparents */ {}, /* is manual */ false,
      score_, false /* deletion_compaction */, compaction_reason);
}

// Look at overall size amplification. If size amplification
// exceeds the configured value, then do a compaction
// of the candidate files all the way upto the earliest
// base file (overrides configured values of file-size ratios,
// min_merge_width and max_merge_width).
//
Compaction* UniversalCompactionBuilder::PickCompactionToReduceSizeAmp() {
  // percentage flexibility while reducing size amplification
  uint64_t ratio = mutable_cf_options_.compaction_options_universal
                       .max_size_amplification_percent;

  unsigned int candidate_count = 0;
  uint64_t candidate_size = 0;
  size_t start_index = 0;
  const SortedRun* sr = nullptr;

  assert(!sorted_runs_.empty());
  if (sorted_runs_.back().being_compacted) {
    return nullptr;
  }

  // Skip files that are already being compacted
  for (size_t loop = 0; loop + 1 < sorted_runs_.size(); loop++) {
    sr = &sorted_runs_[loop];
    if (!sr->being_compacted) {
      start_index = loop;  // Consider this as the first candidate.
      break;
    }
    char file_num_buf[kFormatFileNumberBufSize];
    sr->Dump(file_num_buf, sizeof(file_num_buf), true);
    ROCKS_LOG_BUFFER(log_buffer_,
                     "[%s] Universal: skipping %s[%d] compacted %s",
                     cf_name_.c_str(), file_num_buf, loop,
                     " cannot be a candidate to reduce size amp.\n");
    sr = nullptr;
  }

  if (sr == nullptr) {
    return nullptr;  // no candidate files
  }
  {
    char file_num_buf[kFormatFileNumberBufSize];
    sr->Dump(file_num_buf, sizeof(file_num_buf), true);
    ROCKS_LOG_BUFFER(
        log_buffer_,
        "[%s] Universal: First candidate %s[%" ROCKSDB_PRIszt "] %s",
        cf_name_.c_str(), file_num_buf, start_index, " to reduce size amp.\n");
  }

  // keep adding up all the remaining files
  for (size_t loop = start_index; loop + 1 < sorted_runs_.size(); loop++) {
    sr = &sorted_runs_[loop];
    if (sr->being_compacted) {
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf), true);
      ROCKS_LOG_BUFFER(
          log_buffer_, "[%s] Universal: Possible candidate %s[%d] %s",
          cf_name_.c_str(), file_num_buf, start_index,
          " is already being compacted. No size amp reduction possible.\n");
      return nullptr;
    }
    candidate_size += sr->compensated_file_size;
    candidate_count++;
  }
  if (candidate_count < 2) {
    return nullptr;
  }

  // size of earliest file
  uint64_t earliest_file_size = sorted_runs_.back().size;

  // size amplification = percentage of additional size
  if (candidate_size * 100 < ratio * earliest_file_size) {
    ROCKS_LOG_BUFFER(
        log_buffer_,
        "[%s] Universal: size amp not needed. newer-files-total-size %" PRIu64
        " earliest-file-size %" PRIu64,
        cf_name_.c_str(), candidate_size, earliest_file_size);
    return nullptr;
  } else {
    ROCKS_LOG_BUFFER(
        log_buffer_,
        "[%s] Universal: size amp needed. newer-files-total-size %" PRIu64
        " earliest-file-size %" PRIu64,
        cf_name_.c_str(), candidate_size, earliest_file_size);
  }
  return PickCompactionToOldest(start_index,
                                CompactionReason::kUniversalSizeAmplification);
}

// Pick files marked for compaction. Typically, files are marked by
// CompactOnDeleteCollector due to the presence of tombstones.
Compaction* UniversalCompactionBuilder::PickDeleteTriggeredCompaction() {
  CompactionInputFiles start_level_inputs;
  int output_level;
  std::vector<CompactionInputFiles> inputs;

  if (vstorage_->num_levels() == 1) {
#if defined(ENABLE_SINGLE_LEVEL_DTC)
    // This is single level universal. Since we're basically trying to reclaim
    // space by processing files marked for compaction due to high tombstone
    // density, let's do the same thing as compaction to reduce size amp which
    // has the same goals.
    bool compact = false;

    start_level_inputs.level = 0;
    start_level_inputs.files.clear();
    output_level = 0;
    for (FileMetaData* f : vstorage_->LevelFiles(0)) {
      if (f->marked_for_compaction) {
        compact = true;
      }
      if (compact) {
        start_level_inputs.files.push_back(f);
      }
    }
    if (start_level_inputs.size() <= 1) {
      // If only the last file in L0 is marked for compaction, ignore it
      return nullptr;
    }
    inputs.push_back(start_level_inputs);
#else
    // Disable due to a known race condition.
    // TODO: Reenable once the race condition is fixed
    return nullptr;
#endif  // ENABLE_SINGLE_LEVEL_DTC
  } else {
    int start_level;

    // For multi-level universal, the strategy is to make this look more like
    // leveled. We pick one of the files marked for compaction and compact with
    // overlapping files in the adjacent level.
    picker_->PickFilesMarkedForCompaction(cf_name_, vstorage_, &start_level,
                                          &output_level, &start_level_inputs);
    if (start_level_inputs.empty()) {
      return nullptr;
    }

    // Pick the first non-empty level after the start_level
    for (output_level = start_level + 1; output_level < vstorage_->num_levels();
         output_level++) {
      if (vstorage_->NumLevelFiles(output_level) != 0) {
        break;
      }
    }

    // If all higher levels are empty, pick the highest level as output level
    if (output_level == vstorage_->num_levels()) {
      if (start_level == 0) {
        output_level = vstorage_->num_levels() - 1;
      } else {
        // If start level is non-zero and all higher levels are empty, this
        // compaction will translate into a trivial move. Since the idea is
        // to reclaim space and trivial move doesn't help with that, we
        // skip compaction in this case and return nullptr
        return nullptr;
      }
    }
    if (ioptions_.allow_ingest_behind &&
        output_level == vstorage_->num_levels() - 1) {
      assert(output_level > 1);
      output_level--;
    }

    if (output_level != 0) {
      if (start_level == 0) {
        if (!picker_->GetOverlappingL0Files(vstorage_, &start_level_inputs,
                                            output_level, nullptr)) {
          return nullptr;
        }
      }

      CompactionInputFiles output_level_inputs;
      int parent_index = -1;

      output_level_inputs.level = output_level;
      if (!picker_->SetupOtherInputs(cf_name_, mutable_cf_options_, vstorage_,
                                     &start_level_inputs, &output_level_inputs,
                                     &parent_index, -1)) {
        return nullptr;
      }
      inputs.push_back(start_level_inputs);
      if (!output_level_inputs.empty()) {
        inputs.push_back(output_level_inputs);
      }
      if (picker_->FilesRangeOverlapWithCompaction(inputs, output_level)) {
        return nullptr;
      }
    } else {
      inputs.push_back(start_level_inputs);
    }
  }

  uint64_t estimated_total_size = 0;
  // Use size of the output level as estimated file size
  for (FileMetaData* f : vstorage_->LevelFiles(output_level)) {
    estimated_total_size += f->fd.GetFileSize();
  }
  uint32_t path_id =
      GetPathId(ioptions_, mutable_cf_options_, estimated_total_size);
  return new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, std::move(inputs),
      output_level,
      MaxFileSizeForLevel(mutable_cf_options_, output_level,
                          kCompactionStyleUniversal),
      /* max_grandparent_overlap_bytes */ LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_,
                         output_level, 1),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level),
      /* max_subcompactions */ 0, /* grandparents */ {}, /* is manual */ false,
      score_, false /* deletion_compaction */,
      CompactionReason::kFilesMarkedForCompaction);
}

Compaction* UniversalCompactionBuilder::PickCompactionToOldest(
    size_t start_index, CompactionReason compaction_reason) {
  assert(start_index < sorted_runs_.size());

  // Estimate total file size
  uint64_t estimated_total_size = 0;
  for (size_t loop = start_index; loop < sorted_runs_.size(); loop++) {
    estimated_total_size += sorted_runs_[loop].size;
  }
  uint32_t path_id =
      GetPathId(ioptions_, mutable_cf_options_, estimated_total_size);
  int start_level = sorted_runs_[start_index].level;

  std::vector<CompactionInputFiles> inputs(vstorage_->num_levels());
  for (size_t i = 0; i < inputs.size(); ++i) {
    inputs[i].level = start_level + static_cast<int>(i);
  }
  for (size_t loop = start_index; loop < sorted_runs_.size(); loop++) {
    auto& picking_sr = sorted_runs_[loop];
    if (picking_sr.level == 0) {
      FileMetaData* f = picking_sr.file;
      inputs[0].files.push_back(f);
    } else {
      auto& files = inputs[picking_sr.level - start_level].files;
      for (auto* f : vstorage_->LevelFiles(picking_sr.level)) {
        files.push_back(f);
      }
    }
    std::string comp_reason_print_string;
    if (compaction_reason == CompactionReason::kPeriodicCompaction) {
      comp_reason_print_string = "periodic compaction";
    } else if (compaction_reason ==
               CompactionReason::kUniversalSizeAmplification) {
      comp_reason_print_string = "size amp";
    } else {
      assert(false);
    }

    char file_num_buf[256];
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: %s picking %s",
                     cf_name_.c_str(), comp_reason_print_string.c_str(),
                     file_num_buf);
  }

  // output files at the bottom most level, unless it's reserved
  int output_level = vstorage_->num_levels() - 1;
  // last level is reserved for the files ingested behind
  if (ioptions_.allow_ingest_behind) {
    assert(output_level > 1);
    output_level--;
  }

  // We never check size for
  // compaction_options_universal.compression_size_percent,
  // because we always compact all the files, so always compress.
  return new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, std::move(inputs),
      output_level,
      MaxFileSizeForLevel(mutable_cf_options_, output_level,
                          kCompactionStyleUniversal),
      LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_, start_level,
                         1, true /* enable_compression */),
      GetCompressionOptions(mutable_cf_options_, vstorage_, start_level,
                            true /* enable_compression */),
      /* max_subcompactions */ 0, /* grandparents */ {}, /* is manual */ false,
      score_, false /* deletion_compaction */, compaction_reason);
}

Compaction* UniversalCompactionBuilder::PickPeriodicCompaction() {
  ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: Periodic Compaction",
                   cf_name_.c_str());

  // In universal compaction, sorted runs contain older data are almost always
  // generated earlier too. To simplify the problem, we just try to trigger
  // a full compaction. We start from the oldest sorted run and include
  // all sorted runs, until we hit a sorted already being compacted.
  // Since usually the largest (which is usually the oldest) sorted run is
  // included anyway, doing a full compaction won't increase write
  // amplification much.

  // Get some information from marked files to check whether a file is
  // included in the compaction.

  size_t start_index = sorted_runs_.size();
  while (start_index > 0 && !sorted_runs_[start_index - 1].being_compacted) {
    start_index--;
  }
  if (start_index == sorted_runs_.size()) {
    return nullptr;
  }

  // There is a rare corner case where we can't pick up all the files
  // because some files are being compacted and we end up with picking files
  // but none of them need periodic compaction. Unless we simply recompact
  // the last sorted run (either the last level or last L0 file), we would just
  // execute the compaction, in order to simplify  the logic.
  if (start_index == sorted_runs_.size() - 1) {
    bool included_file_marked = false;
    int start_level = sorted_runs_[start_index].level;
    FileMetaData* start_file = sorted_runs_[start_index].file;
    for (const std::pair<int, FileMetaData*>& level_file_pair :
         vstorage_->FilesMarkedForPeriodicCompaction()) {
      if (start_level != 0) {
        // Last sorted run is a level
        if (start_level == level_file_pair.first) {
          included_file_marked = true;
          break;
        }
      } else {
        // Last sorted run is a L0 file.
        if (start_file == level_file_pair.second) {
          included_file_marked = true;
          break;
        }
      }
    }
    if (!included_file_marked) {
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Universal: Cannot form a compaction covering file "
                       "marked for periodic compaction",
                       cf_name_.c_str());
      return nullptr;
    }
  }

  Compaction* c = PickCompactionToOldest(start_index,
                                         CompactionReason::kPeriodicCompaction);

  TEST_SYNC_POINT_CALLBACK(
      "UniversalCompactionPicker::PickPeriodicCompaction:Return", c);

  return c;
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
