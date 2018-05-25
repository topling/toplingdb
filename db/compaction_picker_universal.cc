//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction_picker_universal.h"
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <cmath>
#include <limits>
#include <queue>
#include <string>
#include <utility>
#include "db/column_family.h"
#include "monitoring/statistics.h"
#include "util/filename.h"
#include "util/log_buffer.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {
namespace {
// Used in universal compaction when trivial move is enabled.
// This structure is used for the construction of min heap
// that contains the file meta data, the level of the file
// and the index of the file in that level

struct InputFileInfo {
  FileMetaData* f = nullptr;
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
    assert(f->smallest_seqno <= f->largest_seqno);
    if (is_first) {
      is_first = false;
      *smallest_seqno = f->smallest_seqno;
      *largest_seqno = f->largest_seqno;
    } else {
      if (f->smallest_seqno < *smallest_seqno) {
        *smallest_seqno = f->smallest_seqno;
      }
      if (f->largest_seqno > *largest_seqno) {
        *largest_seqno = f->largest_seqno;
      }
    }
  }
}
#endif
}  // namespace

// Algorithm that checks to see if there are any overlapping
// files in the input
bool UniversalCompactionPicker::IsInputFilesNonOverlapping(Compaction* c) {
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

    if (curr.level != 0 && curr.index < c->num_input_files(curr.level) - 1) {
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
  // deep copy
  auto need_continue_compaction = vstorage->need_continue_compaction();
  for (auto c : compactions_in_progress_) {
    if (!c->is_finished()) {
      for (auto it = need_continue_compaction.begin();
           it != need_continue_compaction.end();) {
        if (it->second == c->output_level()) {
          it = need_continue_compaction.erase(it);
        } else {
          ++it;
        }
      }
    }
  }
  return !need_continue_compaction.empty() ||
         vstorage->CompactionScore(kLevel0) >= 1;
}

void UniversalCompactionPicker::SortedRun::Dump(char* out_buf,
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

void UniversalCompactionPicker::SortedRun::DumpSizeInfo(
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

std::vector<UniversalCompactionPicker::SortedRun>
UniversalCompactionPicker::CalculateSortedRuns(
    const VersionStorageInfo& vstorage, const ImmutableCFOptions& ioptions) {
  std::vector<UniversalCompactionPicker::SortedRun> ret;
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
      if (f->being_compacted || f->compact_to_level) {
        being_compacted = true;
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
//
Compaction* UniversalCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, LogBuffer* log_buffer) {
  const int kLevel0 = 0;
  double score = vstorage->CompactionScore(kLevel0);
  std::vector<SortedRun> sorted_runs =
      CalculateSortedRuns(*vstorage, ioptions_);

  if (sorted_runs.size() == 0 ||
      (vstorage->need_continue_compaction().empty() &&
       sorted_runs.size() <
           (size_t)mutable_cf_options.level0_file_num_compaction_trigger)) {
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: nothing to do\n",
                     cf_name.c_str());
    TEST_SYNC_POINT_CALLBACK("UniversalCompactionPicker::PickCompaction:Return",
                             nullptr);
    return nullptr;
  }
  VersionStorageInfo::LevelSummaryStorage tmp;
  ROCKS_LOG_BUFFER_MAX_SZ(
      log_buffer, 3072,
      "[%s] Universal: sorted runs files(%" ROCKSDB_PRIszt "): %s\n",
      cf_name.c_str(), sorted_runs.size(), vstorage->LevelSummary(&tmp));

  // Check for size amplification first.
  Compaction* c;
  if (!vstorage->need_continue_compaction().empty() &&
      (c = PickCompactionConitnue(cf_name, mutable_cf_options, vstorage,
                                  log_buffer, 0)) != nullptr) {
    // continue compaction
  } else if ((c = TrivialMovePickCompaction(cf_name, mutable_cf_options,
                                            vstorage,
                                            log_buffer)) != nullptr) {
    // universal trivial move;
  } else if (false &&
             (c = PickCompactionToReduceSizeAmp(
                      cf_name, mutable_cf_options, vstorage,
                      score, sorted_runs, log_buffer)) != nullptr) {
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: compacting for size amp\n",
                     cf_name.c_str());
  } else {
    // Size amplification is within limits. Try reducing read
    // amplification while maintaining file size ratios.
    unsigned int ratio = ioptions_.compaction_options_universal.size_ratio;

    if ((c = PickCompactionToReduceSortedRuns(
             cf_name, mutable_cf_options, vstorage, score, ratio, UINT_MAX,
             sorted_runs, log_buffer)) != nullptr) {
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Universal: compacting for size ratio\n",
                       cf_name.c_str());
    }
  }
  if (c == nullptr) {
    TEST_SYNC_POINT_CALLBACK("UniversalCompactionPicker::PickCompaction:Return",
                             nullptr);
    return nullptr;
  }

  if (c->compaction_reason() != CompactionReason::kUniversalContinue &&
      c->compaction_reason() != CompactionReason::kUniversalTrivialMove &&
      ioptions_.compaction_options_universal.allow_trivial_move) {
    c->set_is_trivial_move(IsInputFilesNonOverlapping(c));
  }

// validate that all the chosen files of L0 are non overlapping in time
#ifndef NDEBUG
  SequenceNumber prev_smallest_seqno = 0U;
  bool is_first = true;

  size_t level_index = 0U;
  if (c->start_level() == 0) {
    for (auto f : *c->inputs(0)) {
      assert(f->smallest_seqno <= f->largest_seqno);
      if (is_first) {
        is_first = false;
      }
      prev_smallest_seqno = f->smallest_seqno;
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
  MeasureTime(ioptions_.statistics, NUM_FILES_IN_SINGLE_COMPACTION,
              c->inputs(0)->size());

  RegisterCompaction(c);
  vstorage->ComputeCompactionScore(ioptions_, mutable_cf_options);

  TEST_SYNC_POINT_CALLBACK("UniversalCompactionPicker::PickCompaction:Return",
                           c);
  return c;
}

uint32_t UniversalCompactionPicker::GetPathId(
    const ImmutableCFOptions& ioptions, uint64_t file_size) {
  if (ioptions.db_paths.size() == 1) {
    return 0;
  }
  static thread_local std::mt19937_64 random;
  uint64_t sum = 0;
  size_t num = ioptions.db_paths.size();
  for (size_t i = 0; i < num; ++i) {
    sum += ioptions.db_paths[i].target_size;
  }
  uint64_t sum2 = sum;
  for (size_t i = 0; i < num-1; ++i) {
    uint64_t target_size = ioptions.db_paths[i].target_size;
    double prob = double(target_size) / sum2;
    if (random() < prob * random.max()) {
      return uint32_t(i);
    }
    sum2 -= target_size;
  }
  return uint32_t(num-1);

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
      file_size * (100 - ioptions.compaction_options_universal.size_ratio) /
      100;
  uint32_t p = 0;
  assert(!ioptions.db_paths.empty());
  for (; p < ioptions.db_paths.size() - 1; p++) {
    uint64_t target_size = ioptions.db_paths[p].target_size;
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
Compaction* UniversalCompactionPicker::PickCompactionToReduceSortedRuns(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, double score, unsigned int ratio,
    unsigned int max_number_of_files_to_compact,
    const std::vector<SortedRun>& sorted_runs, LogBuffer* log_buffer) {
  unsigned int min_merge_width =
      ioptions_.compaction_options_universal.min_merge_width;
  unsigned int max_merge_width =
      ioptions_.compaction_options_universal.max_merge_width;

  bool done = false;
  size_t start_index = 0;
  size_t candidate_count = 0;

  size_t write_buffer_size = mutable_cf_options.write_buffer_size;
  double qlev; // a dynamic level_size multiplier
  double slev; // multiplier for small sst
  double xlev; // multiplier for size reversed levels
  double skip_min_ratio = max_merge_width * std::log2(max_merge_width);
  {
    uint64_t sum = 0;
    for (auto& sr : sorted_runs) sum += sr.compensated_file_size;
    size_t n = mutable_cf_options.level0_file_num_compaction_trigger
             + ioptions_.num_levels - 1;
    sum = std::max<uint64_t>(sum, n * write_buffer_size);
    double q = std::pow(double(sum) / write_buffer_size, 1.0/n);
    qlev = (q - 1) / q;
    qlev = std::max(qlev, 0.51);
    slev = std::sqrt(qlev);
    xlev = std::pow(qlev, 0.4);
  }
  unsigned int max_files_to_compact = std::max(2U,
      std::min(max_merge_width, max_number_of_files_to_compact));
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
  assert(sorted_runs.size() > 0);

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
  const SortedRun* sr = nullptr;
  for (size_t loop1 = 0; loop1 < sr_bysize.size(); loop1++) {
    sr = sr_bysize[loop1];
    if (sr->being_compacted) {
      continue;
    }
    size_t loop = sr - &sorted_runs[0];
    candidate_count = 0;

    char file_num_buf[kFormatFileNumberBufSize];
    sr->Dump(file_num_buf, sizeof(file_num_buf), true);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: Possible candidate %s[%d].",
                     cf_name.c_str(), file_num_buf, loop);

    // ignore compaction_options_universal.size_ratio and stop_style
    uint64_t sum_sr_size = 0;
    uint64_t max_sr_size = 0;
    size_t limit = std::min(sorted_runs.size(), loop + max_files_to_compact);
    for (size_t i = loop; i < limit && !sorted_runs[i].being_compacted; i++) {
      auto x = sorted_runs[i].compensated_file_size;
      sum_sr_size += x;
      max_sr_size = std::max(max_sr_size, x);
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
        // too bad, don't pick the compaction
        char buf[64]; sprintf(buf, " max/sum = %7.5f too big,", max_sr_ratio);
        logSkipping(buf);
        Candidate cand;
        cand.count = candidate_count;
        cand.start = loop;
        cand.max_sr_size  = max_sr_size;
        cand.max_sr_ratio = max_sr_ratio;
        candidate_vec.push_back(cand);
      }
    } else {
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
      ioptions_.compaction_options_universal.compression_size_percent;
  if (ratio_to_compress >= 0) {
    uint64_t total_size = 0;
    for (auto& sorted_run : sorted_runs) {
      total_size += sorted_run.compensated_file_size;
    }

    uint64_t older_file_size = 0;
    for (size_t i = sorted_runs.size() - 1; i >= first_index_after; i--) {
      older_file_size += sorted_runs[i].size;
      if (older_file_size * 100L >= total_size * (long)ratio_to_compress) {
        enable_compression = false;
        break;
      }
    }
  }

  uint64_t estimated_total_size = 0;
  for (size_t i = start_index; i < first_index_after; i++) {
    estimated_total_size += sorted_runs[i].size;
  }
  uint32_t path_id = GetPathId(ioptions_, estimated_total_size);
  int start_level = sorted_runs[start_index].level;
  int output_level;
  if (first_index_after == sorted_runs.size()) {
    output_level = vstorage->num_levels() - 1;
  } else if (sorted_runs[first_index_after].level == 0) {
    output_level = 0;
  } else {
    output_level = sorted_runs[first_index_after].level - 1;
  }

  // last level is reserved for the files ingested behind
  if (ioptions_.allow_ingest_behind &&
      (output_level == vstorage->num_levels() - 1)) {
    assert(output_level > 1);
    output_level--;
  }

  std::vector<CompactionInputFiles> inputs(vstorage->num_levels());
  for (size_t i = 0; i < inputs.size(); ++i) {
    inputs[i].level = start_level + static_cast<int>(i);
  }
  for (size_t i = start_index; i < first_index_after; i++) {
    auto& picking_sr = sorted_runs[i];
    if (picking_sr.level == 0) {
      FileMetaData* picking_file = picking_sr.file;
      inputs[0].files.push_back(picking_file);
    } else {
      auto& files = inputs[picking_sr.level - start_level].files;
      for (auto* f : vstorage->LevelFiles(picking_sr.level)) {
        files.push_back(f);
      }
    }
    char file_num_buf[256];
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), i);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: Picking %s", cf_name.c_str(),
                     file_num_buf);
  }

  CompactionReason compaction_reason;
  if (max_number_of_files_to_compact == UINT_MAX) {
    compaction_reason = CompactionReason::kUniversalSortedRunNum;
  } else {
    compaction_reason = CompactionReason::kUniversalSizeRatio;
  }
  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, std::move(inputs), output_level,
      mutable_cf_options.MaxFileSizeForLevel(output_level), LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, start_level,
                         1, enable_compression),
      /* grandparents */ {}, /* is manual */ false, score,
      false /* deletion_compaction */, false, ioptions_.enable_partial_remove,
      {}, compaction_reason);
}

// Look at overall size amplification. If size amplification
// exceeds the configured value, then do a compaction
// of the candidate files all the way up to the earliest
// base file (overrides configured values of file-size ratios,
// min_merge_width and max_merge_width).
//
Compaction* UniversalCompactionPicker::PickCompactionToReduceSizeAmp(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, double score,
    const std::vector<SortedRun>& sorted_runs, LogBuffer* log_buffer) {
  // percentage flexibility while reducing size amplification
  uint64_t ratio =
      ioptions_.compaction_options_universal.max_size_amplification_percent;

  unsigned int candidate_count = 0;
  uint64_t candidate_size = 0;
  size_t start_index = 0;
  const SortedRun* sr = nullptr;

  // Skip files that are already being compacted
  for (size_t loop = 0; loop < sorted_runs.size() - 1; loop++) {
    sr = &sorted_runs[loop];
    if (!sr->being_compacted) {
      start_index = loop;  // Consider this as the first candidate.
      break;
    }
    char file_num_buf[kFormatFileNumberBufSize];
    sr->Dump(file_num_buf, sizeof(file_num_buf), true);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: skipping %s[%d] compacted %s",
                     cf_name.c_str(), file_num_buf, loop,
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
        log_buffer,
        "[%s] Universal: First candidate %s[%" ROCKSDB_PRIszt "] %s",
        cf_name.c_str(), file_num_buf, start_index, " to reduce size amp.\n");
  }

  // keep adding up all the remaining files
  for (size_t loop = start_index; loop < sorted_runs.size(); loop++) {
    sr = &sorted_runs[loop];
    if (sr->being_compacted) {
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf), true);
      ROCKS_LOG_BUFFER(
          log_buffer, "[%s] Universal: Possible candidate %s[%d] %s",
          cf_name.c_str(), file_num_buf, start_index,
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
  uint64_t earliest_file_size = sorted_runs.back().size;

  // size amplification = percentage of additional size
  if (candidate_size * 100 < ratio * earliest_file_size) {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Universal: size amp not needed. newer-files-total-size %" PRIu64
        " earliest-file-size %" PRIu64,
        cf_name.c_str(), candidate_size, earliest_file_size);
    return nullptr;
  } else {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Universal: size amp needed. newer-files-total-size %" PRIu64
        " earliest-file-size %" PRIu64,
        cf_name.c_str(), candidate_size, earliest_file_size);
  }
  assert(start_index < sorted_runs.size() - 1);

  // Estimate total file size
  uint64_t estimated_total_size = 0;
  for (size_t loop = start_index; loop < sorted_runs.size(); loop++) {
    estimated_total_size += sorted_runs[loop].size;
  }
  uint32_t path_id = GetPathId(ioptions_, estimated_total_size);
  int start_level = sorted_runs[start_index].level;

  std::vector<CompactionInputFiles> inputs(vstorage->num_levels());
  for (size_t i = 0; i < inputs.size(); ++i) {
    inputs[i].level = start_level + static_cast<int>(i);
  }
  // We always compact all the files, so always compress.
  for (size_t loop = start_index; loop < sorted_runs.size(); loop++) {
    auto& picking_sr = sorted_runs[loop];
    if (picking_sr.level == 0) {
      FileMetaData* f = picking_sr.file;
      inputs[0].files.push_back(f);
    } else {
      auto& files = inputs[picking_sr.level - start_level].files;
      for (auto* f : vstorage->LevelFiles(picking_sr.level)) {
        files.push_back(f);
      }
    }
    char file_num_buf[256];
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: size amp picking %s",
                     cf_name.c_str(), file_num_buf);
  }

  // output files at the bottom most level, unless it's reserved
  int output_level = vstorage->num_levels() - 1;
  // last level is reserved for the files ingested behind
  if (ioptions_.allow_ingest_behind) {
    assert(output_level > 1);
    output_level--;
  }

  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, std::move(inputs),
      output_level, mutable_cf_options.MaxFileSizeForLevel(output_level),
      /* max_grandparent_overlap_bytes */ LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options,
                         output_level, 1),
      /* grandparents */ {}, /* is manual */ false, score,
      false /* deletion_compaction */, false,
      ioptions_.enable_partial_remove, {},
      CompactionReason::kUniversalSizeAmplification);
}
}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
