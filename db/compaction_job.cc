//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction_job.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <algorithm>
#include <functional>
#include <list>
#include <memory>
#include <random>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/version_set.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/block.h"
#include "table/block_based_table_factory.h"
#include "table/iterator_wrapper.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/log_buffer.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/sst_file_manager_impl.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {

struct PartialRemoveInfo {
  // from seek(smallest) to seek_for_prev(largest) has been removed
  InternalKey smallest;
  InternalKey largest;
  bool active = false;
};

// Maintains state for each sub-compaction
struct CompactionJob::SubcompactionState {
  const Compaction* compaction;
  std::unique_ptr<CompactionIterator> c_iter;

  // The boundaries of the key-range this compaction is interested in. No two
  // subcompactions may have overlapping key-ranges.
  // 'start' is inclusive, 'end' is exclusive, and nullptr means unbounded
  Slice *start, *end;

  PartialRemoveInfo partial_remove_info;

  // The return status of this subcompaction
  Status status;

  // Files produced by this subcompaction
  struct Output {
    FileMetaData meta;
    bool finished;
    std::shared_ptr<const TableProperties> table_properties;
  };

  // State kept for output being generated
  std::vector<Output> outputs;
  std::unique_ptr<WritableFileWriter> outfile;
  std::unique_ptr<TableBuilder> builder;
  Output* current_output() {
    if (outputs.empty()) {
      // This subcompaction's outptut could be empty if compaction was aborted
      // before this subcompaction had a chance to generate any output files.
      // When subcompactions are executed sequentially this is more likely and
      // will be particulalry likely for the later subcompactions to be empty.
      // Once they are run in parallel however it should be much rarer.
      return nullptr;
    } else {
      return &outputs.back();
    }
  }

  uint64_t current_output_file_size;

  // State during the subcompaction
  uint64_t total_bytes;
  uint64_t num_input_records;
  uint64_t num_output_records;
  CompactionJobStats compaction_job_stats;
  uint64_t approx_size;
  // An index that used to speed up ShouldStopBefore().
  size_t grandparent_index = 0;
  // The number of bytes overlapping between the current output and
  // grandparent files used in ShouldStopBefore().
  uint64_t overlapped_bytes = 0;
  // A flag determine whether the key has been seen in ShouldStopBefore()
  bool seen_key = false;
  std::string compression_dict;

  SubcompactionState(Compaction* c, Slice* _start, Slice* _end,
                     uint64_t size = 0)
      : compaction(c),
        start(_start),
        end(_end),
        outfile(nullptr),
        builder(nullptr),
        current_output_file_size(0),
        total_bytes(0),
        num_input_records(0),
        num_output_records(0),
        approx_size(size),
        grandparent_index(0),
        overlapped_bytes(0),
        seen_key(false),
        compression_dict() {
    assert(compaction != nullptr);
  }

  SubcompactionState(SubcompactionState&& o) { *this = std::move(o); }

  SubcompactionState& operator=(SubcompactionState&& o) {
    compaction = std::move(o.compaction);
    start = std::move(o.start);
    end = std::move(o.end);
    status = std::move(o.status);
    outputs = std::move(o.outputs);
    outfile = std::move(o.outfile);
    builder = std::move(o.builder);
    current_output_file_size = std::move(o.current_output_file_size);
    total_bytes = std::move(o.total_bytes);
    num_input_records = std::move(o.num_input_records);
    num_output_records = std::move(o.num_output_records);
    compaction_job_stats = std::move(o.compaction_job_stats);
    approx_size = std::move(o.approx_size);
    grandparent_index = std::move(o.grandparent_index);
    overlapped_bytes = std::move(o.overlapped_bytes);
    seen_key = std::move(o.seen_key);
    compression_dict = std::move(o.compression_dict);
    return *this;
  }

  // Because member unique_ptrs do not have these.
  SubcompactionState(const SubcompactionState&) = delete;

  SubcompactionState& operator=(const SubcompactionState&) = delete;

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key, uint64_t curr_file_size) {
    const InternalKeyComparator* icmp =
        &compaction->column_family_data()->internal_comparator();
    const std::vector<FileMetaData*>& grandparents = compaction->grandparents();

    // Scan to find earliest grandparent file that contains key.
    while (grandparent_index < grandparents.size() &&
           icmp->Compare(internal_key,
                         grandparents[grandparent_index]->largest().Encode()) >
               0) {
      if (seen_key) {
        overlapped_bytes += grandparents[grandparent_index]->fd.GetFileSize();
      }
      assert(grandparent_index + 1 >= grandparents.size() ||
             icmp->Compare(
                 grandparents[grandparent_index]->largest().Encode(),
                 grandparents[grandparent_index + 1]->smallest().Encode()) <= 0);
      grandparent_index++;
    }
    seen_key = true;

    if (overlapped_bytes + curr_file_size >
        compaction->max_compaction_bytes()) {
      // Too much overlap for current output; start new output
      overlapped_bytes = 0;
      return true;
    }

    return false;
  }
};

// Maintains state for the entire compaction
struct CompactionJob::CompactionState {
  Compaction* const compaction;

  // REQUIRED: subcompaction states are stored in order of increasing
  // key-range
  std::vector<CompactionJob::SubcompactionState> sub_compact_states;
  Status status;

  uint64_t total_bytes;
  uint64_t num_input_records;
  uint64_t num_output_records;

  explicit CompactionState(Compaction* c)
      : compaction(c),
        total_bytes(0),
        num_input_records(0),
        num_output_records(0) {}

  size_t NumOutputFiles() {
    size_t total = 0;
    for (auto& s : sub_compact_states) {
      total += s.outputs.size();
    }
    return total;
  }

  Slice SmallestUserKey() {
    for (const auto& sub_compact_state : sub_compact_states) {
      if (!sub_compact_state.outputs.empty() &&
          sub_compact_state.outputs[0].finished) {
        return sub_compact_state.outputs[0].meta.smallest().user_key();
      }
    }
    // If there is no finished output, return an empty slice.
    return Slice(nullptr, 0);
  }

  Slice LargestUserKey() {
    for (auto it = sub_compact_states.rbegin(); it < sub_compact_states.rend();
         ++it) {
      if (!it->outputs.empty() && it->current_output()->finished) {
        assert(it->current_output() != nullptr);
        return it->current_output()->meta.largest().user_key();
      }
    }
    // If there is no finished output, return an empty slice.
    return Slice(nullptr, 0);
  }
};

void CompactionJob::AggregateStatistics() {
  for (SubcompactionState& sc : compact_->sub_compact_states) {
    compact_->total_bytes += sc.total_bytes;
    compact_->num_input_records += sc.num_input_records;
    compact_->num_output_records += sc.num_output_records;
  }
  if (compaction_job_stats_) {
    for (SubcompactionState& sc : compact_->sub_compact_states) {
      compaction_job_stats_->Add(sc.compaction_job_stats);
    }
  }
}

CompactionJob::CompactionJob(
    int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
    const EnvOptions env_options, VersionSet* versions,
    const std::atomic<bool>* shutting_down,
    const SequenceNumber preserve_deletes_seqnum, LogBuffer* log_buffer,
    Directory* db_directory, Directory* output_directory, Statistics* stats,
    InstrumentedMutex* db_mutex, Status* db_bg_error,
    std::vector<SequenceNumber> existing_snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    const SnapshotChecker* snapshot_checker, std::shared_ptr<Cache> table_cache,
    EventLogger* event_logger, bool paranoid_file_checks, bool measure_io_stats,
    const std::string& dbname, CompactionJobStats* compaction_job_stats)
    : job_id_(job_id),
      compact_(new CompactionState(compaction)),
      compaction_job_stats_(compaction_job_stats),
      compaction_stats_(1),
      dbname_(dbname),
      db_options_(db_options),
      env_options_(env_options),
      env_(db_options.env),
      env_optiosn_for_read_(
          env_->OptimizeForCompactionTableRead(env_options, db_options_)),
      versions_(versions),
      shutting_down_(shutting_down),
      preserve_deletes_seqnum_(preserve_deletes_seqnum),
      log_buffer_(log_buffer),
      db_directory_(db_directory),
      output_directory_(output_directory),
      stats_(stats),
      db_mutex_(db_mutex),
      db_bg_error_(db_bg_error),
      existing_snapshots_(std::move(existing_snapshots)),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      snapshot_checker_(snapshot_checker),
      table_cache_(std::move(table_cache)),
      event_logger_(event_logger),
      paranoid_file_checks_(paranoid_file_checks),
      measure_io_stats_(measure_io_stats) {
  assert(log_buffer_ != nullptr);
  const auto* cfd = compact_->compaction->column_family_data();
  ThreadStatusUtil::SetColumnFamily(cfd, cfd->ioptions()->env,
                                    db_options_.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);
  ReportStartedCompaction(compaction);
}

CompactionJob::~CompactionJob() {
  assert(compact_ == nullptr);
  ThreadStatusUtil::ResetThreadStatus();
}

void CompactionJob::ReportStartedCompaction(
    Compaction* compaction) {
  const auto* cfd = compact_->compaction->column_family_data();
  ThreadStatusUtil::SetColumnFamily(cfd, cfd->ioptions()->env,
                                    db_options_.enable_thread_tracking);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_JOB_ID,
      job_id_);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_INPUT_OUTPUT_LEVEL,
      (static_cast<uint64_t>(compact_->compaction->start_level()) << 32) +
          compact_->compaction->output_level());

  // In the current design, a CompactionJob is always created
  // for non-trivial compaction.
  assert(compaction->IsTrivialMove() == false ||
         compaction->is_manual_compaction() == true);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_PROP_FLAGS,
      compaction->is_manual_compaction() +
          (compaction->deletion_compaction() << 1));

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_TOTAL_INPUT_BYTES,
      compaction->CalculateTotalInputSize());

  IOSTATS_RESET(bytes_written);
  IOSTATS_RESET(bytes_read);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_WRITTEN, 0);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_READ, 0);

  // Set the thread operation after operation properties
  // to ensure GetThreadList() can always show them all together.
  ThreadStatusUtil::SetThreadOperation(
      ThreadStatus::OP_COMPACTION);

  if (compaction_job_stats_) {
    compaction_job_stats_->is_manual_compaction =
        compaction->is_manual_compaction();
  }
}

void CompactionJob::Prepare() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PREPARE);

  // Generate file_levels_ for compaction berfore making Iterator
  auto* c = compact_->compaction;
  assert(c->column_family_data() != nullptr);
  assert(c->column_family_data()->current()->storage_info()
      ->NumLevelFiles(compact_->compaction->level()) > 0);

  // Is this compaction producing files at the bottommost level?
  bottommost_level_ = c->bottommost_level();

  if (c->input_range().size() >= 2) {
    auto& input_range = c->input_range();
    boundaries_.resize(input_range.size() - 1);
    Slice* start = nullptr;
    for (size_t i = 0; i + 1 < input_range.size(); i++) {
      assert(input_range[i].largest != nullptr);
      Slice& end_storage = boundaries_[i];
      end_storage = input_range[i].largest->user_key();
      compact_->sub_compact_states.emplace_back(c, start, &end_storage);
      start = &end_storage;
    }
    compact_->sub_compact_states.emplace_back(c, start, nullptr);
  } else if (c->ShouldFormSubcompactions()) {
    const uint64_t start_micros = env_->NowMicros();
    GenSubcompactionBoundaries();
    MeasureTime(stats_, SUBCOMPACTION_SETUP_TIME,
                env_->NowMicros() - start_micros);

    assert(sizes_.size() == boundaries_.size() + 1);

    for (size_t i = 0; i <= boundaries_.size(); i++) {
      Slice* start = i == 0 ? nullptr : &boundaries_[i - 1];
      Slice* end = i == boundaries_.size() ? nullptr : &boundaries_[i];
      compact_->sub_compact_states.emplace_back(c, start, end, sizes_[i]);
    }
    MeasureTime(stats_, NUM_SUBCOMPACTIONS_SCHEDULED,
                compact_->sub_compact_states.size());
  } else {
    compact_->sub_compact_states.emplace_back(c, nullptr, nullptr);
  }
}

struct RangeWithSize {
  Range range;
  uint64_t size;

  RangeWithSize(const Slice& a, const Slice& b, uint64_t s = 0)
      : range(a, b), size(s) {}
};

// Generates a histogram representing potential divisions of key ranges from
// the input. It adds the starting and/or ending keys of certain input files
// to the working set and then finds the approximate size of data in between
// each consecutive pair of slices. Then it divides these ranges into
// consecutive groups such that each group has a similar size.
void CompactionJob::GenSubcompactionBoundaries() {
  auto* c = compact_->compaction;
  auto* cfd = c->column_family_data();
  const Comparator* cfd_comparator = cfd->user_comparator();
  std::vector<Slice> bounds;
  int start_lvl = c->start_level();
  int out_lvl = c->output_level();

  // Add the starting and/or ending key of certain input files as a potential
  // boundary
  for (size_t lvl_idx = 0; lvl_idx < c->num_input_levels(); lvl_idx++) {
    int lvl = c->level(lvl_idx);
    if (lvl >= start_lvl && lvl <= out_lvl) {
      const LevelFilesBrief* flevel = c->input_levels(lvl_idx);
      size_t num_files = flevel->num_files;

      if (num_files == 0) {
        continue;
      }

      if (lvl == 0) {
        // For level 0 add the starting and ending key of each file since the
        // files may have greatly differing key ranges (not range-partitioned)
        for (size_t i = 0; i < num_files; i++) {
          bounds.emplace_back(flevel->files[i].smallest_key);
          bounds.emplace_back(flevel->files[i].largest_key);
        }
      } else {
        // For all other levels add the smallest/largest key in the level to
        // encompass the range covered by that level
        bounds.emplace_back(flevel->files[0].smallest_key);
        bounds.emplace_back(flevel->files[num_files - 1].largest_key);
        if (lvl == out_lvl) {
          // For the last level include the starting keys of all files since
          // the last level is the largest and probably has the widest key
          // range. Since it's range partitioned, the ending key of one file
          // and the starting key of the next are very close (or identical).
          for (size_t i = 1; i < num_files; i++) {
            bounds.emplace_back(flevel->files[i].smallest_key);
          }
        }
      }
    }
  }
  if (!c->input_range().empty()) {
    assert(c->input_range().size() == 1);
    auto& input_range = c->input_range().front();
    auto uc = cfd->user_comparator();
    for (auto& bound : bounds) {
      if (input_range.smallest != nullptr &&
          uc->Compare(ExtractUserKey(bound),
                      input_range.smallest->user_key()) < 0) {
        bound = input_range.smallest->Encode();
      }
      if (input_range.largest != nullptr &&
          uc->Compare(ExtractUserKey(bound),
                      input_range.largest->user_key()) > 0) {
        bound = input_range.largest->Encode();
      }
    }
  }

  std::sort(bounds.begin(), bounds.end(),
    [cfd_comparator] (const Slice& a, const Slice& b) -> bool {
      return cfd_comparator->Compare(ExtractUserKey(a), ExtractUserKey(b)) < 0;
    });
  // Remove duplicated entries from bounds
  bounds.erase(std::unique(bounds.begin(), bounds.end(),
    [cfd_comparator] (const Slice& a, const Slice& b) -> bool {
      return cfd_comparator->Compare(ExtractUserKey(a), ExtractUserKey(b)) == 0;
    }), bounds.end());

  // Combine consecutive pairs of boundaries into ranges with an approximate
  // size of data covered by keys in that range
  uint64_t sum = 0;
  std::vector<RangeWithSize> ranges;
  auto* v = cfd->current();
  for (auto it = bounds.begin();;) {
    const Slice a = *it;
    it++;

    if (it == bounds.end()) {
      break;
    }

    const Slice b = *it;
    uint64_t size = versions_->ApproximateSize(v, a, b, start_lvl, out_lvl + 1);
    ranges.emplace_back(a, b, size);
    sum += size;
  }

  // Group the ranges into subcompactions
  const double min_file_fill_percent = 4.0 / 5;
  uint64_t max_output_files = static_cast<uint64_t>(
      std::ceil(sum / min_file_fill_percent /
                c->mutable_cf_options()->MaxFileSizeForLevel(out_lvl)));
  uint64_t subcompactions =
      std::min({static_cast<uint64_t>(ranges.size()),
                static_cast<uint64_t>(db_options_.max_subcompactions),
                max_output_files});

  if (subcompactions > 1) {
    double mean = sum * 1.0 / subcompactions;
    // Greedily add ranges to the subcompaction until the sum of the ranges'
    // sizes becomes >= the expected mean size of a subcompaction
    sum = 0;
    for (size_t i = 0; i < ranges.size() - 1; i++) {
      sum += ranges[i].size;
      if (subcompactions == 1) {
        // If there's only one left to schedule then it goes to the end so no
        // need to put an end boundary
        continue;
      }
      if (sum >= mean) {
        boundaries_.emplace_back(ExtractUserKey(ranges[i].range.limit));
        sizes_.emplace_back(sum);
        subcompactions--;
        sum = 0;
      }
    }
    sizes_.emplace_back(sum + ranges.back().size);
  } else {
    // Only one range so its size is the total sum of sizes computed above
    sizes_.emplace_back(sum);
  }
}

Status CompactionJob::Run() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_RUN);
  TEST_SYNC_POINT("CompactionJob::Run():Start");
  log_buffer_->FlushBufferToLog();
  LogCompaction();

  const size_t num_threads = compact_->sub_compact_states.size();
  assert(num_threads > 0);
  const uint64_t start_micros = env_->NowMicros();

  // Launch a thread for each of subcompactions 1...num_threads-1
  std::vector<port::Thread> thread_pool;
  thread_pool.reserve(num_threads - 1);
  for (size_t i = 1; i < compact_->sub_compact_states.size(); i++) {
    thread_pool.emplace_back(&CompactionJob::ProcessKeyValueCompaction, this,
                             &compact_->sub_compact_states[i]);
  }

  // Always schedule the first subcompaction (whether or not there are also
  // others) in the current thread to be efficient with resources
  ProcessKeyValueCompaction(&compact_->sub_compact_states[0]);

  // Wait for all other threads (if there are any) to finish execution
  for (auto& thread : thread_pool) {
    thread.join();
  }

  if (output_directory_) {
    output_directory_->Fsync();
  }

  compaction_stats_.micros = env_->NowMicros() - start_micros;
  MeasureTime(stats_, COMPACTION_TIME, compaction_stats_.micros);

  // Check if any thread encountered an error during execution
  Status status;
  for (const auto& state : compact_->sub_compact_states) {
    if (!state.status.ok()) {
      status = state.status;
      break;
    }
  }

  TablePropertiesCollection tp;
  for (const auto& state : compact_->sub_compact_states) {
    for (const auto& output : state.outputs) {
      auto fn = TableFileName(db_options_.db_paths, output.meta.fd.GetNumber(),
                              output.meta.fd.GetPathId());
      tp[fn] = output.table_properties;
    }
  }
  compact_->compaction->SetOutputTableProperties(std::move(tp));

  LogFlush(db_options_.info_log);
  TEST_SYNC_POINT("CompactionJob::Run():End");

  compact_->status = status;
  return status;
}

Status CompactionJob::Install(const MutableCFOptions& mutable_cf_options) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_INSTALL);
  db_mutex_->AssertHeld();
  Status status = compact_->status;

  std::unordered_map<uint64_t, int> file_remove;
  if (status.ok()) {
    status = InstallCompactionResults(mutable_cf_options, file_remove);
  }
  // Finish up all book-keeping to unify the subcompaction results
  AggregateStatistics();
  UpdateCompactionStats(file_remove);
  RecordCompactionIOStats();

  ColumnFamilyData* cfd = compact_->compaction->column_family_data();
  cfd->internal_stats()->AddCompactionStats(
    compact_->compaction->output_level(), compaction_stats_);

  VersionStorageInfo::LevelSummaryStorage tmp;
  auto vstorage = cfd->current()->storage_info();
  const auto& stats = compaction_stats_;

  double read_write_amp = 0.0;
  double write_amp = 0.0;
  double bytes_read_per_sec = 0;
  double bytes_written_per_sec = 0;

  if (stats.bytes_read_non_output_levels > 0) {
    read_write_amp = (stats.bytes_written + stats.bytes_read_output_level +
                      stats.bytes_read_non_output_levels) /
                     static_cast<double>(stats.bytes_read_non_output_levels);
    write_amp = stats.bytes_written /
                static_cast<double>(stats.bytes_read_non_output_levels);
  }
  if (stats.micros > 0) {
    bytes_read_per_sec =
        (stats.bytes_read_non_output_levels + stats.bytes_read_output_level) /
        static_cast<double>(stats.micros);
    bytes_written_per_sec =
        stats.bytes_written / static_cast<double>(stats.micros);
  }

  ROCKS_LOG_BUFFER(
      log_buffer_,
      "[%s] compacted to: %s, MB/sec: %.1f rd, %.1f wr, level %d, "
      "files in(%d, %d) out(%d) "
      "MB in(%.1f, %.1f) out(%.1f), read-write-amplify(%.1f) "
      "write-amplify(%.1f) %s, records in: %d, records dropped: %d\n",
      cfd->GetName().c_str(), vstorage->LevelSummary(&tmp), bytes_read_per_sec,
      bytes_written_per_sec, compact_->compaction->output_level(),
      stats.num_input_files_in_non_output_levels,
      stats.num_input_files_in_output_level, stats.num_output_files,
      stats.bytes_read_non_output_levels / 1048576.0,
      stats.bytes_read_output_level / 1048576.0,
      stats.bytes_written / 1048576.0, read_write_amp, write_amp,
      status.ToString().c_str(), stats.num_input_records,
      stats.num_dropped_records);

  UpdateCompactionJobStats(stats);

  auto stream = event_logger_->LogToBuffer(log_buffer_);
  stream << "job" << job_id_
         << "event" << "compaction_finished"
         << "compaction_time_micros" << compaction_stats_.micros
         << "output_level" << compact_->compaction->output_level()
         << "num_output_files" << compact_->NumOutputFiles()
         << "total_output_size" << compact_->total_bytes
         << "num_input_records" << compact_->num_input_records
         << "num_output_records" << compact_->num_output_records
         << "num_subcompactions" << compact_->sub_compact_states.size();

  if (compaction_job_stats_ != nullptr) {
    stream << "num_single_delete_mismatches"
           << compaction_job_stats_->num_single_del_mismatch;
    stream << "num_single_delete_fallthrough"
           << compaction_job_stats_->num_single_del_fallthru;
  }

  if (measure_io_stats_ && compaction_job_stats_ != nullptr) {
    stream << "file_write_nanos" << compaction_job_stats_->file_write_nanos;
    stream << "file_range_sync_nanos"
           << compaction_job_stats_->file_range_sync_nanos;
    stream << "file_fsync_nanos" << compaction_job_stats_->file_fsync_nanos;
    stream << "file_prepare_write_nanos"
           << compaction_job_stats_->file_prepare_write_nanos;
  }

  stream << "lsm_state";
  stream.StartArray();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    stream << vstorage->NumLevelFiles(level);
  }
  stream.EndArray();

  CleanupCompaction();
  return status;
}

void CompactionJob::ProcessKeyValueCompaction(SubcompactionState* sub_compact) {
  assert(sub_compact != nullptr);
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  std::unique_ptr<RangeDelAggregator> range_del_agg(
      new RangeDelAggregator(cfd->internal_comparator(), existing_snapshots_));
  std::unique_ptr<InternalIterator> input(versions_->MakeInputIterator(
      sub_compact->compaction, range_del_agg.get()));

  std::unique_ptr<RangeDelAggregator> range_del_agg2(
      new RangeDelAggregator(cfd->internal_comparator(), existing_snapshots_));
  std::unique_ptr<InternalIterator> input2(versions_->MakeInputIterator(
      sub_compact->compaction, range_del_agg2.get()));

  // apply input_range
  if (!sub_compact->compaction->input_range().empty()) {
    typedef CompactionInputFilesRange::IntervalFlag IntervalFlag;
    auto& input_range = sub_compact->compaction->input_range();
    auto range_set = new std::vector<InternalKey>;
    range_set->resize(2);
    RangeEraseSet erase_set;
    if (input_range.front().smallest != nullptr) {
      range_set->front().SetMinPossibleForUserKey(
          input_range.front().smallest->user_key());
      if (input_range.front().flags & IntervalFlag::kSmallestOpen) {
        InternalKey largest;
        largest.SetMaxPossibleForUserKey(
            input_range.front().smallest->user_key());
        erase_set.push(range_set->front(), largest);
      }
    } else {
      input->SeekToFirst();
      range_set->front().DecodeFrom(input->key());
    }
    if (input_range.size() > 1) {
      for (size_t i = 1; i < input_range.size(); ++i) {
        assert(input_range[i - 1].largest != nullptr);
        assert(input_range[i].smallest != nullptr);
        InternalKey smallest, largest;
        if (input_range[i - 1].flags & IntervalFlag::kLargestOpen) {
          smallest.SetMinPossibleForUserKey(
              input_range[i - 1].largest->user_key());
        } else {
          smallest.SetMaxPossibleForUserKey(
              input_range[i - 1].largest->user_key());
        }
        if (input_range[i].flags & IntervalFlag::kSmallestOpen) {
          largest.SetMaxPossibleForUserKey(
              input_range[i].smallest->user_key());
        } else {
          largest.SetMinPossibleForUserKey(
              input_range[i].smallest->user_key());
        }
        erase_set.push(smallest, largest);
      }
    }
    if (input_range.back().largest != nullptr) {
      range_set->back().SetMaxPossibleForUserKey(
          input_range.back().largest->user_key());
      if (input_range.back().flags & IntervalFlag::kLargestOpen) {
        InternalKey smallest;
        smallest.SetMinPossibleForUserKey(
            input_range.back().largest->user_key());
        erase_set.push(smallest, range_set->back());
      }
    } else {
      input->SeekToLast();
      range_set->back().DecodeFrom(input->key());
    }
    if (!erase_set.erase.empty()) {
      std::vector<InternalKey> new_range_set;
      MergeRangeSet(*range_set, erase_set, new_range_set,
                    cfd->ioptions()->internal_comparator, input.get());
      range_set->swap(new_range_set);
    }
    if (range_set->empty()) {
      delete range_set;
      input.reset(NewEmptyInternalIterator());
      input2.reset(NewEmptyInternalIterator());
    } else {
      auto iter = input.release();
      auto wrapper = NewRangeWrappedInternalIterator(
                         iter, cfd->ioptions()->internal_comparator,
                         range_set, nullptr);
      wrapper->RegisterCleanup([](void* arg1, void* arg2) {
        delete reinterpret_cast<std::vector<InternalKey>*>(arg1);
        delete reinterpret_cast<InternalIterator*>(arg2);
      }, range_set, iter);
      input.reset(wrapper);

      auto iter2 = input2.release();
      auto wrapper2 = NewRangeWrappedInternalIterator(
                          iter2, cfd->ioptions()->internal_comparator,
                          range_set, nullptr);
      wrapper2->RegisterCleanup([](void* arg1, void* arg2) {
        delete reinterpret_cast<InternalIterator*>(arg2);
      }, nullptr, iter2);
      input2.reset(wrapper2);
    }
  }

  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PROCESS_KV);

  // I/O measurement variables
  PerfLevel prev_perf_level = PerfLevel::kEnableTime;
  const uint64_t kRecordStatsEvery = 1000;
  uint64_t prev_write_nanos = 0;
  uint64_t prev_fsync_nanos = 0;
  uint64_t prev_range_sync_nanos = 0;
  uint64_t prev_prepare_write_nanos = 0;
  if (measure_io_stats_) {
    prev_perf_level = GetPerfLevel();
    SetPerfLevel(PerfLevel::kEnableTime);
    prev_write_nanos = IOSTATS(write_nanos);
    prev_fsync_nanos = IOSTATS(fsync_nanos);
    prev_range_sync_nanos = IOSTATS(range_sync_nanos);
    prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
  }

  const MutableCFOptions* mutable_cf_options =
      sub_compact->compaction->mutable_cf_options();

  // To build compression dictionary, we sample the first output file, assuming
  // it'll reach the maximum length, and then use the dictionary for compressing
  // subsequent output files. The dictionary may be less than max_dict_bytes if
  // the first output file's length is less than the maximum.
  const int kSampleLenShift = 6;  // 2^6 = 64-byte samples
  std::set<size_t> sample_begin_offsets;
  if (bottommost_level_ &&
      cfd->ioptions()->compression_opts.max_dict_bytes > 0) {
    const size_t kMaxSamples =
        cfd->ioptions()->compression_opts.max_dict_bytes >> kSampleLenShift;
    const size_t kOutFileLen = mutable_cf_options->MaxFileSizeForLevel(
        compact_->compaction->output_level());
    if (kOutFileLen != port::kMaxSizet) {
      const size_t kOutFileNumSamples = kOutFileLen >> kSampleLenShift;
      Random64 generator{versions_->NewFileNumber()};
      for (size_t i = 0; i < kMaxSamples; ++i) {
        sample_begin_offsets.insert(generator.Uniform(kOutFileNumSamples)
                                    << kSampleLenShift);
      }
    }
  }

  auto compaction_filter = cfd->ioptions()->compaction_filter;
  std::unique_ptr<CompactionFilter> compaction_filter_from_factory = nullptr;
  if (compaction_filter == nullptr) {
    compaction_filter_from_factory =
        sub_compact->compaction->CreateCompactionFilter();
    compaction_filter = compaction_filter_from_factory.get();
  }
  MergeHelper merge(
      env_, cfd->user_comparator(), cfd->ioptions()->merge_operator,
      compaction_filter, db_options_.info_log.get(),
      false /* internal key corruption is expected */,
      existing_snapshots_.empty() ? 0 : existing_snapshots_.back(),
      compact_->compaction->level(), db_options_.statistics.get(),
      shutting_down_);
  MergeHelper merge2(
      env_, cfd->user_comparator(), cfd->ioptions()->merge_operator,
      compaction_filter, db_options_.info_log.get(),
      false /* internal key corruption is expected */,
      existing_snapshots_.empty() ? 0 : existing_snapshots_.back(),
      compact_->compaction->level(), db_options_.statistics.get(),
      shutting_down_);

  TEST_SYNC_POINT("CompactionJob::Run():Inprogress");

  Slice* start = sub_compact->start;
  Slice* end = sub_compact->end;
  auto input_seek_to_first = [&] {
    if (start != nullptr) {
      IterKey start_iter;
      start_iter.SetInternalKey(*start, kMaxSequenceNumber, kValueTypeForSeek);
      input->Seek(start_iter.GetInternalKey());
    } else {
      input->SeekToFirst();
    }
    return input->Valid();
  };
  auto input_seek_to_last = [&] {
    if (end != nullptr) {
      IterKey end_iter;
      end_iter.SetInternalKey(*end, kMaxSequenceNumber, kValueTypeForSeek);
      input->SeekForPrev(end_iter.GetInternalKey());
    } else {
      input->SeekToLast();
    }
    return input->Valid();
  };
  if (compact_->sub_compact_states.size() > 1 &&
      (compact_->compaction->enable_partial_remove() ||
       !compact_->compaction->input_range().empty())) {
    assert(compact_->compaction->output_level() != 0);
    if (input_seek_to_last()) {
      sub_compact->partial_remove_info.largest.DecodeFrom(input->key());
    }
    if (input_seek_to_first()) {
      sub_compact->partial_remove_info.smallest.DecodeFrom(input->key());
    }
    if (sub_compact->partial_remove_info.smallest.size() != 0 &&
        sub_compact->partial_remove_info.largest.size() != 0 &&
        IsCoveredBySingleSST(sub_compact)) {
      input.reset(NewEmptyInternalIterator());
      input2.reset(NewEmptyInternalIterator());
      sub_compact->partial_remove_info.smallest.Clear();
      sub_compact->partial_remove_info.largest.Clear();
    }
  } else {
    if (input_seek_to_first()) {
      sub_compact->partial_remove_info.smallest.DecodeFrom(input->key());
    }
  }

  // we allow only 1 compaction event listener. Used by blob storage
  CompactionEventListener* comp_event_listener = nullptr;
#ifndef ROCKSDB_LITE
  for (auto& celitr : cfd->ioptions()->listeners) {
    comp_event_listener = celitr->GetCompactionEventListener();
    if (comp_event_listener != nullptr) {
      break;
    }
  }
#endif  // ROCKSDB_LITE

  Status status;
  auto makeCompactionIterator = [&](InternalIterator* input_x,
                                    MergeHelper& merge_x,
                                    RangeDelAggregator *range_del_agg_x,
                                    CompactionEventListener* compaction_listener_x
                                    ) {
    return std::unique_ptr<CompactionIterator>(new CompactionIterator(
      input_x, cfd->user_comparator(), &merge_x, versions_->LastSequence(),
      &existing_snapshots_, earliest_write_conflict_snapshot_,
	  snapshot_checker_, env_, false, range_del_agg_x,
	  sub_compact->compaction, compaction_filter,
      compaction_listener_x, shutting_down_, preserve_deletes_seqnum_));
  };
  sub_compact->c_iter = makeCompactionIterator(input.get(), merge,
      range_del_agg.get(), comp_event_listener);
  auto c_iter = sub_compact->c_iter.get();
  c_iter->SeekToFirst();
  auto c_iter2 = makeCompactionIterator(input2.get(), merge2,
      range_del_agg2.get(), nullptr);
  c_iter2->SetFilterSampleInterval(0);
  auto second_pass_iter = c_iter2->AdaptToInternalIterator();
  if (c_iter->Valid() &&
      sub_compact->compaction->output_level() != 0) {
    // ShouldStopBefore() maintains state based on keys processed so far. The
    // compaction loop always calls it on the "next" key, thus won't tell it the
    // first key. So we do that here.
    sub_compact->ShouldStopBefore(
      c_iter->key(), sub_compact->current_output_file_size);
  }
  const auto& c_iter_stats = c_iter->iter_stats();
  auto sample_begin_offset_iter = sample_begin_offsets.cbegin();
  // data_begin_offset and compression_dict are only valid while generating
  // dictionary from the first output file.
  size_t data_begin_offset = 0;
  std::string compression_dict;
  compression_dict.reserve(cfd->ioptions()->compression_opts.max_dict_bytes);

  while (status.ok() && !cfd->IsDropped() && c_iter->Valid()) {
    // Invariant: c_iter.status() is guaranteed to be OK if c_iter->Valid()
    // returns true.
    const Slice& key = c_iter->key();
    const Slice& value = c_iter->value();

    // If an end key (exclusive) is specified, check if the current key is
    // >= than it and exit if it is because the iterator is out of its range
    if (end != nullptr &&
        cfd->user_comparator()->Compare(c_iter->user_key(), *end) >= 0) {
      break;
    }
    if (c_iter_stats.num_input_records % kRecordStatsEvery ==
        kRecordStatsEvery - 1) {
      RecordDroppedKeys(c_iter_stats, &sub_compact->compaction_job_stats);
      c_iter->ResetRecordCounts();
      RecordCompactionIOStats();
    }

    // Open output file if necessary
    if (sub_compact->builder == nullptr) {
      status = OpenCompactionOutputFile(sub_compact);
      if (!status.ok()) {
        break;
      }
      sub_compact->builder->SetSecondPassIterator(second_pass_iter.get());
    }
    assert(sub_compact->builder != nullptr);
    assert(sub_compact->current_output() != nullptr);
    sub_compact->builder->Add(key, value);
    sub_compact->current_output_file_size = sub_compact->builder->FileSize();
    sub_compact->current_output()->meta.UpdateBoundaries(
        key, c_iter->ikey().sequence);
    sub_compact->num_output_records++;

    if (sub_compact->outputs.size() == 1) {  // first output file
      // Check if this key/value overlaps any sample intervals; if so, appends
      // overlapping portions to the dictionary.
      for (const auto& data_elmt : {key, value}) {
        size_t data_end_offset = data_begin_offset + data_elmt.size();
        while (sample_begin_offset_iter != sample_begin_offsets.cend() &&
               *sample_begin_offset_iter < data_end_offset) {
          size_t sample_end_offset =
              *sample_begin_offset_iter + (1 << kSampleLenShift);
          // Invariant: Because we advance sample iterator while processing the
          // data_elmt containing the sample's last byte, the current sample
          // cannot end before the current data_elmt.
          assert(data_begin_offset < sample_end_offset);

          size_t data_elmt_copy_offset, data_elmt_copy_len;
          if (*sample_begin_offset_iter <= data_begin_offset) {
            // The sample starts before data_elmt starts, so take bytes starting
            // at the beginning of data_elmt.
            data_elmt_copy_offset = 0;
          } else {
            // data_elmt starts before the sample starts, so take bytes starting
            // at the below offset into data_elmt.
            data_elmt_copy_offset =
                *sample_begin_offset_iter - data_begin_offset;
          }
          if (sample_end_offset <= data_end_offset) {
            // The sample ends before data_elmt ends, so take as many bytes as
            // needed.
            data_elmt_copy_len =
                sample_end_offset - (data_begin_offset + data_elmt_copy_offset);
          } else {
            // data_elmt ends before the sample ends, so take all remaining
            // bytes in data_elmt.
            data_elmt_copy_len =
                data_end_offset - (data_begin_offset + data_elmt_copy_offset);
          }
          compression_dict.append(&data_elmt.data()[data_elmt_copy_offset],
                                  data_elmt_copy_len);
          if (sample_end_offset > data_end_offset) {
            // Didn't finish sample. Try to finish it with the next data_elmt.
            break;
          }
          // Next sample may require bytes from same data_elmt.
          sample_begin_offset_iter++;
        }
        data_begin_offset = data_end_offset;
      }
    }

    // Close output file if it is big enough. Two possibilities determine it's
    // time to close it: (1) the current key should be this file's last key, (2)
    // the next key should not be in this file.
    //
    // TODO(aekmekji): determine if file should be closed earlier than this
    // during subcompactions (i.e. if output size, estimated by input size, is
    // going to be 1.2MB and max_output_file_size = 1MB, prefer to have 0.6MB
    // and 0.6MB instead of 1MB and 0.2MB)
    bool output_file_ended = false;
    Status input_status;
    if (sub_compact->compaction->output_level() != 0 &&
        sub_compact->current_output_file_size >=
            sub_compact->compaction->max_output_file_size()) {
      // (1) this key terminates the file. For historical reasons, the iterator
      // status before advancing will be given to FinishCompactionOutputFile().
      input_status = input->status();
      output_file_ended = true;
    }
    c_iter->Next();
    if (!output_file_ended && c_iter->Valid() &&
        sub_compact->compaction->output_level() != 0 &&
        sub_compact->ShouldStopBefore(
          c_iter->key(), sub_compact->current_output_file_size) &&
        sub_compact->builder != nullptr) {
      // (2) this key belongs to the next file. For historical reasons, the
      // iterator status after advancing will be given to
      // FinishCompactionOutputFile().
      input_status = input->status();
      output_file_ended = true;
    }
    const Slice* next_key = nullptr;
    if (output_file_ended) {
      if (c_iter->Valid()) {
        next_key = &c_iter->key();
      }
      if (sub_compact->compaction->enable_partial_remove()) {
        if (next_key != nullptr &&
            ExtractUserKey(*next_key) !=
                sub_compact->outputs.back().meta.largest().user_key()) {
          sub_compact->partial_remove_info.largest.Clear();
          sub_compact->partial_remove_info.largest.SetMinPossibleForUserKey(
              ExtractUserKey(*next_key));
          if (IsCoverAnyInputSST(sub_compact)) {
            sub_compact->partial_remove_info.active = true;
            break;
          }
        } else {
          output_file_ended = false;
        }
      }
    }
    if (output_file_ended) {
      CompactionIterationStats range_del_out_stats;
      status = FinishCompactionOutputFile(input_status, sub_compact,
                                          range_del_agg.get(),
                                          &range_del_out_stats, next_key);
      RecordDroppedKeys(range_del_out_stats,
                        &sub_compact->compaction_job_stats);
      if (sub_compact->outputs.size() == 1) {
        // Use dictionary from first output file for compression of subsequent
        // files.
        sub_compact->compression_dict = std::move(compression_dict);
      }
    }
  }

  sub_compact->num_input_records = c_iter_stats.num_input_records;
  sub_compact->compaction_job_stats.num_input_deletion_records =
      c_iter_stats.num_input_deletion_records;
  sub_compact->compaction_job_stats.num_corrupt_keys =
      c_iter_stats.num_input_corrupt_records;
  sub_compact->compaction_job_stats.num_single_del_fallthru =
      c_iter_stats.num_single_del_fallthru;
  sub_compact->compaction_job_stats.num_single_del_mismatch =
      c_iter_stats.num_single_del_mismatch;
  sub_compact->compaction_job_stats.total_input_raw_key_bytes +=
      c_iter_stats.total_input_raw_key_bytes;
  sub_compact->compaction_job_stats.total_input_raw_value_bytes +=
      c_iter_stats.total_input_raw_value_bytes;

  RecordTick(stats_, FILTER_OPERATION_TOTAL_TIME,
             c_iter_stats.total_filter_time);
  RecordDroppedKeys(c_iter_stats, &sub_compact->compaction_job_stats);
  RecordCompactionIOStats();

  if (status.ok() && (shutting_down_->load(std::memory_order_relaxed) ||
                      cfd->IsDropped())) {
    status = Status::ShutdownInProgress(
        "Database shutdown or Column family drop during compaction");
  }
  if (status.ok()) {
    status = input->status();
  }
  if (status.ok()) {
    status = c_iter->status();
  }

  if (status.ok() && sub_compact->builder == nullptr &&
      sub_compact->outputs.size() == 0 &&
      range_del_agg->ShouldAddTombstones(bottommost_level_)) {
    // handle subcompaction containing only range deletions
    status = OpenCompactionOutputFile(sub_compact);
  }

  // Call FinishCompactionOutputFile() even if status is not ok: it needs to
  // close the output file.
  if (sub_compact->builder != nullptr) {
    CompactionIterationStats range_del_out_stats;
    Status s = FinishCompactionOutputFile(
        status, sub_compact, range_del_agg.get(), &range_del_out_stats);
    if (status.ok()) {
      status = s;
    }
    RecordDroppedKeys(range_del_out_stats, &sub_compact->compaction_job_stats);
  }
  if (!sub_compact->partial_remove_info.active) {
    if (input_seek_to_last()) {
      sub_compact->partial_remove_info.largest.DecodeFrom(input->key());
    }
  }

  if (measure_io_stats_) {
    sub_compact->compaction_job_stats.file_write_nanos +=
        IOSTATS(write_nanos) - prev_write_nanos;
    sub_compact->compaction_job_stats.file_fsync_nanos +=
        IOSTATS(fsync_nanos) - prev_fsync_nanos;
    sub_compact->compaction_job_stats.file_range_sync_nanos +=
        IOSTATS(range_sync_nanos) - prev_range_sync_nanos;
    sub_compact->compaction_job_stats.file_prepare_write_nanos +=
        IOSTATS(prepare_write_nanos) - prev_prepare_write_nanos;
    if (prev_perf_level != PerfLevel::kEnableTime) {
      SetPerfLevel(prev_perf_level);
    }
  }

  sub_compact->c_iter.reset();
  input.reset();
  sub_compact->status = status;
}

void CompactionJob::RecordDroppedKeys(
    const CompactionIterationStats& c_iter_stats,
    CompactionJobStats* compaction_job_stats) {
  if (c_iter_stats.num_record_drop_user > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_USER,
               c_iter_stats.num_record_drop_user);
  }
  if (c_iter_stats.num_record_drop_hidden > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_NEWER_ENTRY,
               c_iter_stats.num_record_drop_hidden);
    if (compaction_job_stats) {
      compaction_job_stats->num_records_replaced +=
          c_iter_stats.num_record_drop_hidden;
    }
  }
  if (c_iter_stats.num_record_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_OBSOLETE,
               c_iter_stats.num_record_drop_obsolete);
    if (compaction_job_stats) {
      compaction_job_stats->num_expired_deletion_records +=
          c_iter_stats.num_record_drop_obsolete;
    }
  }
  if (c_iter_stats.num_record_drop_range_del > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_RANGE_DEL,
               c_iter_stats.num_record_drop_range_del);
  }
  if (c_iter_stats.num_range_del_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_RANGE_DEL_DROP_OBSOLETE,
               c_iter_stats.num_range_del_drop_obsolete);
  }
  if (c_iter_stats.num_optimized_del_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,
               c_iter_stats.num_optimized_del_drop_obsolete);
  }
}

Status CompactionJob::FinishCompactionOutputFile(
    const Status& input_status, SubcompactionState* sub_compact,
    RangeDelAggregator* range_del_agg,
    CompactionIterationStats* range_del_out_stats,
    const Slice* next_table_min_key /* = nullptr */) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_SYNC_FILE);
  assert(sub_compact != nullptr);
  assert(sub_compact->outfile);
  assert(sub_compact->builder != nullptr);
  assert(sub_compact->current_output() != nullptr);

  uint64_t output_number = sub_compact->current_output()->meta.fd.GetNumber();
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input_status;
  auto meta = &sub_compact->current_output()->meta;
  if (s.ok()) {
    Slice lower_bound_guard, upper_bound_guard;
    std::string smallest_user_key;
    const Slice *lower_bound, *upper_bound;
    if (sub_compact->outputs.size() == 1) {
      // For the first output table, include range tombstones before the min key
      // but after the subcompaction boundary.
      lower_bound = sub_compact->start;
    } else if (meta->smallest().size() > 0) {
      // For subsequent output tables, only include range tombstones from min
      // key onwards since the previous file was extended to contain range
      // tombstones falling before min key.
      smallest_user_key = meta->smallest().user_key().ToString(false /*hex*/);
      lower_bound_guard = Slice(smallest_user_key);
      lower_bound = &lower_bound_guard;
    } else {
      lower_bound = nullptr;
    }
    if (next_table_min_key != nullptr) {
      // This isn't the last file in the subcompaction, so extend until the next
      // file starts.
      upper_bound_guard = ExtractUserKey(*next_table_min_key);
      upper_bound = &upper_bound_guard;
    } else {
      // This is the last file in the subcompaction, so extend until the
      // subcompaction ends.
      upper_bound = sub_compact->end;
    }
    range_del_agg->AddToBuilder(sub_compact->builder.get(), lower_bound,
                                upper_bound, meta, range_del_out_stats,
                                bottommost_level_);
    meta->marked_for_compaction = sub_compact->builder->NeedCompact();
  }
  const uint64_t current_entries = sub_compact->builder->NumEntries();
  if (s.ok()) {
    s = sub_compact->builder->Finish();
  } else {
    sub_compact->builder->Abandon();
  }
  const uint64_t current_bytes = sub_compact->builder->FileSize();
  if (s.ok()) {
    meta->fd.file_size = current_bytes;
  }
  sub_compact->current_output()->finished = true;
  sub_compact->total_bytes += current_bytes;

  // Finish and check for file errors
  if (s.ok()) {
    StopWatch sw(env_, stats_, COMPACTION_OUTFILE_SYNC_MICROS);
    s = sub_compact->outfile->Sync(db_options_.use_fsync);
  }
  if (s.ok()) {
    s = sub_compact->outfile->Close();
  }
  sub_compact->outfile.reset();

  if (s.ok() && current_entries == 0) {
    // If there is nothing to output, no necessary to generate a sst file.
    // This happens when the output level is bottom level, at the same time
    // the sub_compact output nothing.
    std::string fname = TableFileName(
        db_options_.db_paths, meta->fd.GetNumber(), meta->fd.GetPathId());
    env_->DeleteFile(fname);

    // Also need to remove the file from outputs, or it will be added to the
    // VersionEdit.
    assert(!sub_compact->outputs.empty());
    sub_compact->outputs.pop_back();
    sub_compact->builder.reset();
    sub_compact->current_output_file_size = 0;
    return s;
  }

  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  TableProperties tp;
  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    // We set for_compaction to false and don't OptimizeForCompactionTableRead
    // here because this is a special case after we finish the table building
    // No matter whether use_direct_io_for_flush_and_compaction is true,
    // we will regrad this verification as user reads since the goal is
    // to cache it here for further user reads
    InternalIterator* iter = cfd->table_cache()->NewIterator(
        ReadOptions(), env_options_, cfd->internal_comparator(), *meta,
        nullptr /* range_del_agg */, nullptr,
        cfd->internal_stats()->GetFileReadHist(
            compact_->compaction->output_level()),
        false, nullptr /* arena */, false /* skip_filters */,
        compact_->compaction->output_level());
    s = iter->status();

    if (s.ok() && paranoid_file_checks_) {
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {}
      s = iter->status();
    }

    delete iter;

    // Output to event logger and fire events.
    if (s.ok()) {
      tp = sub_compact->builder->GetTableProperties();
      sub_compact->current_output()->table_properties =
          std::make_shared<TableProperties>(tp);
      ROCKS_LOG_INFO(db_options_.info_log,
                     "[%s] [JOB %d] Generated table #%" PRIu64 ": %" PRIu64
                     " keys, %" PRIu64 " bytes%s",
                     cfd->GetName().c_str(), job_id_, output_number,
                     current_entries, current_bytes,
                     meta->marked_for_compaction ? " (need compaction)" : "");
    }
  }
  std::string fname;
  FileDescriptor output_fd;
  if (meta != nullptr) {
    fname = TableFileName(db_options_.db_paths, meta->fd.GetNumber(),
                          meta->fd.GetPathId());
    output_fd = meta->fd;
  } else {
    fname = "(nil)";
  }
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger_, cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname,
      job_id_, output_fd, tp, TableFileCreationReason::kCompaction, s);

#ifndef ROCKSDB_LITE
  // Report new file to SstFileManagerImpl
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());
  if (sfm && meta != nullptr && meta->fd.GetPathId() == 0) {
    auto fn = TableFileName(cfd->ioptions()->db_paths, meta->fd.GetNumber(),
                            meta->fd.GetPathId());
    sfm->OnAddFile(fn);
    if (sfm->IsMaxAllowedSpaceReached()) {
      // TODO(ajkr): should we return OK() if max space was reached by the final
      // compaction output file (similarly to how flush works when full)?
      s = Status::IOError("Max allowed space was reached");
      TEST_SYNC_POINT(
          "CompactionJob::FinishCompactionOutputFile:"
          "MaxAllowedSpaceReached");
      InstrumentedMutexLock l(db_mutex_);
      if (db_bg_error_->ok()) {
        Status new_bg_error = s;
        // may temporarily unlock and lock the mutex.
        EventHelpers::NotifyOnBackgroundError(
            cfd->ioptions()->listeners, BackgroundErrorReason::kCompaction,
            &new_bg_error, db_mutex_);
        if (!new_bg_error.ok()) {
          *db_bg_error_ = new_bg_error;
        }
      }
    }
  }
#endif

  sub_compact->builder.reset();
  sub_compact->current_output_file_size = 0;
  return s;
}


bool CompactionJob::IsCoveredBySingleSST(SubcompactionState* sub_compact) {
  auto& smallest = sub_compact->partial_remove_info.smallest;
  auto& largest = sub_compact->partial_remove_info.largest;
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  auto& ic = cfd->internal_comparator();
  int output_level = compact_->compaction->output_level();
  if (output_level == 0) {
    return false;
  }
  for (auto& level : *compact_->compaction->inputs()) {
    if (level.level != output_level || level.files.empty()) {
      continue;
    }
    auto overlap = FindLevelOverlap(level.files, ic, &smallest, &largest);
    if (overlap.first == overlap.second) {
      auto file = level.files[overlap.first];
      if (ic.Compare(smallest, file->smallest()) > 0 &&
          ic.Compare(largest, file->largest()) < 0) {
        return true;
      }
    }
    break;
  }
  return false;
}

bool CompactionJob::IsCoverAnyInputSST(SubcompactionState* sub_compact) {
  auto& smallest = sub_compact->partial_remove_info.smallest;
  auto& largest = sub_compact->partial_remove_info.largest;
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  auto& ic = cfd->internal_comparator();
  int output_level = compact_->compaction->output_level();
  if (output_level == 0) {
    // ignore level 0
    return false;
  }
  bool found = false;
  for (auto& level : *compact_->compaction->inputs()) {
    if (level.level == 0 || level.files.empty()) {
      continue;
    }
    auto overlap = FindLevelOverlap(level.files, ic, &smallest, &largest);
    if (overlap.first > overlap.second) {
      // none overlap
      continue;
    }
    else if (level.level == output_level && overlap.first == overlap.second) {
      // Make sure current output not full covered by single optput level sst
      auto file = level.files[overlap.first];
      if (ic.Compare(smallest, file->smallest()) > 0 &&
          ic.Compare(largest, file->largest()) < 0) {
        // output sst can't partial remove in middle ...
        return false;
      }
    }
    if (ic.Compare(level.files[overlap.first]->smallest(), smallest) < 0) {
      // can't cover left
      ++overlap.first;
    }
    if (ic.Compare(level.files[overlap.second]->largest(), largest) > 0) {
      // can't cover right
      --overlap.second;
    }
    if (overlap.first <= overlap.second) {
      found = true;
    }
  }
  return found;
}

Status CompactionJob::InstallCompactionResults(
    const MutableCFOptions& mutable_cf_options,
    std::unordered_map<uint64_t, int>& file_remove) {
  db_mutex_->AssertHeld();

  auto* compaction = compact_->compaction;
  // paranoia: verify that the files that we started with
  // still exist in the current version and in the same original level.
  // This ensures that a concurrent compaction did not erroneously
  // pick the same files to compact_.
  if (!versions_->VerifyCompactionFileConsistency(compaction)) {
    Compaction::InputLevelSummaryBuffer inputs_summary;

    ROCKS_LOG_ERROR(db_options_.info_log, "[%s] [JOB %d] Compaction %s aborted",
                    compaction->column_family_data()->GetName().c_str(),
                    job_id_, compaction->InputLevelSummary(&inputs_summary));
    return Status::Corruption("Compaction input files inconsistent");
  }

  {
    Compaction::InputLevelSummaryBuffer inputs_summary;
    ROCKS_LOG_INFO(
        db_options_.info_log, "[%s] [JOB %d] Compacted  %s => %" PRIu64 " bytes",
        compaction->column_family_data()->GetName().c_str(), job_id_,
        compaction->InputLevelSummary(&inputs_summary), compact_->total_bytes);
  }

  // collect partial remove info
  if (!compaction->input_range().empty() ||
      std::find_if(compact_->sub_compact_states.begin(),
                   compact_->sub_compact_states.end(),
                   [](const CompactionJob::SubcompactionState& s) {
                       return s.partial_remove_info.active;
                   }) != compact_->sub_compact_states.end()) {
    RangeEraseSet erase_set;
    for (const auto& s : compact_->sub_compact_states) {
      if (s.partial_remove_info.smallest.size() != 0 &&
          s.partial_remove_info.largest.size() != 0) {
        erase_set.push(s.partial_remove_info.smallest,
                       s.partial_remove_info.largest);
      } else {
        // this sub compact empty ...
        assert(s.partial_remove_info.smallest.size() == 0);
        assert(s.partial_remove_info.largest.size() == 0);
      }
    }
    // reclaim input sst
    auto inputs = compaction->inputs();
    uint8_t compact_to_level = 0;
    if (compaction->enable_partial_remove()) {
      assert(compaction->output_level() <= std::numeric_limits<uint8_t>::max());
      compact_to_level = (uint8_t)compaction->output_level();
    }
    for (auto& level : *inputs) {
      for (auto& file : level.files) {
        PartialRemovedMetaData meta;
        bool changed = meta.InitFrom(file, erase_set, compact_to_level,
                                     compaction->column_family_data(),
                                     env_optiosn_for_read_);
        if (compact_to_level == 0 && !changed) {
          // unchanged sst
          file_remove.emplace(file->fd.GetNumber(), 0);
          continue;
        }
        // should delete first
        compaction->edit()->DeleteFile(level.level, file->fd.GetNumber());
        // then reclaim
        if (!meta.range_set.empty()) {
          compaction->edit()->AddFile(level.level, meta.Get());
        }
        assert(meta.partial_removed >= file->partial_removed);
        file_remove.emplace(file->fd.GetNumber(),
                            meta.partial_removed - file->partial_removed);
      }
    }
  } else {
    compaction->AddInputDeletions(compact_->compaction->edit());
  }
  // Add compaction outputs
  for (const auto& sub_compact : compact_->sub_compact_states) {
    for (const auto& out : sub_compact.outputs) {
      compaction->edit()->AddFile(compaction->output_level(), out.meta);
    }
  }
  return versions_->LogAndApply(compaction->column_family_data(),
                                mutable_cf_options, compaction->edit(),
                                db_mutex_, db_directory_);
}

void CompactionJob::RecordCompactionIOStats() {
  RecordTick(stats_, COMPACT_READ_BYTES, IOSTATS(bytes_read));
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_READ, IOSTATS(bytes_read));
  IOSTATS_RESET(bytes_read);
  RecordTick(stats_, COMPACT_WRITE_BYTES, IOSTATS(bytes_written));
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_WRITTEN, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}

Status CompactionJob::OpenCompactionOutputFile(
    SubcompactionState* sub_compact) {
  assert(sub_compact != nullptr);
  assert(sub_compact->builder == nullptr);
  // no need to lock because VersionSet::next_file_number_ is atomic
  uint64_t file_number = versions_->NewFileNumber();
  std::string fname = TableFileName(db_options_.db_paths, file_number,
                                    sub_compact->compaction->output_path_id());
  // Fire events.
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      cfd->ioptions()->listeners, dbname_, cfd->GetName(), fname, job_id_,
      TableFileCreationReason::kCompaction);
#endif  // !ROCKSDB_LITE
  // Make the output file
  unique_ptr<WritableFile> writable_file;
#ifndef NDEBUG
  bool syncpoint_arg = env_options_.use_direct_writes;
  TEST_SYNC_POINT_CALLBACK("CompactionJob::OpenCompactionOutputFile",
                           &syncpoint_arg);
#endif
  Status s = NewWritableFile(env_, fname, &writable_file, env_options_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "[%s] [JOB %d] OpenCompactionOutputFiles for table #%" PRIu64
        " fails at NewWritableFile with status %s",
        sub_compact->compaction->column_family_data()->GetName().c_str(),
        job_id_, file_number, s.ToString().c_str());
    LogFlush(db_options_.info_log);
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        event_logger_, cfd->ioptions()->listeners, dbname_, cfd->GetName(),
        fname, job_id_, FileDescriptor(), TableProperties(),
        TableFileCreationReason::kCompaction, s);
    return s;
  }

  SubcompactionState::Output out;
  out.meta.fd =
      FileDescriptor(file_number, sub_compact->compaction->output_path_id(), 0);
  out.finished = false;

  sub_compact->outputs.push_back(out);
  writable_file->SetIOPriority(Env::IO_LOW);
  writable_file->SetPreallocationBlockSize(static_cast<size_t>(
      sub_compact->compaction->OutputFilePreallocationSize()));
  sub_compact->outfile.reset(new WritableFileWriter(
      std::move(writable_file), env_options_, db_options_.statistics.get()));

  // If the Column family flag is to only optimize filters for hits,
  // we can skip creating filters if this is the bottommost_level where
  // data is going to be found
  bool skip_filters =
      cfd->ioptions()->optimize_filters_for_hits && bottommost_level_;

  uint64_t output_file_creation_time =
      sub_compact->compaction->MaxInputFileCreationTime();
  if (output_file_creation_time == 0) {
    int64_t _current_time = 0;
    db_options_.env->GetCurrentTime(&_current_time);  // ignore error
    output_file_creation_time = static_cast<uint64_t>(_current_time);
  }

  sub_compact->builder.reset(NewTableBuilder(
      *cfd->ioptions(), cfd->internal_comparator(),
      cfd->int_tbl_prop_collector_factories(), cfd->GetID(), cfd->GetName(),
      sub_compact->outfile.get(), sub_compact->compaction->output_compression(),
      cfd->ioptions()->compression_opts,
      sub_compact->compaction->output_level(), &sub_compact->compression_dict,
      skip_filters, output_file_creation_time));
  LogFlush(db_options_.info_log);
  return s;
}

void CompactionJob::CleanupCompaction() {
  for (SubcompactionState& sub_compact : compact_->sub_compact_states) {
    const auto& sub_status = sub_compact.status;

    if (sub_compact.builder != nullptr) {
      // May happen if we get a shutdown call in the middle of compaction
      sub_compact.builder->Abandon();
      sub_compact.builder.reset();
    } else {
      assert(!sub_status.ok() || sub_compact.outfile == nullptr);
    }
    for (const auto& out : sub_compact.outputs) {
      // If this file was inserted into the table cache then remove
      // them here because this compaction was not committed.
      if (!sub_status.ok()) {
        TableCache::Evict(table_cache_.get(), out.meta.fd.GetNumber());
      }
    }
  }
  compact_->compaction->set_is_finished(true);
  delete compact_;
  compact_ = nullptr;
}

#ifndef ROCKSDB_LITE
namespace {
void CopyPrefix(
    const Slice& src, size_t prefix_length, std::string* dst) {
  assert(prefix_length > 0);
  size_t length = src.size() > prefix_length ? prefix_length : src.size();
  dst->assign(src.data(), length);
}
}  // namespace

#endif  // !ROCKSDB_LITE

void CompactionJob::UpdateCompactionStats(
    const std::unordered_map<uint64_t, int>& file_remove) {
  Compaction* compaction = compact_->compaction;
  compaction_stats_.num_input_files_in_non_output_levels = 0;
  compaction_stats_.num_input_files_in_output_level = 0;
  for (int input_level = 0;
       input_level < static_cast<int>(compaction->num_input_levels());
       ++input_level) {
    if (compaction->level(input_level) != compaction->output_level()) {
      UpdateCompactionInputStatsHelper(
          &compaction_stats_.num_input_files_in_non_output_levels,
          &compaction_stats_.bytes_read_non_output_levels,
          input_level, file_remove);
    } else {
      UpdateCompactionInputStatsHelper(
          &compaction_stats_.num_input_files_in_output_level,
          &compaction_stats_.bytes_read_output_level,
          input_level, file_remove);
    }
  }

  for (const auto& sub_compact : compact_->sub_compact_states) {
    size_t num_output_files = sub_compact.outputs.size();
    if (sub_compact.builder != nullptr) {
      // An error occurred so ignore the last output.
      assert(num_output_files > 0);
      --num_output_files;
    }
    compaction_stats_.num_output_files += static_cast<int>(num_output_files);

    for (const auto& out : sub_compact.outputs) {
      compaction_stats_.bytes_written += out.meta.fd.file_size;
    }
    if (sub_compact.num_input_records > sub_compact.num_output_records) {
      compaction_stats_.num_dropped_records +=
          sub_compact.num_input_records - sub_compact.num_output_records;
    }
  }
}

void CompactionJob::UpdateCompactionInputStatsHelper(
    int* num_files, uint64_t* bytes_read, int input_level,
    const std::unordered_map<uint64_t, int>& file_remove) {
  const Compaction* compaction = compact_->compaction;
  auto num_input_files = compaction->num_input_files(input_level);

  int num_files_add = 0;
  for (size_t i = 0; i < num_input_files; ++i) {
    const auto* file_meta = compaction->input(input_level, i);
    int ratio = kPartialRemovedMax;
    if (!file_remove.empty()) {
      auto find = file_remove.find(file_meta->fd.GetNumber());
      assert(find != file_remove.end());
      ratio = find->second;
    }
    *bytes_read += file_meta->fd.GetFileSize() * ratio / kPartialRemovedMax;
    compaction_stats_.num_input_records +=
        static_cast<uint64_t>(file_meta->num_entries) *
            ratio / kPartialRemovedMax;
    if (ratio > 0) {
      ++num_files_add;
    }
  }
  *num_files += num_files_add;
}

void CompactionJob::UpdateCompactionJobStats(
    const InternalStats::CompactionStats& stats) const {
#ifndef ROCKSDB_LITE
  if (compaction_job_stats_) {
    compaction_job_stats_->elapsed_micros = stats.micros;

    // input information
    compaction_job_stats_->total_input_bytes =
        stats.bytes_read_non_output_levels +
        stats.bytes_read_output_level;
    compaction_job_stats_->num_input_records =
        compact_->num_input_records;
    compaction_job_stats_->num_input_files =
        stats.num_input_files_in_non_output_levels +
        stats.num_input_files_in_output_level;
    compaction_job_stats_->num_input_files_at_output_level =
        stats.num_input_files_in_output_level;

    // output information
    compaction_job_stats_->total_output_bytes = stats.bytes_written;
    compaction_job_stats_->num_output_records =
        compact_->num_output_records;
    compaction_job_stats_->num_output_files = stats.num_output_files;

    if (compact_->NumOutputFiles() > 0U) {
      CopyPrefix(
          compact_->SmallestUserKey(),
          CompactionJobStats::kMaxPrefixLength,
          &compaction_job_stats_->smallest_output_key_prefix);
      CopyPrefix(
          compact_->LargestUserKey(),
          CompactionJobStats::kMaxPrefixLength,
          &compaction_job_stats_->largest_output_key_prefix);
    }
  }
#endif  // !ROCKSDB_LITE
}

void CompactionJob::LogCompaction() {
  Compaction* compaction = compact_->compaction;
  ColumnFamilyData* cfd = compaction->column_family_data();

  // Let's check if anything will get logged. Don't prepare all the info if
  // we're not logging
  if (db_options_.info_log_level <= InfoLogLevel::INFO_LEVEL) {
    Compaction::InputLevelSummaryBuffer inputs_summary;
    ROCKS_LOG_INFO(
        db_options_.info_log, "[%s] [JOB %d] Compacting %s, score %.2f",
        cfd->GetName().c_str(), job_id_,
        compaction->InputLevelSummary(&inputs_summary), compaction->score());
    char scratch[2345];
    compaction->Summary(scratch, sizeof(scratch));
    ROCKS_LOG_INFO(db_options_.info_log, "[%s] Compaction start summary: %s\n",
                   cfd->GetName().c_str(), scratch);
    // build event logger report
    auto stream = event_logger_->Log();
    stream << "job" << job_id_ << "event"
           << "compaction_started";
    for (size_t i = 0; i < compaction->num_input_levels(); ++i) {
      stream << ("files_L" + ToString(compaction->level(i)));
      stream.StartArray();
      for (auto f : *compaction->inputs(i)) {
        stream << f->fd.GetNumber();
      }
      stream.EndArray();
    }
    stream << "score" << compaction->score() << "input_data_size"
           << compaction->CalculateTotalInputSize();
  }
}

}  // namespace rocksdb
