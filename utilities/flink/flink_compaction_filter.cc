// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/flink/flink_compaction_filter.h"

#include <algorithm>
#include <cinttypes>

#include <port/port.h>

namespace ROCKSDB_NAMESPACE {
namespace flink {

static inline
int64_t DeserializeTimestamp(const char* src, std::size_t offset) {
#if 0
  uint64_t result = 0;
  for (unsigned long i = 0; i < sizeof(uint64_t); i++) {
    result |= static_cast<uint64_t>(static_cast<unsigned char>(src[offset + i]))
              << ((sizeof(int64_t) - 1 - i) * BITS_PER_BYTE);
  }
  return static_cast<int64_t>(result);
#else
  return NativeOfBigEndian64(unaligned_load<uint64_t>(src + offset));
#endif
}

CompactionFilter::Decision Decide(const char* ts_bytes, const int64_t ttl,
                                  const std::size_t timestamp_offset,
                                  const int64_t current_timestamp,
                                  const std::shared_ptr<Logger>& logger) {
  int64_t timestamp = DeserializeTimestamp(ts_bytes, timestamp_offset);
  const int64_t ttlWithoutOverflow =
      timestamp > 0 ? std::min(JAVA_MAX_LONG - timestamp, ttl) : ttl;
  Debug(logger.get(),
        "Last access timestamp: %" PRId64 " ms, ttlWithoutOverflow: %" PRId64
        " ms, Current timestamp: %" PRId64 " ms",
        timestamp, ttlWithoutOverflow, current_timestamp);
  return timestamp + ttlWithoutOverflow <= current_timestamp
             ? CompactionFilter::Decision::kRemove
             : CompactionFilter::Decision::kKeep;
}

FlinkCompactionFilter::ConfigHolder::ConfigHolder()
    : config_(const_cast<FlinkCompactionFilter::Config*>(&DISABLED_CONFIG)){};

FlinkCompactionFilter::ConfigHolder::~ConfigHolder() {
  Config* config = config_.load();
  if (config != &DISABLED_CONFIG) {
    delete config;
  }
}

// at the moment Flink configures filters (can be already created) only once
// when user creates state otherwise it can lead to ListElementFilter leak in
// Config or race between its delete in Configure() and usage in FilterV2() the
// method returns true if it was configured before
bool FlinkCompactionFilter::ConfigHolder::Configure(Config* config) {
  bool not_configured = GetConfig() == &DISABLED_CONFIG;
  if (not_configured) {
    assert(config->query_time_after_num_entries_ >= 0);
    config_ = config;
  }
  return not_configured;
}

FlinkCompactionFilter::Config*
FlinkCompactionFilter::ConfigHolder::GetConfig() {
  return config_.load();
}

std::size_t FlinkCompactionFilter::FixedListElementFilter::NextUnexpiredOffset(
    const Slice& list, int64_t ttl, int64_t current_timestamp) const {
  std::size_t offset = 0;
  while (offset < list.size()) {
    Decision decision = Decide(list.data(), ttl, offset + timestamp_offset_,
                               current_timestamp, logger_);
    if (decision != Decision::kKeep) {
      std::size_t new_offset = offset + fixed_size_;
      if (new_offset >= JAVA_MAX_SIZE || new_offset < offset) {
        return JAVA_MAX_SIZE;
      }
      offset = new_offset;
    } else {
      break;
    }
  }
  return offset;
}

const char* FlinkCompactionFilter::Name() const {
  return "FlinkCompactionFilter";
}

FlinkCompactionFilter::FlinkCompactionFilter(
    std::shared_ptr<ConfigHolder> config_holder,
    std::unique_ptr<TimeProvider> time_provider)
    : FlinkCompactionFilter(std::move(config_holder), std::move(time_provider),
                            nullptr){};

FlinkCompactionFilter::FlinkCompactionFilter(
    std::shared_ptr<ConfigHolder> config_holder,
    std::unique_ptr<TimeProvider> time_provider, std::shared_ptr<Logger> logger)
    : config_holder_(std::move(config_holder)),
      time_provider_(std::move(time_provider)),
      logger_(std::move(logger)),
      config_cached_(const_cast<Config*>(&DISABLED_CONFIG)){};

inline void FlinkCompactionFilter::InitConfigIfNotYet() const {
  const_cast<FlinkCompactionFilter*>(this)->config_cached_ =
      config_cached_ == &DISABLED_CONFIG ? config_holder_->GetConfig()
                                         : config_cached_;
}

CompactionFilter::Decision FlinkCompactionFilter::FilterV2(
    int /*level*/, const Slice& key, ValueType value_type,
    const Slice& existing_value, std::string* new_value,
    std::string* /*skip_until*/) const {
  InitConfigIfNotYet();
  CreateListElementFilterIfNull();
  UpdateCurrentTimestampIfStale();

  const char* data = existing_value.data();

  if (logger_ && logger_->GetInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
    Debug(
        logger_.get(),
        "Call FlinkCompactionFilter::FilterV2 - Key: %s, Data: %s, Value type: "
        "%d, "
        "State type: %d, TTL: %" PRId64 " ms, timestamp_offset: %zu",
        key.ToString().c_str(), existing_value.ToString(true).c_str(),
        value_type, config_cached_->state_type_, config_cached_->ttl_,
        config_cached_->timestamp_offset_);
  }

  // too short value to have timestamp at all
  const bool tooShortValue =
      existing_value.size() <
      config_cached_->timestamp_offset_ + TIMESTAMP_BYTE_SIZE;

  const StateType state_type = config_cached_->state_type_;
  const bool value_or_merge =
      value_type == ValueType::kValue || value_type == ValueType::kMergeOperand;
  const bool value_state =
      state_type == StateType::Value && value_type == ValueType::kValue;
  const bool list_entry = state_type == StateType::List && value_or_merge;
  const bool toDecide = value_state || list_entry;
  const bool list_filter = list_entry && list_element_filter_;

  Decision decision = Decision::kKeep;
  if (!tooShortValue && toDecide) {
    decision = list_filter ? ListDecide(existing_value, new_value)
                           : Decide(data, config_cached_->ttl_,
                                    config_cached_->timestamp_offset_,
                                    current_timestamp_, logger_);
  }
  Debug(logger_.get(), "Decision: %d", static_cast<int>(decision));
  return decision;
}

CompactionFilter::Decision FlinkCompactionFilter::ListDecide(
    const Slice& existing_value, std::string* new_value) const {
  std::size_t offset = 0;
  if (offset < existing_value.size()) {
    Decision decision = Decide(existing_value.data(), config_cached_->ttl_,
                               offset + config_cached_->timestamp_offset_,
                               current_timestamp_, logger_);
    if (decision != Decision::kKeep) {
      offset =
          ListNextUnexpiredOffset(existing_value, offset, config_cached_->ttl_);
      if (offset >= JAVA_MAX_SIZE) {
        return Decision::kKeep;
      }
    }
  }
  if (offset >= existing_value.size()) {
    return Decision::kRemove;
  } else if (offset > 0) {
    SetUnexpiredListValue(existing_value, offset, new_value);
    return Decision::kChangeValue;
  }
  return Decision::kKeep;
}

std::size_t FlinkCompactionFilter::ListNextUnexpiredOffset(
    const Slice& existing_value, size_t offset, int64_t ttl) const {
  std::size_t new_offset = list_element_filter_->NextUnexpiredOffset(
      existing_value, ttl, current_timestamp_);
  if (new_offset >= JAVA_MAX_SIZE || new_offset < offset) {
    Error(logger_.get(), "Wrong next offset in list filter: %zu -> %zu", offset,
          new_offset);
    new_offset = JAVA_MAX_SIZE;
  } else {
    Debug(logger_.get(), "Next unexpired offset: %zu -> %zu", offset,
          new_offset);
  }
  return new_offset;
}

void FlinkCompactionFilter::SetUnexpiredListValue(
    const Slice& existing_value, std::size_t offset,
    std::string* new_value) const {
  new_value->clear();
  auto new_value_char = existing_value.data() + offset;
  auto new_value_size = existing_value.size() - offset;
  new_value->assign(new_value_char, new_value_size);
  Logger* logger = logger_.get();
  if (logger && logger->GetInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
    Slice new_value_slice = Slice(new_value_char, new_value_size);
    Debug(logger, "New list value: %s", new_value_slice.ToString(true).c_str());
  }
}
}  // namespace flink
}  // namespace ROCKSDB_NAMESPACE

//###########################################################################
//###########################################################################

#include <topling/side_plugin_repo.h>
#include <topling/side_plugin_factory.h>
#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <db/compaction/compaction_executor.h>
#include <logging/logging.h>

#define DoPrintLog(...) \
    info_log ? ROCKS_LOG_INFO(info_log, __VA_ARGS__) \
             : (void)fprintf(stderr, __VA_ARGS__)

#define PrintLog(level, fmt, ...) \
  do { if (SidePluginRepo::DebugLevel() >= level) \
    DoPrintLog("%s: " fmt "\n", \
            TERARK_PP_SmartForPrintf(rocksdb::StrDateTimeNow(), ## __VA_ARGS__)); \
  } while (0)
#define TRAC(...) PrintLog(4, "TRAC: " __VA_ARGS__)
#define DEBG(...) PrintLog(3, "DEBG: " __VA_ARGS__)
#define INFO(...) PrintLog(2, "INFO: " __VA_ARGS__)
#define WARN(...) PrintLog(1, "WARN: " __VA_ARGS__)

namespace ROCKSDB_NAMESPACE { namespace flink {
struct SideFlinkCompactFilterParams {
  size_t  timestamp_offset;
  int     list_elem_fixed_len; // 0 indicate non-list state
  int64_t ttl;
  int64_t query_time_after_num_entries;
  DATA_IO_LOAD_SAVE_V(SideFlinkCompactFilterParams,
                      1, // current serialization version
                      & timestamp_offset
                      & list_elem_fixed_len
                      & ttl
                      & query_time_after_num_entries
                      );
};
struct SideFlinkCompactionFilter : CompactionFilter, SideFlinkCompactFilterParams {
  mutable int64_t m_cur_milli = -1;
  mutable int64_t m_rec_counter = INT64_MAX;
  SideFlinkCompactionFilter(const SideFlinkCompactFilterParams* p) : SideFlinkCompactFilterParams(*p) {}
  const char* Name() const override { return "FlinkCompactionFilter"; }
  bool IsExpired(const char* ts_bytes) const {
    int64_t timestamp = DeserializeTimestamp(ts_bytes, timestamp_offset);
    int64_t ttlWithoutOverflow = timestamp > 0
                               ? std::min(INT64_MAX - timestamp, ttl) : ttl;
    return timestamp + ttlWithoutOverflow <= m_cur_milli;
  }
  Decision FilterV2(int level, const Slice& key, ValueType value_type,
                    const Slice& existing_value, std::string* new_value,
                    std::string* skip_until) const override {
    if (m_rec_counter >= query_time_after_num_entries) {
      m_rec_counter = 0;
      m_cur_milli = Env::Default()->NowMicros() / 1000;
    }
    m_rec_counter++;
    if (existing_value.size() < timestamp_offset + TIMESTAMP_BYTE_SIZE) {
      return Decision::kKeep; // too short value
    }
    bool expired = IsExpired(existing_value.data());
    if (expired && list_elem_fixed_len > 0) { // list with fixed len elem
      const char *ptr = existing_value.data(), *end = existing_value.end();
      while ((ptr += list_elem_fixed_len) < end) { // begin with 2nd elem
        if (!IsExpired(ptr)) { // find the first unexpired elem
          new_value->assign(ptr, end); // all elem after here are unexpired
          return Decision::kChangeValue;
        }
      }
      return Decision::kRemove;
    }
    return expired ? Decision::kRemove : Decision::kKeep;
  }
  bool IgnoreSnapshots() const override { return true; }
};
struct SideFlinkCompactionFilterFactory : CompactionFilterFactory, SideFlinkCompactFilterParams {
  std::unique_ptr<CompactionFilter>
  CreateCompactionFilter(const CompactionFilter::Context&) override {
    return std::make_unique<SideFlinkCompactionFilter>(this);
  }
  const char* Name() const override { return "FlinkCompactionFilterFactory"; }
};
using FlinkCompactionFilterFactory = SideFlinkCompactionFilterFactory;
ROCKSDB_REG_Plugin(FlinkCompactionFilterFactory, CompactionFilterFactory);

using namespace terark;
struct FlinkCompactionFilterFactory_SerDe : SerDeFunc<CompactionFilterFactory> {
  const CompactionParams* m_cp;
  rocksdb::Logger* info_log;
  int job_id;
  size_t rawzip[2];

  FlinkCompactionFilterFactory_SerDe(const json& js, const SidePluginRepo&) {
    auto cp = m_cp = JS_CompactionParamsDecodePtr(js);
    info_log = cp->info_log;
    const auto& smallest_user_key = cp->smallest_user_key;
    const auto& largest_user_key = cp->largest_user_key;
    job_id = cp->job_id;
    cp->InputBytes(rawzip);
    TRAC("FlinkCompactionFilterFactory_SerDe: job_id = %d, smallest_user_key = %s, largest_user_key = %s, job raw = %.3f GB, zip = %.3f GB",
        cp->job_id, Slice(smallest_user_key).hex().c_str(), Slice(largest_user_key).hex().c_str(), rawzip[0]/1e9, rawzip[1]/1e9);
  }
  void Serialize(FILE* output, const CompactionFilterFactory& cbase)
  const override {
    auto& base = const_cast<CompactionFilterFactory&>(cbase);
    LittleEndianDataOutput<NonOwnerFileStream> dio(output);
    if (IsCompactionWorker()) {
      // nothing is needed to return to DB
    }
    else { // DB Side
      DEBG("job-%05d: FlinkCompactionFilterFactory_SerDe::Serialize: job raw = %.3f GB, zip = %.3f GB, smallest_seqno = %lld",
            job_id, rawzip[0]/1e9, rawzip[1]/1e9, (llong)m_cp->smallest_seqno);
      auto tmp = base.CreateCompactionFilter({}); // just for get config
      auto flink_compact_filter = dynamic_cast<FlinkCompactionFilter*>(tmp.get());
      auto config = flink_compact_filter->GetConfig();
      SideFlinkCompactFilterParams params;
      params.timestamp_offset             = config->timestamp_offset_;
      params.ttl                          = config->ttl_;
      params.query_time_after_num_entries = config->query_time_after_num_entries_;
      params.list_elem_fixed_len = 0;
      if (auto list_elem_filt = config->list_element_filter_factory_.get()) {
        params.list_elem_fixed_len = list_elem_filt->GetFixedElemLen();
        if (params.list_elem_fixed_len <= 0) {
          // now it is too late to known we can not run dcompact,
          // we throw the exception to notify Dcompact Execution is failed
          // and fallback to local compaction
          DEBG("NotSupport job-%05d: FlinkCompactionFilterFactory_SerDe::Serialize: timestamp_offset = %zd, fixed_len = %d, ttl = %lld, query_time_after_num_entries = %lld",
                job_id, params.timestamp_offset, params.list_elem_fixed_len, (llong)params.ttl, (llong)params.query_time_after_num_entries);
          THROW_NotSupported("Flink List Element is not fixed len, can not run dcompact");
        }
      }
      DEBG("Ok Support job-%05d: FlinkCompactionFilterFactory_SerDe::Serialize: timestamp_offset = %zd, fixed_len = %d, ttl = %lld, query_time_after_num_entries = %lld",
            job_id, params.timestamp_offset, params.list_elem_fixed_len, (llong)params.ttl, (llong)params.query_time_after_num_entries);
      dio << params;
    }
  }
  void DeSerialize(FILE* reader, CompactionFilterFactory* base)
  const override {
    LittleEndianDataInput<NonOwnerFileStream> dio(reader);
    if (IsCompactionWorker()) {
      auto fac = dynamic_cast<SideFlinkCompactionFilterFactory*>(base);
      DEBG("job-%05d: FlinkCompactionFilterFactory_SerDe::DeSerialize: job raw = %.3f GB, zip = %.3f GB, smallest_seqno = %lld",
            job_id, rawzip[0]/1e9, rawzip[1]/1e9, (llong)m_cp->smallest_seqno);
      dio >> static_cast<SideFlinkCompactFilterParams&>(*fac);
    }
    else { // DB Side
      // nothing is needed to read from compact worker
    }
  }
};
ROCKSDB_REG_PluginSerDe(FlinkCompactionFilterFactory);

}}  // namespace ROCKSDB_NAMESPACE::flink
