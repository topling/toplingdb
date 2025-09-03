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
namespace ROCKSDB_NAMESPACE { namespace flink {
struct SideFlinkCompactFilterParams {
  size_t  timestamp_offset;
  int     list_elem_fixed_len; // 0 indicate non-list state
  int64_t ttl;
  int64_t query_time_after_num_entries;
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
  SideFlinkCompactionFilterFactory(const json& js, const SidePluginRepo& repo) {
    ROCKSDB_JSON_REQ_PROP(js, timestamp_offset);
    ROCKSDB_JSON_REQ_PROP(js, list_elem_fixed_len);
    ROCKSDB_JSON_REQ_PROP(js, ttl);
    ROCKSDB_JSON_REQ_PROP(js, query_time_after_num_entries);
  }
  std::unique_ptr<CompactionFilter>
  CreateCompactionFilter(const CompactionFilter::Context&) override {
    return std::make_unique<SideFlinkCompactionFilter>(this);
  }
  const char* Name() const override { return "FlinkCompactionFilterFactory"; }
};
using FlinkCompactionFilterFactory = SideFlinkCompactionFilterFactory;
ROCKSDB_REG_Plugin(FlinkCompactionFilterFactory, CompactionFilterFactory);

}}  // namespace ROCKSDB_NAMESPACE::flink
