//
// Created by leipeng on 2020/7/2.
//
#pragma once

#include "json_fwd.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct Options;
struct DBOptions;
struct ColumnFamilyOptions;

class Cache;
class CompactionFilterFactory;
class Comparator;
class ConcurrentTaskLimiter;
class Env;
class EventListener;
class FileChecksumGenFactory;
class FilterPolicy;
class FlushBlockPolicyFactory;
class Logger;
class MemTableRepFactory;
class MergeOperator;
class PersistentCache;
class RateLimiter;
class SliceTransform;
class SstFileManager;
class Statistics;
class TableFactory;
class TablePropertiesCollectorFactory;

class JsonOptionsRepo {
 public:
  JsonOptionsRepo() noexcept;
  ~JsonOptionsRepo();
  JsonOptionsRepo(const JsonOptionsRepo&) noexcept;
  JsonOptionsRepo(JsonOptionsRepo&&) noexcept;
  JsonOptionsRepo& operator=(const JsonOptionsRepo&) noexcept;
  JsonOptionsRepo& operator=(JsonOptionsRepo&&) noexcept;

  Status Import(const std::string& json_str);
  Status Import(const nlohmann::json&);
  Status Export(nlohmann::json*) const;
  Status Export(std::string*, bool pretty = false) const;

  ///@{
  /// the semantic is overwrite
  void Add(const std::string& name, const std::shared_ptr<Options>&);
  void Add(const std::string& name, const std::shared_ptr<DBOptions>&);
  void Add(const std::string& name, const std::shared_ptr<ColumnFamilyOptions>&);

  void Add(const std::string& name, const std::shared_ptr<Cache>&) const;
  void Add(const std::string& name, const std::shared_ptr<CompactionFilterFactory>&) const;
  void Add(const std::string& name, const std::shared_ptr<Comparator>&) const;
  void Add(const std::string& name, const std::shared_ptr<ConcurrentTaskLimiter>&) const;
  void Add(const std::string& name, const std::shared_ptr<Env>&) const;
  void Add(const std::string& name, const std::shared_ptr<EventListener>&) const;
  void Add(const std::string& name, const std::shared_ptr<FileChecksumGenFactory>&) const;
  void Add(const std::string& name, const std::shared_ptr<FilterPolicy>&) const;
  void Add(const std::string& name, const std::shared_ptr<FlushBlockPolicyFactory>&) const;
  void Add(const std::string& name, const std::shared_ptr<Logger>&) const;
  void Add(const std::string& name, const std::shared_ptr<MemTableRepFactory>&) const;
  void Add(const std::string& name, const std::shared_ptr<MergeOperator>&) const;
  void Add(const std::string& name, const std::shared_ptr<PersistentCache>&) const;
  void Add(const std::string& name, const std::shared_ptr<RateLimiter>&) const;
  void Add(const std::string& name, const std::shared_ptr<SliceTransform>&) const;
  void Add(const std::string& name, const std::shared_ptr<SstFileManager>&) const;
  void Add(const std::string& name, const std::shared_ptr<Statistics>&) const;
  void Add(const std::string& name, const std::shared_ptr<TableFactory>&) const;
  void Add(const std::string& name, const std::shared_ptr<TablePropertiesCollectorFactory>&) const;
  ///@}

  bool Get(const std::string& name,  std::shared_ptr<Options>*);
  bool Get(const std::string& name,  std::shared_ptr<DBOptions>*);
  bool Get(const std::string& name,  std::shared_ptr<ColumnFamilyOptions>*);
  
  bool Get(const std::string& name, std::shared_ptr<Cache>*) const;
  bool Get(const std::string& name, std::shared_ptr<CompactionFilterFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<Comparator>*) const;
  bool Get(const std::string& name, std::shared_ptr<ConcurrentTaskLimiter>*) const;
  bool Get(const std::string& name, std::shared_ptr<Env>*) const;
  bool Get(const std::string& name, std::shared_ptr<EventListener>*) const;
  bool Get(const std::string& name, std::shared_ptr<FileChecksumGenFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<FilterPolicy>*) const;
  bool Get(const std::string& name, std::shared_ptr<FlushBlockPolicyFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<Logger>*) const;
  bool Get(const std::string& name, std::shared_ptr<MemTableRepFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<MergeOperator>*) const;
  bool Get(const std::string& name, std::shared_ptr<PersistentCache>*) const;
  bool Get(const std::string& name, std::shared_ptr<RateLimiter>*) const;
  bool Get(const std::string& name, std::shared_ptr<SliceTransform>*) const;
  bool Get(const std::string& name, std::shared_ptr<SstFileManager>*) const;
  bool Get(const std::string& name, std::shared_ptr<Statistics>*) const;
  bool Get(const std::string& name, std::shared_ptr<TableFactory>*) const;
  bool Get(const std::string& name, std::shared_ptr<TablePropertiesCollectorFactory>*) const;

 protected:
  struct Impl;
  std::shared_ptr<Impl> m_impl;
};

}  // namespace ROCKSDB_NAMESPACE
