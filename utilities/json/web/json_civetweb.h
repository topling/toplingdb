//
// Created by leipeng on 2020/8/18.
//
#pragma once

#include <rocksdb/rocksdb_namespace.h>
#include <string>
#include <utilities/json/json_fwd.h>

namespace ROCKSDB_NAMESPACE {

using nlohmann::json;

class JsonCivetServer {
public:
  ~JsonCivetServer();
  JsonCivetServer();
  void Init(const json& conf, JsonPluginRepo*);
  void Close();
  JsonCivetServer(const JsonCivetServer&) = delete;
  JsonCivetServer& operator=(const JsonCivetServer&) = delete;
private:
  class Impl;
  Impl* m_impl;
};

} // ROCKSDB_NAMESPACE
