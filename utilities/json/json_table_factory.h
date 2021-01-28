//
// Created by leipeng on 2020/8/5.
//
#pragma once
#include "rocksdb/status.h"
#include <unordered_map>
#include <stdint.h>

namespace ROCKSDB_NAMESPACE {

std::unordered_map<uint64_t, std::string>&
GetDispatherTableMagicNumberMap();

struct RegTableFactoryMagicNumber {
  RegTableFactoryMagicNumber(uint64_t, const char*);
};
#define ROCKSDB_RegTableFactoryMagicNumber(magic,name) \
  RegTableFactoryMagicNumber g_AutoRegTF_##magic(magic, name)

Status DispatherTableBackPatch(TableFactory* f, const JsonPluginRepo& repo);

} // ROCKSDB_NAMESPACE

