//
// Created by leipeng on 2020/8/5.
//
#pragma once
#include "rocksdb/status.h"
#include <map>
#include <stdint.h>

namespace ROCKSDB_NAMESPACE {

std::map<uint64_t, std::string>&
GetDispatcherTableMagicNumberMap();

struct RegTableFactoryMagicNumber {
  RegTableFactoryMagicNumber(uint64_t, const char*);
};
#define ROCKSDB_RegTableFactoryMagicNumber(magic,name) \
  RegTableFactoryMagicNumber g_AutoRegTF_##magic(magic, name)

void DispatcherTableBackPatch(TableFactory* f, const JsonPluginRepo& repo);

} // ROCKSDB_NAMESPACE

