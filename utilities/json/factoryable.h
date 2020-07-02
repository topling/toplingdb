//
// Created by leipeng on 2020-06-29.
//

#pragma once

#include <memory>
#include <unordered_map>

#include "json_fwd.h"
#include "rocksdb/enum_reflection.h"
#include "rocksdb/preproc.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

using nlohmann::json;

///@note on principle, the factory itself is stateless, but its product
/// can has states, sometimes we need factory of factory, in this case,
/// just let the factory being FactoryFor:
/// class SomeClass : public FactoryFor<SomeClass*> {
///    ...
/// };
template<class InstancePtr>
class FactoryFor {
public:
  virtual ~FactoryFor() {}
  static InstancePtr CreateInstance(const std::string& reg_name, const json&, Status*);
  static InstancePtr GetRepoInstance(const std::string& inst_id);
  static void DeleteRepoInstance(const std::string& inst_id);
  static bool InsertRepoInstance(const std::string& inst_id, InstancePtr);
  static void UpsertRepoInstance(const std::string& inst_id, InstancePtr);

  struct AutoReg {
    AutoReg(const AutoReg&) = delete;
    AutoReg(AutoReg&&) = delete;
    AutoReg& operator=(AutoReg&&) = delete;
    AutoReg& operator=(const AutoReg&) = delete;
    typedef InstancePtr (*CreatorFunc)(const json&, Status*);
    using NameToFuncMap = std::unordered_map<std::string, CreatorFunc>;
    AutoReg(Slice reg_name, CreatorFunc creator);
    ~AutoReg();
    typename NameToFuncMap::iterator ipos;
    struct Impl;
  };
};

template<class InstancePtr>
struct FactoryFor<InstancePtr>::AutoReg::Impl {
  NameToFuncMap func_map;
  std::unordered_map<std::string, InstancePtr> inst_map;
  static Impl& s_singleton() { static Impl imp; return imp; }
};

template<class InstancePtr>
FactoryFor<InstancePtr>::
AutoReg::AutoReg(Slice reg_name, CreatorFunc creator) {
  auto& imp = Impl::s_singleton();
  auto ib = imp.func_map.insert(std::make_pair(reg_name.ToString(), creator));
  if (!ib.second) {
    fprintf(stderr, "FATAL: %s:%d: %s: duplicate reg_name = %s\n"
        , __FILE__, __LINE__, ROCKSDB_FUNC, reg_name.data());
    abort();
  }
  this->ipos = ib.first;
}

template<class InstancePtr>
FactoryFor<InstancePtr>::
AutoReg::~AutoReg() {
  auto& imp = Impl::s_singleton();
  imp.func_map.erase(ipos);
}

template<class InstancePtr>
InstancePtr
FactoryFor<InstancePtr>::
CreateInstance(const std::string& reg_name, const json& js, Status* st) {
  auto& imp = AutoReg::Impl::s_singleton();
  auto iter = imp.func_map.find(reg_name);
  if (imp.func_map.end() != iter) {
    return iter->second(js, st);
  }
  else {
    return InstancePtr(nullptr);
  }
}

template<class InstancePtr>
InstancePtr
FactoryFor<InstancePtr>::
GetRepoInstance(const std::string& inst_id) {
  auto& imp = AutoReg::Impl::s_singleton();
  auto iter = imp.inst_map.find(inst_id);
  if (imp.inst_map.end() != iter) {
    return iter->second;
  }
  else {
    return InstancePtr(nullptr);
  }
}

template<class InstancePtr>
void
FactoryFor<InstancePtr>::
DeleteRepoInstance(const std::string& inst_id) {
  auto& imp = AutoReg::Impl::s_singleton();
  imp.inst_map.erase(inst_id);
}

template<class InstancePtr>
bool
FactoryFor<InstancePtr>::
InsertRepoInstance(const std::string& inst_id, InstancePtr inst) {
  auto& imp = AutoReg::Impl::s_singleton();
  auto ib = imp.inst_map.insert(std::make_pair(inst_id, inst));
  return ib.second;
}

template<class InstancePtr>
void
FactoryFor<InstancePtr>::
UpsertRepoInstance(const std::string& inst_id, InstancePtr inst) {
  auto& imp = AutoReg::Impl::s_singleton();
  auto ib = imp.inst_map.insert(std::make_pair(inst_id, inst));
  if (!ib.second) {
    ib.first->second = inst;
  }
}

template<class Instance>
using FactoryForSP = FactoryFor<std::shared_ptr<Instance> >;

const json& jsonRefType();

///@param Name     string of factory reg_name
///@param Creator  must return base class ptr
#define ROCKSDB_FACTORY_REG(Name, Creator) \
  FactoryFor<decltype(Creator(jsonRefType(),(Status*)0))>:: \
  AutoReg ROCKSDB_PP_CAT_3(g_reg_factory_,Creator,__LINE__)(Name,Creator)

//////////////////////////////////////////////////////////////////////////////

// _REQ_ means 'required'
// _OPT_ means 'optional'
#define ROCKSDB_JSON_REQ_PROP(js, prop) do { \
    auto __iter = js.find(#prop); \
    if (js.end() != __iter) prop = __iter.value().get<decltype(prop)>(); \
    else throw std::invalid_argument("#prop" "is required"); \
  } while (0)
#define ROCKSDB_JSON_OPT_PROP(js, prop) do { \
    auto __iter = js.find(#prop); \
    if (js.end() != __iter) prop = __iter.value().get<decltype(prop)>(); \
  } while (0)
#define ROCKSDB_JSON_OPT_ENUM(js, prop) do { \
    auto __iter = js.find(#prop); \
    if (js.end() != __iter) { \
      const std::string& val = __iter.value().get<std::string>(); \
      if (!enum_value(val, &prop)) \
        throw std::invalid_argument("bad " #prop "=" + val); \
  }} while (0)
#define ROCKSDB_JSON_OPT_NEST(js, prop) \
  do try { \
    auto __iter = js.find(#prop); \
    if (js.end() != __iter) \
      prop = decltype(NestForBase(prop))(__iter.value()); \
  } catch (const std::exception& ex) { \
    return Status::InvalidArgument(ROCKSDB_FUNC, \
       std::string(#prop ": ") + ex.what()); \
  } while (0)

#define ROCKSDB_JSON_OPT_FACT_IMPL(js, prop) do { \
    if (js.is_string()) { \
      const std::string& __inst_id = js.get<std::string>(); \
      prop = FactoryFor<decltype(prop)>::GetRepoInstance(__inst_id); \
      if (!prop) { \
        return Status::NotFound(ROCKSDB_FUNC, "inst_id = " + __inst_id); \
      } \
    } else { \
      Status __status; \
      const std::string& __clazz_name = js.at("class").get<std::string>(); \
      const json& __options = js.at("options"); \
      prop = FactoryFor<decltype(prop)>::CreateInstance( \
                 __clazz_name, __options, &__status); \
      if (!__status.ok()) return __status; \
      assert(!!prop); \
    }} while (0)

#define ROCKSDB_JSON_OPT_FACT(js, prop) do { \
    auto __iter = js.find(#prop); \
    if (js.end() != __iter) { \
      ROCKSDB_JSON_OPT_FACT_IMPL(__iter.value(), prop); \
  }} while (0)


}
