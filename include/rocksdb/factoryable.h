//
// Created by leipeng on 2020-06-29.
//

#pragma once

#include <functional>
#include <unordered_map>
#include <memory>
#include "rocksdb_namespace.h"
#include "slice.h"
#include "status.h"
#include "json_fwd.h"
#include "preproc.h"

namespace ROCKSDB_NAMESPACE {

using nlohmann::json;

///@note on principle, the factory itself is stateless, but its product
/// can has states, sometimes we need factory of factory, in this case,
/// just let the factory being Factoryable:
/// class SomeClass : public Factoryable<SomeClass*> {
///    ...
/// };
template<class InstancePtr>
class Factoryable {
public:
  virtual ~Factoryable() {}
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
    using CreatorFunc = std::function<InstancePtr(const json&, Status*)>;
    using NameToFuncMap = std::unordered_map<std::string, CreatorFunc>;
    AutoReg(Slice reg_name, CreatorFunc creator);
    ~AutoReg();
    typename NameToFuncMap::iterator ipos;
    struct Impl;
  };
};

template<class InstancePtr>
struct Factoryable<InstancePtr>::AutoReg::Impl {
  NameToFuncMap func_map;
  std::unordered_map<std::string, InstancePtr> inst_map;
  static Impl& s_singleton() { static Impl imp; return imp; }
};

template<class InstancePtr>
Factoryable<InstancePtr>::
AutoReg::AutoReg(Slice reg_name, CreatorFunc creator) {
  auto& imp = Impl::s_singleton();
  auto ib = imp.func_map.insert(std::make_pair(reg_name.ToString(), std::move(creator)));
  if (!ib.second) {
    fprintf(stderr, "FATAL: %s:%d: %s: duplicate reg_name = %s\n"
        , __FILE__, __LINE__, __PRETTY_FUNCTION__, reg_name.data());
    abort();
  }
  this->ipos = ib.first;
}

template<class InstancePtr>
Factoryable<InstancePtr>::
AutoReg::~AutoReg() {
  auto& imp = Impl::s_singleton();
  imp.func_map.erase(ipos);
}

template<class InstancePtr>
InstancePtr
Factoryable<InstancePtr>::
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
Factoryable<InstancePtr>::
GetRepoInstance(const std::string& inst_id) {
  auto& imp = AutoReg::Impl::s_singleton();
  auto __iter = imp.inst_map.find(inst_id);
  if (imp.inst_map.end() != __iter) {
    return __iter->second;
  }
  else {
    return InstancePtr(nullptr);
  }
}

template<class InstancePtr>
void
Factoryable<InstancePtr>::
DeleteRepoInstance(const std::string& inst_id) {
  auto& imp = AutoReg::Impl::s_singleton();
  imp.inst_map.erase(inst_id);
}

template<class InstancePtr>
bool
Factoryable<InstancePtr>::
InsertRepoInstance(const std::string& inst_id, InstancePtr inst) {
  auto& imp = AutoReg::Impl::s_singleton();
  auto ib = imp.inst_map.insert(std::make_pair(inst_id, inst));
  return ib.second;
}

template<class InstancePtr>
void
Factoryable<InstancePtr>::
UpsertRepoInstance(const std::string& inst_id, InstancePtr inst) {
  auto& imp = AutoReg::Impl::s_singleton();
  auto ib = imp.inst_map.insert(std::make_pair(inst_id, inst));
  if (!ib.second) {
    ib.first->second = inst;
  }
}

template<class Ptr> struct ExtractInstanceType;
template<class T>
struct ExtractInstanceType<T*> { typedef T type; };
template<class T>
struct ExtractInstanceType<std::shared_ptr<T> > { typedef T type; };

template<class Instance>
using FactoryableSP = Factoryable<std::shared_ptr<Instance> >;

///@param Name     string of factory reg_name
///@param Creator  creator function
///@param Class    class
#define ROCKSDB_FACTORY_AUTO_REG_EX(Name, Class, Creator) \
  Class::AutoReg ROCKSDB_PP_CAT_3(g_reg_factory_,Class,__LINE__)(Name,Creator)

#define ROCKSDB_FACTORY_AUTO_REG(Class, Creator) \
        ROCKSDB_FACTORY_AUTO_REG_EX(#Class, Class, Creator)

//////////////////////////////////////////////////////////////////////////////

#define ROCKSDB_JSON_GET_PROP_3(js, prop, Default) do { \
    auto __iter = js.find(#prop); \
    if (js.end() != __iter) prop = __iter.value().get<decltype(prop)>(); \
    else prop = Default; \
  } while (0)
#define ROCKSDB_JSON_GET_ENUM_3(js, prop, Default) do { \
    auto __iter = js.find(#prop); \
    if (js.end() != __iter) { \
      const std::string& val = __iter.value().get<std::string>(); \
      if (!enum_value(val, &prop)) \
        throw std::invalid_argument("bad " #prop "=" + val); \
    } else \
       prop = Default; \
  } while (0)

#define ROCKSDB_JSON_GET_PROP_2(js, prop) do { \
    auto __iter = js.find(#prop); \
    if (js.end() != __iter) prop = __iter.value().get<decltype(prop)>(); \
  } while (0)
#define ROCKSDB_JSON_GET_ENUM_2(js, prop) do { \
    auto __iter = js.find(#prop); \
    if (js.end() != __iter) { \
      const std::string& val = __iter.value().get<std::string>(); \
      if (!enum_value(val, &prop)) \
        throw std::invalid_argument("bad " #prop "=" + val); \
  }} while (0)

#define ROCKSDB_JSON_GET_NEST_IMPL(js, prop, clazz) do { \
    if (js.is_string()) { \
      const std::string& __inst_id = js.get<std::string>(); \
      prop = clazz::GetRepoInstance(__inst_id); \
      if (!prop) { \
        return Status::NotFound(ROCKSDB_FUNC, "reference id" + __inst_id); \
      } \
    } else { \
      Status __status; \
      const std::string& __clazz_name = js.at("class").get<std::string>(); \
      const json& __options = js.at("options"); \
      prop = clazz::CreateInstance(__clazz_name, __options, &__status); \
      if (!__status.ok()) return __status; \
    }} while (0)

#define ROCKSDB_JSON_GET_NEST_EX(js, prop, clazz) do { \
    auto __iter = js.find(#prop); \
    if (js.end() != __iter) { \
      ROCKSDB_JSON_GET_NEST_IMPL(__iter.value(), prop, clazz); \
  }} while (0)
#define ROCKSDB_JSON_GET_NEST(js, prop) \
    ROCKSDB_JSON_GET_NEST_EX(js, prop, ExtractInstanceType<decltype(prop)>::type)

#define ROCKSDB_JSON_GET_PROP(...) \
  ROCKSDB_PP_VA_NAME(ROCKSDB_JSON_GET_PROP_, __VA_ARGS__)(__VA_ARGS__)

#define ROCKSDB_JSON_GET_ENUM(...) \
  ROCKSDB_PP_VA_NAME(ROCKSDB_JSON_GET_ENUM_, __VA_ARGS__)(__VA_ARGS__)


}
