//
// Created by leipeng on 2020-06-29.
//
// json_plugin_repo.h    is mostly for plugin users
// json_plugin_factory.h is mostly for plugin developers
//
#pragma once

#include <memory>
#include <unordered_map>

#include "json_fwd.h"
#include "json_plugin_repo.h"
#include "rocksdb/enum_reflection.h"
#include "rocksdb/preproc.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

using nlohmann::json;
class JsonOptionsRepo;

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
  static InstancePtr CreateInstance(const std::string& class_name, const json&,
                                    const JsonOptionsRepo&, Status*);
  static InstancePtr GetOrNewInstance(const char* varname, const char* func_name,
                                      const json&, const JsonOptionsRepo&, Status*);

  static InstancePtr GetInstance(const char* varname, const char* func_name,
                                 const json&, const JsonOptionsRepo&, Status*);

  struct AutoReg {
    AutoReg(const AutoReg&) = delete;
    AutoReg(AutoReg&&) = delete;
    AutoReg& operator=(AutoReg&&) = delete;
    AutoReg& operator=(const AutoReg&) = delete;
    typedef InstancePtr (*CreatorFunc)(const json&,const JsonOptionsRepo&,Status*);
    using NameToFuncMap = std::unordered_map<std::string, CreatorFunc>;
    AutoReg(Slice class_name, CreatorFunc creator);
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
AutoReg::AutoReg(Slice class_name, CreatorFunc creator) {
  auto& imp = Impl::s_singleton();
  auto ib = imp.func_map.insert(std::make_pair(class_name.ToString(), creator));
  if (!ib.second) {
    fprintf(stderr, "FATAL: %s:%d: %s: duplicate class_name = %s\n"
        , __FILE__, __LINE__, ROCKSDB_FUNC, class_name.data());
    abort();
  }
  this->ipos = ib.first;
}

template<class InstancePtr>
FactoryFor<InstancePtr>::AutoReg::~AutoReg() {
  auto& imp = Impl::s_singleton();
  imp.func_map.erase(ipos);
}

template<class InstancePtr>
InstancePtr
FactoryFor<InstancePtr>::
CreateInstance(const std::string& class_name, const json& js,
               const JsonOptionsRepo& repo, Status* st) {
  auto& imp = AutoReg::Impl::s_singleton();
  auto iter = imp.func_map.find(class_name);
  if (imp.func_map.end() != iter) {
    return iter->second(js, repo, st);
  }
  else {
    return InstancePtr(nullptr);
  }
}

template<class InstancePtr>
InstancePtr
FactoryFor<InstancePtr>::
GetInstance(const char* varname, const char* func_name,
            const json& js, const JsonOptionsRepo& repo, Status* s) {
  if (js.is_string()) {
    const std::string& str_val = js.get<std::string>();
    if (str_val.empty()) {
      *s = Status::NotFound(
          func_name, std::string(varname) + " inst_id/class_name is empty");
	  return InstancePtr(nullptr);
    }
    InstancePtr p(nullptr);
    if ('$' == str_val[0]) {
      if (str_val.size() < 3) {
        *s = Status::NotFound(func_name,
                              std::string(varname) + " inst_id is too short");
        return InstancePtr(nullptr);
      }
      const auto inst_id = '{' == str_val[1]
                         ? str_val.substr(2, str_val.size() - 3)
                         : str_val.substr(1, str_val.size() - 1);
      repo.Get(inst_id, &p);
    } else {
      repo.Get(str_val, &p); // the whole str_val is inst_id
    }
    *s = Status::NotFound(func_name,
                          std::string(varname) + "inst_id = " + str_val);
    return p;
  }
  else {
    *s = Status::InvalidArgument(func_name,
      std::string(varname) + " must be a string for reference to object");
	return InstancePtr(nullptr);
  }
}

///@param varname just for error report
///@param func_name just for error report
template<class InstancePtr>
InstancePtr
FactoryFor<InstancePtr>::
GetOrNewInstance(const char* varname, const char* func_name,
                 const json& js, const JsonOptionsRepo& repo, Status* s) {
  if (js.is_string()) {
    const std::string& str_val = js.get<std::string>();
    if (str_val.empty()) {
      *s = Status::NotFound(
          func_name, std::string(varname) + " inst_id/class_name is empty");
      return InstancePtr(nullptr);
    }
    if ('$' == str_val[0]) {
      if (str_val.size() < 3) {
        *s = Status::NotFound(func_name,
                                std::string(varname) + " inst_id is too short");
        return InstancePtr(nullptr);
      }
      const auto inst_id = '{' == str_val[1]
                         ? str_val.substr(2, str_val.size() - 3)
                         : str_val.substr(1, str_val.size() - 1);
      InstancePtr p;
      if (!repo.Get(inst_id, &p)) {
        *s = Status::NotFound(func_name,
                              std::string(varname) + "inst_id = " + inst_id);
      }
      return p;
    } else { // CreateInstance with empty json options
      const std::string& clazz_name = str_val;
      return CreateInstance(clazz_name, json{}, repo, s);
    }
  } else {
    const std::string& clazz_name = js.at("class").get<std::string>();
    const json& options = js.at("options");
    return CreateInstance(clazz_name, options, repo, s);
  }
}

template<class Instance>
using FactoryForSP = FactoryFor<std::shared_ptr<Instance> >;

const json& jsonRefType();
const JsonOptionsRepo& repoRefType();

///@param Name     string of factory class_name
///@param Creator  must return base class ptr
#define ROCKSDB_FACTORY_REG(Name, Creator) \
  FactoryFor<decltype(Creator(jsonRefType(),repoRefType(),(Status*)0))>:: \
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
    Status __status; \
    prop = FactoryFor<decltype(prop)>:: \
        GetOrNewInstance(#prop, ROCKSDB_FUNC, js, repo, &__status); \
    if (!__status.ok()) return __status; \
  } while (0)

#define ROCKSDB_JSON_OPT_FACT(js, prop) do { \
    auto __iter = js.find(#prop); \
    if (js.end() != __iter) { \
      ROCKSDB_JSON_OPT_FACT_IMPL(__iter.value(), prop); \
  }} while (0)


}
