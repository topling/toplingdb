#ifndef INCLUDE_NLOHMANN_JSON_FWD_HPP_
#define INCLUDE_NLOHMANN_JSON_FWD_HPP_

#include <cstdint> // int64_t, uint64_t
#include <map> // map
#include <memory> // allocator
#include <string> // string
#include <vector> // vector

#ifndef JSON_USE_STD_MAP
#include <terark/gold_hash_map.hpp>

namespace terark {
template<class Key, class Val, class IgnoreLess, class IgnoreAlloc>
class JsonStrMap : public gold_hash_map<Key, Val
    , std::hash<Key>, std::equal_to<Key>
    , node_layout<std::pair<Key, Val>, unsigned, SafeCopy, ValueOut>
    >
{
public:
    typedef IgnoreAlloc allocator_type;

    template<class Iter>
    JsonStrMap(Iter first, Iter last) {
        this->enable_freelist();
        for (Iter iter = first; iter != last; ++iter) {
            this->insert_i(iter->first, iter->second);
        }
    }
    JsonStrMap() {
        this->enable_freelist();
    }
};
} // namespace terark

#endif // #ifndef JSON_USE_STD_MAP

/*!
@brief namespace for Niels Lohmann
@see https://github.com/nlohmann
@since version 1.0.0
*/
namespace nlohmann
{
/*!
@brief default JSONSerializer template argument

This serializer ignores the template arguments and uses ADL
([argument-dependent lookup](https://en.cppreference.com/w/cpp/language/adl))
for serialization.
*/
template<typename T = void, typename SFINAE = void>
struct adl_serializer;

template<template<typename U, typename V, typename... Args> class ObjectType =
#ifdef JSON_USE_STD_MAP
         std::map,
#else
         terark::JsonStrMap,
#endif
         template<typename U, typename... Args> class ArrayType = std::vector,
         class StringType = std::string, class BooleanType = bool,
         class NumberIntegerType = std::int64_t,
         class NumberUnsignedType = std::uint64_t,
         class NumberFloatType = double,
         template<typename U> class AllocatorType = std::allocator,
         template<typename T, typename SFINAE = void> class JSONSerializer =
         adl_serializer,
         class BinaryType = std::vector<std::uint8_t>>
class basic_json;

/*!
@brief JSON Pointer

A JSON pointer defines a string syntax for identifying a specific value
within a JSON document. It can be used with functions `at` and
`operator[]`. Furthermore, JSON pointers are the base for JSON patches.

@sa [RFC 6901](https://tools.ietf.org/html/rfc6901)

@since version 2.0.0
*/
template<typename BasicJsonType>
class json_pointer;

/*!
@brief default JSON class

This type is the default specialization of the @ref basic_json class which
uses the standard template types.

@since version 1.0.0
*/
#ifdef JSON_USE_STD_MAP
using json = basic_json<>;
#else
typedef basic_json<terark::JsonStrMap> json;
/*
typedef basic_json<terark::JsonStrMap> JsonBase;
class json : public JsonBase {
public:
    using JsonBase::JsonBase;
};
*/
#endif

}  // namespace nlohmann

#endif  // INCLUDE_NLOHMANN_JSON_FWD_HPP_
