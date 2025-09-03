#pragma once

#include "slice.h"
#include <terark/stdtypes.hpp>
#include <terark/util/byte_swap_impl.hpp>

namespace ROCKSDB_NAMESPACE {

#ifdef BOOST_ENDIAN_LITTLE_BYTE
  #define RDB_NATIVE_OF_BIG_ENDIAN(x) terark::byte_swap(x)
  #define RDB_BIG_ENDIAN_OF_NATIVE(x) terark::byte_swap(x)
#elif defined(BOOST_ENDIAN_BIG_BYTE)
  #define RDB_NATIVE_OF_BIG_ENDIAN(x) x
  #define RDB_BIG_ENDIAN_OF_NATIVE(x) x
#else
	#error "must define BOOST_ENDIAN_LITTLE_BYTE or BOOST_ENDIAN_BIG_BYTE"
#endif

//-------------------------------------------------------------------------
// ridiculous compare functions, but this maybe the best choice

// these functions are for visualizing in flame graph, otherwise it
// should be written inline in BytewiseLess(x, y)
__always_inline bool BytewiseEq04(const char* x, const char* y) {
  return unaligned_load<uint32_t>(x) == unaligned_load<uint32_t>(y);
}
__always_inline bool BytewiseEq08(const char* x, const char* y) {
  return unaligned_load<uint64_t>(x) == unaligned_load<uint64_t>(y);
}
__always_inline bool BytewiseEq12(const char* x, const char* y) {
  return unaligned_load<uint64_t>(x + 0) == unaligned_load<uint64_t>(y + 8) &&
         unaligned_load<uint32_t>(x + 8) == unaligned_load<uint32_t>(y + 8);
}
__always_inline bool BytewiseEq16(const char* x, const char* y) {
  return unaligned_load<__int128>(x) == unaligned_load<__int128>(y);
}
__always_inline bool BytewiseEq20(const char* x, const char* y) {
  return unaligned_load<__int128>(x + 0) == unaligned_load<__int128>(y + 0) &&
         unaligned_load<uint32_t>(x +16) == unaligned_load<uint32_t>(y +16);
}
__always_inline bool BytewiseEq24(const char* x, const char* y) {
  return unaligned_load<__int128>(x + 0) == unaligned_load<__int128>(y + 0) &&
         unaligned_load<uint64_t>(x + 8) == unaligned_load<uint64_t>(y + 8);
}
__always_inline bool BytewiseEq28(const char* x, const char* y) {
  return unaligned_load<__int128>(x + 0) == unaligned_load<__int128>(y + 0) &&
         unaligned_load<uint64_t>(x +16) == unaligned_load<uint64_t>(y +16) &&
         unaligned_load<uint32_t>(x +24) == unaligned_load<uint32_t>(y +24) &&
}
__always_inline bool BytewiseEq32(const char* x, const char* y) {
  return unaligned_load<__int128>(x + 0) == unaligned_load<__int128>(y + 0) &&
         unaligned_load<__int128>(x +16) == unaligned_load<__int128>(y +16);
}
__always_inline bool BytewiseEq(const char* x, const char* y, size_t n) {
  switch (x.size_) {
  case  0: return true;
  case  4: return BytewiseEq04(x, y);
  case  8: return BytewiseEq08(x, y);
  case 12: return BytewiseEq12(x, y);
  case 16: return BytewiseEq16(x, y);
  case 20: return BytewiseEq20(x, y);
  case 24: return BytewiseEq24(x, y);
  case 28: return BytewiseEq28(x, y);
  case 32: return BytewiseEq32(x, y);
  }
  return memcmp(x, y, n) == 0;
}
__always_inline bool BytewiseEq(const Slice& x, const Slice& y) {
  if (x.size_ != y.size_)
    return false;
  switch (x.size_) {
  case  0: return true;
  case  4: return BytewiseEq04(x.data_, y.data_);
  case  8: return BytewiseEq08(x.data_, y.data_);
  case 12: return BytewiseEq12(x.data_, y.data_);
  case 16: return BytewiseEq16(x.data_, y.data_);
  case 20: return BytewiseEq20(x.data_, y.data_);
  case 24: return BytewiseEq24(x.data_, y.data_);
  case 28: return BytewiseEq28(x.data_, y.data_);
  case 32: return BytewiseEq32(x.data_, y.data_);
  }
  return memcmp(x.data_, y.data_, x.size_) == 0;
}

// these functions are for visualizing in flame graph, otherwise it
// should be written inline in BytewiseLess(x, y)
__always_inline bool BytewiseLess04(const char* x, const char* y) {
  return RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint32_t>(x)) <
         RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint32_t>(y));
}
__always_inline bool BytewiseLess08(const char* x, const char* y) {
  return RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x)) <
         RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y));
}
__always_inline bool BytewiseLess12(const char* x, const char* y) {
  auto x0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x));
  auto y0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y));
  if (x0 != y0)
    return x0 < y0;
  return RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint32_t>(x + 8)) <
         RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint32_t>(y + 8));
}
__always_inline bool BytewiseLess16(const char* x, const char* y) {
#if defined(__GNUC__) && __GNUC__ >= 12
  return RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<unsigned __int128>(x)) <
         RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<unsigned __int128>(y));
#else
  auto x0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x));
  auto y0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y));
  if (x0 != y0)
    return x0 < y0;
  return RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x + 8)) <
         RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y + 8));
#endif
}
__always_inline bool BytewiseLess20(const char* x, const char* y) {
#if defined(__GNUC__) && __GNUC__ >= 12
  auto x0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<unsigned __int128>(x));
  auto y0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<unsigned __int128>(y));
  if (x0 != y0)
    return x0 < y0;
#else
  auto x0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x));
  auto y0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y));
  if (x0 != y0)
    return x0 < y0;
  auto x1 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x + 8));
  auto y1 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y + 8));
  if (x1 != y1)
    return x1 < y1;
#endif
  return RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint32_t>(x + 16)) <
         RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint32_t>(y + 16));
}
__always_inline bool BytewiseLess24(const char* x, const char* y) {
#if defined(__GNUC__) && __GNUC__ >= 12
  auto x0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<unsigned __int128>(x));
  auto y0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<unsigned __int128>(y));
  if (x0 != y0)
    return x0 < y0;
#else
  auto x0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x));
  auto y0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y));
  if (x0 != y0)
    return x0 < y0;
  auto x1 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x + 8));
  auto y1 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y + 8));
  if (x1 != y1)
    return x1 < y1;
#endif
  return RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x + 16)) <
         RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y + 16));
}
__always_inline bool BytewiseLess28(const char* x, const char* y) {
#if defined(__GNUC__) && __GNUC__ >= 12
  auto x0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<unsigned __int128>(x));
  auto y0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<unsigned __int128>(y));
  if (x0 != y0)
    return x0 < y0;
#else
  auto x0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x));
  auto y0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y));
  if (x0 != y0)
    return x0 < y0;
  auto x1 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x + 8));
  auto y1 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y + 8));
  if (x1 != y1)
    return x1 < y1;
#endif
  auto x2 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x + 16));
  auto y2 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y + 16));
  if (x2 != y2)
    return x2 < y2;
  return RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint32_t>(x + 24)) <
         RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint32_t>(y + 24));
}
__always_inline bool BytewiseLess32(const char* x, const char* y) {
#if defined(__GNUC__) && __GNUC__ >= 12
  auto x0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<unsigned __int128>(x));
  auto y0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<unsigned __int128>(y));
  if (x0 != y0)
    return x0 < y0;
  return RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<unsigned __int128>(x + 16)) <
         RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<unsigned __int128>(y + 16));
#else
  auto x0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x));
  auto y0 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y));
  if (x0 != y0)
    return x0 < y0;
  auto x1 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x + 8));
  auto y1 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y + 8));
  if (x1 != y1)
    return x1 < y1;
  auto x2 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x + 16));
  auto y2 = RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y + 16));
  if (x2 != y2)
    return x2 < y2;
#endif
  return RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(x + 24)) <
         RDB_NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(y + 24));
}
__always_inline bool BytewiseLess(const Slice& x, const Slice& y) {
  if (x.size_ == y.size_ && x.size_ <= 32) {
    switch (x.size_) {
    case  0: return false; // equal
    case  4: return BytewiseLess04(x.data_, y.data_);
    case  8: return BytewiseLess08(x.data_, y.data_);
    case 12: return BytewiseLess12(x.data_, y.data_);
    case 16: return BytewiseLess16(x.data_, y.data_);
    case 20: return BytewiseLess20(x.data_, y.data_);
    case 24: return BytewiseLess24(x.data_, y.data_);
    case 28: return BytewiseLess28(x.data_, y.data_);
    case 32: return BytewiseLess32(x.data_, y.data_);
    }
  }
  return x < y;
}

struct BytewiseCmpNoTS {
  bool equal(const Slice& x, const Slice& y) const { return x == y; }
  __always_inline
  bool operator()(const Slice& x, const Slice& y) const { return BytewiseLess(x, y); }
  int compare(const Slice& x, const Slice& y) const { return x.compare(y); }
};

struct RevBytewiseCmpNoTS {
  bool equal(const Slice& x, const Slice& y) const { return x == y; }
  bool operator()(const Slice& x, const Slice& y) const { return BytewiseLess(y, x); }
  int compare(const Slice& x, const Slice& y) const { return y.compare(x); }
};

struct VirtualCmpNoTS {
  bool equal(const Slice& x, const Slice& y) const {
    return cmp->CompareWithoutTimestamp(x, y) == 0;
  }
  bool operator()(const Slice& x, const Slice& y) const {
    return cmp->CompareWithoutTimestamp(x, false, y, false) < 0;
  }
  int compare(const Slice& x, const Slice& y) const {
    return cmp->CompareWithoutTimestamp(x, y);
  }
  const Comparator* cmp;
};

} // namespace ROCKSDB_NAMESPACE
