#pragma once

#include <rocksdb/slice.h>

namespace rocksdb {

inline
void AdjustValueByOffsetLength(Slice* v, size_t offset, size_t length) {
  if (v->size() > offset) {
    if (v->size() > offset + length) {
      v->size_  = length;
      v->data_ += offset;
    } else {
      v->remove_prefix(offset);
    }
  }
  else {
    *v = Slice(); // empty
  }
}

template<class OffsetLength>
void AdjustValueByOffsetLength(Slice* v, const OffsetLength& ol) {
  AdjustValueByOffsetLength(v, ol.value_data_offset, ol.value_data_length);
}

} // namespace rocksdb
