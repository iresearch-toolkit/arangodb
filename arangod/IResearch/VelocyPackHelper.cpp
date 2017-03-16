//////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 EMC Corporation
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "VelocyPackHelper.h"

namespace arangodb {
namespace iresearch {

namespace {

// according to Slice.h:330
uint8_t const COMPACT_ARRAY = 0x13;
uint8_t const COMPACT_OBJECT = 0x14;

inline bool isArrayOrObject(VPackSlice const& slice) {
  auto const type = slice.type();
  return VPackValueType::Array == type || VPackValueType::Object == type;
}

inline bool isCompactArrayOrObject(VPackSlice const& slice) {
  TRI_ASSERT(isArrayOrObject(slice));

  auto const head = slice.head();
  return COMPACT_ARRAY == head || COMPACT_OBJECT == head;
}

}

// can't make it noexcept since VPackSlice::getNthOffset is not noexcept
void ObjectIterator::Iterator::reset() {
  TRI_ASSERT(isArrayOrObject(_slice));

  if (!_size) {
    return;
  }

  // according to Iterator.h:160 and Iterator.h:194
  auto const offset = isCompactArrayOrObject(_slice)
    ? _slice.getNthOffset(0)
    : _slice.findDataOffset(_slice.head());

  _value.reset(_slice.start() + offset);
}

void ObjectIterator::Iterator::next() noexcept {
  if (++_value.pos < _size) {
    auto const& value = _value.value;
    _value.reset(value.start() + value.byteSize());
  }
}

ObjectIterator::ObjectIterator(VPackSlice const& slice) {
  if (isArrayOrObject(slice)) {
    _stack.emplace_back(slice);

    while (isArrayOrObject(topValue())) {
      _stack.emplace_back(topValue());
    }
  }
}

ObjectIterator& ObjectIterator::operator++() {
  TRI_ASSERT(valid());

  for (top().next(); !top().valid(); top().next()) {
    _stack.pop_back();

    if (!valid()) {
      return *this;
    }
  }

  while (isArrayOrObject(topValue())) {
    _stack.emplace_back(topValue());
  }

  return *this;
}

} // iresearch
} // arangodb

