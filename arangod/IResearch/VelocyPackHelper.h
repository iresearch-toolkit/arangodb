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

#ifndef ARANGODB_IRESEARCH__IRESEARCH_VELOCY_PACK_HELPER_H
#define ARANGODB_IRESEARCH__IRESEARCH_VELOCY_PACK_HELPER_H 1

#include "Basics/Common.h"

#include "velocypack/Slice.h"
#include "velocypack/velocypack-aliases.h"

namespace arangodb {
namespace iresearch {

//////////////////////////////////////////////////////////////////////////////
/// @brief parses a numeric sub-element
/// @return success
//////////////////////////////////////////////////////////////////////////////
template<typename T>
inline bool getNumber(
  T& buf,
  arangodb::velocypack::Slice const& slice
) noexcept {
  if (!slice.isNumber()) {
    return false;
  }

  typedef typename std::conditional<
    std::is_floating_point<T>::value, T, double
  >::type NumType;

  try {
    auto value = slice.getNumber<NumType>();

    buf = static_cast<T>(value);

    return value == static_cast<decltype(value)>(buf);
  } catch (...) {
    // NOOP
  }

  return false;
}

//////////////////////////////////////////////////////////////////////////////
/// @brief parses a numeric sub-element, or uses a default if it does not exist
/// @return success
//////////////////////////////////////////////////////////////////////////////
template<typename T>
inline bool getNumber(
  T& buf,
  arangodb::velocypack::Slice const& slice,
  std::string const& fieldName,
  bool& seen,
  T fallback
) noexcept {
  seen = slice.hasKey(fieldName);

  if (!seen) {
    buf = fallback;

    return true;
  }

  return getNumber(buf, slice.get(fieldName));
}

//////////////////////////////////////////////////////////////////////////////
/// @brief parses a string sub-element, or uses a default if it does not exist
/// @return success
//////////////////////////////////////////////////////////////////////////////
inline bool getString(
  std::string& buf,
  arangodb::velocypack::Slice const& slice,
  std::string const& fieldName,
  bool& seen,
  std::string const& fallback
) noexcept {
  seen = slice.hasKey(fieldName);

  if (!seen) {
    buf = fallback;

    return true;
  }

  auto field = slice.get(fieldName);

  if (!field.isString()) {
    return false;
  }

  buf = field.copyString();

  return true;
}

//////////////////////////////////////////////////////////////////////////////
/// @class ObjectIterator
/// @return allows to traverse VPack objects in a unified way
//////////////////////////////////////////////////////////////////////////////
class ObjectIterator {
 public:
  ////////////////////////////////////////////////////////////////////////////
  /// @struct Value
  /// @brief represents of value of the iterator
  ////////////////////////////////////////////////////////////////////////////
  struct Value {
    explicit Value(VPackValueType type) noexcept
      : type(type) {
    }

    void reset(uint8_t const* start) noexcept {
      // whether or not we're in the context of array or object
      VPackValueLength const isArray = VPackValueType::Array != type;

      key = VPackSlice(start);
      value = VPackSlice(start + isArray*key.byteSize());
    }

    ///////////////////////////////////////////////////////////////////////////
    /// @brief type of the current level (Array or Object)
    ///////////////////////////////////////////////////////////////////////////
    VPackValueType type;

    ///////////////////////////////////////////////////////////////////////////
    /// @brief position at the current level
    ///////////////////////////////////////////////////////////////////////////
    VPackValueLength pos{};

    ///////////////////////////////////////////////////////////////////////////
    /// @brief current key at the current level
    ///          type == Array --> key == value;
    ///////////////////////////////////////////////////////////////////////////
    VPackSlice key;

    ///////////////////////////////////////////////////////////////////////////
    /// @brief current value at the current level
    ///////////////////////////////////////////////////////////////////////////
    VPackSlice value;
  }; // Value

  ObjectIterator() = default;
  explicit ObjectIterator(VPackSlice const& slice);

  /////////////////////////////////////////////////////////////////////////////
  /// @brief prefix increment operator
  /////////////////////////////////////////////////////////////////////////////
  ObjectIterator& operator++();

  /////////////////////////////////////////////////////////////////////////////
  /// @brief postfix increment operator
  /////////////////////////////////////////////////////////////////////////////
  ObjectIterator operator++(int) {
    ObjectIterator tmp = *this;
    ++(*this);
    return tmp;
  }

  /////////////////////////////////////////////////////////////////////////////
  /// @return reference to the value at the topmost level of the hierarchy
  /////////////////////////////////////////////////////////////////////////////
  Value const& operator*() const noexcept {
    TRI_ASSERT(valid());
    return *_stack.back();
  }

  /////////////////////////////////////////////////////////////////////////////
  /// @return true, if iterator is valid, false otherwise
  /////////////////////////////////////////////////////////////////////////////
  bool valid() const noexcept {
    return !_stack.empty();
  }

  /////////////////////////////////////////////////////////////////////////////
  /// @return current hierarchy depth
  /////////////////////////////////////////////////////////////////////////////
  size_t depth() const noexcept {
    return _stack.size();
  }

  /////////////////////////////////////////////////////////////////////////////
  /// @return value at the specified hierarchy depth
  /////////////////////////////////////////////////////////////////////////////
  Value const& value(size_t depth) const noexcept {
    TRI_ASSERT(depth < _stack.size());
    return *_stack[depth];
  }

  /////////////////////////////////////////////////////////////////////////////
  /// @brief visits each level of the current hierarchy
  /////////////////////////////////////////////////////////////////////////////
  template<typename Visitor>
  void visit(Visitor visitor) const {
    for (auto& it : _stack) {
      visitor(*it);
    }
  }

  bool operator==(ObjectIterator const& rhs) const noexcept {
    return _stack == rhs._stack;
  }

  bool operator!=(ObjectIterator const& rhs) const noexcept {
    return !(*this == rhs);
  }

 private:
  class Iterator {
   public:
    explicit Iterator(VPackSlice const& slice)
      : _slice(slice),
        _size(slice.length()),
        _value(slice.type()) {
      reset();
    }

    void next() noexcept;
    void reset();

    Value const& value() const noexcept {
      return operator*();
    }

    Value const& operator*() const noexcept {
      return _value;
    }

    bool valid() const noexcept {
      return _value.pos < _size;
    }

    bool operator==(Iterator const& rhs) const noexcept {
      return _slice.start() == rhs._slice.start()
        && _value.pos == rhs._value.pos;
    }

    bool operator!=(Iterator const& rhs) const noexcept {
      return !(*this == rhs);
    }

  private:
    VPackSlice _slice;
    VPackValueLength const _size;
    Value _value;
  }; // Iterator

  Iterator& top() noexcept {
    TRI_ASSERT(_stack.empty());
    return _stack.back();
  }

  VPackSlice const& topValue() noexcept {
    return top().value().value;
  }

  std::vector<Iterator> _stack;
}; // ObjectIterator

} // iresearch
} // arangodb
#endif
