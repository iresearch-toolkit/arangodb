
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

#include "IResearchDocument.h"
#include "IResearchViewMeta.h"

#include "utils/log.hpp"

namespace arangodb {
namespace iresearch {

namespace {

// wrapper for use of iResearch bstring with the iResearch unbounded_object_pool
struct Buffer : public std::string {
  typedef std::shared_ptr<std::string> ptr;

  static ptr make() {
    auto buf = std::make_shared<std::string>();
    buf->clear(); // clear buffer
    return buf;
  }
};

const size_t DEFAULT_POOL_SIZE = 8; // arbitrary value
irs::unbounded_object_pool<Buffer> s_pool(DEFAULT_POOL_SIZE);

inline irs::string_ref toStringRef(VPackSlice const& slice) {
  TRI_ASSERT(slice.isString());

  size_t size;
  auto const* str = slice.getString(size);

  return irs::string_ref(str, size);
}

// appends the specified 'value' to 'out'
inline void append(std::string& out, size_t value) {
  auto const size = out.size(); // intial size
  out.resize(size + 21); // enough to hold all numbers up to 64-bits
  auto const written = sprintf(&out[size], IR_SIZE_T_SPECIFIER, value);
  out.resize(size + written);
}

// returns 'rootMeta' in case if can't find the specified 'field'
inline IResearchLinkMeta const* findMeta(
    irs::string_ref const& key,
    IResearchLinkMeta const* rootMeta) {
  TRI_ASSERT(rootMeta);

  auto& fields = rootMeta->_fields;

  auto const it = fields.find(std::string(key)); // TODO: use string_ref
  return fields.end() == it ? rootMeta : &(it->second);
}

}

// ----------------------------------------------------------------------------
// --SECTION--                                     FieldIterator implementation
// ----------------------------------------------------------------------------

Field::Field(Field&& rhs)
  : _name(std::move(rhs._name)),
    _meta(rhs._meta) {
  rhs._meta = nullptr;
}

Field::Field(Field const& rhs) {
  *this = rhs;
}

Field& Field::operator=(Field const& rhs) {
  if (this != &rhs) {
    _name = s_pool.emplace(); // init buffer for name
    *_name = *rhs._name; // copy content
    _meta = rhs._meta;
  }
  return *this;
}

Field& Field::operator=(Field&& rhs) {
  if (this != &rhs) {
    _name = std::move(rhs._name);
    _meta = rhs._meta;
    rhs._meta = nullptr;
  }
  return *this;
}

void FieldIterator::appendName(
    irs::string_ref const& name,
    IteratorValue const& value) {
  auto& out = nameBuffer();

  if (!out.empty()) {
    out += _meta->_nestingDelimiter;
  }

  // TODO: add name mangling here
  out.append(name.c_str(), name.size());

  if (VPackValueType::Array == value.type) {
    out += _meta->_nestingListOffsetPrefix;
    append(out, value.pos);
    out += _meta->_nestingListOffsetSuffix;
  }
}

void FieldIterator::nextTop() {
  auto& it = top().it;
  auto const* topMeta = top().meta;
  bool const isArray = top().it.value().type == VPackValueType::Array;

  while (it.next()) {
    nameBuffer().resize(top().name);

    auto& value = it.value();

    auto key = irs::string_ref::nil;
    if (!isArray) {
      key = toStringRef(value.key);
      auto* meta = findMeta(key, topMeta);

      if (topMeta->_includeAllFields && topMeta == meta) {
        // filter out fields
        continue;
      }

      top().meta = topMeta = meta;
    }
    appendName(key, value);

    break;
  }
}

bool FieldIterator::push(VPackSlice slice, IResearchLinkMeta const* topMeta) {
  while (isArrayOrObject(slice)) {
    _stack.emplace_back(slice, nameBuffer().size(), *topMeta);

    auto& it = top().it;

    if (!it.valid()) {
      // empty object or array
      return false;
    }

    auto& value = it.value();

    auto key = irs::string_ref::nil;
    if (value.type != VPackValueType::Array) {
      key = toStringRef(value.key);
      auto* meta = findMeta(key, topMeta);

      if (topMeta->_includeAllFields && topMeta == meta) {
        // filter out fields
        return false;
      }

      top().meta = topMeta = meta;
    }
    appendName(key, value);

    slice = value.value;
  }

  return true;
}

void FieldIterator::next() {
  do {
    // advance top iterator
    nextTop();

    // pop all exhausted iterators
    for (; !topIter().valid(); nextTop()) {
      _stack.pop_back();

      if (!valid()) {
        return;
      }

      // reset name to previous size
      nameBuffer().resize(top().name);
    }

  } while (!push(topIter().value().value, top().meta));
}

FieldIterator::FieldIterator() noexcept
  : _value(nullptr) {
  // default constructor doesn't require buffer
}

FieldIterator::FieldIterator(
    VPackSlice const& doc,
    IResearchLinkMeta const& linkMeta,
    IResearchViewMeta const& viewMeta)
  : _meta(&viewMeta), _value(s_pool.emplace()) {
  if (isArrayOrObject(doc) && !push(doc, &linkMeta)) {
    next();
  }
}

} // iresearch
} // arangodb
