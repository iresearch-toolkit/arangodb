
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

inline bool inObjectFiltered(
    std::string& buffer,
    IResearchLinkMeta const*& rootMeta,
    IResearchViewMeta const& /*viewMeta*/,
    IteratorValue const& value) {
  TRI_ASSERT(value.key.isString());

  auto const key = toStringRef(value.key);

  auto const meta = findMeta(key, rootMeta);

  if (meta == rootMeta) {
    return false;
  }

  buffer.append(key.c_str(), key.size());
  rootMeta = meta;

  return true;
}

inline bool inObject(
    std::string& buffer,
    IResearchLinkMeta const*& rootMeta,
    IResearchViewMeta const& /*viewMeta*/,
    IteratorValue const& value) {
  auto const key = toStringRef(value.key);

  buffer.append(key.c_str(), key.size());
  rootMeta = findMeta(key, rootMeta);

  return true;
}

inline bool inArrayOrdered(
    std::string& buffer,
    IResearchLinkMeta const*& /*rootMeta*/,
    IResearchViewMeta const& viewMeta,
    IteratorValue const& value) {

  buffer += viewMeta._nestingListOffsetPrefix;
  append(buffer, value.pos);
  buffer += viewMeta._nestingListOffsetSuffix;

  return true;
}

inline bool inArray(
    std::string& /*buffer*/,
    IResearchLinkMeta const*& /*rootMeta*/,
    IResearchViewMeta const& /*viewMeta*/,
    IteratorValue const& /*value*/) noexcept {
  // does nothing
  return true;
}

typedef bool(*Filter)(
  std::string& buffer,
  IResearchLinkMeta const*& rootMeta,
  IResearchViewMeta const& viewMeta,
  IteratorValue const& value
);

Filter valueAcceptors[] = {
  &inObject,         // type == Object, nestListValues == false, includeAllValues == false
  &inObjectFiltered, // type == Object, nestListValues == false, includeAllValues == true
  &inObject,         // type == Object, nestListValues == true , includeAllValues == false
  &inObjectFiltered, // type == Object, nestListValues == true , includeAllValues == true
  &inArray,          // type == Array , nestListValues == flase, includeAllValues == false
  &inArray,          // type == Array , nestListValues == flase, includeAllValues == true
  &inArrayOrdered,   // type == Array , nestListValues == true,  includeAllValues == false
  &inArrayOrdered    // type == Array , nestListValues == true,  includeAllValues == false
};

inline Filter getFilter(
    VPackSlice value,
    IResearchLinkMeta const& meta) noexcept {
  TRI_ASSERT(isArrayOrObject(value));

  return valueAcceptors[
    4 * value.isArray()
      + 2 * meta._nestListValues
      + meta._includeAllFields
   ];
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

void FieldIterator::nextTop() {
  auto& name = nameBuffer();
  auto& level = top();
  auto& it = level.it;
  auto const* topMeta = level.meta;
  auto const filter = level.filter;

  name.resize(level.nameLength);
  while (it.next() && !(filter(name, topMeta, *_meta, it.value()))) {
    // filtered out
    name.resize(level.nameLength);
  }
}

bool FieldIterator::push(VPackSlice slice, IResearchLinkMeta const* topMeta) {
  auto& name = nameBuffer();

  while (isArrayOrObject(slice)) {
    if (!name.empty() && !slice.isArray()) {
      name += _meta->_nestingDelimiter;
    }

    auto const filter = getFilter(slice, *topMeta);

    _stack.emplace_back(slice, name.size(), *topMeta, filter);

    auto& it = top().it;

    if (!it.valid()) {
      // empty object or array, skip it
      return false;
    }

    auto& value = it.value();

    if (!filter(name, topMeta, *_meta, value)) {
      // filtered out
      continue;
    }

    slice = value.value;
  }

  return true;
}

void FieldIterator::next() {
  do {
    // advance top iterator
    nextTop();

    // pop all exhausted iterators
    for (; !top().it.valid(); nextTop()) {
      _stack.pop_back();

      if (!valid()) {
        // reached the end
        return;
      }

      // reset name to previous size
      nameBuffer().resize(top().nameLength);
    }

  } while (!push(top().it.value().value, top().meta));
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
