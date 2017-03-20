
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

#ifndef ARANGOD_IRESEARCH__IRESEARCH_DOCUMENT_H
#define ARANGOD_IRESEARCH__IRESEARCH_DOCUMENT_H 1

#include "VocBase/voc-types.h"

#include "IResearchLinkMeta.h"
#include "VelocyPackHelper.h"

#include "store/data_output.hpp"
#include "utils/attributes.hpp"
#include "analysis/token_streams.hpp"

namespace arangodb {
namespace iresearch {

struct IResearchViewMeta;

class Field {
 public:
  Field(std::shared_ptr<std::string> const& name)
    : _name(name) {
  }
  Field(Field&& rhs);
  Field(Field const& rhs);
  Field& operator=(Field const& rhs);
  Field& operator=(Field&& rhs);

  irs::string_ref name() const noexcept {
    TRI_ASSERT(_name);
    return *_name;
  }

  irs::flags const features() const {
    return irs::flags::empty_instance();
  }

  irs::token_stream& get_tokens() const {
    static irs::null_token_stream stream;
    return stream;
  }

  float_t boost() const {
    TRI_ASSERT(_meta);
    return _meta->_boost;
  }

 private:
  friend class FieldIterator;

  std::shared_ptr<std::string> _name; // buffer for field name
  IResearchLinkMeta const* _meta;
}; // Field

class FieldIterator {
 public:
  FieldIterator() noexcept;
  FieldIterator(
    VPackSlice const& doc,
    IResearchLinkMeta const& linkMeta,
    IResearchViewMeta const& viewMeta
  );

  Field const& operator*() const noexcept {
    return _value;
  }

  FieldIterator& operator++() {
    next();
    return *this;
  }

  FieldIterator operator++(int) {
    FieldIterator tmp = *this;
    next();
    return std::move(tmp);
  }

  bool valid() const noexcept {
    return !_stack.empty();
  }

 private:
  struct Level {
    Level(VPackSlice slice, size_t name, IResearchLinkMeta const& meta)
      : it(slice), name(name), meta(&meta) {
    }

    Iterator it;
    size_t name; // length of the name at the current level
    IResearchLinkMeta const* meta; // metadata
  };

  Level& top() noexcept {
    TRI_ASSERT(!_stack.empty());
    return _stack.back();
  }

  Iterator& topIter() noexcept {
    return top().it;
  }

  std::string& nameBuffer() noexcept {
    TRI_ASSERT(_value._name);
    return *_value._name;
  }

  void next();
  void nextTop();
  bool push(VPackSlice slice, IResearchLinkMeta const* topMeta);
  void appendName(irs::string_ref const& name, IteratorValue const& value);

  std::vector<Level> _stack;
  IResearchViewMeta const* _meta;
  Field _value;
}; // FieldIterator

} // iresearch
} // arangodb

#endif
