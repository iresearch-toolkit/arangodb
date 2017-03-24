
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
#include "search/filter.hpp"

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

struct IResearchViewMeta;

class Field {
 public:
  Field() = default;
  Field(Field&& rhs);
  Field(Field const& rhs);
  Field& operator=(Field const& rhs);
  Field& operator=(Field&& rhs);

  irs::string_ref name() const noexcept {
    TRI_ASSERT(_name);
    return *_name;
  }

  irs::flags const& features() const {
    return irs::flags::empty_instance();
  }

  irs::token_stream& get_tokens() const {
    TRI_ASSERT(_tokenizer);
    return *_tokenizer;
  }

  float_t boost() const {
    TRI_ASSERT(_meta);
    return _meta->_boost;
  }

 private:
  friend class FieldIterator;

  irs::analysis::analyzer::ptr _tokenizer;
  std::shared_ptr<std::string> _name; // buffer for field name
  IResearchLinkMeta const* _meta{};
}; // Field

class FieldIterator : public std::iterator<std::forward_iterator_tag, const Field> {
 public:
  static FieldIterator END;

  static irs::filter::ptr filter(TRI_voc_cid_t cid);
  static irs::filter::ptr filter(TRI_voc_cid_t cid, TRI_voc_rid_t rid);

  FieldIterator() = default;

  FieldIterator(
    VPackSlice const& doc,
    IResearchLinkMeta const& linkMeta,
    IResearchViewMeta const& viewMeta
  );

  FieldIterator(
    TRI_voc_cid_t cid,
    TRI_voc_rid_t rid,
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

  bool operator==(FieldIterator const& rhs) const noexcept {
    return _stack == rhs._stack;
  }

  bool operator!=(FieldIterator const& rhs) const noexcept {
    return !(*this == rhs);
  }

  // support range based traversal
  FieldIterator& begin() noexcept { return *this; }
  FieldIterator& end() noexcept { return END; };

 private:
  typedef bool(*Filter)(
    std::string& buffer,
    IResearchLinkMeta const*& rootMeta,
    IResearchViewMeta const& viewMeta,
    IteratorValue const& value
  );

  struct Level {
    Level(VPackSlice slice, size_t nameLength, IResearchLinkMeta const& meta, Filter filter)
      : it(slice), nameLength(nameLength), meta(&meta), filter(filter) {
    }

    bool operator==(Level const& rhs) const noexcept {
      return it == rhs.it;
    }

    bool operator !=(Level const& rhs) const noexcept {
      return !(*this == rhs);
    }

    Iterator it;
    size_t nameLength; // length of the name at the current level
    IResearchLinkMeta const* meta; // metadata
    Filter filter;
  }; // Level

  Level& top() noexcept {
    TRI_ASSERT(!_stack.empty());
    return _stack.back();
  }

  std::string& nameBuffer() noexcept {
    TRI_ASSERT(_value._name);
    return *_value._name;
  }

  void next();
  IResearchLinkMeta const* nextTop();
  bool push(VPackSlice slice, IResearchLinkMeta const*& topMeta);

  IResearchLinkMeta::Tokenizers::iterator _begin;
  IResearchLinkMeta::Tokenizers::iterator _end;
  std::vector<Level> _stack;
  IResearchViewMeta const* _meta{};
  Field _value; // iterator's value
}; // FieldIterator

// stored ArangoDB document primary key
class DocumentPrimaryKey {
 public:
  static irs::string_ref const& PK();

  DocumentPrimaryKey(TRI_voc_cid_t cid, TRI_voc_rid_t rid) noexcept;

  irs::string_ref const& name() const noexcept { return PK(); }
  bool write(irs::data_output& out) const;

  DocumentPrimaryKey const* begin() const noexcept { return this; }
  DocumentPrimaryKey const* end() const noexcept { return 1 + this; }

 private:
  uint64_t _keys[2]; // TRI_voc_cid_t + TRI_voc_rid_t
}; // DocumentPrimaryKey

NS_END // iresearch
NS_END // arangodb
#endif