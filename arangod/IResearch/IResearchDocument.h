
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

namespace arangodb {
namespace iresearch {

struct IResearchViewMeta;

////////////////////////////////////////////////////////////////////////////////
/// @brief indexed/stored document field adapter for IResearch
////////////////////////////////////////////////////////////////////////////////
struct Field {
  Field() = default;
  Field(Field const&) = default;
  Field(Field&& rhs);
  Field& operator=(Field&& rhs);

  irs::string_ref const& name() const noexcept {
    return _name;
  }

  irs::flags const& features() const {
    TRI_ASSERT(_features);
    return *_features;
  }

  irs::token_stream& get_tokens() const {
    TRI_ASSERT(_tokenizer);
    return *_tokenizer;
  }

  float_t boost() const {
    return _boost;
  }

  bool write(irs::data_output&) const noexcept {
    return true;
  }

  irs::flags const* _features{ &irs::flags::empty_instance() };
  std::shared_ptr<irs::token_stream>_tokenizer;
  irs::string_ref _name;
  float_t _boost{1.f};
}; // Field

////////////////////////////////////////////////////////////////////////////////
/// @brief allows to iterate over the provided VPack accoring the specified
///        IResearchLinkMeta and IResearchViewMeta
////////////////////////////////////////////////////////////////////////////////
class FieldIterator : public std::iterator<std::forward_iterator_tag, Field const> {
 public:
  static FieldIterator END; // unified end for all field iterators

  FieldIterator() = default;

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

  // We don't support postfix increment since it requires
  // deep copy of all buffers and tokenizers which is quite
  // expensive and useless

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
  typedef IResearchLinkMeta::TokenizerPool const* TokenizerIterator;

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

  IteratorValue const& topValue() noexcept {
    return top().it.value();
  }

  std::string& nameBuffer() noexcept {
    TRI_ASSERT(_name);
    return *_name;
  }

  void next();
  IResearchLinkMeta const* nextTop();
  bool push(VPackSlice slice, IResearchLinkMeta const*& topMeta);
  bool setValue(VPackSlice const& value, IResearchLinkMeta const& context);

  void resetTokenizers(IResearchLinkMeta const& context) {
    auto const& tokenizers = context._tokenizers;
    _begin = tokenizers.data();
    _end = _begin + tokenizers.size();
  }

  TokenizerIterator _begin{};
  TokenizerIterator _end{1 + _begin}; // prevent invalid behaviour on first 'next'
  IResearchViewMeta const* _meta{};
  std::vector<Level> _stack;
  std::shared_ptr<std::string> _name; // buffer for field name
  Field _value; // iterator's value
}; // FieldIterator

////////////////////////////////////////////////////////////////////////////////
/// @brief allows to iterate over the specified CID and RID as indexed fields
////////////////////////////////////////////////////////////////////////////////
class DocumentPrimaryKeyIterator : public std::iterator<std::forward_iterator_tag, Field const> {
 public:
  static DocumentPrimaryKeyIterator END; // unified end for all document iterators

  static irs::filter::ptr filter(TRI_voc_cid_t cid);
  static irs::filter::ptr filter(TRI_voc_cid_t cid, TRI_voc_rid_t rid);

  DocumentPrimaryKeyIterator(
    TRI_voc_cid_t const cid,
    TRI_voc_rid_t const rid
  );

  Field const& operator*() const noexcept {
    return _value;
  }

  DocumentPrimaryKeyIterator& operator++() {
    next();
    return *this;
  }

  bool operator==(DocumentPrimaryKeyIterator const& rhs) const noexcept {
    return _next == rhs._next;
  }

  bool operator!=(DocumentPrimaryKeyIterator const& rhs) const noexcept {
    return !(*this == rhs);
  }

  // support range-based traversal
  DocumentPrimaryKeyIterator& begin() { return *this; }
  DocumentPrimaryKeyIterator& end() { return END; }

 private:
  DocumentPrimaryKeyIterator() noexcept : _next{2} { } // end

  void next();
  void setCidValue();
  void setRidValue();

  Field _value;
  TRI_voc_rid_t _cid;
  TRI_voc_rid_t _rid;
  size_t _next{};
}; // DocumentPrimaryKeyIterator

////////////////////////////////////////////////////////////////////////////////
/// @brief represents stored primary key of the ArangoDB document
////////////////////////////////////////////////////////////////////////////////
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

} // iresearch
} // arangodb

#endif
