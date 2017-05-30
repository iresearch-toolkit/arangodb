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

#include "Aql/AstNode.h"
#include "IResearchLinkMeta.h"
#include "VelocyPackHelper.h"

#include "store/data_output.hpp"
#include "utils/attributes.hpp"
#include "analysis/token_streams.hpp"
#include "search/filter.hpp"

NS_BEGIN(iresearch)

class boolean_filter;

NS_END // iresearch

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

////////////////////////////////////////////////////////////////////////////////
/// @brief indexed/stored document field adapter for IResearch
////////////////////////////////////////////////////////////////////////////////
struct Field {
  struct init_stream_t{}; // initialize stream

  static void setCidValue(Field& field, TRI_voc_cid_t& cid);
  static void setCidValue(Field& field, TRI_voc_cid_t& cid, init_stream_t);
  static void setRidValue(Field& field, TRI_voc_rid_t& rid);
  static void setRidValue(Field& field, TRI_voc_rid_t& rid, init_stream_t);

  Field() = default;
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
///        IResearchLinkMeta
////////////////////////////////////////////////////////////////////////////////
class FieldIterator : public std::iterator<std::forward_iterator_tag, Field const> {
 public:
  static FieldIterator const END; // unified end for all field iterators

  explicit FieldIterator();
  FieldIterator(VPackSlice const& doc, IResearchLinkMeta const& linkMeta);

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

  void reset(
    VPackSlice const& doc,
    IResearchLinkMeta const& linkMeta
  );

 private:
  typedef IResearchLinkMeta::TokenizerPool const* TokenizerIterator;

  typedef bool(*Filter)(
    std::string& buffer,
    IResearchLinkMeta const*& rootMeta,
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

  // disallow copy and assign
  FieldIterator(FieldIterator const&) = delete;
  FieldIterator& operator=(FieldIterator const&) = delete;

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
  TokenizerIterator _end{};
  std::vector<Level> _stack;
  std::shared_ptr<std::string> _name; // buffer for field name
  Field _value; // iterator's value
}; // FieldIterator

////////////////////////////////////////////////////////////////////////////////
/// @brief represents stored primary key of the ArangoDB document
////////////////////////////////////////////////////////////////////////////////
class DocumentPrimaryKey {
 public:
  static irs::string_ref const& PK();

  DocumentPrimaryKey() = default;
  DocumentPrimaryKey(TRI_voc_cid_t cid, TRI_voc_rid_t rid) noexcept;

  irs::string_ref const& name() const noexcept { return PK(); }
  bool read(irs::bytes_ref const& in) noexcept;
  bool write(irs::data_output& out) const;

  TRI_voc_cid_t cid() const noexcept { return _keys[0]; }
  void cid(TRI_voc_cid_t cid) noexcept { _keys[0] = cid; }

  TRI_voc_rid_t rid() const noexcept { return _keys[1]; }
  void rid(TRI_voc_rid_t rid) noexcept { _keys[1] = rid; }

 private:
  // FIXME: define storage format (LE or BE)
  uint64_t _keys[2]{}; // TRI_voc_cid_t + TRI_voc_rid_t
}; // DocumentPrimaryKey

struct FilterFactory {
  static irs::filter::ptr filter(TRI_voc_cid_t cid);
  static irs::filter::ptr filter(TRI_voc_cid_t cid, TRI_voc_rid_t rid);
  static bool filter(irs::boolean_filter& rootFilter, arangodb::aql::AstNode const& root);
}; // FilterFactory

NS_END // iresearch
NS_END // arangodb
#endif