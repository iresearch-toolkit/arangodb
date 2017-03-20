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

#ifndef ARANGOD_IRESEARCH__IRESEARCH_FILED_ITERATOR_H
#define ARANGOD_IRESEARCH__IRESEARCH_FILED_ITERATOR_H 1

#include <iterator>

#include "search/filter.hpp"

#include "VocBase/voc-types.h"

NS_BEGIN(iresearch)

class token_stream;; // forward declaration

NS_END // iresearch

NS_BEGIN(arangodb)
NS_BEGIN(velocypack)

class Slice; // forward declaration

NS_END // velocypack
NS_END // arangodb

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

struct IResearchLinkMeta; // forward declaration
struct IResearchViewMeta; // forward declaration

class Field1 {
 public:
  irs::string_ref const& name() const;
  irs::flags const features() const;
  irs::token_stream& get_tokens() const;
  float_t boost() const;
};

class IResearchFieldIterator: public std::iterator<std::input_iterator_tag, Field1> {
 public:
  IResearchFieldIterator() noexcept; // denotes end()
  IResearchFieldIterator(
    TRI_voc_cid_t cid,
    TRI_voc_rid_t rid,
    arangodb::velocypack::Slice const& slice,
    IResearchViewMeta const& viewMeta,
    IResearchLinkMeta const& linkMeta
  );
  IResearchFieldIterator(IResearchFieldIterator const& other);
  IResearchFieldIterator(IResearchFieldIterator&& other) noexcept;

  IResearchFieldIterator& operator++();
  IResearchFieldIterator operator++(int);
  bool operator==(IResearchFieldIterator const& other) const noexcept;
  bool operator!=(IResearchFieldIterator const& other) const noexcept;
  value_type& operator*() const;

  static irs::filter::ptr filter(TRI_voc_cid_t cid);
  static irs::filter::ptr filter(TRI_voc_cid_t cid, TRI_voc_rid_t rid);
};

NS_END // iresearch
NS_END // arangodb
#endif
