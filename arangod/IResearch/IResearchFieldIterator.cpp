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

#include "analysis/token_streams.hpp"
#include "search/boolean_filter.hpp"
#include "search/term_filter.hpp"
#include "IResearchLinkMeta.h"

#include "IResearchFieldIterator.h"

NS_LOCAL

irs::string_ref CID_FIELD("@_CID");
irs::string_ref RID_FIELD("@_REV");

const size_t DEFAULT_POOL_SIZE = 8; // arbitrary value

// wrapper for use of iResearch bstring with the iResearch unbounded_object_pool
struct Buffer: public irs::bstring {
  typedef std::shared_ptr<irs::bstring> ptr;
  static ptr make() { return std::make_shared<irs::bstring>(); }
};

// static pool of thread-safe buffers
Buffer::ptr getBuffer() {
  static irs::unbounded_object_pool<Buffer> pool(DEFAULT_POOL_SIZE);
  auto buf = pool.emplace();

  buf->clear();

  return buf;
}

NS_END

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

irs::string_ref const& Field1::name() const {
  static irs::string_ref rr = "ttt";
  return rr;
}

irs::flags const Field1::features() const {
  return irs::flags::empty_instance();
}

irs::token_stream& Field1::get_tokens() const {
  static irs::null_token_stream stream;
  return stream;
}

float_t Field1::boost() const {
  return 1.f;
}

IResearchFieldIterator::IResearchFieldIterator() noexcept {
  // FIXME TODO
}

IResearchFieldIterator::IResearchFieldIterator(
  TRI_voc_cid_t cid,
  TRI_voc_rid_t rid,
  arangodb::velocypack::Slice const& slice,
  IResearchViewMeta const& viewMeta,
  IResearchLinkMeta const& linkMeta
) {
  // FIXME TODO
}

IResearchFieldIterator::IResearchFieldIterator(IResearchFieldIterator const& other) {
  // FIXME TODO
}

IResearchFieldIterator::IResearchFieldIterator(IResearchFieldIterator&& other) noexcept {
  // FIXME TODO
}

IResearchFieldIterator& IResearchFieldIterator::operator++() {
  return *this; // FIXME TODO
}

IResearchFieldIterator IResearchFieldIterator::operator++(int) {
  return *this; // FIXME TODO
}

bool IResearchFieldIterator::operator==(IResearchFieldIterator const& other) const noexcept {
  return true; // FIXME TODO
}

bool IResearchFieldIterator::operator!=(IResearchFieldIterator const& other) const noexcept {
  return !(*this == other);
}

IResearchFieldIterator::value_type& IResearchFieldIterator::operator*() const {
  value_type v;
  return v; // FIXME TODO
}

/*static*/ irs::filter::ptr IResearchFieldIterator::filter(TRI_voc_cid_t cid) {
  irs::bytes_ref cidTerm(reinterpret_cast<irs::byte_type*>(&cid, sizeof(cid)));
  auto filter = irs::by_term::make();

  // filter matching on cid
  static_cast<irs::by_term&>(*filter).field(CID_FIELD).term(cidTerm);

  return std::move(filter);
}

/*static*/ irs::filter::ptr IResearchFieldIterator::filter(
  TRI_voc_cid_t cid,
  TRI_voc_rid_t rid
) {
  irs::bytes_ref cidTerm(reinterpret_cast<irs::byte_type*>(&cid, sizeof(cid)));
  irs::bytes_ref ridTerm(reinterpret_cast<irs::byte_type*>(&rid, sizeof(rid)));
  auto filter = irs::And::make();

  // filter matching on cid and rid
  static_cast<irs::And&>(*filter).add<irs::by_term>().field(CID_FIELD).term(cidTerm);
  static_cast<irs::And&>(*filter).add<irs::by_term>().field(RID_FIELD).term(ridTerm);

  return std::move(filter);
}

NS_END // iresearch
NS_END // arangodb

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
