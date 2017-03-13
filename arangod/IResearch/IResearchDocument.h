
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

#include "store/data_output.hpp"
#include "utils/attributes.hpp"
#include "analysis/token_streams.hpp"

class Field {
 public:
  irs::string_ref const& name() const {
    static irs::string_ref rr = "ttt";
    return rr;
  }

  irs::flags const features() const {
    return irs::flags::empty_instance();
  }

  irs::token_stream& get_tokens() const {
    static irs::null_token_stream stream;
    return stream;
  }

  float_t boost() const {
    return 1.f;
  }
};

// stored arangodb document primary key
class StoredPrimaryKey {
 public:
  static irs::string_ref const NAME;

  StoredPrimaryKey(TRI_voc_cid_t cid, TRI_voc_rid_t rid) noexcept
    : _keys{ rid, cid } {
    static_assert(
      sizeof(_keys) == sizeof(cid) + sizeof(rid),
      "Invalid size"
    );
  }

  irs::string_ref const& name() const noexcept {
    return NAME;
  }

  bool write(irs::data_output& out) const;

 private:
  uint64_t _keys[2]; // revisionId + collectionId
}; // StoredPrimaryKey

#endif
