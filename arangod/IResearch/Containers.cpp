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

#include "Containers.h"

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

template<>
size_t Hasher::operator()<irs::bytes_ref>(irs::bytes_ref const& value) const {
  static irs::bytes_ref_hash_t hasher;

  return hasher(value);
}

template<>
size_t Hasher::operator()<irs::string_ref>(irs::string_ref const& value) const {
  static irs::string_ref_hash_t hasher;

  return hasher(value);
}

NS_END // iresearch
NS_END // arangodb

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
