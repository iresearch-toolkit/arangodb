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

#ifndef ARANGODB_IRESEARCH__IRESEARCH_VELOCY_PACK_HELPER_H
#define ARANGODB_IRESEARCH__IRESEARCH_VELOCY_PACK_HELPER_H 1

#include "velocypack/Slice.h"

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

//////////////////////////////////////////////////////////////////////////////
/// @brief parses a numeric sub-element
/// @return success
//////////////////////////////////////////////////////////////////////////////
template<typename T>
inline bool getNumber(
  T& buf,
  arangodb::velocypack::Slice const& slice
) noexcept {
  if (!slice.isNumber()) {
    return false;
  }

  typedef typename std::conditional<
    std::is_floating_point<T>::value, T, double
  >::type NumType;

  try {
    auto value = slice.getNumber<NumType>();

    buf = static_cast<T>(value);

    return value == static_cast<decltype(value)>(buf);
  } catch (...) {
    // NOOP
  }

  return false;
}

//////////////////////////////////////////////////////////////////////////////
/// @brief parses a numeric sub-element, or uses a default if it does not exist
/// @return success
//////////////////////////////////////////////////////////////////////////////
template<typename T>
inline bool getNumber(
  T& buf,
  arangodb::velocypack::Slice const& slice,
  std::string const& fieldName,
  bool& seen,
  T fallback
) noexcept {
  seen = slice.hasKey(fieldName);

  if (!seen) {
    buf = fallback;

    return true;
  }

  return getNumber(buf, slice.get(fieldName));
}

//////////////////////////////////////////////////////////////////////////////
/// @brief parses a string sub-element, or uses a default if it does not exist
/// @return success
//////////////////////////////////////////////////////////////////////////////
inline bool getString(
  std::string& buf,
  arangodb::velocypack::Slice const& slice,
  std::string const& fieldName,
  bool& seen,
  std::string const& fallback
) noexcept {
  seen = slice.hasKey(fieldName);

  if (!seen) {
    buf = fallback;

    return true;
  }

  auto field = slice.get(fieldName);

  if (!field.isString()) {
    return false;
  }

  buf = field.copyString();

  return true;
}

NS_END // iresearch
NS_END // arangodb
#endif
