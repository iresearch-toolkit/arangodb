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

#ifndef ARANGODB_IRESEARCH__IRESEARCH_LINK_META_H
#define ARANGODB_IRESEARCH__IRESEARCH_LINK_META_H 1

#include <locale>
#include <map>
#include <mutex>

#include <velocypack/Builder.h>
#include <velocypack/velocypack-aliases.h>

#include "shared.hpp"
#include "analysis/analyzer.hpp"

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

// -----------------------------------------------------------------------------
// --SECTION--                                                      public types
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief enum of possible ways to process list values
////////////////////////////////////////////////////////////////////////////////
namespace ListValuation {
  enum Type {
    IGNORED, // skip indexing list value
    ORDERED, // index using relative offset as attribute name
    MULTIVALUED, // index by treating listed values as alternatives (SQL IN)
  };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief metadata describing how to process a field in a collection
////////////////////////////////////////////////////////////////////////////////
struct IResearchLinkMeta {
  struct Mask {
    bool _boost;
    bool _depth;
    bool _fields;
    bool _listValuation;
    bool _locale;
    bool _tokenizers;
    Mask(bool mask = false) noexcept;
  };

  // name -> {args, ptr}
  typedef std::multimap<std::string, std::pair<std::string, irs::analysis::analyzer::ptr>> Tokenizers;

  float_t _boost;
  size_t _depth;
  std::map<std::string, IResearchLinkMeta> _fields;
  ListValuation::Type _listValuation;
  std::mutex _mutex; // for use with _tokenizers
  std::locale _locale;
  Tokenizers _tokenizers;
  // NOTE: if adding fields don't forget to modify the default constructor !!!
  // NOTE: if adding fields don't forget to modify the copy constructor !!!
  // NOTE: if adding fields don't forget to modify the move constructor !!!
  // NOTE: if adding fields don't forget to modify the copy assignment operator !!!
  // NOTE: if adding fields don't forget to modify the move assignment operator !!!
  // NOTE: if adding fields don't forget to modify the comparison operator !!!
  // NOTE: if adding fields don't forget to modify IResearchLinkMeta::Mask !!!
  // NOTE: if adding fields don't forget to modify IResearchLinkMeta::Mask constructor !!!
  // NOTE: if adding fields don't forget to modify the init(...) function !!!
  // NOTE: if adding fields don't forget to modify the json(...) function !!!
  // NOTE: if adding fields don't forget to modify the memSize() function !!!

  IResearchLinkMeta();
  IResearchLinkMeta(IResearchLinkMeta const& other);
  IResearchLinkMeta(IResearchLinkMeta&& other) noexcept;

  IResearchLinkMeta& operator=(IResearchLinkMeta&& other) noexcept;
  IResearchLinkMeta& operator=(IResearchLinkMeta const& other);

  bool operator==(IResearchLinkMeta const& other) const noexcept;
  bool operator!=(IResearchLinkMeta const& other) const noexcept;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief return default IResearchLinkMeta values
  ////////////////////////////////////////////////////////////////////////////////
  static const IResearchLinkMeta& DEFAULT();

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief initialize IResearchLinkMeta with values from a JSON description
  ///        return success or set 'errorField' to specific field with error
  ///        on failure state is undefined
  /// @param mask if set reflects which fields were initialized from JSON
  ////////////////////////////////////////////////////////////////////////////////
  bool init(
    VPackSlice const& slice,
    std::string& errorField,
    IResearchLinkMeta const& defaults = DEFAULT(),
    Mask* mask = nullptr
  ) noexcept;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief fill and return a JSON description of a IResearchLinkMeta object
  ///        do not fill values identical to ones available in 'ignoreEqual'
  ///        or (if 'mask' != nullptr) values in 'mask' that are set to false
  ///        return success or set TRI_set_errno(...) and return false
  ////////////////////////////////////////////////////////////////////////////////
  bool json(
    VPackBuilder& builder,
    IResearchLinkMeta const* ignoreEqual = nullptr,
    Mask const* mask = nullptr
  ) const;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief amount of memory in bytes occupied by this index
  ////////////////////////////////////////////////////////////////////////////////
  size_t memSize() const;
};

NS_END // iresearch
NS_END // arangodb
#endif
