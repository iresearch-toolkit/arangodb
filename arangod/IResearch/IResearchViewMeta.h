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

#ifndef ARANGODB_IRESEARCH__IRESEARCH_VIEW_META_H
#define ARANGODB_IRESEARCH__IRESEARCH_VIEW_META_H 1

#include <locale>
#include <unordered_set>

#include "shared.hpp"
#include "iql/parser_common.hpp"
#include "utils/attributes.hpp"

#include "VocBase/voc-types.h"

NS_BEGIN(arangodb)
NS_BEGIN(velocypack)

class Builder; // forward declarations
struct ObjectBuilder; // forward declarations
class Slice; // forward declarations

NS_END // velocypack
NS_END // arangodb

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

// -----------------------------------------------------------------------------
// --SECTION--                                                      public types
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief enum of possible consolidation policy thresholds
////////////////////////////////////////////////////////////////////////////////
namespace ConsolidationPolicy {
  enum Type {
    BYTES, // {threshold} > segment_bytes / (all_segment_bytes / #segments)
    BYTES_ACCUM, // {threshold} > (segment_bytes + sum_of_merge_candidate_segment_bytes) / all_segment_bytes
    COUNT, // {threshold} > segment_docs{valid} / (all_segment_docs{valid} / #segments)
    FILL,  // {threshold} > #segment_docs{valid} / (#segment_docs{valid} + #segment_docs{removed})
    eLast
  };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief metadata describing the IResearch view
////////////////////////////////////////////////////////////////////////////////
struct IResearchViewMeta {
  struct CommitBaseMeta {
    size_t _cleanupIntervalStep; // issue cleanup after <count> commits (0 == disable)
    struct {
      size_t _intervalStep; // apply consolidation policy with every Nth commit (0 == disable)
      float _threshold;
    } _consolidate[ConsolidationPolicy::eLast]; // consolidation thresholds
    bool operator==(CommitBaseMeta const& other) const noexcept;
  };

  struct CommitBulkMeta: public CommitBaseMeta {
    size_t _commitIntervalBatchSize; // issue commit after <count> records bulk indexed
    bool operator==(CommitBulkMeta const& other) const noexcept;
    bool operator!=(CommitBulkMeta const& other) const noexcept;
  };

  struct CommitItemMeta: public CommitBaseMeta {
    size_t _commitIntervalMsec; // issue commit after <interval> milliseconds (0 == disable)
    bool operator==(CommitItemMeta const& other) const noexcept;
    bool operator!=(CommitItemMeta const& other) const noexcept;
  };

  struct Mask {
    bool _collections;
    bool _commitBulk;
    bool _commitItem;
    bool _dataPath;
    bool _iid;
    bool _locale;
    bool _name;
    bool _nestingDelimiter;
    bool _nestingListOffsetPrefix;
    bool _nestingListOffsetSuffix;
    bool _scorers;
    bool _threadsMaxIdle;
    bool _threadsMaxTotal;
    Mask(bool mask = false) noexcept;
  };

  std::unordered_set<TRI_voc_cid_t> _collections; // fully indexed collection IDs
  CommitBulkMeta _commitBulk;
  CommitItemMeta _commitItem;
  std::string _dataPath; // data file path
  irs::flags _features; // non-persisted dynamic value based on scorers
  TRI_idx_iid_t _iid;
  std::locale _locale;
  std::string _name; // IResearch view name
  std::string _nestingDelimiter;
  std::string _nestingListOffsetPrefix;
  std::string _nestingListOffsetSuffix;
  irs::iql::order_functions _scorers; // supported scorers
  size_t _threadsMaxIdle; // maximum idle number of threads for single-run tasks
  size_t _threadsMaxTotal; // maximum total number of threads for single-run tasks
  // NOTE: if adding fields don't forget to modify the default constructor !!!
  // NOTE: if adding fields don't forget to modify the copy constructor !!!
  // NOTE: if adding fields don't forget to modify the move constructor !!!
  // NOTE: if adding fields don't forget to modify the comparison operator !!!
  // NOTE: if adding fields don't forget to modify IResearchLinkMeta::Mask !!!
  // NOTE: if adding fields don't forget to modify IResearchLinkMeta::Mask constructor !!!
  // NOTE: if adding fields don't forget to modify the init(...) function !!!
  // NOTE: if adding fields don't forget to modify the json(...) function !!!
  // NOTE: if adding fields don't forget to modify the memSize() function !!!

  IResearchViewMeta();
  IResearchViewMeta(IResearchViewMeta const& other);
  IResearchViewMeta(IResearchViewMeta&& other) noexcept;

  IResearchViewMeta& operator=(IResearchViewMeta&& other) noexcept;
  IResearchViewMeta& operator=(IResearchViewMeta const& other);

  bool operator==(IResearchViewMeta const& other) const noexcept;
  bool operator!=(IResearchViewMeta const& other) const noexcept;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief return default IResearchViewMeta values
  ////////////////////////////////////////////////////////////////////////////////
  static const IResearchViewMeta& DEFAULT();

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief initialize IResearchViewMeta with values from a JSON description
  ///        return success or set 'errorField' to specific field with error
  ///        on failure state is undefined
  /// @param mask if set reflects which fields were initialized from JSON
  ////////////////////////////////////////////////////////////////////////////////
  bool init(
    arangodb::velocypack::Slice const& slice,
    std::string& errorField,
    IResearchViewMeta const& defaults = DEFAULT(),
    Mask* mask = nullptr
  ) noexcept;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief fill and return a JSON description of a IResearchViewMeta object
  ///        do not fill values identical to ones available in 'ignoreEqual'
  ///        or (if 'mask' != nullptr) values in 'mask' that are set to false
  ///        elements are appended to an existing object
  ///        return success or set TRI_set_errno(...) and return false
  ////////////////////////////////////////////////////////////////////////////////
  bool json(
    arangodb::velocypack::Builder& builder,
    IResearchViewMeta const* ignoreEqual = nullptr,
    Mask const* mask = nullptr
  ) const;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief fill and return a JSON description of a IResearchViewMeta object
  ///        do not fill values identical to ones available in 'ignoreEqual'
  ///        or (if 'mask' != nullptr) values in 'mask' that are set to false
  ///        elements are appended to an existing object
  ///        return success or set TRI_set_errno(...) and return false
  ////////////////////////////////////////////////////////////////////////////////
  bool json(
    arangodb::velocypack::ObjectBuilder const& builder,
    IResearchViewMeta const* ignoreEqual = nullptr,
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