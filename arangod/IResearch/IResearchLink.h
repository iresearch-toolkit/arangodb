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

#ifndef ARANGOD_IRESEARCH__IRESEARCH_LINK_H
#define ARANGOD_IRESEARCH__IRESEARCH_LINK_H 1

#include "IResearchLinkMeta.h"
#include "IResearchView.h"

#include "Indexes/Index.h"

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

class IResearchLink final: public Index {
 public:
  typedef std::shared_ptr<IResearchLink> ptr;

  IResearchLink(
    TRI_idx_iid_t iid,
    arangodb::LogicalCollection* collection,
    IResearchLinkMeta&& meta,
    IResearchView::ptr view
  );

  bool allowExpansion() const override;
  bool canBeDropped() const override;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief called when the iResearch Link is dropped
  ////////////////////////////////////////////////////////////////////////////////
  int drop() override;

  bool hasBatchInsert() const override;
  bool hasSelectivityEstimate() const override;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief insert an ArangoDB document into an iResearch View using '_meta' params
  ////////////////////////////////////////////////////////////////////////////////
  int insert(
    transaction::Methods* trx,
    TRI_voc_rid_t rid,
    VPackSlice const& doc,
    bool isRollback
  ) override;

  bool isPersistent() const override;
  bool isSorted() const override;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief index comparator, used by the coordinator to detect if the specified
  ///        definition is the same as this link
  ////////////////////////////////////////////////////////////////////////////////
  bool matchesDefinition(VPackSlice const& slice) const override;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief amount of memory in bytes occupied by this iResearch Link
  ////////////////////////////////////////////////////////////////////////////////
  size_t memory() const override;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief remove an ArangoDB document from an iResearch View
  ////////////////////////////////////////////////////////////////////////////////
  int remove(
    transaction::Methods* trx,
    TRI_voc_rid_t rid,
    VPackSlice const& doc,
    bool isRollback
  ) override;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief fill and return a JSON description of a IResearchLink object
  /// @param withFigures output 'figures' section with e.g. memory size
  ////////////////////////////////////////////////////////////////////////////////
  void toVelocyPack(VPackBuilder& builder, bool withFigures) const override;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief iResearch Link index type enum value
  ////////////////////////////////////////////////////////////////////////////////
  IndexType type() const override;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief iResearch Link index type string value
  ////////////////////////////////////////////////////////////////////////////////
  char const* typeName() const;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief called when the iResearch Link is unloaded from memory
  ////////////////////////////////////////////////////////////////////////////////
  int unload() override;

 private:
  IResearchLinkMeta _meta; // how this collection should be indexed
  IResearchView::ptr _view; // effectively the index itself
}; // IResearchLink

int EnhanceJsonIResearchLink(
  VPackSlice const definition,
  VPackBuilder& builder,
  bool create
) noexcept;

IResearchLink::ptr createIResearchLink(
  TRI_idx_iid_t iid,
  arangodb::LogicalCollection* collection,
  VPackSlice const& info
) noexcept;

NS_END // iresearch
NS_END // arangodb
#endif