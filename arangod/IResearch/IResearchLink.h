
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

#include "Indexes/Index.h"

namespace arangodb {
namespace iresearch {

class IResearchView;

class IResearchLink final : public Index {
 public:
  typedef std::shared_ptr<IResearchLink> ptr;

  IResearchLink(
    TRI_idx_iid_t iid,
    arangodb::LogicalCollection* collection,
    VPackSlice const& info
  );

  IndexType type() const override {
    // TODO: don't use enum
    return Index::TRI_IDX_TYPE_IRESEARCH_LINK;
  }

  char const* typeName() const;

  bool isPersistent() const override { return true; }

  // maps to multivalued
  bool allowExpansion() const override { return true; }

  bool canBeDropped() const override { return true; }

  bool isSorted() const override { return false; }

  bool hasSelectivityEstimate() const override { return false; }

  bool hasBatchInsert() const override {
    // TODO: should be true, need to implement such functionality in IResearch
    return false;
  }

  size_t memory() const override;

  void toVelocyPack(VPackBuilder& builder, bool withFigures) const override;
  void toVelocyPackFigures(VPackBuilder& builder) const override;

  bool matchesDefinition(VPackSlice const& slice) const override;

  int insert(
    transaction::Methods* trx,
    TRI_voc_rid_t revId,
    arangodb::velocypack::Slice const& doc,
    bool isRollback
  ) override;

  int remove(
    transaction::Methods* trx,
    TRI_voc_rid_t revId,
    arangodb::velocypack::Slice const& doc,
    bool isRollback
  ) override;

  int unload() override;

  int cleanup() override;

  bool supportsFilterCondition(
    arangodb::aql::AstNode const* node,
    arangodb::aql::Variable const* reference,
    size_t itemsInIndex,
    size_t& estimatedItems,
    double& estimatedCost
  ) const override;

  virtual IndexIterator* iteratorForCondition(
    transaction::Methods* trx,
    ManagedDocumentResult* mmdr,
    arangodb::aql::AstNode const* node,
    arangodb::aql::Variable const* reference,
    bool
  ) const override;

  /// @brief specializes the condition for use with the index
  arangodb::aql::AstNode* specializeCondition(
      arangodb::aql::AstNode* node,
      arangodb::aql::Variable const* reference
  ) const override;

 private:
  IResearchLinkMeta _meta;
  IResearchView* _view;
}; // IResearchLink

int EnhanceJsonIResearchLink(
  VPackSlice const definition,
  VPackBuilder& builder,
  bool create) noexcept;

IResearchLink::ptr createIResearchLink(
  TRI_idx_iid_t iid,
  arangodb::LogicalCollection* collection,
  VPackSlice const& info) noexcept;

} // iresearch
} // arangodb

#endif
