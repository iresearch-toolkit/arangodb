
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

#include "IResearchLink.h"
#include "IResearchView.h"
#include "IResearchDocument.h"

#include "VocBase/LogicalCollection.h"

namespace arangodb {
namespace iresearch {

IResearchLink::IResearchLink(
    TRI_idx_iid_t iid,
    arangodb::LogicalCollection* collection,
    VPackSlice const& info)
  : Index(iid, collection, info) {
  _unique = false; // always non unique
  _sparse = true;  // always sparse

  // initialize link meta
  std::string error;
  if (!_meta.init(info, error)) {

  }
}

char const* IResearchLink::typeName() const { 
  return "IResearch";
}

size_t IResearchLink::memory() const {
  return 0;
}

void IResearchLink::toVelocyPack(VPackBuilder& builder, bool withFigures) const {
  Index::toVelocyPack(builder, withFigures);
  builder.add("unique", VPackValue(_unique));
  builder.add("sparse", VPackValue(_sparse));
}

void IResearchLink::toVelocyPackFigures(VPackBuilder& builder) const {
  TRI_ASSERT(builder.isOpenObject());
  builder.add("memory", VPackValue(this->memory()));
}

bool IResearchLink::matchesDefinition(VPackSlice const& slice) const {
  return false;
}

int IResearchLink::insert(
    transaction::Methods* trx,
    TRI_voc_rid_t rid,
    arangodb::velocypack::Slice const& doc,
    bool isRollback) {
  const auto cid = collection()->cid();

  if (fields().empty()) {
    // index all fields
  } else {
    // index specified fields
  }

  StoredPrimaryKey pk(rid, cid);
  Field fld;

  return 0; //_view->insert(&fld, &fld + 1, &pk, &pk + 1);
}

int IResearchLink::remove(
    transaction::Methods* trx,
    TRI_voc_rid_t revId,
    arangodb::velocypack::Slice const& doc,
    bool isRollback) {
  return TRI_ERROR_NO_ERROR;
}

int IResearchLink::unload() {
  // does nothing
  return TRI_ERROR_NO_ERROR;
}

int IResearchLink::cleanup() {
  return TRI_ERROR_NO_ERROR;
}

/// @brief checks whether the index supports the condition
bool IResearchLink::supportsFilterCondition(
    arangodb::aql::AstNode const* node,
    arangodb::aql::Variable const* reference,
    size_t itemsInIndex,
    size_t& estimatedItems,
    double& estimatedCost) const {
  return false;
}

/// @brief creates an IndexIterator for the given Condition
IndexIterator* IResearchLink::iteratorForCondition(
    transaction::Methods* trx,
    ManagedDocumentResult* mmdr,
    arangodb::aql::AstNode const* node,
    arangodb::aql::Variable const* reference, bool) const {
  return nullptr;
}

/// @brief specializes the condition for use with the index
arangodb::aql::AstNode* IResearchLink::specializeCondition(
    arangodb::aql::AstNode* node,
    arangodb::aql::Variable const* reference) const {
  return nullptr;
}

int EnhanceJsonIResearchLink(
    VPackSlice const definition,
    VPackBuilder& builder,
    bool create) noexcept {
  if (definition.isNone()) {
    return TRI_ERROR_BAD_PARAMETER;
  }

  std::string error;
  IResearchLinkMeta meta;

  if (!meta.init(definition, error)) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "error parsing view link parameters from json: " << error;

    return TRI_ERROR_BAD_PARAMETER;
  }

  int res = TRI_ERROR_NO_ERROR;

  try {
    res = meta.json(builder) ? TRI_ERROR_NO_ERROR : TRI_ERROR_BAD_PARAMETER;
  } catch (std::exception const& e) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "error serializaing view link parameters to json: " << e.what();

    return TRI_ERROR_BAD_PARAMETER;
  } catch (...) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "error serializaing view link parameters to json";

    return TRI_ERROR_BAD_PARAMETER;
  }

  return res;
}

IResearchLink::ptr createIResearchLink(
    TRI_idx_iid_t iid,
    arangodb::LogicalCollection* collection,
    VPackSlice const& info) noexcept {
  try {
    return std::make_shared<IResearchLink>(iid, collection, info);
  } catch (std::exception const& e) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "error creating view link " << e.what();
  } catch (...) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "error creating view link";
  }

  return nullptr;
}

} // iresearch
} // arangodb
