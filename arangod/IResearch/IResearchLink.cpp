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

#include "StorageEngine/TransactionState.h"
#include "Transaction/Methods.h"
#include "VocBase/LogicalCollection.h"

#include "IResearchLink.h"

NS_LOCAL

static const std::string VIEW_NAME_FIELD("name");

////////////////////////////////////////////////////////////////////////////////
/// @brief return a reference to a static VPackSlice of an empty index definition
////////////////////////////////////////////////////////////////////////////////
VPackSlice const& emptyParentSlice() {
  static const struct EmptySlice {
    VPackBuilder _builder;
    VPackSlice _slice;
    EmptySlice() {
      VPackBuilder fieldsBuilder;

      fieldsBuilder.openArray();
      fieldsBuilder.close(); // empty array
      _builder.openObject();
      _builder.add("fields", fieldsBuilder.slice()); // empty array
      _builder.close(); // object with just one field required by the Index constructor
      _slice = _builder.slice();
    }
  } emptySlice;

  return emptySlice._slice;
}

NS_END

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

IResearchLink::IResearchLink(
  TRI_idx_iid_t iid,
  arangodb::LogicalCollection* collection,
  IResearchLinkMeta&& meta,
  IResearchView::ptr view
) : Index(iid, collection, emptyParentSlice()),
    _meta(std::move(meta)),
    _view(std::move(view)) {
  _unique = false; // cannot be unique since multiple fields are indexed
  _sparse = true;  // always sparse
}

bool IResearchLink::allowExpansion() const {
  return true; // maps to multivalued
}

bool IResearchLink::canBeDropped() const {
  return true; // valid for a link to be dropped from an iResearch view
}

int IResearchLink::drop() {
  if (!_collection || !_view) {
    return TRI_ERROR_ARANGO_COLLECTION_NOT_LOADED; // '_collection' and '_view' required
  }

  return _view->drop(_collection->cid());
}

bool IResearchLink::hasBatchInsert() const {
  // TODO: should be true, need to implement such functionality in IResearch
  return false;
}

bool IResearchLink::hasSelectivityEstimate() const {
  return false; // selectivity can only be determined per query since multiple fields are indexed
}

int IResearchLink::insert(
  transaction::Methods* trx,
  TRI_voc_rid_t rid,
  VPackSlice const& doc,
  bool isRollback
) {
  if (!_collection || !_view) {
    return TRI_ERROR_ARANGO_COLLECTION_NOT_LOADED; // '_collection' and '_view' required
  }

  if (!trx || !trx->state()) {
    return TRI_ERROR_BAD_PARAMETER; // 'trx' and transaction state required
  }

  auto trxState = trx->state();

  if (!trxState) {
    return TRI_ERROR_BAD_PARAMETER; // transaction state required
  }

  return _view->insert(trxState->id(), _collection->cid(), rid, doc, _meta);
}

bool IResearchLink::isPersistent() const {
  return true; // records persisted into the iResearch view
}

bool IResearchLink::isSorted() const {
  return false; // iResearch does not provide a fixed default sort order
}

bool IResearchLink::matchesDefinition(VPackSlice const& slice) const {
  if (slice.hasKey(VIEW_NAME_FIELD)) {
    if (!_view) {
      return false; // slice has 'name' but the current object does not
    }

    auto name = slice.get(VIEW_NAME_FIELD);
    VPackValueLength nameLength;
    auto nameValue = slice.getString(nameLength);
    irs::string_ref sliceName(nameValue, nameLength);

    if (sliceName != _view->name()) {
      return false; // iResearch View names of current object and slice do nto match
    }
  } else if (_view) {
    return false; // slice has no 'name' but the current object does
  }

  IResearchLinkMeta other;
  std::string errorField;

  return other.init(slice, errorField) && _meta == other;
}

size_t IResearchLink::memory() const {
  auto size = sizeof(IResearchLink); // includes empty members from parent

  size += _meta.memory();

  if (_view) {
    // <iResearch View size> / <number of link instances>
    size += _view->memory() / std::max(size_t(1), _view->linkCount());
  }

  return size;
}

int IResearchLink::remove(
  transaction::Methods* trx,
  TRI_voc_rid_t rid,
  VPackSlice const& doc,
  bool isRollback
) {
  if (!_collection || !_view) {
    return TRI_ERROR_ARANGO_COLLECTION_NOT_LOADED; // '_collection' and '_view' required
  }

  if (!trx || !trx->state()) {
    return TRI_ERROR_BAD_PARAMETER; // 'trx' and transaction state required
  }

  auto trxState = trx->state();

  if (!trxState) {
    return TRI_ERROR_BAD_PARAMETER; // transaction state required
  }

  // remove documents matching on cid and rid
  return _view->remove(trx->state()->id(), _collection->cid(), rid);
}

void IResearchLink::toVelocyPack(VPackBuilder& builder, bool withFigures) const {
  TRI_ASSERT(builder.isOpenObject());
  bool success = _meta.json(builder);
  TRI_ASSERT(success);

  builder.add("id", VPackValue(std::to_string(_iid)));
  builder.add("type", VPackValue(typeName()));

  if (_view) {
    builder.add(VIEW_NAME_FIELD, VPackValue(_view->name()));
  }

  if (withFigures) {
    VPackBuilder figuresBuilder;

    figuresBuilder.openObject();
    toVelocyPackFigures(figuresBuilder);
    figuresBuilder.close();
    figuresBuilder.add("figures", figuresBuilder.slice());
  }
}

Index::IndexType IResearchLink::type() const {
  // TODO: don't use enum
  return Index::TRI_IDX_TYPE_IRESEARCH_LINK;
}

char const* IResearchLink::typeName() const {
  return "iresearch";
}

int IResearchLink::unload() {
  _view.reset(); // release reference to the iresearch View

  return TRI_ERROR_NO_ERROR;
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
  VPackSlice const& info
) noexcept {
  // TODO: should somehow pass an error to the caller (return nullptr means "Out of memory")

  IResearchView::ptr view;

  if (info.hasKey(VIEW_NAME_FIELD)) {
    auto name = info.get(VIEW_NAME_FIELD);
    VPackValueLength nameLength;
    auto nameValue = info.getString(nameLength);
    irs::string_ref viewName(nameValue, nameLength);

    view = IResearchView::find(viewName);
  }

  if (!view) {
    TRI_set_errno(TRI_ERROR_ARANGO_COLLECTION_NOT_FOUND);

    return nullptr; // no view to link with
  }

  std::string error;
  IResearchLinkMeta meta;

  try {
    if (!meta.init(info, error)) {
      TRI_set_errno(TRI_ERROR_BAD_PARAMETER);

      return nullptr; // failed to parse metadata
    }

    return std::make_shared<IResearchLink>(iid, collection, std::move(meta), view);
  } catch (std::exception const& e) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "error creating view link " << e.what();
  } catch (...) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "error creating view link";
  }

  return nullptr;
}

NS_END // iresearch
NS_END // arangodb

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------