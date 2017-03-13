
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

#include "IResearchView.h"

#include "VocBase/LogicalCollection.h"

#include "Logger/Logger.h"
#include "Logger/LogMacros.h"

#include "Utils/SingleCollectionTransaction.h"
#include "Utils/StandaloneTransactionContext.h"

#include <velocypack/Builder.h>
#include <velocypack/Iterator.h>
#include <velocypack/velocypack-aliases.h>

#include "formats/formats.hpp"

using namespace arangodb::iresearch;

const irs::string_ref IRS_CURRENT_FORMAT = "1_0";

/* static */ IndexStore IndexStore::make(irs::directory::ptr&& dir) {
  irs::index_writer::ptr writer;
  irs::directory_reader::ptr reader;

  try {
    auto format = irs::formats::get(IRS_CURRENT_FORMAT);
    TRI_ASSERT(format);

    // create writer
    writer = irs::index_writer::make(*dir, format, irs::OM_CREATE_APPEND);
    TRI_ASSERT(writer);

    // open reader
    reader = irs::directory_reader::open(*dir, format);
    TRI_ASSERT(reader);
  } catch (...) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught exception while initializing an IndexStore";
    throw;
  }

  return IndexStore(
    std::move(dir), std::move(writer), std::move(reader)
  );
}

IndexStore::IndexStore(IndexStore&& rhs)
  : _dir(std::move(rhs._dir)),
    _writer(std::move(rhs._writer)),
    _reader(std::move(rhs._reader)) {
}

IndexStore& IndexStore::operator=(IndexStore&& rhs) {
  if (this != &rhs) {
    _dir = std::move(rhs._dir);
    _writer = std::move(rhs._writer);
    _reader = std::move(rhs._reader);
  }

  return *this;
}

int IndexStore::insert(StoredPrimaryKey const& pk) noexcept {
  Field fld;
  try {
    if (!_writer->insert(&fld, &fld + 1, &pk, &pk + 1)) {
      LOG_TOPIC(WARN, Logger::DEVEL) << "failed to insert into index!";

      return TRI_ERROR_INTERNAL;
    }
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while inserting into index: " << e.what();

    return TRI_ERROR_INTERNAL;
  } catch (...) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while inserting into index";
    IR_EXCEPTION();

    return TRI_ERROR_INTERNAL;
  }

  return TRI_ERROR_NO_ERROR;
}

int IndexStore::remove(std::shared_ptr<irs::filter> const& filter) noexcept {
  try {
    _writer->remove(filter);
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while removing index: " << e.what();

    return TRI_ERROR_INTERNAL;
  } catch (...) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while removing index!";
    IR_EXCEPTION();

    return TRI_ERROR_INTERNAL;
  }

  return TRI_ERROR_NO_ERROR;
}

int IndexStore::merge(IndexStore& src) noexcept {
  if (this == &src) {
    return TRI_ERROR_NO_ERROR; // merge with self, noop
  }

  try {
    src._writer->commit(); // ensure have latest view in reader

    auto pReader = src.reader();

    _writer->import(*pReader);
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while importing index: " << e.what();

    return TRI_ERROR_INTERNAL;
  } catch (...) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while importing index!";
    IR_EXCEPTION();

    return TRI_ERROR_INTERNAL;
  }

  return TRI_ERROR_NO_ERROR;
}

int IndexStore::commit() noexcept {
  try {
    _writer->commit();
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while commiting index: " << e.what();

    return TRI_ERROR_INTERNAL;
  } catch (...) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while commiting index!";
    IR_EXCEPTION();

    return TRI_ERROR_INTERNAL;
  }

  return TRI_ERROR_NO_ERROR;
}

int IndexStore::consolidate(irs::index_writer::consolidation_policy_t const& policy) noexcept {
  try {
    _writer->consolidate(policy, false);
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while consolidating index: " << e.what();

    return TRI_ERROR_INTERNAL;
  } catch (...) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while consolidating index!";
    IR_EXCEPTION();

    return TRI_ERROR_INTERNAL;
  }

  return TRI_ERROR_INTERNAL;
}

int IndexStore::cleanup() noexcept {
  try {
    irs::directory_utils::remove_all_unreferenced(*_dir);
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while cleaning up index: " << e.what();

    return TRI_ERROR_INTERNAL;
  } catch (...) {
    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while cleaning up index!";
    IR_EXCEPTION();

    return TRI_ERROR_INTERNAL;
  }

  return TRI_ERROR_INTERNAL;
}

size_t IndexStore::memory() const noexcept {
  size_t size = 0;

  auto& dir = *_dir;
  dir.visit([&dir, &size](std::string& file)->bool {
    uint64_t length;

    size += dir.length(length, file) ? length : 0;

    return true;
  });

  return size;
}

void IndexStore::close() noexcept {
  // noexcept
  _writer->close();
  _dir->close();
}

bool IResearchView::properties(VPackSlice const& props) {
  std::string error;
  IResearchViewMeta meta;

  if (!meta.init(props, error)) {
    return false;
  }

  static std::string const COLLECTIONS = "collections";
  static std::string const COLLECTION = "collection";
  static std::string const PROPERTIES = "properties";

  auto collections = props.get(COLLECTIONS);
  if (!collections.isArray()) {
    return false;
  }

  for (auto const& collection : VPackArrayIterator(collections)) {
    if (collection.isObject()) {
      return false;
    }

    auto cid = collection.get(COLLECTION);

    if (cid.isNone()) {
      return false;
    }

    auto linkProp = collection.get(PROPERTIES);

    if (linkProp.isNone()) {
      return false;
    }

    TRI_voc_cid_t cidValue;
    try {
      cidValue = cid.getNumber<TRI_voc_cid_t>();
    } catch (...) {
      return false;
    }

    TRI_vocbase_t* vocbase{};

    SingleCollectionTransaction trx(
      StandaloneTransactionContext::Create(vocbase),
      cidValue,
      AccessMode::Type::WRITE
    );

    auto const res = trx.begin();
    if (TRI_ERROR_NO_ERROR != res) {
      return false;
    }

    auto created = false;
    auto* col = trx.documentCollection();
    auto index = col->createIndex(&trx, props, created); // TODO

    if (!index) {
      return false;
    }
  }

  // noexcept

  // TODO: use move
  //_meta = std::move(meta);

  return true;
}

bool IResearchView::properties(VPackBuilder& props) const {
  return _meta.json(props);
}

bool IResearchView::drop() {
  return false;
}

