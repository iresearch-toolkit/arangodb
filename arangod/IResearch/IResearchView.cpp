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

#include "formats/formats.hpp"

#include "IResearchFieldIterator.h"
#include "IResearchLink.h"

#include "VocBase/LogicalCollection.h"
#include "Basics/files.h"
#include "Basics/VelocyPackHelper.h"

#include "Logger/Logger.h"
#include "Logger/LogMacros.h"

#include "Utils/SingleCollectionTransaction.h"
#include "Utils/StandaloneTransactionContext.h"

#include <velocypack/Builder.h>
#include <velocypack/Iterator.h>
#include <velocypack/velocypack-aliases.h>

#include "IResearchView.h"

NS_LOCAL

typedef irs::async_utils::read_write_mutex::read_mutex ReadMutex;
typedef irs::async_utils::read_write_mutex::write_mutex WriteMutex;

// stored ArangoDB document primary key
class DocumentPrimaryKey {
public:
  DocumentPrimaryKey(TRI_voc_cid_t cid, TRI_voc_rid_t rid) noexcept;

  irs::string_ref const& name() const noexcept;
  bool write(irs::data_output& out) const;

private:
  static irs::string_ref const _name; // stored column name
  uint64_t _keys[2]; // TRI_voc_cid_t + TRI_voc_rid_t
};

irs::string_ref const DocumentPrimaryKey::_name("_pk");

DocumentPrimaryKey::DocumentPrimaryKey(
  TRI_voc_cid_t cid, TRI_voc_rid_t rid
) noexcept: _keys{ cid, rid } {
  static_assert(sizeof(_keys) == sizeof(cid) + sizeof(rid), "Invalid size");
}

irs::string_ref const& DocumentPrimaryKey::name() const noexcept {
  return _name;
}

bool DocumentPrimaryKey::write(irs::data_output& out) const {
  out.write_bytes(
    reinterpret_cast<const irs::byte_type*>(_keys),
    sizeof(_keys)
  );

  return true;
}

NS_END

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

IndexStore::IndexStore(
    irs::directory::ptr&& dir,
    irs::index_writer::ptr&& writer,
    irs::directory_reader::ptr&& reader) noexcept
  : _dir(std::move(_dir)),
    _writer(std::move(_writer)),
    _reader(std::move(_reader)) {
  assert(_dir && _writer && _reader);
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

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

int IResearchView::drop() {
  _threadPool.stop();

  WriteMutex mutex(_mutex);

  // ...........................................................................
  // if an exception occurs below than a drop retry would most likely happen
  // ...........................................................................
  try {
    for (auto& tidStore: _storeByTid) {
      for (auto& fidStore: tidStore.second._storeByFid) {
        fidStore.second._writer->close();
        fidStore.second._directory.close();
      }

      SCOPED_LOCK(tidStore.second._mutex);
      tidStore.second._removals.clear();
    }

    _storeByTid.clear();

    for (auto& fidStore: _storeByWalFid._storeByFid) {
      fidStore.second._writer->close();
      fidStore.second._directory.close();
    }

    _storeByWalFid._storeByFid.clear();
    _storePersisted._writer->close();
    _storePersisted._directory.close();

    return TRI_RemoveDirectory(_meta._dataPath.c_str());
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while removing iResearch '" << name() << "': " << e.what();
    IR_EXCEPTION();
  } catch (...) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while removing iResearch '" << name() << "'";
    IR_EXCEPTION();
  }

  return TRI_ERROR_INTERNAL;
}

int IResearchView::drop(TRI_voc_cid_t cid) {
  auto filter = IResearchFieldIterator::filter(cid);
  std::shared_ptr<irs::filter> shared_filter(std::move(filter));
  ReadMutex mutex(_mutex); // '_storeByTid' & '_storeByWalFid' can be asynchronously updated
  SCOPED_LOCK(mutex);

  // ...........................................................................
  // if an exception occurs below than a drop retry would most likely happen
  // ...........................................................................
  try {
    for (auto& tidStore: _storeByTid) {
      for (auto& fidStore : tidStore.second._storeByFid) {
        fidStore.second._writer->remove(shared_filter);
      }
    }

    for (auto& fidStore: _storeByWalFid._storeByFid) {
      fidStore.second._writer->remove(shared_filter);
    }

    _storePersisted._writer->remove(shared_filter);

    return TRI_ERROR_NO_ERROR;
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while removing from iResearch '" << name() << "', collection '" << cid << "': " << e.what();
    IR_EXCEPTION();
  } catch (...) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while removing from iResearch '" << name() << "', collection '" << cid << "'";
    IR_EXCEPTION();
  }

  return TRI_ERROR_INTERNAL;
}

std::string const& IResearchView::name() const noexcept {
  ReadMutex mutex(_mutex); // '_meta' can be asynchronously updated
  SCOPED_LOCK(mutex);

  return _meta._name;
}

int IResearchView::insert(
  TRI_voc_fid_t fid,
  TRI_voc_tid_t tid,
  TRI_voc_cid_t cid,
  TRI_voc_rid_t rid,
  arangodb::velocypack::Slice const& doc,
  IResearchLinkMeta const& meta
) {
  DocumentPrimaryKey attribute(cid, rid);
  auto begin = IResearchFieldIterator(cid, rid, doc, meta);
  auto end = IResearchFieldIterator();
  WriteMutex mutex(_mutex); // '_storeByTid' & '_storeByFid' can be asynchronously updated
  SCOPED_LOCK(mutex);
  auto& store = _storeByTid[tid]._storeByFid[fid];

  mutex.unlock(true); // downgrade to a read-lock

  try {
    if (store._writer->insert(end, end, &attribute, &attribute + 1)) {
      return TRI_ERROR_NO_ERROR;
    }

    LOG_TOPIC(WARN, Logger::FIXME) << "failed inserting into iResearch '" << name() << "', collection '" << cid << "', revision '" << rid << "'";
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while inserting into iResearch '" << name() << "', collection '" << cid << "', revision '" << rid << "': " << e.what();
    IR_EXCEPTION();
  } catch (...) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while inserting into iResearch '" << name() << "', collection '" << cid << "', revision '" << rid << "'";
    IR_EXCEPTION();
  }

  return TRI_ERROR_INTERNAL;
}

size_t IResearchView::linkCount() const noexcept {
  ReadMutex mutex(_mutex); // '_links' can be asynchronously updated
  SCOPED_LOCK(mutex);

  return _links.size();
}

size_t IResearchView::memory() const {
  return 0;  // FIXME TODO
}

bool IResearchView::modify(VPackSlice const& definition) {
  return false; // FIXME TODO
}

bool IResearchView::properties(VPackSlice const& props, TRI_vocbase_t* vocbase) {
  std::string error; // TODO: should somehow push it to the caller
  IResearchViewMeta meta;

  if (!meta.init(props, error)) {
    return false;
  }

  static std::string const LINKS = "links";
  static std::string const PROPERTIES = "properties";

  auto const collections = props.get(LINKS);

  if (!collections.isArray()) {
    return false;
  }

  for (auto const& collection : VPackArrayIterator(collections)) {
    if (!collection.isObject()) {
      return false;
    }

    auto const cid = basics::VelocyPackHelper::extractIdValue(collection);

    if (0 == cid) {
      // can't extract cid
      return false;
    }

    auto const linkProp = collection.get(PROPERTIES);

    if (linkProp.isNone()) {
      return false;
    }

    SingleCollectionTransaction trx(
      StandaloneTransactionContext::Create(vocbase),
      cid,
      AccessMode::Type::WRITE
    );

    auto const res = trx.begin();

    if (TRI_ERROR_NO_ERROR != res) {
      // can't start a transaction
      return false;
    }

    auto created = false;
    auto index = trx.documentCollection()->createIndex(&trx, linkProp, created);

    if (!index) {
      // something went wrong
      return false;
    }

    if (created) {
      _links.insert(std::static_pointer_cast<IResearchLink>(index));
    }
  }

  // noexcept
  _meta = std::move(meta);

  return true;
}

bool IResearchView::properties(VPackBuilder& props) const {
  return _meta.json(props);
}

int IResearchView::query(
  std::function<int(arangodb::transaction::Methods const&, VPackSlice const&)> const& visitor,
  arangodb::transaction::Methods& trx,
  std::string const& query,
  std::ostream* error /*= nullptr*/
) {
  return 0; // FIXME TODO
}

int IResearchView::remove(
  TRI_voc_tid_t tid,
  TRI_voc_cid_t cid,
  TRI_voc_rid_t rid
) {
  auto filter = IResearchFieldIterator::filter(cid, rid);
  std::shared_ptr<irs::filter> shared_filter(std::move(filter));
  WriteMutex mutex(_mutex); // '_storeByTid' can be asynchronously updated
  SCOPED_LOCK(mutex);
  auto& store = _storeByTid[tid];

  mutex.unlock(true); // downgrade to a read-lock

  // ...........................................................................
  // if an exception occurs below than the transaction is droped including all
  // all of its fid stores, no impact to iResearch View data integrity
  // ...........................................................................
  try {
    for (auto& fidStore: store._storeByFid) {
      fidStore.second._writer->remove(shared_filter);
    }

    SCOPED_LOCK(store._mutex); // '_removals' can be asynchronously updated
    store._removals.emplace_back(shared_filter);

    return TRI_ERROR_NO_ERROR;
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while removing from iResearch '" << name() << "', collection '" << cid << "', revision '" << rid << "': " << e.what();
    IR_EXCEPTION();
  } catch (...) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while removing from iResearch '" << name() << "', collection '" << cid << "', revision '" << rid << "'";
    IR_EXCEPTION();
  }

  return TRI_ERROR_INTERNAL;
}

bool IResearchView::sync() {
  ReadMutex mutex(_mutex);

  try {
    for (auto& tidStore: _storeByTid) {
      for (auto& fidStore: tidStore.second._storeByFid) {
        fidStore.second._writer->commit();
      }
    }

    for (auto& fidStore: _storeByWalFid._storeByFid) {
      fidStore.second._writer->commit();
    }

    _storePersisted._writer->commit();

    return true;
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while syncing iResearch '" << name() << "': " << e.what();
    IR_EXCEPTION();
  } catch (...) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while syncing iResearch '" << name() << "'";
    IR_EXCEPTION();
  }

  return false;
}

NS_END // iresearch
NS_END // arangodb

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------