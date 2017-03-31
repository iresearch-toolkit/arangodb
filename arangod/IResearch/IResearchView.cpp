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
#include "store/memory_directory.hpp"
#include "store/fs_directory.hpp"
#include "utils/directory_utils.hpp"
#include "utils/memory.hpp"

#include "IResearchDocument.h"
#include "IResearchLink.h"

#include "VocBase/LogicalCollection.h"
#include "VocBase/LogicalView.h"
#include "VocBase/PhysicalView.h"

#include "Basics/Result.h"
#include "Basics/files.h"
#include "Basics/VelocyPackHelper.h"
#include "Indexes/Index.h"
#include "Logger/Logger.h"
#include "Logger/LogMacros.h"

#include "Utils/CollectionNameResolver.h"
#include "Utils/UserTransaction.h"

#include "Transaction/StandaloneContext.h"

#include <velocypack/Builder.h>
#include <velocypack/Iterator.h>
#include <velocypack/velocypack-aliases.h>

#include "IResearchView.h"

NS_LOCAL

////////////////////////////////////////////////////////////////////////////////
/// @brief the name of the field in the iResearch View definition denoting the
///        corresponding link definitions
////////////////////////////////////////////////////////////////////////////////
const std::string LINKS_FIELD("links");

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

arangodb::Result createPersistedDataDirectory(
    irs::directory::ptr& dstDirectory, // out param
    irs::index_writer::ptr& dstWriter, // out param
    std::string const& dstDataPath,
    irs::directory_reader srcReader, // !srcReader ==  do not import
    std::string const& viewName
) noexcept {
  try {
    auto directory = irs::directory::make<irs::fs_directory>(dstDataPath);

    if (!directory) {
      return arangodb::Result(
        TRI_ERROR_BAD_PARAMETER,
        std::string("error creating persistent directry for iResearch view '") + viewName + "' at path '" + dstDataPath + "'"
      );
    }

    auto format = irs::formats::get("1_0");
    auto writer = irs::index_writer::make(*directory, format, irs::OM_CREATE_APPEND);

    if (!writer) {
      return arangodb::Result(
        TRI_ERROR_BAD_PARAMETER,
        std::string("error creating persistent writer for iResearch view '") + viewName + "' at path '" + dstDataPath + "'"
      );
    }

    irs::all filter;

    writer->remove(filter);
    writer->commit(); // clear destination directory

    if (srcReader) {
      srcReader = srcReader.reopen();
      writer->import(srcReader);
      writer->commit(); // initialize 'store' as a copy of the existing store
    }

    dstDirectory = std::move(directory);
    dstWriter = std::move(writer);

    return arangodb::Result(/*TRI_ERROR_NO_ERROR*/);
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, arangodb::Logger::FIXME) << "caught exception while creating iResearch view '" << viewName << "' data path '" + dstDataPath + "': " << e.what();
    IR_EXCEPTION();
    return arangodb::Result(
      TRI_ERROR_BAD_PARAMETER,
      std::string("error creating iResearch view '") + viewName + "' data path '" + dstDataPath + "'"
    );
  } catch (...) {
    LOG_TOPIC(WARN, arangodb::Logger::FIXME) << "caught exception while creating iResearch view '" << viewName << "' data path '" + dstDataPath + "'";
    IR_EXCEPTION();
    return arangodb::Result(
      TRI_ERROR_BAD_PARAMETER,
      std::string("error creating iResearch view '") + viewName + "' data path '" + dstDataPath + "'"
    );
  }
}

// @brief approximate iResearch directory instance size
size_t directoryMemory(irs::directory const& directory, std::string const& viewName) noexcept {
  size_t size = 0;

  try {
    directory.visit([&directory, &size](std::string& file)->bool {
      uint64_t length;

      size += directory.length(length, file) ? length : 0;

      return true;
    });
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, arangodb::Logger::FIXME) << "caught error while calculating size of iResearch view '" << viewName << "': " << e.what();
    IR_EXCEPTION();
  } catch (...) {
    LOG_TOPIC(WARN, arangodb::Logger::FIXME) << "caught error while calculating size of iResearch view '" << viewName << "'";
    IR_EXCEPTION();
  }

  return size;
}

arangodb::iresearch::IResearchLink* findFirstMatchingLink(
    arangodb::LogicalCollection const& collection,
    arangodb::iresearch::IResearchView const& view
) {
  for (auto& index: collection.getIndexes()) {
    if (!index || arangodb::Index::TRI_IDX_TYPE_IRESEARCH_LINK != index->type()) {
      continue; // not an iresearch Link
    }

    #ifdef ARANGODB_ENABLE_MAINTAINER_MODE
      auto* link = reinterpret_cast<arangodb::iresearch::IResearchLink*>(index.get());
    #else
      auto* link = static_cast<arangodb::iresearch::IResearchLink*>(index.get());
    #endif

    if (link && *link == view) {
      return link; // found required link
    }
  }

  return nullptr;
}

arangodb::Result updateLinks(
    TRI_vocbase_t& vocbase,
    arangodb::iresearch::IResearchView const& view,
    arangodb::velocypack::Slice const& links
) noexcept {
  try {
    if (!links.isObject()) {
      return arangodb::Result(
        TRI_ERROR_BAD_PARAMETER,
        std::string("error parsing link parameters from json for iResearch view '") + view.name() + "'"
      );
    }

    struct State {
      arangodb::LogicalCollection* _collection = nullptr;
      size_t _collectionsToLockOffset; // std::numeric_limits<size_t>::max() == removal only
      arangodb::iresearch::IResearchLink const* _link = nullptr;
      size_t _linkDefinitionsOffset;
      bool _valid = true;
      State(size_t collectionsToLockOffset)
        : State(collectionsToLockOffset, std::numeric_limits<size_t>::max()) {}
      State(size_t collectionsToLockOffset, size_t linkDefinitionsOffset)
        : _collectionsToLockOffset(collectionsToLockOffset), _linkDefinitionsOffset(linkDefinitionsOffset) {}
    };
    std::vector<std::string> collectionsToLock;
    std::vector<std::pair<arangodb::velocypack::Slice, arangodb::iresearch::IResearchLinkMeta>> linkDefinitions;
    std::vector<State> linkModifications;

    for (VPackObjectIterator linksItr(links); linksItr.valid(); ++linksItr) {
      auto collection = linksItr.key();

      if (!collection.isString()) {
        return arangodb::Result(
          TRI_ERROR_BAD_PARAMETER,
          std::string("error parsing link parameters from json for iResearch view '") + view.name() + "' offset '" + arangodb::basics::StringUtils::itoa(linksItr.index()) + '"'
        );
      }

      auto link = linksItr.value();
      auto collectionName = collection.copyString();

      if (link.isNull()) {
        linkModifications.emplace_back(collectionsToLock.size());
        collectionsToLock.emplace_back(collectionName);

        continue; // only removal requested
      }

      std::string error;
      arangodb::iresearch::IResearchLinkMeta linkMeta;

      if (!linkMeta.init(link, error)) {
        return arangodb::Result(
          TRI_ERROR_BAD_PARAMETER,
          std::string("error parsing link parameters from json for iResearch view '") + view.name() + "' collection '" + collectionName + "' error '" + error + "'"
        );
      }

      linkModifications.emplace_back(collectionsToLock.size(), linkDefinitions.size());
      collectionsToLock.emplace_back(collectionName);
      linkDefinitions.emplace_back(link, std::move(linkMeta));
    }

    if (collectionsToLock.empty()) {
      return arangodb::Result(/*TRI_ERROR_NO_ERROR*/); // nothing to update
    }

    static std::vector<std::string> const EMPTY;
    arangodb::UserTransaction trx(
      arangodb::transaction::StandaloneContext::Create(&vocbase),
      EMPTY, // readCollections
      EMPTY, // writeCollections
      collectionsToLock, // exclusiveCollections
      arangodb::transaction::Methods::DefaultLockTimeout, // lockTimeout
      false, // waitForSync
      false // allowImplicitCollections
    );

    auto res = trx.begin();

    if (TRI_ERROR_NO_ERROR != res) {
      return arangodb::Result(
        res,
        std::string("failed to start collection updating transaction for iResearch view '") + view.name() + "'"
      );
    }

    auto* resolver = trx.resolver();

    if (!resolver) {
      return arangodb::Result(
        TRI_ERROR_INTERNAL,
        std::string("failed to get resolver from transaction while updating while iResearch view '") + view.name() + "'"
      );
    }

    // resolve corresponding collection and link
    for (auto itr = linkModifications.begin(), end = linkModifications.end(); itr != end;) {
      auto& state = *itr;
      auto& collectionName = collectionsToLock[state._collectionsToLockOffset];

      state._collection = const_cast<arangodb::LogicalCollection*>(resolver->getCollectionStruct(collectionName));

      if (!state._collection) {
        return arangodb::Result(
          TRI_ERROR_ARANGO_COLLECTION_NOT_FOUND,
          std::string("failed to get collection while updating iResearch view '") + view.name() + "' collection '" + collectionName + "'"
        );
      }

      state._link = findFirstMatchingLink(*(state._collection), view);

      // remove modification state if no change on existing link or removal of non-existant link
      if ((state._link && state._linkDefinitionsOffset < linkDefinitions.size() && *(state._link) == linkDefinitions[state._linkDefinitionsOffset].second)
        || (!state._link && state._linkDefinitionsOffset >= linkDefinitions.size())) {
        itr = linkModifications.erase(itr);
        continue;
      }

      ++itr;
    }

    // execute removals
    for (auto& state: linkModifications) {
      if (state._link) {
        state._valid = state._collection->dropIndex(state._link->id());
      }
    }

    // execute additions
    for (auto& state: linkModifications) {
      if (state._valid && state._linkDefinitionsOffset < linkDefinitions.size()) {
        bool isNew = false;
        state._valid = state._collection->createIndex(&trx, linkDefinitions[state._linkDefinitionsOffset].first, isNew) && isNew;
      }
    }

    std::string error;

    // validate success
    for (auto& state: linkModifications) {
      if (!state._valid) {
        error.append(error.empty() ? "" : ", ").append(collectionsToLock[state._collectionsToLockOffset]);
      }
    }

    if (error.empty()) {
      return arangodb::Result(trx.commit());
    }

    trx.abort();

    return arangodb::Result(
      TRI_ERROR_ARANGO_ILLEGAL_STATE,
      std::string("failed to update links while updating iResearch view '") + view.name() + "', retry same request or examine errors for collections: " + error
    );
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, arangodb::Logger::FIXME) << "caught exception while updating links for iResearch view '" << view.name() << "': " << e.what();
    IR_EXCEPTION();
    return arangodb::Result(
      TRI_ERROR_BAD_PARAMETER,
      std::string("error updating links for iResearch view '") + view.name() + "'"
    );
  } catch (...) {
    LOG_TOPIC(WARN, arangodb::Logger::FIXME) << "caught exception while updating links for iResearch view '" << view.name() << "'";
    IR_EXCEPTION();
    return arangodb::Result(
      TRI_ERROR_BAD_PARAMETER,
      std::string("error updating links for iResearch view '") + view.name() + "'"
    );
  }
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

//int IndexStore::insert(StoredPrimaryKey const& pk) noexcept {
//  Field fld;
//  try {
//    if (!_writer->insert(&fld, &fld + 1, &pk, &pk + 1)) {
//      LOG_TOPIC(WARN, Logger::DEVEL) << "failed to insert into index!";
//
//      return TRI_ERROR_INTERNAL;
//    }
//  } catch (std::exception& e) {
//    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while inserting into index: " << e.what();
//
//    return TRI_ERROR_INTERNAL;
//  } catch (...) {
//    LOG_TOPIC(WARN, Logger::DEVEL) << "caught error while inserting into index";
//    IR_EXCEPTION();
//
//    return TRI_ERROR_INTERNAL;
//  }
//
//  return TRI_ERROR_NO_ERROR;
//}

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

IResearchView::DataStore& IResearchView::DataStore::operator=(
    IResearchView::DataStore&& other
) noexcept {
  if (this != &other) {
    _directory = std::move(other._directory);
    _reader = std::move(other._reader);
    _writer = std::move(other._writer);
  }

  return *this;
}

IResearchView::DataStore::operator bool() const noexcept {
  return _directory && _writer;
}

IResearchView::MemoryStore::MemoryStore() {
  auto format = irs::formats::get("1_0");

  _directory = irs::directory::make<irs::memory_directory>();

  // create writer before reader to ensure data directory is present
  _writer = irs::index_writer::make(*_directory, format, irs::OM_CREATE_APPEND);
  _writer->commit(); // initialize 'store'
}

IResearchView::IResearchView(
  arangodb::LogicalView* view,
  arangodb::velocypack::Slice const& info
) : ViewImplementation(view, info),
   _asyncMetaRevision(1),
   _asyncTerminate(false),
   _threadPool(0, 0) { // 0 == create pool with no threads, i.e. not running anything
  // add asynchronous commit job
  _threadPool.run(
    [this]()->void {
    struct State {
      struct PolicyState {
        size_t _intervalCount;
        size_t _intervalStep;
        std::shared_ptr<irs::index_writer::consolidation_policy_t> _policy;
        PolicyState(size_t intervalStep, std::shared_ptr<irs::index_writer::consolidation_policy_t> policy)
          : _intervalCount(0), _intervalStep(intervalStep), _policy(policy) {
        }
      };

      size_t _asyncMetaRevision;
      size_t _cleanupIntervalCount;
      size_t _cleanupIntervalStep;
      size_t _commitIntervalMsecRemainder;
      size_t _commitTimeoutMsec;
      std::vector<PolicyState> _consolidationPolicies;

      State(): _asyncMetaRevision(0) {} // differs from IResearchView constructor above
      State(IResearchViewMeta::CommitItemMeta const& meta)
        : _asyncMetaRevision(0), // differs from IResearchView constructor above
          _cleanupIntervalCount(0),
          _cleanupIntervalStep(meta._cleanupIntervalStep),
          _commitIntervalMsecRemainder(std::numeric_limits<size_t>::max()),
          _commitTimeoutMsec(meta._commitTimeoutMsec) {
        for (auto& entry: meta._consolidationPolicies) {
          if (entry.policy()) {
            _consolidationPolicies.emplace_back(
              entry.intervalStep(),
              irs::memory::make_unique<irs::index_writer::consolidation_policy_t>(entry.policy())
            );
          }
        }
      }
    };

    State state;
    ReadMutex mutex(_mutex); // '_meta' can be asynchronously modified

    for(;;) {
      // ...........................................................................
      // here sleep untill timeout
      // ...........................................................................
      {
        SCOPED_LOCK_NAMED(mutex, lock); // for '_meta._commitItem._commitIntervalMsec'
        SCOPED_LOCK_NAMED(_asyncMutex, asyncLock); // aquire before '_asyncTerminate' check

        if (_asyncTerminate.load()) {
          break;
        }

        if (!_meta._commitItem._commitIntervalMsec) {
          lock.unlock(); // do not hold read lock while waiting on condition
          _asyncCondition.wait(asyncLock); // wait forever
          continue;
        }

        auto startTime = std::chrono::system_clock::now();
        auto endTime = startTime
          + std::chrono::milliseconds(std::min(state._commitIntervalMsecRemainder, _meta._commitItem._commitIntervalMsec))
          ;

        lock.unlock(); // do not hold read lock while waiting on condition
        state._commitIntervalMsecRemainder = std::numeric_limits<size_t>::max(); // longest possible time assuming an uninterrupted sleep

        if (std::cv_status::timeout != _asyncCondition.wait_until(asyncLock, endTime)) {
          auto nowTime = std::chrono::system_clock::now();

          // if still need to sleep more then must relock '_meta' and sleep for min (remainder, interval)
          if (nowTime < endTime) {
            state._commitIntervalMsecRemainder = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - nowTime).count();

            continue; // need to reaquire lock to chech for change in '_meta'
          }
        }

        if (_asyncTerminate.load()) {
          break;
        }
      }

      // .............................................................................
      // reload state if required
      // .............................................................................
      if (_asyncMetaRevision.load() != state._asyncMetaRevision) {
        SCOPED_LOCK(mutex);
        state = State(_meta._commitItem);
        state._asyncMetaRevision = _asyncMetaRevision.load();
      }

      char runId = 0; // value not used
      auto thresholdMsec = TRI_microtime() * 1000 + state._commitTimeoutMsec;

      LOG_TOPIC(DEBUG, Logger::FIXME) << "starting flush for iResearch view '" << name() << "' run id '" << size_t(&runId) << "'";

      // .............................................................................
      // apply consolidation policies
      // .............................................................................
      for (auto& entry: state._consolidationPolicies) {
        if (!entry._intervalStep || ++entry._intervalCount < entry._intervalStep) {
          continue; // skip if interval not reached or no valid policy to execute
        }

        entry._intervalCount = 0;
        LOG_TOPIC(DEBUG, Logger::FIXME) << "starting consolidation for iResearch view '" << name() << "' run id '" << size_t(&runId) << "'";

        try {
          SCOPED_LOCK(mutex);

          for (auto& tidStore: _storeByTid) {
            for (auto& fidStore: tidStore.second._storeByFid) {
              fidStore.second._writer->consolidate(entry._policy, false);
            }
          }

          for (auto& fidStore: _storeByWalFid) {
            fidStore.second._writer->consolidate(entry._policy, false);
          }

          if (_storePersisted) {
            _storePersisted._writer->consolidate(entry._policy, false);
          }

          _storePersisted._writer->consolidate(entry._policy, false);
        } catch (std::exception& e) {
          LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while consolidating iResearch view '" << name() << "': " << e.what();
          IR_EXCEPTION();
        } catch (...) {
          LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while consolidating iResearch view '" << name() << "'";
          IR_EXCEPTION();
        }

        LOG_TOPIC(DEBUG, Logger::FIXME) << "finished consolidation for iResearch view '" << name() << "' run id '" << size_t(&runId) << "'";
      }

      // .............................................................................
      // apply data store commit
      // .............................................................................

      LOG_TOPIC(DEBUG, Logger::FIXME) << "starting commit for iResearch view '" << name() << "' run id '" << size_t(&runId) << "'";
      sync(std::max(size_t(1), size_t(thresholdMsec - TRI_microtime() * 1000))); // set min 1 msec to enable early termination
      LOG_TOPIC(DEBUG, Logger::FIXME) << "finished commit for iResearch view '" << name() << "' run id '" << size_t(&runId) << "'";

      // .............................................................................
      // apply cleanup
      // .............................................................................
      if (state._cleanupIntervalStep && ++state._cleanupIntervalCount >= state._cleanupIntervalStep) {
        state._cleanupIntervalCount = 0;
        LOG_TOPIC(DEBUG, Logger::FIXME) << "starting cleanup for iResearch view '" << name() << "' run id '" << size_t(&runId) << "'";
        cleanup(std::max(size_t(1), size_t(thresholdMsec - TRI_microtime() * 1000))); // set min 1 msec to enable early termination
        LOG_TOPIC(DEBUG, Logger::FIXME) << "finished cleanup for iResearch view '" << name() << "' run id '" << size_t(&runId) << "'";
      }

      LOG_TOPIC(DEBUG, Logger::FIXME) << "finished flush for iResearch view '" << name() << "' run id '" << size_t(&runId) << "'";
    }
  });
}

IResearchView::~IResearchView() {
  _asyncTerminate.store(true); // mark long-running async jobs for terminatation

  {
    SCOPED_LOCK(_asyncMutex);
    _asyncCondition.notify_all(); // trigger reload of settings for async jobs
  }

  _threadPool.max_threads_delta(_threadPool.tasks_pending()); // finish ASAP
  _threadPool.stop();

  WriteMutex mutex(_mutex); // '_meta' can be asynchronously read
  SCOPED_LOCK(mutex);

  if (_storePersisted) {
    _storePersisted._writer->commit();
    _storePersisted._writer->close();
    _storePersisted._writer.reset();
    _storePersisted._directory->close();
    _storePersisted._directory.reset();
  }
}

bool IResearchView::cleanup(size_t maxMsec /*= 0*/) {
  ReadMutex mutex(_mutex);
  auto thresholdSec = TRI_microtime() + maxMsec/1000.0;

  try {
    SCOPED_LOCK(mutex);

    for (auto& tidStore: _storeByTid) {
      for (auto& fidStore: tidStore.second._storeByFid) {
        LOG_TOPIC(DEBUG, Logger::FIXME) << "starting transaction-store cleanup for iResearch view '" << name() << "' tid '" << tidStore.first << "'" << "' fid '" << fidStore.first << "'";
        irs::directory_utils::remove_all_unreferenced(*(fidStore.second._directory));
        LOG_TOPIC(DEBUG, Logger::FIXME) << "finished transaction-store cleanup for iResearch view '" << name() << "' tid '" << tidStore.first << "'" << "' fid '" << fidStore.first << "'";

        if (maxMsec && TRI_microtime() >= thresholdSec) {
          return true; // skip if timout exceeded
        }
      }
    }

    for (auto& fidStore: _storeByWalFid) {
      LOG_TOPIC(DEBUG, Logger::FIXME) << "starting memory-store cleanup for iResearch view '" << name() << "' fid '" << fidStore.first << "'";
      fidStore.second._writer->commit();
      LOG_TOPIC(DEBUG, Logger::FIXME) << "finished memory-store cleanup for iResearch view '" << name() << "' fid '" << fidStore.first << "'";

      if (maxMsec && TRI_microtime() >= thresholdSec) {
        return true; // skip if timout exceeded
      }
    }

    if (_storePersisted) {
      LOG_TOPIC(DEBUG, Logger::FIXME) << "starting persisted-store cleanup for iResearch view '" << name() << "'";
      _storePersisted._writer->commit();
      LOG_TOPIC(DEBUG, Logger::FIXME) << "finished persisted-store cleanup for iResearch view '" << name() << "'";
    }

    return true;
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception during cleanup of iResearch view '" << name() << "': " << e.what();
    IR_EXCEPTION();
  } catch (...) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception during cleanup of iResearch view '" << name() << "'";
    IR_EXCEPTION();
  }

  return false;
}

void IResearchView::drop() {
  _asyncTerminate.store(true); // mark long-running async jobs for terminatation

  {
    SCOPED_LOCK(_asyncMutex);
    _asyncCondition.notify_all(); // trigger reload of settings for async jobs
  }

  _threadPool.stop();

  WriteMutex mutex(_mutex); // '_meta' can be asynchronously read
  SCOPED_LOCK(mutex); // members can be asynchronously updated

  // ...........................................................................
  // if an exception occurs below than a drop retry would most likely happen
  // ...........................................................................
  try {
    _storeByTid.clear();
    _storeByWalFid.clear();

    if (_storePersisted) {
      _storePersisted._writer->close();
      _storePersisted._writer.reset();
      _storePersisted._directory->close();
      _storePersisted._directory.reset();
    }

    if (TRI_ERROR_NO_ERROR == TRI_RemoveDirectory(_meta._dataPath.c_str())) {
      return; // success
    }
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while removing iResearch view '" << name() << "': " << e.what();
    IR_EXCEPTION();
    throw;
  } catch (...) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while removing iResearch view '" << name() << "'";
    IR_EXCEPTION();
    throw;
  }

  throw std::runtime_error(std::string("failed to remove iResearch view '") + name() + "'");
}

int IResearchView::drop(TRI_voc_cid_t cid) {
  if (!_logicalView || !_logicalView->getPhysical()) {
    LOG_TOPIC(WARN, Logger::FIXME) << "failed to find meta-store while dropping collection from iResearch view '" << name() <<"' cid '" << cid << "'";

    return TRI_ERROR_INTERNAL;
  }

  auto& metaStore = *(_logicalView->getPhysical());
  std::shared_ptr<irs::filter> shared_filter(iresearch::DocumentIterator::filter(cid));
  WriteMutex mutex(_mutex); // '_storeByTid' & '_storeByWalFid' can be asynchronously updated
  SCOPED_LOCK(mutex);

  _meta._collections.erase(cid); // will no longer be fully indexed
  mutex.unlock(true); // downgrade to a read-lock

  auto res = metaStore.persistProperties(); // persist '_meta' definition

  if (!res.ok()) {
    LOG_TOPIC(WARN, Logger::FIXME) << "failed to persisting view definition while dropping collection from iResearch view '" << name() <<"' cid '" << cid << "'";

    return res.errorNumber();
  }

  // ...........................................................................
  // if an exception occurs below than a drop retry would most likely happen
  // ...........................................................................
  try {
    for (auto& tidStore: _storeByTid) {
      for (auto& fidStore: tidStore.second._storeByFid) {
        fidStore.second._writer->remove(shared_filter);
      }
    }

    for (auto& fidStore: _storeByWalFid) {
      fidStore.second._writer->remove(shared_filter);
    }

    if (_storePersisted) {
      _storePersisted._writer->remove(shared_filter);
    }

    return TRI_ERROR_NO_ERROR;
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while removing from iResearch view '" << name() << "', collection '" << cid << "': " << e.what();
    IR_EXCEPTION();
  } catch (...) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while removing from iResearch view '" << name() << "', collection '" << cid << "'";
    IR_EXCEPTION();
  }

  return TRI_ERROR_INTERNAL;
}

void IResearchView::getPropertiesVPack(
  arangodb::velocypack::Builder& builder
) const {
  ReadMutex mutex(_mutex);
  SCOPED_LOCK(mutex); // '_meta' can be asynchronously updated

  _meta.json(builder);

  if (!_logicalView) {
    return; // nothing more to output
  }

  std::vector<std::string> collections;

  // add CIDs of fully indexed collections to list
  for (auto& entry: _meta._collections) {
    collections.emplace_back(arangodb::basics::StringUtils::itoa(entry));
  }

  {
    ReadMutex linksMutex(_linksMutex);
    SCOPED_LOCK(linksMutex); // '_links' can be asynchronously updated (small scope to get info for some-point-in-time)

    // add CIDs of registered collections to list
    for (auto& entry: _links) {
      if (entry) {
        auto* collection = entry->collection();

        if (collection) {
          collections.emplace_back(arangodb::basics::StringUtils::itoa(collection->cid()));
        }
      }
    }
  }

  arangodb::velocypack::Builder linksBuilder;

  try {
    static std::vector<std::string> const EMPTY;
    UserTransaction trx(
      transaction::StandaloneContext::Create(_logicalView->vocbase()),
      collections, // readCollections
      EMPTY, // writeCollections
      EMPTY, // exclusiveCollections
      transaction::Methods::DefaultLockTimeout, // lockTimeout
      false, // waitForSync
      false // allowImplicitCollections
    );

    auto res = trx.begin();

    if (TRI_ERROR_NO_ERROR != res) {
      return; // nothing more to output
    }

    auto* state = trx.state();

    if (!state) {
      return; // nothing more to output
    }

    arangodb::velocypack::ObjectBuilder linksBuilderWrapper(&linksBuilder);

    for (auto& collectionName: state->collectionNames()) {
      for (auto& index: trx.indexesForCollection(collectionName)) {
        if (index && arangodb::Index::IndexType::TRI_IDX_TYPE_IRESEARCH_LINK == index->type()) {
          // TODO FIXME find a better way to retrieve an iResearch Link
          #ifdef ARANGODB_ENABLE_MAINTAINER_MODE
            auto* ptr = reinterpret_cast<IResearchLink*>(index.get());
          #else
            auto* ptr = static_cast<IResearchLink*>(index.get());
          #endif

          if (!ptr || *ptr != *this) {
            continue; // the index is not a link for the current view
          }

          arangodb::velocypack::Builder linkBuilder;

          linkBuilder.openObject();
          ptr->toVelocyPack(linkBuilder, false);
          linkBuilder.close();
          linksBuilderWrapper->add(collectionName, linkBuilder.slice());
        }
      }
    }
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, arangodb::Logger::FIXME) << "caught error while generating json for iResearch view '" << name() << "': " << e.what();
    IR_EXCEPTION();
    return; // do not add 'links' section
  } catch (...) {
    LOG_TOPIC(WARN, arangodb::Logger::FIXME) << "caught error while generating json for iResearch view '" << name() << "'";
    IR_EXCEPTION();
    return; // do not add 'links' section
  }

  builder.add(LINKS_FIELD, linksBuilder.slice());
}

int IResearchView::insert(
    TRI_voc_fid_t fid,
    TRI_voc_tid_t tid,
    TRI_voc_cid_t cid,
    TRI_voc_rid_t rid,
    arangodb::velocypack::Slice const& doc,
    IResearchLinkMeta const& meta
) {
  FieldIterator body(doc, meta, _meta);

  if (!body.valid()) {
    // nothing to index
    return TRI_ERROR_NO_ERROR;
  }

  DocumentIterator fields(cid, rid, body);
  DocumentPrimaryKey attributes(cid, rid);

  WriteMutex mutex(_mutex); // '_storeByTid' & '_storeByFid' can be asynchronously updated
  SCOPED_LOCK(mutex);
  auto& store = _storeByTid[tid]._storeByFid[fid];

  mutex.unlock(true); // downgrade to a read-lock

  try {
    if (store._writer->insert(fields.begin(), fields.end(), attributes.begin(), attributes.end())) {
      return TRI_ERROR_NO_ERROR;
    }

    LOG_TOPIC(WARN, Logger::FIXME) << "failed inserting into iResearch view '" << name() << "', collection '" << cid << "', revision '" << rid << "'";
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while inserting into iResearch view '" << name() << "', collection '" << cid << "', revision '" << rid << "': " << e.what();
    IR_EXCEPTION();
  } catch (...) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while inserting into iResearch view '" << name() << "', collection '" << cid << "', revision '" << rid << "'";
    IR_EXCEPTION();
  }

  return TRI_ERROR_INTERNAL;
}

size_t IResearchView::linkCount() const noexcept {
  ReadMutex mutex(_linksMutex); // '_links' can be asynchronously updated
  SCOPED_LOCK(mutex);

  return _links.size();
}

/*static*/ IResearchView* IResearchView::linkRegister(
    TRI_vocbase_t& vocbase,
    std::string const& viewName,
    LinkPtr const& ptr
) {
  if (!ptr) {
    return nullptr; // do not register empty pointers
  }

  auto logicalView = vocbase.lookupView(viewName);

  if (!logicalView || IResearchView::type() != logicalView->type()) {
    return nullptr; // no such view
  }

  // TODO FIXME find a better way to look up an iResearch View
  #ifdef ARANGODB_ENABLE_MAINTAINER_MODE
    auto* view = reinterpret_cast<IResearchView*>(logicalView->getImplementation());
  #else
    auto* view = static_cast<IResearchView*>(logicalView->getImplementation());
  #endif

  if (!view) {
    return nullptr;
  }

  WriteMutex mutex(view->_linksMutex); // '_links' can be asynchronously updated
  SCOPED_LOCK(mutex);

  return view->_links.emplace(ptr).second ? view : nullptr; // no duplicate registration
}

bool IResearchView::linkUnregister(LinkPtr const& ptr) {
  WriteMutex mutex(_linksMutex); // '_links' can be asynchronously updated
  SCOPED_LOCK(mutex);

  return _links.erase(ptr) > 0;
}

/*static*/ IResearchView::ptr IResearchView::make(
  arangodb::LogicalView* view,
  arangodb::velocypack::Slice const& info,
  bool isNew
) {
  PTR_NAMED(IResearchView, ptr, view, info);
  auto& impl = reinterpret_cast<IResearchView&>(*ptr);
  std::string error;

  if (!impl._meta.init(info, error)) {
    LOG_TOPIC(WARN, Logger::FIXME) << "failed to initialize iResearch view from definition, error: " << error;

    return nullptr;
  }

  // skip link creation for previously created views or if no links were specified in the definition
  if (!isNew || !info.hasKey(LINKS_FIELD)) {
    return std::move(ptr);
  }

  if (!impl._logicalView || !impl._logicalView->vocbase()) {
    LOG_TOPIC(WARN, Logger::FIXME) << "failed to find vocbase while updating links for iResearch view '" << impl.name() << "'";

    return nullptr;
  }

  // create links for a new iresearch View instance
  auto res = updateLinks(*(impl._logicalView->vocbase()), impl, info.get(LINKS_FIELD));

  return res.ok() ? std::move(ptr) : nullptr;
}

size_t IResearchView::memory() const {
  ReadMutex mutex(_mutex); // view members can be asynchronously updated
  SCOPED_LOCK(mutex);
  size_t size = sizeof(IResearchView);

  {
    ReadMutex linksMutex(_linksMutex);
    SCOPED_LOCK(linksMutex); // '_links' can be asynchronously updated (small scope to get info for some-point-in-time)

    for (auto& entry: _links) {
      size += sizeof(entry.get()) + sizeof(*(entry.get()));

      if (entry) {
        size += entry->memory();
      }
    }
  }

  size += _meta.memory();

  for (auto& tidEntry: _storeByTid) {
    size += sizeof(tidEntry.first) + sizeof(tidEntry.second);

    for (auto& fidEntry: tidEntry.second._storeByFid) {
      size += sizeof(fidEntry.first) + sizeof(fidEntry.second);
      size += directoryMemory(*(fidEntry.second._directory), name());
    }

    // no way to determine size of actual filter
    SCOPED_LOCK(tidEntry.second._mutex);
    size += tidEntry.second._removals.size() * (sizeof(decltype(tidEntry.second._removals)::pointer) + sizeof(decltype(tidEntry.second._removals)::value_type));
  }

  for (auto& fidEntry: _storeByWalFid) {
    size += sizeof(fidEntry.first) + sizeof(fidEntry.second);
    size += directoryMemory(*(fidEntry.second._directory), name());
  }

  if (_storePersisted) {
    size += directoryMemory(*(_storePersisted._directory), name());
  }

  return size;
}

std::string const& IResearchView::name() const noexcept {
  ReadMutex mutex(_mutex); // '_meta' can be asynchronously updated
  SCOPED_LOCK(mutex);

  return _meta._name;
}

void IResearchView::open() {
  WriteMutex mutex(_mutex); // '_meta' can be asynchronously updated
  SCOPED_LOCK(mutex);

  if (_storePersisted) {
    return; // view already open
  }

  try {
    auto format = irs::formats::get("1_0");

    if (format) {
      _storePersisted._directory =
        irs::directory::make<irs::fs_directory>(_meta._dataPath);

      if (_storePersisted._directory) {
        // create writer before reader to ensure data directory is present
        _storePersisted._writer =
          irs::index_writer::make(*(_storePersisted._directory), format, irs::OM_CREATE_APPEND);

        if (_storePersisted._writer) {
          _storePersisted._writer->commit(); // initialize 'store'
          _threadPool.max_idle(_meta._threadsMaxIdle);
          _threadPool.max_threads(_meta._threadsMaxTotal); // start pool

          return; // success
        }
      }
    }
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while opening iResearch view '" << name() << "': " << e.what();
    IR_EXCEPTION();
    throw;
  } catch (...) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while opening iResearch view '" << name() << "'";
    IR_EXCEPTION();
    throw;
  }

  LOG_TOPIC(WARN, Logger::FIXME) << "failed to open iResearch view '" << name() << "'";
  throw std::runtime_error(std::string("failed to open iResearch view '") + name() + "'");
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
  std::shared_ptr<irs::filter> shared_filter(iresearch::DocumentIterator::filter(cid, rid));
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
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while removing from iResearch view '" << name() << "', collection '" << cid << "', revision '" << rid << "': " << e.what();
    IR_EXCEPTION();
  } catch (...) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while removing from iResearch view '" << name() << "', collection '" << cid << "', revision '" << rid << "'";
    IR_EXCEPTION();
  }

  return TRI_ERROR_INTERNAL;
}

bool IResearchView::sync(size_t maxMsec /*= 0*/) {
  ReadMutex mutex(_mutex);
  auto thresholdSec = TRI_microtime() + maxMsec/1000.0;

  try {
    SCOPED_LOCK(mutex);

    for (auto& tidStore: _storeByTid) {
      for (auto& fidStore: tidStore.second._storeByFid) {
        LOG_TOPIC(DEBUG, Logger::FIXME) << "starting transaction-store sync for iResearch view '" << name() << "' tid '" << tidStore.first << "'" << "' fid '" << fidStore.first << "'";
        fidStore.second._writer->commit();
        LOG_TOPIC(DEBUG, Logger::FIXME) << "finished transaction-store sync for iResearch view '" << name() << "' tid '" << tidStore.first << "'" << "' fid '" << fidStore.first << "'";

        if (maxMsec && TRI_microtime() >= thresholdSec) {
          return true; // skip if timout exceeded
        }
      }
    }

    for (auto& fidStore: _storeByWalFid) {
      LOG_TOPIC(DEBUG, Logger::FIXME) << "starting memory-store sync for iResearch view '" << name() << "' fid '" << fidStore.first << "'";
      fidStore.second._writer->commit();
      LOG_TOPIC(DEBUG, Logger::FIXME) << "finished memory-store sync for iResearch view '" << name() << "' fid '" << fidStore.first << "'";

      if (maxMsec && TRI_microtime() >= thresholdSec) {
        return true; // skip if timout exceeded
      }
    }

    if (_storePersisted) {
      LOG_TOPIC(DEBUG, Logger::FIXME) << "starting persisted-sync cleanup for iResearch view '" << name() << "'";
      _storePersisted._writer->commit();
      LOG_TOPIC(DEBUG, Logger::FIXME) << "finished persisted-sync cleanup for iResearch view '" << name() << "'";
    }

    return true;
  } catch (std::exception& e) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception during sync of iResearch view '" << name() << "': " << e.what();
    IR_EXCEPTION();
  } catch (...) {
    LOG_TOPIC(WARN, Logger::FIXME) << "caught exception during sync of iResearch view '" << name() << "'";
    IR_EXCEPTION();
  }

  return false;
}

/*static*/ std::string const& IResearchView::type() noexcept {
  static const std::string  type = "iresearch";

  return type;
}

arangodb::Result IResearchView::updateProperties(
  arangodb::velocypack::Slice const& slice,
  bool doSync
) {
  if (!_logicalView || !_logicalView->getPhysical()) {
    return arangodb::Result(
      TRI_ERROR_INTERNAL,
      std::string("failed to find meta-store while updating iResearch view '") + name() + "'"
    );
  }

  auto& metaStore = *(_logicalView->getPhysical());
  std::string error;
  IResearchViewMeta meta;
  IResearchViewMeta::Mask mask;
  WriteMutex mutex(_mutex); // '_meta' can be asynchronously read
  SCOPED_LOCK(mutex);

  if (!meta.init(slice, error, _meta, &mask)) {
    return arangodb::Result(TRI_ERROR_BAD_PARAMETER, std::move(error));
  }

  // update links if requested
  if (slice.hasKey(LINKS_FIELD)) {
    auto* vocbase = _logicalView->vocbase();

    if (!vocbase) {
      return arangodb::Result(
        TRI_ERROR_INTERNAL,
        std::string("failed to find vocbase while updating links for iResearch view '") + name() + "'"
      );
    }

    auto res = updateLinks(*vocbase, *this, slice.get(LINKS_FIELD));

    if (!res.ok()) {
      return res;
    }
  }

  // reset non-updatable values to match current meta
  meta._collections = _meta._collections;
  meta._iid = _meta._iid;
  meta._name = _meta._name;

  DataStore storePersisted; // renamed persisted data store
  std::string srcDataPath = _meta._dataPath;
  char const* dropDataPath = nullptr;

  // copy directory to new location
  if (mask._dataPath) {
    auto res = createPersistedDataDirectory(
      storePersisted._directory,
      storePersisted._writer,
      meta._dataPath,
      _storePersisted._reader, // reader from the original persisted data store
      name()
    );

    if (!res.ok()) {
      return res;
    }

    try {
      storePersisted._reader = irs::directory_reader::open(*(storePersisted._directory));
      dropDataPath = _storePersisted ? srcDataPath.c_str() : nullptr;
    } catch (std::exception& e) {
      LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while opening iResearch view '" << name() << "' data path '" + meta._dataPath + "': " << e.what();
      IR_EXCEPTION();
      return arangodb::Result(
        TRI_ERROR_BAD_PARAMETER,
        std::string("error opening iResearch view '") + name() + "' data path '" + meta._dataPath + "'"
      );
    } catch (...) {
      LOG_TOPIC(WARN, Logger::FIXME) << "caught exception while opening iResearch view '" << name() << "' data path '" + meta._dataPath + "'";
      IR_EXCEPTION();
      return arangodb::Result(
        TRI_ERROR_BAD_PARAMETER,
        std::string("error opening iResearch view '") + name() + "' data path '" + meta._dataPath + "'"
      );
    }
  }

  if (mask._threadsMaxIdle) {
    _threadPool.max_idle(meta._threadsMaxIdle);
  }

  if (mask._threadsMaxTotal) {
    _threadPool.max_threads(meta._threadsMaxTotal);
  }

  _meta = std::move(meta);

  if (mask._dataPath) {
    _storePersisted = std::move(storePersisted);
  }

  {
    SCOPED_LOCK(_asyncMutex);
    _asyncCondition.notify_all(); // trigger reload of timeout settings for async jobs
  }

  mutex.unlock(true); // downgrade to a read-lock

  // on failure the iResearch View is in an inconsistent state between the in-memory definition and the persisted definition
  // since canot revert meta because write-lock was released to allow call getPropertiesVPack(..) to complete
  auto res = metaStore.persistProperties(); // persist '_meta' definition

  return res.ok() && dropDataPath ? arangodb::Result(TRI_RemoveDirectory(dropDataPath)) : res;
}

NS_END // iresearch
NS_END // arangodb

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
