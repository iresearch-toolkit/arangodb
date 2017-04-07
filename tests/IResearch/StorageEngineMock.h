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

#ifndef ARANGODB_IRESEARCH__IRESEARCH_STORAGE_ENGINE_MOCK_H
#define ARANGODB_IRESEARCH__IRESEARCH_STORAGE_ENGINE_MOCK_H 1

#include "StorageEngine/StorageEngine.h"
#include "StorageEngine/TransactionCollection.h"
#include "StorageEngine/TransactionState.h"
#include "VocBase/PhysicalCollection.h"
#include "VocBase/PhysicalView.h"

class PhysicalCollectionMock: public arangodb::PhysicalCollection {
 public:
  PhysicalCollectionMock(arangodb::LogicalCollection* collection, arangodb::velocypack::Slice const& info);
  virtual bool applyForTickRange(TRI_voc_tick_t dataMin, TRI_voc_tick_t dataMax, std::function<bool(TRI_voc_tick_t foundTick, TRI_df_marker_t const* marker)> const& callback) override;
  virtual PhysicalCollection* clone(arangodb::LogicalCollection*, PhysicalCollection*) override;
  virtual int close() override;
  virtual std::shared_ptr<arangodb::Index> createIndex(arangodb::transaction::Methods* trx, arangodb::velocypack::Slice const& info, bool& created) override;
  virtual void deferDropCollection(std::function<bool(arangodb::LogicalCollection*)> callback) override;
  virtual bool doCompact() const override;
  virtual bool dropIndex(TRI_idx_iid_t iid) override;
  virtual void figuresSpecific(std::shared_ptr<arangodb::velocypack::Builder>&) override;
  virtual std::unique_ptr<arangodb::IndexIterator> getAllIterator(arangodb::transaction::Methods* trx, arangodb::ManagedDocumentResult* mdr, bool reverse) override;
  virtual std::unique_ptr<arangodb::IndexIterator> getAnyIterator(arangodb::transaction::Methods* trx, arangodb::ManagedDocumentResult* mdr) override;
  virtual void getPropertiesVPack(arangodb::velocypack::Builder&) const override;
  virtual uint32_t indexBuckets() const override;
  virtual int64_t initialCount() const override;
  virtual int insert(arangodb::transaction::Methods* trx, arangodb::velocypack::Slice const newSlice, arangodb::ManagedDocumentResult& result, arangodb::OperationOptions& options, TRI_voc_tick_t& resultMarkerTick, bool lock) override;
  virtual void invokeOnAllElements(std::function<bool(arangodb::DocumentIdentifierToken const&)> callback) override;
  virtual bool isFullyCollected() const override;
  virtual int iterateMarkersOnLoad(arangodb::transaction::Methods* trx) override;
  virtual size_t journalSize() const override;
  virtual std::shared_ptr<arangodb::Index> lookupIndex(arangodb::velocypack::Slice const&) const override;
  virtual uint8_t const* lookupRevisionVPack(TRI_voc_rid_t revisionId) const override;
  virtual uint8_t const* lookupRevisionVPackConditional(TRI_voc_rid_t revisionId, TRI_voc_tick_t maxTick, bool excludeWal) const override;
  virtual size_t memory() const override;
  virtual uint64_t numberDocuments() const override;
  virtual void open(bool ignoreErrors) override;
  virtual std::string const& path() const override;
  virtual arangodb::Result persistProperties() override;
  virtual void prepareIndexes(arangodb::velocypack::Slice indexesSlice) override;
  virtual int read(arangodb::transaction::Methods*, arangodb::velocypack::Slice const key, arangodb::ManagedDocumentResult& result, bool) override;
  virtual bool readDocument(arangodb::transaction::Methods* trx, arangodb::DocumentIdentifierToken const& token, arangodb::ManagedDocumentResult& result) override;
  virtual bool readDocumentConditional(arangodb::transaction::Methods* trx, arangodb::DocumentIdentifierToken const& token, TRI_voc_tick_t maxTick, arangodb::ManagedDocumentResult& result) override;
  virtual int remove(arangodb::transaction::Methods* trx, arangodb::velocypack::Slice const slice, arangodb::ManagedDocumentResult& previous, arangodb::OperationOptions& options, TRI_voc_tick_t& resultMarkerTick, bool lock, TRI_voc_rid_t const& revisionId, TRI_voc_rid_t& prevRev) override;
  virtual int replace(arangodb::transaction::Methods* trx, arangodb::velocypack::Slice const newSlice, arangodb::ManagedDocumentResult& result, arangodb::OperationOptions& options, TRI_voc_tick_t& resultMarkerTick, bool lock, TRI_voc_rid_t& prevRev, arangodb::ManagedDocumentResult& previous, TRI_voc_rid_t const revisionId, arangodb::velocypack::Slice const fromSlice, arangodb::velocypack::Slice const toSlice) override;
  virtual int restoreIndex(arangodb::transaction::Methods*, arangodb::velocypack::Slice const&, std::shared_ptr<arangodb::Index>&) override;
  virtual TRI_voc_rid_t revision() const override;
  virtual int rotateActiveJournal() override;
  virtual void setPath(std::string const&) override;
  virtual void sizeHint(arangodb::transaction::Methods* trx, int64_t hint) override;
  virtual void truncate(arangodb::transaction::Methods* trx, arangodb::OperationOptions& options) override;
  virtual int update(arangodb::transaction::Methods* trx, arangodb::velocypack::Slice const newSlice, arangodb::ManagedDocumentResult& result, arangodb::OperationOptions& options, TRI_voc_tick_t& resultMarkerTick, bool lock, TRI_voc_rid_t& prevRev, arangodb::ManagedDocumentResult& previous, TRI_voc_rid_t const& revisionId, arangodb::velocypack::Slice const key) override;
  virtual void updateCount(int64_t) override;
  virtual arangodb::Result updateProperties(arangodb::velocypack::Slice const& slice, bool doSync) override;
};

class PhysicalViewMock: public arangodb::PhysicalView {
 public:
  static int persistPropertiesResult;

  PhysicalViewMock(arangodb::LogicalView* view, arangodb::velocypack::Slice const& info);
  virtual PhysicalView* clone(arangodb::LogicalView*, arangodb::PhysicalView*) override;
  virtual void drop() override;
  virtual void getPropertiesVPack(arangodb::velocypack::Builder&, bool includeSystem = false) const override;
  virtual void open() override;
  virtual std::string const& path() const override;
  virtual arangodb::Result persistProperties() override;
  virtual void setPath(std::string const&) override;
  virtual arangodb::Result updateProperties(arangodb::velocypack::Slice const& slice, bool doSync) override;
};

class TransactionCollectionMock: public arangodb::TransactionCollection {
 public:
  TransactionCollectionMock(arangodb::TransactionState* state, TRI_voc_cid_t cid);
  virtual bool canAccess(arangodb::AccessMode::Type accessType) const override;
  virtual void freeOperations(arangodb::transaction::Methods* activeTrx, bool mustRollback) override;
  virtual bool hasOperations() const override;
  virtual bool isLocked() const override;
  virtual bool isLocked(arangodb::AccessMode::Type, int nestingLevel) const override;
  virtual int lock() override;
  virtual int lock(arangodb::AccessMode::Type, int nestingLevel) override;
  virtual void release() override;
  virtual int unlock(arangodb::AccessMode::Type, int nestingLevel) override;
  virtual int updateUsage(arangodb::AccessMode::Type accessType, int nestingLevel) override;
  virtual void unuse(int nestingLevel) override;
  virtual int use(int nestingLevel) override;
};

class TransactionStateMock: public arangodb::TransactionState {
 public:
  static size_t abortTransactionCount;
  static size_t beginTransactionCount;
  static size_t commitTransactionCount;

  TransactionStateMock(TRI_vocbase_t* vocbase);
  virtual int abortTransaction(arangodb::transaction::Methods* trx) override;
  virtual int beginTransaction(arangodb::transaction::Hints hints) override;
  virtual int commitTransaction(arangodb::transaction::Methods* trx) override;
  virtual bool hasFailedOperations() const override;
};

class StorageEngineMock: public arangodb::StorageEngine {
 public:
  StorageEngineMock();
  virtual void addAqlFunctions() override;
  virtual void addDocumentRevision(TRI_voc_tick_t databaseId, TRI_voc_cid_t collectionId, arangodb::velocypack::Slice const& document) override;
  virtual void addOptimizerRules() override;
  virtual void addRestHandlers(arangodb::rest::RestHandlerFactory*) override;
  virtual void addV8Functions() override;
  virtual void changeCollection(TRI_vocbase_t* vocbase, TRI_voc_cid_t id, arangodb::LogicalCollection const* parameters, bool doSync) override;
  virtual void changeView(TRI_vocbase_t* vocbase, TRI_voc_cid_t id, arangodb::LogicalView const*, bool doSync) override;
  virtual bool cleanupCompactionBlockers(TRI_vocbase_t* vocbase) override;
  virtual std::string collectionPath(TRI_vocbase_t const* vocbase, TRI_voc_cid_t id) const override;
  virtual std::string createCollection(TRI_vocbase_t* vocbase, TRI_voc_cid_t id, arangodb::LogicalCollection const*) override;
  virtual Database* createDatabase(TRI_voc_tick_t id, arangodb::velocypack::Slice const& args, int& status) override;
  virtual void createIndex(TRI_vocbase_t* vocbase, TRI_voc_cid_t collectionId, TRI_idx_iid_t id, arangodb::velocypack::Slice const& data) override;
  virtual arangodb::PhysicalCollection* createPhysicalCollection(arangodb::LogicalCollection* collection, VPackSlice const& info) override;
  virtual arangodb::PhysicalView* createPhysicalView(arangodb::LogicalView* view, VPackSlice const& info) override;
  virtual arangodb::TransactionCollection* createTransactionCollection(arangodb::TransactionState* state, TRI_voc_cid_t cid, arangodb::AccessMode::Type, int nestingLevel) override;
  virtual arangodb::transaction::ContextData* createTransactionContextData() override;
  virtual arangodb::TransactionState* createTransactionState(TRI_vocbase_t* vocbase) override;
  virtual void createView(TRI_vocbase_t* vocbase, TRI_voc_cid_t id, arangodb::LogicalView const*) override;
  virtual std::string databasePath(TRI_vocbase_t const* vocbase) const override;
  virtual void destroyCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection* collection) override;
  virtual void destroyView(TRI_vocbase_t* vocbase, arangodb::LogicalView*) override;
  virtual arangodb::Result dropCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection* collection) override;
  virtual void dropDatabase(Database*, int& status) override;
  virtual void dropIndex(TRI_vocbase_t* vocbase, TRI_voc_cid_t collectionId, TRI_idx_iid_t id) override;
  virtual void dropIndexWalMarker(TRI_vocbase_t* vocbase, TRI_voc_cid_t collectionId, arangodb::velocypack::Slice const& data, bool useMarker, int&) override;
  virtual arangodb::Result dropView(TRI_vocbase_t* vocbase, arangodb::LogicalView*) override;
  virtual int extendCompactionBlocker(TRI_vocbase_t* vocbase, TRI_voc_tick_t id, double ttl) override;
  virtual void getCollectionInfo(TRI_vocbase_t* vocbase, TRI_voc_cid_t cid, arangodb::velocypack::Builder& result, bool includeIndexes, TRI_voc_tick_t maxTick) override;
  virtual int getCollectionsAndIndexes(TRI_vocbase_t* vocbase, arangodb::velocypack::Builder& result, bool wasCleanShutdown, bool isUpgrade) override;
  virtual void getDatabases(arangodb::velocypack::Builder& result) override;
  virtual int getViews(TRI_vocbase_t* vocbase, arangodb::velocypack::Builder& result) override;
  virtual int insertCompactionBlocker(TRI_vocbase_t* vocbase, double ttl, TRI_voc_tick_t& id) override;
  virtual void iterateDocuments(TRI_voc_tick_t databaseId, TRI_voc_cid_t collectionId, std::function<void(arangodb::velocypack::Slice const&)> const& cb) override;
  virtual int openCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection* collection, bool ignoreErrors) override;
  virtual Database* openDatabase(arangodb::velocypack::Slice const& args, bool isUpgrade, int& status) override;
  virtual arangodb::Result persistCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection const* collection) override;
  virtual arangodb::Result persistView(TRI_vocbase_t* vocbase, arangodb::LogicalView const*) override;
  virtual void prepareDropDatabase(TRI_vocbase_t* vocbase, bool useWriteMarker, int& status) override;
  virtual void preventCompaction(TRI_vocbase_t* vocbase, std::function<void(TRI_vocbase_t*)> const& callback) override;
  virtual int removeCompactionBlocker(TRI_vocbase_t* vocbase, TRI_voc_tick_t id) override;
  virtual void removeDocumentRevision(TRI_voc_tick_t databaseId, TRI_voc_cid_t collectionId, arangodb::velocypack::Slice const& document) override;
  virtual arangodb::Result renameCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection const* collection, std::string const& oldName) override;
  virtual int shutdownDatabase(TRI_vocbase_t* vocbase) override;
  virtual void signalCleanup(TRI_vocbase_t* vocbase) override;
  virtual bool tryPreventCompaction(TRI_vocbase_t* vocbase, std::function<void(TRI_vocbase_t*)> const& callback, bool checkForActiveBlockers) override;
  virtual void unloadCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection* collection) override;
  virtual void waitForSync(TRI_voc_tick_t tick) override;
  virtual void waitUntilDeletion(TRI_voc_tick_t id, bool force, int& status) override;
  virtual int writeCreateMarker(TRI_voc_tick_t id, VPackSlice const& slice) override;
};

#endif