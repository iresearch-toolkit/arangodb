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

#include "catch.hpp"

#include "utils/locale_utils.hpp"
#include "utils/utf8_path.hpp"

#include "Basics/files.h"
#include "Indexes/IndexIterator.h"
#include "IResearch/IResearchLink.h"
#include "IResearch/IResearchLinkMeta.h"
#include "IResearch/IResearchView.h"
#include "RestServer/ViewTypesFeature.h"
#include "StorageEngine/EngineSelectorFeature.h"
#include "StorageEngine/StorageEngine.h"
#include "StorageEngine/TransactionCollection.h"
#include "StorageEngine/TransactionState.h"
#include "velocypack/Iterator.h"
#include "velocypack/Parser.h"
#include "VocBase/KeyGenerator.h"
#include "VocBase/LogicalView.h"
#include "VocBase/PhysicalCollection.h"
#include "VocBase/PhysicalView.h"

NS_LOCAL

class PhysicalCollectionMock: public arangodb::PhysicalCollection {
 public:
  PhysicalCollectionMock(arangodb::LogicalCollection* collection, VPackSlice const& info): PhysicalCollection(collection, info) {}
  virtual bool applyForTickRange(TRI_voc_tick_t dataMin, TRI_voc_tick_t dataMax, std::function<bool(TRI_voc_tick_t foundTick, TRI_df_marker_t const* marker)> const& callback) override { TRI_ASSERT(false); return false; }
  virtual PhysicalCollection* clone(arangodb::LogicalCollection*, PhysicalCollection*) override { TRI_ASSERT(false); return nullptr; }
  virtual int close() override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual std::shared_ptr<arangodb::Index> createIndex(arangodb::transaction::Methods* trx, arangodb::velocypack::Slice const& info, bool& created) override { _indexes.emplace_back(arangodb::iresearch::IResearchLink::make(1, _logicalCollection, info)); created = true; return _indexes.back(); }
  virtual void deferDropCollection(std::function<bool(arangodb::LogicalCollection*)> callback) override { TRI_ASSERT(false); }
  virtual bool doCompact() const override { TRI_ASSERT(false); return false; }
  virtual bool dropIndex(TRI_idx_iid_t iid) override { for(auto& itr = _indexes.begin(), end = _indexes.end(); itr != end; ++itr) { if ((*itr)->id() == iid) { _indexes.erase(itr); return true; } } return false; }
  virtual void figuresSpecific(std::shared_ptr<arangodb::velocypack::Builder>&) override { TRI_ASSERT(false); }
  virtual std::unique_ptr<arangodb::IndexIterator> getAllIterator(arangodb::transaction::Methods* trx, arangodb::ManagedDocumentResult* mdr, bool reverse) override { TRI_ASSERT(false); return nullptr; }
  virtual std::unique_ptr<arangodb::IndexIterator> getAnyIterator(arangodb::transaction::Methods* trx, arangodb::ManagedDocumentResult* mdr) override { TRI_ASSERT(false); return nullptr; }
  virtual void getPropertiesVPack(arangodb::velocypack::Builder&) const override { TRI_ASSERT(false); }
  virtual uint32_t indexBuckets() const override { TRI_ASSERT(false); return 0; }
  virtual int64_t initialCount() const override { TRI_ASSERT(false); return 0; }
  virtual int insert(arangodb::transaction::Methods* trx, arangodb::velocypack::Slice const newSlice, arangodb::ManagedDocumentResult& result, arangodb::OperationOptions& options, TRI_voc_tick_t& resultMarkerTick, bool lock) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual void invokeOnAllElements(std::function<bool(arangodb::DocumentIdentifierToken const&)> callback) override { TRI_ASSERT(false); }
  virtual bool isFullyCollected() const override { TRI_ASSERT(false); return false; }
  virtual int iterateMarkersOnLoad(arangodb::transaction::Methods* trx) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual size_t journalSize() const override { TRI_ASSERT(false); return 0; }
  virtual std::shared_ptr<arangodb::Index> lookupIndex(arangodb::velocypack::Slice const&) const override { TRI_ASSERT(false); return nullptr; }
  virtual uint8_t const* lookupRevisionVPack(TRI_voc_rid_t revisionId) const override { TRI_ASSERT(false); return nullptr; }
  virtual uint8_t const* lookupRevisionVPackConditional(TRI_voc_rid_t revisionId, TRI_voc_tick_t maxTick, bool excludeWal) const override { TRI_ASSERT(false); return nullptr; }
  virtual size_t memory() const override { TRI_ASSERT(false); return 0; }
  virtual uint64_t numberDocuments() const override { TRI_ASSERT(false); return 0; }
  virtual void open(bool ignoreErrors) override { TRI_ASSERT(false); }
  virtual std::string const& path() const override { TRI_ASSERT(false); static std::string value; return value; }
  virtual arangodb::Result persistProperties() override { TRI_ASSERT(false); return arangodb::Result(TRI_ERROR_INTERNAL); }
  virtual void prepareIndexes(arangodb::velocypack::Slice indexesSlice) override { TRI_ASSERT(false); }
  virtual int read(arangodb::transaction::Methods*, arangodb::velocypack::Slice const key, arangodb::ManagedDocumentResult& result, bool) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual bool readDocument(arangodb::transaction::Methods* trx, arangodb::DocumentIdentifierToken const& token, arangodb::ManagedDocumentResult& result) override { TRI_ASSERT(false); return false; }
  virtual bool readDocumentConditional(arangodb::transaction::Methods* trx, arangodb::DocumentIdentifierToken const& token, TRI_voc_tick_t maxTick, arangodb::ManagedDocumentResult& result) override { TRI_ASSERT(false); return false; }
  virtual int remove(arangodb::transaction::Methods* trx, arangodb::velocypack::Slice const slice, arangodb::ManagedDocumentResult& previous, arangodb::OperationOptions& options, TRI_voc_tick_t& resultMarkerTick, bool lock, TRI_voc_rid_t const& revisionId, TRI_voc_rid_t& prevRev) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual int replace(arangodb::transaction::Methods* trx, arangodb::velocypack::Slice const newSlice, arangodb::ManagedDocumentResult& result, arangodb::OperationOptions& options, TRI_voc_tick_t& resultMarkerTick, bool lock, TRI_voc_rid_t& prevRev, arangodb::ManagedDocumentResult& previous, TRI_voc_rid_t const revisionId, arangodb::velocypack::Slice const fromSlice, arangodb::velocypack::Slice const toSlice) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual int restoreIndex(arangodb::transaction::Methods*, arangodb::velocypack::Slice const&, std::shared_ptr<arangodb::Index>&) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual TRI_voc_rid_t revision() const override { TRI_ASSERT(false); return 0; }
  virtual int rotateActiveJournal() override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual void setPath(std::string const&) override { TRI_ASSERT(false); }
  virtual void sizeHint(arangodb::transaction::Methods* trx, int64_t hint) override { TRI_ASSERT(false); }
  virtual void truncate(arangodb::transaction::Methods* trx, arangodb::OperationOptions& options) override { TRI_ASSERT(false); }
  virtual int update(arangodb::transaction::Methods* trx, arangodb::velocypack::Slice const newSlice, arangodb::ManagedDocumentResult& result, arangodb::OperationOptions& options, TRI_voc_tick_t& resultMarkerTick, bool lock, TRI_voc_rid_t& prevRev, arangodb::ManagedDocumentResult& previous, TRI_voc_rid_t const& revisionId, arangodb::velocypack::Slice const key) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual void updateCount(int64_t) override { TRI_ASSERT(false); }
  virtual arangodb::Result updateProperties(arangodb::velocypack::Slice const& slice, bool doSync) override { TRI_ASSERT(false); return arangodb::Result(TRI_ERROR_INTERNAL); }
};

class PhysicalViewMock: public arangodb::PhysicalView {
 public:
  static int persistPropertiesResult;

  PhysicalViewMock(arangodb::LogicalView* view, VPackSlice const& info): PhysicalView(view, info) {}
  virtual PhysicalView* clone(arangodb::LogicalView*, arangodb::PhysicalView*) override { TRI_ASSERT(false); return nullptr; }
  virtual void drop() override { TRI_ASSERT(false); }
  virtual void getPropertiesVPack(arangodb::velocypack::Builder&, bool includeSystem = false) const override { TRI_ASSERT(false); }
  virtual void open() override { TRI_ASSERT(false); }
  virtual std::string const& path() const override { TRI_ASSERT(false); static std::string invalid("<invalid>"); return invalid; }
  virtual arangodb::Result persistProperties() override { return arangodb::Result(persistPropertiesResult); }
  virtual void setPath(std::string const&) override { TRI_ASSERT(false); }
  virtual arangodb::Result updateProperties(arangodb::velocypack::Slice const& slice, bool doSync) override { TRI_ASSERT(false); return arangodb::Result(TRI_ERROR_INTERNAL); }
};

int PhysicalViewMock::persistPropertiesResult;

class TransactionCollectionMock: public arangodb::TransactionCollection {
 public:
   TransactionCollectionMock(arangodb::TransactionState* state, TRI_voc_cid_t cid): TransactionCollection(state, cid) { }
   virtual bool canAccess(arangodb::AccessMode::Type accessType) const override { return true; }
   virtual void freeOperations(arangodb::transaction::Methods* activeTrx, bool mustRollback) override { TRI_ASSERT(false); }
   virtual bool hasOperations() const override { TRI_ASSERT(false); return false; }
   virtual bool isLocked() const override { TRI_ASSERT(false); return false; }
   virtual bool isLocked(arangodb::AccessMode::Type, int nestingLevel) const override { TRI_ASSERT(false); return false; }
   virtual int lock() override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
   virtual int lock(arangodb::AccessMode::Type, int nestingLevel) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
   virtual void release() override { if (_collection) { _transaction->vocbase()->releaseCollection(_collection); _collection = nullptr; } }
   virtual int unlock(arangodb::AccessMode::Type, int nestingLevel) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
   virtual int updateUsage(arangodb::AccessMode::Type accessType, int nestingLevel) override { return TRI_ERROR_NO_ERROR; }
   virtual void unuse(int nestingLevel) override { TRI_ASSERT(false); }
   virtual int use(int nestingLevel) override { TRI_vocbase_col_status_e status; _collection = _transaction->vocbase()->useCollection(_cid, status); return _collection ? TRI_ERROR_NO_ERROR : TRI_ERROR_INTERNAL; }
};

class TransactionStateMock: public arangodb::TransactionState {
 public:
  static size_t abortTransactionCount;
  static size_t beginTransactionCount;
  static size_t commitTransactionCount;

  TransactionStateMock(TRI_vocbase_t* vocbase): TransactionState(vocbase) {}
  virtual int abortTransaction(arangodb::transaction::Methods* trx) override { ++abortTransactionCount; updateStatus(arangodb::transaction::Status::ABORTED); unuseCollections(_nestingLevel); return TRI_ERROR_NO_ERROR; }
  virtual int beginTransaction(arangodb::transaction::Hints hints) override { ++beginTransactionCount; useCollections(_nestingLevel); updateStatus(arangodb::transaction::Status::RUNNING); return TRI_ERROR_NO_ERROR; }
  virtual int commitTransaction(arangodb::transaction::Methods* trx) override { ++commitTransactionCount; updateStatus(arangodb::transaction::Status::COMMITTED); unuseCollections(_nestingLevel); return TRI_ERROR_NO_ERROR; }
  virtual bool hasFailedOperations() const override { TRI_ASSERT(false); return false; }
};

size_t TransactionStateMock::abortTransactionCount;
size_t TransactionStateMock::beginTransactionCount;
size_t TransactionStateMock::commitTransactionCount;

class StorageEngineMock: public arangodb::StorageEngine {
 public:
  StorageEngineMock(): StorageEngine(nullptr, "", "", nullptr) {}
  virtual void addAqlFunctions() override { TRI_ASSERT(false); }
  virtual void addDocumentRevision(TRI_voc_tick_t databaseId, TRI_voc_cid_t collectionId, arangodb::velocypack::Slice const& document) override { TRI_ASSERT(false); }
  virtual void addOptimizerRules() override { TRI_ASSERT(false); }
  virtual void addRestHandlers(arangodb::rest::RestHandlerFactory*) override { TRI_ASSERT(false); }
  virtual void addV8Functions() override { TRI_ASSERT(false); }
  virtual void changeCollection(TRI_vocbase_t* vocbase, TRI_voc_cid_t id, arangodb::LogicalCollection const* parameters, bool doSync) override { TRI_ASSERT(false); }
  virtual void changeView(TRI_vocbase_t* vocbase, TRI_voc_cid_t id, arangodb::LogicalView const*, bool doSync) override { TRI_ASSERT(false); }
  virtual bool cleanupCompactionBlockers(TRI_vocbase_t* vocbase) override { TRI_ASSERT(false); return false; }
  virtual std::string collectionPath(TRI_vocbase_t const* vocbase, TRI_voc_cid_t id) const override { TRI_ASSERT(false); return "<invalid>"; }
  virtual std::string createCollection(TRI_vocbase_t* vocbase, TRI_voc_cid_t id, arangodb::LogicalCollection const*) override { TRI_ASSERT(false); return "<invalid>"; }
  virtual Database* createDatabase(TRI_voc_tick_t id, arangodb::velocypack::Slice const& args, int& status) override { TRI_ASSERT(false); return nullptr; }
  virtual void createIndex(TRI_vocbase_t* vocbase, TRI_voc_cid_t collectionId, TRI_idx_iid_t id, arangodb::velocypack::Slice const& data) override { TRI_ASSERT(false); }
  virtual arangodb::PhysicalCollection* createPhysicalCollection(arangodb::LogicalCollection* collection, VPackSlice const& info) override { return new PhysicalCollectionMock(collection, info); }
  virtual arangodb::PhysicalView* createPhysicalView(arangodb::LogicalView* view, VPackSlice const& info) override { return new PhysicalViewMock(view, info); }
  virtual arangodb::TransactionCollection* createTransactionCollection(arangodb::TransactionState* state, TRI_voc_cid_t cid, arangodb::AccessMode::Type, int nestingLevel) override { return new TransactionCollectionMock(state, cid); }
  virtual arangodb::transaction::ContextData* createTransactionContextData() override { TRI_ASSERT(false); return nullptr; }
  virtual arangodb::TransactionState* createTransactionState(TRI_vocbase_t* vocbase) override { return new TransactionStateMock(vocbase); }
  virtual void createView(TRI_vocbase_t* vocbase, TRI_voc_cid_t id, arangodb::LogicalView const*) override { TRI_ASSERT(false); }
  virtual std::string databasePath(TRI_vocbase_t const* vocbase) const override { TRI_ASSERT(false); return nullptr; }
  virtual void destroyCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection* collection) override { TRI_ASSERT(false); }
  virtual void destroyView(TRI_vocbase_t* vocbase, arangodb::LogicalView*) override { TRI_ASSERT(false); }
  virtual arangodb::Result dropCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection* collection) override { TRI_ASSERT(false); return arangodb::Result(TRI_ERROR_INTERNAL); }
  virtual void dropDatabase(Database*, int& status) override { TRI_ASSERT(false); }
  virtual void dropIndex(TRI_vocbase_t* vocbase, TRI_voc_cid_t collectionId, TRI_idx_iid_t id) override { TRI_ASSERT(false); }
  virtual void dropIndexWalMarker(TRI_vocbase_t* vocbase, TRI_voc_cid_t collectionId, arangodb::velocypack::Slice const& data, bool useMarker, int&) override { TRI_ASSERT(false); }
  virtual arangodb::Result dropView(TRI_vocbase_t* vocbase, arangodb::LogicalView*) override { TRI_ASSERT(false); return arangodb::Result(TRI_ERROR_INTERNAL); }
  virtual int extendCompactionBlocker(TRI_vocbase_t* vocbase, TRI_voc_tick_t id, double ttl) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual void getCollectionInfo(TRI_vocbase_t* vocbase, TRI_voc_cid_t cid, arangodb::velocypack::Builder& result, bool includeIndexes, TRI_voc_tick_t maxTick) override { TRI_ASSERT(false); }
  virtual int getCollectionsAndIndexes(TRI_vocbase_t* vocbase, arangodb::velocypack::Builder& result, bool wasCleanShutdown, bool isUpgrade) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual void getDatabases(arangodb::velocypack::Builder& result) override { TRI_ASSERT(false); }
  virtual int getViews(TRI_vocbase_t* vocbase, arangodb::velocypack::Builder& result) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual int insertCompactionBlocker(TRI_vocbase_t* vocbase, double ttl, TRI_voc_tick_t& id) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual void iterateDocuments(TRI_voc_tick_t databaseId, TRI_voc_cid_t collectionId, std::function<void(arangodb::velocypack::Slice const&)> const& cb) override { TRI_ASSERT(false); }
  virtual int openCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection* collection, bool ignoreErrors) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual Database* openDatabase(arangodb::velocypack::Slice const& args, bool isUpgrade, int& status) override { TRI_ASSERT(false); return nullptr; }
  virtual arangodb::Result persistCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection const* collection) override { TRI_ASSERT(false); return arangodb::Result(TRI_ERROR_INTERNAL); }
  virtual arangodb::Result persistView(TRI_vocbase_t* vocbase, arangodb::LogicalView const*) override { TRI_ASSERT(false); return arangodb::Result(TRI_ERROR_INTERNAL); }
  virtual void prepareDropDatabase(TRI_vocbase_t* vocbase, bool useWriteMarker, int& status) override { TRI_ASSERT(false); }
  virtual void preventCompaction(TRI_vocbase_t* vocbase, std::function<void(TRI_vocbase_t*)> const& callback) override { TRI_ASSERT(false); }
  virtual int removeCompactionBlocker(TRI_vocbase_t* vocbase, TRI_voc_tick_t id) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual void removeDocumentRevision(TRI_voc_tick_t databaseId, TRI_voc_cid_t collectionId, arangodb::velocypack::Slice const& document) override { TRI_ASSERT(false); }
  virtual arangodb::Result renameCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection const* collection, std::string const& oldName) override { TRI_ASSERT(false); return arangodb::Result(TRI_ERROR_INTERNAL); }
  virtual int shutdownDatabase(TRI_vocbase_t* vocbase) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
  virtual void signalCleanup(TRI_vocbase_t* vocbase) override { TRI_ASSERT(false); }
  virtual bool tryPreventCompaction(TRI_vocbase_t* vocbase, std::function<void(TRI_vocbase_t*)> const& callback, bool checkForActiveBlockers) override { TRI_ASSERT(false); return false; }
  virtual void unloadCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection* collection) override { TRI_ASSERT(false); }
  virtual void waitForSync(TRI_voc_tick_t tick) override { TRI_ASSERT(false); }
  virtual void waitUntilDeletion(TRI_voc_tick_t id, bool force, int& status) override { TRI_ASSERT(false); }
  virtual int writeCreateMarker(TRI_voc_tick_t id, VPackSlice const& slice) override { TRI_ASSERT(false); return TRI_ERROR_INTERNAL; }
};

NS_END

// -----------------------------------------------------------------------------
// --SECTION--                                                 setup / tear-down
// -----------------------------------------------------------------------------

struct IResearchViewSetup {
  StorageEngineMock engine;
  arangodb::application_features::ApplicationServer server;
  std::string testFilesystemPath;

  IResearchViewSetup(): server(nullptr, nullptr) {
    arangodb::EngineSelectorFeature::ENGINE = &engine;
    arangodb::application_features::ApplicationServer::server->addFeature(
      new arangodb::ViewTypesFeature(arangodb::application_features::ApplicationServer::server)
    );
    arangodb::ViewTypesFeature::registerViewImplementation(
      arangodb::iresearch::IResearchView::type(),
      arangodb::iresearch::IResearchView::make
    );

    PhysicalViewMock::persistPropertiesResult = TRI_ERROR_NO_ERROR;
    TransactionStateMock::abortTransactionCount = 0;
    TransactionStateMock::beginTransactionCount = 0;
    TransactionStateMock::commitTransactionCount = 0;
    testFilesystemPath = (
      irs::utf8_path()/
      TRI_GetTempPath()/
      (std::string("arangodb_tests.") + std::to_string(TRI_microtime()))
    ).utf8();

    long systemError;
    std::string systemErrorStr;
    TRI_CreateDirectory(testFilesystemPath.c_str(), systemError, systemErrorStr);

    // suppress log messages since tests check error conditions
    arangodb::LogTopic::setLogLevel(arangodb::Logger::FIXME.name(), arangodb::LogLevel::FATAL);
    irs::logger::output_le(iresearch::logger::IRL_FATAL, stderr);
  }

  ~IResearchViewSetup() {
    TRI_RemoveDirectory(testFilesystemPath.c_str());
    arangodb::LogTopic::setLogLevel(arangodb::Logger::FIXME.name(), arangodb::LogLevel::DEFAULT);
    arangodb::application_features::ApplicationServer::server = nullptr;
    arangodb::EngineSelectorFeature::ENGINE = nullptr;
  }
};

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief setup
////////////////////////////////////////////////////////////////////////////////

TEST_CASE("IResearchViewTest", "[iresearch][iresearch-view]") {
  IResearchViewSetup s;
  UNUSED(s);

SECTION("test_defaults") {
  auto namedJson = arangodb::velocypack::Parser::fromJson("{ \"name\": \"testView\" }");
  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"name\": \"testView\" \
  }");
  arangodb::iresearch::IResearchViewMeta expectedMeta;

  expectedMeta._name = "testView";

  // existing view definition
  {
    auto view = arangodb::iresearch::IResearchView::make(nullptr, json->slice(), false);
    CHECK(false == (!view));

    arangodb::velocypack::Builder builder;

    builder.openObject();
    view->getPropertiesVPack(builder);
    builder.close();

    auto slice = builder.slice();
    arangodb::iresearch::IResearchViewMeta meta;
    std::string error;

    CHECK((12U == slice.length()));
    CHECK((meta.init(slice, error) && expectedMeta == meta));
  }

  // existing view definition with LogicalView
  {
    arangodb::LogicalView logicalView(nullptr, namedJson->slice());
    auto view = arangodb::iresearch::IResearchView::make(&logicalView, json->slice(), false);
    CHECK(false == (!view));

    arangodb::velocypack::Builder builder;

    builder.openObject();
    view->getPropertiesVPack(builder);
    builder.close();

    auto slice = builder.slice();
    arangodb::iresearch::IResearchViewMeta meta;
    std::string error;

    CHECK((13U == slice.length()));
    CHECK((meta.init(slice, error) && expectedMeta == meta));
  }

  // new view definition
  {
    auto view = arangodb::iresearch::IResearchView::make(nullptr, json->slice(), true);
    CHECK(false == (!view));

    arangodb::velocypack::Builder builder;

    builder.openObject();
    view->getPropertiesVPack(builder);
    builder.close();

    auto slice = builder.slice();
    arangodb::iresearch::IResearchViewMeta meta;
    std::string error;

    CHECK((12U == slice.length()));
    CHECK((meta.init(slice, error) && expectedMeta == meta));
  }

  // new view definition with LogicalView
  {
    arangodb::LogicalView logicalView(nullptr, namedJson->slice());
    auto view = arangodb::iresearch::IResearchView::make(&logicalView, json->slice(), true);
    CHECK(false == (!view));

    arangodb::velocypack::Builder builder;

    builder.openObject();
    view->getPropertiesVPack(builder);
    builder.close();

    auto slice = builder.slice();
    arangodb::iresearch::IResearchViewMeta meta;
    std::string error;

    CHECK((13U == slice.length()));
    CHECK((meta.init(slice, error) && expectedMeta == meta));

    auto tmpSlice = slice.get("links");
    CHECK((true == tmpSlice.isObject() && 0 == tmpSlice.length()));
  }
}

SECTION("test_drop") {
  // FIXME TODO implement
}

SECTION("test_move_datapath") {
  // FIXME TODO implement
}

SECTION("test_open") {
  // FIXME TODO implement
}

SECTION("test_update") {
  auto createJson = arangodb::velocypack::Parser::fromJson("{ \
    \"name\": \"testView\", \
    \"type\": \"iresearch\" \
  }");

  // modify meta params
  {
    TRI_vocbase_t vocbase(TRI_vocbase_type_e::TRI_VOCBASE_TYPE_NORMAL, 1, "testVocbase");
    auto logicalView = vocbase.createView(createJson->slice(), 0);
    CHECK((false == !logicalView));
    auto view = logicalView->getImplementation();
    CHECK((false == !view));

    arangodb::iresearch::IResearchViewMeta expectedMeta;
    auto updateJson = arangodb::velocypack::Parser::fromJson("{ \
      \"locale\": \"en\", \
      \"name\": \"<invalid and ignored>\", \
      \"threadsMaxIdle\": 10, \
      \"threadsMaxTotal\": 20 \
    }");

    expectedMeta._name = "testView";
    expectedMeta._locale = irs::locale_utils::locale("en", true);
    expectedMeta._threadsMaxIdle = 10;
    expectedMeta._threadsMaxTotal = 20;
    CHECK((view->updateProperties(updateJson->slice(), true).ok()));

    arangodb::velocypack::Builder builder;

    builder.openObject();
    view->getPropertiesVPack(builder);
    builder.close();

    auto slice = builder.slice();
    arangodb::iresearch::IResearchViewMeta meta;
    std::string error;

    CHECK((13U == slice.length()));
    CHECK((meta.init(slice, error) && expectedMeta == meta));

    auto tmpSlice = slice.get("links");
    CHECK((true == tmpSlice.isObject() && 0 == tmpSlice.length()));
  }

  // test rollback on meta modification failure
  {
    TRI_vocbase_t vocbase(TRI_vocbase_type_e::TRI_VOCBASE_TYPE_NORMAL, 1, "testVocbase");
    auto logicalView = vocbase.createView(createJson->slice(), 0);
    CHECK((false == !logicalView));
    auto view = logicalView->getImplementation();
    CHECK((false == !view));

    std::string dataPath = (irs::utf8_path()/s.testFilesystemPath/std::string("deleteme")).utf8();
    auto res = TRI_CreateDatafile(dataPath, 1); // create a file where the data path directory should be
    arangodb::iresearch::IResearchViewMeta expectedMeta;
    auto updateJson = arangodb::velocypack::Parser::fromJson(std::string() + "{ \
      \"dataPath\": \"" + arangodb::basics::StringUtils::replace(dataPath, "\\", "/") + "\", \
      \"locale\": \"en\", \
      \"threadsMaxIdle\": 10, \
      \"threadsMaxTotal\": 20 \
    }");

    expectedMeta._name = "testView";
    CHECK((TRI_ERROR_BAD_PARAMETER == view->updateProperties(updateJson->slice(), true).errorNumber()));

    arangodb::velocypack::Builder builder;

    builder.openObject();
    view->getPropertiesVPack(builder);
    builder.close();

    auto slice = builder.slice();
    arangodb::iresearch::IResearchViewMeta meta;
    std::string error;

    CHECK((13U == slice.length()));
    CHECK((meta.init(slice, error) && expectedMeta == meta));

    auto tmpSlice = slice.get("links");
    CHECK((true == tmpSlice.isObject() && 0 == tmpSlice.length()));
  }

  // test rollback on persist failure
  {
    TRI_vocbase_t vocbase(TRI_vocbase_type_e::TRI_VOCBASE_TYPE_NORMAL, 1, "testVocbase");
    auto logicalView = vocbase.createView(createJson->slice(), 0);
    CHECK((false == !logicalView));
    auto view = logicalView->getImplementation();
    CHECK((false == !view));

    arangodb::iresearch::IResearchViewMeta expectedMeta;
    auto updateJson = arangodb::velocypack::Parser::fromJson("{ \
      \"locale\": \"en\", \
      \"threadsMaxIdle\": 10, \
      \"threadsMaxTotal\": 20 \
    }");

    expectedMeta._name = "testView";
    PhysicalViewMock::persistPropertiesResult = TRI_ERROR_INTERNAL; // test fail
    CHECK((TRI_ERROR_INTERNAL == view->updateProperties(updateJson->slice(), true).errorNumber()));
    PhysicalViewMock::persistPropertiesResult = TRI_ERROR_NO_ERROR; // revert to valid

    arangodb::velocypack::Builder builder;

    builder.openObject();
    view->getPropertiesVPack(builder);
    builder.close();

    auto slice = builder.slice();
    arangodb::iresearch::IResearchViewMeta meta;
    std::string error;

    CHECK((13U == slice.length()));
    CHECK((meta.init(slice, error) && expectedMeta == meta));

    auto tmpSlice = slice.get("links");
    CHECK((true == tmpSlice.isObject() && 0 == tmpSlice.length()));
  }

  // add a new link
  {
    TRI_vocbase_t vocbase(TRI_vocbase_type_e::TRI_VOCBASE_TYPE_NORMAL, 1, "testVocbase");
    auto collectionJson = arangodb::velocypack::Parser::fromJson("{ \"name\": \"testCollection\" }");
    auto* logicalCollection = vocbase.createCollection(collectionJson->slice(), 0);
    CHECK((nullptr != logicalCollection));
    auto logicalView = vocbase.createView(createJson->slice(), 0);
    CHECK((false == !logicalView));
    auto view = logicalView->getImplementation();
    CHECK((false == !view));

    arangodb::iresearch::IResearchViewMeta expectedMeta;
    std::unordered_map<std::string, arangodb::iresearch::IResearchLinkMeta> expectedLinkMeta;
    auto updateJson = arangodb::velocypack::Parser::fromJson("{ \
      \"links\": { \
        \"testCollection\": { \
        } \
      }}");

    expectedMeta._collections.insert(logicalCollection->cid());
    expectedMeta._name = "testView";
    expectedLinkMeta["testCollection"]; // use defaults
    CHECK((view->updateProperties(updateJson->slice(), true).ok()));

    arangodb::velocypack::Builder builder;

    builder.openObject();
    view->getPropertiesVPack(builder);
    builder.close();

    auto slice = builder.slice();
    arangodb::iresearch::IResearchViewMeta meta;
    std::string error;

    CHECK((13U == slice.length()));
    CHECK((meta.init(slice, error) && expectedMeta == meta));

    auto tmpSlice = slice.get("links");
    CHECK((true == tmpSlice.isObject() && 1 == tmpSlice.length()));

    for (arangodb::velocypack::ObjectIterator itr(tmpSlice); itr.valid(); ++itr) {
      arangodb::iresearch::IResearchLinkMeta linkMeta;
      auto key = itr.key();
      auto value = itr.value();
      CHECK((true == key.isString()));

      auto expectedItr = expectedLinkMeta.find(key.copyString());
      CHECK((
        true == value.isObject()
        && expectedItr != expectedLinkMeta.end()
        && linkMeta.init(value, error)
        && expectedItr->second == linkMeta
      ));
      expectedLinkMeta.erase(expectedItr);
    }

    CHECK((true == expectedLinkMeta.empty()));
  }

  // add new link to non-existant collection
  {
    TRI_vocbase_t vocbase(TRI_vocbase_type_e::TRI_VOCBASE_TYPE_NORMAL, 1, "testVocbase");
    auto logicalView = vocbase.createView(createJson->slice(), 0);
    CHECK((false == !logicalView));
    auto view = logicalView->getImplementation();
    CHECK((false == !view));

    arangodb::iresearch::IResearchViewMeta expectedMeta;
    auto updateJson = arangodb::velocypack::Parser::fromJson("{ \
      \"links\": { \
        \"testCollection\": {} \
      }}");

    expectedMeta._name = "testView";
    CHECK((TRI_ERROR_BAD_PARAMETER == view->updateProperties(updateJson->slice(), true).errorNumber()));

    arangodb::velocypack::Builder builder;

    builder.openObject();
    view->getPropertiesVPack(builder);
    builder.close();

    auto slice = builder.slice();
    arangodb::iresearch::IResearchViewMeta meta;
    std::string error;

    CHECK((13U == slice.length()));
    CHECK((meta.init(slice, error) && expectedMeta == meta));

    auto tmpSlice = slice.get("links");
    CHECK((true == tmpSlice.isObject() && 0 == tmpSlice.length()));
  }

  // remove link
  {
    TRI_vocbase_t vocbase(TRI_vocbase_type_e::TRI_VOCBASE_TYPE_NORMAL, 1, "testVocbase");
    auto collectionJson = arangodb::velocypack::Parser::fromJson("{ \"name\": \"testCollection\" }");
    auto* logicalCollection = vocbase.createCollection(collectionJson->slice(), 0);
    CHECK((nullptr != logicalCollection));
    auto logicalView = vocbase.createView(createJson->slice(), 0);
    CHECK((false == !logicalView));
    auto view = logicalView->getImplementation();
    CHECK((false == !view));

    arangodb::iresearch::IResearchViewMeta expectedMeta;

    expectedMeta._collections.insert(logicalCollection->cid());
    expectedMeta._name = "testView";

    {
      auto updateJson = arangodb::velocypack::Parser::fromJson("{ \
        \"links\": { \
          \"testCollection\": {} \
      }}");

      CHECK((view->updateProperties(updateJson->slice(), true).ok()));

      arangodb::velocypack::Builder builder;

      builder.openObject();
      view->getPropertiesVPack(builder);
      builder.close();

      auto slice = builder.slice();
      arangodb::iresearch::IResearchViewMeta meta;
      std::string error;

      CHECK((13U == slice.length()));
      CHECK((meta.init(slice, error) && expectedMeta == meta));

      auto tmpSlice = slice.get("links");
      CHECK((true == tmpSlice.isObject() && 1 == tmpSlice.length()));
    }

    {
      auto updateJson = arangodb::velocypack::Parser::fromJson("{ \
        \"links\": { \
          \"testCollection\": null \
      }}");

      CHECK((view->updateProperties(updateJson->slice(), true).ok()));

      arangodb::velocypack::Builder builder;

      builder.openObject();
      view->getPropertiesVPack(builder);
      builder.close();

      auto slice = builder.slice();
      arangodb::iresearch::IResearchViewMeta meta;
      std::string error;

      CHECK((13U == slice.length()));
      CHECK((meta.init(slice, error) && expectedMeta == meta));

      auto tmpSlice = slice.get("links");
      CHECK((true == tmpSlice.isObject() && 0 == tmpSlice.length()));
    }
  }

  // remove link from non-existant collection
  {
    TRI_vocbase_t vocbase(TRI_vocbase_type_e::TRI_VOCBASE_TYPE_NORMAL, 1, "testVocbase");
    auto logicalView = vocbase.createView(createJson->slice(), 0);
    CHECK((false == !logicalView));
    auto view = logicalView->getImplementation();
    CHECK((false == !view));

    arangodb::iresearch::IResearchViewMeta expectedMeta;
    auto updateJson = arangodb::velocypack::Parser::fromJson("{ \
      \"links\": { \
        \"testCollection\": null \
      }}");

    expectedMeta._name = "testView";
    CHECK((TRI_ERROR_BAD_PARAMETER == view->updateProperties(updateJson->slice(), true).errorNumber()));

    arangodb::velocypack::Builder builder;

    builder.openObject();
    view->getPropertiesVPack(builder);
    builder.close();

    auto slice = builder.slice();
    arangodb::iresearch::IResearchViewMeta meta;
    std::string error;

    CHECK((13U == slice.length()));
    CHECK((meta.init(slice, error) && expectedMeta == meta));

    auto tmpSlice = slice.get("links");
    CHECK((true == tmpSlice.isObject() && 0 == tmpSlice.length()));
  }

  // remove non-existant link
  {
    TRI_vocbase_t vocbase(TRI_vocbase_type_e::TRI_VOCBASE_TYPE_NORMAL, 1, "testVocbase");
    auto collectionJson = arangodb::velocypack::Parser::fromJson("{ \"name\": \"testCollection\" }");
    auto* logicalCollection = vocbase.createCollection(collectionJson->slice(), 0);
    CHECK((nullptr != logicalCollection));
    auto logicalView = vocbase.createView(createJson->slice(), 0);
    CHECK((false == !logicalView));
    auto view = logicalView->getImplementation();
    CHECK((false == !view));

    auto updateJson = arangodb::velocypack::Parser::fromJson("{ \
      \"links\": { \
        \"testCollection\": null \
    }}");
    arangodb::iresearch::IResearchViewMeta expectedMeta;

    expectedMeta._name = "testView";

    CHECK((view->updateProperties(updateJson->slice(), true).ok()));

    arangodb::velocypack::Builder builder;

    builder.openObject();
    view->getPropertiesVPack(builder);
    builder.close();

    auto slice = builder.slice();
    arangodb::iresearch::IResearchViewMeta meta;
    std::string error;

    CHECK((13U == slice.length()));
    CHECK((meta.init(slice, error) && expectedMeta == meta));

    auto tmpSlice = slice.get("links");
    CHECK((true == tmpSlice.isObject() && 0 == tmpSlice.length()));
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief generate tests
////////////////////////////////////////////////////////////////////////////////

}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------