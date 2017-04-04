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

#include "IResearch/IResearchView.h"
#include "StorageEngine/EngineSelectorFeature.h"
#include "StorageEngine/StorageEngine.h"
#include "StorageEngine/TransactionState.h"
#include "velocypack/Iterator.h"
#include "velocypack/Parser.h"
#include "VocBase/LogicalView.h"
#include "VocBase/PhysicalView.h"

NS_LOCAL

class PhysicalViewMock: public arangodb::PhysicalView {
 public:
  static int persistPropertiesResult;

  PhysicalViewMock(arangodb::LogicalView* view, VPackSlice const& info): PhysicalView(view, info) {}
  virtual std::string const& path() const override { TRI_ASSERT(false); static std::string invalid("<invalid>"); return invalid; }
  virtual void setPath(std::string const&) override { TRI_ASSERT(false); }
  virtual arangodb::Result updateProperties(arangodb::velocypack::Slice const& slice, bool doSync) override { TRI_ASSERT(false); return arangodb::Result(TRI_ERROR_INTERNAL); }
  virtual arangodb::Result persistProperties() override { return arangodb::Result(persistPropertiesResult); }
  virtual PhysicalView* clone(arangodb::LogicalView*, arangodb::PhysicalView*) override { TRI_ASSERT(false); return nullptr; }
  virtual void getPropertiesVPack(arangodb::velocypack::Builder&, bool includeSystem = false) const override { TRI_ASSERT(false); }
  virtual void open() override { TRI_ASSERT(false); }
  virtual void drop() override { TRI_ASSERT(false); }
};

int PhysicalViewMock::persistPropertiesResult;

class TransactionStateMock: public arangodb::TransactionState {
 public:
  TransactionStateMock(TRI_vocbase_t* vocbase): TransactionState(vocbase) {}
  virtual int abortTransaction(arangodb::transaction::Methods* trx) override { return TRI_ERROR_NO_ERROR; }
  virtual int beginTransaction(arangodb::transaction::Hints hints) override { return TRI_ERROR_NO_ERROR; }
  virtual int commitTransaction(arangodb::transaction::Methods* trx) override { return TRI_ERROR_NO_ERROR; }
  virtual bool hasFailedOperations() const override { TRI_ASSERT(false); return false; }
};

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
  virtual arangodb::PhysicalCollection* createPhysicalCollection(arangodb::LogicalCollection*, VPackSlice const&) override { TRI_ASSERT(false); return nullptr; }
  virtual arangodb::PhysicalView* createPhysicalView(arangodb::LogicalView* view, VPackSlice const& info) override { return new PhysicalViewMock(view, info); }
  virtual arangodb::TransactionCollection* createTransactionCollection(arangodb::TransactionState*, TRI_voc_cid_t, arangodb::AccessMode::Type, int nestingLevel) override { TRI_ASSERT(false); return nullptr; }
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

  IResearchViewSetup() {
    arangodb::EngineSelectorFeature::ENGINE = &engine;
    PhysicalViewMock::persistPropertiesResult = TRI_ERROR_NO_ERROR;
  }

  ~IResearchViewSetup() {
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
  auto namedJson = arangodb::velocypack::Parser::fromJson("{ \"name\": \"testView\" }");
  auto createJson = arangodb::velocypack::Parser::fromJson("{ \
    \"name\": \"testView\" \
  }");
  arangodb::LogicalView logicalView(nullptr, namedJson->slice());
  auto view = arangodb::iresearch::IResearchView::make(&logicalView, createJson->slice(), false);
  CHECK(false == (!view));

  // modify meta params
  {
    arangodb::iresearch::IResearchViewMeta expectedMeta;
    auto updateJson = arangodb::velocypack::Parser::fromJson("{ \
      \"locale\": \"en\", \
      \"name\": \"testView\", \
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
    // FIXME TODO implement
  }

  // test rollback on persist failure
  {
    // FIXME TODO implement
  }

  // add a new link
  {
    // FIXME TODO implement
  }

  // add another link and remove first link
  {
    // FIXME TODO implement
  }

  // test rollback on link modification failure
  {
    // FIXME TODO implement
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief generate tests
////////////////////////////////////////////////////////////////////////////////

}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------