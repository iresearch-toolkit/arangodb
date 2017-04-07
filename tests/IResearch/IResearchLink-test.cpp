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
#include "StorageEngineMock.h"

#include "IResearch/IResearchLink.h"
#include "Logger/Logger.h"
#include "Logger/LogTopic.h"
#include "RestServer/ViewTypesFeature.h"
#include "StorageEngine/EngineSelectorFeature.h"
#include "velocypack/Parser.h"
#include "VocBase/KeyGenerator.h"
#include "VocBase/LogicalCollection.h"

// -----------------------------------------------------------------------------
// --SECTION--                                                 setup / tear-down
// -----------------------------------------------------------------------------

struct IResearchLinkSetup {
  StorageEngineMock engine;
  arangodb::application_features::ApplicationServer server;

  IResearchLinkSetup(): server(nullptr, nullptr) {
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

    // suppress log messages since tests check error conditions
    arangodb::LogTopic::setLogLevel(arangodb::Logger::FIXME.name(), arangodb::LogLevel::FATAL);
  }

  ~IResearchLinkSetup() {
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

TEST_CASE("IResearchLinkTest", "[iresearch][iresearch-link]") {
  IResearchLinkSetup s;
  UNUSED(s);

SECTION("test_defaults") {
  // no view specified
  {
    auto json = arangodb::velocypack::Parser::fromJson("{}");
    auto link = arangodb::iresearch::IResearchLink::make(1, nullptr, json->slice());
    CHECK((true == !link));
  }

  // no view can be found
  {
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"testView\" }");
    auto link = arangodb::iresearch::IResearchLink::make(1, nullptr, json->slice());
    CHECK((true == !link));
  }

  // valid link creation
  {
    TRI_vocbase_t vocbase(TRI_vocbase_type_e::TRI_VOCBASE_TYPE_NORMAL, 1, "testVocbase");
    auto linkJson = arangodb::velocypack::Parser::fromJson("{ \"name\": \"testView\" }");
    auto collectionJson = arangodb::velocypack::Parser::fromJson("{ \"name\": \"testCollection\" }");
    auto viewJson = arangodb::velocypack::Parser::fromJson("{ \"name\": \"testView\", \"type\": \"iresearch\" }");
    auto* logicalCollection = vocbase.createCollection(collectionJson->slice(), 0);
    CHECK((nullptr != logicalCollection));
    auto logicalView = vocbase.createView(viewJson->slice(), 0);
    CHECK((false == !logicalView));

    bool created;
    auto link = logicalCollection->createIndex(nullptr, linkJson->slice(), created);
    CHECK((false == !link && created));
    CHECK((true == link->allowExpansion()));
    CHECK((true == link->canBeDropped()));
    CHECK((logicalCollection == link->collection()));
    CHECK((link->fieldNames().empty()));
    CHECK((link->fields().empty()));
    CHECK((true == link->hasBatchInsert()));
    CHECK((false == link->hasExpansion()));
    CHECK((false == link->hasSelectivityEstimate()));
    CHECK((false == link->implicitlyUnique()));
    CHECK((true == link->isPersistent()));
    CHECK((false == link->isSorted()));
    CHECK((0 < link->memory()));
    CHECK((true == link->sparse()));
    CHECK((arangodb::Index::IndexType::TRI_IDX_TYPE_IRESEARCH_LINK == link->type()));
    CHECK((std::string("iresearch") == link->typeName()));
    CHECK((false == link->unique()));

    arangodb::iresearch::IResearchLinkMeta actualMeta;
    arangodb::iresearch::IResearchLinkMeta expectedMeta;
    auto builder = link->toVelocyPack(true);
    std::string error;

    CHECK((actualMeta.init(builder->slice(), error) && expectedMeta == actualMeta));
    auto slice = builder->slice();
    CHECK((
      slice.hasKey("name")
      && slice.get("name").isString()
      && std::string("testView") == slice.get("name").copyString()
      && slice.hasKey("figures")
      && slice.get("figures").isObject()
      && slice.get("figures").hasKey("memory")
      && slice.get("figures").get("memory").isNumber()
      && 0 < slice.get("figures").get("memory").getUInt()
    ));
    CHECK((logicalCollection->dropIndex(link->id()) && logicalCollection->getIndexes().empty()));
  }
}

SECTION("test_write") {
  // TODO FIXME check insert works
  // TODO FIXME check remove works
}

////////////////////////////////////////////////////////////////////////////////
/// @brief generate tests
////////////////////////////////////////////////////////////////////////////////

}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------