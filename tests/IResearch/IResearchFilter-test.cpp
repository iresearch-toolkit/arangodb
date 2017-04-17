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

#include "IResearch/IResearchDocument.h"
#include "IResearch/IResearchLinkMeta.h"
#include "IResearch/IResearchViewMeta.h"

#include "StorageEngine/EngineSelectorFeature.h"
#include "RestServer/AqlFeature.h"
#include "Aql/Ast.h"
#include "Aql/Query.h"

#include "analysis/analyzers.hpp"
#include "analysis/token_streams.hpp"
#include "analysis/token_attributes.hpp"
#include "search/term_filter.hpp"
#include "search/boolean_filter.hpp"

namespace {

std::string mangleName(irs::string_ref const& name, irs::string_ref const& suffix) {
  std::string mangledName(name.c_str(), name.size());
  mangledName += '\0';
  mangledName.append(suffix.c_str(), suffix.size());
  return mangledName;
}

std::string mangleBool(irs::string_ref const& name) { return mangleName(name, "_b"); }
std::string mangleNull(irs::string_ref const& name) { return mangleName(name, "_n"); }
std::string mangleNumeric(irs::string_ref const& name) { return mangleName(name, "_d"); }

void assertFilterSuccess(std::string const& queryString, irs::filter const& expected) {
  TRI_vocbase_t vocbase(TRI_vocbase_type_e::TRI_VOCBASE_TYPE_NORMAL, 1, "testVocbase");

  arangodb::aql::Query query(
     true, &vocbase, queryString.c_str(),
     queryString.size(), nullptr, nullptr,
     arangodb::aql::PART_MAIN
  );

  auto const parseResult = query.parse();
  REQUIRE(TRI_ERROR_NO_ERROR == parseResult.code);

  auto* root = query.ast()->root();
  REQUIRE(root);
  auto* filterNode = root->getMember(1);
  REQUIRE(filterNode);

  irs::Or actual;
  CHECK(arangodb::iresearch::FilterFactory::filter(actual, *filterNode));
  CHECK(expected == actual);
}

void assertFilterFail(std::string const& queryString) {
  TRI_vocbase_t vocbase(TRI_vocbase_type_e::TRI_VOCBASE_TYPE_NORMAL, 1, "testVocbase");

  arangodb::aql::Query query(
     true, &vocbase, queryString.c_str(),
     queryString.size(), nullptr, nullptr,
     arangodb::aql::PART_MAIN
  );

  auto const parseResult = query.parse();
  REQUIRE(TRI_ERROR_NO_ERROR == parseResult.code);

  auto* root = query.ast()->root();
  REQUIRE(root);
  auto* filterNode = root->getMember(1);
  REQUIRE(filterNode);

  irs::Or actual;
  CHECK(!arangodb::iresearch::FilterFactory::filter(actual, *filterNode));
}

}

// -----------------------------------------------------------------------------
// --SECTION--                                                 setup / tear-down
// -----------------------------------------------------------------------------

struct IResearchFilterSetup {
  StorageEngineMock engine;
  arangodb::application_features::ApplicationServer server;

  IResearchFilterSetup(): server(nullptr, nullptr) {
    arangodb::EngineSelectorFeature::ENGINE = &engine;
    arangodb::AqlFeature* aqlFeature;
    arangodb::application_features::ApplicationServer::server->addFeature(
      aqlFeature = new arangodb::AqlFeature(&server)
    );
    aqlFeature->start();
  }

  ~IResearchFilterSetup() {
    arangodb::application_features::ApplicationServer::server = nullptr;
    arangodb::EngineSelectorFeature::ENGINE = nullptr;
  }
}; // IResearchFilterSetup

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief setup
////////////////////////////////////////////////////////////////////////////////

TEST_CASE("IResearchFilterTest", "[iresearch][iresearch-filter]") {
  IResearchFilterSetup s;
  UNUSED(s);

SECTION("BinaryIn") {
  // simple attribute
  {
    std::string const queryString = "FOR d IN collection FILTER d.a in ['1','2','3'] RETURN d";

    irs::Or expected;
    auto& root = expected.add<irs::Or>();
    root.add<irs::by_term>().field("a").term("1");
    root.add<irs::by_term>().field("a").term("2");
    root.add<irs::by_term>().field("a").term("3");

    assertFilterSuccess(queryString, expected);
  }

  // complex attribute name
  {
    std::string const queryString = "FOR d IN collection FILTER d.a.b.c.e.f in ['1','2','3'] RETURN d";

    irs::Or expected;
    auto& root = expected.add<irs::Or>();
    root.add<irs::by_term>().field("a.b.c.e.f").term("1");
    root.add<irs::by_term>().field("a.b.c.e.f").term("2");
    root.add<irs::by_term>().field("a.b.c.e.f").term("3");

    assertFilterSuccess(queryString, expected);
  }

  // heterogeneous array values
  {
    std::string const queryString = "FOR d IN collection FILTER d.quick.brown.fox in ['1',null,true,false,2] RETURN d";

    irs::Or expected;
    auto& root = expected.add<irs::Or>();
    root.add<irs::by_term>().field("quick.brown.fox").term("1");
    root.add<irs::by_term>().field(mangleNull("quick.brown.fox")).term(irs::null_token_stream::value_null());
    root.add<irs::by_term>().field(mangleBool("quick.brown.fox")).term(irs::boolean_token_stream::value_true());
    root.add<irs::by_term>().field(mangleBool("quick.brown.fox")).term(irs::boolean_token_stream::value_false());
    {
      irs::numeric_token_stream stream;
      auto& term = stream.attributes().get<irs::term_attribute>();
      stream.reset(2.);
      CHECK(stream.next());
      root.add<irs::by_term>().field(mangleNumeric("quick.brown.fox")).term(term->value());
    }

    assertFilterSuccess(queryString, expected);
  }

  // not a value in array
  {
    std::string const queryString = "FOR d IN collection FILTER d.a in ['1',['2'],'3'] RETURN d";
    assertFilterFail(queryString);
  }

  // not a constant in array
  {
    std::string const queryString = "FOR d IN collection FILTER d.a in ['1', d, '3'] RETURN d";
    assertFilterFail(queryString);
  }
}

SECTION("BinaryNotIn") {
  // simple attribute
  {
    std::string const queryString = "FOR d IN collection FILTER d.a not in ['1','2','3'] RETURN d";

    irs::Or expected;
    auto& root = expected.add<irs::Not>().filter<irs::And>();
    root.add<irs::by_term>().field("a").term("1");
    root.add<irs::by_term>().field("a").term("2");
    root.add<irs::by_term>().field("a").term("3");

    assertFilterSuccess(queryString, expected);
  }

  // complex attribute name
  {
    std::string const queryString = "FOR d IN collection FILTER d.a.b.c.e.f not in ['1','2','3'] RETURN d";

    irs::Or expected;
    auto& root = expected.add<irs::Not>().filter<irs::And>();
    root.add<irs::by_term>().field("a.b.c.e.f").term("1");
    root.add<irs::by_term>().field("a.b.c.e.f").term("2");
    root.add<irs::by_term>().field("a.b.c.e.f").term("3");

    assertFilterSuccess(queryString, expected);
  }

  // heterogeneous array values
  {
    std::string const queryString = "FOR d IN collection FILTER d.quick.brown.fox not in ['1',null,true,false,2] RETURN d";

    irs::Or expected;
    auto& root = expected.add<irs::Not>().filter<irs::And>();
    root.add<irs::by_term>().field("quick.brown.fox").term("1");
    root.add<irs::by_term>().field(mangleNull("quick.brown.fox")).term(irs::null_token_stream::value_null());
    root.add<irs::by_term>().field(mangleBool("quick.brown.fox")).term(irs::boolean_token_stream::value_true());
    root.add<irs::by_term>().field(mangleBool("quick.brown.fox")).term(irs::boolean_token_stream::value_false());
    {
      irs::numeric_token_stream stream;
      auto& term = stream.attributes().get<irs::term_attribute>();
      stream.reset(2.);
      CHECK(stream.next());
      root.add<irs::by_term>().field(mangleNumeric("quick.brown.fox")).term(term->value());
    }

    assertFilterSuccess(queryString, expected);
  }

  // not a value in array
  {
    std::string const queryString = "FOR d IN collection FILTER d.a not in ['1',['2'],'3'] RETURN d";
    assertFilterFail(queryString);
  }

  // not a constant in array
  {
    std::string const queryString = "FOR d IN collection FILTER d.a not in ['1', d, '3'] RETURN d";
    assertFilterFail(queryString);
  }
}

SECTION("BinaryEq") {
  // simple string attribute
  {
    std::string const queryString = "FOR d IN collection FILTER d.a == '1' RETURN d";

    irs::Or expected;
    expected.add<irs::by_term>().field("a").term("1");

    assertFilterSuccess(queryString, expected);
  }

  // complex attribute name, string
  {
    std::string const queryString = "FOR d IN collection FILTER d.a.b.c == '1' RETURN d";

    irs::Or expected;
    expected.add<irs::by_term>().field("a.b.c").term("1");

    assertFilterSuccess(queryString, expected);
  }

  // complex boolean attribute, true
  {
    std::string const queryString = "FOR d IN collection FILTER d.a.b.c == true RETURN d";

    irs::Or expected;
    expected.add<irs::by_term>().field(mangleBool("a.b.c")).term(irs::boolean_token_stream::value_true());

    assertFilterSuccess(queryString, expected);
  }

  // complex boolean attribute, false
  {
    std::string const queryString = "FOR d IN collection FILTER d.a.b.c.bool == false RETURN d";

    irs::Or expected;
    expected.add<irs::by_term>().field(mangleBool("a.b.c.bool")).term(irs::boolean_token_stream::value_false());

    assertFilterSuccess(queryString, expected);
  }

  // complex boolean attribute, null
  {
    std::string const queryString = "FOR d IN collection FILTER d.a.b.c.bool == null RETURN d";

    irs::Or expected;
    expected.add<irs::by_term>().field(mangleNull("a.b.c.bool")).term(irs::null_token_stream::value_null());

    assertFilterSuccess(queryString, expected);
  }

  // complex boolean attribute, numeric
  {
    std::string const queryString = "FOR d IN collection FILTER d.a.b.c.numeric == 3 RETURN d";

    irs::numeric_token_stream stream;
    stream.reset(3.);
    CHECK(stream.next());
    auto& term = stream.attributes().get<irs::term_attribute>();

    irs::Or expected;
    expected.add<irs::by_term>().field(mangleNumeric("a.b.c.numeric")).term(term->value());

    assertFilterSuccess(queryString, expected);
  }
}

SECTION("Visit_Ast") {
  std::string const queryString = "FOR d IN collection FILTER d.b.c.a == 1 RETURN d";

  TRI_vocbase_t vocbase(TRI_vocbase_type_e::TRI_VOCBASE_TYPE_NORMAL, 1, "testVocbase");

  arangodb::aql::Query query(
     true, &vocbase, queryString.c_str(),
     queryString.size(), nullptr, nullptr,
     arangodb::aql::PART_MAIN
  );

  auto const parseResult = query.parse();
  REQUIRE(TRI_ERROR_NO_ERROR == parseResult.code);

  auto* root = query.ast()->root();
  REQUIRE(root);
  auto* filterNode = root->getMember(1);
  REQUIRE(filterNode);


  arangodb::iresearch::visit<false>(*filterNode, [](arangodb::aql::AstNode const& node) {
    if (node.type == arangodb::aql::NODE_TYPE_REFERENCE) {
      return true;
    }

    try {
      std::cout << node.toString() << std::endl;
    } catch (...) { }
    return true;
  });

  std::cout << std::endl;

  arangodb::iresearch::visit<true>(*filterNode, [](arangodb::aql::AstNode const& node) {
    if (node.type == arangodb::aql::NODE_TYPE_REFERENCE) {
      return true;
    }

    try {
      std::cout << node.toString() << std::endl;
    } catch (...) { }
    return true;
  });
}

////////////////////////////////////////////////////////////////////////////////
/// @brief generate tests
////////////////////////////////////////////////////////////////////////////////

}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
