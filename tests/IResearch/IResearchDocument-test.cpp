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

#include "IResearch/IResearchDocument.h"
#include "IResearch/IResearchLinkMeta.h"
#include "IResearch/IResearchViewMeta.h"

#include "velocypack/Iterator.h"
#include "velocypack/Builder.h"
#include "velocypack/Parser.h"
#include "velocypack/velocypack-aliases.h"

// -----------------------------------------------------------------------------
// --SECTION--                                                 setup / tear-down
// -----------------------------------------------------------------------------

struct IResearchDocumentSetup { };

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief setup
////////////////////////////////////////////////////////////////////////////////

TEST_CASE("IResearchDocumentTest", "[iresearch][iresearch-document]") {
  IResearchDocumentSetup s;
  UNUSED(s);

SECTION("static_checks") {
  static_assert(
    std::is_same<
      std::forward_iterator_tag,
      arangodb::iresearch::FieldIterator::iterator_category
    >::value,
    "Invalid iterator category"
  );

  static_assert(
    std::is_same<
      arangodb::iresearch::Field const,
      arangodb::iresearch::FieldIterator::value_type
    >::value,
    "Invalid iterator value type"
  );

  static_assert(
    std::is_same<
      arangodb::iresearch::Field const&,
      arangodb::iresearch::FieldIterator::reference
    >::value,
    "Invalid iterator reference type"
  );

  static_assert(
    std::is_same<
      arangodb::iresearch::Field const*,
      arangodb::iresearch::FieldIterator::pointer
    >::value,
    "Invalid iterator pointer type"
  );

  static_assert(
    std::is_same<
      std::ptrdiff_t,
      arangodb::iresearch::FieldIterator::difference_type
    >::value,
    "Invalid iterator difference type"
  );
}

SECTION("default_ctor") {
  arangodb::iresearch::FieldIterator it;
  CHECK(!it.valid());
  CHECK(arangodb::iresearch::FieldIterator() == it);
}

SECTION("traverse_complex_object_custom_nested_delimiter") {
  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"nested\": { \"foo\": \"str\" }, \
    \"keys\": [ \"1\",\"2\",\"3\",\"4\" ], \
    \"tokenizers\": {}, \
    \"boost\": \"10\", \
    \"depth\": \"20\", \
    \"fields\": { \"fieldA\" : { \"name\" : \"a\" }, \"fieldB\" : { \"name\" : \"b\" } }, \
    \"listValuation\": \"ignored\", \
    \"locale\": \"ru_RU.KOI8-R\", \
    \"array\" : [ \
      { \"id\" : \"1\", \"subarr\" : [ \"1\", \"2\", \"3\" ], \"subobj\" : { \"id\" : \"1\" } }, \
      { \"subarr\" : [ \"4\", \"5\", \"6\" ], \"subobj\" : { \"name\" : \"foo\" }, \"id\" : \"2\" }, \
      { \"id\" : \"3\", \"subarr\" : [ \"7\", \"8\", \"9\" ], \"subobj\" : { \"id\" : \"2\" } } \
    ] \
  }");

std::unordered_map<std::string, size_t> expectedValues {
    { "nested---foo", 1 },
    { "keys", 4 },
    { "boost", 1 },
    { "depth", 1 },
    { "fields---fieldA---name", 1 },
    { "fields---fieldB---name", 1 },
    { "listValuation", 1 },
    { "locale", 1 },
    { "array---id", 3 },
    { "array---subarr", 9 },
    { "array---subobj---id", 2 },
    { "array---subobj---name", 1 },
    { "array---id", 2 }
  };

  auto const slice = json->slice();

  arangodb::iresearch::IResearchViewMeta viewMeta;
  viewMeta._nestingDelimiter = "---";
  arangodb::iresearch::IResearchLinkMeta linkMeta;
  linkMeta._includeAllFields = true; // include all fields

  arangodb::iresearch::FieldIterator it(slice, linkMeta, viewMeta);
  CHECK(it != arangodb::iresearch::FieldIterator());

  while (it.valid()) {
    std::string const actualName = std::string((*it).name());
    auto const expectedValue = expectedValues.find(actualName);
    REQUIRE(expectedValues.end() != expectedValue);

    auto& refs = expectedValue->second;
    if (!--refs) {
      expectedValues.erase(expectedValue);
    }

    auto const prev = it;
    CHECK(prev == it++);
  }

  CHECK(expectedValues.empty());
  CHECK(it == arangodb::iresearch::FieldIterator());
}

SECTION("traverse_complex_object_all_fields") {
  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"nested\": { \"foo\": \"str\" }, \
    \"keys\": [ \"1\",\"2\",\"3\",\"4\" ], \
    \"tokenizers\": {}, \
    \"boost\": \"10\", \
    \"depth\": \"20\", \
    \"fields\": { \"fieldA\" : { \"name\" : \"a\" }, \"fieldB\" : { \"name\" : \"b\" } }, \
    \"listValuation\": \"ignored\", \
    \"locale\": \"ru_RU.KOI8-R\", \
    \"array\" : [ \
      { \"id\" : \"1\", \"subarr\" : [ \"1\", \"2\", \"3\" ], \"subobj\" : { \"id\" : \"1\" } }, \
      { \"subarr\" : [ \"4\", \"5\", \"6\" ], \"subobj\" : { \"name\" : \"foo\" }, \"id\" : \"2\" }, \
      { \"id\" : \"3\", \"subarr\" : [ \"7\", \"8\", \"9\" ], \"subobj\" : { \"id\" : \"2\" } } \
    ] \
  }");

std::unordered_map<std::string, size_t> expectedValues {
    { "nested.foo", 1 },
    { "keys", 4 },
    { "boost", 1 },
    { "depth", 1 },
    { "fields.fieldA.name", 1 },
    { "fields.fieldB.name", 1 },
    { "listValuation", 1 },
    { "locale", 1 },
    { "array.id", 3 },
    { "array.subarr", 9 },
    { "array.subobj.id", 2 },
    { "array.subobj.name", 1 },
    { "array.id", 2 }
  };

  auto const slice = json->slice();

  arangodb::iresearch::IResearchViewMeta viewMeta;
  arangodb::iresearch::IResearchLinkMeta linkMeta;
  linkMeta._includeAllFields = true;

  arangodb::iresearch::FieldIterator it(slice, linkMeta, viewMeta);
  CHECK(it != arangodb::iresearch::FieldIterator());

  while (it.valid()) {
    std::string const actualName = std::string((*it).name());
    auto const expectedValue = expectedValues.find(actualName);
    REQUIRE(expectedValues.end() != expectedValue);

    auto& refs = expectedValue->second;
    if (!--refs) {
      expectedValues.erase(expectedValue);
    }

    ++it;
  }

  CHECK(expectedValues.empty());
  CHECK(it == arangodb::iresearch::FieldIterator());
}

SECTION("traverse_complex_object_ordered_all_fields") {
  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"nested\": { \"foo\": \"str\" }, \
    \"keys\": [ \"1\",\"2\",\"3\",\"4\" ], \
    \"tokenizers\": {}, \
    \"boost\": \"10\", \
    \"depth\": \"20\", \
    \"fields\": { \"fieldA\" : { \"name\" : \"a\" }, \"fieldB\" : { \"name\" : \"b\" } }, \
    \"listValuation\": \"ignored\", \
    \"locale\": \"ru_RU.KOI8-R\", \
    \"array\" : [ \
      { \"id\" : \"1\", \"subarr\" : [ \"1\", \"2\", \"3\" ], \"subobj\" : { \"id\" : \"1\" } }, \
      { \"subarr\" : [ \"4\", \"5\", \"6\" ], \"subobj\" : { \"name\" : \"foo\" }, \"id\" : \"2\" }, \
      { \"id\" : \"3\", \"subarr\" : [ \"7\", \"8\", \"9\" ], \"subobj\" : { \"id\" : \"2\" } } \
    ] \
  }");

  std::unordered_multiset<std::string> expectedValues {
    "nested.foo",
    "keys[0]",
    "keys[1]",
    "keys[2]",
    "keys[3]",
    "boost",
    "depth",
    "fields.fieldA.name",
    "fields.fieldB.name",
    "listValuation",
    "locale",

    "array[0].id",
    "array[0].subarr[0]",
    "array[0].subarr[1]",
    "array[0].subarr[2]",
    "array[0].subobj.id",

    "array[1].subarr[0]",
    "array[1].subarr[1]",
    "array[1].subarr[2]",
    "array[1].subobj.name",
    "array[1].id",

    "array[2].id",
    "array[2].subarr[0]",
    "array[2].subarr[1]",
    "array[2].subarr[2]",
    "array[2].subobj.id"
  };

  auto const slice = json->slice();

  arangodb::iresearch::IResearchViewMeta viewMeta;
  arangodb::iresearch::IResearchLinkMeta linkMeta;
  linkMeta._includeAllFields = true; // include all fields
  linkMeta._nestListValues = true; // allow indexes in field names

  for (auto const& field : arangodb::iresearch::FieldIterator(slice, linkMeta, viewMeta)) {
    std::string const actualName = std::string(field.name());
    CHECK(1 == expectedValues.erase(actualName));
  }

  CHECK(expectedValues.empty());
}

SECTION("traverse_complex_object_ordered_all_filtered") {
  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"nested\": { \"foo\": \"str\" }, \
    \"keys\": [ \"1\",\"2\",\"3\",\"4\" ], \
    \"tokenizers\": {}, \
    \"boost\": \"10\", \
    \"depth\": \"20\", \
    \"fields\": { \"fieldA\" : { \"name\" : \"a\" }, \"fieldB\" : { \"name\" : \"b\" } }, \
    \"listValuation\": \"ignored\", \
    \"locale\": \"ru_RU.KOI8-R\", \
    \"array\" : [ \
      { \"id\" : \"1\", \"subarr\" : [ \"1\", \"2\", \"3\" ], \"subobj\" : { \"id\" : \"1\" } }, \
      { \"subarr\" : [ \"4\", \"5\", \"6\" ], \"subobj\" : { \"name\" : \"foo\" }, \"id\" : \"2\" }, \
      { \"id\" : \"3\", \"subarr\" : [ \"7\", \"8\", \"9\" ], \"subobj\" : { \"id\" : \"2\" } } \
    ] \
  }");

  auto linkMetaJson = arangodb::velocypack::Parser::fromJson("{ \
    \"boost\" : 10, \
    \"includeAllFields\" : false, \
    \"nestListValues\" : true, \
    \"fields\" : { \"boost\" : { \"boost\" : 10 } } \
  }");

  auto const slice = json->slice();
  arangodb::iresearch::IResearchViewMeta viewMeta;
  arangodb::iresearch::IResearchLinkMeta linkMeta;

  std::string error;
  REQUIRE(linkMeta.init(linkMetaJson->slice(), error));

  arangodb::iresearch::FieldIterator it(slice, linkMeta, viewMeta);
  CHECK(it.valid());
  CHECK(it != arangodb::iresearch::FieldIterator());

  auto& value = *it;
  CHECK("boost" == value.name());
  CHECK(10.f == value.boost());

  ++it;
  CHECK(!it.valid());
  CHECK(it == arangodb::iresearch::FieldIterator());
}

SECTION("traverse_complex_object_ordered_all_filtered") {
  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"nested\": { \"foo\": \"str\" }, \
    \"keys\": [ \"1\",\"2\",\"3\",\"4\" ], \
    \"tokenizers\": {}, \
    \"boost\": \"10\", \
    \"depth\": \"20\", \
    \"fields\": { \"fieldA\" : { \"name\" : \"a\" }, \"fieldB\" : { \"name\" : \"b\" } }, \
    \"listValuation\": \"ignored\", \
    \"locale\": \"ru_RU.KOI8-R\", \
    \"array\" : [ \
      { \"id\" : \"1\", \"subarr\" : [ \"1\", \"2\", \"3\" ], \"subobj\" : { \"id\" : \"1\" } }, \
      { \"subarr\" : [ \"4\", \"5\", \"6\" ], \"subobj\" : { \"name\" : \"foo\" }, \"id\" : \"2\" }, \
      { \"id\" : \"3\", \"subarr\" : [ \"7\", \"8\", \"9\" ], \"subobj\" : { \"id\" : \"2\" } } \
    ] \
  }");

  auto const slice = json->slice();

  arangodb::iresearch::IResearchViewMeta viewMeta;
  arangodb::iresearch::IResearchLinkMeta linkMeta;
  linkMeta._includeAllFields = false; // ignore all fields
  linkMeta._nestListValues = true; // allow indexes in field names

  arangodb::iresearch::FieldIterator it(slice, linkMeta, viewMeta);
  CHECK(!it.valid());
  CHECK(it == arangodb::iresearch::FieldIterator());
}

SECTION("traverse_complex_object_ordered_all_fields_custom_list_offset_prefix_suffix") {
  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"nested\": { \"foo\": \"str\" }, \
    \"keys\": [ \"1\",\"2\",\"3\",\"4\" ], \
    \"tokenizers\": {}, \
    \"boost\": \"10\", \
    \"depth\": \"20\", \
    \"fields\": { \"fieldA\" : { \"name\" : \"a\" }, \"fieldB\" : { \"name\" : \"b\" } }, \
    \"listValuation\": \"ignored\", \
    \"locale\": \"ru_RU.KOI8-R\", \
    \"array\" : [ \
      { \"id\" : \"1\", \"subarr\" : [ \"1\", \"2\", \"3\" ], \"subobj\" : { \"id\" : \"1\" } }, \
      { \"subarr\" : [ \"4\", \"5\", \"6\" ], \"subobj\" : { \"name\" : \"foo\" }, \"id\" : \"2\" }, \
      { \"id\" : \"3\", \"subarr\" : [ \"7\", \"8\", \"9\" ], \"subobj\" : { \"id\" : \"2\" } } \
    ] \
  }");

  std::unordered_multiset<std::string> expectedValues {
    "nested.foo",
    "keys{0}",
    "keys{1}",
    "keys{2}",
    "keys{3}",
    "boost",
    "depth",
    "fields.fieldA.name",
    "fields.fieldB.name",
    "listValuation",
    "locale",

    "array{0}.id",
    "array{0}.subarr{0}",
    "array{0}.subarr{1}",
    "array{0}.subarr{2}",
    "array{0}.subobj.id",

    "array{1}.subarr{0}",
    "array{1}.subarr{1}",
    "array{1}.subarr{2}",
    "array{1}.subobj.name",
    "array{1}.id",

    "array{2}.id",
    "array{2}.subarr{0}",
    "array{2}.subarr{1}",
    "array{2}.subarr{2}",
    "array{2}.subobj.id"
  };

  auto const slice = json->slice();

  arangodb::iresearch::IResearchViewMeta viewMeta;
  viewMeta._nestingListOffsetPrefix = "{";
  viewMeta._nestingListOffsetSuffix= "}";
  arangodb::iresearch::IResearchLinkMeta linkMeta;
  linkMeta._includeAllFields = true; // include all fields
  linkMeta._nestListValues = true; // allow indexes in field names

  arangodb::iresearch::FieldIterator it(slice, linkMeta, viewMeta);
  CHECK(it != arangodb::iresearch::FieldIterator());

  for (arangodb::iresearch::FieldIterator const end; it != end; ++it) {
    std::string const actualName = std::string((*it).name());
    CHECK(1 == expectedValues.erase(actualName));
  }

  CHECK(expectedValues.empty());
  CHECK(it == arangodb::iresearch::FieldIterator());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief generate tests
////////////////////////////////////////////////////////////////////////////////

}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------