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

#include "analysis/analyzers.hpp"

NS_LOCAL

struct TestAttribute: public irs::attribute {
  DECLARE_ATTRIBUTE_TYPE();
  DECLARE_FACTORY_DEFAULT();
  TestAttribute() noexcept: irs::attribute(TestAttribute::type()) {}
  virtual void clear() override {}
};

DEFINE_ATTRIBUTE_TYPE(TestAttribute);
DEFINE_FACTORY_DEFAULT(TestAttribute);

class EmptyTokenizer: public irs::analysis::analyzer {
public:
  DECLARE_ANALYZER_TYPE();
  EmptyTokenizer(): irs::analysis::analyzer(EmptyTokenizer::type()) { _attrs.add<TestAttribute>(); }
  virtual iresearch::attributes const& attributes() const NOEXCEPT override { return _attrs; }
  static ptr make(irs::string_ref const&) { PTR_NAMED(EmptyTokenizer, ptr); return ptr; }
  virtual bool next() override { return false; }
  virtual bool reset(irs::string_ref const& data) override { return true; }

private:
  irs::attributes _attrs;
};

DEFINE_ANALYZER_TYPE_NAMED(EmptyTokenizer, "iresearch-document-empty");
REGISTER_ANALYZER(EmptyTokenizer);

NS_END

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
  CHECK(it == arangodb::iresearch::FieldIterator());
  CHECK(it == it.begin());
  CHECK(it == it.end());
  CHECK(it == arangodb::iresearch::FieldIterator::END);
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

  // default analyzer
  auto const expected_analyzer = irs::analysis::analyzers::get("identity", "");

  while (it.valid()) {
    auto& field = *it;
    std::string const actualName = std::string(field.name());
    auto const expectedValue = expectedValues.find(actualName);
    REQUIRE(expectedValues.end() != expectedValue);

    auto& refs = expectedValue->second;
    if (!--refs) {
      expectedValues.erase(expectedValue);
    }

    auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(field.get_tokens());
    CHECK(expected_analyzer->attributes().features() == field.features());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(linkMeta._boost == field.boost());

    ++it;
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

  // default analyzer
  auto const expected_analyzer = irs::analysis::analyzers::get("identity", "");

  while (it.valid()) {
    auto& field = *it;
    std::string const actualName = std::string(field.name());
    auto const expectedValue = expectedValues.find(actualName);
    REQUIRE(expectedValues.end() != expectedValue);

    auto& refs = expectedValue->second;
    if (!--refs) {
      expectedValues.erase(expectedValue);
    }

    auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(field.get_tokens());
    CHECK(expected_analyzer->attributes().features() == field.features());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(linkMeta._boost == field.boost());

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

  // default analyzer
  auto const expected_analyzer = irs::analysis::analyzers::get("identity", "");

  for (auto const& field : arangodb::iresearch::FieldIterator(slice, linkMeta, viewMeta)) {
    std::string const actualName = std::string(field.name());
    CHECK(1 == expectedValues.erase(actualName));

    auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(field.get_tokens());
    CHECK(expected_analyzer->attributes().features() == field.features());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(linkMeta._boost == field.boost());
  }

  CHECK(expectedValues.empty());
}

SECTION("traverse_complex_object_ordered_filtered") {
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
    \"boost\" : 1, \
    \"includeAllFields\" : false, \
    \"nestListValues\" : true, \
    \"fields\" : { \"boost\" : { \"boost\" : 10 } }, \
    \"tokenizers\" : { \"identity\": [\"\"] } \
  }");

  auto const slice = json->slice();
  arangodb::iresearch::IResearchViewMeta viewMeta;
  arangodb::iresearch::IResearchLinkMeta linkMeta;

  std::string error;
  REQUIRE(linkMeta.init(linkMetaJson->slice(), error));

  arangodb::iresearch::FieldIterator it(slice, linkMeta, viewMeta);
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  auto& value = *it;
  CHECK("boost" == value.name());
  const auto expected_analyzer = irs::analysis::analyzers::get("identity", "");
  auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(value.get_tokens());
  CHECK(expected_analyzer->attributes().features() == value.features());
  CHECK(&expected_analyzer->type() == &analyzer.type());
  CHECK(10.f == value.boost());

  ++it;
  CHECK(!it.valid());
  CHECK(it == arangodb::iresearch::FieldIterator());
}

SECTION("traverse_complex_object_ordered_filtered") {
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

SECTION("traverse_complex_object_ordered_empty_tokenizers") {
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
  linkMeta._tokenizers.clear(); // clear all tokenizers
  linkMeta._includeAllFields = true; // include all fields

  arangodb::iresearch::FieldIterator it(slice, linkMeta, viewMeta);
  CHECK(!it.valid());
  CHECK(it == arangodb::iresearch::FieldIterator());
}

SECTION("traverse_complex_object_ordered_check_value_types") {
  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"stringValue\": \"string\", \
    \"nullValue\": null, \
    \"trueValue\": true, \
    \"falseValue\": false, \
    \"smallIntValue\": 10, \
    \"smallNegativeIntValue\": -5, \
    \"bigIntValue\": 2147483647, \
    \"bigNegativeIntValue\": -2147483648, \
    \"smallDoubleValue\": 20.123, \
    \"bigDoubleValue\": 1.79769e+308, \
    \"bigNegativeDoubleValue\": -1.79769e+308 \
  }");

  auto const slice = json->slice();

  arangodb::iresearch::IResearchViewMeta viewMeta;
  arangodb::iresearch::IResearchLinkMeta linkMeta;
  linkMeta._tokenizers.emplace_back("iresearch-document-empty", "en"); // add tokenizer
  linkMeta._includeAllFields = true; // include all fields

  arangodb::iresearch::FieldIterator it(slice, linkMeta, viewMeta);
  CHECK(it != arangodb::iresearch::FieldIterator());

  // stringValue (with IdentityTokenizer)
  {
    auto& field = *it;
    CHECK("stringValue" == field.name());
    CHECK(1.f == field.boost());

    auto const expected_analyzer = irs::analysis::analyzers::get("identity", "");
    auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(field.get_tokens());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(expected_analyzer->attributes().features() == field.features());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // stringValue (with EmptyTokenizer)
  {
    auto& field = *it;
    CHECK("stringValue" == field.name());
    CHECK(1.f == field.boost());

    auto const expected_analyzer = irs::analysis::analyzers::get("iresearch-document-empty", "en");
    auto& analyzer = dynamic_cast<EmptyTokenizer&>(field.get_tokens());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(expected_analyzer->attributes().features() == field.features());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // nullValue
  {
    auto& field = *it;
    CHECK("nullValue" == field.name());
    CHECK(1.f == field.boost());

    auto& analyzer = dynamic_cast<irs::null_token_stream&>(field.get_tokens());
    CHECK(analyzer.next());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // trueValue
  {
    auto& field = *it;
    CHECK("trueValue" == field.name());
    CHECK(1.f == field.boost());

    auto& analyzer = dynamic_cast<irs::boolean_token_stream&>(field.get_tokens());
    CHECK(analyzer.next());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // falseValue
  {
    auto& field = *it;
    CHECK("falseValue" == field.name());
    CHECK(1.f == field.boost());

    auto& analyzer = dynamic_cast<irs::boolean_token_stream&>(field.get_tokens());
    CHECK(analyzer.next());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // smallIntValue
  {
    auto& field = *it;
    CHECK("smallIntValue" == field.name());
    CHECK(1.f == field.boost());

    auto& analyzer = dynamic_cast<irs::numeric_token_stream&>(field.get_tokens());
    CHECK(analyzer.next());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // smallNegativeIntValue
  {
    auto& field = *it;
    CHECK("smallNegativeIntValue" == field.name());
    CHECK(1.f == field.boost());

    auto& analyzer = dynamic_cast<irs::numeric_token_stream&>(field.get_tokens());
    CHECK(analyzer.next());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // bigIntValue
  {
    auto& field = *it;
    CHECK("bigIntValue" == field.name());
    CHECK(1.f == field.boost());

    auto& analyzer = dynamic_cast<irs::numeric_token_stream&>(field.get_tokens());
    CHECK(analyzer.next());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // bigNegativeIntValue
  {
    auto& field = *it;
    CHECK("bigNegativeIntValue" == field.name());
    CHECK(1.f == field.boost());

    auto& analyzer = dynamic_cast<irs::numeric_token_stream&>(field.get_tokens());
    CHECK(analyzer.next());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // smallDoubleValue
  {
    auto& field = *it;
    CHECK("smallDoubleValue" == field.name());
    CHECK(1.f == field.boost());

    auto& analyzer = dynamic_cast<irs::numeric_token_stream&>(field.get_tokens());
    CHECK(analyzer.next());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // bigDoubleValue
  {
    auto& field = *it;
    CHECK("bigDoubleValue" == field.name());
    CHECK(1.f == field.boost());

    auto& analyzer = dynamic_cast<irs::numeric_token_stream&>(field.get_tokens());
    CHECK(analyzer.next());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // bigNegativeDoubleValue
  {
    auto& field = *it;
    CHECK("bigNegativeDoubleValue" == field.name());
    CHECK(1.f == field.boost());

    auto& analyzer = dynamic_cast<irs::numeric_token_stream&>(field.get_tokens());
    CHECK(analyzer.next());
  }

  ++it;
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

  // default analyzer
  auto const expected_analyzer = irs::analysis::analyzers::get("identity", "");

  for (arangodb::iresearch::FieldIterator const end; it != end; ++it) {
    auto& field = *it;
    std::string const actualName = std::string(field.name());
    CHECK(1 == expectedValues.erase(actualName));

    auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(field.get_tokens());
    CHECK(expected_analyzer->attributes().features() == field.features());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(linkMeta._boost == field.boost());
  }

  CHECK(expectedValues.empty());
  CHECK(it == arangodb::iresearch::FieldIterator());
}

SECTION("traverse_complex_object_check_meta_inheritance") {
  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"nested\": { \"foo\": \"str\" }, \
    \"keys\": [ \"1\",\"2\",\"3\",\"4\" ], \
    \"tokenizers\": {}, \
    \"boost\": \"10\", \
    \"depth\": 20, \
    \"fields\": { \"fieldA\" : { \"name\" : \"a\" }, \"fieldB\" : { \"name\" : \"b\" } }, \
    \"listValuation\": \"ignored\", \
    \"locale\": null, \
    \"array\" : [ \
      { \"id\" : 1, \"subarr\" : [ \"1\", \"2\", \"3\" ], \"subobj\" : { \"id\" : 1 } }, \
      { \"subarr\" : [ \"4\", \"5\", \"6\" ], \"subobj\" : { \"name\" : \"foo\" }, \"id\" : \"2\" }, \
      { \"id\" : 3, \"subarr\" : [ \"7\", \"8\", \"9\" ], \"subobj\" : { \"id\" : 2 } } \
    ] \
  }");

  auto const slice = json->slice();

  auto linkMetaJson = arangodb::velocypack::Parser::fromJson("{ \
    \"boost\" : 1, \
    \"includeAllFields\" : true, \
    \"nestListValues\" : true, \
    \"fields\" : { \
       \"boost\" : { \"boost\" : 10, \"tokenizers\" : { \"identity\" : [\"\"] } }, \
       \"keys\" : { \"nestListValues\" : false, \"tokenizers\" : { \"identity\" : [\"\"] } }, \
       \"depth\" : { \"boost\" : 5, \"nestListValues\" : true }, \
       \"fields\" : { \"includeAllFields\" : false, \"boost\" : 3, \"fields\" : { \"fieldA\" : { \"includeAllFields\" : true } } }, \
       \"listValuation\" : { \"includeAllFields\" : false }, \
       \"array\" : { \
         \"fields\" : { \"subarr\" : { \"nestListValues\" : false }, \"subobj\": { \"includeAllFields\" : false }, \"id\" : { \"boost\" : 2 } } \
       } \
     }, \
    \"tokenizers\" : { \"iresearch-document-empty\" : [\"en\"], \"identity\": [\"\"] } \
  }");

  arangodb::iresearch::IResearchViewMeta viewMeta;
  arangodb::iresearch::IResearchLinkMeta linkMeta;

  std::string error;
  REQUIRE(linkMeta.init(linkMetaJson->slice(), error));

  arangodb::iresearch::FieldIterator it(slice, linkMeta, viewMeta);
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // nested.foo (with IdentityTokenizer)
  {
    auto& value = *it;
    CHECK("nested.foo" == value.name());
    const auto expected_analyzer = irs::analysis::analyzers::get("identity", "");
    auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(value.get_tokens());
    CHECK(expected_analyzer->attributes().features() == value.features());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(1.f == value.boost());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // nested.foo (with EmptyTokenizer)
  {
    auto& value = *it;
    CHECK("nested.foo" == value.name());
    auto& analyzer = dynamic_cast<EmptyTokenizer&>(value.get_tokens());
    CHECK(!analyzer.next());
    CHECK(1.f == value.boost());
  }

  // keys[]
  for (size_t i = 0; i < 4; ++i) {
    ++it;
    REQUIRE(it.valid());
    REQUIRE(it != arangodb::iresearch::FieldIterator());

    auto& value = *it;
    CHECK("keys" == value.name());
    const auto expected_analyzer = irs::analysis::analyzers::get("identity", "");
    auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(value.get_tokens());
    CHECK(expected_analyzer->attributes().features() == value.features());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(1.f == value.boost());

  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // boost
  {
    auto& value = *it;
    CHECK("boost" == value.name());
    const auto expected_analyzer = irs::analysis::analyzers::get("identity", "");
    auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(value.get_tokens());
    CHECK(expected_analyzer->attributes().features() == value.features());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(10.f == value.boost());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // depth
  {
    auto& value = *it;
    CHECK("depth" == value.name());
    auto& analyzer = dynamic_cast<irs::numeric_token_stream&>(value.get_tokens());
    CHECK(analyzer.next());
    CHECK(5.f == value.boost());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // fields.fieldA (with IdenitytTokenizer)
  {
    auto& value = *it;
    CHECK("fields.fieldA.name" == value.name());
    const auto expected_analyzer = irs::analysis::analyzers::get("identity", "");
    auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(value.get_tokens());
    CHECK(expected_analyzer->attributes().features() == value.features());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(3.f == value.boost());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // fields.fieldA (with EmptyTokenizer)
  {
    auto& value = *it;
    CHECK("fields.fieldA.name" == value.name());
    auto& analyzer = dynamic_cast<EmptyTokenizer&>(value.get_tokens());
    CHECK(!analyzer.next());
    CHECK(3.f == value.boost());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // listValuation (with IdenitytTokenizer)
  {
    auto& value = *it;
    CHECK("listValuation" == value.name());
    const auto expected_analyzer = irs::analysis::analyzers::get("identity", "");
    auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(value.get_tokens());
    CHECK(expected_analyzer->attributes().features() == value.features());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(1.f == value.boost());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // listValuation (with EmptyTokenizer)
  {
    auto& value = *it;
    CHECK("listValuation" == value.name());
    auto& analyzer = dynamic_cast<EmptyTokenizer&>(value.get_tokens());
    CHECK(!analyzer.next());
    CHECK(1.f == value.boost());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // locale
  {
    auto& value = *it;
    CHECK("locale" == value.name());
    auto& analyzer = dynamic_cast<irs::null_token_stream&>(value.get_tokens());
    CHECK(analyzer.next());
    CHECK(1.f == value.boost());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // array[0].id
  {
    auto& value = *it;
    CHECK("array[0].id" == value.name());
    auto& analyzer = dynamic_cast<irs::numeric_token_stream&>(value.get_tokens());
    CHECK(analyzer.next());
    CHECK(2.f == value.boost());
  }


  // array[0].subarr[0-2]
  for (size_t i = 0; i < 3; ++i) {
    ++it;
    REQUIRE(it.valid());
    REQUIRE(it != arangodb::iresearch::FieldIterator());

    // IdentityTokenizer
    {
      auto& value = *it;
      CHECK("array[0].subarr" == value.name());
      const auto expected_analyzer = irs::analysis::analyzers::get("identity", "");
      auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(value.get_tokens());
      CHECK(expected_analyzer->attributes().features() == value.features());
      CHECK(&expected_analyzer->type() == &analyzer.type());
      CHECK(1.f == value.boost());
    }

    ++it;
    REQUIRE(it.valid());
    REQUIRE(it != arangodb::iresearch::FieldIterator());

    // EmptyTokenizer
    {
      auto& value = *it;
      CHECK("array[0].subarr" == value.name());
      auto& analyzer = dynamic_cast<EmptyTokenizer&>(value.get_tokens());
      CHECK(!analyzer.next());
      CHECK(1.f == value.boost());
    }
  }

   // array[1].subarr[0-2]
  for (size_t i = 0; i < 3; ++i) {
    ++it;
    REQUIRE(it.valid());
    REQUIRE(it != arangodb::iresearch::FieldIterator());

    // IdentityTokenizer
    {
      auto& value = *it;
      CHECK("array[1].subarr" == value.name());
      const auto expected_analyzer = irs::analysis::analyzers::get("identity", "");
      auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(value.get_tokens());
      CHECK(expected_analyzer->attributes().features() == value.features());
      CHECK(&expected_analyzer->type() == &analyzer.type());
      CHECK(1.f == value.boost());
    }

    ++it;
    REQUIRE(it.valid());
    REQUIRE(it != arangodb::iresearch::FieldIterator());

    // EmptyTokenizer
    {
      auto& value = *it;
      CHECK("array[1].subarr" == value.name());
      auto& analyzer = dynamic_cast<EmptyTokenizer&>(value.get_tokens());
      CHECK(!analyzer.next());
      CHECK(1.f == value.boost());
    }
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // array[1].id (IdentityTokenizer)
  {
    auto& value = *it;
    CHECK("array[1].id" == value.name());
    const auto expected_analyzer = irs::analysis::analyzers::get("identity", "");
    auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(value.get_tokens());
    CHECK(expected_analyzer->attributes().features() == value.features());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(2.f == value.boost());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // array[1].id (EmptyTokenizer)
  {
    auto& value = *it;
    CHECK("array[1].id" == value.name());
    auto& analyzer = dynamic_cast<EmptyTokenizer&>(value.get_tokens());
    CHECK(!analyzer.next());
    CHECK(2.f == value.boost());
  }

  ++it;
  REQUIRE(it.valid());
  REQUIRE(it != arangodb::iresearch::FieldIterator());

  // array[2].id (IdentityTokenizer)
  {
    auto& value = *it;
    CHECK("array[2].id" == value.name());
    const auto expected_analyzer = irs::analysis::analyzers::get("identity", "");
    auto& analyzer = dynamic_cast<irs::numeric_token_stream&>(value.get_tokens());
    CHECK(analyzer.next());
    CHECK(2.f == value.boost());
  }

  // array[2].subarr[0-2]
  for (size_t i = 0; i < 3; ++i) {
    ++it;
    REQUIRE(it.valid());
    REQUIRE(it != arangodb::iresearch::FieldIterator());

    // IdentityTokenizer
    {
      auto& value = *it;
      CHECK("array[2].subarr" == value.name());
      const auto expected_analyzer = irs::analysis::analyzers::get("identity", "");
      auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(value.get_tokens());
      CHECK(expected_analyzer->attributes().features() == value.features());
      CHECK(&expected_analyzer->type() == &analyzer.type());
      CHECK(1.f == value.boost());
    }

    ++it;
    REQUIRE(it.valid());
    REQUIRE(it != arangodb::iresearch::FieldIterator());

    // EmptyTokenizer
    {
      auto& value = *it;
      CHECK("array[2].subarr" == value.name());
      auto& analyzer = dynamic_cast<EmptyTokenizer&>(value.get_tokens());
      CHECK(!analyzer.next());
      CHECK(1.f == value.boost());
    }
  }

  ++it;
  CHECK(!it.valid());
  CHECK(it == arangodb::iresearch::FieldIterator());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief generate tests
////////////////////////////////////////////////////////////////////////////////

}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
