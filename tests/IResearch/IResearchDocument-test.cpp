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

class InvalidTokenizer: public irs::analysis::analyzer {
 public:
  DECLARE_ANALYZER_TYPE();
  InvalidTokenizer(): irs::analysis::analyzer(InvalidTokenizer::type()) { _attrs.add<TestAttribute>(); }
  virtual iresearch::attributes const& attributes() const NOEXCEPT override { return _attrs; }
  static ptr make(irs::string_ref const&) { return nullptr; }
  virtual bool next() override { return false; }
  virtual bool reset(irs::string_ref const& data) override { return true; }

private:
  irs::attributes _attrs;
};

DEFINE_ANALYZER_TYPE_NAMED(InvalidTokenizer, "iresearch-document-invalid");
REGISTER_ANALYZER(InvalidTokenizer);

std::string mangleName(irs::string_ref const& name, irs::string_ref const& suffix) {
  std::string mangledName(name.c_str(), name.size());
  mangledName += '\0';
  mangledName.append(suffix.c_str(), suffix.size());
  return mangledName;
}

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

SECTION("FieldIterator_default_ctor") {
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
    { mangleName("nested---foo", "identity"), 1 },
    { mangleName("keys", "identity"), 4 },
    { mangleName("boost", "identity"), 1 },
    { mangleName("depth", "identity"), 1 },
    { mangleName("fields---fieldA---name", "identity"), 1 },
    { mangleName("fields---fieldB---name", "identity"), 1 },
    { mangleName("listValuation", "identity"), 1 },
    { mangleName("locale", "identity"), 1 },
    { mangleName("array---id", "identity"), 3 },
    { mangleName("array---subarr", "identity"), 9 },
    { mangleName("array---subobj---id", "identity"), 2 },
    { mangleName("array---subobj---name", "identity"), 1 },
    { mangleName("array---id", "identity"), 2 }
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
    { mangleName("nested.foo", "identity"), 1 },
    { mangleName("keys", "identity"), 4 },
    { mangleName("boost", "identity"), 1 },
    { mangleName("depth", "identity"), 1 },
    { mangleName("fields.fieldA.name", "identity"), 1 },
    { mangleName("fields.fieldB.name", "identity"), 1 },
    { mangleName("listValuation", "identity"), 1 },
    { mangleName("locale", "identity"), 1 },
    { mangleName("array.id", "identity"), 3 },
    { mangleName("array.subarr", "identity"), 9 },
    { mangleName("array.subobj.id", "identity"), 2 },
    { mangleName("array.subobj.name", "identity"), 1 },
    { mangleName("array.id", "identity"), 2 }
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
    mangleName("nested.foo", "identity"),
    mangleName("keys[0]", "identity"),
    mangleName("keys[1]", "identity"),
    mangleName("keys[2]", "identity"),
    mangleName("keys[3]", "identity"),
    mangleName("boost", "identity"),
    mangleName("depth", "identity"),
    mangleName("fields.fieldA.name", "identity"),
    mangleName("fields.fieldB.name", "identity"),
    mangleName("listValuation", "identity"),
    mangleName("locale", "identity"),

    mangleName("array[0].id", "identity"),
    mangleName("array[0].subarr[0]", "identity"),
    mangleName("array[0].subarr[1]", "identity"),
    mangleName("array[0].subarr[2]", "identity"),
    mangleName("array[0].subobj.id", "identity"),

    mangleName("array[1].subarr[0]", "identity"),
    mangleName("array[1].subarr[1]", "identity"),
    mangleName("array[1].subarr[2]", "identity"),
    mangleName("array[1].subobj.name", "identity"),
    mangleName("array[1].id", "identity"),

    mangleName("array[2].id", "identity"),
    mangleName("array[2].subarr[0]", "identity"),
    mangleName("array[2].subarr[1]", "identity"),
    mangleName("array[2].subarr[2]", "identity"),
    mangleName("array[2].subobj.id", "identity")
  };

  auto const slice = json->slice();

  arangodb::iresearch::IResearchViewMeta viewMeta;
  arangodb::iresearch::IResearchLinkMeta linkMeta;
  linkMeta._includeAllFields = true; // include all fields
  linkMeta._nestListValues = true; // allow indexes in field names

  // default analyzer
  auto const expected_analyzer = irs::analysis::analyzers::get("identity", "");

  arangodb::iresearch::FieldIterator doc(slice, linkMeta, viewMeta);
  auto& begin = doc.begin();
  auto& end = doc.end();
  for (;begin != end; ++begin) {
    auto& field = *begin;
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
  CHECK(mangleName("boost","identity") == value.name());
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
    CHECK(mangleName("stringValue", "identity") == field.name());
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
    CHECK(mangleName("stringValue", "iresearch-document-emptyen") == field.name());
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
    CHECK(mangleName("nullValue", "_n") == field.name());
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
    CHECK(mangleName("trueValue", "_b") == field.name());
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
    CHECK(mangleName("falseValue", "_b") == field.name());
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
    CHECK(mangleName("smallIntValue", "_d") == field.name());
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
    CHECK(mangleName("smallNegativeIntValue", "_d") == field.name());
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
    CHECK(mangleName("bigIntValue", "_d") == field.name());
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
    CHECK(mangleName("bigNegativeIntValue", "_d") == field.name());
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
    CHECK(mangleName("smallDoubleValue", "_d") == field.name());
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
    CHECK(mangleName("bigDoubleValue", "_d") == field.name());
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
    CHECK(mangleName("bigNegativeDoubleValue", "_d") == field.name());
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
    mangleName("nested.foo", "identity"),
    mangleName("keys{0}", "identity"),
    mangleName("keys{1}", "identity"),
    mangleName("keys{2}", "identity"),
    mangleName("keys{3}", "identity"),
    mangleName("boost", "identity"),
    mangleName("depth", "identity"),
    mangleName("fields.fieldA.name", "identity"),
    mangleName("fields.fieldB.name", "identity"),
    mangleName("listValuation", "identity"),
    mangleName("locale", "identity"),

    mangleName("array{0}.id", "identity"),
    mangleName("array{0}.subarr{0}", "identity"),
    mangleName("array{0}.subarr{1}", "identity"),
    mangleName("array{0}.subarr{2}", "identity"),
    mangleName("array{0}.subobj.id", "identity"),

    mangleName("array{1}.subarr{0}", "identity"),
    mangleName("array{1}.subarr{1}", "identity"),
    mangleName("array{1}.subarr{2}", "identity"),
    mangleName("array{1}.subobj.name", "identity"),
    mangleName("array{1}.id", "identity"),

    mangleName("array{2}.id", "identity"),
    mangleName("array{2}.subarr{0}", "identity"),
    mangleName("array{2}.subarr{1}", "identity"),
    mangleName("array{2}.subarr{2}", "identity"),
    mangleName("array{2}.subobj.id", "identity")
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
    CHECK(mangleName("nested.foo", "identity") == value.name());
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
    CHECK(mangleName("nested.foo", "iresearch-document-emptyen") == value.name());
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
    CHECK(mangleName("keys", "identity") == value.name());
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
    CHECK(mangleName("boost", "identity") == value.name());
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
    CHECK(mangleName("depth", "_d") == value.name());
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
    CHECK(mangleName("fields.fieldA.name", "identity") == value.name());
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
    CHECK(mangleName("fields.fieldA.name", "iresearch-document-emptyen") == value.name());
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
    CHECK(mangleName("listValuation", "identity") == value.name());
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
    CHECK(mangleName("listValuation", "iresearch-document-emptyen") == value.name());
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
    CHECK(mangleName("locale", "_n") == value.name());
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
    CHECK(mangleName("array[0].id", "_d") == value.name());
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
      CHECK(mangleName("array[0].subarr", "identity") == value.name());
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
      CHECK(mangleName("array[0].subarr", "iresearch-document-emptyen") == value.name());
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
      CHECK(mangleName("array[1].subarr", "identity") == value.name());
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
      CHECK(mangleName("array[1].subarr", "iresearch-document-emptyen") == value.name());
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
    CHECK(mangleName("array[1].id", "identity") == value.name());
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
    CHECK(mangleName("array[1].id", "iresearch-document-emptyen") == value.name());
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
    CHECK(mangleName("array[2].id", "_d") == value.name());
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
      CHECK(mangleName("array[2].subarr", "identity") == value.name());
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
      CHECK(mangleName("array[2].subarr", "iresearch-document-emptyen") == value.name());
      auto& analyzer = dynamic_cast<EmptyTokenizer&>(value.get_tokens());
      CHECK(!analyzer.next());
      CHECK(1.f == value.boost());
    }
  }

  ++it;
  CHECK(!it.valid());
  CHECK(it == arangodb::iresearch::FieldIterator());
}

SECTION("DocumentIterator_default_ctor") {
  CHECK(arangodb::iresearch::DocumentIterator::END == arangodb::iresearch::DocumentIterator::END.begin());
  CHECK(arangodb::iresearch::DocumentIterator::END == arangodb::iresearch::DocumentIterator::END.end());
}

SECTION("DocumentIterator_empty_field_iterator") {
  arangodb::iresearch::FieldIterator body;
  arangodb::iresearch::DocumentIterator it(0, 1, body);

  REQUIRE(arangodb::iresearch::DocumentIterator::END != it);
  REQUIRE(it == it.begin());
  REQUIRE(it != it.end());

  {
    auto& field = *it;
    CHECK("@_CID" == field.name());
    CHECK(1.f == field.boost());
    CHECK(irs::flags::empty_instance() == field.features());
    auto& stream = dynamic_cast<irs::string_token_stream&>(field.get_tokens());
    CHECK(stream.next());
  }

  ++it;
  REQUIRE(arangodb::iresearch::DocumentIterator::END != it);

  {
    auto& field = *it;
    CHECK("@_REV" == field.name());
    CHECK(1.f == field.boost());
    CHECK(irs::flags::empty_instance() == field.features());
    auto& stream = dynamic_cast<irs::string_token_stream&>(field.get_tokens());
    CHECK(stream.next());
  }

  ++it;
  REQUIRE(arangodb::iresearch::DocumentIterator::END == it);
}

SECTION("DocumentIterator_non_empty_field_iterator") {
  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"stringValue\": \"string\", \
    \"nullValue\": null \
  }");

  auto const slice = json->slice();

  arangodb::iresearch::IResearchViewMeta viewMeta;
  arangodb::iresearch::IResearchLinkMeta linkMeta;
  linkMeta._tokenizers.emplace_back("iresearch-document-empty", "en"); // add tokenizer
  linkMeta._includeAllFields = true; // include all fields

  arangodb::iresearch::FieldIterator body(slice, linkMeta, viewMeta);
  CHECK(body != arangodb::iresearch::FieldIterator());

  arangodb::iresearch::DocumentIterator it(0, 1, body);

  REQUIRE(arangodb::iresearch::DocumentIterator::END != it);
  REQUIRE(it == it.begin());
  REQUIRE(it != it.end());

  {
    auto& field = *it;
    CHECK("@_CID" == field.name());
    CHECK(1.f == field.boost());
    CHECK(irs::flags::empty_instance() == field.features());
    auto& stream = dynamic_cast<irs::string_token_stream&>(field.get_tokens());
    CHECK(stream.next());
  }

  ++it;
  REQUIRE(arangodb::iresearch::DocumentIterator::END != it);

  {
    auto& field = *it;
    CHECK("@_REV" == field.name());
    CHECK(1.f == field.boost());
    CHECK(irs::flags::empty_instance() == field.features());
    auto& stream = dynamic_cast<irs::string_token_stream&>(field.get_tokens());
    CHECK(stream.next());
  }

  ++it;
  REQUIRE(arangodb::iresearch::DocumentIterator::END != it);

  // stringValue (with IdentityTokenizer)
  {
    auto& field = *it;
    CHECK(mangleName("stringValue", "identity") == field.name());
    CHECK(1.f == field.boost());

    auto const expected_analyzer = irs::analysis::analyzers::get("identity", "");
    auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(field.get_tokens());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(expected_analyzer->attributes().features() == field.features());
  }

  ++it;
  REQUIRE(arangodb::iresearch::DocumentIterator::END != it);

  // stringValue (with EmptyTokenizer)
  {
    auto& field = *it;
    CHECK(mangleName("stringValue", "iresearch-document-emptyen") == field.name());
    CHECK(1.f == field.boost());

    auto const expected_analyzer = irs::analysis::analyzers::get("iresearch-document-empty", "en");
    auto& analyzer = dynamic_cast<EmptyTokenizer&>(field.get_tokens());
    CHECK(&expected_analyzer->type() == &analyzer.type());
    CHECK(expected_analyzer->attributes().features() == field.features());
  }

  ++it;
  REQUIRE(arangodb::iresearch::DocumentIterator::END != it);

  // nullValue
  {
    auto& field = *it;
    CHECK(mangleName("nullValue", "_n") == field.name());
    CHECK(1.f == field.boost());

    auto& analyzer = dynamic_cast<irs::null_token_stream&>(field.get_tokens());
    CHECK(analyzer.next());
  }

  ++it;
  REQUIRE(arangodb::iresearch::DocumentIterator::END == it);
}

SECTION("DocumentIterator_nullptr_tokenizer") {
  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"stringValue\": \"string\" \
  }");

  auto const slice = json->slice();

  // last tokenizer invalid
  {
    arangodb::iresearch::IResearchViewMeta viewMeta;
    arangodb::iresearch::IResearchLinkMeta linkMeta;
    linkMeta._tokenizers.emplace_back("iresearch-document-empty", "en"); // add tokenizer
    linkMeta._tokenizers.emplace_back("iresearch-document-invalid", "en"); // add tokenizer
    linkMeta._includeAllFields = true; // include all fields

    arangodb::iresearch::FieldIterator it(slice, linkMeta, viewMeta);
    REQUIRE(it.valid());
    REQUIRE(it != arangodb::iresearch::FieldIterator());

    // stringValue (with IdentityTokenizer)
    {
      auto& field = *it;
      CHECK(mangleName("stringValue", "identity") == field.name());
      CHECK(1.f == field.boost());

      auto const expected_analyzer = irs::analysis::analyzers::get("identity", "");
      auto& analyzer = dynamic_cast<irs::analysis::analyzer&>(field.get_tokens());
      CHECK(&expected_analyzer->type() == &analyzer.type());
      CHECK(expected_analyzer->attributes().features() == field.features());
    }

    ++it;
    REQUIRE(it.valid());
    REQUIRE(arangodb::iresearch::FieldIterator::END != it);

    // stringValue (with EmptyTokenizer)
    {
      auto& field = *it;
      CHECK(mangleName("stringValue", "iresearch-document-emptyen") == field.name());
      CHECK(1.f == field.boost());

      auto const expected_analyzer = irs::analysis::analyzers::get("iresearch-document-empty", "en");
      auto& analyzer = dynamic_cast<EmptyTokenizer&>(field.get_tokens());
      CHECK(&expected_analyzer->type() == &analyzer.type());
      CHECK(expected_analyzer->attributes().features() == field.features());
    }

    ++it;
    REQUIRE(!it.valid());
    REQUIRE(arangodb::iresearch::FieldIterator::END == it);
  }

  // first tokenizer is invalid
  {
    arangodb::iresearch::IResearchViewMeta viewMeta;
    arangodb::iresearch::IResearchLinkMeta linkMeta;
    linkMeta._tokenizers.clear();
    linkMeta._tokenizers.emplace_back("iresearch-document-invalid", "en"); // add tokenizer
    linkMeta._tokenizers.emplace_back("iresearch-document-empty", "en"); // add tokenizer
    linkMeta._includeAllFields = true; // include all fields

    arangodb::iresearch::FieldIterator it(slice, linkMeta, viewMeta);
    REQUIRE(it.valid());
    REQUIRE(it != arangodb::iresearch::FieldIterator());

    // stringValue (with EmptyTokenizer)
    {
      auto& field = *it;
      CHECK(mangleName("stringValue", "iresearch-document-emptyen") == field.name());
      CHECK(1.f == field.boost());

      auto const expected_analyzer = irs::analysis::analyzers::get("iresearch-document-empty", "en");
      auto& analyzer = dynamic_cast<EmptyTokenizer&>(field.get_tokens());
      CHECK(&expected_analyzer->type() == &analyzer.type());
      CHECK(expected_analyzer->attributes().features() == field.features());
    }

    ++it;
    REQUIRE(!it.valid());
    REQUIRE(arangodb::iresearch::FieldIterator::END == it);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief generate tests
////////////////////////////////////////////////////////////////////////////////

}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
