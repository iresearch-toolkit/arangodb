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

#include <boost/test/unit_test.hpp>

#include "IResearch/VelocyPackHelper.h"

#include "velocypack/Iterator.h"
#include "velocypack/Builder.h"
#include "velocypack/Parser.h"
#include "velocypack/velocypack-aliases.h"

// -----------------------------------------------------------------------------
// --SECTION--                                                 setup / tear-down
// -----------------------------------------------------------------------------

struct IResearchVelocyPackHelperSetup { };

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief setup
////////////////////////////////////////////////////////////////////////////////

BOOST_FIXTURE_TEST_SUITE(IResearchVelocyPackHelperTest, IResearchVelocyPackHelperSetup)

BOOST_AUTO_TEST_CASE(test_defaults) {
  arangodb::iresearch::ObjectIterator it;
  BOOST_CHECK_EQUAL(0U, it.depth());
  BOOST_CHECK(!it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator() == it);

  size_t calls_count = 0;
  auto visitor = [&calls_count](arangodb::iresearch::IteratorValue const&) {
    ++calls_count;
  };
  it.visit(visitor);
  BOOST_CHECK_EQUAL(0U, calls_count);
  // we not able to move the invalid iterator forward
}

BOOST_AUTO_TEST_CASE(test_empty_object) {
  auto json = arangodb::velocypack::Parser::fromJson("{ }");
  auto slice = json->slice();

  arangodb::iresearch::ObjectIterator it(slice);

  BOOST_CHECK_EQUAL(1U, it.depth());
  BOOST_CHECK(it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator(slice) == it);

  auto& value = it.value(0); // value at level 0
  BOOST_CHECK_EQUAL(0U, value.pos);
  BOOST_CHECK(VPackValueType::Object == value.type);
  BOOST_CHECK(value.key.isNone());
  BOOST_CHECK(value.value.isNone());
  BOOST_CHECK(&value == &*it);

  ++it;

  BOOST_CHECK_EQUAL(0U, it.depth());
  BOOST_CHECK(!it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator() == it);
}

BOOST_AUTO_TEST_CASE(test_subarray_of_emptyobjects) {
  auto json = arangodb::velocypack::Parser::fromJson("[ {}, {}, {} ]");
  auto slice = json->slice();

  arangodb::iresearch::ObjectIterator it(slice);

  BOOST_CHECK_EQUAL(2U, it.depth());
  BOOST_CHECK(it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator(slice) == it);

  // check value at level 0
  {
    auto& value = it.value(0);
    BOOST_CHECK_EQUAL(0U, value.pos);
    BOOST_CHECK(VPackValueType::Array == value.type);
    BOOST_CHECK(value.key.isObject());
    BOOST_CHECK(value.value.isObject());
  }

  // check value at level 1
  {
    auto& value = it.value(1);
    BOOST_CHECK_EQUAL(0U, value.pos);
    BOOST_CHECK(VPackValueType::Object == value.type);
    BOOST_CHECK(value.key.isNone());
    BOOST_CHECK(value.value.isNone());
    BOOST_CHECK(&value == &*it);
  }

  {
    auto const prev = it;
    BOOST_CHECK(prev == it++);
  }

  // check value at level 0
  {
    auto& value = it.value(0);
    BOOST_CHECK_EQUAL(1U, value.pos);
    BOOST_CHECK(VPackValueType::Array == value.type);
    BOOST_CHECK(value.key.isObject());
    BOOST_CHECK(value.value.isObject());
  }

  // check value at level 1
  {
    auto& value = it.value(1);
    BOOST_CHECK_EQUAL(0U, value.pos);
    BOOST_CHECK(VPackValueType::Object == value.type);
    BOOST_CHECK(value.key.isNone());
    BOOST_CHECK(value.value.isNone());
    BOOST_CHECK(&value == &*it);
  }

  ++it;

  // check value at level 0
  {
    auto& value = it.value(0);
    BOOST_CHECK_EQUAL(2U, value.pos);
    BOOST_CHECK(VPackValueType::Array == value.type);
    BOOST_CHECK(value.key.isObject());
    BOOST_CHECK(value.value.isObject());
  }

  // check value at level 1
  {
    auto& value = it.value(1);
    BOOST_CHECK_EQUAL(0U, value.pos);
    BOOST_CHECK(VPackValueType::Object == value.type);
    BOOST_CHECK(value.key.isNone());
    BOOST_CHECK(value.value.isNone());
    BOOST_CHECK(&value == &*it);
  }

  {
    auto const prev = it;
    BOOST_CHECK(prev == it++);
  }

  BOOST_CHECK_EQUAL(0U, it.depth());
  BOOST_CHECK(!it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator() == it);
}

BOOST_AUTO_TEST_CASE(test_small_plain_object) {
  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"boost\": \"10\" \
  }");
  auto slice = json->slice();

  arangodb::iresearch::ObjectIterator it(slice);

  BOOST_CHECK_EQUAL(1U, it.depth());
  BOOST_CHECK(it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator(slice) == it);

  auto& value = *it;

  BOOST_CHECK_EQUAL(0U, value.pos);
  BOOST_CHECK(VPackValueType::Object == value.type);
  BOOST_CHECK(value.key.isString());
  BOOST_CHECK("boost" == value.key.copyString());
  BOOST_CHECK(value.value.isString());
  BOOST_CHECK("10" == value.value.copyString());

  ++it;

  BOOST_CHECK_EQUAL(0U, it.depth());
  BOOST_CHECK(!it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator() == it);
}

BOOST_AUTO_TEST_CASE(test_empty_subarray) {
  auto json = arangodb::velocypack::Parser::fromJson("[ [ [ ] ] ]");
  auto slice = json->slice();

  arangodb::iresearch::ObjectIterator it(slice);

  BOOST_CHECK_EQUAL(3U, it.depth());
  BOOST_CHECK(it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator(slice) == it);

  // check that ObjectIterator::visit & ObjectIterator::value operates on the same values
  {
    bool result = true;
    size_t level = 0;
    auto check_levels = [&it, level, &result](arangodb::iresearch::IteratorValue const& value) mutable {
      result &= (&(it.value(level++)) == &value);
    };
    it.visit(check_levels);
    BOOST_CHECK(result);
  }

  // check value at level 0
  {
    auto& value = it.value(0);
    BOOST_CHECK_EQUAL(0U, value.pos);
    BOOST_CHECK(VPackValueType::Array == value.type);
    BOOST_CHECK(value.key.isArray());
    BOOST_CHECK(value.value.isArray());
  }

  // check value at level 1
  {
    auto& value = it.value(1);
    BOOST_CHECK_EQUAL(0U, value.pos);
    BOOST_CHECK(VPackValueType::Array == value.type);
    BOOST_CHECK(value.key.isArray());
    BOOST_CHECK(value.value.isArray());
  }

  // check value at level 2
  {
    auto& value = it.value(2);
    BOOST_CHECK_EQUAL(0U, value.pos);
    BOOST_CHECK(VPackValueType::Array == value.type);
    BOOST_CHECK(value.key.isNone());
    BOOST_CHECK(value.value.isNone());
    BOOST_CHECK(&value == &*it);
  }

  ++it;

  BOOST_CHECK_EQUAL(0U, it.depth());
  BOOST_CHECK(!it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator() == it);
}

BOOST_AUTO_TEST_CASE(test_empty_subobject) {
  auto json = arangodb::velocypack::Parser::fromJson("{ \"sub0\" : { \"sub1\" : { } } }");
  auto slice = json->slice();

  arangodb::iresearch::ObjectIterator it(slice);

  BOOST_CHECK_EQUAL(3U, it.depth());
  BOOST_CHECK(it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator(slice) == it);

  // check that ObjectIterator::visit & ObjectIterator::value operates on the same values
  {
    bool result = true;
    size_t level = 0;
    auto check_levels = [&it, level, &result](arangodb::iresearch::IteratorValue const& value) mutable {
      result &= (&(it.value(level++)) == &value);
    };
    it.visit(check_levels);
    BOOST_CHECK(result);
  }

  // check value at level 0
  {
    auto& value = it.value(0);
    BOOST_CHECK_EQUAL(0U, value.pos);
    BOOST_CHECK(VPackValueType::Object == value.type);
    BOOST_CHECK(value.key.isString());
    BOOST_CHECK("sub0" == value.key.copyString());
    BOOST_CHECK(value.value.isObject());
  }

  // check value at level 1
  {
    auto& value = it.value(1);
    BOOST_CHECK_EQUAL(0U, value.pos);
    BOOST_CHECK(VPackValueType::Object == value.type);
    BOOST_CHECK(value.key.isString());
    BOOST_CHECK("sub1" == value.key.copyString());
    BOOST_CHECK(value.value.isObject());
  }

  // check value at level 2
  {
    auto& value = it.value(2);
    BOOST_CHECK_EQUAL(0U, value.pos);
    BOOST_CHECK(VPackValueType::Object == value.type);
    BOOST_CHECK(value.key.isNone());
    BOOST_CHECK(value.value.isNone());
    BOOST_CHECK(&value == &*it);
  }

  ++it;

  BOOST_CHECK_EQUAL(0U, it.depth());
  BOOST_CHECK(!it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator() == it);
}

BOOST_AUTO_TEST_CASE(test_empty_array) {
  auto json = arangodb::velocypack::Parser::fromJson("[ ]");
  auto slice = json->slice();

  arangodb::iresearch::ObjectIterator it(slice);

  BOOST_CHECK_EQUAL(1U, it.depth());
  BOOST_CHECK(it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator(slice) == it);

  auto& value = it.value(0); // value at level 0
  BOOST_CHECK_EQUAL(0U, value.pos);
  BOOST_CHECK(VPackValueType::Array == value.type);
  BOOST_CHECK(value.key.isNone());
  BOOST_CHECK(value.value.isNone());
  BOOST_CHECK(&value == &*it);

  ++it;

  BOOST_CHECK_EQUAL(0U, it.depth());
  BOOST_CHECK(!it.valid());
  BOOST_CHECK(arangodb::iresearch::ObjectIterator() == it);
}

BOOST_AUTO_TEST_CASE(test_complex_object) {
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
    "nested{0}.foo{0}=str",
    "keys{1}[0]=1",
    "keys{1}[1]=2",
    "keys{1}[2]=3",
    "keys{1}[3]=4",
    "tokenizers{2}=",
    "boost{3}=10",
    "depth{4}=20",
    "fields{5}.fieldA{0}.name{0}=a",
    "fields{5}.fieldB{1}.name{0}=b",
    "listValuation{6}=ignored",
    "locale{7}=ru_RU.KOI8-R",

    "array{8}[0].id{0}=1",
    "array{8}[0].subarr{1}[0]=1",
    "array{8}[0].subarr{1}[1]=2",
    "array{8}[0].subarr{1}[2]=3",
    "array{8}[0].subobj{2}.id{0}=1",

    "array{8}[1].subarr{0}[0]=4",
    "array{8}[1].subarr{0}[1]=5",
    "array{8}[1].subarr{0}[2]=6",
    "array{8}[1].subobj{1}.name{0}=foo",
    "array{8}[1].id{2}=2",

    "array{8}[2].id{0}=3",
    "array{8}[2].subarr{1}[0]=7",
    "array{8}[2].subarr{1}[1]=8",
    "array{8}[2].subarr{1}[2]=9",
    "array{8}[2].subobj{2}.id{0}=2",
  };

  auto slice = json->slice();

  std::string name;
  auto visitor = [&name](arangodb::iresearch::IteratorValue const& value) {
    if (value.type == VPackValueType::Array) {
      name += '[';
      name += std::to_string(value.pos);
      name += ']';
    } else if (value.type == VPackValueType::Object) {
      if (!value.key.isString()) {
        return;
      }

      if (!name.empty()) {
        name += '.';
      }

      name += value.key.copyString();
      name += '{';
      name += std::to_string(value.pos);
      name += '}';
    }
  };

  for (arangodb::iresearch::ObjectIterator it(slice); it.valid(); ++it) {
    it.visit(visitor);
    name += '=';

    auto& value = *it;
    if (value.value.isString()) {
      name += value.value.copyString();
    }

    BOOST_CHECK(expectedValues.erase(name) > 0);

    name.clear();
  }

  BOOST_CHECK(expectedValues.empty());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief generate tests
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_SUITE_END()

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
