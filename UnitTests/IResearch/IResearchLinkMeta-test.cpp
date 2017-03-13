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

#include "analysis/analyzers.hpp"
#include "utils/locale_utils.hpp"

#include "IResearch/IResearchLinkMeta.h"
#include "velocypack/Builder.h"
#include "velocypack/Iterator.h"
#include "velocypack/Parser.h"

NS_LOCAL

class EmptyTokenizer: public irs::analysis::analyzer {
public:
  DECLARE_ANALYZER_TYPE();
  EmptyTokenizer(): irs::analysis::analyzer(EmptyTokenizer::type()) {}
  virtual iresearch::attributes const& attributes() const NOEXCEPT override { return _attrs; }
  static ptr make(irs::string_ref const&) { PTR_NAMED(EmptyTokenizer, ptr); return ptr; }
  virtual bool next() override { return false; }
  virtual bool reset(irs::string_ref const& data) override { return true; }

private:
  irs::attributes _attrs;
};

DEFINE_ANALYZER_TYPE_NAMED(EmptyTokenizer, "empty");
REGISTER_ANALYZER(EmptyTokenizer);

NS_END

// -----------------------------------------------------------------------------
// --SECTION--                                                 setup / tear-down
// -----------------------------------------------------------------------------

struct IResearchLinkMetaSetup {
  IResearchLinkMetaSetup() {
    BOOST_TEST_MESSAGE("setup IResearchLinkMeta");
  }

  ~IResearchLinkMetaSetup() {
    BOOST_TEST_MESSAGE("tear-down IResearchLinkMeta");
  }
};

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief setup
////////////////////////////////////////////////////////////////////////////////

BOOST_FIXTURE_TEST_SUITE(IResearchLinkMetaTest, IResearchLinkMetaSetup)

BOOST_AUTO_TEST_CASE(test_defaults) {
  arangodb::iresearch::IResearchLinkMeta meta;

  BOOST_CHECK_EQUAL(1., meta._boost);
  BOOST_CHECK_EQUAL(std::numeric_limits<size_t>::max(), meta._depth);
  BOOST_CHECK_EQUAL(true, meta._fields.empty());
  BOOST_CHECK_EQUAL(arangodb::iresearch::ListValuation::MULTIVALUED, meta._listValuation);
  BOOST_CHECK_EQUAL(std::string("C"), irs::locale_utils::name(meta._locale));
  BOOST_CHECK_EQUAL(1, meta._tokenizers.size());
  BOOST_CHECK_EQUAL("identity", meta._tokenizers.begin()->first);
  BOOST_CHECK_EQUAL("", meta._tokenizers.begin()->second.first);
  BOOST_CHECK_EQUAL(false, !meta._tokenizers.begin()->second.second);
}

BOOST_AUTO_TEST_CASE(test_inheritDefaults) {
  arangodb::iresearch::IResearchLinkMeta defaults;
  arangodb::iresearch::IResearchLinkMeta meta;
  std::unordered_set<std::string> expectedFields = { "abc" };
  std::unordered_set<std::string> expectedOverrides = { "xyz" };
  std::string tmpString;

  defaults._boost = 3.14f;
  defaults._depth = 42;
  defaults._fields["abc"] = std::move(arangodb::iresearch::IResearchLinkMeta());
  defaults._listValuation = arangodb::iresearch::ListValuation::ORDERED;
  defaults._locale = irs::locale_utils::locale("ru");
  defaults._tokenizers.clear();
  defaults._tokenizers.emplace(
    std::piecewise_construct, 
    std::forward_as_tuple("empty"),
    std::forward_as_tuple("en", nullptr)
  );
  defaults._fields["abc"]._fields["xyz"] = std::move(arangodb::iresearch::IResearchLinkMeta());

  auto json = arangodb::velocypack::Parser::fromJson("{}");
  BOOST_REQUIRE_EQUAL(true, meta.init(json->slice(), tmpString, defaults));
  BOOST_CHECK_EQUAL(3.14f, meta._boost);
  BOOST_CHECK_EQUAL(42, meta._depth);
  BOOST_CHECK_EQUAL(1, meta._fields.size());

  for (auto& field: meta._fields) {
    BOOST_CHECK_EQUAL(1, expectedFields.erase(field.first));
    BOOST_CHECK_EQUAL(1, field.second._fields.size());

    for (auto& fieldOverride: field.second._fields) {
      auto& actual = fieldOverride.second;
      BOOST_CHECK_EQUAL(1, expectedOverrides.erase(fieldOverride.first));

      if ("xyz" == fieldOverride.first) {
        BOOST_CHECK_EQUAL(1., actual._boost);
        BOOST_CHECK_EQUAL(std::numeric_limits<size_t>::max(), actual._depth);
        BOOST_CHECK_EQUAL(true, actual._fields.empty());
        BOOST_CHECK_EQUAL(arangodb::iresearch::ListValuation::MULTIVALUED, actual._listValuation);
        BOOST_CHECK_EQUAL(std::string("C"), irs::locale_utils::name(actual._locale));
        BOOST_CHECK_EQUAL(1, actual._tokenizers.size());
        BOOST_CHECK_EQUAL("identity", actual._tokenizers.begin()->first);
        BOOST_CHECK_EQUAL("", actual._tokenizers.begin()->second.first);
        BOOST_CHECK_EQUAL(false, !actual._tokenizers.begin()->second.second);
      }
    }
  }

  BOOST_CHECK_EQUAL(true, expectedOverrides.empty());
  BOOST_CHECK_EQUAL(true, expectedFields.empty());
  BOOST_CHECK_EQUAL(arangodb::iresearch::ListValuation::ORDERED, meta._listValuation);
  BOOST_CHECK_EQUAL(std::string("ru"), irs::locale_utils::name(meta._locale));

  BOOST_CHECK_EQUAL(1, meta._tokenizers.size());
  BOOST_CHECK_EQUAL("empty", meta._tokenizers.begin()->first);
  BOOST_CHECK_EQUAL("en", meta._tokenizers.begin()->second.first);
  BOOST_CHECK_EQUAL(false, !meta._tokenizers.begin()->second.second);
}

BOOST_AUTO_TEST_CASE(test_readDefaults) {
  arangodb::iresearch::IResearchLinkMeta meta;
  auto json = arangodb::velocypack::Parser::fromJson("{}");
  std::string tmpString;

  BOOST_REQUIRE_EQUAL(true, meta.init(json->slice(), tmpString));
  BOOST_CHECK_EQUAL(1., meta._boost);
  BOOST_CHECK_EQUAL(std::numeric_limits<size_t>::max(), meta._depth);
  BOOST_CHECK_EQUAL(true, meta._fields.empty());
  BOOST_CHECK_EQUAL(arangodb::iresearch::ListValuation::MULTIVALUED, meta._listValuation);
  BOOST_CHECK_EQUAL(std::string("C"), irs::locale_utils::name(meta._locale));
  BOOST_CHECK_EQUAL(1, meta._tokenizers.size());
  BOOST_CHECK_EQUAL("identity", meta._tokenizers.begin()->first);
  BOOST_CHECK_EQUAL("", meta._tokenizers.begin()->second.first);
  BOOST_CHECK_EQUAL(false, !meta._tokenizers.begin()->second.second);
}

BOOST_AUTO_TEST_CASE(test_readCustomizedValues) {
  std::unordered_set<std::string> expectedFields = { "a", "b", "c" };
  std::unordered_set<std::string> expectedOverrides = { "default", "all", "some", "none" };
  std::unordered_set<std::string> expectedTokenizers = { "empty", "identity" };
  arangodb::iresearch::IResearchLinkMeta meta;
  std::string tmpString;

  {
    auto json = arangodb::velocypack::Parser::fromJson("{ \"listValuation\": \"invalid\" }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), tmpString));
  }

  {
    auto json = arangodb::velocypack::Parser::fromJson("{ \
      \"boost\": 10, \
      \"depth\": 20, \
      \"fields\": { \
        \"a\": {}, \
        \"b\": {}, \
        \"c\": { \
          \"fields\": { \
            \"default\": { \"boost\": 1, \"depth\": 0, \"fields\": {}, \"listValuation\": \"multivalued\", \"locale\": \"C\", \"tokenizers\": { \"identity\": [\"\"] } }, \
            \"all\": { \"boost\": 11, \"depth\": 21, \"fields\": {\"d\": {}, \"e\": {}}, \"listValuation\": \"ignored\", \"locale\": \"en_US.UTF-8\", \"tokenizers\": { \"empty\": [\"en\"] } }, \
            \"some\": { \"boost\": 12, \"listValuation\": \"ordered\" }, \
            \"none\": {} \
          } \
        } \
      }, \
      \"listValuation\": \"ignored\", \
      \"locale\": \"ru_RU.KOI8-R\", \
      \"tokenizers\": { \"empty\": [\"en\"], \"identity\": [\"\"] } \
    }");
    BOOST_REQUIRE_EQUAL(true, meta.init(json->slice(), tmpString));
    BOOST_CHECK_EQUAL(10., meta._boost);
    BOOST_CHECK_EQUAL(20, meta._depth);
    BOOST_CHECK_EQUAL(3, meta._fields.size());

    for (auto& field: meta._fields) {
      BOOST_CHECK_EQUAL(1, expectedFields.erase(field.first));

      for (auto& fieldOverride: field.second._fields) {
        auto& actual = fieldOverride.second;

        BOOST_CHECK_EQUAL(1, expectedOverrides.erase(fieldOverride.first));

        if ("default" == fieldOverride.first) {
          BOOST_CHECK_EQUAL(1., actual._boost);
          BOOST_CHECK_EQUAL(0, actual._depth);
          BOOST_CHECK_EQUAL(true, actual._fields.empty());
          BOOST_CHECK_EQUAL(arangodb::iresearch::ListValuation::MULTIVALUED, actual._listValuation);
          BOOST_CHECK_EQUAL(std::string("C"), iresearch::locale_utils::name(actual._locale));
          BOOST_CHECK_EQUAL(1, actual._tokenizers.size());
          BOOST_CHECK_EQUAL("identity", actual._tokenizers.begin()->first);
          BOOST_CHECK_EQUAL("", actual._tokenizers.begin()->second.first);
          BOOST_CHECK_EQUAL(false, !actual._tokenizers.begin()->second.second);
        } else if ("all" == fieldOverride.first) {
          BOOST_CHECK_EQUAL(11., actual._boost);
          BOOST_CHECK_EQUAL(21, actual._depth);
          BOOST_CHECK_EQUAL(2, actual._fields.size());
          BOOST_CHECK_EQUAL(true, actual._fields.find("d") != actual._fields.end());
          BOOST_CHECK_EQUAL(true, actual._fields.find("e") != actual._fields.end());
          BOOST_CHECK_EQUAL(arangodb::iresearch::ListValuation::IGNORED, actual._listValuation);
          BOOST_CHECK_EQUAL(std::string("en_US.UTF-8"), irs::locale_utils::name(actual._locale));
          BOOST_CHECK_EQUAL(1, actual._tokenizers.size());
          BOOST_CHECK_EQUAL("empty", actual._tokenizers.begin()->first);
          BOOST_CHECK_EQUAL("en", actual._tokenizers.begin()->second.first);
          BOOST_CHECK_EQUAL(false, !actual._tokenizers.begin()->second.second);
        } else if ("some" == fieldOverride.first) {
          BOOST_CHECK_EQUAL(12., actual._boost);
          BOOST_CHECK_EQUAL(20, actual._depth); // inherited
          BOOST_CHECK_EQUAL(true, actual._fields.empty()); // not inherited
          BOOST_CHECK_EQUAL(arangodb::iresearch::ListValuation::ORDERED, actual._listValuation);
          BOOST_CHECK_EQUAL(std::string("ru_RU.UTF-8"), irs::locale_utils::name(actual._locale)); // inherited
          BOOST_CHECK_EQUAL(2, actual._tokenizers.size());
          auto itr = actual._tokenizers.begin();
          BOOST_CHECK_EQUAL("empty", itr->first);
          BOOST_CHECK_EQUAL("en", itr->second.first);
          BOOST_CHECK_EQUAL(false, !itr->second.second);
          ++itr;
          BOOST_CHECK_EQUAL("identity", itr->first);
          BOOST_CHECK_EQUAL("", itr->second.first);
          BOOST_CHECK_EQUAL(false, !itr->second.second);
        } else if ("none" == fieldOverride.first) {
          BOOST_CHECK_EQUAL(10., actual._boost); // inherited
          BOOST_CHECK_EQUAL(20, actual._depth); // inherited
          BOOST_CHECK_EQUAL(true, actual._fields.empty()); // not inherited
          BOOST_CHECK_EQUAL(arangodb::iresearch::ListValuation::IGNORED, actual._listValuation); // inherited
          BOOST_CHECK_EQUAL(std::string("ru_RU.UTF-8"), irs::locale_utils::name(actual._locale)); // inherited
          auto itr = actual._tokenizers.begin();
          BOOST_CHECK_EQUAL("empty", itr->first);
          BOOST_CHECK_EQUAL("en", itr->second.first);
          BOOST_CHECK_EQUAL(false, !itr->second.second);
          ++itr;
          BOOST_CHECK_EQUAL("identity", itr->first);
          BOOST_CHECK_EQUAL("", itr->second.first);
          BOOST_CHECK_EQUAL(false, !itr->second.second);
        }
      }
    }

    BOOST_CHECK_EQUAL(true, expectedOverrides.empty());
    BOOST_CHECK_EQUAL(true, expectedFields.empty());
    BOOST_CHECK_EQUAL(arangodb::iresearch::ListValuation::IGNORED, meta._listValuation);
    BOOST_CHECK_EQUAL(std::string("ru_RU.UTF-8"), irs::locale_utils::name(meta._locale));
    auto itr = meta._tokenizers.begin();
    BOOST_CHECK_EQUAL("empty", itr->first);
    BOOST_CHECK_EQUAL("en", itr->second.first);
    BOOST_CHECK_EQUAL(false, !itr->second.second);
    ++itr;
    BOOST_CHECK_EQUAL("identity", itr->first);
    BOOST_CHECK_EQUAL("", itr->second.first);
    BOOST_CHECK_EQUAL(false, !itr->second.second);
  }
}

BOOST_AUTO_TEST_CASE(test_writeDefaults) {
  arangodb::iresearch::IResearchLinkMeta meta;
  arangodb::velocypack::Builder builder;
  arangodb::velocypack::Slice tmpSlice;

  BOOST_REQUIRE_EQUAL(true, meta.json(builder));

  auto slice = builder.slice();

  BOOST_CHECK_EQUAL(6, slice.length());
  tmpSlice = slice.get("boost");
  BOOST_CHECK_EQUAL(true, tmpSlice.isNumber() && 1. == tmpSlice.getDouble());
  tmpSlice = slice.get("depth");
  BOOST_CHECK_EQUAL(true, tmpSlice.isUInt() && std::numeric_limits<size_t>::max() == tmpSlice.getUInt());
  tmpSlice = slice.get("fields");
  BOOST_CHECK_EQUAL(true, tmpSlice.isObject() && 0 == tmpSlice.length());
  tmpSlice = slice.get("listValuation");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("multivalued") == tmpSlice.copyString());
  tmpSlice = slice.get("locale");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("C") == tmpSlice.copyString());
  tmpSlice = slice.get("tokenizers");
  BOOST_CHECK_EQUAL(
    true,
    tmpSlice.isObject() &&
    1 == tmpSlice.length() &&
    tmpSlice.keyAt(0).isString() &&
    std::string("identity") == tmpSlice.keyAt(0).copyString() &&
    tmpSlice.valueAt(0).isArray() &&
    1 == tmpSlice.valueAt(0).length() &&
    tmpSlice.valueAt(0).at(0).isString() &&
    std::string("") == tmpSlice.valueAt(0).at(0).copyString()
  );
}

BOOST_AUTO_TEST_CASE(test_writeCustomizedValues) {
  arangodb::iresearch::IResearchLinkMeta meta;

  meta._boost = 10.;
  meta._depth = 20;
  meta._listValuation = arangodb::iresearch::ListValuation::IGNORED;
  meta._locale = irs::locale_utils::locale("en_UK.UTF-8");
  meta._tokenizers.clear();
  meta._tokenizers.emplace(
    std::piecewise_construct,
    std::forward_as_tuple("identity"),
    std::forward_as_tuple("", nullptr)
  );
  meta._tokenizers.emplace(
    std::piecewise_construct,
    std::forward_as_tuple("empty"),
    std::forward_as_tuple("en", nullptr)
  );
  meta._fields["a"] = meta; // copy from meta
  meta._fields["a"]._fields.clear(); // do not inherit fields to match jSon inheritance
  meta._fields["b"] = meta; // copy from meta
  meta._fields["b"]._fields.clear(); // do not inherit fields to match jSon inheritance
  meta._fields["c"] = meta; // copy from meta
  meta._fields["c"]._fields.clear(); // do not inherit fields to match jSon inheritance
  meta._fields["c"]._fields["default"]; // default values
  meta._fields["c"]._fields["all"]; // will override values below
  meta._fields["c"]._fields["some"] = meta._fields["c"]; // initialize with parent, override below
  meta._fields["c"]._fields["none"] = meta._fields["c"]; // initialize with parent

  auto& overrideDefault = meta._fields["c"]._fields["default"];
  auto& overrideAll = meta._fields["c"]._fields["all"];
  auto& overrideSome = meta._fields["c"]._fields["some"];
  auto& overrideNone = meta._fields["c"]._fields["none"];

  overrideAll._boost = 11.;
  overrideAll._depth = 21;
  overrideAll._fields.clear(); // do not inherit fields to match jSon inheritance
  overrideAll._fields["x"] = std::move(arangodb::iresearch::IResearchLinkMeta());
  overrideAll._fields["y"] = std::move(arangodb::iresearch::IResearchLinkMeta());
  overrideAll._listValuation = arangodb::iresearch::ListValuation::ORDERED;
  overrideAll._locale = irs::locale_utils::locale("en_US.UTF-8");
  overrideAll._tokenizers.clear();
  overrideAll._tokenizers.emplace(
    std::piecewise_construct,
    std::forward_as_tuple("empty"),
    std::forward_as_tuple("en", nullptr)
  );
  overrideSome._depth = 22;
  overrideSome._fields.clear(); // do not inherit fields to match jSon inheritance
  overrideSome._listValuation = arangodb::iresearch::ListValuation::MULTIVALUED;
  overrideNone._fields.clear(); // do not inherit fields to match jSon inheritance

  std::unordered_set<std::string> expectedFields = { "a", "b", "c" };
  std::unordered_set<std::string> expectedOverrides = { "default", "all", "some", "none" };
  std::unordered_set<std::string> expectedTokenizers = { "empty", "identity" };
  arangodb::velocypack::Builder builder;
  arangodb::velocypack::Slice tmpSlice;

  BOOST_REQUIRE_EQUAL(true, meta.json(builder));

  auto slice = builder.slice();

  BOOST_CHECK_EQUAL(6, slice.length());
  tmpSlice = slice.get("boost");
  BOOST_CHECK_EQUAL(true, tmpSlice.isNumber() && 10. == tmpSlice.getDouble());
  tmpSlice = slice.get("depth");
  BOOST_CHECK_EQUAL(true, tmpSlice.isUInt() && 20. == tmpSlice.getUInt());
  tmpSlice = slice.get("fields");
  BOOST_CHECK_EQUAL(true, tmpSlice.isObject() && 3 == tmpSlice.length());

  for (arangodb::velocypack::ObjectIterator itr(tmpSlice); itr.valid(); ++itr) {
    auto key = itr.key();
    auto value = itr.value();
    BOOST_CHECK_EQUAL(true, key.isString() && 1 == expectedFields.erase(key.copyString()));
    BOOST_CHECK_EQUAL(true, value.isObject());

    if (!value.hasKey("fields")) {
      continue;
    }

    tmpSlice = value.get("fields");

    for (arangodb::velocypack::ObjectIterator overrideItr(tmpSlice); overrideItr.valid(); ++overrideItr) {
      auto fieldOverride = overrideItr.key();
      auto sliceOverride = overrideItr.value();
      BOOST_CHECK_EQUAL(true, fieldOverride.isString() && sliceOverride.isObject());
      BOOST_CHECK_EQUAL(1, expectedOverrides.erase(fieldOverride.copyString()));

      if ("default" == fieldOverride.copyString()) {
        BOOST_CHECK_EQUAL(5, sliceOverride.length());
        tmpSlice = sliceOverride.get("boost");
        BOOST_CHECK_EQUAL(true, tmpSlice.isNumber() && 1. == tmpSlice.getDouble());
        tmpSlice = sliceOverride.get("depth");
        BOOST_CHECK_EQUAL(true, tmpSlice.isUInt() && std::numeric_limits<size_t>::max() == tmpSlice.getUInt());
        tmpSlice = sliceOverride.get("listValuation");
        BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("multivalued") == tmpSlice.copyString());
        tmpSlice = sliceOverride.get("locale");
        BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("C") == tmpSlice.copyString());
        tmpSlice = sliceOverride.get("tokenizers");
        BOOST_CHECK_EQUAL(
          true,
          tmpSlice.isObject() &&
          1 == tmpSlice.length() &&
          tmpSlice.keyAt(0).isString() &&
          std::string("identity") == tmpSlice.keyAt(0).copyString() &&
          tmpSlice.valueAt(0).isArray() &&
          1 == tmpSlice.valueAt(0).length() &&
          tmpSlice.valueAt(0).at(0).isString() &&
          std::string("") == tmpSlice.valueAt(0).at(0).copyString()
        );
      } else if ("all" == fieldOverride.copyString()) {
        std::unordered_set<std::string> expectedFields = { "x", "y" };
        BOOST_CHECK_EQUAL(6, sliceOverride.length());
        tmpSlice = sliceOverride.get("boost");
        BOOST_CHECK_EQUAL(true, tmpSlice.isNumber() && 11. == tmpSlice.getDouble());
        tmpSlice = sliceOverride.get("depth");
        BOOST_CHECK_EQUAL(true, tmpSlice.isUInt() && 21 == tmpSlice.getUInt());
        tmpSlice = sliceOverride.get("fields");
        BOOST_CHECK_EQUAL(true, tmpSlice.isObject() && 2 == tmpSlice.length());
        for (arangodb::velocypack::ObjectIterator overrideFieldItr(tmpSlice); overrideFieldItr.valid(); ++overrideFieldItr) {
          BOOST_CHECK_EQUAL(true, overrideFieldItr.key().isString() && 1 == expectedFields.erase(overrideFieldItr.key().copyString()));
        }
        BOOST_CHECK_EQUAL(true, expectedFields.empty());
        tmpSlice = sliceOverride.get("listValuation");
        BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("ordered") == tmpSlice.copyString());
        tmpSlice = sliceOverride.get("locale");
        BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("en_US.UTF-8") == tmpSlice.copyString());
        tmpSlice = sliceOverride.get("tokenizers");
        BOOST_CHECK_EQUAL(
          true,
          tmpSlice.isObject() &&
          1 == tmpSlice.length() &&
          tmpSlice.keyAt(0).isString() &&
          std::string("empty") == tmpSlice.keyAt(0).copyString() &&
          tmpSlice.valueAt(0).isArray() &&
          1 == tmpSlice.valueAt(0).length() &&
          tmpSlice.valueAt(0).at(0).isString() &&
          std::string("en") == tmpSlice.valueAt(0).at(0).copyString()
        );
      } else if ("some" == fieldOverride.copyString()) {
        BOOST_CHECK_EQUAL(2, sliceOverride.length());
        tmpSlice = sliceOverride.get("depth");
        BOOST_CHECK_EQUAL(true, tmpSlice.isUInt() && 22 == tmpSlice.getUInt());
        tmpSlice = sliceOverride.get("listValuation");
        BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("multivalued") == tmpSlice.copyString());
      } else if ("none" == fieldOverride.copyString()) {
        BOOST_CHECK_EQUAL(0, sliceOverride.length());
      }
    }
  }

  BOOST_CHECK_EQUAL(true, expectedOverrides.empty());
  BOOST_CHECK_EQUAL(true, expectedFields.empty());
  tmpSlice = slice.get("listValuation");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("ignored") == tmpSlice.copyString());
  tmpSlice = slice.get("locale");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("en_UK.UTF-8") == tmpSlice.copyString());
  tmpSlice = slice.get("tokenizers");
  BOOST_CHECK_EQUAL(true, tmpSlice.isObject() && 2 == tmpSlice.length());

  for (arangodb::velocypack::ObjectIterator tokenizersItr(tmpSlice); tokenizersItr.valid(); ++tokenizersItr) {
    auto key = tokenizersItr.key();
    auto value = tokenizersItr.value();
    BOOST_CHECK_EQUAL(true, key.isString() && 1 == expectedTokenizers.erase(key.copyString()));

    auto args = key.copyString() == "empty" ? "en" : "";

    BOOST_CHECK_EQUAL(
      true,
      value.isArray() &&
      1 == value.length() &&
      value.at(0).isString() &&
      std::string(args) == value.at(0).copyString()
    );
  }

  BOOST_CHECK_EQUAL(true, expectedTokenizers.empty());
}

BOOST_AUTO_TEST_CASE(test_readMaskAll) {
  arangodb::iresearch::IResearchLinkMeta meta;
  arangodb::iresearch::IResearchLinkMeta::Mask mask;
  std::string tmpString;

  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"boost\": 10, \
    \"depth\": 20, \
    \"fields\": { \"a\": {} }, \
    \"listValuation\": \"ignored\", \
    \"locale\": \"ru_RU.KOI8-R\", \
    \"tokenizers\": {} \
  }");
  BOOST_REQUIRE_EQUAL(true, meta.init(json->slice(), tmpString, arangodb::iresearch::IResearchLinkMeta::DEFAULT(), &mask));
  BOOST_CHECK_EQUAL(true, mask._boost);
  BOOST_CHECK_EQUAL(true, mask._depth);
  BOOST_CHECK_EQUAL(true, mask._fields);
  BOOST_CHECK_EQUAL(true, mask._listValuation);
  BOOST_CHECK_EQUAL(true, mask._locale);
  BOOST_CHECK_EQUAL(true, mask._tokenizers);
}

BOOST_AUTO_TEST_CASE(test_readMaskNone) {
  arangodb::iresearch::IResearchLinkMeta meta;
  arangodb::iresearch::IResearchLinkMeta::Mask mask;
  std::string tmpString;

  auto json = arangodb::velocypack::Parser::fromJson("{}");
  BOOST_REQUIRE_EQUAL(true, meta.init(json->slice(), tmpString, arangodb::iresearch::IResearchLinkMeta::DEFAULT(), &mask));
  BOOST_CHECK_EQUAL(false, mask._boost);
  BOOST_CHECK_EQUAL(false, mask._depth);
  BOOST_CHECK_EQUAL(false, mask._fields);
  BOOST_CHECK_EQUAL(false, mask._listValuation);
  BOOST_CHECK_EQUAL(false, mask._locale);
  BOOST_CHECK_EQUAL(false, mask._tokenizers);
}

BOOST_AUTO_TEST_CASE(test_writeMaskAll) {
  arangodb::iresearch::IResearchLinkMeta meta;
  arangodb::iresearch::IResearchLinkMeta::Mask mask(true);
  arangodb::velocypack::Builder builder;

  BOOST_REQUIRE_EQUAL(true, meta.json(builder, nullptr, &mask));

  auto slice = builder.slice();

  BOOST_CHECK_EQUAL(6, slice.length());
  BOOST_CHECK_EQUAL(true, slice.hasKey("boost"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("depth"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("fields"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("listValuation"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("locale"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("tokenizers"));
}

BOOST_AUTO_TEST_CASE(test_writeMaskNone) {
  arangodb::iresearch::IResearchLinkMeta meta;
  arangodb::iresearch::IResearchLinkMeta::Mask mask(false);
  arangodb::velocypack::Builder builder;

  BOOST_REQUIRE_EQUAL(true, meta.json(builder, nullptr, &mask));

  auto slice = builder.slice();

  BOOST_CHECK_EQUAL(0, slice.length());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief generate tests
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_SUITE_END()

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
