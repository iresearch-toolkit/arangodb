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

#include "utils/locale_utils.hpp"

#include "IResearch/IResearchViewMeta.h"
#include "velocypack/Iterator.h"
#include "velocypack/Parser.h"

NS_LOCAL

irs::iql::order_functions defaultScorers;
const irs::iql::order_function::contextual_function_t invalidScorerFn;
const irs::iql::order_function invalidScorer(invalidScorerFn);

NS_END

// -----------------------------------------------------------------------------
// --SECTION--                                                 setup / tear-down
// -----------------------------------------------------------------------------

struct IResearchViewMetaSetup {
  IResearchViewMetaSetup() {
    BOOST_TEST_MESSAGE("setup IResearchViewMeta");

    auto& scorers = defaultScorers;
    auto defaultScorersCollector = [&scorers](
      std::string const& name, irs::flags const& features, irs::iql::order_function const& builder, bool isDefault
      )->bool {
      if (isDefault) {
        scorers.emplace(name, builder); // default scorers always present
      };
      return true;
    };

    defaultScorers.clear();
    //SimilarityDocumentAdapter::visitScorers(defaultScorersCollector); FIXME TODO

  }

  ~IResearchViewMetaSetup() {
    BOOST_TEST_MESSAGE("tear-down IResearchViewMeta");

    defaultScorers.clear();
  }
};

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief setup
////////////////////////////////////////////////////////////////////////////////

BOOST_FIXTURE_TEST_SUITE(IResearchViewMetaTest, IResearchViewMetaSetup)

BOOST_AUTO_TEST_CASE(test_defaults) {
  arangodb::iresearch::IResearchViewMeta meta;

  BOOST_CHECK_EQUAL(true, meta._collections.empty());
  BOOST_CHECK_EQUAL(10, meta._commitBulk._cleanupIntervalStep);
  BOOST_CHECK_EQUAL(10000, meta._commitBulk._commitIntervalBatchSize);

  for (size_t i = 0, count = arangodb::iresearch::ConsolidationPolicy::eLast; i < count; ++i) {
    BOOST_CHECK_EQUAL(10, meta._commitBulk._consolidate[i]._intervalStep);
    BOOST_CHECK_EQUAL(0.85f, meta._commitBulk._consolidate[i]._threshold);
  }

  BOOST_CHECK_EQUAL(10, meta._commitItem._cleanupIntervalStep);
  BOOST_CHECK_EQUAL(60 * 1000, meta._commitItem._commitIntervalMsec);

  for (size_t i = 0, count = arangodb::iresearch::ConsolidationPolicy::eLast; i < count; ++i) {
    BOOST_CHECK_EQUAL(10, meta._commitItem._consolidate[i]._intervalStep);
    BOOST_CHECK_EQUAL(0.85f, meta._commitItem._consolidate[i]._threshold);
  }

  BOOST_CHECK_EQUAL(std::string(""), meta._dataPath);
  BOOST_CHECK_EQUAL(0, meta._iid);
  BOOST_CHECK_EQUAL(std::string("C"), irs::locale_utils::name(meta._locale));
  BOOST_CHECK_EQUAL(std::string(""), meta._name);
  BOOST_CHECK_EQUAL(std::string("."), meta._nestingDelimiter);
  BOOST_CHECK_EQUAL(std::string("["), meta._nestingListOffsetPrefix);
  BOOST_CHECK_EQUAL(std::string("]"), meta._nestingListOffsetSuffix);
  BOOST_CHECK_EQUAL(true, defaultScorers == meta._scorers);
  BOOST_CHECK_EQUAL(5, meta._threadsMaxIdle);
  BOOST_CHECK_EQUAL(5, meta._threadsMaxTotal);
}

BOOST_AUTO_TEST_CASE(test_inheritDefaults) {
  arangodb::iresearch::IResearchViewMeta defaults;
  arangodb::iresearch::IResearchViewMeta meta;
  std::string tmpString;

  defaults._collections.insert(42);
  defaults._commitBulk._cleanupIntervalStep = 123;
  defaults._commitBulk._commitIntervalBatchSize = 321;
  defaults._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep = 10;
  defaults._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold = .1f;
  defaults._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep = 15;
  defaults._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold = .15f;
  defaults._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep = 20;
  defaults._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold = .2f;
  defaults._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep = 30;
  defaults._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold = .3f;
  defaults._commitItem._cleanupIntervalStep = 654;
  defaults._commitItem._commitIntervalMsec = 456;
  defaults._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep = 101;
  defaults._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold = .11f;
  defaults._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep = 151;
  defaults._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold = .151f;
  defaults._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep = 201;
  defaults._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold = .21f;
  defaults._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep = 301;
  defaults._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold = .31f;
  defaults._dataPath = "path";
  defaults._iid = 10;
  defaults._locale = irs::locale_utils::locale("ru");
  defaults._nestingDelimiter = ":";
  defaults._nestingListOffsetPrefix = "<";
  defaults._nestingListOffsetSuffix = ">";
  defaults._scorers.emplace("testScorer", invalidScorer);
  defaults._threadsMaxIdle = 8;
  defaults._threadsMaxTotal = 16;

  // test missing required fields
  {
    auto json = arangodb::velocypack::Parser::fromJson("{}");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), tmpString));
  }

  {
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\" }");
    BOOST_REQUIRE_EQUAL(true, meta.init(json->slice(), tmpString, defaults));
    BOOST_CHECK_EQUAL(1, meta._collections.size());
    BOOST_CHECK_EQUAL(42, *(meta._collections.begin()));
    BOOST_CHECK_EQUAL(123, meta._commitBulk._cleanupIntervalStep);
    BOOST_CHECK_EQUAL(321, meta._commitBulk._commitIntervalBatchSize);
    BOOST_CHECK_EQUAL(10, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep);
    BOOST_CHECK_EQUAL(.1f, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold);
    BOOST_CHECK_EQUAL(15, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep);
    BOOST_CHECK_EQUAL(.15f, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold);
    BOOST_CHECK_EQUAL(20, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep);
    BOOST_CHECK_EQUAL(.2f, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold);
    BOOST_CHECK_EQUAL(30, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep);
    BOOST_CHECK_EQUAL(.3f, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold);
    BOOST_CHECK_EQUAL(654, meta._commitItem._cleanupIntervalStep);
    BOOST_CHECK_EQUAL(456, meta._commitItem._commitIntervalMsec);
    BOOST_CHECK_EQUAL(101, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep);
    BOOST_CHECK_EQUAL(.11f, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold);
    BOOST_CHECK_EQUAL(151, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep);
    BOOST_CHECK_EQUAL(.151f, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold);
    BOOST_CHECK_EQUAL(201, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep);
    BOOST_CHECK_EQUAL(.21f, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold);
    BOOST_CHECK_EQUAL(301, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep);
    BOOST_CHECK_EQUAL(.31f, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold);
    BOOST_CHECK_EQUAL(std::string("path"), meta._dataPath);
    BOOST_CHECK_EQUAL(10, meta._iid);
    BOOST_CHECK_EQUAL(std::string("ru"), irs::locale_utils::name(meta._locale));
    BOOST_CHECK_EQUAL(std::string("abc"), meta._name);
    BOOST_CHECK_EQUAL(std::string(":"), meta._nestingDelimiter);
    BOOST_CHECK_EQUAL(std::string("<"), meta._nestingListOffsetPrefix);
    BOOST_CHECK_EQUAL(std::string(">"), meta._nestingListOffsetSuffix);
    BOOST_CHECK_EQUAL(defaultScorers.size() + 1, meta._scorers.size());
    BOOST_CHECK_EQUAL(true, meta._scorers.find("testScorer") != meta._scorers.end());
    BOOST_CHECK_EQUAL(8, meta._threadsMaxIdle);
    BOOST_CHECK_EQUAL(16, meta._threadsMaxTotal);
  }
}

BOOST_AUTO_TEST_CASE(test_readDefaults) {
  arangodb::iresearch::IResearchViewMeta meta;
  std::string tmpString;

  // test missing required fields
  {
    auto json = arangodb::velocypack::Parser::fromJson("{}");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), tmpString));
  }

  {
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\" }");
    BOOST_REQUIRE_EQUAL(true, meta.init(json->slice(), tmpString));
    BOOST_CHECK_EQUAL(true, meta._collections.empty());
    BOOST_CHECK_EQUAL(10, meta._commitBulk._cleanupIntervalStep);
    BOOST_CHECK_EQUAL(10000, meta._commitBulk._commitIntervalBatchSize);

    for (size_t i = 0, count = arangodb::iresearch::ConsolidationPolicy::eLast; i < count; ++i) {
      BOOST_CHECK_EQUAL(10, meta._commitBulk._consolidate[i]._intervalStep);
      BOOST_CHECK_EQUAL(0.85f, meta._commitBulk._consolidate[i]._threshold);
    }

    BOOST_CHECK_EQUAL(10, meta._commitItem._cleanupIntervalStep);
    BOOST_CHECK_EQUAL(60 * 1000, meta._commitItem._commitIntervalMsec);

    for (size_t i = 0, count = arangodb::iresearch::ConsolidationPolicy::eLast; i < count; ++i) {
      BOOST_CHECK_EQUAL(10, meta._commitItem._consolidate[i]._intervalStep);
      BOOST_CHECK_EQUAL(0.85f, meta._commitItem._consolidate[i]._threshold);
    }

    BOOST_CHECK_EQUAL(std::string(""), meta._dataPath);
    BOOST_CHECK_EQUAL(0, meta._iid);
    BOOST_CHECK_EQUAL(std::string("C"), irs::locale_utils::name(meta._locale));
    BOOST_CHECK_EQUAL(std::string("abc"), meta._name);
    BOOST_CHECK_EQUAL(std::string("."), meta._nestingDelimiter);
    BOOST_CHECK_EQUAL(std::string("["), meta._nestingListOffsetPrefix);
    BOOST_CHECK_EQUAL(std::string("]"), meta._nestingListOffsetSuffix);
    BOOST_CHECK_EQUAL(true, defaultScorers == meta._scorers);
    BOOST_CHECK_EQUAL(5, meta._threadsMaxIdle);
    BOOST_CHECK_EQUAL(5, meta._threadsMaxTotal);
  }
}

BOOST_AUTO_TEST_CASE(test_readCustomizedValues) {
  std::unordered_set<TRI_voc_cid_t> expectedCollections = { 42 };
  std::unordered_set<std::string> expectedScorers = { "tfidf" };
  arangodb::iresearch::IResearchViewMeta meta;

  // .............................................................................
  // test invalid value
  // .............................................................................

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"collections\": \"invalid\" }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("collections"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitBulk\": \"invalid\" }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitBulk"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitBulk\": { \"commitIntervalBatchSize\": 0.5 } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitBulk=>commitIntervalBatchSize"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitBulk\": { \"cleanupIntervalStep\": 0.5 } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitBulk=>cleanupIntervalStep"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitBulk\": { \"consolidate\": \"invalid\" } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitBulk=>consolidate"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitBulk\": { \"consolidate\": { \"invalid\": \"abc\" } } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitBulk=>consolidate=>invalid"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitBulk\": { \"consolidate\": { \"invalid\": 0.5 } } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitBulk=>consolidate=>invalid"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitBulk\": { \"consolidate\": { \"bytes\": { \"intervalStep\": 0.5, \"threshold\": 1 } } } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitBulk=>consolidate=>bytes=>intervalStep"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitBulk\": { \"consolidate\": { \"bytes\": { \"threshold\": -0.5 } } } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitBulk=>consolidate=>bytes=>threshold"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitBulk\": { \"consolidate\": { \"bytes\": { \"threshold\": 1.5 } } } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitBulk=>consolidate=>bytes=>threshold"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitItem\": \"invalid\" }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitItem"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitItem\": { \"commitIntervalMsec\": 0.5 } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitItem=>commitIntervalMsec"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitItem\": { \"cleanupIntervalStep\": 0.5 } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitItem=>cleanupIntervalStep"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitItem\": { \"consolidate\": \"invalid\" } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitItem=>consolidate"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitItem\": { \"consolidate\": { \"invalid\": \"abc\" } } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitItem=>consolidate=>invalid"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitItem\": { \"consolidate\": { \"invalid\": 0.5 } } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitItem=>consolidate=>invalid"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitItem\": { \"consolidate\": { \"bytes\": { \"intervalStep\": 0.5, \"threshold\": 1 } } } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitItem=>consolidate=>bytes=>intervalStep"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitItem\": { \"consolidate\": { \"bytes\": { \"threshold\": -0.5 } } } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitItem=>consolidate=>bytes=>threshold"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"commitItem\": { \"consolidate\": { \"bytes\": { \"threshold\": 1.5 } } } }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("commitItem=>consolidate=>bytes=>threshold"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"threadsMaxIdle\": 0.5 }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("threadsMaxIdle"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"threadsMaxTotal\": 0.5 }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("threadsMaxTotal"), errorField);
  }

  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\", \"threadsMaxTotal\": 0 }");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(std::string("threadsMaxTotal"), errorField);
  }

  // .............................................................................
  // test valid value
  // .............................................................................

  // test disabled consolidate (all empty)
  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \
      \"commitBulk\": { \"consolidate\": {} }, \
      \"commitItem\": { \"consolidate\": {} }, \
      \"name\": \"abc\" \
    }");
    BOOST_REQUIRE_EQUAL(true, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(0, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep);
    BOOST_CHECK_EQUAL(std::numeric_limits<float>::infinity(), meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep);
    BOOST_CHECK_EQUAL(std::numeric_limits<float>::infinity(), meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep);
    BOOST_CHECK_EQUAL(std::numeric_limits<float>::infinity(), meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep);
    BOOST_CHECK_EQUAL(std::numeric_limits<float>::infinity(), meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep);
    BOOST_CHECK_EQUAL(std::numeric_limits<float>::infinity(), meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep);
    BOOST_CHECK_EQUAL(std::numeric_limits<float>::infinity(), meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep);
    BOOST_CHECK_EQUAL(std::numeric_limits<float>::infinity(), meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep);
    BOOST_CHECK_EQUAL(std::numeric_limits<float>::infinity(), meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold);
    BOOST_CHECK_EQUAL(std::string("abc"), meta._name);
  }

  // test disabled consolidate (implicit disable due to value)
  {
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{ \
      \"commitBulk\": { \"consolidate\": { \"bytes\": { \"intervalStep\": 0, \"threshold\": 0.1 }, \"count\": { \"intervalStep\": 0 } } }, \
      \"commitItem\": { \"consolidate\": { \"bytes_accum\": { \"intervalStep\": 0, \"threshold\": 0.2 }, \"fill\": { \"intervalStep\": 0 } } }, \
      \"name\": \"abc\" \
    }");
    BOOST_REQUIRE_EQUAL(true, meta.init(json->slice(), errorField));
    BOOST_CHECK_EQUAL(0, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep);
    BOOST_CHECK_EQUAL(0.1f, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep);
    BOOST_CHECK_EQUAL(std::numeric_limits<float>::infinity(), meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep);
    BOOST_CHECK_EQUAL(0.85f, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep);
    BOOST_CHECK_EQUAL(std::numeric_limits<float>::infinity(), meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep);
    BOOST_CHECK_EQUAL(std::numeric_limits<float>::infinity(), meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep);
    BOOST_CHECK_EQUAL(0.2f, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep);
    BOOST_CHECK_EQUAL(std::numeric_limits<float>::infinity(), meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold);
    BOOST_CHECK_EQUAL(0, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep);
    BOOST_CHECK_EQUAL(0.85f, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold);
    BOOST_CHECK_EQUAL(std::string("abc"), meta._name);
  }

  // test all parameters set to custom values
  std::string errorField;
  auto json = arangodb::velocypack::Parser::fromJson("{ \
        \"collections\": [ 42 ], \
        \"commitBulk\": { \"commitIntervalBatchSize\": 321, \"cleanupIntervalStep\": 123, \"consolidate\": { \"bytes\": { \"intervalStep\": 100, \"threshold\": 0.1 }, \"bytes_accum\": { \"intervalStep\": 150, \"threshold\": 0.15 }, \"count\": { \"intervalStep\": 200 }, \"fill\": {} } }, \
        \"commitItem\": { \"commitIntervalMsec\": 456, \"cleanupIntervalStep\": 654, \"consolidate\": { \"bytes\": { \"intervalStep\": 1001, \"threshold\": 0.11 }, \"bytes_accum\": { \"intervalStep\": 1501, \"threshold\": 0.151 }, \"count\": { \"intervalStep\": 2001 }, \"fill\": {} } }, \
        \"id\": 10, \
        \"locale\": \"ru_RU.KOI8-R\", \
        \"name\": \"abc\", \
        \"nestingDelimiter\": \"->\", \
        \"nestingListOffsetPrefix\": \"-{\", \
        \"nestingListOffsetSuffix\": \"}-\", \
        \"dataPath\": \"somepath\", \
        \"scorers\": [ \"tfidf\" ], \
        \"threadsMaxIdle\": 8, \
        \"threadsMaxTotal\": 16 \
    }");
  BOOST_REQUIRE_EQUAL(true, meta.init(json->slice(), errorField));
  BOOST_CHECK_EQUAL(1, meta._collections.size());

  for (auto& collection: meta._collections) {
    BOOST_CHECK_EQUAL(1, expectedCollections.erase(collection));
  }

  BOOST_CHECK_EQUAL(true, expectedCollections.empty());
  BOOST_CHECK_EQUAL(42, *(meta._collections.begin()));
  BOOST_CHECK_EQUAL(123, meta._commitBulk._cleanupIntervalStep);
  BOOST_CHECK_EQUAL(321, meta._commitBulk._commitIntervalBatchSize);
  BOOST_CHECK_EQUAL(100, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep);
  BOOST_CHECK_EQUAL(.1f, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold);
  BOOST_CHECK_EQUAL(150, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep);
  BOOST_CHECK_EQUAL(.15f, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold);
  BOOST_CHECK_EQUAL(200, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep);
  BOOST_CHECK_EQUAL(.85f, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold);
  BOOST_CHECK_EQUAL(10, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep);
  BOOST_CHECK_EQUAL(.85f, meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold);
  BOOST_CHECK_EQUAL(654, meta._commitItem._cleanupIntervalStep);
  BOOST_CHECK_EQUAL(456, meta._commitItem._commitIntervalMsec);
  BOOST_CHECK_EQUAL(1001, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep);
  BOOST_CHECK_EQUAL(.11f, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold);
  BOOST_CHECK_EQUAL(1501, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep);
  BOOST_CHECK_EQUAL(.151f, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold);
  BOOST_CHECK_EQUAL(2001, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep);
  BOOST_CHECK_EQUAL(.85f, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold);
  BOOST_CHECK_EQUAL(10, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep);
  BOOST_CHECK_EQUAL(.85f, meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold);
  BOOST_CHECK_EQUAL(std::string("somepath"), meta._dataPath);
  BOOST_CHECK_EQUAL(10, meta._iid);
  BOOST_CHECK_EQUAL(std::string("ru_RU.UTF-8"), iresearch::locale_utils::name(meta._locale));
  BOOST_CHECK_EQUAL(std::string("abc"), meta._name);
  BOOST_CHECK_EQUAL(std::string("->"), meta._nestingDelimiter);
  BOOST_CHECK_EQUAL(std::string("-{"), meta._nestingListOffsetPrefix);
  BOOST_CHECK_EQUAL(std::string("}-"), meta._nestingListOffsetSuffix);
  BOOST_CHECK_EQUAL(defaultScorers.size() + 1, meta._scorers.size());

  for (auto& scorer: meta._scorers) {
    BOOST_CHECK_EQUAL(true, defaultScorers.find(scorer.first) != defaultScorers.end() || 1 == expectedScorers.erase(scorer.first));
  }

  BOOST_CHECK_EQUAL(true, expectedScorers.empty());
  BOOST_CHECK_EQUAL(true, meta._scorers.find("tfidf") != meta._scorers.end());
  BOOST_CHECK_EQUAL(8, meta._threadsMaxIdle);
  BOOST_CHECK_EQUAL(16, meta._threadsMaxTotal);
}

BOOST_AUTO_TEST_CASE(test_writeDefaults) {
  std::unordered_map<std::string, std::unordered_map<std::string, double>> expectedCommitBulkConsolidate = {
    { "bytes",{ { "intervalStep", 10 },{ "threshold", .85f } } },
    { "bytes_accum",{ { "intervalStep", 10 },{ "threshold", .85f } } },
    { "count",{ { "intervalStep", 10 },{ "threshold", .85f } } },
    { "fill",{ { "intervalStep", 10 },{ "threshold", .85f } } }
  };
  std::unordered_map<std::string, std::unordered_map<std::string, double>> expectedCommitItemConsolidate = {
    { "bytes",{ { "intervalStep", 10 },{ "threshold", .85f } } },
    { "bytes_accum",{ { "intervalStep", 10 },{ "threshold", .85f } } },
    { "count",{ { "intervalStep", 10 },{ "threshold", .85f } } },
    { "fill",{ { "intervalStep", 10 },{ "threshold", .85f } } }
  };
  arangodb::iresearch::IResearchViewMeta meta;
  arangodb::velocypack::Builder builder;
  arangodb::velocypack::Slice tmpSlice;
  arangodb::velocypack::Slice tmpSlice2;

  BOOST_REQUIRE_EQUAL(true, meta.json(arangodb::velocypack::ObjectBuilder(&builder)));

  auto slice = builder.slice();

  BOOST_CHECK_EQUAL(12, slice.length());
  tmpSlice = slice.get("collections");
  BOOST_CHECK_EQUAL(true, tmpSlice.isArray() && 0 == tmpSlice.length());
  tmpSlice = slice.get("commitBulk");
  BOOST_CHECK_EQUAL(true, tmpSlice.isObject() && 3 == tmpSlice.length());
  tmpSlice2 = tmpSlice.get("cleanupIntervalStep");
  BOOST_CHECK_EQUAL(true, tmpSlice2.isUInt() && 10 == tmpSlice2.getUInt());
  tmpSlice2 = tmpSlice.get("commitIntervalBatchSize");
  BOOST_CHECK_EQUAL(true, tmpSlice2.isUInt() && 10000 == tmpSlice2.getUInt());
  tmpSlice2 = tmpSlice.get("consolidate");
  BOOST_CHECK_EQUAL(true, tmpSlice2.isObject() && 4 == tmpSlice2.length());

  for (arangodb::velocypack::ObjectIterator itr(tmpSlice2); itr.valid(); ++itr) {
    auto key = itr.key();
    auto value = itr.value();
    auto& expectedPolicy = expectedCommitBulkConsolidate[key.copyString()];
    BOOST_CHECK_EQUAL(true, key.isString());
    BOOST_CHECK_EQUAL(true, value.isObject() && 2 == value.length());

    for (arangodb::velocypack::ObjectIterator itr2(value); itr2.valid(); ++itr2) {
      auto key2 = itr2.key();
      auto value2 = itr2.value();
      BOOST_CHECK_EQUAL(true, key2.isString() && expectedPolicy[key2.copyString()] == value2.getNumber<double>());
      expectedPolicy.erase(key2.copyString());
    }

    expectedCommitBulkConsolidate.erase(key.copyString());
  }

  BOOST_CHECK_EQUAL(true, expectedCommitBulkConsolidate.empty());
  tmpSlice = slice.get("commitItem");
  BOOST_CHECK_EQUAL(true, tmpSlice.isObject() && 3 == tmpSlice.length());
  tmpSlice2 = tmpSlice.get("cleanupIntervalStep");
  BOOST_CHECK_EQUAL(true, tmpSlice2.isUInt() && 10 == tmpSlice2.getUInt());
  tmpSlice2 = tmpSlice.get("commitIntervalMsec");
  BOOST_CHECK_EQUAL(true, tmpSlice2.isUInt() && 60000 == tmpSlice2.getUInt());
  tmpSlice2 = tmpSlice.get("consolidate");
  BOOST_CHECK_EQUAL(true, tmpSlice2.isObject() && 4 == tmpSlice2.length());

  for (arangodb::velocypack::ObjectIterator itr(tmpSlice2); itr.valid(); ++itr) {
    auto key = itr.key();
    auto value = itr.value();
    auto& expectedPolicy = expectedCommitItemConsolidate[key.copyString()];
    BOOST_CHECK_EQUAL(true, key.isString());
    BOOST_CHECK_EQUAL(true, value.isObject() && 2 == value.length());

    for (arangodb::velocypack::ObjectIterator itr2(value); itr2.valid(); ++itr2) {
      auto key2 = itr2.key();
      auto value2 = itr2.value();
      BOOST_CHECK_EQUAL(true, key2.isString() && expectedPolicy[key2.copyString()] == value2.getNumber<double>());
      expectedPolicy.erase(key2.copyString());
    }

    expectedCommitItemConsolidate.erase(key.copyString());
  }

  BOOST_CHECK_EQUAL(true, expectedCommitItemConsolidate.empty());
  tmpSlice = slice.get("id");
  BOOST_CHECK_EQUAL(true, tmpSlice.isNumber() && 0 == tmpSlice.getUInt());
  tmpSlice = slice.get("locale");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("C") == tmpSlice.copyString());
  tmpSlice = slice.get("name");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("") == tmpSlice.copyString());
  tmpSlice = slice.get("nestingDelimiter");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string(".") == tmpSlice.copyString());
  tmpSlice = slice.get("nestingListOffsetPrefix");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("[") == tmpSlice.copyString());
  tmpSlice = slice.get("nestingListOffsetSuffix");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("]") == tmpSlice.copyString());
  tmpSlice = slice.get("scorers");
  BOOST_CHECK_EQUAL(true, tmpSlice.isArray() && defaultScorers.size() == tmpSlice.length());
  tmpSlice = slice.get("threadsMaxIdle");
  BOOST_CHECK_EQUAL(true, tmpSlice.isNumber() && 5 == tmpSlice.getUInt());
  tmpSlice = slice.get("threadsMaxTotal");
  BOOST_CHECK_EQUAL(true, tmpSlice.isNumber() && 5 == tmpSlice.getUInt());
}

BOOST_AUTO_TEST_CASE(test_writeCustomizedValues) {
  // test disabled consolidate
  {
    arangodb::iresearch::IResearchViewMeta meta;

    meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep = 0;
    meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold = .1f;
    meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep = 0;
    //meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold = <leave as default>
    meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep = 0;
    meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold = std::numeric_limits<float>::infinity();
    meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep = 0;
    //meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold = <leave as default>
    meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep = 0;
    //meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold = <leave as default>
    meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep = 0;
    meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold = std::numeric_limits<float>::infinity();
    meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep = 0;
    //meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold = <leave as default>
    meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep = 0;
    meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold = .2f;

    arangodb::velocypack::Builder builder;
    arangodb::velocypack::Slice tmpSlice;

    BOOST_REQUIRE_EQUAL(true, meta.json(arangodb::velocypack::ObjectBuilder(&builder)));

    auto slice = builder.slice();

    tmpSlice = slice.get("commitBulk");
    BOOST_CHECK_EQUAL(true, tmpSlice.isObject());
    tmpSlice = tmpSlice.get("consolidate");
    BOOST_CHECK_EQUAL(true, tmpSlice.isObject() && 0 == tmpSlice.length());
    tmpSlice = slice.get("commitItem");
    BOOST_CHECK_EQUAL(true, tmpSlice.isObject());
    tmpSlice = tmpSlice.get("consolidate");
    BOOST_CHECK_EQUAL(true, tmpSlice.isObject() && 0 == tmpSlice.length());
  }

  arangodb::iresearch::IResearchViewMeta meta;

  // test all parameters set to custom values
  meta._commitBulk._cleanupIntervalStep = 123;
  meta._commitBulk._commitIntervalBatchSize = 321;
  meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep = 100;
  meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold = .1f;
  meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep = 150;
  meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold = .15f;
  meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep = 200;
  meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold = .2f;
  meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep = 300;
  meta._commitBulk._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold = .3f;
  meta._collections.insert(42);
  meta._collections.insert(52);
  meta._collections.insert(62);
  meta._commitItem._cleanupIntervalStep = 654;
  meta._commitItem._commitIntervalMsec = 456;
  meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._intervalStep = 101;
  meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES]._threshold = .11f;
  meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._intervalStep = 151;
  meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::BYTES_ACCUM]._threshold = .151f;
  meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._intervalStep = 201;
  meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::COUNT]._threshold = .21f;
  meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._intervalStep = 301;
  meta._commitItem._consolidate[arangodb::iresearch::ConsolidationPolicy::FILL]._threshold = .31f;
  meta._iid = 10;
  meta._locale = iresearch::locale_utils::locale("en_UK.UTF-8");
  meta._name = "abc";
  meta._nestingDelimiter = "!";
  meta._nestingListOffsetPrefix = "(";
  meta._nestingListOffsetSuffix = ")";
  meta._dataPath = "somepath";
  meta._scorers.emplace("scorer1", invalidScorer);
  meta._scorers.emplace("scorer2", invalidScorer);
  meta._scorers.emplace("scorer3", invalidScorer);
  meta._threadsMaxIdle = 8;
  meta._threadsMaxTotal = 16;

  std::unordered_set<TRI_voc_cid_t> expectedCollections = { 42, 52, 62 };
  std::unordered_map<std::string, std::unordered_map<std::string, double>> expectedCommitBulkConsolidate = {
    { "bytes",{ { "intervalStep", 100 },{ "threshold", .1f } } },
    { "bytes_accum",{ { "intervalStep", 150 },{ "threshold", .15f } } },
    { "count",{ { "intervalStep", 200 },{ "threshold", .2f } } },
    { "fill",{ { "intervalStep", 300 },{ "threshold", .3f } } }
  };
  std::unordered_map<std::string, std::unordered_map<std::string, double>> expectedCommitItemConsolidate = {
    { "bytes",{ { "intervalStep", 101 },{ "threshold", .11f } } },
    { "bytes_accum",{ { "intervalStep", 151 },{ "threshold", .151f } } },
    { "count",{ { "intervalStep", 201 },{ "threshold", .21f } } },
    { "fill",{ { "intervalStep", 301 },{ "threshold", .31f } } }
  };
  std::unordered_set<std::string> expectedScorers = { "scorer1", "scorer2", "scorer3" };
  arangodb::velocypack::Builder builder;
  arangodb::velocypack::Slice tmpSlice;
  arangodb::velocypack::Slice tmpSlice2;

  BOOST_REQUIRE_EQUAL(true, meta.json(arangodb::velocypack::ObjectBuilder(&builder)));

  auto slice = builder.slice();

  BOOST_CHECK_EQUAL(13, slice.length());
  tmpSlice = slice.get("collections");
  BOOST_CHECK_EQUAL(true, tmpSlice.isArray() && 3 == tmpSlice.length());

  for (arangodb::velocypack::ArrayIterator itr(tmpSlice); itr.valid(); ++itr) {
    auto value = itr.value();
    BOOST_CHECK_EQUAL(true, value.isUInt() && 1 == expectedCollections.erase(value.getUInt()));
  }

  BOOST_CHECK_EQUAL(true, expectedCollections.empty());
  tmpSlice = slice.get("commitBulk");
  BOOST_CHECK_EQUAL(true, tmpSlice.isObject() && 3 == tmpSlice.length());
  tmpSlice2 = tmpSlice.get("cleanupIntervalStep");
  BOOST_CHECK_EQUAL(true, tmpSlice2.isNumber() && 123 == tmpSlice2.getUInt());
  tmpSlice2 = tmpSlice.get("commitIntervalBatchSize");
  BOOST_CHECK_EQUAL(true, tmpSlice2.isNumber() && 321 == tmpSlice2.getUInt());
  tmpSlice2 = tmpSlice.get("consolidate");
  BOOST_CHECK_EQUAL(true, tmpSlice2.isObject() && 4 == tmpSlice2.length());

  for (arangodb::velocypack::ObjectIterator itr(tmpSlice2); itr.valid(); ++itr) {
    auto key = itr.key();
    auto value = itr.value();
    auto& expectedPolicy = expectedCommitBulkConsolidate[key.copyString()];
    BOOST_CHECK_EQUAL(true, key.isString());
    BOOST_CHECK_EQUAL(true, value.isObject() && 2 == value.length());

    for (arangodb::velocypack::ObjectIterator itr2(value); itr2.valid(); ++itr2) {
      auto key2 = itr2.key();
      auto value2 = itr2.value();
      BOOST_CHECK_EQUAL(true, key2.isString() && expectedPolicy[key2.copyString()] == value2.getNumber<double>());
      expectedPolicy.erase(key2.copyString());
    }

    expectedCommitBulkConsolidate.erase(key.copyString());
  }

  BOOST_CHECK_EQUAL(true, expectedCommitBulkConsolidate.empty());
  tmpSlice = slice.get("commitItem");
  BOOST_CHECK_EQUAL(true, tmpSlice.isObject() && 3 == tmpSlice.length());
  tmpSlice2 = tmpSlice.get("cleanupIntervalStep");
  BOOST_CHECK_EQUAL(true, tmpSlice2.isUInt() && 654 == tmpSlice2.getUInt());
  tmpSlice2 = tmpSlice.get("commitIntervalMsec");
  BOOST_CHECK_EQUAL(true, tmpSlice2.isUInt() && 456 == tmpSlice2.getUInt());
  tmpSlice2 = tmpSlice.get("consolidate");
  BOOST_CHECK_EQUAL(true, tmpSlice2.isObject() && 4 == tmpSlice2.length());

  for (arangodb::velocypack::ObjectIterator itr(tmpSlice2); itr.valid(); ++itr) {
    auto key = itr.key();
    auto value = itr.value();
    auto& expectedPolicy = expectedCommitItemConsolidate[key.copyString()];
    BOOST_CHECK_EQUAL(true, key.isString());
    BOOST_CHECK_EQUAL(true, value.isObject() && 2 == value.length());

    for (arangodb::velocypack::ObjectIterator itr2(value); itr2.valid(); ++itr2) {
      auto key2 = itr2.key();
      auto value2 = itr2.value();
      BOOST_CHECK_EQUAL(true, key2.isString() && expectedPolicy[key2.copyString()] == value2.getNumber<double>());
      expectedPolicy.erase(key2.copyString());
    }

    expectedCommitItemConsolidate.erase(key.copyString());
  }

  BOOST_CHECK_EQUAL(true, expectedCommitItemConsolidate.empty());
  tmpSlice = slice.get("dataPath");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("somepath") == tmpSlice.copyString());
  tmpSlice = slice.get("id");
  BOOST_CHECK_EQUAL(true, tmpSlice.isNumber() && 10 == tmpSlice.getUInt());
  tmpSlice = slice.get("locale");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("en_UK.UTF-8") == tmpSlice.copyString());
  tmpSlice = slice.get("name");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("abc") == tmpSlice.copyString());
  tmpSlice = slice.get("nestingDelimiter");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("!") == tmpSlice.copyString());
  tmpSlice = slice.get("nestingListOffsetPrefix");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string("(") == tmpSlice.copyString());
  tmpSlice = slice.get("nestingListOffsetSuffix");
  BOOST_CHECK_EQUAL(true, tmpSlice.isString() && std::string(")") == tmpSlice.copyString());
  tmpSlice = slice.get("scorers");
  BOOST_CHECK_EQUAL(true, tmpSlice.isArray() && defaultScorers.size() + 3 == tmpSlice.length());

  for (arangodb::velocypack::ArrayIterator itr(tmpSlice); itr.valid(); ++itr) {
    auto value = itr.value();
    BOOST_CHECK_EQUAL(
      true,
      value.isString() &&
      (defaultScorers.find(value.copyString()) != defaultScorers.end() ||
       1 == expectedScorers.erase(value.copyString()))
    );
  }

  BOOST_CHECK_EQUAL(true, expectedScorers.empty());
  tmpSlice = slice.get("threadsMaxIdle");
  BOOST_CHECK_EQUAL(true, tmpSlice.isNumber() && 8 == tmpSlice.getUInt());
  tmpSlice = slice.get("threadsMaxTotal");
  BOOST_CHECK_EQUAL(true, tmpSlice.isNumber() && 16 == tmpSlice.getUInt());
}

BOOST_AUTO_TEST_CASE(test_readMaskAll) {
  arangodb::iresearch::IResearchViewMeta meta;
  arangodb::iresearch::IResearchViewMeta::Mask mask;
  std::string errorField;

  auto json = arangodb::velocypack::Parser::fromJson("{ \
    \"collections\": [ 42 ], \
    \"commitBulk\": { \"commitIntervalBatchSize\": 321, \"cleanupIntervalStep\": 123, \"consolidate\": { \"bytes\": { \"threshold\": 0.1 } } }, \
    \"commitItem\": { \"commitIntervalMsec\": 654, \"cleanupIntervalStep\": 456, \"consolidate\": {\"bytes_accum\": { \"threshold\": 0.1 } } }, \
    \"dataPath\": \"somepath\", \
    \"id\": 10, \
    \"locale\": \"ru_RU.KOI8-R\", \
    \"name\": \"abc\", \
    \"nestingDelimiter\": \"->\", \
    \"nestingListOffsetPrefix\": \"-{\", \
    \"nestingListOffsetSuffix\": \"}-\", \
    \"scorers\": [ \"tfidf\" ], \
    \"threadsMaxIdle\": 8, \
    \"threadsMaxTotal\": 16 \
  }");
  BOOST_REQUIRE_EQUAL(true, meta.init(json->slice(), errorField, arangodb::iresearch::IResearchViewMeta::DEFAULT(), &mask));
  BOOST_CHECK_EQUAL(true, mask._collections);
  BOOST_CHECK_EQUAL(true, mask._commitBulk);
  BOOST_CHECK_EQUAL(true, mask._commitItem);
  BOOST_CHECK_EQUAL(true, mask._dataPath);
  BOOST_CHECK_EQUAL(true, mask._iid);
  BOOST_CHECK_EQUAL(true, mask._locale);
  BOOST_CHECK_EQUAL(true, mask._name);
  BOOST_CHECK_EQUAL(true, mask._nestingDelimiter);
  BOOST_CHECK_EQUAL(true, mask._nestingListOffsetPrefix);
  BOOST_CHECK_EQUAL(true, mask._nestingListOffsetSuffix);
  BOOST_CHECK_EQUAL(true, mask._scorers);
  BOOST_CHECK_EQUAL(true, mask._threadsMaxIdle);
  BOOST_CHECK_EQUAL(true, mask._threadsMaxTotal);
}

BOOST_AUTO_TEST_CASE(test_readMaskNone) {
  // test missing required fields
  {
    arangodb::iresearch::IResearchViewMeta meta;
    std::string errorField;
    auto json = arangodb::velocypack::Parser::fromJson("{}");
    BOOST_CHECK_EQUAL(false, meta.init(json->slice(), errorField));
  }

  arangodb::iresearch::IResearchViewMeta meta;
  arangodb::iresearch::IResearchViewMeta::Mask mask;
  std::string errorField;

  auto json = arangodb::velocypack::Parser::fromJson("{ \"name\": \"abc\" }");
  BOOST_REQUIRE_EQUAL(true, meta.init(json->slice(), errorField, arangodb::iresearch::IResearchViewMeta::DEFAULT(), &mask));
  BOOST_CHECK_EQUAL(false, mask._collections);
  BOOST_CHECK_EQUAL(false, mask._commitBulk);
  BOOST_CHECK_EQUAL(false, mask._commitItem);
  BOOST_CHECK_EQUAL(false, mask._dataPath);
  BOOST_CHECK_EQUAL(false, mask._iid);
  BOOST_CHECK_EQUAL(false, mask._locale);
  BOOST_CHECK_EQUAL(true, mask._name);
  BOOST_CHECK_EQUAL(false, mask._nestingDelimiter);
  BOOST_CHECK_EQUAL(false, mask._nestingListOffsetPrefix);
  BOOST_CHECK_EQUAL(false, mask._nestingListOffsetSuffix);
  BOOST_CHECK_EQUAL(false, mask._scorers);
  BOOST_CHECK_EQUAL(false, mask._threadsMaxIdle);
  BOOST_CHECK_EQUAL(false, mask._threadsMaxTotal);
}

BOOST_AUTO_TEST_CASE(test_writeMaskAll) {
  arangodb::iresearch::IResearchViewMeta meta;
  arangodb::iresearch::IResearchViewMeta::Mask mask(true);
  arangodb::velocypack::Builder builder;
  arangodb::velocypack::Slice tmpSlice;

  meta._dataPath = "path"; // add a value so that attribute is not omitted

  BOOST_REQUIRE_EQUAL(true, meta.json(arangodb::velocypack::ObjectBuilder(&builder), nullptr, &mask));

  auto slice = builder.slice();

  BOOST_CHECK_EQUAL(13, slice.length());
  BOOST_CHECK_EQUAL(true, slice.hasKey("collections"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("commitBulk"));
  tmpSlice = slice.get("commitBulk");
  BOOST_CHECK_EQUAL(true, tmpSlice.hasKey("cleanupIntervalStep"));
  BOOST_CHECK_EQUAL(true, tmpSlice.hasKey("commitIntervalBatchSize"));
  BOOST_CHECK_EQUAL(true, tmpSlice.hasKey("consolidate"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("commitItem"));
  tmpSlice = slice.get("commitItem");
  BOOST_CHECK_EQUAL(true, tmpSlice.hasKey("cleanupIntervalStep"));
  BOOST_CHECK_EQUAL(true, tmpSlice.hasKey("commitIntervalMsec"));
  BOOST_CHECK_EQUAL(true, tmpSlice.hasKey("consolidate"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("dataPath"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("id"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("locale"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("name"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("nestingDelimiter"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("nestingListOffsetPrefix"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("nestingListOffsetSuffix"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("scorers"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("threadsMaxIdle"));
  BOOST_CHECK_EQUAL(true, slice.hasKey("threadsMaxTotal"));
}

BOOST_AUTO_TEST_CASE(test_writeMaskNone) {
  arangodb::iresearch::IResearchViewMeta meta;
  arangodb::iresearch::IResearchViewMeta::Mask mask(false);
  arangodb::velocypack::Builder builder;

  BOOST_REQUIRE_EQUAL(true, meta.json(arangodb::velocypack::ObjectBuilder(&builder), nullptr, &mask));

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