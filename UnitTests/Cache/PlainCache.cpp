////////////////////////////////////////////////////////////////////////////////
/// @brief test suite for arangodb::cache::PlainBucket
///
/// @file
///
/// DISCLAIMER
///
/// Copyright 2017 triagens GmbH, Cologne, Germany
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
/// Copyright holder is triAGENS GmbH, Cologne, Germany
///
/// @author Daniel H. Larkin
/// @author Copyright 2017, triAGENS GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "Basics/Common.h"

#define BOOST_TEST_INCLUDED
#include <boost/test/unit_test.hpp>

#include "Cache/Manager.h"
#include "Cache/PlainCache.h"

#include <stdint.h>
#include <string>

#include <iostream>

using namespace arangodb::cache;

// -----------------------------------------------------------------------------
// --SECTION--                                                 setup / tear-down
// -----------------------------------------------------------------------------

struct CCachePlainCacheSetup {
  CCachePlainCacheSetup() { BOOST_TEST_MESSAGE("setup PlainCache"); }

  ~CCachePlainCacheSetup() { BOOST_TEST_MESSAGE("tear-down PlainCache"); }
};
// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief setup
////////////////////////////////////////////////////////////////////////////////

BOOST_FIXTURE_TEST_SUITE(CCachePlainCacheTest, CCachePlainCacheSetup)

////////////////////////////////////////////////////////////////////////////////
/// @brief test construction
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_CASE(tst_construction) {
  Manager manager(1024ULL * 1024ULL);
  PlainCache cache1(&manager, 256ULL * 1024ULL, false);
  PlainCache cache2(&manager, 512ULL * 1024ULL, false);

  BOOST_CHECK_EQUAL(0ULL, cache1.usage());
  BOOST_CHECK_EQUAL(256ULL * 1024ULL, cache1.limit());
  BOOST_CHECK_EQUAL(0ULL, cache2.usage());
  BOOST_CHECK(512ULL * 1024ULL > cache2.limit());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief test insertion to check for
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_CASE(tst_insertion) {
  uint64_t cacheLimit = 256ULL * 1024ULL;
  Manager manager(4ULL * cacheLimit);
  PlainCache cache(&manager, cacheLimit, false);

  for (uint64_t i = 0; i < 1024; i++) {
    CachedValue* value =
        CachedValue::construct(&i, sizeof(uint64_t), &i, sizeof(uint64_t));
    bool success = cache.insert(value);
    BOOST_CHECK(success);
    auto f = cache.find(&i, sizeof(uint64_t));
    BOOST_CHECK(f.found());
  }

  for (uint64_t i = 0; i < 1024; i++) {
    CachedValue* value =
        CachedValue::construct(&i, sizeof(uint64_t), &i, sizeof(uint64_t));
    bool success = cache.insert(value);
    BOOST_CHECK(!success);
    delete value;
    auto f = cache.find(&i, sizeof(uint64_t));
    BOOST_CHECK(f.found());
  }

  uint64_t notInserted = 0;
  for (uint64_t i = 1024; i < 128 * 1024; i++) {
    CachedValue* value =
        CachedValue::construct(&i, sizeof(uint64_t), &i, sizeof(uint64_t));
    bool success = cache.insert(value);
    if (success) {
      auto f = cache.find(&i, sizeof(uint64_t));
      BOOST_CHECK(f.found());
    } else {
      delete value;
      notInserted++;
    }
  }
  BOOST_CHECK(notInserted > 0);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief test removal
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_CASE(tst_removal) {
  uint64_t cacheLimit = 256ULL * 1024ULL;
  Manager manager(4ULL * cacheLimit);
  PlainCache cache(&manager, cacheLimit, false);

  for (uint64_t i = 0; i < 1024; i++) {
    CachedValue* value =
        CachedValue::construct(&i, sizeof(uint64_t), &i, sizeof(uint64_t));
    bool success = cache.insert(value);
    BOOST_CHECK(success);
    auto f = cache.find(&i, sizeof(uint64_t));
    BOOST_CHECK(f.found());
  }

  // test removal of bogus keys
  for (uint64_t i = 1024; i < 2048; i++) {
    bool removed = cache.remove(&i, sizeof(uint64_t));
    BOOST_ASSERT(!removed);
    // ensure existing keys not removed
    for (uint64_t j = 0; j < 1024; j++) {
      auto f = cache.find(&j, sizeof(uint64_t));
      BOOST_CHECK(f.found());
    }
  }

  // remove actual keys
  for (uint64_t i = 0; i < 1024; i++) {
    bool removed = cache.remove(&i, sizeof(uint64_t));
    BOOST_CHECK(removed);
    auto f = cache.find(&i, sizeof(uint64_t));
    BOOST_CHECK(!f.found());
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief generate tests
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_SUITE_END()

// Local Variables:
// mode: outline-minor
// outline-regexp: "^\\(/// @brief\\|/// {@inheritDoc}\\|/// @addtogroup\\|//
// --SECTION--\\|/// @\\}\\)"
// End:
