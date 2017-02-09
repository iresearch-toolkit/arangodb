////////////////////////////////////////////////////////////////////////////////
/// @brief test suite for arangodb::cache::CachedValue
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

#include "Cache/CachedValue.h"

#include <stdint.h>
#include <string>

using namespace arangodb::cache;

// -----------------------------------------------------------------------------
// --SECTION--                                                 setup / tear-down
// -----------------------------------------------------------------------------

struct CCacheCachedValueSetup {
  CCacheCachedValueSetup() { BOOST_TEST_MESSAGE("setup CachedValue"); }

  ~CCacheCachedValueSetup() { BOOST_TEST_MESSAGE("tear-down CachedValue"); }
};
// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief setup
////////////////////////////////////////////////////////////////////////////////

BOOST_FIXTURE_TEST_SUITE(CCacheCachedValueTest, CCacheCachedValueSetup)

////////////////////////////////////////////////////////////////////////////////
/// @brief test construct with valid data
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_CASE(tst_construct_valid) {
  uint64_t k = 1;
  std::string v("test");
  CachedValue* cv;

  // fixed key, variable value
  cv = CachedValue::construct(&k, sizeof(uint64_t), v.data(), v.size());
  BOOST_CHECK(nullptr != cv);
  BOOST_CHECK_EQUAL(sizeof(uint64_t), cv->keySize);
  BOOST_CHECK_EQUAL(v.size(), cv->valueSize);
  BOOST_CHECK_EQUAL(sizeof(CachedValue) + sizeof(uint64_t) + v.size(),
                    cv->size());
  BOOST_CHECK_EQUAL(k, *reinterpret_cast<uint64_t const*>(cv->key()));
  BOOST_CHECK_EQUAL(0, memcmp(v.data(), cv->value(), v.size()));
  delete cv;

  // variable key, fixed value
  cv = CachedValue::construct(v.data(), v.size(), &k, sizeof(uint64_t));
  BOOST_CHECK(nullptr != cv);
  BOOST_CHECK_EQUAL(v.size(), cv->keySize);
  BOOST_CHECK_EQUAL(sizeof(uint64_t), cv->valueSize);
  BOOST_CHECK_EQUAL(sizeof(CachedValue) + sizeof(uint64_t) + v.size(),
                    cv->size());
  BOOST_CHECK_EQUAL(0, memcmp(v.data(), cv->key(), v.size()));
  BOOST_CHECK_EQUAL(k, *reinterpret_cast<uint64_t const*>(cv->value()));
  delete cv;

  // fixed key, zero length value
  cv = CachedValue::construct(&k, sizeof(uint64_t), nullptr, 0);
  BOOST_CHECK(nullptr != cv);
  BOOST_CHECK_EQUAL(sizeof(uint64_t), cv->keySize);
  BOOST_CHECK_EQUAL(0ULL, cv->valueSize);
  BOOST_CHECK_EQUAL(sizeof(CachedValue) + sizeof(uint64_t), cv->size());
  BOOST_CHECK_EQUAL(k, *reinterpret_cast<uint64_t const*>(cv->key()));
  BOOST_CHECK(nullptr == cv->value());
  delete cv;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief test construct with invalid data
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_CASE(tst_construct_invalid) {
  uint64_t k = 1;
  std::string v("test");
  CachedValue* cv;

  // zero size key
  cv = CachedValue::construct(&k, 0, v.data(), v.size());
  BOOST_CHECK(nullptr == cv);

  // nullptr key, zero size
  cv = CachedValue::construct(nullptr, 0, v.data(), v.size());
  BOOST_CHECK(nullptr == cv);

  // nullptr key, non-zero size
  cv = CachedValue::construct(nullptr, sizeof(uint64_t), v.data(), v.size());
  BOOST_CHECK(nullptr == cv);

  // nullptr value, non-zero length
  cv = CachedValue::construct(&k, sizeof(uint64_t), nullptr, v.size());
  BOOST_CHECK(nullptr == cv);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief test copy
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_CASE(tst_copy) {
  uint64_t k = 1;
  std::string v("test");

  // fixed key, variable value
  auto original =
      CachedValue::construct(&k, sizeof(uint64_t), v.data(), v.size());
  auto copy = original->copy();
  BOOST_CHECK(nullptr != copy);
  BOOST_CHECK_EQUAL(sizeof(uint64_t), copy->keySize);
  BOOST_CHECK_EQUAL(v.size(), copy->valueSize);
  BOOST_CHECK_EQUAL(sizeof(CachedValue) + sizeof(uint64_t) + v.size(),
                    copy->size());
  BOOST_CHECK_EQUAL(k, *reinterpret_cast<uint64_t const*>(copy->key()));
  BOOST_CHECK_EQUAL(0, memcmp(v.data(), copy->value(), v.size()));
  delete original;
  delete copy;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief test key comparison
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_CASE(tst_key_comparison) {
  std::string k1("test");
  std::string k2("testing");
  std::string k3("TEST");
  uint64_t v = 1;

  auto cv = CachedValue::construct(k1.data(), k1.size(), &v, sizeof(uint64_t));

  // same key
  BOOST_CHECK(cv->sameKey(k1.data(), k1.size()));

  // different length, matching prefix
  BOOST_CHECK(!cv->sameKey(k2.data(), k2.size()));

  // same length, different key
  BOOST_CHECK(!cv->sameKey(k3.data(), k3.size()));

  delete cv;
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
