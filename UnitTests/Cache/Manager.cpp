////////////////////////////////////////////////////////////////////////////////
/// @brief test suite for arangodb::cache::Manager
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

#include <stdint.h>
#include <iostream>

using namespace arangodb::cache;

// -----------------------------------------------------------------------------
// --SECTION--                                                 setup / tear-down
// -----------------------------------------------------------------------------

struct CCacheManagerSetup {
  CCacheManagerSetup() { BOOST_TEST_MESSAGE("setup Manager"); }

  ~CCacheManagerSetup() { BOOST_TEST_MESSAGE("tear-down Manager"); }
};
// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief setup
////////////////////////////////////////////////////////////////////////////////

BOOST_FIXTURE_TEST_SUITE(CCacheManagerTest, CCacheManagerSetup)

////////////////////////////////////////////////////////////////////////////////
/// @brief test constructor with valid data
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_CASE(tst_constructor) {
  uint64_t requestLimit = 1024 * 1024;
  Manager manager(requestLimit);

  BOOST_CHECK_EQUAL(requestLimit, manager.globalLimit());
  BOOST_CHECK_EQUAL(0ULL, manager.transactionTerm());

  BOOST_CHECK(0ULL < manager.globalAllocation());
  BOOST_CHECK(requestLimit > manager.globalAllocation());

  uint64_t bigRequestLimit = 4ULL * 1024ULL * 1024ULL * 1024ULL;
  Manager bigManager(bigRequestLimit);

  BOOST_CHECK_EQUAL(bigRequestLimit, bigManager.globalLimit());
  BOOST_CHECK_EQUAL(0ULL, bigManager.transactionTerm());

  BOOST_CHECK((1024ULL * 1024ULL) < bigManager.globalAllocation());
  BOOST_CHECK(bigRequestLimit > bigManager.globalAllocation());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief test transaction term management
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_CASE(tst_transaction_term) {
  uint64_t requestLimit = 1024 * 1024;
  Manager manager(requestLimit);

  BOOST_CHECK_EQUAL(0ULL, manager.transactionTerm());

  manager.startTransaction();
  BOOST_CHECK_EQUAL(1ULL, manager.transactionTerm());
  manager.endTransaction();
  BOOST_CHECK_EQUAL(2ULL, manager.transactionTerm());

  manager.startTransaction();
  BOOST_CHECK_EQUAL(3ULL, manager.transactionTerm());
  manager.startTransaction();
  BOOST_CHECK_EQUAL(3ULL, manager.transactionTerm());
  manager.endTransaction();
  BOOST_CHECK_EQUAL(3ULL, manager.transactionTerm());
  manager.endTransaction();
  BOOST_CHECK_EQUAL(4ULL, manager.transactionTerm());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief test cache registration
////////////////////////////////////////////////////////////////////////////////

BOOST_AUTO_TEST_CASE(tst_registration) {
  uint64_t requestLimit = 1024 * 1024;
  Manager manager(requestLimit);

  Cache* cache1 = reinterpret_cast<Cache*>(0x1ULL);
  uint64_t limit1 = 16 * 1024;
  Cache* cache2 = reinterpret_cast<Cache*>(0x2ULL);
  uint64_t limit2 = 64 * 1024;

  std::list<Metadata>::iterator it1 = manager.registerCache(cache1, limit1);
  std::list<Metadata>::iterator it2 = manager.registerCache(cache2, limit2);

  uint64_t fullAllocation = manager.globalAllocation();
  BOOST_CHECK_MESSAGE(requestLimit > fullAllocation,
                      requestLimit << " !> " << fullAllocation);
  BOOST_CHECK_MESSAGE(limit1 + limit2 < fullAllocation,
                      limit1 + limit2 << " !< " << fullAllocation);

  it1->lock();
  BOOST_CHECK(cache1 == it1->cache());
  BOOST_CHECK_EQUAL(0ULL, it1->usage());
  BOOST_CHECK_EQUAL(limit1, it1->softLimit());
  BOOST_CHECK_EQUAL(limit1, it1->hardLimit());
  it1->unlock();

  it2->lock();
  BOOST_CHECK(cache2 == it2->cache());
  BOOST_CHECK_EQUAL(0ULL, it2->usage());
  BOOST_CHECK_EQUAL(limit2, it2->softLimit());
  BOOST_CHECK_EQUAL(limit2, it2->hardLimit());
  it2->unlock();

  manager.unregisterCache(it1);

  uint64_t partialAllocation = manager.globalAllocation();
  BOOST_CHECK_MESSAGE(fullAllocation - limit1 > partialAllocation,
                      fullAllocation - limit1 << " !> " << partialAllocation);
  BOOST_CHECK_MESSAGE(limit2 < partialAllocation,
                      limit2 << " !< " << partialAllocation);

  it2->lock();
  BOOST_CHECK(cache2 == it2->cache());
  BOOST_CHECK_EQUAL(0ULL, it2->usage());
  BOOST_CHECK_EQUAL(limit2, it2->softLimit());
  BOOST_CHECK_EQUAL(limit2, it2->hardLimit());
  it2->unlock();

  manager.unregisterCache(it2);

  BOOST_CHECK_MESSAGE(partialAllocation - limit2 > manager.globalAllocation(),
                      partialAllocation - limit2 << " !> "
                                                 << manager.globalAllocation());

  // test for exception when no room left
  std::vector<std::list<Metadata>::iterator> ms;
  ms.reserve(10);
  bool caught = false;
  try {
    for (size_t i = 0; i < 10; i++) {
      ms.emplace_back(
          manager.registerCache(reinterpret_cast<Cache*>(i), 1024 * 512));
    }
  } catch (std::bad_alloc) {
    caught = true;
  }
  BOOST_CHECK_MESSAGE(caught, "Didn't throw exception on full.");
  for (auto m : ms) {
    manager.unregisterCache(m);
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
