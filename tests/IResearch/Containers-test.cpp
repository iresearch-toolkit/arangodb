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

#include "IResearch/Containers.h"

// -----------------------------------------------------------------------------
// --SECTION--                                                 setup / tear-down
// -----------------------------------------------------------------------------

struct ContainersSetup { };

// -----------------------------------------------------------------------------
// --SECTION--                                                        test suite
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief setup
////////////////////////////////////////////////////////////////////////////////

TEST_CASE("ContainersTest", "[iresearch][iresearch-containers]") {
  ContainersSetup s;
  UNUSED(s);

  SECTION("test_Hasher") {
    arangodb::iresearch::Hasher hasher;

    // ensure hashing of irs::bytes_ref is possible
    {
      irs::string_ref strRef("abcdefg");
      irs::bytes_ref ref = irs::ref_cast<irs::byte_type>(strRef);
      CHECK(false == (0 == hasher(ref)));
    }

    // ensure hashing of irs::string_ref is possible
    {
      irs::string_ref ref("abcdefg");
      CHECK(false == (0 == hasher(ref)));
    }
  }

  SECTION("test_UniqueHeapInstance") {
    {
      struct TestStruct {
      };

      // ensure copy works (different instance)
      {
        arangodb::iresearch::UniqueHeapInstance<TestStruct> instance;
        auto* ptr = instance.get();

        arangodb::iresearch::UniqueHeapInstance<TestStruct> instance1;
        auto* ptr1 = instance1.get();
        CHECK(false == (ptr == instance1.get()));
        instance1 = instance;
        CHECK(false == (ptr1 == instance1.get()));
        CHECK(false == (ptr == instance1.get()));

        arangodb::iresearch::UniqueHeapInstance<TestStruct> instance2(instance);
        CHECK(false == (ptr == instance2.get()));
      }

      // ensure element copy works (different instance)
      {
        arangodb::iresearch::UniqueHeapInstance<TestStruct> instance;
        auto* ptr = instance.get();

        arangodb::iresearch::UniqueHeapInstance<TestStruct> instance1;
        auto* ptr1 = instance1.get();
        CHECK(false == (ptr == instance1.get()));
        instance1 = *instance;
        CHECK(true == (ptr1 == instance1.get()));
        CHECK(false == (ptr == instance1.get()));

        arangodb::iresearch::UniqueHeapInstance<TestStruct> instance2(*instance);
        CHECK(false == (ptr == instance2.get()));
      }
    }

    {
      static size_t counter = 0;
      struct TestStruct {
        size_t id;
        TestStruct(): id(++counter) {}
        TestStruct(TestStruct&& other): id(other.id) {};
        TestStruct(TestStruct const&) = delete;
        TestStruct& operator=(TestStruct const&) = delete;
        TestStruct& operator=(TestStruct&& other) { id = other.id; return *this; }
      };

      // ensure move works (same instance)
      {
        arangodb::iresearch::UniqueHeapInstance<TestStruct> instance;
        auto* ptr = instance.get();

        arangodb::iresearch::UniqueHeapInstance<TestStruct> instance1;
        CHECK(false == (ptr == instance1.get()));
        instance1 = std::move(instance);
        CHECK(true == (ptr == instance1.get()));

        arangodb::iresearch::UniqueHeapInstance<TestStruct> instance2(std::move(instance1));
        CHECK(true == (ptr == instance2.get()));
      }

      // ensure value move works (same instance)
      {
        arangodb::iresearch::UniqueHeapInstance<TestStruct> instance;
        auto* ptr = instance.get();
        auto id = ptr->id;

        arangodb::iresearch::UniqueHeapInstance<TestStruct> instance1;
        CHECK(false == (ptr == instance1.get()));
        CHECK(false == (id == instance1->id));
        instance1 = std::move(*instance);
        CHECK(true == (id == instance1->id));

        arangodb::iresearch::UniqueHeapInstance<TestStruct> instance2(std::move(*instance1));
        CHECK(true == (id == instance2->id));
      }
    }
  }

////////////////////////////////////////////////////////////////////////////////
/// @brief generate tests
////////////////////////////////////////////////////////////////////////////////

}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------