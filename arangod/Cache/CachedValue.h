////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2017 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Daniel H. Larkin
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGODB_CACHE_CACHED_VALUE_H
#define ARANGODB_CACHE_CACHED_VALUE_H

#include "Basics/Common.h"

#include <stdint.h>
#include <atomic>

namespace arangodb {
namespace cache {

struct CachedValue {
  // This is the beginning of a cache data entry, it will be allocated
  // using std::malloc with the right size for key and value, which
  // resides directly behind the following entries. The reference count
  // is used to lend CachedValues to clients.
  std::atomic<uint32_t> refCount;
  uint32_t keySize;
  uint64_t valueSize;

  // simple accessor methods
  uint8_t const* key() const;
  uint8_t const* value() const;
  uint64_t size() const;

  // utility to check key equality
  bool sameKey(void const* k, uint32_t kSize);

  // aliases to handle refCount
  void lease();
  void release();
  bool isFreeable();

  // utility methods to make creation easier
  CachedValue* copy() const;
  static CachedValue* construct(void const* k, uint32_t kSize, void const* v,
                                uint64_t vSize);
  static void operator delete(void* ptr);
};

static_assert(sizeof(CachedValue) == 16);

};  // end namespace cache
};  // end namespace arangodb

#endif
