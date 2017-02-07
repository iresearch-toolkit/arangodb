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
  uint8_t* key();
  uint8_t* value();
  uint64_t size();

  // aliases to handle refCount
  void lease();
  void release();

  // utility methods to make creation easier easier
  CachedValue* copy();
  static CachedValue* construct(uint32_t kSize, uint8_t* k, uint64_t vSize,
                                uint8_t* v);
};

static_assert(sizeof(CachedValue) == 16);

};  // end namespace cache
};  // end namespace arangodb

#endif
