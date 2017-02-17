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

#ifndef ARANGODB_CACHE_PLAIN_CACHE_H
#define ARANGODB_CACHE_PLAIN_CACHE_H

#include "Basics/Common.h"
#include "Cache/Cache.h"
#include "Cache/CachedValue.h"
#include "Cache/FrequencyBuffer.h"
#include "Cache/Manager.h"
#include "Cache/ManagerTasks.h"
#include "Cache/Metadata.h"
#include "Cache/PlainBucket.h"
#include "Cache/State.h"

#include <stdint.h>
#include <atomic>
#include <chrono>
#include <list>

namespace arangodb {
namespace cache {

class Manager;  // forward declaration

class PlainCache final : public Cache {
 public:
  PlainCache() = delete;
  PlainCache(PlainCache const&) = delete;
  PlainCache& operator=(PlainCache const&) = delete;

 public:
  // creator -- do not use constructor explicitly
  static std::shared_ptr<Cache> create(Manager* manager, uint64_t requestedSize,
                                       bool allowGrowth);

  Cache::Finding find(void const* key, uint32_t keySize);
  bool insert(CachedValue* value);
  bool remove(void const* key, uint32_t keySize);

 private:
  // main table info
  PlainBucket* _table;
  uint32_t _logSize;
  uint64_t _tableSize;
  uint32_t _maskShift;
  uint32_t _bucketMask;

  // auxiliary table info
  PlainBucket* _auxiliaryTable;
  uint32_t _auxiliaryLogSize;
  uint64_t _auxiliaryTableSize;
  uint32_t _auxiliaryMaskShift;
  uint32_t _auxiliaryBucketMask;

  // friend class manager and tasks
  friend class FreeMemoryTask;
  friend class Manager;
  friend class MigrateTask;

 private:
  PlainCache(Manager* manager, uint64_t requestedLimit,
             bool allowGrowth);  // TODO: create CacheManagerFeature so
                                 // first parameter can be removed
  ~PlainCache();

  // management
  bool freeMemory();
  bool migrate();
  void clearTables();

  // helpers
  std::pair<bool, PlainBucket*> getBucket(uint32_t hash, int64_t maxTries = 10,
                                          bool singleOperation = true);
  void clearTable(PlainBucket* table, uint64_t tableSize);
  uint32_t getIndex(uint32_t hash, bool useAuxiliary) const;
};

};  // end namespace cache
};  // end namespace arangodb

#endif
