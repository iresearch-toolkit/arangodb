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

#ifndef ARANGODB_CACHE_CACHE_H
#define ARANGODB_CACHE_CACHE_H

#include "Basics/Common.h"
#include "Cache/CachedValue.h"
#include "Cache/FrequencyBuffer.h"
#include "Cache/Manager.h"
#include "Cache/Metadata.h"
#include "Cache/State.h"

#include <stdint.h>
#include <list>

namespace arangodb {
namespace cache {

class Cache {
 protected:
  typedef FrequencyBuffer<uint8_t> StatBuffer;

 protected:
  State _state;

  // whether to allow the cache to resize larger when it fills
  bool _allowGrowth;

  // structures to handle eviction-upon-insertion rate
  enum class Stat : uint8_t { eviction = 1, noEviction = 2 };
  StatBuffer _evictionStats;
  std::atomic<uint64_t> _insertionCount;

  // allow communication with manager
  Manager* _manager;
  std::list<Metadata>::iterator _metadata;

  // keep track of number of open operations to allow clean shutdown
  std::atomic<uint32_t> _openOperations;

  // times to wait until requesting is allowed again
  Manager::time_point _migrateRequestTime;
  Manager::time_point _resizeRequestTime;

 public:
  // helper class for managing cached value lifecycles
  class Finding {
   private:
    CachedValue* _value;

   public:
    Finding(CachedValue* v);
    ~Finding();

    void reset(CachedValue* v);

    bool found() const;
    CachedValue const* value() const;
    CachedValue* copy() const;
  };

 public:
  Cache(Manager* manager, uint64_t requestedLimit,
        bool allowGrowth);  // TODO: create CacheManagerFeature so
                            // first parameter can be removed

  // primary functionality
  virtual Finding find(void const* key, uint32_t keySize) = 0;
  virtual bool insert(CachedValue* value) = 0;
  virtual bool remove(void const* key, uint32_t keySize) = 0;

  // info methods
  uint64_t limit();
  uint64_t usage();

  // auxiliary functionality
  void requestResize(uint64_t requestedLimit = 0);

  // management
  std::list<Metadata>::iterator& metadata();
  void shutdown();
  virtual void freeMemory() = 0;
  virtual void migrate() = 0;

 protected:
  bool isOperational() const;
  void startOperation();
  void endOperation();

  bool isMigrating() const;
  void requestMigrate(uint32_t requestedLogSize = 0);

  void freeValue(CachedValue* value);
  virtual void clearTables() = 0;

  uint32_t hashKey(void const* key, uint32_t keySize) const;
  void recordStat(Cache::Stat stat);
};

};  // end namespace cache
};  // end namespace arangodb

#endif
