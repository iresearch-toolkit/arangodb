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

#ifndef ARANGODB_CACHE_MANAGER_H
#define ARANGODB_CACHE_MANAGER_H

#include "Basics/Common.h"
#include "Basics/asio-helper.h"
#include "Cache/CachedValue.h"
#include "Cache/FrequencyBuffer.h"
#include "Cache/Metadata.h"
#include "Cache/State.h"

#include <stdint.h>
#include <atomic>
#include <chrono>
#include <list>
#include <memory>
#include <stack>
#include <utility>

namespace arangodb {
namespace cache {

class Cache;           // forward declaration
class FreeMemoryTask;  // forward declaration
class MigrateTask;     // forward declaration

class Manager {
 public:
  typedef FrequencyBuffer<Cache*> StatBuffer;
  typedef std::chrono::time_point<std::chrono::steady_clock> time_point;
  typedef std::list<Metadata>::iterator MetadataItr;

 public:
  Manager(boost::asio::io_service* ioService, uint64_t globalLimit);
  ~Manager();

  // cache factory
  enum CacheType { Plain, Transactional };
  std::shared_ptr<Cache> createCache(Manager::CacheType type,
                                     uint64_t requestedLimit, bool allowGrowth);

  // change global cache limit
  bool resize(uint64_t newGlobalLimit);

  // report current limit and usage
  uint64_t globalLimit();
  uint64_t globalAllocation();

  // transaction management
  void startTransaction();
  void endTransaction();
  uint64_t transactionTerm();

 private:
  // simple state variable for locking and other purposes
  State _state;

  // structure to handle access frequency monitoring
  Manager::StatBuffer _accessStats;

  // list of metadata objects to keep track of all the registered caches
  std::list<Metadata> _caches;

  // actual tables to lease out
  std::stack<uint8_t*> _tables[32];

  // global statistics
  uint64_t _globalSoftLimit;
  uint64_t _globalHardLimit;
  uint64_t _globalAllocation;

  // transaction management
  std::atomic<uint64_t> _openTransactions;
  std::atomic<uint64_t> _transactionTerm;

  boost::asio::io_service* _ioService;

  // friend class tasks and caches to allow access
  friend class Cache;
  friend class FreeMemoryTask;
  friend class MigrateTask;
  friend class PlainCache;
  friend class TransactionalCache;

 private:
  // expose io_service
  boost::asio::io_service* ioService();

  // register and unregister individual caches
  Manager::MetadataItr registerCache(Cache* cache, uint64_t requestedLimit,
                                     std::function<void(Cache*)> deleter);
  void unregisterCache(Manager::MetadataItr& metadata);

  // allow individual caches to request changes to their allocations
  std::pair<bool, Manager::time_point> requestResize(
      Manager::MetadataItr& metadata, uint64_t requestedLimit);
  std::pair<bool, Manager::time_point> requestMigrate(
      Manager::MetadataItr& metadata, uint32_t requestedLogSize);

  // report cache access
  void reportAccess(Cache* cache);

  // periodically run to rebalance allocations globally?
  void rebalance();

  // methods to adjust individual caches
  void resizeCache(Manager::MetadataItr& metadata, uint64_t newLimit);
  void leaseTable(Manager::MetadataItr& metadata, uint32_t logSize);
  void reclaimTables(Manager::MetadataItr& metadata,
                     bool auxiliaryOnly = false);

  // helpers for allocations
  bool increaseAllowed(uint64_t increase) const;
  uint64_t tableSize(uint32_t logSize) const;

  // helper for wait times
  Manager::time_point futureTime(uint64_t secondsFromNow);

  // helpers for global resizing
  void freeUnusedTables();
  bool adjustGlobalLimitsIfAllowed(uint64_t newGlobalLimit);
  uint64_t resizeAllCaches(Manager::StatBuffer::stats_t* stats, bool noTasks,
                           bool aggressive, uint64_t goal);
};

};  // end namespace cache
};  // end namespace arangodb

#endif
