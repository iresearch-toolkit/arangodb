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
#include "Cache/Cache.h"
#include "Cache/CachedValue.h"
#include "Cache/FrequencyBuffer.h"
#include "Cache/Metadata.h"

#include <stdint.h>
#include <atomic>
#include <chrono>
#include <list>
#include <memory>
#include <stack>
#include <utility>

namespace arangodb {
namespace cache {

class Manager {
 public:
  typedef FrequencyBuffer<Cache*> StatBuffer;
  typedef std::chrono::time_point<std::chrono::steady_clock> time_point;

 private:
  // simple state variable for locking and other purposes
  std::atomic<uint32_t> _state;

  // state flags
  static uint32_t FLAG_LOCK;
  static uint32_t FLAG_MIGRATING;
  static uint32_t FLAG_RESIZING;
  static uint32_t FLAG_REBALANCING;

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

 public:
  Manager(uint64_t globalLimit);
  ~Manager();

  // change global cache limit
  bool resize(uint64_t newGlobalLimit);

  // report current limit and usage
  uint64_t globalLimit();
  uint64_t globalAllocation();

  // register and unregister individual caches
  std::list<Metadata>::iterator registerCache(Cache* cache,
                                              uint64_t requestedLimit);
  void unregisterCache(std::list<Metadata>::iterator& metadata);

  // allow individual caches to request changes to their allocations
  std::pair<bool, Manager::time_point> requestResize(
      std::list<Metadata>::iterator& metadata, uint64_t requestedLimit);
  std::pair<bool, Manager::time_point> requestMigrate(
      std::list<Metadata>::iterator& metadata, uint32_t requestedLogSize);

  // transaction management
  void startTransaction();
  void endTransaction();
  uint64_t transactionTerm();

  // report cache access
  void reportAccess(Cache* cache);

 private:
  // methods to check and affect global state
  void lock();
  void unlock();
  bool isLocked() const;
  void toggleResizing();
  bool isResizing() const;

  // periodically run to rebalance allocations globally
  void rebalance();

  // methods to adjust individual caches
  void resizeCache(std::list<Metadata>::iterator& metadata, uint64_t newLimit);
  void leaseTable(std::list<Metadata>::iterator& metadata, uint32_t logSize);
  void reclaimTables(std::list<Metadata>::iterator& metadata,
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
