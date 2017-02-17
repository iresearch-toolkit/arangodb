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

#include "Cache/Manager.h"
#include "Basics/Common.h"
#include "Basics/asio-helper.h"
#include "Cache/Cache.h"
#include "Cache/CachedValue.h"
#include "Cache/FrequencyBuffer.h"
#include "Cache/ManagerTasks.h"
#include "Cache/Metadata.h"
#include "Cache/PlainCache.h"
#include "Cache/State.h"
#include "Cache/TransactionalCache.h"

#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <list>
#include <memory>
#include <set>
#include <stack>
#include <utility>

#include <iostream>  // TODO

using namespace arangodb::cache;

static constexpr size_t BUCKET_SIZE = 64;
static constexpr size_t TABLE_LOG_SIZE_ADJUSTMENT = 6;
static constexpr size_t MIN_TABLE_LOG_SIZE = 3;
static constexpr size_t MIN_LOG_SIZE = 10;
static constexpr size_t MIN_CACHE_SIZE = 1024;
// use 16 for sizeof std::list node -- should be valid for most libraries
static constexpr uint64_t CACHE_RECORD_OVERHEAD = sizeof(Metadata) + 16;
// assume at most 16 slots in each stack -- TODO: check validity
static constexpr uint64_t TABLE_LISTS_OVERHEAD = 32 * 16 * 8;

Manager::Manager(boost::asio::io_service* ioService, uint64_t globalLimit)
    : _state(),
      _accessStats((globalLimit >= (1024ULL * 1024ULL * 1024ULL))
                       ? ((1024ULL * 1024ULL) / sizeof(std::shared_ptr<Cache>))
                       : (globalLimit / 8192ULL)),
      _accessCounter(0),
      _caches(),
      _globalSoftLimit(globalLimit),
      _globalHardLimit(globalLimit),
      _globalAllocation(sizeof(Manager) + TABLE_LISTS_OVERHEAD +
                        _accessStats.memoryUsage()),
      _openTransactions(0),
      _transactionTerm(0),
      _ioService(ioService),
      _resizeAttempt(0),
      _outstandingTasks(0) {
  TRI_ASSERT(_globalAllocation < _globalSoftLimit);
  TRI_ASSERT(_globalAllocation < _globalHardLimit);
}

Manager::~Manager() {
  _state.lock();
  while (!_caches.empty()) {
    _caches.begin()->lock();
    auto cache = _caches.begin()->cache();
    _caches.begin()->unlock();
    _state.unlock();
    cache->shutdown();
    _state.lock();
  }
  _state.unlock();

  _state.lock();
  freeUnusedTables();
  _state.unlock();
}

std::shared_ptr<Cache> Manager::createCache(Manager::CacheType type,
                                            uint64_t requestedLimit,
                                            bool allowGrowth) {
  std::shared_ptr<Cache> result(nullptr);
  switch (type) {
    case CacheType::Plain:
      result = PlainCache::create(this, requestedLimit, allowGrowth);
      break;
    case CacheType::Transactional:
      result = TransactionalCache::create(this, requestedLimit, allowGrowth);
      break;
    default:
      break;
  }

  return result;
}

// change global cache limit
bool Manager::resize(uint64_t newGlobalLimit) {
  if (newGlobalLimit < MINIMUM_SIZE) {
    return false;
  }

  bool success = true;
  _state.lock();

  if (_state.isSet(State::Flag::resizing)) {
    // if we still have outstanding resize tasks, return unsuccessful
    success = false;
  } else {
    // otherwise we need to actually resize
    _state.toggleFlag(State::Flag::resizing);
    internalResize(newGlobalLimit, true);
  }

  _state.unlock();
  return success;
}

uint64_t Manager::globalLimit() {
  _state.lock();
  uint64_t limit =
      _state.isSet(State::Flag::resizing) ? _globalSoftLimit : _globalHardLimit;
  _state.unlock();

  return limit;
}

uint64_t Manager::globalAllocation() {
  _state.lock();
  uint64_t allocation = _globalAllocation;
  _state.unlock();

  return allocation;
}

void Manager::startTransaction() {
  if (++_openTransactions == 1) {
    _transactionTerm++;
  }
}

void Manager::endTransaction() {
  if (--_openTransactions == 0) {
    _transactionTerm++;
  }
}

uint64_t Manager::transactionTerm() { return _transactionTerm.load(); }

boost::asio::io_service* Manager::ioService() { return _ioService; }

// register and unregister individual caches
Manager::MetadataItr Manager::registerCache(
    Cache* cache, uint64_t requestedLimit,
    std::function<void(Cache*)> deleter) {
  uint32_t logSize = 0;
  uint32_t tableLogSize = MIN_TABLE_LOG_SIZE;
  for (; (1ULL << logSize) < requestedLimit; logSize++) {
  }
  uint64_t grantedLimit = 1ULL << logSize;
  if (logSize > (TABLE_LOG_SIZE_ADJUSTMENT + MIN_TABLE_LOG_SIZE)) {
    tableLogSize = logSize - TABLE_LOG_SIZE_ADJUSTMENT;
  }

  _state.lock();

  while (logSize >= MIN_LOG_SIZE) {
    uint64_t tableAllocation =
        _tables[tableLogSize].empty() ? tableSize(tableLogSize) : 0;
    if (increaseAllowed(grantedLimit + tableAllocation +
                        CACHE_RECORD_OVERHEAD)) {
      break;
    }

    grantedLimit >>= 1U;
    logSize--;
    if (tableLogSize > MIN_TABLE_LOG_SIZE) {
      tableLogSize--;
    }
  }

  if (logSize < MIN_LOG_SIZE) {
    _state.unlock();
    throw std::bad_alloc();
  }

  _globalAllocation += (grantedLimit + CACHE_RECORD_OVERHEAD);
  _caches.emplace_front(std::shared_ptr<Cache>(cache, deleter), grantedLimit);
  auto metadata = _caches.begin();
  metadata->lock();
  leaseTable(metadata, tableLogSize);
  metadata->unlock();
  _state.unlock();

  return metadata;
}

void Manager::unregisterCache(Manager::MetadataItr& metadata) {
  _state.lock();

  if (_caches.size() == 0) {
    _state.unlock();
    return;
  }

  metadata->lock();
  _globalAllocation -= (metadata->hardLimit() + CACHE_RECORD_OVERHEAD);
  reclaimTables(metadata);
  _accessStats.purgeRecord(metadata->cache());
  metadata->unlock();

  _caches.erase(metadata);

  _state.unlock();
}

std::pair<bool, Manager::time_point> Manager::requestResize(
    Manager::MetadataItr& metadata, uint64_t requestedLimit) {
  Manager::time_point nextRequest = futureTime(30);
  bool allowed = false;

  _state.lock();
  metadata->lock();

  if ((requestedLimit < metadata->hardLimit()) ||
      increaseAllowed(requestedLimit - metadata->hardLimit())) {
    allowed = true;
    nextRequest = std::chrono::steady_clock::now();
    resizeCache(metadata, requestedLimit);
  } else {
    metadata->unlock();
  }
  _state.unlock();

  return std::pair<bool, Manager::time_point>(allowed, nextRequest);
}

std::pair<bool, Manager::time_point> Manager::requestMigrate(
    Manager::MetadataItr& metadata, uint32_t requestedLogSize) {
  Manager::time_point nextRequest = futureTime(30);
  bool allowed = false;

  _state.lock();
  if (!_tables[requestedLogSize].empty() ||
      increaseAllowed(tableSize(requestedLogSize))) {
    allowed = true;
  }
  if (allowed) {
    metadata->lock();
    if (metadata->isSet(State::Flag::migrating)) {
      allowed = false;
      metadata->unlock();
    } else {
      nextRequest = std::chrono::steady_clock::now();
      migrateCache(metadata, requestedLogSize);  // unlocks metadata
    }
  }
  _state.unlock();

  return std::pair<bool, Manager::time_point>(allowed, nextRequest);
}

void Manager::reportAccess(std::shared_ptr<Cache> cache) {
  if (((++_accessCounter) & 0x7FULL) == 0) {  // record 1 in 128
    _accessStats.insertRecord(cache);
  }
}

std::shared_ptr<Manager::PriorityList> Manager::priorityList() {
  TRI_ASSERT(_state.isLocked());
  std::shared_ptr<PriorityList> list(new PriorityList());
  list->reserve(_caches.size());

  // catalog accessed caches
  auto stats = _accessStats.getFrequencies();
  std::set<Cache*> accessed;
  for (auto s : *stats) {
    accessed.emplace(s.first.get());
  }

  // gather all unaccessed caches at beginning of list
  for (auto m : _caches) {
    m.lock();
    auto cache = m.cache();
    m.unlock();

    auto found = accessed.find(cache.get());
    if (found == accessed.end()) {
      list->emplace_back(cache);
    }
  }

  // gather all accessed caches in order
  for (auto s : *stats) {
    list->emplace_back(s.first);
  }

  return list;
}

void Manager::rebalance() {
  _state.lock();
  if (_state.isSet(State::Flag::resizing)) {
    _state.unlock();
    return;
  }

  // start rebalancing
  _state.toggleFlag(State::Flag::rebalancing);

  // determine strategy

  // allow background tasks if more than 7/8ths full
  bool allowTasks =
      _globalAllocation > (_globalHardLimit - (_globalHardLimit >> 3));

  // be aggressive if more than 3/4ths full
  bool beAggressive =
      _globalAllocation > (_globalHardLimit - (_globalHardLimit >> 2));

  // aim for 1/4th with background tasks, 1/8th if no tasks but aggressive, no
  // goal otherwise
  uint64_t goal = beAggressive ? (allowTasks ? (_globalAllocation >> 2)
                                             : (_globalAllocation >> 3))
                               : 0;

  // get stats on cache access to prioritize freeing from less frequently used
  // caches first, so more frequently used ones stay large
  std::shared_ptr<PriorityList> cacheList = priorityList();

  // just adjust limits
  uint64_t reclaimed =
      resizeAllCaches(cacheList, allowTasks, beAggressive, goal);
  _globalAllocation -= reclaimed;

  _state.toggleFlag(State::Flag::rebalancing);
  _state.unlock();
}

void Manager::resizeCache(Manager::MetadataItr& metadata, uint64_t newLimit) {
  TRI_ASSERT(_state.isLocked());
  TRI_ASSERT(metadata->isLocked());

  if (metadata->usage() <= newLimit) {
    bool success = metadata->adjustLimits(newLimit, newLimit);
    TRI_ASSERT(success);
    metadata->unlock();
    return;
  }

  bool success = metadata->adjustLimits(newLimit, metadata->hardLimit());
  TRI_ASSERT(success);
  metadata->toggleFlag(State::Flag::resizing);
  metadata->unlock();

  auto task = std::make_shared<FreeMemoryTask>(this, metadata);
  bool dispatched = task->dispatch();
  if (!dispatched) {
    // TODO: decide what to do if we don't have an io_service
    std::cerr << "NO SERVICE TO DISPATCH FREE MEMORY TASK" << std::endl;
  }
}

void Manager::migrateCache(Manager::MetadataItr& metadata, uint32_t logSize) {
  TRI_ASSERT(_state.isLocked());
  TRI_ASSERT(metadata->isLocked());

  leaseTable(metadata, logSize);
  metadata->toggleFlag(State::Flag::migrating);
  metadata->unlock();

  auto task = std::make_shared<MigrateTask>(this, metadata);
  bool dispatched = task->dispatch();
  if (!dispatched) {
    // TODO: decide what to do if we don't have an io_service
    std::cerr << "NO SERVICE TO DISPATCH MIGRATE TASK" << std::endl;
    metadata->lock();
    reclaimTables(metadata, true);
    metadata->unlock();
  }
}

void Manager::leaseTable(Manager::MetadataItr& metadata, uint32_t logSize) {
  TRI_ASSERT(_state.isLocked());
  TRI_ASSERT(metadata->isLocked());

  uint8_t* table;
  if (_tables[logSize].empty()) {
    table = new uint8_t[tableSize(logSize)];
    memset(table, 0, tableSize(logSize));
    _globalAllocation += tableSize(logSize);
  } else {
    table = _tables[logSize].top();
    _tables[logSize].pop();
  }

  // if main null, main, otherwise auxiliary
  metadata->grantAuxiliaryTable(table, logSize);
  if (metadata->table() == nullptr) {
    metadata->swapTables();
  }
}

void Manager::reclaimTables(Manager::MetadataItr& metadata,
                            bool auxiliaryOnly) {
  TRI_ASSERT(_state.isLocked());
  TRI_ASSERT(metadata->isLocked());

  uint8_t* table;
  uint32_t logSize;

  logSize = metadata->auxiliaryLogSize();
  table = metadata->releaseAuxiliaryTable();
  if (table != nullptr) {
    _tables[logSize].push(table);
  }

  if (auxiliaryOnly) {
    return;
  }

  logSize = metadata->logSize();
  table = metadata->releaseTable();
  if (table != nullptr) {
    _tables[logSize].push(table);
  }
}

bool Manager::increaseAllowed(uint64_t increase) const {
  TRI_ASSERT(_state.isLocked());
  if (_state.isSet(State::Flag::resizing) &&
      (_globalAllocation < _globalSoftLimit)) {
    return (increase < (_globalSoftLimit - _globalAllocation));
  }

  return (increase < (_globalHardLimit - _globalAllocation));
}

uint64_t Manager::tableSize(uint32_t logSize) const {
  return (BUCKET_SIZE * (1ULL << logSize));
}

Manager::time_point Manager::futureTime(uint64_t secondsFromNow) {
  return (std::chrono::steady_clock::now() +
          std::chrono::seconds(secondsFromNow));
}

void Manager::freeUnusedTables() {
  TRI_ASSERT(_state.isLocked());
  for (size_t i = 0; i < 32; i++) {
    while (!_tables[i].empty()) {
      uint8_t* table = _tables[i].top();
      delete[] table;
      _tables[i].pop();
    }
  }
}

void Manager::internalResize(uint64_t newGlobalLimit, bool firstAttempt) {
  TRI_ASSERT(_state.isLocked());
  bool done = false;
  std::shared_ptr<PriorityList> cacheList(nullptr);
  uint64_t reclaimed = 0;

  if (firstAttempt) {
    _resizeAttempt = 0;
  }

  // if limit is safe, just set it
  done = adjustGlobalLimitsIfAllowed(newGlobalLimit);

  // see if we can free enough from unused tables
  if (!done) {
    freeUnusedTables();
    done = adjustGlobalLimitsIfAllowed(newGlobalLimit);
  }

  // must resize individual caches
  if (!done) {
    _globalSoftLimit = newGlobalLimit;

    // get stats on cache access to prioritize freeing from less frequently used
    // caches first, so more frequently used ones stay large
    cacheList = priorityList();

    // first just adjust limits down to usage
    reclaimed = resizeAllCaches(cacheList, true, true,
                                _globalAllocation - _globalSoftLimit);
    _globalAllocation -= reclaimed;
    done = adjustGlobalLimitsIfAllowed(newGlobalLimit);
  }

  // still haven't freed enough, now try cutting allocations more aggressively
  // by allowing use of background tasks to actually free memory from caches
  if (!done) {
    if ((_resizeAttempt % 2) == 0) {
      reclaimed = resizeAllCaches(cacheList, false, true,
                                  _globalAllocation - _globalSoftLimit);
    } else {
      reclaimed =
          migrateAllCaches(cacheList, _globalAllocation - _globalSoftLimit);
    }
  }

  if (done) {
    _state.toggleFlag(State::Flag::resizing);
  }
}

bool Manager::adjustGlobalLimitsIfAllowed(uint64_t newGlobalLimit) {
  TRI_ASSERT(_state.isLocked());
  if (newGlobalLimit < _globalAllocation) {
    return false;
  }

  _globalSoftLimit = newGlobalLimit;
  _globalHardLimit = newGlobalLimit;

  return true;
}

uint64_t Manager::resizeAllCaches(std::shared_ptr<PriorityList> cacheList,
                                  bool noTasks, bool aggressive,
                                  uint64_t goal) {
  TRI_ASSERT(_state.isLocked());
  uint64_t reclaimed = 0;

  for (std::shared_ptr<Cache> c : *cacheList) {
    // skip this cache if it is already resizing or shutdown!
    if (!c->canResize()) {
      continue;
    }

    auto metadata = c->metadata();
    metadata->lock();

    uint64_t newLimit;
    if (aggressive) {
      newLimit =
          (noTasks ? metadata->usage()
                   : std::min(metadata->usage(), metadata->hardLimit() / 4));
    } else {
      newLimit =
          (noTasks ? std::max(metadata->usage(), metadata->hardLimit() / 2)
                   : std::min(metadata->usage(), metadata->hardLimit() / 2));
    }
    newLimit = std::max(newLimit, MIN_CACHE_SIZE);

    reclaimed += metadata->hardLimit() - newLimit;
    resizeCache(metadata, newLimit);  // unlocks cache

    if (goal > 0 && reclaimed >= goal) {
      break;
    }
  }

  return reclaimed;
}

uint64_t Manager::migrateAllCaches(std::shared_ptr<PriorityList> cacheList,
                                   uint64_t goal) {
  TRI_ASSERT(_state.isLocked());
  uint64_t reclaimed = 0;

  for (std::shared_ptr<Cache> c : *cacheList) {
    // skip this cache if it is already migrating or shutdown!
    if (!c->canMigrate()) {
      continue;
    }

    auto metadata = c->metadata();
    metadata->lock();

    uint32_t logSize = metadata->logSize();
    if (logSize > MIN_TABLE_LOG_SIZE) {
      // TODO: ensure new table allocation does not violate _globalHardLimit!
      reclaimed += (tableSize(logSize) - tableSize(logSize - 1));
      migrateCache(metadata, logSize - 1);  // unlocks metadata
    }
    if (metadata->isLocked()) {
      metadata->unlock();
    }

    if (goal > 0 && reclaimed >= goal) {
      break;
    }
  }

  return reclaimed;
}
