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
#include "Random/RandomGenerator.h"

#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <list>
#include <memory>
#include <stack>
#include <utility>

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
                       ? (128ULL * 1024ULL)
                       : (globalLimit / 8192ULL)),
      _caches(),
      _globalSoftLimit(globalLimit),
      _globalHardLimit(globalLimit),
      _globalAllocation(sizeof(Manager) + TABLE_LISTS_OVERHEAD +
                        _accessStats.memoryUsage()),
      _openTransactions(0),
      _transactionTerm(0),
      _ioService(ioService) {
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
  bool done = false;
  bool success = true;
  std::unique_ptr<Manager::StatBuffer::stats_t> stats(nullptr);
  uint64_t reclaimed = 0;

  _state.lock();

  // if limit is safe, just set it
  done = adjustGlobalLimitsIfAllowed(newGlobalLimit);

  // otherwise see if we can free enough from unused tables
  if (!done) {
    freeUnusedTables();
    done = adjustGlobalLimitsIfAllowed(newGlobalLimit);
  }

  // otherwise if we still have outstanding resize tasks, return unsuccessful
  if (!done && _state.isSet(State::Flag::resizing)) {
    done = true;
    success = false;
  }

  // must resize individual caches
  if (!done) {
    _state.toggleFlag(State::Flag::resizing);
    _globalSoftLimit = newGlobalLimit;

    // get stats on cache access to prioritize freeing from less frequently used
    // caches first, so more frequently used ones stay large
    stats.reset(_accessStats.getFrequencies());

    // first just adjust limits down to usage
    reclaimed = resizeAllCaches(stats.get(), true, true,
                                _globalAllocation - _globalSoftLimit);
    _globalAllocation -= reclaimed;
    done = adjustGlobalLimitsIfAllowed(newGlobalLimit);
  }

  // still haven't freed enough, now try cutting allocations more aggressively
  // by allowing use of background tasks to actually free memory from caches
  if (!done) {
    reclaimed = resizeAllCaches(stats.get(), false, true,
                                _globalAllocation - _globalSoftLimit);
  }

  // TODO: handle case where we still need to free more memory

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

  metadata->lock();
  _globalAllocation -= (metadata->hardLimit() + CACHE_RECORD_OVERHEAD);
  reclaimTables(metadata);
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

  TRI_ASSERT(requestedLimit > metadata->hardLimit());
  if ((requestedLimit < metadata->hardLimit()) ||
      increaseAllowed(requestedLimit - metadata->hardLimit())) {
    bool success = metadata->adjustLimits(requestedLimit, requestedLimit);
    if (success) {
      allowed = true;
      _globalAllocation += (requestedLimit - metadata->hardLimit());
    }
  }

  metadata->unlock();
  _state.unlock();

  return std::pair<bool, Manager::time_point>(allowed, nextRequest);
}

std::pair<bool, Manager::time_point> Manager::requestMigrate(
    Manager::MetadataItr& metadata, uint32_t requestedLogSize) {
  Manager::time_point nextRequest = futureTime(30);
  bool allowed = false;

  _state.lock();
  metadata->lock();

  if (!_tables[requestedLogSize].empty() ||
      increaseAllowed(tableSize(requestedLogSize))) {
    allowed = true;
  }
  if (allowed) {
    leaseTable(metadata, requestedLogSize);
  }

  metadata->unlock();
  _state.unlock();

  MigrateTask task(this, metadata);
  bool dispatched = task.dispatch();
  if (!dispatched) {
    // TODO: decide what to do if we don't have an io_service
    allowed = false;
    _state.lock();
    metadata->lock();
    reclaimTables(metadata, true);
    metadata->unlock();
    _state.unlock();
  }

  return std::pair<bool, Manager::time_point>(allowed, nextRequest);
}

void Manager::reportAccess(Cache* cache) {
  if (0U == RandomGenerator::interval(100U)) {
    _accessStats.insertRecord(cache);
  }
}

void Manager::rebalance() {
  // TODO: implement this
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

  FreeMemoryTask task(this, metadata);
  bool dispatched = task.dispatch();
  if (!dispatched) {
    // TODO: decide what to do if we don't have an io_service
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

bool Manager::adjustGlobalLimitsIfAllowed(uint64_t newGlobalLimit) {
  TRI_ASSERT(_state.isLocked());
  if (newGlobalLimit < _globalAllocation) {
    return false;
  }

  _globalSoftLimit = newGlobalLimit;
  _globalHardLimit = newGlobalLimit;

  return true;
}

uint64_t Manager::resizeAllCaches(Manager::StatBuffer::stats_t* stats,
                                  bool noTasks, bool aggressive,
                                  uint64_t goal) {
  TRI_ASSERT(_state.isLocked());
  uint64_t reclaimed = 0;

  for (auto s : *stats) {
    auto metadata = s.first->metadata();
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
