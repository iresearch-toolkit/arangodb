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
#include "Cache/Cache.h"
#include "Cache/CachedValue.h"
#include "Cache/FrequencyBuffer.h"
#include "Cache/Metadata.h"

#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
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

uint32_t Manager::FLAG_LOCK = 0x01;
uint32_t Manager::FLAG_MIGRATING = 0x02;
uint32_t Manager::FLAG_RESIZING = 0x04;
uint32_t Manager::FLAG_REBALANCING = 0x08;

Manager::Manager(uint64_t globalLimit)
    : _state(0),
      _accessStats((globalLimit >= (1024 * 1024 * 1024))
                       ? (128 * 1024)
                       : (globalLimit / 8192ULL)),
      _caches(),
      _globalSoftLimit(globalLimit),
      _globalHardLimit(globalLimit),
      _globalAllocation(sizeof(Manager) + TABLE_LISTS_OVERHEAD +
                        _accessStats.memoryUsage()),
      _openTransactions(0),
      _transactionTerm(0) {
  TRI_ASSERT(_globalAllocation < _globalSoftLimit);
  TRI_ASSERT(_globalAllocation < _globalHardLimit);
}

Manager::~Manager() {
  while (_caches.size() > 0) {
    // wait for caches to unregister
    usleep(1);
  }

  lock();
  freeUnusedTables();
  unlock();
}

// change global cache limit
bool Manager::resize(uint64_t newGlobalLimit) {
  bool done = false;
  bool success = true;
  std::unique_ptr<Manager::StatBuffer::stats_t> stats(nullptr);
  uint64_t reclaimed = 0;

  lock();

  // if limit is safe, just set it
  done = adjustGlobalLimitsIfAllowed(newGlobalLimit);

  // otherwise see if we can free enough from unused tables
  if (!done) {
    freeUnusedTables();
    done = adjustGlobalLimitsIfAllowed(newGlobalLimit);
  }

  // otherwise if we still have outstanding resize tasks, return unsuccessful
  if (!done && isResizing()) {
    done = true;
    success = false;
  }

  // must resize individual caches
  if (!done) {
    toggleResizing();
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

  unlock();
  return success;
}

uint64_t Manager::globalLimit() {
  lock();
  uint64_t limit = isResizing() ? _globalSoftLimit : _globalHardLimit;
  unlock();

  return limit;
}

uint64_t Manager::globalAllocation() {
  lock();
  uint64_t allocation = _globalAllocation;
  unlock();

  return allocation;
}

// register and unregister individual caches
std::list<Metadata>::iterator Manager::registerCache(Cache* cache,
                                                     uint64_t requestedLimit) {
  uint32_t logSize = 0;
  uint32_t tableLogSize = MIN_TABLE_LOG_SIZE;
  for (; (1ULL << logSize) < requestedLimit; logSize++) {
  }
  uint64_t grantedLimit = 1ULL << logSize;
  if (logSize > (TABLE_LOG_SIZE_ADJUSTMENT + MIN_TABLE_LOG_SIZE)) {
    tableLogSize = logSize - TABLE_LOG_SIZE_ADJUSTMENT;
  }

  lock();

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
    unlock();
    throw std::bad_alloc();
  }

  _globalAllocation += (grantedLimit + CACHE_RECORD_OVERHEAD);
  _caches.emplace_front(cache, grantedLimit);
  auto metadata = _caches.begin();
  metadata->lock();
  leaseTable(metadata, tableLogSize);
  metadata->unlock();
  unlock();

  return metadata;
}

void Manager::unregisterCache(std::list<Metadata>::iterator& metadata) {
  lock();

  metadata->lock();
  _globalAllocation -= (metadata->hardLimit() + CACHE_RECORD_OVERHEAD);
  reclaimTables(metadata);
  metadata->unlock();

  _caches.erase(metadata);

  unlock();
}

std::pair<bool, Manager::time_point> Manager::requestResize(
    std::list<Metadata>::iterator& metadata, uint64_t requestedLimit) {
  Manager::time_point nextRequest = futureTime(30);
  bool allowed = false;

  lock();
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
  unlock();

  return std::pair<bool, Manager::time_point>(allowed, nextRequest);
}

std::pair<bool, Manager::time_point> Manager::requestMigrate(
    std::list<Metadata>::iterator& metadata, uint32_t requestedLogSize) {
  Manager::time_point nextRequest = futureTime(30);
  bool allowed = false;

  lock();
  metadata->lock();

  if (!_tables[requestedLogSize].empty() ||
      increaseAllowed(tableSize(requestedLogSize))) {
    allowed = true;
  }
  if (allowed) {
    leaseTable(metadata, requestedLogSize);
  }

  metadata->unlock();
  unlock();

  // TODO: queue actual migrate task

  return std::pair<bool, Manager::time_point>(allowed, nextRequest);
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

void Manager::reportAccess(Cache* cache) {
  // TODO: only record ~1% of accesses -- fast prng?
  _accessStats.insertRecord(cache);
}

void Manager::lock() {
  uint32_t expected;
  while (true) {
    // expect unlocked, but need to preserve migrating status
    expected = _state.load() & (~FLAG_LOCK);
    bool success = _state.compare_exchange_weak(
        expected, (expected | FLAG_LOCK));  // try to lock
    if (success) {
      break;
    }
    // TODO: exponential back-off for failure?
  }
}

void Manager::unlock() {
  TRI_ASSERT(isLocked());
  _state &= ~FLAG_LOCK;
}

bool Manager::isLocked() const { return ((_state.load() & FLAG_LOCK) > 0); }

void Manager::toggleResizing() {
  TRI_ASSERT(isLocked());
  _state ^= FLAG_RESIZING;
}

bool Manager::isResizing() const {
  TRI_ASSERT(isLocked());
  return ((_state.load() & FLAG_RESIZING) > 0);
}

void Manager::rebalance() {
  // TODO: implement this
}

void Manager::resizeCache(std::list<Metadata>::iterator& metadata,
                          uint64_t newLimit) {
  TRI_ASSERT(isLocked());
  TRI_ASSERT(metadata->isLocked());

  if (metadata->usage() <= newLimit) {
    bool success = metadata->adjustLimits(newLimit, newLimit);
    TRI_ASSERT(success);
    metadata->unlock();
    return;
  }

  bool success = metadata->adjustLimits(newLimit, metadata->hardLimit());
  TRI_ASSERT(success);
  metadata->toggleResizing();
  metadata->unlock();

  // TODO: queue actual free task
}

void Manager::leaseTable(std::list<Metadata>::iterator& metadata,
                         uint32_t logSize) {
  TRI_ASSERT(isLocked());
  TRI_ASSERT(metadata->isLocked());

  uint8_t* table;
  if (_tables[logSize].empty()) {
    table = new uint8_t[tableSize(logSize)];
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

void Manager::reclaimTables(std::list<Metadata>::iterator& metadata,
                            bool auxiliaryOnly) {
  TRI_ASSERT(isLocked());
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
  TRI_ASSERT(isLocked());
  if (isResizing() && (_globalAllocation < _globalSoftLimit)) {
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
  TRI_ASSERT(isLocked());
  for (size_t i = 0; i < 32; i++) {
    while (!_tables[i].empty()) {
      uint8_t* table = _tables[i].top();
      delete[] table;
      _tables[i].pop();
    }
  }
}

bool Manager::adjustGlobalLimitsIfAllowed(uint64_t newGlobalLimit) {
  TRI_ASSERT(isLocked());
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
  TRI_ASSERT(isLocked());
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
