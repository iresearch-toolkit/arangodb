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

#include "Cache/Cache.h"
#include "Basics/Common.h"
#include "Basics/fasthash.h"
#include "Cache/CachedValue.h"
#include "Cache/Manager.h"
#include "Cache/Metadata.h"
#include "Cache/State.h"

#include <stdint.h>
#include <algorithm>
#include <chrono>
#include <list>

using namespace arangodb::cache;

Cache::Finding::Finding(CachedValue* v) : _value(v) {
  if (_value != nullptr) {
    _value->lease();
  }
}

Cache::Finding::~Finding() {
  if (_value != nullptr) {
    _value->release();
  }
}

void Cache::Finding::reset(CachedValue* v) {
  if (_value != nullptr) {
    _value->release();
  }

  _value = v;
  if (_value != nullptr) {
    _value->lease();
  }
}

bool Cache::Finding::found() const { return (_value != nullptr); }

CachedValue const* Cache::Finding::value() const { return _value; }

CachedValue* Cache::Finding::copy() const {
  return ((_value == nullptr) ? nullptr : _value->copy());
}

void Cache::destroy(std::shared_ptr<Cache> cache) {
  if (cache.get() != nullptr) {
    cache->shutdown();
  }
}

uint64_t Cache::limit() {
  uint64_t limit = 0;
  _state.lock();
  if (isOperational()) {
    _metadata->lock();
    limit = _metadata->softLimit();
    _metadata->unlock();
  }
  _state.unlock();
  return limit;
}

uint64_t Cache::usage() {
  uint64_t usage = 0;
  _state.lock();
  if (isOperational()) {
    _metadata->lock();
    usage = _metadata->usage();
    _metadata->unlock();
  }
  _state.unlock();
  return usage;
}

void Cache::requestResize(uint64_t requestedLimit) {
  _state.lock(10LL);
  if (_allowGrowth && (std::chrono::steady_clock::now() > _resizeRequestTime)) {
    _metadata->lock();
    uint64_t newLimit =
        (requestedLimit > 0) ? requestedLimit : (_metadata->hardLimit() * 2);
    _metadata->unlock();
    auto result = _manager->requestResize(_metadata, newLimit);
    _resizeRequestTime = result.second;
  }
  _state.unlock();
}

Cache::Cache(Manager* manager, uint64_t requestedLimit, bool allowGrowth,
             std::function<void(Cache*)> deleter)
    : _state(),
      _allowGrowth(allowGrowth),
      _evictionStats(1024),
      _insertionCount(0),
      _manager(manager),
      _openOperations(),
      _migrateRequestTime(std::chrono::steady_clock::now()),
      _resizeRequestTime(std::chrono::steady_clock::now()) {
  try {
    _metadata = _manager->registerCache(this, requestedLimit, deleter);
  } catch (std::bad_alloc) {
    // could not register, mark as non-operational
    _state.toggleFlag(State::Flag::shutdown);
  }
}

bool Cache::isOperational() const {
  TRI_ASSERT(_state.isLocked());
  return !_state.isSet(State::Flag::shutdown);
}

void Cache::startOperation() { ++_openOperations; }

void Cache::endOperation() { --_openOperations; }

bool Cache::isMigrating() const {
  TRI_ASSERT(_state.isLocked());
  return _state.isSet(State::Flag::migrating);
}

void Cache::requestMigrate(uint32_t requestedLogSize) {
  if ((++_insertionCount & 0xFFF) == 0) {
    auto stats = _evictionStats.getFrequencies();
    if (((stats->size() == 1) &&
         ((*stats)[0].first == static_cast<uint8_t>(Stat::eviction))) ||
        ((stats->size() == 2) &&
         ((*stats)[0].second * 16 > (*stats)[1].second))) {
      _state.lock(10LL);
      if (!isMigrating() &&
          (std::chrono::steady_clock::now() > _resizeRequestTime)) {
        _metadata->lock();
        uint32_t newLogSize = (requestedLogSize > 0)
                                  ? requestedLogSize
                                  : (_metadata->logSize() + 1);
        _metadata->unlock();
        auto result = _manager->requestMigrate(_metadata, newLogSize);
        _resizeRequestTime = result.second;
      }
      _state.unlock();
    }
  }
}

void Cache::freeValue(CachedValue* value) {
  while (value->refCount.load() > 0) {
    usleep(1);
  }

  delete value;
}

bool Cache::reclaimMemory(uint64_t size) {
  _metadata->lock();
  _metadata->adjustUsageIfAllowed(-static_cast<int64_t>(size));
  bool underLimit = (_metadata->softLimit() >= _metadata->usage());
  _metadata->unlock();

  return underLimit;
}

uint32_t Cache::hashKey(void const* key, uint32_t keySize) const {
  return std::max(static_cast<uint32_t>(1),
                  fasthash32(key, keySize, 0xdeadbeefUL));
}

void Cache::recordStat(Cache::Stat stat) {
  _evictionStats.insertRecord(static_cast<uint8_t>(stat));
}

Manager::MetadataItr& Cache::metadata() { return _metadata; }

void Cache::shutdown() {
  _state.lock();
  if (isOperational()) {
    while (_openOperations.load() > 0) {
      usleep(1);
    }

    _state.clear();
    _state.toggleFlag(State::Flag::shutdown);

    clearTables();
    _manager->unregisterCache(_metadata);
  }
  _state.unlock();
}

bool Cache::canResize() {
  bool allowed = true;
  _state.lock();
  if (isOperational()) {
    _metadata->lock();
    if (_metadata->isSet(State::Flag::resizing)) {
      allowed = false;
    }
    _metadata->unlock();
  } else {
    allowed = false;
  }
  _state.unlock();

  return allowed;
}

bool Cache::canMigrate() {
  bool allowed = true;
  _state.lock();
  if (isOperational()) {
    _metadata->lock();
    if (_metadata->isSet(State::Flag::migrating)) {
      allowed = false;
    }
    _metadata->unlock();
  } else {
    allowed = false;
  }
  _state.unlock();

  return allowed;
}
