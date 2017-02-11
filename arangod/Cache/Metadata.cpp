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

#include "Cache/Metadata.h"
#include "Cache/Cache.h"

#include <atomic>
#include <cstdint>

using namespace arangodb::cache;

uint32_t Metadata::FLAG_LOCK = 0x01;
uint32_t Metadata::FLAG_MIGRATING = 0x02;
uint32_t Metadata::FLAG_RESIZING = 0x04;

Metadata::Metadata(Cache* cache, uint64_t limit, uint8_t* table,
                   uint32_t logSize)
    : _state(0),
      _cache(cache),
      _usage(0),
      _softLimit(limit),
      _hardLimit(limit),
      _table(table),
      _auxiliaryTable(nullptr),
      _logSize(logSize),
      _auxiliaryLogSize(0) {}

Metadata::Metadata(Metadata const& other)
    : _state(other._state.load()),
      _cache(other._cache),
      _usage(other._usage),
      _softLimit(other._softLimit),
      _hardLimit(other._hardLimit),
      _table(other._table),
      _auxiliaryTable(other._auxiliaryTable),
      _logSize(other._logSize),
      _auxiliaryLogSize(other._auxiliaryLogSize) {}

void Metadata::lock() {
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

void Metadata::unlock() {
  TRI_ASSERT(isLocked());
  _state &= ~FLAG_LOCK;
}

bool Metadata::isLocked() const { return ((_state.load() & FLAG_LOCK) > 0); }

Cache* Metadata::cache() const {
  TRI_ASSERT(isLocked());
  return _cache;
}

uint32_t Metadata::logSize() const {
  TRI_ASSERT(isLocked());
  return _logSize;
}

uint32_t Metadata::auxiliaryLogSize() const {
  TRI_ASSERT(isLocked());
  return _auxiliaryLogSize;
}

uint8_t* Metadata::table() const {
  TRI_ASSERT(isLocked());
  return _table;
}

uint8_t* Metadata::auxiliaryTable() const {
  TRI_ASSERT(isLocked());
  return _auxiliaryTable;
}

uint64_t Metadata::usage() const {
  TRI_ASSERT(isLocked());
  return _usage;
}

uint64_t Metadata::softLimit() const {
  TRI_ASSERT(isLocked());
  return _softLimit;
}

uint64_t Metadata::hardLimit() const {
  TRI_ASSERT(isLocked());
  return _hardLimit;
}

bool Metadata::adjustUsageIfAllowed(int64_t usageChange) {
  TRI_ASSERT(isLocked());

  if (usageChange < 0) {
    _usage -= static_cast<uint64_t>(-usageChange);
    return true;
  }

  if ((static_cast<uint64_t>(usageChange) + _usage <= _softLimit) ||
      ((_usage > _softLimit) &&
       (static_cast<uint64_t>(usageChange) + _usage <= _hardLimit))) {
    _usage += static_cast<uint64_t>(usageChange);
    return true;
  }

  return false;
}

bool Metadata::adjustLimits(uint64_t softLimit, uint64_t hardLimit) {
  TRI_ASSERT(isLocked());

  if (hardLimit < _usage) {
    return false;
  }

  _softLimit = softLimit;
  _hardLimit = hardLimit;

  return true;
}

bool Metadata::isMigrating() const {
  TRI_ASSERT(isLocked());
  return ((_state.load() & FLAG_MIGRATING) > 0);
}

void Metadata::toggleMigrating() {
  TRI_ASSERT(isLocked());
  _state ^= FLAG_MIGRATING;
}

void Metadata::grantAuxiliaryTable(uint8_t* table, uint32_t logSize) {
  TRI_ASSERT(isLocked());
  _auxiliaryTable = table;
  _auxiliaryLogSize = logSize;
}

void Metadata::swapTables() {
  TRI_ASSERT(isLocked());
  std::swap(_table, _auxiliaryTable);
  std::swap(_logSize, _auxiliaryLogSize);
}

bool Metadata::isResizing() const {
  TRI_ASSERT(isLocked());
  return ((_state.load() & FLAG_RESIZING) > 0);
}

void Metadata::toggleResizing() {
  TRI_ASSERT(isLocked());
  _state ^= FLAG_RESIZING;
}

uint8_t* Metadata::releaseTable() {
  TRI_ASSERT(isLocked());
  uint8_t* table = _table;
  _table = nullptr;
  _logSize = 0;
  return table;
}

uint8_t* Metadata::releaseAuxiliaryTable() {
  TRI_ASSERT(isLocked());
  uint8_t* table = _auxiliaryTable;
  _auxiliaryTable = nullptr;
  _auxiliaryLogSize = 0;
  return table;
}
