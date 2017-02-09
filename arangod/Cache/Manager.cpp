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
#include <atomic>
#include <chrono>
#include <list>
#include <utility>

using namespace arangodb::cache;

uint32_t Manager::FLAG_LOCK = 0x01;
uint32_t Manager::FLAG_MIGRATING = 0x02;
uint32_t Manager::FLAG_FREEING = 0x04;

Manager::Manager(uint64_t globalLimit)
    : _state(0),
      _accessStats(256 * 1024),
      _caches(),
      _globalLimit(globalLimit),
      _globalAllocation(0),
      _globalUsage(0),
      _openTransactions(0),
      _transactionTerm(0) {
  // TODO: implement this
}

Manager::~Manager() {
  // TODO: implement this
}

// change global cache limit
void Manager::resize(uint64_t newGlobalLimit) {
  // TODO: implement this
}

// register and unregister individual caches
std::list<Metadata>::iterator Manager::registerCache(Cache* cache,
                                                     uint64_t requestedLimit) {
  // TODO: implement this
  return _caches.end();
}

void Manager::unregisterCache(std::list<Metadata>::iterator& metadata) {
  // TODO: implement this
}

std::pair<bool, std::time_t> Manager::requestResize(
    std::list<Metadata>::iterator& metadata, uint64_t requestedLimit) {
  // TODO: implement this
  return std::pair<bool, uint64_t>(false, 0);
}

std::pair<bool, std::time_t> Manager::requestMigrate(
    std::list<Metadata>::iterator& metadata, uint32_t requestedLogSize) {
  // TODO: implement this
  return std::pair<bool, uint64_t>(false, 0);
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

void Manager::reportAccess(Cache const* cache) {
  // TODO: implement this
}

void Manager::lock() {
  // TODO: implement this
}

void Manager::unlock() {
  // TODO: implement this
}

void Manager::rebalance() {
  // TODO: implement this
}

void Manager::resize(std::list<Metadata>::iterator& metadata,
                     uint64_t newLimit) {
  // TODO: implement this
}

void Manager::lease(std::list<Metadata>::iterator& metadata, uint32_t logSize) {
  // TODO: implement this
}

void Manager::reclaim(std::list<Metadata>::iterator& metadata) {
  // TODO: implement this
}
