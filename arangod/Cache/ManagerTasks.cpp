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

#include "Cache/ManagerTasks.h"
#include "Basics/Common.h"
#include "Basics/asio-helper.h"
#include "Cache/Cache.h"
#include "Cache/Manager.h"
#include "Cache/Metadata.h"

using namespace arangodb::cache;

FreeMemoryTask::FreeMemoryTask(Manager* manager, Manager::MetadataItr& metadata)
    : _manager(manager) {
  metadata->lock();
  _cache = metadata->cache();
  metadata->unlock();
}

FreeMemoryTask::~FreeMemoryTask() {}

bool FreeMemoryTask::dispatch() {
  auto ioService = _manager->ioService();
  if (ioService == nullptr) {
    return false;
  }

  _manager->_outstandingTasks++;
  auto self = shared_from_this();
  ioService->post([self, this]() -> void { run(); });

  return true;
}

void FreeMemoryTask::run() {
  _cache->freeMemory();
  auto metadata = _cache->metadata();
  _manager->_state.lock();
  metadata->lock();
  uint64_t reclaimed = metadata->hardLimit() - metadata->softLimit();
  metadata->adjustLimits(metadata->softLimit(), metadata->softLimit());
  metadata->toggleFlag(State::Flag::resizing);
  metadata->unlock();
  _manager->_globalAllocation -= reclaimed;
  _manager->_state.unlock();

  // if last outstanding task, let manager continue resizing process
  if (--(_manager->_outstandingTasks) == 0) {
    _manager->_state.lock();
    _manager->internalResize(_manager->_globalSoftLimit, false);
    _manager->_state.unlock();
  }
}

MigrateTask::MigrateTask(Manager* manager, Manager::MetadataItr& metadata)
    : _manager(manager) {
  metadata->lock();
  _cache = metadata->cache();
  metadata->unlock();
}

MigrateTask::~MigrateTask() {}

bool MigrateTask::dispatch() {
  auto ioService = _manager->ioService();
  if (ioService == nullptr) {
    return false;
  }

  _manager->_outstandingTasks++;
  auto self = shared_from_this();
  ioService->post([self, this]() -> void { run(); });

  return true;
}

void MigrateTask::run() {
  // do the actual migration
  _cache->migrate();

  auto metadata = _cache->metadata();
  _manager->_state.lock();
  metadata->lock();
  _manager->reclaimTables(metadata, true);
  metadata->toggleFlag(State::Flag::migrating);
  metadata->unlock();
  _manager->_state.unlock();

  // if last task, let manager continue resizing process if necessary
  if (--(_manager->_outstandingTasks) == 0) {
    _manager->_state.lock();
    if (_manager->_state.isSet(State::Flag::resizing)) {
      _manager->internalResize(_manager->_globalSoftLimit, false);
    }
    _manager->_state.unlock();
  }
}
