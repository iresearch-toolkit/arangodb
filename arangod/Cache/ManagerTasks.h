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

#ifndef ARANGODB_CACHE_MANAGER_TASKS_H
#define ARANGODB_CACHE_MANAGER_TASKS_H

#include "Basics/Common.h"
#include "Cache/Cache.h"
#include "Cache/Manager.h"
#include "Cache/Metadata.h"

#include <stdint.h>
#include <atomic>
#include <memory>

namespace arangodb {
namespace cache {

class FreeMemoryTask : public std::enable_shared_from_this<FreeMemoryTask> {
 private:
  Manager* _manager;
  std::shared_ptr<Cache> _cache;

 public:
  FreeMemoryTask() = delete;
  FreeMemoryTask(FreeMemoryTask const&) = delete;
  FreeMemoryTask& operator=(FreeMemoryTask const&) = delete;

  FreeMemoryTask(Manager* manager, Manager::MetadataItr& metadata);
  ~FreeMemoryTask();

  bool dispatch();

 private:
  void run();
};

class MigrateTask : public std::enable_shared_from_this<MigrateTask> {
 private:
  Manager* _manager;
  std::shared_ptr<Cache> _cache;

 public:
  MigrateTask() = delete;
  MigrateTask(MigrateTask const&) = delete;
  MigrateTask& operator=(MigrateTask const&) = delete;

  MigrateTask(Manager* manager, Manager::MetadataItr& metadata);
  ~MigrateTask();

  bool dispatch();

 private:
  void run();
};

};  // end namespace cache
};  // end namespace arangodb

#endif
