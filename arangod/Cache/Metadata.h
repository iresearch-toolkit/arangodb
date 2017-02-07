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

#ifndef ARANGODB_CACHE_METADATA_H
#define ARANGODB_CACHE_METADATA_H

#include "Basics/Common.h"
#include "Cache/Cache.h"

#include <atomic>
#include <cstdint>

namespace arangodb {
namespace cache {

class Cache;  // forward declaration

class Metadata {
 public:
  Metadata(Cache* cache, uint64_t limit, uint8_t* table, uint32_t logSize);

  // record must be locked for both reading and writing!
  void lock();
  void unlock();
  bool isLocked();

  Cache* cache();

  uint32_t logSize();
  uint32_t auxiliaryLogSize();
  uint8_t* table();
  uint8_t* auxiliaryTable();
  uint64_t usage();
  uint64_t softLimit();
  uint64_t hardLimit();

  bool adjustUsageIfAllowed(int64_t usageChange);
  bool adjustLimits(uint64_t softLimit, uint64_t hardLimit);

  bool isMigrating();
  void toggleMigrating();
  void grantAuxiliaryTable(uint8_t* table, uint32_t logSize);
  void swapTables();

  uint8_t* releaseTable();
  uint8_t* releaseAuxiliaryTable();

 private:
  // simple state variable for locking and other purposes
  std::atomic<uint32_t> _state;

  // state flags
  static uint32_t FLAG_LOCK;
  static uint32_t FLAG_MIGRATING;
  static uint32_t FLAG_RESIZING;

  // pointer to underlying cache
  Cache* _cache;

  // vital information about memory usage
  uint64_t _usage;
  uint64_t _softLimit;
  uint64_t _hardLimit;

  // information about table leases
  uint8_t* _table;
  uint8_t* _auxiliaryTable;
  uint32_t _logSize;
  uint32_t _auxiliaryLogSize;
};

};  // end namespace cache
};  // end namespace arangodb

#endif
