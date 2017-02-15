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
#include "Cache/State.h"

#include <atomic>
#include <cstdint>

namespace arangodb {
namespace cache {

class Cache;  // forward declaration

class Metadata {
 public:
  Metadata(std::shared_ptr<Cache> cache, uint64_t limit,
           uint8_t* table = nullptr, uint32_t logSize = 0);
  Metadata(Metadata const& other);

  // record must be locked for both reading and writing!
  void lock();
  void unlock();
  bool isLocked() const;

  std::shared_ptr<Cache> cache() const;

  uint32_t logSize() const;
  uint32_t auxiliaryLogSize() const;
  uint8_t* table() const;
  uint8_t* auxiliaryTable() const;
  uint64_t usage() const;
  uint64_t softLimit() const;
  uint64_t hardLimit() const;

  bool adjustUsageIfAllowed(int64_t usageChange);
  bool adjustLimits(uint64_t softLimit, uint64_t hardLimit);

  void grantAuxiliaryTable(uint8_t* table, uint32_t logSize);
  void swapTables();

  uint8_t* releaseTable();
  uint8_t* releaseAuxiliaryTable();

  bool isSet(State::Flag flag) const;
  void toggleFlag(State::Flag flag);

 private:
  State _state;

  // pointer to underlying cache
  std::shared_ptr<Cache> _cache;

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
