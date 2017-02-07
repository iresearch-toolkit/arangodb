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

#ifndef ARANGODB_CACHE_TRANSACTIONAL_BUCKET_H
#define ARANGODB_CACHE_TRANSACTIONAL_BUCKET_H

#include "Basics/Common.h"
#include "Cache/CachedValue.h"

#include <stdint.h>
#include <atomic>

namespace arangodb {
namespace cache {

struct alignas(64) TransactionalBucket {
  // simple state variable for locking and other purposes
  std::atomic<uint32_t> _state;

  // state flags
  static uint32_t FLAG_LOCK;
  static uint32_t FLAG_MIGRATED;
  static uint32_t FLAG_BLACKLISTED;

  // actual cached entries
  uint32_t _cachedHashes[3];
  CachedValue* _cachedData[3];
  static size_t SLOTS_DATA;

  // blacklist entries for transactional semantics
  uint32_t _blacklistHashes[4];
  uint64_t _blacklistTerm;
  static size_t SLOTS_BLACKLIST;

// padding, if necessary?
#ifdef TRI_PADDING_32
  uint32_t _padding[3];
#endif

  // must lock before using any other operations
  bool lock(int64_t maxWait);
  void unlock();

  // state checkers
  bool isLocked() const;
  bool isMigrated() const;
  bool isFullyBlacklisted() const;
  bool isFull() const;

  // primary functions
  CachedValue* find(uint32_t hash, uint32_t keySize, uint8_t* key) const;
  void insert(uint32_t hash, CachedValue* value);
  CachedValue* remove(uint32_t hash, uint32_t keySize, uint8_t* key);
  void blacklist(uint32_t hash);

  // auxiliary functions
  void updateBlacklistTerm(uint64_t term);
  bool isBlacklisted(uint32_t hash) const;
  CachedValue* evictionCandidate() const;
  void evict(CachedValue* value);
  void toggleMigrated();
};

};  // end namespace cache
};  // end namespace arangodb

#endif
