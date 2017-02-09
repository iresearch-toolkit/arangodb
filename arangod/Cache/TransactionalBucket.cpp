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

#include "Cache/TransactionalBucket.h"
#include "Basics/Common.h"
#include "Cache/CachedValue.h"

#include <stdint.h>
#include <atomic>

using namespace arangodb::cache;

uint32_t TransactionalBucket::FLAG_LOCK = 0x01;
uint32_t TransactionalBucket::FLAG_MIGRATED = 0x02;
uint32_t TransactionalBucket::FLAG_BLACKLISTED = 0x04;

size_t TransactionalBucket::SLOTS_DATA = 3;
size_t TransactionalBucket::SLOTS_BLACKLIST = 4;

TransactionalBucket::TransactionalBucket() {
  memset(this, 0, sizeof(TransactionalBucket));
}

bool TransactionalBucket::lock(uint64_t transactionTerm, int64_t maxTries) {
  uint32_t expected;
  int64_t attempt = 0;
  while (maxTries < 0 || attempt < maxTries) {
    // expect unlocked, but need to preserve migrating status
    expected = _state.load() & (~FLAG_LOCK);
    bool success = _state.compare_exchange_weak(
        expected, (expected | FLAG_LOCK));  // try to lock
    if (success) {
      updateBlacklistTerm(transactionTerm);
      return true;
    }
    attempt++;
    // TODO: exponential back-off for failure?
  }

  return false;
}

void TransactionalBucket::unlock() {
  TRI_ASSERT(isLocked());
  _state &= ~FLAG_LOCK;
}

bool TransactionalBucket::isLocked() const {
  return ((_state.load() & FLAG_LOCK) > 0);
}

bool TransactionalBucket::isMigrated() const {
  TRI_ASSERT(isLocked());
  return ((_state.load() & FLAG_MIGRATED) > 0);
}

bool TransactionalBucket::isFullyBlacklisted() const {
  TRI_ASSERT(isLocked());
  return ((_state.load() & FLAG_BLACKLISTED) > 0);
}

bool TransactionalBucket::isFull() const {
  TRI_ASSERT(isLocked());
  bool hasEmptySlot = false;
  for (size_t i = 0; i < SLOTS_DATA; i++) {
    size_t slot = SLOTS_DATA - (i + 1);
    if (_cachedHashes[slot] == 0) {
      hasEmptySlot = true;
      break;
    }
  }

  return !hasEmptySlot;
}

CachedValue* TransactionalBucket::find(uint32_t hash, void const* key,
                                       uint32_t keySize, bool moveToFront) {
  TRI_ASSERT(isLocked());
  CachedValue* result = nullptr;

  for (size_t i = 0; i < SLOTS_DATA; i++) {
    if (_cachedHashes[i] == 0) {
      break;
    }
    if (_cachedHashes[i] == hash && _cachedData[i]->sameKey(key, keySize)) {
      result = _cachedData[i];
      if (moveToFront) {
        moveSlot(i, true);
      }
      break;
    }
  }

  return result;
}

void TransactionalBucket::insert(uint32_t hash, CachedValue* value) {
  TRI_ASSERT(isLocked());
  if (isBlacklisted(hash)) {
    return;
  }

  for (size_t i = 0; i < SLOTS_DATA; i++) {
    if (_cachedHashes[i] == 0) {
      // found an empty slot
      _cachedHashes[i] = hash;
      _cachedData[i] = value;
      if (i != 0) {
        moveSlot(i, true);
      }
      return;
    }
  }
}

CachedValue* TransactionalBucket::remove(uint32_t hash, void const* key,
                                         uint32_t keySize) {
  TRI_ASSERT(isLocked());
  CachedValue* value = find(hash, key, keySize, false);
  if (value != nullptr) {
    evict(value, false);
  }

  return value;
}

void TransactionalBucket::blacklist(uint32_t hash, void const* key,
                                    uint32_t keySize) {
  TRI_ASSERT(isLocked());
  // remove key if it's here
  remove(hash, key, keySize);

  if (isFullyBlacklisted()) {
    return;
  }

  for (size_t i = 0; i < SLOTS_BLACKLIST; i++) {
    if (_blacklistHashes[i] == 0) {
      // found an empty slot
      _blacklistHashes[i] = hash;
      return;
    }
  }

  // no empty slot found, fully blacklist
  toggleFullyBlacklisted();
}

bool TransactionalBucket::isBlacklisted(uint32_t hash) const {
  TRI_ASSERT(isLocked());
  if (isFullyBlacklisted()) {
    return true;
  }

  bool blacklisted = false;
  for (size_t i = 0; i < SLOTS_BLACKLIST; i++) {
    if (_blacklistHashes[i] == hash) {
      blacklisted = true;
      break;
    }
  }

  return blacklisted;
}

CachedValue* TransactionalBucket::evictionCandidate() const {
  TRI_ASSERT(isLocked());
  for (size_t i = 0; i < SLOTS_DATA; i++) {
    size_t slot = SLOTS_DATA - (i + 1);
    if (_cachedHashes[slot] == 0) {
      continue;
    }
    if (_cachedData[slot]->isFreeable()) {
      return _cachedData[slot];
    }
  }

  return nullptr;
}

void TransactionalBucket::evict(CachedValue* value, bool optimizeForInsertion) {
  TRI_ASSERT(isLocked());
  for (size_t i = 0; i < SLOTS_DATA; i++) {
    size_t slot = SLOTS_DATA - (i + 1);
    if (_cachedData[slot] == value) {
      // found a match
      _cachedHashes[slot] = 0;
      _cachedData[slot] = nullptr;
      moveSlot(slot, optimizeForInsertion);
      return;
    }
  }
}

void TransactionalBucket::toggleMigrated() {
  TRI_ASSERT(isLocked());
  _state ^= FLAG_MIGRATED;
}

void TransactionalBucket::toggleFullyBlacklisted() {
  TRI_ASSERT(isLocked());
  _state ^= FLAG_BLACKLISTED;
}

void TransactionalBucket::updateBlacklistTerm(uint64_t term) {
  if (term > _blacklistTerm) {
    _blacklistTerm = term;

    if (isFullyBlacklisted()) {
      toggleFullyBlacklisted();
    }

    memset(_blacklistHashes, 0, (SLOTS_BLACKLIST * sizeof(uint32_t)));
  }
}

void TransactionalBucket::moveSlot(size_t slot, bool moveToFront) {
  uint32_t hash = _cachedHashes[slot];
  CachedValue* value = _cachedData[slot];
  size_t i = slot;
  if (moveToFront) {
    // move slot to front
    for (; i >= 1; i--) {
      _cachedHashes[i] = _cachedHashes[i - 1];
      _cachedData[i] = _cachedData[i - 1];
    }
  } else {
    // move slot to back
    for (; (i < SLOTS_DATA - 1) && (_cachedHashes[i + 1] != 0); i++) {
      _cachedHashes[i] = _cachedHashes[i + 1];
      _cachedData[i] = _cachedData[i + 1];
    }
  }
  if (i != slot) {
    _cachedHashes[i] = hash;
    _cachedData[i] = value;
  }
}
