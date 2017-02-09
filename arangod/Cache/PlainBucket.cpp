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

#include "Cache/PlainBucket.h"
#include "Basics/Common.h"
#include "Cache/CachedValue.h"

#include <stdint.h>
#include <atomic>

using namespace arangodb::cache;

uint32_t PlainBucket::FLAG_LOCK = 0x01;
uint32_t PlainBucket::FLAG_MIGRATED = 0x02;

size_t PlainBucket::SLOTS_DATA = 5;

PlainBucket::PlainBucket() { memset(this, 0, sizeof(PlainBucket)); }

bool PlainBucket::lock(int64_t maxTries) {
  uint32_t expected;
  int64_t attempt = 0;
  while (maxTries < 0 || attempt < maxTries) {
    // expect unlocked, but need to preserve migrating status
    expected = _state.load() & (~FLAG_LOCK);
    bool success = _state.compare_exchange_weak(
        expected, (expected | FLAG_LOCK));  // try to lock
    if (success) {
      return true;
    }
    attempt++;
    // TODO: exponential back-off for failure?
  }

  return false;
}

void PlainBucket::unlock() {
  TRI_ASSERT(isLocked());
  _state &= ~FLAG_LOCK;
}

bool PlainBucket::isLocked() const { return ((_state.load() & FLAG_LOCK) > 0); }

bool PlainBucket::isMigrated() const {
  TRI_ASSERT(isLocked());
  return ((_state.load() & FLAG_MIGRATED) > 0);
}

bool PlainBucket::isFull() const {
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

CachedValue* PlainBucket::find(uint32_t hash, uint8_t const* key,
                               uint32_t keySize, bool moveToFront) {
  TRI_ASSERT(isLocked());
  for (size_t i = 0; i < SLOTS_DATA; i++) {
    if (_cachedHashes[i] == 0) {
      break;
    }
    if (_cachedHashes[i] == hash && _cachedData[i]->sameKey(key, keySize)) {
      return _cachedData[i];
    }
  }
  return nullptr;
}

// requires there to be an open slot, otherwise will not be inserted
void PlainBucket::insert(uint32_t hash, CachedValue* value) {
  TRI_ASSERT(isLocked());
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

CachedValue* PlainBucket::remove(uint32_t hash, uint8_t const* key,
                                 uint32_t keySize) {
  TRI_ASSERT(isLocked());
  CachedValue* value = find(hash, key, keySize, false);
  if (value != nullptr) {
    evict(value, false);
  }

  return value;
}

CachedValue* PlainBucket::evictionCandidate() const {
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

void PlainBucket::evict(CachedValue* value, bool optimizeForInsertion) {
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

void PlainBucket::toggleMigrated() {
  TRI_ASSERT(isLocked());
  _state ^= FLAG_MIGRATED;
}

void PlainBucket::moveSlot(size_t slot, bool moveToFront) {
  uint32_t hash = _cachedHashes[slot];
  CachedValue* value = _cachedData[slot];
  if (moveToFront) {
    // move slot to front
    for (size_t i = slot; i >= 1; i--) {
      _cachedHashes[i] = _cachedHashes[i - 1];
      _cachedData[i] = _cachedData[i - 1];
    }
    _cachedHashes[0] = hash;
    _cachedData[0] = value;
  } else {
    // move slot to back
    for (size_t i = slot; (i < SLOTS_DATA - 1) && (_cachedHashes[i + 1] != 0);
         i++) {
      _cachedHashes[i] = _cachedHashes[i + 1];
      _cachedData[i] = _cachedData[i + 1];
    }
    _cachedHashes[SLOTS_DATA - 1] = hash;
    _cachedData[SLOTS_DATA - 1] = value;
  }
}
