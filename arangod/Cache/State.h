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

#ifndef ARANGODB_CACHE_STATE_H
#define ARANGODB_CACHE_STATE_H

#include "Basics/Common.h"

#include <atomic>
#include <cstdint>

namespace arangodb {
namespace cache {

struct State {
  typedef std::function<void()> CallbackType;

  // each flag must have exactly one bit set, fit in uint32_t
  enum class Flag : uint32_t {
    locked = 0x00000001,
    blacklisted = 0x00000002,
    migrated = 0x00000004,
    migrating = 0x00000008,
    rebalancing = 0x00000010,
    resizing = 0x00000020,
    shutdown = 0x00000040
  };

  State();
  State(State const& other);

  bool isLocked() const;
  bool lock(int64_t maxTries = -1LL, State::CallbackType cb = []() -> void {});
  // must be locked to unlock
  void unlock();

  // check and toggle public flags; must be locked first
  bool isSet(State::Flag flag) const;
  void toggleFlag(State::Flag flag);

  // clear all flags besides Flag::locked; must be locked first
  void clear();

 private:
  // simple state variable for locking and other purposes
  std::atomic<uint32_t> _state;
};

static_assert(sizeof(State) == 4);

};  // end namespace cache
};  // end namespace arangodb

#endif
