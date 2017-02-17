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

#ifndef ARANGODB_CACHE_FREQUENCY_BUFFER_H
#define ARANGODB_CACHE_FREQUENCY_BUFFER_H

#include "Basics/Common.h"

#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace arangodb {
namespace cache {

template <class T>
class FrequencyBuffer {
 public:
  typedef std::vector<std::pair<T, uint64_t>> stats_t;

 private:
  std::atomic<uint64_t> _current;
  uint64_t _capacity;
  uint64_t _mask;
  std::unique_ptr<T[]> _buffer;

 public:
  FrequencyBuffer(uint64_t capacity) : _current(0) {
    size_t i = 0;
    for (; (1ULL << i) < capacity; i++) {
    }
    _capacity = (1 << i);
    _mask = _capacity - 1;
    _buffer.reset(new T[_capacity]());
  }

  uint64_t memoryUsage() {
    return ((_capacity * sizeof(T)) + sizeof(FrequencyBuffer<T>));
  }

  void insertRecord(T const& record) {
    ++_current;
    _buffer[_current & _mask] = record;
  }

  void purgeRecord(T const& record) {
    for (size_t i = 0; i < _capacity; i++) {
      if (_buffer[i] == record) {
        _buffer[i] = T();
      }
    }
  }

  // return frequencies in ASCENDING order
  std::shared_ptr<FrequencyBuffer::stats_t> getFrequencies() const {
    // calculate frequencies
    std::unordered_map<T, uint64_t> frequencies;
    for (size_t i = 0; i < _capacity; i++) {
      T entry = _buffer[i];
      if (entry != T()) {
        frequencies[entry]++;
      }
    }

    // gather and sort frequencies
    std::shared_ptr<stats_t> data(new stats_t());
    data->reserve(frequencies.size());
    for (auto f : frequencies) {
      data->emplace_back(std::pair<T, uint64_t>(f.first, f.second));
    }
    std::sort(data->begin(), data->end(),
              [](std::pair<T, uint64_t>& left, std::pair<T, uint64_t>& right) {
                return left.second < right.second;
              });

    return data;
  }
};

};  // end namespace cache
};  // end namespace arangodb

#endif
