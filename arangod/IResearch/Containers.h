//////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 EMC Corporation
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGODB_IRESEARCH__IRESEARCH_CONTAINERS_H
#define ARANGODB_IRESEARCH__IRESEARCH_CONTAINERS_H 1

#include <memory>
#include <unordered_map>

#include "utils/hash_utils.hpp"
#include "utils/map_utils.hpp"
#include "utils/memory.hpp"
#include "utils/string.hpp"

NS_LOCAL

template <typename... > struct typelist;

NS_END

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

struct Hasher {
  template<typename T>
  size_t operator()(T const& value) const;
};

////////////////////////////////////////////////////////////////////////////////
/// @brief a wrapper around a type, placing the value on the heap to allow
///        declaration of map member variables whos' values are of the type
///        being declared
////////////////////////////////////////////////////////////////////////////////
template<typename T>
class UniqueHeapInstance {
 public:
  template<
    typename... Args,
    typename = std::enable_if_t<
      !std::is_same<typelist<UniqueHeapInstance>,
      typelist<std::decay_t<Args>...>>::value
    > // prevent matching of copy/move constructor
  >
  UniqueHeapInstance(Args&&... args);
  UniqueHeapInstance(UniqueHeapInstance const& other);
  UniqueHeapInstance(UniqueHeapInstance&& other) noexcept;
  UniqueHeapInstance& operator=(UniqueHeapInstance const& other);
  UniqueHeapInstance& operator=(UniqueHeapInstance&& other) noexcept;
  T& operator=(T const& other);
  T& operator=(T&& other);
  T& operator*() const noexcept;
  T* operator->() const noexcept;
  bool operator==(UniqueHeapInstance const& other) const;
  bool operator!=(UniqueHeapInstance const& other) const;
  T* get() noexcept;
  T const* get() const noexcept;

 private:
  std::unique_ptr<T> _instance;
};

template<typename T>
template<
  typename... Args,
  typename = std::enable_if_t<
    !std::is_same<typelist<UniqueHeapInstance<T>>,
    typelist<std::decay_t<Args>...>>::value
  >
>
UniqueHeapInstance<T>::UniqueHeapInstance(Args&&... args)
  : _instance(irs::memory::make_unique<T>(std::forward<Args>(args)...)) {
}

template<typename T>
UniqueHeapInstance<T>::UniqueHeapInstance(UniqueHeapInstance const& other)
  : _instance(irs::memory::make_unique<T>(*(other._instance))) {
}

template<typename T>
UniqueHeapInstance<T>::UniqueHeapInstance(UniqueHeapInstance&& other) noexcept
  : _instance(std::move(other._instance)) {
}

template<typename T>
UniqueHeapInstance<T>& UniqueHeapInstance<T>::operator=(
  UniqueHeapInstance const& other
) {
  if (this != &other) {
    _instance = irs::memory::make_unique<T>(*(other._instance));
  }

  return *this;
}

template<typename T>
UniqueHeapInstance<T>& UniqueHeapInstance<T>::operator=(
  UniqueHeapInstance&& other
  )  noexcept {
  if (this != &other) {
    _instance = std::move(other._instance);
  }

  return *this;
}

template<typename T>
T& UniqueHeapInstance<T>::operator=(T const& other) {
  *_instance = other;

  return *_instance;
}

template<typename T>
T& UniqueHeapInstance<T>::operator=(T&& other) {
  *_instance = std::move(other);

  return *_instance;
}

template<typename T>
T& UniqueHeapInstance<T>::operator*() const noexcept {
  return *_instance;
}

template<typename T>
T* UniqueHeapInstance<T>::operator->() const noexcept {
  return _instance.get();
}

template<typename T>
bool UniqueHeapInstance<T>::operator==(UniqueHeapInstance const& other) const {
  return _instance
    ? (other._instance && *_instance == *(other._instance)) : !other._instance;
}

template<typename T>
bool UniqueHeapInstance<T>::operator!=(UniqueHeapInstance const& other) const {
  return !(*this == other);
}

template<typename T>
T* UniqueHeapInstance<T>::get() noexcept {
  return _instance.get();
}

template<typename T>
T const* UniqueHeapInstance<T>::get() const noexcept {
  return _instance.get();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief a map whose key is an irs::hashed_basic_string_ref and the actual
///        key memory is in an std::pair beside the value
///        allowing the use of the map with an irs::basic_string_ref without
///        the need to allocaate memmory during find(...)
////////////////////////////////////////////////////////////////////////////////
template<typename CharType, typename V>
class UnorderedRefKeyMap {
  typedef std::unordered_map<
    irs::hashed_basic_string_ref<CharType>,
    std::pair<std::basic_string<CharType>, V>
  > MapType;
 public:
  typedef typename MapType::key_type KeyType;
  typedef V value_type;

  class ConstIterator {
   public:
    bool operator==(ConstIterator const& other) const noexcept;
    bool operator!=(ConstIterator const& other) const noexcept;
    ConstIterator& operator*() noexcept;
    ConstIterator& operator++();
    const KeyType& key() const noexcept;
    const V& value() const noexcept;

   private:
    friend UnorderedRefKeyMap;
    typename MapType::const_iterator _itr;

    ConstIterator(typename MapType::const_iterator const& itr);
  };

  class Iterator {
   public:
    bool operator==(Iterator const& other) const noexcept;
    bool operator!=(Iterator const& other) const noexcept;
    Iterator& operator*() noexcept;
    Iterator& operator++();
    const KeyType& key() const noexcept;
    V& value() const noexcept;

   private:
    friend UnorderedRefKeyMap;
    typename MapType::iterator _itr;

    Iterator(typename MapType::iterator const& itr);
  };

  UnorderedRefKeyMap();
  UnorderedRefKeyMap(UnorderedRefKeyMap const& other);
  UnorderedRefKeyMap(UnorderedRefKeyMap&& other);

  UnorderedRefKeyMap& operator=(UnorderedRefKeyMap const& other);
  UnorderedRefKeyMap& operator=(UnorderedRefKeyMap&& other);

  V& operator[](KeyType const& key);
  V& operator[](typename KeyType::base_t const& key);

  Iterator begin();
  ConstIterator begin() const;

  void clear();

  template<typename... Args>
  std::pair<Iterator, bool> emplace(KeyType const& key, Args&&... args);
  template<typename... Args>
  std::pair<Iterator, bool> emplace(typename KeyType::base_t const& key, Args&&... args);

  bool empty() const;

  Iterator end();
  ConstIterator end() const;

  Iterator find(KeyType const& key) noexcept;
  Iterator find(typename KeyType::base_t const& key) noexcept;
  ConstIterator find(KeyType const& key) const noexcept;
  ConstIterator find(typename KeyType::base_t const& key) const noexcept;

  V* findPtr(KeyType const& key) noexcept;
  V* findPtr(typename KeyType::base_t const& key) noexcept;
  V const* findPtr(KeyType const& key) const noexcept;
  V const* findPtr(typename KeyType::base_t const& key) const noexcept;

  size_t size() const noexcept;

 private:
  struct KeyGenerator {
    typename MapType::key_type operator()(typename MapType::key_type const& key, typename MapType::mapped_type const& value) const;
  };

  const KeyGenerator _generator;
  const Hasher _hasher;
  MapType _map;
};

template<typename CharType, typename V>
UnorderedRefKeyMap<CharType, V>::ConstIterator::ConstIterator(
  typename UnorderedRefKeyMap<CharType, V>::MapType::const_iterator const& itr
): _itr(itr) {
}

template<typename CharType, typename V>
bool UnorderedRefKeyMap<CharType, V>::ConstIterator::operator==(
  typename UnorderedRefKeyMap<CharType, V>::ConstIterator const& other
) const noexcept {
  return _itr == other._itr;
}

template<typename CharType, typename V>
bool UnorderedRefKeyMap<CharType, V>::ConstIterator::operator!=(
  typename UnorderedRefKeyMap<CharType, V>::ConstIterator const& other
) const noexcept {
  return !(*this == other);
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::ConstIterator& UnorderedRefKeyMap<CharType, V>::ConstIterator::operator*() noexcept {
  return *this;
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::ConstIterator& UnorderedRefKeyMap<CharType, V>::ConstIterator::operator++() {
  ++_itr;

  return *this;
}

template<typename CharType, typename V>
const typename UnorderedRefKeyMap<CharType, V>::KeyType& UnorderedRefKeyMap<CharType, V>::ConstIterator::key() const noexcept {
  return _itr->first;
}

template<typename CharType, typename V>
const V& UnorderedRefKeyMap<CharType, V>::ConstIterator::value() const noexcept {
  return _itr->second.second;
}

template<typename CharType, typename V>
UnorderedRefKeyMap<CharType, V>::Iterator::Iterator(
  typename UnorderedRefKeyMap<CharType, V>::MapType::iterator const& itr
): _itr(itr) {
}

template<typename CharType, typename V>
bool UnorderedRefKeyMap<CharType, V>::Iterator::operator==(
  typename UnorderedRefKeyMap<CharType, V>::Iterator const& other
) const noexcept {
  return _itr == other._itr;
}

template<typename CharType, typename V>
bool UnorderedRefKeyMap<CharType, V>::Iterator::operator!=(
  typename UnorderedRefKeyMap<CharType, V>::Iterator const& other
) const noexcept {
  return !(*this == other);
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::Iterator& UnorderedRefKeyMap<CharType, V>::Iterator::operator*() noexcept {
  return *this;
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::Iterator& UnorderedRefKeyMap<CharType, V>::Iterator::operator++() {
  ++_itr;

  return *this;
}

template<typename CharType, typename V>
const typename UnorderedRefKeyMap<CharType, V>::KeyType& UnorderedRefKeyMap<CharType, V>::Iterator::key() const noexcept {
  return _itr->first;
}

template<typename CharType, typename V>
V& UnorderedRefKeyMap<CharType, V>::Iterator::value() const noexcept {
  return _itr->second.second;
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::MapType::key_type UnorderedRefKeyMap<CharType, V>::KeyGenerator::operator()(
  typename UnorderedRefKeyMap<CharType, V>::MapType::key_type const& key,
  typename UnorderedRefKeyMap<CharType, V>::MapType::mapped_type const& value
) const {
  return MapType::key_type(key.hash(), value.first);
}

template<typename CharType, typename V>
UnorderedRefKeyMap<CharType, V>::UnorderedRefKeyMap() {
}

template<typename CharType, typename V>
UnorderedRefKeyMap<CharType, V>::UnorderedRefKeyMap(
  UnorderedRefKeyMap<CharType, V> const& other
) {
  *this = other;
}

template<typename CharType, typename V>
UnorderedRefKeyMap<CharType, V>::UnorderedRefKeyMap(
  UnorderedRefKeyMap<CharType, V>&& other
) {
  *this = std::move(other);
}

template<typename CharType, typename V>
UnorderedRefKeyMap<CharType, V>& UnorderedRefKeyMap<CharType, V>::operator=(UnorderedRefKeyMap<CharType, V> const& other) {
  if (this != &other) {
    _map = other._map;
  }

  return *this;
}

template<typename CharType, typename V>
UnorderedRefKeyMap<CharType, V>& UnorderedRefKeyMap<CharType, V>::operator=(UnorderedRefKeyMap<CharType, V>&& other) {
  if (this != &other) {
    _map = std::move(other._map);
  }

  return *this;
}

template<typename CharType, typename V>
V& UnorderedRefKeyMap<CharType, V>::operator[](
  typename UnorderedRefKeyMap<CharType, V>::KeyType const& key
) {
  return irs::map_utils::try_emplace_update_key(
    _map,
    _generator,
    key, // use same key for MapType::key_type and MapType::value_type.first
    std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple() // MapType::value_type
  ).first->second.second;
}

template<typename CharType, typename V>
V& UnorderedRefKeyMap<CharType, V>::operator[](
  typename UnorderedRefKeyMap<CharType, V>::KeyType::base_t const& key
) {
  return (*this)[irs::make_hashed_ref(key, _hasher)];
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::Iterator UnorderedRefKeyMap<CharType, V>::begin() {
  return Iterator(_map.begin());
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::ConstIterator UnorderedRefKeyMap<CharType, V>::begin() const {
  return ConstIterator(_map.begin());
}

template<typename CharType, typename V>
void UnorderedRefKeyMap<CharType, V>::clear() {
  _map.clear();
}

template<typename CharType, typename V>
template<typename... Args>
std::pair<typename UnorderedRefKeyMap<CharType, V>::Iterator, bool> UnorderedRefKeyMap<CharType, V>::emplace(
  typename UnorderedRefKeyMap<CharType, V>::KeyType const& key, Args&&... args
) {
  auto res = irs::map_utils::try_emplace_update_key(
    _map,
    _generator,
    key, // use same key for MapType::key_type and MapType::value_type.first
    std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple(std::forward<Args>(args)...) // MapType::value_type
  );

  return std::make_pair(Iterator(res.first), res.second);
}

template<typename CharType, typename V>
template<typename... Args>
std::pair<typename UnorderedRefKeyMap<CharType, V>::Iterator, bool> UnorderedRefKeyMap<CharType, V>::emplace(
  typename UnorderedRefKeyMap<CharType, V>::KeyType::base_t const& key, Args&&... args
) {
  return emplace(
    irs::make_hashed_ref(key, _hasher),
    std::forward<Args>(args)...
  );
}

template<typename CharType, typename V>
bool UnorderedRefKeyMap<CharType, V>::empty() const {
  return _map.empty();
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::Iterator UnorderedRefKeyMap<CharType, V>::end() {
  return Iterator(_map.end());
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::ConstIterator UnorderedRefKeyMap<CharType, V>::end() const {
  return ConstIterator(_map.end());
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::Iterator UnorderedRefKeyMap<CharType, V>::find(
  typename UnorderedRefKeyMap<CharType, V>::KeyType const& key
) noexcept {
  return Iterator(_map.find(key));
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::Iterator UnorderedRefKeyMap<CharType, V>::find(
  typename UnorderedRefKeyMap<CharType, V>::KeyType::base_t const& key
) noexcept {
  return find(irs::make_hashed_ref(key, _hasher));
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::ConstIterator UnorderedRefKeyMap<CharType, V>::find(
  typename UnorderedRefKeyMap<CharType, V>::KeyType const& key
) const noexcept {
  return ConstIterator(_map.find(key));
}

template<typename CharType, typename V>
typename UnorderedRefKeyMap<CharType, V>::ConstIterator UnorderedRefKeyMap<CharType, V>::find(
  typename UnorderedRefKeyMap<CharType, V>::KeyType::base_t const& key
) const noexcept {
  return find(irs::make_hashed_ref(key, _hasher));
}

template<typename CharType, typename V>
V* UnorderedRefKeyMap<CharType, V>::findPtr(
  typename UnorderedRefKeyMap<CharType, V>::KeyType const& key
) noexcept {
  auto itr = _map.find(key);

  return itr == _map.end() ? nullptr : &(itr->second.second);
}

template<typename CharType, typename V>
V* UnorderedRefKeyMap<CharType, V>::findPtr(
  typename UnorderedRefKeyMap<CharType, V>::KeyType::base_t const& key
) noexcept {
  return findPtr(irs::make_hashed_ref(key, _hasher));
}

template<typename CharType, typename V>
V const* UnorderedRefKeyMap<CharType, V>::findPtr(
  typename UnorderedRefKeyMap<CharType, V>::KeyType const& key
) const noexcept {
  auto itr = _map.find(key);

  return itr == _map.end() ? nullptr : &(itr->second.second);
}

template<typename CharType, typename V>
V const* UnorderedRefKeyMap<CharType, V>::findPtr(
  typename UnorderedRefKeyMap<CharType, V>::KeyType::base_t const& key
) const noexcept {
  return findPtr(irs::make_hashed_ref(key, _hasher));
}

template<typename CharType, typename V>
size_t UnorderedRefKeyMap<CharType, V>::size() const noexcept {
  return _map.size();
}

NS_END // iresearch
NS_END // arangodb
#endif