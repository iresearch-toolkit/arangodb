
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

#ifndef ARANGOD_IRESEARCH__IRESEARCH_VIEW_H
#define ARANGOD_IRESEARCH__IRESEARCH_VIEW_H 1

#include "IResearchLink.h"
#include "IResearchViewMeta.h"
#include "IResearchDocument.h"

#include "Logger/LogMacros.h"
#include "Logger/Logger.h"
#include "Logger/LogLevel.h"

#include "velocypack/velocypack-aliases.h"

#include "store/memory_directory.hpp"
#include "index/index_writer.hpp"
#include "index/directory_reader.hpp"

namespace arangodb {
namespace iresearch {

class IndexStore {
 public:
  template<typename Directory, typename... Args>
  static IndexStore make(Args&&... args) {
    return IndexStore::make(Directory::make(std::forward<Args>(args)...));
  }

  static IndexStore make(irs::directory::ptr&& dir);

  operator bool() const { return static_cast<bool>(_dir); }

  IndexStore() = default;
  IndexStore(IndexStore&& rhs);
  IndexStore& operator=(IndexStore&& rhs);

  int insert(StoredPrimaryKey const& pk) noexcept;
  int remove(std::shared_ptr<irs::filter> const& filter) noexcept;
  int merge(IndexStore& src) noexcept;
  int consolidate(irs::index_writer::consolidation_policy_t const& policy) noexcept;
  int commit() noexcept;
  int cleanup() noexcept;
  size_t memory() const noexcept;
  void close() noexcept;

 private:
  IndexStore(
    irs::directory::ptr&& dir,
    irs::index_writer::ptr writer,
    irs::directory_reader::ptr reader
  ) noexcept;

  // disallow copy and assign
  IndexStore(const IndexStore&) = delete;
  IndexStore operator=(const IndexStore&) = delete;

  irs::directory_reader reader() {
    // TODO if concurrency issues are encountered implement reader pool with shared_ptr that on delete returns reader to pool
    return _reader = _reader.reopen();
  }

  irs::directory::ptr _dir;
  irs::index_writer::ptr _writer;
  irs::directory_reader _reader;
}; // IndexStore

class IResearchView /* : public ViewImpl */ {
 public:
  IResearchView() = default;

  bool properties(VPackSlice const& props);
  bool properties(VPackBuilder& props) const;
  bool drop();

 private:
  IResearchViewMeta _meta;
  IndexStore _store;
  std::unordered_set<IResearchLink::ptr> _links;
};

} // iresearch
} // arangodb

#endif
