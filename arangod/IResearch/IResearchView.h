
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

#include "IResearchViewMeta.h"
#include "IResearchDocument.h"

#include "VocBase/ViewImplementation.h"

#include "store/memory_directory.hpp"
#include "index/index_writer.hpp"
#include "index/directory_reader.hpp"
#include "utils/async_utils.hpp"

NS_BEGIN(arangodb)
NS_BEGIN(transaction)

class Methods; // forward declaration

NS_END // transaction
NS_END // arangodb

NS_BEGIN(arangodb)
NS_BEGIN(iresearch)

class IResearchLink; // forward declaration
struct IResearchLinkMeta; // forward declaration

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

//  int insert(StoredPrimaryKey const& pk) noexcept;
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
    irs::index_writer::ptr&& writer,
    irs::directory_reader::ptr&& reader
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

class IResearchView final : public arangodb::ViewImplementation {
 public:
  ///////////////////////////////////////////////////////////////////////////////
  /// @brief type of the view
  ///////////////////////////////////////////////////////////////////////////////
  static std::string type;

  ///////////////////////////////////////////////////////////////////////////////
  /// @brief view factory
  /// @returns initialized view object
  ///////////////////////////////////////////////////////////////////////////////
  static std::unique_ptr<ViewImplementation> make(
    arangodb::LogicalView*,
    arangodb::velocypack::Slice const& info,
    bool isNew
  );

  ///////////////////////////////////////////////////////////////////////////////
  /// --SECTION--                                              ViewImplementation
  ///////////////////////////////////////////////////////////////////////////////

  arangodb::Result updateProperties(
    arangodb::velocypack::Slice const& slice,
    bool doSync
  ) override;

  void getPropertiesVPack(arangodb::velocypack::Builder&) const override;

  void open() override;

  ///////////////////////////////////////////////////////////////////////////////
  /// @brief drop this iResearch View
  ///////////////////////////////////////////////////////////////////////////////
  void drop() override;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief drop collection matching 'cid' from the iResearch View
  ////////////////////////////////////////////////////////////////////////////////
  int drop(TRI_voc_cid_t cid);

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief the name identifying the current iResearch View
  ////////////////////////////////////////////////////////////////////////////////
  std::string const& name() const noexcept;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief insert a document into the iResearch View
  ///        to be done in the scope of transaction 'tid' and 'meta'
  ////////////////////////////////////////////////////////////////////////////////
  int insert(
    TRI_voc_fid_t fid,
    TRI_voc_tid_t tid,
    TRI_voc_cid_t cid,
    TRI_voc_rid_t rid,
    arangodb::velocypack::Slice const& doc,
    IResearchLinkMeta const& meta
  );

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief count of known links registered with this view
  ////////////////////////////////////////////////////////////////////////////////
  size_t linkCount() const noexcept;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief amount of memory in bytes occupied by this iResearch Link
  ////////////////////////////////////////////////////////////////////////////////
  size_t memory() const;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief Modify configuration parameters of the iResearch View
  ////////////////////////////////////////////////////////////////////////////////
  bool modify(VPackSlice const& definition);

  bool properties(VPackSlice const& props, TRI_vocbase_t* vocbase);
  bool properties(arangodb::velocypack::Builder& props) const;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief query the iResearch View and return error code
  ///        visitor: returns TRI_ERROR_NO_ERROR if no error, else stops iteration
  /// @return error code and optionally write message to non-nullptr 'error'
  ////////////////////////////////////////////////////////////////////////////////
  int query(
    std::function<int(arangodb::transaction::Methods const&, VPackSlice const&)> const& visitor,
    arangodb::transaction::Methods& trx,
    std::string const& query,
    std::ostream* error = nullptr
  );

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief remove documents matching 'cid' and 'rid' from the iResearch View
  ///        to be done in the scope of transaction 'tid'
  ////////////////////////////////////////////////////////////////////////////////
  int remove(TRI_voc_tid_t tid, TRI_voc_cid_t cid, TRI_voc_rid_t rid);

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief wait for a flush of all index data to its respective stores
  ////////////////////////////////////////////////////////////////////////////////
  bool sync();

 private:
  IResearchView(
    arangodb::LogicalView*,
    arangodb::velocypack::Slice const& info
  ) noexcept;

  template<typename Directory>
  struct DataStore {
    Directory _directory;
    //irs::unbounded_object_pool<irs::directory_reader> _readerPool;
    irs::index_writer::ptr _writer;

    template<typename... Args>
    DataStore(Args&&... args): _directory(std::forward<Args>(args)...) {
      auto format = irs::formats::get("1_0");

      // create writer before reader to ensure data directory is present
      _writer = irs::index_writer::make(_directory, format, irs::OM_CREATE_APPEND);
      _writer->commit(); // initialize 'store'
    }
  };

  struct FidStore {
    std::unordered_map<TRI_voc_fid_t, DataStore<irs::memory_directory>> _storeByFid;
  };

  struct TidStore: public FidStore {
    std::mutex _mutex; // for use with '_removals'
    std::vector<std::shared_ptr<irs::filter>> _removals; // removal filters to be applied to during merge
  };

  std::unordered_set<std::shared_ptr<IResearchLink>> _links;
  IResearchViewMeta _meta;
  mutable irs::async_utils::read_write_mutex _mutex; // for use with member maps/sets
  std::unordered_map<TRI_voc_tid_t, TidStore> _storeByTid;
  FidStore _storeByWalFid;
  DataStore<irs::memory_directory> _storePersisted;
  irs::async_utils::thread_pool _threadPool;
};

NS_END // iresearch
NS_END // arangodb
#endif
