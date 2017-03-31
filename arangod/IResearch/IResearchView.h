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

#include "store/directory.hpp"
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

///////////////////////////////////////////////////////////////////////////////
/// --SECTION--                                              ViewImplementation
///////////////////////////////////////////////////////////////////////////////
class IResearchView final: public arangodb::ViewImplementation {
 public:
  typedef std::unique_ptr<arangodb::ViewImplementation> ptr;
  typedef std::shared_ptr<IResearchLink> LinkPtr;

  ///////////////////////////////////////////////////////////////////////////////
  /// @brief destructor to clean up resources
  ///////////////////////////////////////////////////////////////////////////////
  virtual ~IResearchView();

  ///////////////////////////////////////////////////////////////////////////////
  /// @brief a garbage collection function for the view
  /// @param maxMsec try not to exceed the specified time, casues partial cleanup
  ///                0 == full cleanup
  /// @return success
  ///////////////////////////////////////////////////////////////////////////////
  bool cleanup(size_t maxMsec = 0);

  ///////////////////////////////////////////////////////////////////////////////
  /// @brief drop this iResearch View
  ///////////////////////////////////////////////////////////////////////////////
  void drop() override;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief drop collection matching 'cid' from the iResearch View
  ////////////////////////////////////////////////////////////////////////////////
  int drop(TRI_voc_cid_t cid);

  ///////////////////////////////////////////////////////////////////////////////
  /// @brief fill and return a JSON description of a IResearchView object
  ///        only fields describing the view itself, not 'link' descriptions
  ////////////////////////////////////////////////////////////////////////////////
  void getPropertiesVPack(arangodb::velocypack::Builder& builder) const override;

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

  ///////////////////////////////////////////////////////////////////////////////
  /// @brief register an iResearch Link with the specified view
  /// @return iResearch View registered with
  ///         or nullptr if not found or already registered
  ///////////////////////////////////////////////////////////////////////////////
  static IResearchView* linkRegister(
    TRI_vocbase_t& vocbase,
    std::string const& viewName,
    LinkPtr const& ptr
  );

  ///////////////////////////////////////////////////////////////////////////////
  /// @brief unregister an iResearch Link from the specified view
  /// @return the specified iResearch Link was previously registered
  ///////////////////////////////////////////////////////////////////////////////
  bool linkUnregister(LinkPtr const& ptr);

  ///////////////////////////////////////////////////////////////////////////////
  /// @brief view factory
  /// @returns initialized view object
  ///////////////////////////////////////////////////////////////////////////////
  static ptr make(
    arangodb::LogicalView* view,
    arangodb::velocypack::Slice const& info,
    bool isNew
  );

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief amount of memory in bytes occupied by this iResearch Link
  ////////////////////////////////////////////////////////////////////////////////
  size_t memory() const;

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief Modify configuration parameters of the iResearch View
  ////////////////////////////////////////////////////////////////////////////////
  bool modify(VPackSlice const& definition);

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief the name identifying the current iResearch View
  ////////////////////////////////////////////////////////////////////////////////
  std::string const& name() const noexcept;

  ///////////////////////////////////////////////////////////////////////////////
  /// @brief opens an existing view when the server is restarted
  ///////////////////////////////////////////////////////////////////////////////
  void open() override;

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
  /// @param maxMsec try not to exceed the specified time, casues partial sync
  ///                0 == full sync
  /// @return success
  ////////////////////////////////////////////////////////////////////////////////
  bool sync(size_t maxMsec = 0);

  ////////////////////////////////////////////////////////////////////////////////
  /// @brief the view type as used when selecting which view to instantiate
  ////////////////////////////////////////////////////////////////////////////////
  static std::string const& type() noexcept;

  ///////////////////////////////////////////////////////////////////////////////
  /// @brief called when a view's properties are updated (i.e. delta-modified)
  ///////////////////////////////////////////////////////////////////////////////
  arangodb::Result updateProperties(
    arangodb::velocypack::Slice const& slice,
    bool doSync
  ) override;

 private:
  struct DataStore {
    irs::directory::ptr _directory;
    irs::directory_reader _reader;
    irs::index_writer::ptr _writer;
    DataStore& operator=(DataStore&& other) noexcept;
    operator bool() const noexcept;
  };

  struct MemoryStore: public DataStore {
    MemoryStore(); // initialize _directory and _writer during allocation
  };

  typedef std::unordered_map<TRI_voc_fid_t, MemoryStore> MemoryStoreByFid;

  struct TidStore {
    mutable std::mutex _mutex; // for use with '_removals' (allow use in const functions)
    std::vector<std::shared_ptr<irs::filter>> _removals; // removal filters to be applied to during merge
    MemoryStoreByFid _storeByFid;
  };

  typedef std::unordered_map<TRI_voc_tid_t, TidStore> MemoryStoreByTid;

  std::condition_variable _asyncCondition; // trigger reload of timeout settings for async jobs
  std::atomic<size_t> _asyncMetaRevision; // arbitrary meta modification id, async jobs should reload if different
  std::mutex _asyncMutex; // mutex used with '_asyncCondition' and associated timeouts
  std::atomic<bool> _asyncTerminate; // trigger termination of long-running async jobs
  std::unordered_set<LinkPtr> _links;
  mutable irs::async_utils::read_write_mutex _linksMutex; // for use with '_links', separate to allow '_links' modification during '_meta' update
  IResearchViewMeta _meta;
  mutable irs::async_utils::read_write_mutex _mutex; // for use with member maps/sets and '_meta'
  MemoryStoreByTid _storeByTid;
  MemoryStoreByFid _storeByWalFid;
  DataStore _storePersisted;
  irs::async_utils::thread_pool _threadPool;

  IResearchView(
    arangodb::LogicalView*,
    arangodb::velocypack::Slice const& info
  );
};

NS_END // iresearch
NS_END // arangodb
#endif