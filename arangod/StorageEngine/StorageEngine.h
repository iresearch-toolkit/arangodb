////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2016 ArangoDB GmbH, Cologne, Germany
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
/// @author Jan Steemann
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGOD_STORAGE_ENGINE_STORAGE_ENGINE_H
#define ARANGOD_STORAGE_ENGINE_STORAGE_ENGINE_H 1

#include "Basics/Common.h"
#include "ApplicationFeatures/ApplicationFeature.h"
#include "Indexes/IndexFactory.h"
#include "MMFiles/MMFilesCollectorCache.h"
#include "VocBase/voc-types.h"
#include "VocBase/vocbase.h"

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>

namespace arangodb {
class LogicalCollection;
class PhysicalCollection;

class StorageEngine : public application_features::ApplicationFeature {
 public:

  // create the storage engine
  StorageEngine(application_features::ApplicationServer* server,
                std::string const& engineName, std::string const& featureName,
                IndexFactory* indexFactory)
      : application_features::ApplicationFeature(server, featureName),
        _indexFactory(indexFactory),
        _typeName(engineName) {

    // each specific storage engine feature is optional. the storage engine selection feature
    // will make sure that exactly one engine is selected at startup
    setOptional(true);
    // storage engines must not use elevated privileges for files etc
    requiresElevatedPrivileges(false);

    startsAfter("DatabasePath");
    startsAfter("EngineSelector");
    startsAfter("FileDescriptors");
    startsAfter("Temp");
  }

  virtual void start() {}
  virtual void stop() {}
  virtual bool inRecovery() { return false; }
  virtual void recoveryDone(TRI_vocbase_t* vocbase) {}
  int writeCreateMarker(TRI_voc_tick_t id, VPackSlice const& slice){ return TRI_ERROR_NO_ERROR; };


  // create storage-engine specific collection
  virtual PhysicalCollection* createPhysicalCollection(LogicalCollection*) = 0;


  // status functionality
  // --------------------

  // return the name of the storage engine
  char const* typeName() const { return _typeName.c_str(); }

  // inventory functionality
  // -----------------------

  // fill the Builder object with an array of databases that were detected
  // by the storage engine. this method must sort out databases that were not
  // fully created (see "createDatabase" below). called at server start only
  virtual void getDatabases(arangodb::velocypack::Builder& result) = 0;

  // fills the provided builder with information about the collection
  virtual void getCollectionInfo(TRI_vocbase_t* vocbase, TRI_voc_cid_t cid,
                                 arangodb::velocypack::Builder& result,
                                 bool includeIndexes, TRI_voc_tick_t maxTick) = 0;

  // fill the Builder object with an array of collections (and their corresponding
  // indexes) that were detected by the storage engine. called at server start separately
  // for each database
  virtual int getCollectionsAndIndexes(TRI_vocbase_t* vocbase, arangodb::velocypack::Builder& result,
                                       bool wasCleanShutdown, bool isUpgrade) = 0;

  // return the path for a database
  virtual std::string databasePath(TRI_vocbase_t const* vocbase) const = 0;

  // return the path for a collection
  virtual std::string collectionPath(TRI_vocbase_t const* vocbase, TRI_voc_cid_t id) const = 0;

  //virtual TRI_vocbase_t* openDatabase(arangodb::velocypack::Slice const& parameters, bool isUpgrade) = 0;

  // database, collection and index management
  // -----------------------------------------


  // TODO add pre / post conditions
  using Database = TRI_vocbase_t;
  using CollectionView = LogicalCollection;
  // if not stated other wise functions may throw and the caller has to take care of error handling
  // the return values will be the usual  TRI_ERROR_* codes 
  
  virtual Database* openDatabase(arangodb::velocypack::Slice const& args, bool isUpgrade, int& status) = 0;
  Database* openDatabase(arangodb::velocypack::Slice const& args, bool isUpgrade){
    int status;
    Database* rv = openDatabase(args, isUpgrade, status);
    TRI_ASSERT(status == TRI_ERROR_NO_ERROR);
    TRI_ASSERT(rv == nullptr);
    return rv;
  }

  //return empty string when not found
  virtual std::string getName(Database*) const = 0;
  virtual std::string getPath(Database*) const = 0;
  virtual std::string getName(Database*, CollectionView*) const = 0;
  virtual std::string getPath(Database*, CollectionView*) const = 0;

  // asks the storage engine to create a database as specified in the VPack
  // Slice object and persist the creation info. It is guaranteed by the server that
  // no other active database with the same name and id exists when this function
  // is called. If this operation fails somewhere in the middle, the storage
  // engine is required to fully clean up the creation and throw only then,
  // so that subsequent database creation requests will not fail.
  // the WAL entry for the database creation will be written *after* the call
  // to "createDatabase" returns
  //no way to aquire id within this function?!
  virtual Database* createDatabase(TRI_voc_tick_t id, arangodb::velocypack::Slice const& args, int& status) = 0;
  Database* createDatabase(TRI_voc_tick_t id, arangodb::velocypack::Slice const& args ){
    int status;
    Database* rv = createDatabase(id, args, status);
    TRI_ASSERT(status == TRI_ERROR_NO_ERROR);
    TRI_ASSERT(rv == nullptr);
    return rv;
  }

  // asks the storage engine to drop the specified database and persist the
  // deletion info. Note that physical deletion of the database data must not
  // be carried out by this call, as there may still be readers of the database's data.
  // It is recommended that this operation only sets a deletion flag for the database
  // but let's an async task perform the actual deletion.
  // the WAL entry for database deletion will be written *after* the call
  // to "prepareDropDatabase" returns
  //
  // is done under a lock in database feature
  virtual void prepareDropDatabase(TRI_vocbase_t* vocbase, bool useWriteMarker, int& status) = 0;
  void prepareDropDatabase(Database* db, bool useWriteMarker){
    int status = 0;
    prepareDropDatabase(db, status);
    TRI_ASSERT(status == TRI_ERROR_NO_ERROR);
  };

  /// @brief wait until a database directory disappears
  //
  // should not require a lock
  virtual void waitUntilDeletion(TRI_voc_tick_t id, bool force, int& status) = 0;

  // perform a physical deletion of the database
  virtual void dropDatabase(Database*, int& status) = 0;
  void dropDatabase(Database* db){
    int status;
    dropDatabase(db, status);
    TRI_ASSERT(status == TRI_ERROR_NO_ERROR);
  };
  



public:
  // asks the storage engine to create a collection as specified in the VPack
  // Slice object and persist the creation info. It is guaranteed by the server
  // that no other active collection with the same name and id exists in the same
  // database when this function is called. If this operation fails somewhere in
  // the middle, the storage engine is required to fully clean up the creation
  // and throw only then, so that subsequent collection creation requests will not fail.
  // the WAL entry for the collection creation will be written *after* the call
  // to "createCollection" returns
  virtual std::string createCollection(TRI_vocbase_t* vocbase, TRI_voc_cid_t id,
                                       arangodb::LogicalCollection const* parameters) = 0;

  // asks the storage engine to drop the specified collection and persist the
  // deletion info. Note that physical deletion of the collection data must not
  // be carried out by this call, as there may
  // still be readers of the collection's data. It is recommended that this operation
  // only sets a deletion flag for the collection but let's an async task perform
  // the actual deletion.
  // the WAL entry for collection deletion will be written *after* the call
  // to "dropCollection" returns
  virtual void prepareDropCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection* collection) = 0;

  // perform a physical deletion of the collection
  virtual void dropCollection(TRI_vocbase_t* vocbase, arangodb::LogicalCollection* collection) = 0;

  // asks the storage engine to change properties of the collection as specified in
  // the VPack Slice object and persist them. If this operation fails
  // somewhere in the middle, the storage engine is required to fully revert the
  // property changes and throw only then, so that subsequent operations will not fail.
  // the WAL entry for the propery change will be written *after* the call
  // to "changeCollection" returns
  virtual void changeCollection(TRI_vocbase_t* vocbase, TRI_voc_cid_t id,
                                arangodb::LogicalCollection const* parameters,
                                bool doSync) = 0;

  // asks the storage engine to create an index as specified in the VPack
  // Slice object and persist the creation info. The database id, collection id
  // and index data are passed in the Slice object. Note that this function
  // is not responsible for inserting the individual documents into the index.
  // If this operation fails somewhere in the middle, the storage engine is required
  // to fully clean up the creation and throw only then, so that subsequent index
  // creation requests will not fail.
  // the WAL entry for the index creation will be written *after* the call
  // to "createIndex" returns
  virtual void createIndex(TRI_vocbase_t* vocbase, TRI_voc_cid_t collectionId,
                           TRI_idx_iid_t id, arangodb::velocypack::Slice const& data) = 0;

  // asks the storage engine to drop the specified index and persist the deletion
  // info. Note that physical deletion of the index must not be carried out by this call,
  // as there may still be users of the index. It is recommended that this operation
  // only sets a deletion flag for the index but let's an async task perform
  // the actual deletion.
  // the WAL entry for index deletion will be written *after* the call
  // to "dropIndex" returns
  virtual void dropIndex(TRI_vocbase_t* vocbase, TRI_voc_cid_t collectionId,
                         TRI_idx_iid_t id) = 0;

  // Returns the StorageEngine-specific implementation
  // of the IndexFactory. This is used to validate
  // information about indexes.
  IndexFactory const* indexFactory() {
    // The factory has to be created by the implementation
    // and shall never be deleted
    TRI_ASSERT(_indexFactory.get() != nullptr);
    return _indexFactory.get();
  }

  virtual void unloadCollection(TRI_vocbase_t* vocbase, TRI_voc_cid_t collectionId) = 0;

  virtual void signalCleanup(TRI_vocbase_t* vocbase) = 0;

  // document operations
  // -------------------

  // iterate all documents of the underlying collection
  // this is called when a collection is openend, and all its documents need to be added to
  // indexes etc.
  virtual void iterateDocuments(TRI_voc_tick_t databaseId, TRI_voc_cid_t collectionId,
                                std::function<void(arangodb::velocypack::Slice const&)> const& cb) = 0;


  // adds a document to the storage engine
  // this will be called by the WAL collector when surviving documents are being moved
  // into the storage engine's realm
  virtual void addDocumentRevision(TRI_voc_tick_t databaseId, TRI_voc_cid_t collectionId,
                                   arangodb::velocypack::Slice const& document) = 0;

  // removes a document from the storage engine
  // this will be called by the WAL collector when non-surviving documents are being removed
  // from the storage engine's realm
  virtual void removeDocumentRevision(TRI_voc_tick_t databaseId, TRI_voc_cid_t collectionId,
                                      arangodb::velocypack::Slice const& document) = 0;

  /// @brief remove data of expired compaction blockers
  virtual bool cleanupCompactionBlockers(TRI_vocbase_t* vocbase) = 0;

  /// @brief insert a compaction blocker
  virtual int insertCompactionBlocker(TRI_vocbase_t* vocbase, double ttl, TRI_voc_tick_t& id) = 0;

  /// @brief touch an existing compaction blocker
  virtual int extendCompactionBlocker(TRI_vocbase_t* vocbase, TRI_voc_tick_t id, double ttl) = 0;

  /// @brief remove an existing compaction blocker
  virtual int removeCompactionBlocker(TRI_vocbase_t* vocbase, TRI_voc_tick_t id) = 0;

  /// @brief a callback function that is run while it is guaranteed that there is no compaction ongoing
  virtual void preventCompaction(TRI_vocbase_t* vocbase,
                                 std::function<void(TRI_vocbase_t*)> const& callback) = 0;

  /// @brief a callback function that is run there is no compaction ongoing
  virtual bool tryPreventCompaction(TRI_vocbase_t* vocbase,
                                    std::function<void(TRI_vocbase_t*)> const& callback,
                                    bool checkForActiveBlockers) = 0;

  virtual int shutdownDatabase(TRI_vocbase_t* vocbase) = 0;

  virtual int openCollection(TRI_vocbase_t* vocbase, LogicalCollection* collection, bool ignoreErrors) = 0;

  /// @brief transfer markers into a collection
  virtual int transferMarkers(LogicalCollection* collection, MMFilesCollectorCache*,
                              MMFilesOperationsType const&) = 0;

 protected:
  arangodb::LogicalCollection* registerCollection(
      TRI_vocbase_t* vocbase, arangodb::velocypack::Slice params) {
    return vocbase->registerCollection(true, params);
  }

 private:

  std::unique_ptr<IndexFactory> const _indexFactory;

  std::string const _typeName;

};

}

#endif
