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
/// @author Dr. Frank Celler
////////////////////////////////////////////////////////////////////////////////

#include "v8-vocindex.h"
#include "Basics/conversions.h"
#include "Basics/ReadLocker.h"
#include "Basics/StringUtils.h"
#include "Basics/tri-strings.h"
#include "Basics/VelocyPackHelper.h"
#include "Cluster/ClusterInfo.h"
#include "Cluster/ClusterMethods.h"
#include "Indexes/Index.h"
#include "Indexes/IndexFactory.h"
#include "StorageEngine/EngineSelectorFeature.h"
#include "StorageEngine/StorageEngine.h"
#include "Transaction/Helpers.h"
#include "Transaction/Hints.h"
#include "Utils/Events.h"
#include "Utils/SingleCollectionTransaction.h"
#include "Transaction/V8Context.h"
#include "V8/v8-conv.h"
#include "V8/v8-globals.h"
#include "V8/v8-utils.h"
#include "V8/v8-vpack.h"
#include "V8Server/v8-collection.h"
#include "V8Server/v8-externals.h"
#include "V8Server/v8-vocbase.h"
#include "V8Server/v8-vocbaseprivate.h"
#include "VocBase/modes.h"
#include "VocBase/LogicalCollection.h"

#include <velocypack/Builder.h>
#include <velocypack/Iterator.h>
#include <velocypack/velocypack-aliases.h>

using namespace arangodb;
using namespace arangodb::basics;
using namespace arangodb::rest;

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if argument is an index identifier
////////////////////////////////////////////////////////////////////////////////

static bool IsIndexHandle(v8::Handle<v8::Value> const arg,
                          std::string& collectionName, TRI_idx_iid_t& iid) {
  TRI_ASSERT(collectionName.empty());
  TRI_ASSERT(iid == 0);

  if (arg->IsNumber()) {
    // numeric index id
    iid = (TRI_idx_iid_t)arg->ToNumber()->Value();
    return true;
  }

  if (!arg->IsString()) {
    return false;
  }

  v8::String::Utf8Value str(arg);

  if (*str == nullptr) {
    return false;
  }

  size_t split;
  if (arangodb::Index::validateHandle(*str, &split)) {
    collectionName = std::string(*str, split);
    iid = StringUtils::uint64(*str + split + 1, str.length() - split - 1);
    return true;
  }

  if (arangodb::Index::validateId(*str)) {
    iid = StringUtils::uint64(*str, str.length());
    return true;
  }

  return false;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the index representation
////////////////////////////////////////////////////////////////////////////////

static v8::Handle<v8::Value> IndexRep(v8::Isolate* isolate,
                                      std::string const& collectionName,
                                      VPackSlice const& idx) {
  v8::EscapableHandleScope scope(isolate);
  TRI_ASSERT(!idx.isNone());

  v8::Handle<v8::Object> rep = TRI_VPackToV8(isolate, idx)->ToObject();

  std::string iid = TRI_ObjectToString(rep->Get(TRI_V8_ASCII_STRING("id")));
  std::string const id = collectionName + TRI_INDEX_HANDLE_SEPARATOR_STR + iid;
  rep->Set(TRI_V8_ASCII_STRING("id"), TRI_V8_STD_STRING(id));

  return scope.Escape<v8::Value>(rep);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief enhances the json of an index
////////////////////////////////////////////////////////////////////////////////

static int EnhanceIndexJson(v8::FunctionCallbackInfo<v8::Value> const& args,
                            VPackBuilder& builder, bool create) {
  v8::Isolate* isolate = args.GetIsolate();
  v8::HandleScope scope(isolate);

  v8::Handle<v8::Object> obj = args[0].As<v8::Object>();
  VPackBuilder input;
  int res = TRI_V8ToVPack(isolate, input, obj, false);
  if (res != TRI_ERROR_NO_ERROR) {
    // Failed to parse input object
    return res;
  }

  StorageEngine* engine = EngineSelectorFeature::ENGINE;
  IndexFactory const* idxFactory = engine->indexFactory(); 
  return idxFactory->enhanceIndexDefinition(input.slice(), builder, create);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief ensures an index, coordinator case
////////////////////////////////////////////////////////////////////////////////

int EnsureIndexCoordinator(std::string const& databaseName,
                           std::string const& cid,
                           VPackSlice const slice, bool create,
                           VPackBuilder& resultBuilder, std::string& errorMsg) {
  TRI_ASSERT(!slice.isNone());
  return ClusterInfo::instance()->ensureIndexCoordinator(
      databaseName, cid, slice, create, &arangodb::Index::Compare,
      resultBuilder, errorMsg, 360.0);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief ensures an index, locally
////////////////////////////////////////////////////////////////////////////////

static void EnsureIndexLocal(v8::FunctionCallbackInfo<v8::Value> const& args,
                             arangodb::LogicalCollection* collection,
                             VPackSlice const& slice, bool create) {
  v8::Isolate* isolate = args.GetIsolate();
  v8::HandleScope scope(isolate);

  TRI_ASSERT(collection != nullptr);
  READ_LOCKER(readLocker, collection->vocbase()->_inventoryLock);

  SingleCollectionTransaction trx(
      transaction::V8Context::Create(collection->vocbase(), true),
      collection->cid(), create ? AccessMode::Type::WRITE : AccessMode::Type::READ);

  int res = trx.begin();

  if (res != TRI_ERROR_NO_ERROR) {
    TRI_V8_THROW_EXCEPTION(res);
  }

  // disallow index creation in read-only mode
  if (!collection->isSystem() && create &&
      TRI_GetOperationModeServer() == TRI_VOCBASE_MODE_NO_CREATE) {
    TRI_V8_THROW_EXCEPTION(TRI_ERROR_ARANGO_READ_ONLY);
  }

  bool created = false;
  std::shared_ptr<arangodb::Index> idx;
  if (create) {
    // TODO Encapsulate in try{}catch(){} instead of errno()
    idx = collection->createIndex(&trx, slice, created);
    if (idx == nullptr) {
      // something went wrong during creation
      int res = TRI_errno();
      TRI_V8_THROW_EXCEPTION(res);
    }
  } else {
    idx = collection->lookupIndex(slice);
    if (idx == nullptr) {
      // Index not found
      TRI_V8_RETURN_NULL();
    }
  }

  transaction::BuilderLeaser builder(&trx);
  builder->openObject();
  try {
    idx->toVelocyPack(*(builder.get()), false);
  } catch (...) {
    TRI_V8_THROW_EXCEPTION_MEMORY();
  }
  builder->close();

  v8::Handle<v8::Value> ret =
      IndexRep(isolate, collection->name(), builder->slice());

  res = trx.commit();

  if (res != TRI_ERROR_NO_ERROR) {
    TRI_V8_THROW_EXCEPTION_MESSAGE(res, TRI_errno_string(res));
  }

  if (ret->IsObject()) {
    ret->ToObject()->Set(TRI_V8_ASCII_STRING("isNewlyCreated"),
                         v8::Boolean::New(isolate, created));
  }

  TRI_V8_RETURN(ret);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief ensures an index
////////////////////////////////////////////////////////////////////////////////

static void EnsureIndex(v8::FunctionCallbackInfo<v8::Value> const& args,
                        bool create, char const* functionName) {
  v8::Isolate* isolate = args.GetIsolate();
  v8::HandleScope scope(isolate);

  arangodb::LogicalCollection* collection =
      TRI_UnwrapClass<arangodb::LogicalCollection>(args.Holder(), WRP_VOCBASE_COL_TYPE);

  if (collection == nullptr) {
    TRI_V8_THROW_EXCEPTION_INTERNAL("cannot extract collection");
  }

  if (args.Length() != 1 || !args[0]->IsObject()) {
    std::string name(functionName);
    name.append("(<description>)");
    TRI_V8_THROW_EXCEPTION_USAGE(name.c_str());
  }

  VPackBuilder builder;
  int res = EnhanceIndexJson(args, builder, create);
  
  if (res != TRI_ERROR_NO_ERROR) {
    TRI_V8_THROW_EXCEPTION(res);
  }

  VPackSlice slice = builder.slice();
  if (res == TRI_ERROR_NO_ERROR && ServerState::instance()->isCoordinator()) {
    TRI_ASSERT(slice.isObject());

    std::string const dbname(collection->dbName());
    std::string const collname(collection->name());
    auto c = ClusterInfo::instance()->getCollection(dbname, collname);

    // check if there is an attempt to create a unique index on non-shard keys
    if (create) {
      Index::validateFields(slice);

      VPackSlice v = slice.get("unique");

      /* the following combinations of shardKeys and indexKeys are allowed/not allowed:

      shardKeys     indexKeys
              a             a        ok
              a             b    not ok
              a           a b        ok
            a b             a    not ok
            a b             b    not ok
            a b           a b        ok
            a b         a b c        ok
          a b c           a b    not ok
          a b c         a b c        ok
      */

      if (v.isBoolean() && v.getBoolean()) {
        // unique index, now check if fields and shard keys match
        VPackSlice flds = slice.get("fields");
        if (flds.isArray() && c->numberOfShards() > 1) {
          std::vector<std::string> const& shardKeys = c->shardKeys();
          std::unordered_set<std::string> indexKeys;
          size_t n = static_cast<size_t>(flds.length());
          
          for (size_t i = 0; i < n; ++i) {
            VPackSlice f = flds.at(i);
            if (!f.isString()) {
              // index attributes must be strings
              TRI_V8_THROW_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL, "index field names should be strings");
            }
            indexKeys.emplace(f.copyString());
          }
           
          // all shard-keys must be covered by the index
          for (auto& it : shardKeys) {
            if (indexKeys.find(it) == indexKeys.end()) {
              TRI_V8_THROW_EXCEPTION_MESSAGE(TRI_ERROR_CLUSTER_UNSUPPORTED, "shard key '" + it + "' must be present in unique index");
            }
          }
        }
      }
    }
  }

  TRI_ASSERT(!slice.isNone());
  events::CreateIndex(collection->name(), slice);

  // ensure an index, coordinator case
  if (ServerState::instance()->isCoordinator()) {
    VPackBuilder resultBuilder;
    std::string errorMsg;
#ifdef USE_ENTERPRISE
    int res = EnsureIndexCoordinatorEnterprise(collection, slice, create,
                                               resultBuilder, errorMsg);
#else
    std::string const databaseName(collection->dbName());
    std::string const cid = collection->cid_as_string();
    int res = EnsureIndexCoordinator(databaseName, cid, slice, create,
                                     resultBuilder, errorMsg);
#endif
    if (res != TRI_ERROR_NO_ERROR) {
      TRI_V8_THROW_EXCEPTION_MESSAGE(res, errorMsg);
    }
    if (resultBuilder.slice().isNone()) {
      if (!create) {
        // did not find a suitable index
        TRI_V8_RETURN_NULL();
      }

      TRI_V8_THROW_EXCEPTION_MEMORY();
    }
    v8::Handle<v8::Value> ret = IndexRep(isolate, collection->name(), resultBuilder.slice());
    TRI_V8_RETURN(ret);
  } else {
    EnsureIndexLocal(args, collection, slice, create);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief was docuBlock collectionEnsureIndex
////////////////////////////////////////////////////////////////////////////////

static void JS_EnsureIndexVocbaseCol(
    v8::FunctionCallbackInfo<v8::Value> const& args) {
  TRI_V8_TRY_CATCH_BEGIN(isolate);
  v8::HandleScope scope(isolate);

  PREVENT_EMBEDDED_TRANSACTION();

  EnsureIndex(args, true, "ensureIndex");
  TRI_V8_TRY_CATCH_END
}

////////////////////////////////////////////////////////////////////////////////
/// @brief looks up an index
////////////////////////////////////////////////////////////////////////////////

static void JS_LookupIndexVocbaseCol(
    v8::FunctionCallbackInfo<v8::Value> const& args) {
  TRI_V8_TRY_CATCH_BEGIN(isolate);
  v8::HandleScope scope(isolate);

  EnsureIndex(args, false, "lookupIndex");
  TRI_V8_TRY_CATCH_END
}
////////////////////////////////////////////////////////////////////////////////
/// @brief drops an index, coordinator case
////////////////////////////////////////////////////////////////////////////////

int DropIndexCoordinator(
    std::string const& databaseName,
    std::string const& cid,
    TRI_idx_iid_t const iid) {
  std::string errorMsg;

  return ClusterInfo::instance()->dropIndexCoordinator(databaseName, cid,
                                                       iid, errorMsg, 0.0);

}

////////////////////////////////////////////////////////////////////////////////
/// @brief was docuBlock col_dropIndex
////////////////////////////////////////////////////////////////////////////////

static void JS_DropIndexVocbaseCol(
    v8::FunctionCallbackInfo<v8::Value> const& args) {
  TRI_V8_TRY_CATCH_BEGIN(isolate);
  v8::HandleScope scope(isolate);

  PREVENT_EMBEDDED_TRANSACTION();

  arangodb::LogicalCollection* collection =
      TRI_UnwrapClass<arangodb::LogicalCollection>(args.Holder(), WRP_VOCBASE_COL_TYPE);

  if (collection == nullptr) {
    TRI_V8_THROW_EXCEPTION_INTERNAL("cannot extract collection");
  }

  if (args.Length() != 1) {
    TRI_V8_THROW_EXCEPTION_USAGE("dropIndex(<index-handle>)");
  }

  if (ServerState::instance()->isCoordinator()) {
    std::string collectionName;
    TRI_idx_iid_t iid = 0;
    v8::Handle<v8::Value> const val = args[0];

    // extract the index identifier from a string
    if (val->IsString() || val->IsStringObject() || val->IsNumber()) {
      if (!IsIndexHandle(val, collectionName, iid)) {
        TRI_V8_THROW_EXCEPTION(TRI_ERROR_ARANGO_INDEX_HANDLE_BAD);
      }
    }

    // extract the index identifier from an object
    else if (val->IsObject()) {
      TRI_GET_GLOBALS();

      v8::Handle<v8::Object> obj = val->ToObject();
      TRI_GET_GLOBAL_STRING(IdKey);
      v8::Handle<v8::Value> iidVal = obj->Get(IdKey);

      if (!IsIndexHandle(iidVal, collectionName, iid)) {
        TRI_V8_THROW_EXCEPTION(TRI_ERROR_ARANGO_INDEX_HANDLE_BAD);
      }
    }

    if (!collectionName.empty()) {
      CollectionNameResolver resolver(collection->vocbase());

      if (!EqualCollection(&resolver, collectionName, collection)) {
        TRI_V8_THROW_EXCEPTION(TRI_ERROR_ARANGO_CROSS_COLLECTION_REQUEST);
      }
    }

#ifdef USE_ENTERPRISE
    int res = DropIndexCoordinatorEnterprise(collection, iid);
#else
    std::string const databaseName(collection->dbName());
    std::string const cid = collection->cid_as_string();
    int res = DropIndexCoordinator(databaseName, cid, iid);
#endif
    if (res == TRI_ERROR_NO_ERROR) {
      TRI_V8_RETURN_TRUE();
    }
    TRI_V8_RETURN_FALSE();
  }
  
  READ_LOCKER(readLocker, collection->vocbase()->_inventoryLock);

  SingleCollectionTransaction trx(
      transaction::V8Context::Create(collection->vocbase(), true),
      collection->cid(), AccessMode::Type::WRITE);

  int res = trx.begin();

  if (res != TRI_ERROR_NO_ERROR) {
    TRI_V8_THROW_EXCEPTION(res);
  }

  LogicalCollection* col = trx.documentCollection();

  auto idx = TRI_LookupIndexByHandle(isolate, trx.resolver(), collection,
                                     args[0], true);

  if (idx == nullptr || idx->id() == 0) {
    TRI_V8_RETURN_FALSE();
  }

  if (!idx->canBeDropped()) {
    TRI_V8_THROW_EXCEPTION(TRI_ERROR_FORBIDDEN);
  }

  bool ok = col->dropIndex(idx->id());

  if (ok) {
    TRI_V8_RETURN_TRUE();
  }

  TRI_V8_RETURN_FALSE();
  TRI_V8_TRY_CATCH_END
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns information about the indexes, coordinator case
////////////////////////////////////////////////////////////////////////////////

static void GetIndexesCoordinator(
    v8::FunctionCallbackInfo<v8::Value> const& args,
    arangodb::LogicalCollection const* collection, bool withFigures) {
  // warning This may be obsolete.
  v8::Isolate* isolate = args.GetIsolate();
  v8::HandleScope scope(isolate);

  std::string const databaseName(collection->dbName());
  std::string const cid = collection->cid_as_string();
  std::string const collectionName(collection->name());

  auto c = ClusterInfo::instance()->getCollection(databaseName, cid);

  v8::Handle<v8::Array> ret = v8::Array::New(isolate);

  VPackBuilder tmp;
  c->getIndexesVPack(tmp, withFigures);
  VPackSlice slice = tmp.slice();

  if (slice.isArray()) {
    uint32_t j = 0;
    for (auto const& v : VPackArrayIterator(slice)) {
      if (!v.isNone()) {
        ret->Set(j++, IndexRep(isolate, collectionName, v));
      }
    }
  }

  TRI_V8_RETURN(ret);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief was docuBlock collectionGetIndexes
////////////////////////////////////////////////////////////////////////////////

static void JS_GetIndexesVocbaseCol(
    v8::FunctionCallbackInfo<v8::Value> const& args) {
  TRI_V8_TRY_CATCH_BEGIN(isolate);
  v8::HandleScope scope(isolate);

  arangodb::LogicalCollection* collection =
      TRI_UnwrapClass<arangodb::LogicalCollection>(args.Holder(), WRP_VOCBASE_COL_TYPE);

  if (collection == nullptr) {
    TRI_V8_THROW_EXCEPTION_INTERNAL("cannot extract collection");
  }
  
  bool withFigures = false;
  if (args.Length() > 0) {
    withFigures = TRI_ObjectToBoolean(args[0]);
  }

  if (ServerState::instance()->isCoordinator()) {
    GetIndexesCoordinator(args, collection, withFigures);
    return;
  }

  SingleCollectionTransaction trx(
      transaction::V8Context::Create(collection->vocbase(), true),
      collection->cid(), AccessMode::Type::READ);
    
  trx.addHint(transaction::Hints::Hint::NO_USAGE_LOCK);

  int res = trx.begin();

  if (res != TRI_ERROR_NO_ERROR) {
    TRI_V8_THROW_EXCEPTION(res);
  }

  // READ-LOCK start
  trx.lockRead();

  std::string const collectionName(collection->name());

  // get list of indexes
  transaction::BuilderLeaser builder(&trx);
  auto indexes = collection->getIndexes();

  trx.finish(res);
  // READ-LOCK end

  size_t const n = indexes.size();
  v8::Handle<v8::Array> result = v8::Array::New(isolate, static_cast<int>(n));

  for (size_t i = 0; i < n; ++i) {
    auto const& idx = indexes[i];
    builder->clear();
    builder->openObject();
    idx->toVelocyPack(*(builder.get()), withFigures);
    builder->close();
    result->Set(static_cast<uint32_t>(i),
                IndexRep(isolate, collectionName, builder->slice()));
  }

  TRI_V8_RETURN(result);
  TRI_V8_TRY_CATCH_END
}

////////////////////////////////////////////////////////////////////////////////
/// @brief looks up an index identifier
////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arangodb::Index> TRI_LookupIndexByHandle(
    v8::Isolate* isolate, arangodb::CollectionNameResolver const* resolver,
    arangodb::LogicalCollection const* collection,
    v8::Handle<v8::Value> const val, bool ignoreNotFound) {
  // reset the collection identifier
  std::string collectionName;
  TRI_idx_iid_t iid = 0;

  // assume we are already loaded
  TRI_ASSERT(collection != nullptr);

  // extract the index identifier from a string
  if (val->IsString() || val->IsStringObject() || val->IsNumber()) {
    if (!IsIndexHandle(val, collectionName, iid)) {
      TRI_V8_SET_EXCEPTION(TRI_ERROR_ARANGO_INDEX_HANDLE_BAD);
      return nullptr;
    }
  }

  // extract the index identifier from an object
  else if (val->IsObject()) {
    TRI_GET_GLOBALS();

    v8::Handle<v8::Object> obj = val->ToObject();
    TRI_GET_GLOBAL_STRING(IdKey);
    v8::Handle<v8::Value> iidVal = obj->Get(IdKey);

    if (!IsIndexHandle(iidVal, collectionName, iid)) {
      TRI_V8_SET_EXCEPTION(TRI_ERROR_ARANGO_INDEX_HANDLE_BAD);
      return nullptr;
    }
  }

  if (!collectionName.empty()) {
    if (!EqualCollection(resolver, collectionName, collection)) {
      // I wish this error provided me with more information!
      // e.g. 'cannot access index outside the collection it was defined in'
      TRI_V8_SET_EXCEPTION(TRI_ERROR_ARANGO_CROSS_COLLECTION_REQUEST);
      return nullptr;
    }
  }

  auto idx = collection->lookupIndex(iid);

  if (idx == nullptr) {
    if (!ignoreNotFound) {
      TRI_V8_SET_EXCEPTION(TRI_ERROR_ARANGO_INDEX_NOT_FOUND);
      return nullptr;
    }
  }

  return idx;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief create a collection
////////////////////////////////////////////////////////////////////////////////

static void CreateVocBase(v8::FunctionCallbackInfo<v8::Value> const& args,
                          TRI_col_type_e collectionType) {
  v8::Isolate* isolate = args.GetIsolate();
  v8::HandleScope scope(isolate);

  TRI_vocbase_t* vocbase = GetContextVocBase(isolate);

  if (vocbase == nullptr) {
    TRI_V8_THROW_EXCEPTION(TRI_ERROR_ARANGO_DATABASE_NOT_FOUND);
  }

  // ...........................................................................
  // We require exactly 1 or exactly 2 arguments -- anything else is an error
  // ...........................................................................

  if (args.Length() < 1 || args.Length() > 3) {
    TRI_V8_THROW_EXCEPTION_USAGE("_create(<name>, <properties>, <type>)");
  }

  if (TRI_GetOperationModeServer() == TRI_VOCBASE_MODE_NO_CREATE) {
    TRI_V8_THROW_EXCEPTION(TRI_ERROR_ARANGO_READ_ONLY);
  }

  // optional, third parameter can override collection type
  if (args.Length() == 3 && args[2]->IsString()) {
    std::string typeString = TRI_ObjectToString(args[2]);
    if (typeString == "edge") {
      collectionType = TRI_COL_TYPE_EDGE;
    } else if (typeString == "document") {
      collectionType = TRI_COL_TYPE_DOCUMENT;
    }
  }

  PREVENT_EMBEDDED_TRANSACTION();

  // extract the name
  std::string const name = TRI_ObjectToString(args[0]);

  VPackBuilder builder;
  VPackSlice infoSlice;
  if (2 <= args.Length()) {
    if (!args[1]->IsObject()) {
      TRI_V8_THROW_TYPE_ERROR("<properties> must be an object");
    }
    v8::Handle<v8::Object> obj = args[1]->ToObject();
    // Add the type and name into the object. Easier in v8 than in VPack
    obj->Set(TRI_V8_ASCII_STRING("type"),
             v8::Number::New(isolate, static_cast<int>(collectionType)));
    obj->Set(TRI_V8_ASCII_STRING("name"), TRI_V8_STD_STRING(name));

    int res = TRI_V8ToVPack(isolate, builder, obj, false);

    if (res != TRI_ERROR_NO_ERROR) {
      TRI_V8_THROW_EXCEPTION(res);
    }

  } else {
    // create an empty properties object
    builder.openObject();
    builder.add("type", VPackValue(static_cast<int>(collectionType)));
    builder.add("name", VPackValue(name));
    builder.close();
  }
    
  infoSlice = builder.slice();

  if (ServerState::instance()->isCoordinator()) {
    std::unique_ptr<LogicalCollection> col =
        ClusterMethods::createCollectionOnCoordinator(collectionType, vocbase,
                                                      infoSlice);
    TRI_V8_RETURN(WrapCollection(isolate, col.release()));
  }

  try {
    TRI_voc_cid_t cid = 0;
    arangodb::LogicalCollection const* collection =
        vocbase->createCollection(infoSlice, cid);

    TRI_ASSERT(collection != nullptr);

    v8::Handle<v8::Value> result = WrapCollection(isolate, collection);
    if (result.IsEmpty()) {
      TRI_V8_THROW_EXCEPTION_MEMORY();
    }

    TRI_V8_RETURN(result);
  } catch (basics::Exception const& ex) {
    TRI_V8_THROW_EXCEPTION_MESSAGE(ex.code(), ex.what());
  } catch (std::exception const& ex) {
    TRI_V8_THROW_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL, ex.what());
  } catch (...) {
    TRI_V8_THROW_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL, "cannot create collection");
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief was docuBlock collectionDatabaseCreate
////////////////////////////////////////////////////////////////////////////////

static void JS_CreateVocbase(v8::FunctionCallbackInfo<v8::Value> const& args) {
  TRI_V8_TRY_CATCH_BEGIN(isolate);
  CreateVocBase(args, TRI_COL_TYPE_DOCUMENT);
  TRI_V8_TRY_CATCH_END
}

////////////////////////////////////////////////////////////////////////////////
/// @brief was docuBlock collectionCreateDocumentCollection
////////////////////////////////////////////////////////////////////////////////

static void JS_CreateDocumentCollectionVocbase(
    v8::FunctionCallbackInfo<v8::Value> const& args) {
  TRI_V8_TRY_CATCH_BEGIN(isolate);
  CreateVocBase(args, TRI_COL_TYPE_DOCUMENT);
  TRI_V8_TRY_CATCH_END
}

////////////////////////////////////////////////////////////////////////////////
/// @brief was docuBlock collectionCreateEdgeCollection
////////////////////////////////////////////////////////////////////////////////

static void JS_CreateEdgeCollectionVocbase(
    v8::FunctionCallbackInfo<v8::Value> const& args) {
  TRI_V8_TRY_CATCH_BEGIN(isolate);
  CreateVocBase(args, TRI_COL_TYPE_EDGE);
  TRI_V8_TRY_CATCH_END
}

void TRI_InitV8IndexArangoDB(v8::Isolate* isolate,
                             v8::Handle<v8::ObjectTemplate> rt) {
  TRI_AddMethodVocbase(isolate, rt, TRI_V8_ASCII_STRING("_create"),
                       JS_CreateVocbase, true);
  TRI_AddMethodVocbase(isolate, rt,
                       TRI_V8_ASCII_STRING("_createEdgeCollection"),
                       JS_CreateEdgeCollectionVocbase);
  TRI_AddMethodVocbase(isolate, rt,
                       TRI_V8_ASCII_STRING("_createDocumentCollection"),
                       JS_CreateDocumentCollectionVocbase);
}

void TRI_InitV8IndexCollection(v8::Isolate* isolate,
                               v8::Handle<v8::ObjectTemplate> rt) {
  TRI_AddMethodVocbase(isolate, rt, TRI_V8_ASCII_STRING("dropIndex"),
                       JS_DropIndexVocbaseCol);
  TRI_AddMethodVocbase(isolate, rt, TRI_V8_ASCII_STRING("ensureIndex"),
                       JS_EnsureIndexVocbaseCol);
  TRI_AddMethodVocbase(isolate, rt, TRI_V8_ASCII_STRING("lookupIndex"),
                       JS_LookupIndexVocbaseCol);
  TRI_AddMethodVocbase(isolate, rt, TRI_V8_ASCII_STRING("getIndexes"),
                       JS_GetIndexesVocbaseCol);
}
