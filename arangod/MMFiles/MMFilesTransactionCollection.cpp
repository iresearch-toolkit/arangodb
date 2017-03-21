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

#include "MMFilesTransactionCollection.h"
#include "Basics/Exceptions.h"
#include "Cluster/CollectionLockState.h"
#include "Logger/Logger.h"
#include "MMFiles/MMFilesDocumentOperation.h"
#include "MMFiles/MMFilesCollection.h"
#include "StorageEngine/TransactionState.h"
#include "Transaction/Methods.h"
#include "Transaction/Hints.h"
#include "VocBase/LogicalCollection.h"
#include "VocBase/modes.h"

using namespace arangodb;

MMFilesTransactionCollection::MMFilesTransactionCollection(TransactionState* trx, TRI_voc_cid_t cid, AccessMode::Type accessType, int nestingLevel)
    : TransactionCollection(trx, cid),
      _operations(nullptr),
      _originalRevision(0), 
      _nestingLevel(nestingLevel), 
      _compactionLocked(false), 
      _waitForSync(false),
      _accessType(accessType), 
      _lockType(AccessMode::Type::NONE) {} 

MMFilesTransactionCollection::~MMFilesTransactionCollection() {}

/// @brief request a main-level lock for a collection
int MMFilesTransactionCollection::lock() {
  return lock(_accessType, 0);
}

/// @brief request a lock for a collection
int MMFilesTransactionCollection::lock(AccessMode::Type accessType,
                                       int nestingLevel) {
  if (isWrite(accessType) && !isWrite(_accessType)) {
    // wrong lock type
    return TRI_ERROR_INTERNAL;
  }

  if (isLocked()) {
    // already locked
    return TRI_ERROR_NO_ERROR;
  }

  return doLock(accessType, nestingLevel);
}

/// @brief request an unlock for a collection
int MMFilesTransactionCollection::unlock(AccessMode::Type accessType,
                                         int nestingLevel) {
  if (isWrite(accessType) && !isWrite(_accessType)) {
    // wrong lock type: write-unlock requested but collection is read-only
    return TRI_ERROR_INTERNAL;
  }

  if (!isLocked()) {
    // already unlocked
    return TRI_ERROR_NO_ERROR;
  }

  return doUnlock(accessType, nestingLevel);
}

/// @brief check if a collection is locked in a specific mode in a transaction
bool MMFilesTransactionCollection::isLocked(AccessMode::Type accessType, int nestingLevel) const {
  if (isWrite(accessType) && !isWrite(_accessType)) {
    // wrong lock type
    LOG_TOPIC(WARN, arangodb::Logger::FIXME) << "logic error. checking wrong lock type";
    return false;
  }

  return isLocked();
}
  
/// @brief check whether a collection is locked at all
bool MMFilesTransactionCollection::isLocked() const {
  return (_lockType != AccessMode::Type::NONE);
}
  
/// @brief whether or not any write operations for the collection happened
bool MMFilesTransactionCollection::hasOperations() const {
  return (_operations != nullptr && !_operations->empty());
}

void MMFilesTransactionCollection::addOperation(MMFilesDocumentOperation* operation) {
  if (_operations == nullptr) {
    _operations = new std::vector<MMFilesDocumentOperation*>;
    _operations->reserve(16);
  } 
  _operations->push_back(operation);
}
  
void MMFilesTransactionCollection::freeOperations(transaction::Methods* activeTrx, bool mustRollback) {
  if (!hasOperations()) {
    return;
  }
  
  bool const isSingleOperationTransaction = _transaction->hasHint(transaction::Hints::Hint::SINGLE_OPERATION);

  if (mustRollback) {
    // revert all operations
    for (auto it = _operations->rbegin(); it != _operations->rend(); ++it) {
      MMFilesDocumentOperation* op = (*it);

      try {
        op->revert(activeTrx);
      } catch (...) {
      }
      delete op;
    }
  } else {
    // no rollback. simply delete all operations
    for (auto it = _operations->rbegin(); it != _operations->rend(); ++it) {
      delete (*it);
    }
  }

  auto physical = static_cast<MMFilesCollection*>(_collection->getPhysical());
  TRI_ASSERT(physical != nullptr);

  if (mustRollback) {
    physical->setRevision(_originalRevision, true);
  } else if (!physical->isVolatile() && !isSingleOperationTransaction) {
    // only count logfileEntries if the collection is durable
    physical->increaseUncollectedLogfileEntries(_operations->size());
  }

  delete _operations;
  _operations = nullptr;
}

bool MMFilesTransactionCollection::canAccess(AccessMode::Type accessType) const {
  if (_collection == nullptr) {
    if (!_transaction->hasHint(transaction::Hints::Hint::LOCK_NEVER) ||
        !_transaction->hasHint(transaction::Hints::Hint::NO_USAGE_LOCK)) {
      // not opened. probably a mistake made by the caller
      return false;
    }
    // ok
  }

  // check if access type matches
  if (AccessMode::isWriteOrExclusive(accessType) && 
      !AccessMode::isWriteOrExclusive(_accessType)) {
    // type doesn't match. probably also a mistake by the caller
    return false;
  }

  return true;
}

int MMFilesTransactionCollection::updateUsage(AccessMode::Type accessType, int nestingLevel) {
  if (AccessMode::isWriteOrExclusive(accessType) && 
      !AccessMode::isWriteOrExclusive(_accessType)) {
    if (nestingLevel > 0) {
      // trying to write access a collection that is only marked with
      // read-access
      return TRI_ERROR_TRANSACTION_UNREGISTERED_COLLECTION;
    }

    TRI_ASSERT(nestingLevel == 0);

    // upgrade collection type to write-access
    _accessType = accessType;
  }

  if (nestingLevel < _nestingLevel) {
    _nestingLevel = nestingLevel;
  }

  // all correct
  return TRI_ERROR_NO_ERROR;
}

int MMFilesTransactionCollection::use(int nestingLevel) {
  if (_nestingLevel != nestingLevel) {
    // only process our own collections
    return TRI_ERROR_NO_ERROR;
  }

  if (_collection == nullptr) {
    // open the collection
    if (!_transaction->hasHint(transaction::Hints::Hint::LOCK_NEVER) &&
        !_transaction->hasHint(transaction::Hints::Hint::NO_USAGE_LOCK)) {
      // use and usage-lock
      TRI_vocbase_col_status_e status;
      LOG_TRX(_transaction, nestingLevel) << "using collection " << _cid;
      _collection = _transaction->vocbase()->useCollection(_cid, status);
    } else {
      // use without usage-lock (lock already set externally)
      _collection = _transaction->vocbase()->lookupCollection(_cid);

      if (_collection == nullptr) {
        return TRI_ERROR_ARANGO_COLLECTION_NOT_FOUND;
      }
    }

    if (_collection == nullptr) {
      // something went wrong
      int res = TRI_errno();

      if (res == TRI_ERROR_NO_ERROR) {
        // must return an error
        res = TRI_ERROR_INTERNAL;
      }
      return res;
    }

    if (AccessMode::isWriteOrExclusive(_accessType) &&
        TRI_GetOperationModeServer() == TRI_VOCBASE_MODE_NO_CREATE &&
        !LogicalCollection::IsSystemName(_collection->name())) {
      return TRI_ERROR_ARANGO_READ_ONLY;
    }

    // store the waitForSync property
    _waitForSync = _collection->waitForSync();
  }

  TRI_ASSERT(_collection != nullptr);
  auto physical = static_cast<MMFilesCollection*>(_collection->getPhysical());
  TRI_ASSERT(physical != nullptr);

  if (nestingLevel == 0 &&
      AccessMode::isWriteOrExclusive(_accessType)) {
    // read-lock the compaction lock
    if (!_transaction->hasHint(transaction::Hints::Hint::NO_COMPACTION_LOCK)) {
      if (!_compactionLocked) {
        physical->preventCompaction();
        _compactionLocked = true;
      }
    }
  }

  if (AccessMode::isWriteOrExclusive(_accessType) && _originalRevision == 0) {
    // store original revision at transaction start
    _originalRevision = _collection->revision();
  }

  bool shouldLock = _transaction->hasHint(transaction::Hints::Hint::LOCK_ENTIRELY);

  if (!shouldLock) {
    shouldLock = (AccessMode::isWriteOrExclusive(_accessType) && !_transaction->hasHint(transaction::Hints::Hint::SINGLE_OPERATION));
  }

  if (shouldLock && !isLocked()) {
    // r/w lock the collection
    int res = doLock(_accessType, nestingLevel);

    if (res != TRI_ERROR_NO_ERROR) {
      return res;
    }
  }

  return TRI_ERROR_NO_ERROR;
}
  
void MMFilesTransactionCollection::unuse(int nestingLevel) {
  if (isLocked() &&
      (nestingLevel == 0 || _nestingLevel == nestingLevel)) {
    // unlock our own r/w locks
    doUnlock(_accessType, nestingLevel);
  }

  // the top level transaction releases all collections
  if (nestingLevel == 0 && _collection != nullptr) {
    if (!_transaction->hasHint(transaction::Hints::Hint::NO_COMPACTION_LOCK)) {
      if (AccessMode::isWriteOrExclusive(_accessType) && _compactionLocked) {
        auto physical = static_cast<MMFilesCollection*>(_collection->getPhysical());
        TRI_ASSERT(physical != nullptr);
        // read-unlock the compaction lock
        physical->allowCompaction();
        _compactionLocked = false;
      }
    }

    _lockType = AccessMode::Type::NONE;
  }
}
    
void MMFilesTransactionCollection::release() {
  // the top level transaction releases all collections
  if (_collection != nullptr) {
    // unuse collection, remove usage-lock
    LOG_TRX(_transaction, 0) << "unusing collection " << _cid;

    _transaction->vocbase()->releaseCollection(_collection);
    _collection = nullptr;
  }
}

/// @brief lock a collection
int MMFilesTransactionCollection::doLock(AccessMode::Type type, int nestingLevel) {
  if (_transaction->hasHint(transaction::Hints::Hint::LOCK_NEVER)) {
    // never lock
    return TRI_ERROR_NO_ERROR;
  }

  TRI_ASSERT(_collection != nullptr);

  if (CollectionLockState::_noLockHeaders != nullptr) {
    std::string collName(_collection->name());
    auto it = CollectionLockState::_noLockHeaders->find(collName);
    if (it != CollectionLockState::_noLockHeaders->end()) {
      // do not lock by command
      // LOCKING-DEBUG
      // std::cout << "LockCollection blocked: " << collName << std::endl;
      return TRI_ERROR_NO_ERROR;
    }
  }

  TRI_ASSERT(!isLocked());

  LogicalCollection* collection = _collection;
  TRI_ASSERT(collection != nullptr);

  auto physical = static_cast<MMFilesCollection*>(collection->getPhysical());
  TRI_ASSERT(physical != nullptr);

  double timeout = _transaction->timeout();
  if (_transaction->hasHint(transaction::Hints::Hint::TRY_LOCK)) {
    // give up early if we cannot acquire the lock instantly
    timeout = 0.00000001;
  }
  
  bool const useDeadlockDetector = !_transaction->hasHint(transaction::Hints::Hint::SINGLE_OPERATION);

  int res;
  if (!isWrite(type)) {
    LOG_TRX(_transaction, nestingLevel) << "read-locking collection " << _cid;
    res = physical->beginReadTimed(useDeadlockDetector, timeout);
  } else { // WRITE or EXCLUSIVE
    LOG_TRX(_transaction, nestingLevel) << "write-locking collection " << _cid;
    res = physical->beginWriteTimed(useDeadlockDetector, timeout);
  }

  if (res == TRI_ERROR_NO_ERROR) {
    _lockType = type;
  }

  return res;
}

/// @brief unlock a collection
int MMFilesTransactionCollection::doUnlock(AccessMode::Type type, int nestingLevel) {
  if (_transaction->hasHint(transaction::Hints::Hint::LOCK_NEVER)) {
    // never unlock
    return TRI_ERROR_NO_ERROR;
  }

  TRI_ASSERT(_collection != nullptr);

  if (CollectionLockState::_noLockHeaders != nullptr) {
    std::string collName(_collection->name());
    auto it = CollectionLockState::_noLockHeaders->find(collName);
    if (it != CollectionLockState::_noLockHeaders->end()) {
      // do not lock by command
      // LOCKING-DEBUG
      // std::cout << "UnlockCollection blocked: " << collName << std::endl;
      return TRI_ERROR_NO_ERROR;
    }
  }

  TRI_ASSERT(isLocked());

  if (_nestingLevel < nestingLevel) {
    // only process our own collections
    return TRI_ERROR_NO_ERROR;
  }

  if (!isWrite(type) && isWrite(_lockType)) {
    // do not remove a write-lock if a read-unlock was requested!
    return TRI_ERROR_NO_ERROR;
  } else if (isWrite(type) && !isWrite(_lockType)) {
    // we should never try to write-unlock a collection that we have only
    // read-locked
    LOG_TOPIC(ERR, arangodb::Logger::FIXME) << "logic error in UnlockCollection";
    TRI_ASSERT(false);
    return TRI_ERROR_INTERNAL;
  }

  bool const useDeadlockDetector = !_transaction->hasHint(transaction::Hints::Hint::SINGLE_OPERATION);

  LogicalCollection* collection = _collection;
  TRI_ASSERT(collection != nullptr);

  auto physical = static_cast<MMFilesCollection*>(collection->getPhysical());
  TRI_ASSERT(physical != nullptr);

  if (!isWrite(_lockType)) {
    LOG_TRX(_transaction, nestingLevel) << "read-unlocking collection " << _cid;
    physical->endRead(useDeadlockDetector);
  } else { // WRITE or EXCLUSIVE
    LOG_TRX(_transaction, nestingLevel) << "write-unlocking collection " << _cid;
    physical->endWrite(useDeadlockDetector);
  }

  _lockType = AccessMode::Type::NONE;

  return TRI_ERROR_NO_ERROR;
}

bool MMFilesTransactionCollection::isWrite(AccessMode::Type type) const {
  return (type == AccessMode::Type::WRITE || type == AccessMode::Type::EXCLUSIVE);
}
