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
/// @author Max Neunhoeffer
/// @author Jan Steemann
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGODB_BASICS_INDEX_BUCKET_H
#define ARANGODB_BASICS_INDEX_BUCKET_H 1

#include "Basics/Common.h"
#include "Basics/files.h"
#include "Basics/memory-map.h"
#include "Logger/Logger.h"

namespace arangodb {
namespace basics {

template <class EntryType, class IndexType>
struct IndexBucket {
  IndexType _nrAlloc;       // the size of the table
  IndexType _nrUsed;        // the number of used entries
  IndexType _nrCollisions;  // the number of entries that have
                            // a key that was previously in the table
  EntryType* _table;        // the table itself
  int _file;                // file descriptor for memory mapped file (-1 = no file)
  char* _filename;          // name of memory mapped file (nullptr = no file)

  IndexBucket() 
      : _nrAlloc(0), 
        _nrUsed(0), 
        _nrCollisions(0), 
        _table(nullptr), 
        _file(-1), 
        _filename(nullptr) {}
  IndexBucket(IndexBucket const&) = delete;
  IndexBucket& operator=(IndexBucket const&) = delete;
  
  // move ctor. this takes over responsibility for the resources from other
  IndexBucket(IndexBucket&& other) 
      : _nrAlloc(other._nrAlloc), 
        _nrUsed(other._nrUsed), 
        _nrCollisions(other._nrCollisions), 
        _table(other._table), 
        _file(other._file), 
        _filename(other._filename) {
    other._nrAlloc = 0;
    other._nrUsed = 0;
    other._nrCollisions = 0;
    other._table = nullptr;
    other._file = -1;
    other._filename = nullptr;
  }

  IndexBucket& operator=(IndexBucket&& other) {
    deallocate(); // free own resources first
      
    _nrAlloc = other._nrAlloc;
    _nrUsed = other._nrUsed;
    _nrCollisions = other._nrCollisions;
    _table = other._table;
    _file = other._file;
    _filename = other._filename;

    other._nrAlloc = 0;
    other._nrUsed = 0;
    other._nrCollisions = 0;
    other._table = nullptr;
    other._file = -1;
    other._filename = nullptr;

    return *this;
  }

  ~IndexBucket() {
    deallocate();
  }

 public:
  size_t memoryUsage() const {
    return _nrAlloc * sizeof(EntryType);
  }

  void allocate(size_t numberElements) {
    TRI_ASSERT(_nrAlloc == 0);
    TRI_ASSERT(_nrUsed == 0);
    TRI_ASSERT(_table == nullptr);
    TRI_ASSERT(_file == -1);
    TRI_ASSERT(_filename == nullptr);

    _file = allocateTempfile(_filename, numberElements * sizeof(EntryType));

    try {
      _table = allocateMemory(numberElements);
      TRI_ASSERT(_table != nullptr);

#ifdef __linux__
      if (numberElements > 1000000) {
        uintptr_t mem = reinterpret_cast<uintptr_t>(_table);
        uintptr_t pageSize = getpagesize();
        mem = (mem / pageSize) * pageSize;
        void* memptr = reinterpret_cast<void*>(mem);
        TRI_MMFileAdvise(memptr, numberElements * sizeof(EntryType),
                         TRI_MADVISE_RANDOM);
      }
#endif

      _nrAlloc = numberElements;
    } catch (...) {
      TRI_ASSERT(_file != -1);
      deallocateTempfile();
      TRI_ASSERT(_file == -1);
      throw;
    }
  }

  void deallocate() {
    deallocateMemory();
    deallocateTempfile();
  }

 private:
  EntryType* allocateMemory(size_t numberElements) {
    TRI_ASSERT(numberElements > 0);

    if (_file == -1) {
      return new EntryType[numberElements]();
    }
  
    // initialize the file 
    size_t const totalSize = numberElements * sizeof(EntryType);
    TRI_ASSERT(_file > 0);
    
    void* data = mmap(nullptr, totalSize, PROT_WRITE | PROT_READ, MAP_SHARED | MAP_POPULATE, _file, 0);
    
    if (data == nullptr || data == MAP_FAILED) {
      THROW_ARANGO_EXCEPTION(TRI_ERROR_OUT_OF_MEMORY);
    }

    try {
      // call placement new constructor
      (void) new (data) EntryType[numberElements]();
    
      return static_cast<EntryType*>(data);
    } catch (...) {
      munmap(data, totalSize);
      throw;
    }
  }

  void deallocateMemory() {
    if (_table == nullptr) {
      return;
    } 
    if (_file == -1) {
      delete[] _table;
    } else {
      if (munmap(_table, _nrAlloc * sizeof(EntryType)) != 0) {
        // unmapping failed
        LOG(WARN) << "munmap failed";
      }
    }
    _table = nullptr;
    _nrAlloc = 0;
    _nrUsed = 0;
  }

  int allocateTempfile(char*& filename, size_t filesize) {
    TRI_ASSERT(filename == nullptr);

    if (filesize < 8192) {
      // use new/malloc
      return -1;
    }

    // create a temporary file
    long errorCode;
    std::string errorMessage;

    if (TRI_GetTempName(nullptr, &filename, false, errorCode, errorMessage) != TRI_ERROR_NO_ERROR) {
      // go on without file, but with regular new/malloc
      return -1;
    } 

    TRI_ASSERT(filename != nullptr);
    
    int fd = TRI_CreateDatafile(filename, filesize);
    if (fd < 0) {
      TRI_Free(TRI_CORE_MEM_ZONE, filename);
      filename = nullptr;
    } 
    
    return fd;
  }

  void deallocateTempfile() {
    if (_file >= 0) {
      // close file pointer and reset fd
      TRI_CLOSE(_file);
      _file = -1;
    }
    if (_filename != nullptr) {
      TRI_UnlinkFile(_filename);
      TRI_Free(TRI_CORE_MEM_ZONE, _filename);
      _filename = nullptr;
    }
  }
};

}
}

#endif
