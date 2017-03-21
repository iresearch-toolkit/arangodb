foreach (LINK_DIR ${V8_LINK_DIRECTORIES})
  link_directories("${LINK_DIR}")
endforeach()

################################################################################
## IResearch
################################################################################

if (USE_IRESEARCH)
  set(IRESEARCH_TESTS_SOURCES
    IResearch/VelocyPackHelper-test.cpp
    IResearch/IResearchLinkMeta-test.cpp
    IResearch/IResearchViewMeta-test.cpp
    IResearch/IResearchDocument-test.cpp
  )
endif ()

# linking is a dark art...workmonitordummy is somehow necessary
add_executable(
  arangodbtests
  ../lib/Basics/WorkMonitorDummy.cpp
  Basics/icu-helper.cpp
  Basics/AttributeNameParserTest.cpp
  Basics/associative-multi-pointer-test.cpp
  Basics/associative-multi-pointer-nohashcache-test.cpp
  Basics/conversions-test.cpp
  Basics/csv-test.cpp
  Basics/files-test.cpp
  Basics/fpconv-test.cpp
  Basics/json-test.cpp
  Basics/hashes-test.cpp
  Basics/skiplist-test.cpp
  Basics/priorityqueue-test.cpp
  Basics/string-buffer-test.cpp
  Basics/string-utf8-normalize-test.cpp
  Basics/string-utf8-test.cpp
  Basics/string-test.cpp
  Basics/vector-test.cpp
  Basics/structure-size-test.cpp
  Basics/EndpointTest.cpp
  Basics/StringBufferTest.cpp
  Basics/StringUtilsTest.cpp
  Basics/VelocyPackHelper-test.cpp
  Cache/CachedValue.cpp
  Cache/FrequencyBuffer.cpp
  Cache/Manager.cpp
  Cache/Metadata.cpp
  Cache/MockScheduler.cpp
  Cache/PlainBucket.cpp
  Cache/PlainCache.cpp
  Cache/Rebalancer.cpp
  Cache/State.cpp
  Cache/Table.cpp
  Cache/TransactionalBucket.cpp
  Cache/TransactionalCache.cpp
  Cache/TransactionalStore.cpp
  Cache/TransactionManager.cpp
  Cache/TransactionsWithBackingStore.cpp
  Geo/georeg.cpp
  Pregel/typedbuffer.cpp
  ${IRESEARCH_TESTS_SOURCES}
  main.cpp
)

target_link_libraries(
  arangodbtests
  arangoserver
  rocksdblib
)

target_include_directories(arangodbtests PRIVATE
  ${INCLUDE_DIRECTORIES}
)

# add these includes as system includes because otherwise
# the compiler will emit warnings for fakeit.hpp
target_include_directories(arangodbtests SYSTEM PRIVATE
  ${CMAKE_SOURCE_DIR}/3rdParty/catch
  ${CMAKE_SOURCE_DIR}/3rdParty/fakeit
)
