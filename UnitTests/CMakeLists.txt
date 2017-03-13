# -*- mode: CMAKE; -*-

include_directories(.)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/${CMAKE_TEST_DIRECTORY}")

################################################################################
## basics_suite
################################################################################

add_executable(${TEST_BASICS_SUITE}
  Basics/Runner.cpp
  Basics/conversions-test.cpp
  Basics/csv-test.cpp
  Basics/files-test.cpp
  Basics/fpconv-test.cpp
  Basics/json-test.cpp
  Basics/hashes-test.cpp
  Basics/associative-multi-pointer-test.cpp
  Basics/associative-multi-pointer-nohashcache-test.cpp
  Basics/skiplist-test.cpp
  Basics/priorityqueue-test.cpp
  Basics/string-buffer-test.cpp
  Basics/string-utf8-normalize-test.cpp
  Basics/string-utf8-test.cpp
  Basics/string-test.cpp
  Basics/structure-size-test.cpp
  Basics/vector-test.cpp
  Basics/EndpointTest.cpp
  Basics/StringBufferTest.cpp
  Basics/StringUtilsTest.cpp
  Basics/icu-helper.cpp
  ../lib/ApplicationFeatures/LanguageFeature.cpp
  ../lib/Basics/WorkMonitorDummy.cpp
)

include_directories(
  ${TEST_BASICS_SUITE}
  PUBLIC ${Boost_UNIT_TEST_INCLUDE_DIR}
)

target_link_libraries(${TEST_BASICS_SUITE}
  ${LIB_ARANGO}
  boost_system
  boost_boost
  ${MSVC_LIBS}
  ${SYSTEM_LIBRARIES}
)

if (NOT USE_PRECOMPILED_V8)
  add_dependencies(basics_suite v8_build)
endif ()

################################################################################
## geo_suite
################################################################################

add_executable(${TEST_GEO_SUITE}
  Geo/georeg.cpp
  ../lib/Basics/WorkMonitorDummy.cpp
  ../arangod/MMFiles/geo-index.cpp
)

target_link_libraries(${TEST_GEO_SUITE}
  ${LIB_ARANGO}
  ${MSVC_LIBS}
  ${SYSTEM_LIBRARIES}
)

if (NOT USE_PRECOMPILED_V8)
  add_dependencies(geo_suite v8_build)
endif ()

################################################################################
## iresearch_suite
################################################################################

if (USE_IRESEARCH)
  find_package(Boost COMPONENTS unit_test_framework)
  find_package(IResearch REQUIRED)

  add_executable(${TEST_IRESEARCH_SUITE}
    ../lib/Basics/WorkMonitorDummy.cpp # required by LIB_ARANGO
    IResearch/Runner.cpp
  )

  target_include_directories(${TEST_IRESEARCH_SUITE}
    PRIVATE ${Boost_UNIT_TEST_INCLUDE_DIR}
    PRIVATE ${IRESEARCH_INCLUDE}
  )

  target_link_libraries(${TEST_IRESEARCH_SUITE}
    ${LIB_ARANGO_IRESEARCH}
    ${LIB_ARANGO}
    ${OPENSSL_LIBRARIES} # required by LIB_ARANGO
    zlibstatic # required by LIB_ARANGO
  )

  add_dependencies(${TEST_IRESEARCH_SUITE}
   ${LIB_ARANGO_IRESEARCH}
    ${LIB_ARANGO}
    zlibstatic # required by LIB_ARANGO
  )

  find_package(OpenSSL REQUIRED)
  list(APPEND IRESEARCH_LIB_RESOURCES
    "${LIB_EAY_DEBUG_DLL}"
    "${SSL_EAY_DEBUG_DLL}"
    "${LIB_EAY_RELEASE_DLL}"
    "${SSL_EAY_RELEASE_DLL}"
  )

  foreach(ELEMENT ${IRESEARCH_LIB_RESOURCES})
    add_custom_command(
      TARGET ${TEST_IRESEARCH_SUITE} POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E echo "copying library resource:" "${ELEMENT}" " -> " "$<TARGET_FILE_DIR:${TEST_IRESEARCH_SUITE}>"
      COMMAND cp -lf ${ELEMENT} $<TARGET_FILE_DIR:${TEST_IRESEARCH_SUITE}> || ${CMAKE_COMMAND} -E copy ${ELEMENT} $<TARGET_FILE_DIR:${TEST_IRESEARCH_SUITE}>
    )
  endforeach()
endif()
