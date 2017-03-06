# ArangoDB-IResearch integration
#
# Copyright (c) 2017 by EMC Corporation, All Rights Reserved
#
# This software contains the intellectual property of EMC Corporation or is licensed to
# EMC Corporation from third parties. Use of this software and the intellectual property
# contained therein is expressly limited to the terms and conditions of the License
# Agreement under which it is provided by or on behalf of EMC.

# - Find iResearch (iresearch.dll core/*/*.hpp)
# This module defines
#  IRESEARCH_CXX_FLAGS, flags to use with CXX compilers
#  IRESEARCH_INCLUDE, directory containing headers
#  IRESEARCH_LIBRARY_DIR, directory containing iResearch libraries
#  IRESEARCH_ROOT, root directory of iResearch
#  IRESEARCH_SHARED_LIBS, path to libiresearch.so/libiresearch.dll
#  IRESEARCH_STATIC_LIBS, path to libiresearch.a/libiresearch.lib
#  IRESEARCH_SHARED_LIB_RESOURCES, shared libraries required to use iResearch
#  IRESEARCH_FOUND, whether iResearch has been found

if ("${IRESEARCH_ROOT}" STREQUAL "")
  set(IRESEARCH_ROOT "$ENV{IRESEARCH_ROOT}")
  if ("${IRESEARCH_ROOT}" STREQUAL "")
    set(IRESEARCH_ROOT "${CMAKE_SOURCE_DIR}/3rdParty/iresearch")
  endif()
  if (NOT "${IRESEARCH_ROOT}" STREQUAL "")
    string(REPLACE "\"" "" IRESEARCH_ROOT ${IRESEARCH_ROOT})
  endif()
endif()

if (NOT "${IRESEARCH_ROOT}" STREQUAL "")
  string(REPLACE "\\" "/" IRESEARCH_ROOT ${IRESEARCH_ROOT})
endif()

if (NOT EXISTS "${CMAKE_SOURCE_DIR}/3rdParty/iresearch" AND NOT EXISTS "${IRESEARCH_ROOT}")
  execute_process(
    COMMAND git submodule update --init --recursive -- "3rdParty/iresearch"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endif()

set(IRESEARCH_SEARCH_HEADER_PATHS
  ${IRESEARCH_ROOT}/core
  ${IRESEARCH_ROOT}/external
  ${IRESEARCH_ROOT}/build/core
)

set(IRESEARCH_SEARCH_LIB_PATH
  ${IRESEARCH_ROOT}/build/bin
  ${IRESEARCH_ROOT}/build/bin/Release
  ${IRESEARCH_ROOT}/build/bin/Debug
)

set(IRESEARCH_CXX_FLAGS " ") # has to be a non-empty string
#set(IRESEARCH_CXX_FLAGS "-DIRESEARCH_DLL") Arango now links statically against iResearch

if (MSVC)
  # do not use min/max '#define' from Microsoft since iResearch declares methods
  set(IRESEARCH_CXX_FLAGS ${IRESEARCH_CXX_FLAGS} "-DNOMINMAX")
endif ()

foreach (ELEMENT document iql/parser.hh utils/version_defines.hpp)
  unset(ELEMENT_INC CACHE)
  find_path(ELEMENT_INC
    NAMES ${ELEMENT}
    PATHS ${IRESEARCH_SEARCH_HEADER_PATHS}
    NO_DEFAULT_PATH # make sure we don't accidentally pick up a different version
  )

  if (NOT ELEMENT_INC)
    unset(IRESEARCH_INCLUDE)
    unset(IRESEARCH_INCLUDE CACHE)
    break()
  endif ()

  list(APPEND IRESEARCH_INCLUDE ${ELEMENT_INC})
endforeach ()

find_library(IRESEARCH_SHARED_LIB
  NAMES libiresearch.lib libiresearch.so
  PATHS ${IRESEARCH_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH
)

find_library(IRESEARCH_STATIC_LIB
  NAMES libiresearch-scrt-s.lib libiresearch-s.a
  PATHS ${IRESEARCH_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH
)

find_library(IRESEARCH_ANALYZER_TEXT_SHARED_LIB
  NAMES libanalyzer-text.lib libanalyzer-text.so
  PATHS ${IRESEARCH_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH
)

find_library(IRESEARCH_ANALYZER_TEXT_STATIC_LIB
  NAMES libanalyzer-text-scrt-s.lib libanalyzer-text-s.a
  PATHS ${IRESEARCH_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH
)

find_library(IRESEARCH_FORMAT10_SHARED_LIB
  NAMES libformat-1_0.lib libformat-1_0.so
  PATHS ${IRESEARCH_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH
)

find_library(IRESEARCH_FORMAT10_STATIC_LIB
  NAMES libformat-1_0-scrt-s.lib libformat-1_0-s.a
  PATHS ${IRESEARCH_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH
)

find_library(IRESEARCH_TFIDF_SHARED_LIB
  NAMES libscorer-tfidf.lib libscorer-tfidf.so
  PATHS ${IRESEARCH_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH
)

find_library(IRESEARCH_TFIDF_STATIC_LIB
  NAMES libscorer-tfidf-scrt-s.lib libscorer-tfidf-s.a
  PATHS ${IRESEARCH_SEARCH_LIB_PATH}
  NO_DEFAULT_PATH
)

# ensure Boost DLLs are searched for
if (MSVC)
  set(Boost_USE_STATIC_LIBS OFF)
  set(Boost_USE_STATIC_RUNTIME OFF)
endif ()

find_package(Boost COMPONENTS filesystem locale system REQUIRED)
#find_package(OpenFST REQUIRED) Arango now links statically against iResearch

foreach (ELEMENT ${IRESEARCH_SHARED_LIB} ${IRESEARCH_ANALYZER_TEXT_SHARED_LIB} ${IRESEARCH_FORMAT10_SHARED_LIB} ${IRESEARCH_TFIDF_SHARED_LIB} ${OpenFST_LIBRARY})
  get_filename_component(ELEMENT_FILENAME ${ELEMENT} NAME)
  string(REGEX MATCH "^(.*)\\.(lib|so)$" ELEMENT_MATCHES ${ELEMENT_FILENAME})

  if (NOT ELEMENT_MATCHES)
    continue()
  endif ()

  get_filename_component(ELEMENT_DIRECTORY ${ELEMENT} DIRECTORY)
  file(GLOB ELEMENT_LIB
    "${ELEMENT_DIRECTORY}/${CMAKE_MATCH_1}.so"
    "${ELEMENT_DIRECTORY}/lib${CMAKE_MATCH_1}.so"
    "${ELEMENT_DIRECTORY}/${CMAKE_MATCH_1}.so.*"
    "${ELEMENT_DIRECTORY}/lib${CMAKE_MATCH_1}.so.*"
    "${ELEMENT_DIRECTORY}/${CMAKE_MATCH_1}.dll"
    "${ELEMENT_DIRECTORY}/lib${CMAKE_MATCH_1}.dll"
    "${ELEMENT_DIRECTORY}/${CMAKE_MATCH_1}[0-9][0-9].dll"
    "${ELEMENT_DIRECTORY}/lib${CMAKE_MATCH_1}[0-9][0-9].dll"
    "${ELEMENT_DIRECTORY}/${CMAKE_MATCH_1}.pdb"
    "${ELEMENT_DIRECTORY}/lib${CMAKE_MATCH_1}.pdb"
    "${ELEMENT_DIRECTORY}/${CMAKE_MATCH_1}[0-9][0-9].pdb"
    "${ELEMENT_DIRECTORY}/lib${CMAKE_MATCH_1}[0-9][0-9].pdb"
  )

  if (ELEMENT_LIB)
    list(APPEND IRESEARCH_SHARED_LIB_RESOURCES ${ELEMENT_LIB})
  endif()
endforeach ()

# find 3rd-party iResearch shared library dependencies
foreach(ELEMENT icudt icudata icuin icui18n icuuc)
  foreach(ELEMENT_DIRECTORY ${IRESEARCH_SEARCH_LIB_PATH})
    file(GLOB ELEMENT_LIB
      "${ELEMENT_DIRECTORY}/${ELEMENT}.so"
      "${ELEMENT_DIRECTORY}/lib${ELEMENT}.so"
      "${ELEMENT_DIRECTORY}/${ELEMENT}.so.*"
      "${ELEMENT_DIRECTORY}/lib${ELEMENT}.so.*"
      "${ELEMENT_DIRECTORY}/${ELEMENT}.dll"
      "${ELEMENT_DIRECTORY}/lib${ELEMENT}.dll"
      "${ELEMENT_DIRECTORY}/${ELEMENT}[0-9][0-9].dll"
      "${ELEMENT_DIRECTORY}/lib${ELEMENT}[0-9][0-9].dll"
      "${ELEMENT_DIRECTORY}/${ELEMENT}.pdb"
      "${ELEMENT_DIRECTORY}/lib${ELEMENT}.pdb"
      "${ELEMENT_DIRECTORY}/${ELEMENT}[0-9][0-9].pdb"
      "${ELEMENT_DIRECTORY}/lib${ELEMENT}[0-9][0-9].pdb"
    )

    if (ELEMENT_LIB)
      list(APPEND IRESEARCH_SHARED_LIB_RESOURCES ${ELEMENT_LIB})
    endif()
  endforeach()
endforeach()

if (IRESEARCH_INCLUDE AND IRESEARCH_STATIC_LIB)
#if (IRESEARCH_INCLUDE AND IRESEARCH_SHARED_LIB) Arango now links statically against iResearch
  set(IRESEARCH_FOUND TRUE)
  set(IRESEARCH_INCLUDE ${IRESEARCH_INCLUDE} ${Boost_INCLUDE_DIRS}) # Boost required to compile with iResearch
  set(IRESEARCH_LIBRARY_DIR
    "${IRESEARCH_SEARCH_LIB_PATH}"
    CACHE PATH
    "Directory containing iResearch libraries"
    FORCE
  )
  # library order is important
  foreach(ELEMENT ${IRESEARCH_ANALYZER_TEXT_SHARED_LIB} ${IRESEARCH_FORMAT10_SHARED_LIB} ${IRESEARCH_TFIDF_SHARED_LIB} ${IRESEARCH_SHARED_LIB})
    if(ELEMENT)
      list(APPEND IRESEARCH_SHARED_LIBS ${ELEMENT})
    endif()
  endforeach()
  foreach(ELEMENT ${IRESEARCH_STATIC_LIB})
    if(ELEMENT)
      list(APPEND IRESEARCH_STATIC_LIBS ${ELEMENT})
    endif()
  endforeach()
else ()
  set(IRESEARCH_FOUND FALSE)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(IRESEARCH
  DEFAULT_MSG
  IRESEARCH_INCLUDE
  IRESEARCH_SHARED_LIBS
  IRESEARCH_STATIC_LIBS
  IRESEARCH_CXX_FLAGS
)
message("IRESEARCH_INCLUDE: " ${IRESEARCH_INCLUDE})
message("IRESEARCH_LIBRARY_DIR: " ${IRESEARCH_LIBRARY_DIR})
message("IRESEARCH_SHARED_LIBS: " ${IRESEARCH_SHARED_LIBS})
message("IRESEARCH_STATIC_LIBS: " ${IRESEARCH_STATIC_LIBS})
message("IRESEARCH_SHARED_LIB_RESOURCES: " ${IRESEARCH_SHARED_LIB_RESOURCES})

mark_as_advanced(
  IRESEARCH_CXX_FLAGS
  IRESEARCH_INCLUDE
  IRESEARCH_LIBRARY_DIR
  IRESEARCH_SHARED_LIBS
  IRESEARCH_STATIC_LIBS
  IRESEARCH_SHARED_LIB_RESOURCES
)
