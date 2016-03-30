# - Try to find CityHash
#
# Once done this will define
#
#  CityHash_FOUND - system has CityHash
#  CityHash_INCLUDE_DIR - the CityHash include directory
#  CityHash_LIBRARY - Link these to use CityHash
#
find_path(CityHash_INCLUDE_DIR
  NAMES city.h
  HINTS "/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/cityhash/include/cityhash" "/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/cityhash/include"
  )

find_library(CityHash_LIBRARY
  NAMES cityhash
  HINTS "/opt/scidb/${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR}/3rdparty/cityhash/lib"
  )

set(CityHash_FOUND TRUE)
if ("${CityHash_INCLUDE_DIR}" STREQUAL "CityHash_INCLUDE_DIR-NOTFOUND")
  set(CityHash_FOUND FALSE)
endif()
if ("${CityHash_LIBRARY}" STREQUAL "CityHash_LIBRARY-NOTFOUND")
  set(CityHash_FOUND FALSE)
endif()

if(NOT CityHash_FOUND)
  if(CityHash_FIND_REQUIRED STREQUAL "CityHash_FIND_REQUIRED")
    message(FATAL_ERROR "CMake was unable to find CityHash")
  endif()
endif()
