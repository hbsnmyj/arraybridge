########################################
# BEGIN_COPYRIGHT
#
# Copyright (C) 2014-2015 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
########################################

# TODO: enable once all build machines are upgraded
#cmake_minimum_required(VERSION 2.8.5)

message(STATUS "**BEGIN util/dmalloc/CMakeLists.txt --------------------------------")

# shared library for user defined objects
set (dmalloc_src
    dmalloc.cpp
)

message(STATUS "Debug: CMAKE_C_FLAGS is ${CMAKE_C_FLAGS}")
message(STATUS "Debug: CMAKE_CXX_FLAGS is ${CMAKE_CXX_FLAGS}")
message(STATUS "Debug: GENERAL_OUTPUT_DIRECTORY is ${GENERAL_OUTPUT_DIRECTORY}")

add_library(dmalloc SHARED ${dmalloc_src})

extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "libdmalloc${CMAKE_SHARED_LIBRARY_SUFFIX}" dmalloc)
set_target_properties(dmalloc PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})


message(STATUS "**END util/dmalloc/CMakeLists.txt --------------------------------")
