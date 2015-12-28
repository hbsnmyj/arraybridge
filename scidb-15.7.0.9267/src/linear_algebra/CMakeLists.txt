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

message(STATUS "**BEGIN linear_algebra/CMakeLists.txt --------------------------------")

# shared library for user defined objects
set (linear_algebra_src
    plugin.cpp
    spgemm/LogicalSpgemm.cpp
    spgemm/PhysicalSpgemm.cpp
    stats/HypergeometricFunctions.cpp
)

message(STATUS "Debug: CMAKE_C_FLAGS is ${CMAKE_C_FLAGS}")
message(STATUS "Debug: CMAKE_CXX_FLAGS is ${CMAKE_CXX_FLAGS}")

#
# complete the linar_algebra library settings
#
add_library(linear_algebra SHARED ${linear_algebra_src})

extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "liblinear_algebra${CMAKE_SHARED_LIBRARY_SUFFIX}" linear_algebra)
set_target_properties(linear_algebra PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)


message(STATUS "**END linear_algebra/CMakeLists.txt --------------------------------")
