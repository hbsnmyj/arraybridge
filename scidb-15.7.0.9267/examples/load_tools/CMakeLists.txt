########################################
# BEGIN_COPYRIGHT
#
# Copyright (C) 2015-2015 SciDB, Inc.
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

# shared library for user defined objects
set (load_tools_src
    Functions.cpp
    LogicalParse.cpp
    PhysicalParse.cpp
    LogicalSplit.cpp
    PhysicalSplit.cpp
    plugin.cpp
)

file(GLOB load_tools_include "*.h")
add_library(prototype_load_tools SHARED ${load_tools_src} ${load_tools_include})
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}/plugins" "libprototype_load_tools${CMAKE_SHARED_LIBRARY_SUFFIX}" prototype_load_tools)
set_target_properties(prototype_load_tools PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY}/plugins)