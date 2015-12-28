#!/bin/bash
#
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
#
#
CMAKE_INSTALL_PREFIX="$1"
DESTINATION="$2"
LIB="$3"
CMAKE_SHARED_LIBRARY_SUFFIX="$4"
SCIDB_VERSION_MAJOR="$5"
SCIDB_VERSION_MINOR="$6"
SCIDB_P4="$7"
if [ "$SCIDB_P4" = "scidb" ]; then
    PRIORITY=10
else
    PRIORITY=20
fi

mkdir -p ${CMAKE_INSTALL_PREFIX}/alternatives
mkdir -p ${CMAKE_INSTALL_PREFIX}/admindir

update-alternatives --altdir ${CMAKE_INSTALL_PREFIX}/alternatives --admindir ${CMAKE_INSTALL_PREFIX}/admindir --install \
${CMAKE_INSTALL_PREFIX}/${DESTINATION}/lib${LIB}${CMAKE_SHARED_LIBRARY_SUFFIX} \
${LIB}.${SCIDB_VERSION_MAJOR}.${SCIDB_VERSION_MINOR} \
${CMAKE_INSTALL_PREFIX}/${DESTINATION}/lib${LIB}-${SCIDB_P4}${CMAKE_SHARED_LIBRARY_SUFFIX} \
${PRIORITY}
