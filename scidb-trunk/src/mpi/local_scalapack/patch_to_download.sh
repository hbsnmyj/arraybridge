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
################################################################
# Modify all files in installer
# substituting NETLIB_MIRROR for NETLIB_URL
#
# ARGUMENTS
SETUP_DIR=$1
NETLIB_SITE=$2
MIRROR_SITE=$3
################################################################
cd ${SETUP_DIR}
find . -type f -exec sed -i "s@${2}@${3}@g" {} \;
#
# Also if NETLIB_SITE has prefix www. stripit and search for that also
#
find . -type f -exec sed -i "s@${2#www.}@${3#www.}@g" {} \;
