#!/bin/bash
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2015 SciDB, Inc.
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

# Note: Prefer "127.0.0.1" to "localhost" because the former is used
# to generate the default config.ini, and ~/.pgpass Postgres auth
# requires an exact string match.

export DB_HOST=${SCIDB_HOST:=127.0.0.1}
export DB_PORT=5432
export DB_NAME=${SCIDB_NAME:=mydb}
export DB_USER=${SCIDB_DB_USER:=mydb}
export DB_PASSWD=${SCIDB_DB_PASSWD:=mydb}
export IQUERY_HOST=${SCIDB_HOST:=127.0.0.1}
export IQUERY_PORT=${SCIDB_PORT:=1239}
export SCIDB_CLUSTER_NAME=$DB_NAME

export PYTHONPATH="${SCIDB_SOURCE_PATH}/tests/harness/pyLib/"

if [ "${SCIDB_BUILD_PATH}" != "" -a "${SCIDB_DATA_PATH}" != "" ] ; then
   rm -f ${SCIDB_DATA_PATH}/0/tests
   ln -s ${SCIDB_SOURCE_PATH}/tests ${SCIDB_DATA_PATH}/0/tests

   export DOC_DATA="${SCIDB_SOURCE_PATH}/tests/harness/testcases/data/doc"
   export TESTCASES_DIR="${SCIDB_BUILD_PATH}/tests/harness/testcases/"
   export TEST_DATA_DIR="${SCIDB_DATA_PATH}/0/tests/harness/testcases/data"
   export TEST_UTILS_DIR="${SCIDB_SOURCE_PATH}/tests/utils"
   export TEST_BIN_DIR="${SCIDB_BUILD_PATH}/bin"
fi

function create_login_file() {
    local login_file=$1/${USER}_root_login
    echo "[security_password]" > $login_file
    echo "user-name      = root" >> $login_file
    echo "user-password  = Paradigm4" >> $login_file
    chmod 600 $login_file
    echo "$login_file"   # return $login_file
}

export SCIDB_CONFIG_USER=$(create_login_file ${SCIDB_BUILD_PATH})
# echo "SCIDB_CONFIG_USER=$SCIDB_CONFIG_USER"
