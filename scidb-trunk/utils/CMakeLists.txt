########################################
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
########################################

# Add subfolder of every utility
add_subdirectory("iquery")
#Building shim is temporarily disabled because it does not work with C++14. See #4745.
#add_subdirectory("shim")
add_subdirectory("scidblib")

set(CONFIGURE_PREPARE_DB_METADATA_LOCAL "data/meta.sql")

set(CONFIGURE_PREPARE_DB_METADATA "${CMAKE_INSTALL_PREFIX}/share/scidb/meta.sql")

set(CONFIGURE_SCIDBPY_CONFIG "${CMAKE_INSTALL_PREFIX}/etc/config.ini")

configure_file(scidb-prepare-db.sh.in "${GENERAL_OUTPUT_DIRECTORY}/scidb-prepare-db.sh")
configure_file(init-db.sh "${GENERAL_OUTPUT_DIRECTORY}/init-db.sh" COPYONLY)
configure_file(log1.properties "${GENERAL_OUTPUT_DIRECTORY}/log1.properties" COPYONLY)
if(CMAKE_BUILD_TYPE STREQUAL "Valgrind")
  set(CONFIGURE_SCIDB_PY_VALGRIND "use_valgrind = True")
else(CMAKE_BUILD_TYPE STREQUAL "Valgrind")
  set(CONFIGURE_SCIDB_PY_VALGRIND "use_valgrind = False")
endif(CMAKE_BUILD_TYPE STREQUAL "Valgrind")
configure_file(scidb.py.in "${GENERAL_OUTPUT_DIRECTORY}/scidb.py")
configure_file(prelude.txt.in "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/prelude.txt")
configure_file(scidb_cores "${GENERAL_OUTPUT_DIRECTORY}/packaging_only/scidb_cores")
configure_file(loadpipe.py "${GENERAL_OUTPUT_DIRECTORY}/loadpipe.py")
configure_file(scidb_backup.py "${GENERAL_OUTPUT_DIRECTORY}/scidb_backup.py")
configure_file(calculate_chunk_length.py "${GENERAL_OUTPUT_DIRECTORY}/calculate_chunk_length.py")
configure_file(remove_arrays.py "${GENERAL_OUTPUT_DIRECTORY}/remove_arrays.py")
configure_file(list_arrays.py "${GENERAL_OUTPUT_DIRECTORY}/list_arrays.py")

add_executable(benchGen benchGen.cc) 
extractDebugInfo("${GENERAL_OUTPUT_DIRECTORY}" "benchGen" benchGen)
set_target_properties(benchGen PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${GENERAL_OUTPUT_DIRECTORY})
