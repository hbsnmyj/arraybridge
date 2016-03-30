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

set (compression_src
    ByteOutputItr.cpp
    ByteInputItr.cpp
    BitInputItr.cpp
    BitOutputItr.cpp
    Compressor.cpp
    RunLengthEncoding.cpp
    BitmapEncoding.cpp
    NullSuppression.cpp
    DictionaryEncoding.cpp
)

file(GLOB compression_include "*.h")

# These files are to be deprecated/removed. No reason to waste time
# checking the conversions. See Issue #4996
set_source_files_properties(RunLengthEncoding.cpp BitmapEncoding.cpp NullSuppression.cpp DictionaryEncoding.cpp
                             PROPERTIES COMPILE_FLAGS "-Wno-conversion")

add_library(compression_lib STATIC ${compression_src} ${compression_include})

target_link_libraries(compression_lib ${Boost_LIBRARIES} ${ZLIB_LIBRARIES} ${BZIP2_LIBRARIES} system_lib)
