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

if (NOT WITHOUT_SERVER)
    set (usr_namespace_src
        ConnectionClientCommunicator.cpp
        NamespaceDesc.cpp
        NamespaceObject.cpp
        NamespacesCommunicator.cpp
        RoleDesc.cpp
        SecurityCommunicator.cpp
        UserDesc.cpp
    )

    file(GLOB usr_namespace_include "*.h")

    add_library(usr_namespace_lib STATIC ${usr_namespace_src} ${usr_namespace_include})
    target_link_libraries(usr_namespace_lib ${Boost_LIBRARIES} ${LOG4CXX_LIBRARIES})
    target_link_libraries(usr_namespace_lib network_lib)
endif()
