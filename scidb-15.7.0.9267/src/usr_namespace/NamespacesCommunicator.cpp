/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2015 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/*
 * Security.cpp
 *
 *  Created on: May 19, 2015
 *      Author: mcorbett@paradigm4.com
 */

#include <usr_namespace/NamespacesCommunicator.h>


#include "log4cxx/logger.h"
#include <string>
#include "util/PluginManager.h"
#include <memory>
#include <boost/assign.hpp>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <util/session/Session.h>

namespace scidb
{

    namespace namespaces
    {

    } // namespace namespaces
} // namespace scidb
