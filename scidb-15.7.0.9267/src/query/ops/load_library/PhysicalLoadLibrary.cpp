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

/**
 * @file PhysicalLoadLibrary.cpp
 *
 * @brief Physical DDL operator which load user defined library
 *
 * @author roman.simakov@gmail.com
 */

#include <string.h>

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "util/PluginManager.h"
#include "query/FunctionDescription.h"
#include "query/OperatorLibrary.h"
#include "query/FunctionLibrary.h"
#include "query/QueryProcessor.h"
#include "query/Query.h"


using namespace std;
using namespace boost;

namespace scidb
{


class PhysicalLoadLibrary: public PhysicalOperator
{
public:
    PhysicalLoadLibrary(const std::string& logicalName, const std::string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    std::shared_ptr<Array> execute(std::vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);

        const string libraryName = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getString();

        const bool isCoordinator = query->isCoordinator();

        getInjectedErrorListener().check(); // testing only, noop in release build

        PluginManager::getInstance()->loadLibrary(libraryName, isCoordinator);

        // It's DDL command and should not return a value
        return std::shared_ptr< Array>();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalLoadLibrary, "load_library", "impl_load_library")

} //namespace
