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
 * OperatorLibrary.cpp
 *
 *  Created on: Feb 11, 2010
 *      Author: roman.simakov@gmail.com
 */

#include <map>
#include <string>

#include <log4cxx/logger.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/helpers/exception.h>

#include <memory>
#include <boost/foreach.hpp>

#include "query/OperatorLibrary.h"
#include "system/Exceptions.h"
#include "system/SystemCatalog.h"
#include "util/PluginManager.h"

#define LOGICAL_BUILDIN_OPERATOR(op)  BaseLogicalOperatorFactory* get_logicalFactory##op()
#define PHYSICAL_BUILDIN_OPERATOR(op)  BasePhysicalOperatorFactory* get_physicalFactory##op()

namespace scidb
{

#include "query/ops/BuildInOps.inc"

} // namespace

#undef LOGICAL_BUILDIN_OPERATOR
#undef PHYSICAL_BUILDIN_OPERATOR

#define LOGICAL_BUILDIN_OPERATOR(op) addLogicalOperatorFactory(get_logicalFactory##op())
#define PHYSICAL_BUILDIN_OPERATOR(op) addPhysicalOperatorFactory(get_physicalFactory##op())

using namespace std;
using namespace boost;

namespace scidb
{

// to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.operator"));

OperatorLibrary::OperatorLibrary()
{
#include "query/ops/BuildInOps.inc"
}


std::shared_ptr<LogicalOperator> OperatorLibrary::createLogicalOperator(const std::string& logicalName,
        const std::string& alias)
{
    LOG4CXX_TRACE(logger, "Creating logical operator: " << logicalName);

    LogicalOperatorFactoriesMap::const_iterator lOpIt = _logicalOperatorFactories.find(logicalName);

    if (lOpIt == _logicalOperatorFactories.end())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_LOGICAL_OP_DOESNT_EXIST) << logicalName;
    }

    return (*lOpIt).second->createLogicalOperator(alias);
}


std::shared_ptr<PhysicalOperator> OperatorLibrary::createPhysicalOperator(const std::string& logicalName,
        const std::string& physicalName, const PhysicalOperator::Parameters& parameters, const ArrayDesc& schema)
{
    LOG4CXX_TRACE(logger, "Creating physical operator: " << physicalName << " for logical operator: " << logicalName);

    PhysicalOperatorFactoriesMap::const_iterator lOpIt = _physicalOperatorFactories.find(logicalName);

    if (lOpIt != _physicalOperatorFactories.end())
    {
        PhysicalOperatorFactories::const_iterator pOpIt = (*lOpIt).second.find(physicalName);

        if (pOpIt != (*lOpIt).second.end())
        {
            return (*pOpIt).second->createPhysicalOperator(parameters, schema);
        }
    }

    throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_PHYSICAL_OP_DOESNT_EXIST) << physicalName << logicalName;
}


void OperatorLibrary::createPhysicalOperators(const std::string& logicalName,
        std::vector< std::shared_ptr<PhysicalOperator> >& physicalOperators,
        const PhysicalOperator::Parameters& parameters, const ArrayDesc& schema)
{
    // Adding build-in operators
    PhysicalOperatorFactoriesMap::const_iterator pOpIt = _physicalOperatorFactories.find(logicalName);

    if (pOpIt == _physicalOperatorFactories.end())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_LOGICAL_OP_DOESNT_EXIST) << logicalName;
    }

    BOOST_FOREACH(const PhysicalOperatorFactoriesPair &pOpFactory, (*pOpIt).second)
    {
        LOG4CXX_TRACE(logger, "Creating physical operator: " << pOpFactory.first << " for logical operator: " << logicalName);
        physicalOperators.push_back( pOpFactory.second->createPhysicalOperator(parameters, schema) );
    }
}


void OperatorLibrary::addLogicalOperatorFactory(BaseLogicalOperatorFactory* factory)
{
    const string& logicalName = factory->getLogicalName();

    LOG4CXX_DEBUG(logger, "Add logical operator factory: " << logicalName);

    if (_logicalOperatorFactories[logicalName] == NULL)
        _logicalOperatorFactories[logicalName] = factory;
    else
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_LOGICAL_OP_ALREADY_REGISTERED) << logicalName;

    _operatorLibraries.addObject(logicalName);
}


void OperatorLibrary::addPhysicalOperatorFactory(BasePhysicalOperatorFactory* factory)
{
    const string& logicalName = factory->getLogicalName();
    const string& physicalName = factory->getPhysicalName();

    LOG4CXX_DEBUG(logger, "Add physical operator factory: " << physicalName
                  << " for logical operator: " << logicalName);

    // Logical operator MUST already exist.  This catches inconsistent naming
    // of the logical operator across DECLARE_*_OPERATOR_FACTORY() macros.
    // For example, if you want to hide the foo() operator by changing its name
    // to _foo(), you must change the name in both macro calls, else we throw.
    if (_logicalOperatorFactories[logicalName] == NULL) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL,
                               SCIDB_LE_LOGICAL_OP_DOESNT_EXIST) << logicalName;
    }

    if (_physicalOperatorFactories[logicalName][physicalName] == NULL) {
        _physicalOperatorFactories[logicalName][physicalName] = factory;
    } else {
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_PHYSICAL_OP_ALREADY_REGISTERED)
            << physicalName << logicalName;
    }
}


void OperatorLibrary::getPhysicalNames(const string& logicalName,
        vector<string> &physicalOperatorsNames)
{
    PhysicalOperatorFactoriesMap::const_iterator pOpIt = _physicalOperatorFactories.find(logicalName);

    if (pOpIt == _physicalOperatorFactories.end())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_LOGICAL_OP_DOESNT_EXIST) << logicalName;
    }

    physicalOperatorsNames.reserve(_physicalOperatorFactories.size());

    BOOST_FOREACH(const PhysicalOperatorFactoriesPair &pOpFactory, (*pOpIt).second)
    {
        physicalOperatorsNames.push_back(pOpFactory.first);
    }
}


void OperatorLibrary::getLogicalNames(vector<string> &logicalOperatorsNames, bool showHidden)
{
    LogicalOperatorFactories::iterator it;
    for(it = _logicalOperatorFactories.begin(); it != _logicalOperatorFactories.end(); ++it)
    {
        std::shared_ptr<LogicalOperator> logicalOperator = it->second->createLogicalOperator("");
        if (showHidden || !isHiddenOp(logicalOperator->getLogicalName())) {
            logicalOperatorsNames.push_back(it->first);
        }
    }
}

bool OperatorLibrary::hasLogicalOperator(const string &logicalOperatorName)
{
    return (_logicalOperatorFactories.find(logicalOperatorName) != _logicalOperatorFactories.end());
}

} // namespace
