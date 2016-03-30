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
 * LogicalRename.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include <boost/format.hpp>
#include <log4cxx/logger.h>
#include <query/Operator.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <usr_namespace/NamespacesCommunicator.h>
#include <usr_namespace/Permissions.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.logical_rename"));

using namespace std;
using namespace boost;

/**
 * @brief The operator: rename().
 *
 * @par Synopsis:
 *   rename( oldArray, newArray )
 *
 * @par Summary:
 *   Changes the name of an array.
 *
 * @par Input:
 *   - oldArray: an existing array.
 *   - newArray: the new name of the array.
 *
 * @par Output array:
 *   NULL
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalRename: public LogicalOperator
{
public:
	LogicalRename(const string& logicalName, const std::string& alias):
	    LogicalOperator(logicalName, alias)
	{
	    _properties.exclusive = true;
	    _properties.ddl = true;
		ADD_PARAM_IN_ARRAY_NAME()
		ADD_PARAM_OUT_ARRAY_NAME()
	}

    std::string inferPermissions(std::shared_ptr<Query>& query)
    {
        // Ensure we have the proper permissions
        std::string permissions;
        permissions.push_back(scidb::permissions::namespaces::CreateArray);
        permissions.push_back(scidb::permissions::namespaces::DeleteArray);
        return permissions;
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr<Query> query)
    {
        assert(schemas.size() == 0);
        assert(_parameters.size() == 2);
        assert(((std::shared_ptr<OperatorParam>&)_parameters[0])->getParamType() == PARAM_ARRAY_REF);
        assert(((std::shared_ptr<OperatorParam>&)_parameters[1])->getParamType() == PARAM_ARRAY_REF);

        std::string oldArrayName;
        std::string oldNamespaceName;
        std::string newArrayName;
        std::string newNamespaceName;

        const string &oldArrayNameOrg =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        query->getNamespaceArrayNames(oldArrayNameOrg, oldNamespaceName, oldArrayName);

        const string &newArrayNameOrg =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[1])->getObjectName();
        query->getNamespaceArrayNames(newArrayNameOrg, newNamespaceName, newArrayName);

        if(newNamespaceName != oldNamespaceName)
        {
            throw USER_QUERY_EXCEPTION(
                SCIDB_SE_INFER_SCHEMA, SCIDB_LE_CANNOT_RENAME_ACROSS_NAMESPACES,
                _parameters[1]->getParsingContext())
                << ArrayDesc::makeQualifiedArrayName(oldNamespaceName, oldArrayName)
                << ArrayDesc::makeQualifiedArrayName(newNamespaceName, newArrayName);
        }

        if (scidb::namespaces::Communicator::containsArray(newNamespaceName, newArrayName))
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAY_ALREADY_EXIST,
                _parameters[1]->getParsingContext()) << newArrayName;
        }
        ArrayDesc arrDesc;
        arrDesc.setDistribution(defaultPartitioning());
        arrDesc.setResidency(query->getDefaultArrayResidency());
        return arrDesc;
    }


    void inferArrayAccess(std::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);
        SCIDB_ASSERT(_parameters.size() > 1);

        // from
        SCIDB_ASSERT(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& oldArrayNameOrg = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        SCIDB_ASSERT(oldArrayNameOrg.find('@') == std::string::npos);

        std::string oldArrayName;
        std::string oldNamespaceName;
        query->getNamespaceArrayNames(oldArrayNameOrg, oldNamespaceName, oldArrayName);

        std::shared_ptr<SystemCatalog::LockDesc> lock(
            new SystemCatalog::LockDesc(
                oldNamespaceName,
                oldArrayName,
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                SystemCatalog::LockDesc::COORD,
                SystemCatalog::LockDesc::RNF));
        std::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        SCIDB_ASSERT(resLock);
        SCIDB_ASSERT(resLock->getLockMode() >= SystemCatalog::LockDesc::RNF);

        // to
        SCIDB_ASSERT(_parameters[1]->getParamType() == PARAM_ARRAY_REF);
        const string &newArrayNameOrg =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[1])->getObjectName();
        SCIDB_ASSERT(newArrayNameOrg.find('@') == std::string::npos);

        std::string newArrayName;
        std::string newNamespaceName;
        query->getNamespaceArrayNames(newArrayNameOrg, newNamespaceName, newArrayName);
        lock.reset(new SystemCatalog::LockDesc(newArrayName,
                                               query->getQueryID(),
                                               Cluster::getInstance()->getLocalInstanceId(),
                                               SystemCatalog::LockDesc::COORD,
                                               SystemCatalog::LockDesc::XCL));
        resLock = query->requestLock(lock);
        SCIDB_ASSERT(resLock);
        SCIDB_ASSERT(resLock->getLockMode() >= SystemCatalog::LockDesc::XCL);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRename, "rename")


}  // namespace ops
