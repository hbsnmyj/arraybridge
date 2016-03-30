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
 * LogicalRemoveVersions.cpp
 *
 *  Created on: Jun 11, 2014
 *      Author: sfridella
 */

#include <query/Operator.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <usr_namespace/Permissions.h>
#include <usr_namespace/NamespacesCommunicator.h>

using namespace std;

namespace scidb {

/**
 * @brief The operator: remove_versions().
 *
 * @par Synopsis:
 *   remove_versions( targetArray, oldestVersionToSave )
 *
 * @par Summary:
 *   Removes all versions of targetArray that are older than
 *   oldestVersionToSave
 *
 * @par Input:
 *   - targetArray: the array which is targeted.
 *   - oldestVersionToSave: the version, prior to which all versions will be removed.
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
class LogicalRemoveVersions: public LogicalOperator
{
public:
    LogicalRemoveVersions(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
	{
            ADD_PARAM_IN_ARRAY_NAME()
            ADD_PARAM_CONSTANT("uint64")
            _properties.exclusive = true;
            _properties.ddl = true;
	}

    std::string inferPermissions(std::shared_ptr<Query>& query)
    {
        // Ensure we have the proper permissions
        std::string permissions;
        permissions.push_back(scidb::permissions::namespaces::DeleteArray);
        return permissions;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 0);
        ArrayDesc arrDesc;
        arrDesc.setDistribution(defaultPartitioning());
        arrDesc.setResidency(query->getDefaultArrayResidency());
        return arrDesc;
    }

    void inferArrayAccess(std::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);
        assert(_parameters.size() == 2);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& arrayNameOrg =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        assert(arrayNameOrg.find('@') == std::string::npos);
        VersionID targetVersion =
            evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                     query,
                     TID_INT64).getInt64();

        if (targetVersion < 1 || targetVersion > SystemCatalog::MAX_VERSIONID) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAY_VERSION_DOESNT_EXIST) << targetVersion;
        }

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrg, namespaceName, arrayName);

        std::shared_ptr<SystemCatalog::LockDesc>  lock(
            new SystemCatalog::LockDesc(
                namespaceName,
                arrayName,
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                SystemCatalog::LockDesc::COORD,
                SystemCatalog::LockDesc::RM));
        lock->setArrayVersion(targetVersion);
        std::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= SystemCatalog::LockDesc::RM);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalRemoveVersions, "remove_versions")


}  // namespace scidb
