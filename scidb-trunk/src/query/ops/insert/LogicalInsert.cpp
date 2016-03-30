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
 * LogicalInsert.cpp
 *
 *  Created on: Aug 30, 2012
 *      Author: poliocough@gmail.com
 */

#include <boost/foreach.hpp>
#include <map>

#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <system/Exceptions.h>
#include <smgr/io/Storage.h>
#include <usr_namespace/NamespacesCommunicator.h>
#include <usr_namespace/Permissions.h>

using namespace std;
using namespace boost;

namespace scidb
{

/**
 * @brief The operator: insert().
 *
 * @par Synopsis:
 *   insert( sourceArray, targetArrayName )
 *
 * @par Summary:
 *   Inserts all data from left array into the persistent
 *   targetArray.  targetArray must exist with matching dimensions and
 *   attributes.  targetArray must also be mutable. The operator shall
 *   create a new version of targetArray that contains all data of the
 *   array that would have been received by merge(sourceArray,
 *   targetArrayName). In other words, new data is inserted between
 *   old data and overwrites any overlapping old values.  The
 *   resulting array is then returned.
 *
 * @par Input:
 *   - sourceArray the array or query that provides inserted data
 *   - targetArrayName: the name of the persistent array inserted into
 *
 * @par Output array:
 *   - the result of insertion
 *   - same schema as targetArray
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   Some might wonder - if this returns the same result as
 *   merge(sourceArray, targetArrayName), then why not use
 *   store(merge())? The answer is that
 *   1. this runs a lot faster - it does not perform a full scan of targetArray
 *   2. this also generates less chunk headers
 */
class LogicalInsert: public  LogicalOperator
{
public:

    /**
     * Default conforming to the operator factory mechanism
     * @param[in] logicalName "insert"
     * @param[in] alias not used by this operator
     */
    LogicalInsert(const string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        ADD_PARAM_INPUT()
        ADD_PARAM_OUT_ARRAY_NAME()
    }

    std::string inferPermissions(std::shared_ptr<Query>& query)
    {
        // Ensure we have permissions to update the array in the namespace
        std::string permissions;
        permissions.push_back(scidb::permissions::namespaces::ReadArray);
        permissions.push_back(scidb::permissions::namespaces::UpdateArray);
        return permissions;
    }

    /**
     * Request a lock for all arrays that will be accessed by this operator.
     * Calls requestLock with the write lock over the target array (array inserted into)
     * @param query the query context
     */
    void inferArrayAccess(std::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);
        SCIDB_ASSERT(_parameters.size() > 0);
        SCIDB_ASSERT(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& arrayNameOrg = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        SCIDB_ASSERT(ArrayDesc::isNameUnversioned(arrayNameOrg));

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrg, namespaceName, arrayName);

        ArrayDesc srcDesc;
        SCIDB_ASSERT(!srcDesc.isTransient());
        scidb::namespaces::Communicator::getArrayDesc(
            namespaceName, arrayName, SystemCatalog::ANY_VERSION, srcDesc);

        const SystemCatalog::LockDesc::LockMode lockMode =
            srcDesc.isTransient() ? SystemCatalog::LockDesc::XCL : SystemCatalog::LockDesc::WR;

        std::shared_ptr<SystemCatalog::LockDesc>  lock(
            make_shared<SystemCatalog::LockDesc>(
                namespaceName,
                arrayName,
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                SystemCatalog::LockDesc::COORD,
                lockMode));
        std::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        SCIDB_ASSERT(resLock);
        SCIDB_ASSERT(resLock->getLockMode() >= SystemCatalog::LockDesc::WR);
    }

    /**
     * Perform operator-specific checks of input and return the shape of the output. Currently,
     * the output array must exist.
     * @param schemas the shapes of the input arrays
     * @param query the query context
     */
    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        SCIDB_ASSERT(schemas.size() == 1);
        SCIDB_ASSERT(_parameters.size() == 1);

        string arrayNameOrg =
			((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        SCIDB_ASSERT(ArrayDesc::isNameUnversioned(arrayNameOrg));

        //Ensure attributes names uniqueness.

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrg, namespaceName, arrayName);

        ArrayDesc dstDesc;
        ArrayDesc const& srcDesc = schemas[0];
        ArrayID arrayId = query->getCatalogVersion(namespaceName, arrayName);
        bool fArrayDesc = scidb::namespaces::Communicator::getArrayDesc(
            namespaceName, arrayName, arrayId, dstDesc, false);
        if (!fArrayDesc) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAY_DOESNT_EXIST) << arrayName;
        }
        ArrayDesc::checkConformity(srcDesc, dstDesc,
                                   ArrayDesc::IGNORE_PSCHEME |
                                   ArrayDesc::IGNORE_OVERLAP |
                                   ArrayDesc::IGNORE_INTERVAL); // allows auto-repart()

        SCIDB_ASSERT(dstDesc.getId() == dstDesc.getUAId());
        SCIDB_ASSERT(dstDesc.getName() == arrayName);
        SCIDB_ASSERT(dstDesc.getUAId() > 0);
        return dstDesc;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalInsert, "insert")

}  // namespace scidb
