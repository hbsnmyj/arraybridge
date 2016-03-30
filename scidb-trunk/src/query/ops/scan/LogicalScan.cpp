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
 * LogicalScan.cpp
 *
 *  Created on: Mar 9, 2010
 *      Author: Emad
 */
#include <log4cxx/logger.h>
#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <system/Exceptions.h>
#include <usr_namespace/NamespacesCommunicator.h>
#include <usr_namespace/Permissions.h>

using namespace std;

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.logical_scan"));

/**
 * @brief The operator: scan().
 *
 * @par Synopsis:
 *   scan( srcArray [, ifTrim] )
 *
 * @par Summary:
 *   Produces a result array that is equivalent to a stored array.
 *
 * @par Input:
 *   - srcArray: the array to scan, with srcAttrs and srcDims.
 *   - ifTrim: whether to turn an unbounded array to a bounded array. Default value is false.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims (ifTrim=false), or trimmed srcDims (ifTrim=true).
 *   <br> ]
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
class LogicalScan: public  LogicalOperator
{
public:
    LogicalScan(const std::string& logicalName, const std::string& alias):
                    LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;

        // - With ADD_PARAM_INPUT()
        //   which is a typical way of providing an input array name,
        //   the array name will NOT appear in _parameters.
        // - With ADD_PARAM_IN_ARRAY_NAME2(),
        //   the array name will appear in _parameters.
        //   So the next parameter will be _parameters[1].
        ADD_PARAM_IN_ARRAY_NAME2(PLACEHOLDER_ARRAY_NAME_VERSION|PLACEHOLDER_ARRAY_NAME_INDEX_NAME);
        ADD_PARAM_VARIES()
    }

    std::vector<std::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<std::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() == 1) {
            res.push_back(PARAM_CONSTANT(TID_BOOL));
        }
        return res;
    }

    std::string inferPermissions(std::shared_ptr<Query>& query)
    {
        LOG4CXX_DEBUG(logger, "LogicalScan::inferPermissions() = ReadArray");
        // Ensure we have permissions to read the array in the namespace
        std::string permissions;
        permissions.push_back(scidb::permissions::namespaces::ReadArray);
        return permissions;
    }

    void inferArrayAccess(std::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);

        assert(!_parameters.empty());
        assert(_parameters.front()->getParamType() == PARAM_ARRAY_REF);

        const string& arrayNameOrg =
            ((std::shared_ptr<OperatorParamReference>&)_parameters.front())->getObjectName();
        assert(arrayNameOrg.find('@') == std::string::npos);

        std::string namespaceName;
        std::string arrayName;
        query->getNamespaceArrayNames(arrayNameOrg, namespaceName, arrayName);

        ArrayDesc srcDesc;
        scidb::namespaces::Communicator::getArrayDesc(
            namespaceName, arrayName, SystemCatalog::ANY_VERSION, srcDesc);
        if (srcDesc.isTransient())
        {
            std::shared_ptr<SystemCatalog::LockDesc> lock(
                make_shared<SystemCatalog::LockDesc>(
                    namespaceName,
                    arrayName,
                    query->getQueryID(),
                    Cluster::getInstance()->getLocalInstanceId(),
                    SystemCatalog::LockDesc::COORD,
                    SystemCatalog::LockDesc::XCL));
            std::shared_ptr<SystemCatalog::LockDesc> resLock(query->requestLock(lock));

            SCIDB_ASSERT(resLock);
            SCIDB_ASSERT(resLock->getLockMode() == SystemCatalog::LockDesc::XCL);
        }
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1 || _parameters.size() == 2);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);

        std::shared_ptr<OperatorParamArrayReference>& arrayRef = (std::shared_ptr<OperatorParamArrayReference>&)_parameters[0];
        assert(arrayRef->getArrayName().find('@') == string::npos);
        assert(ArrayDesc::isNameUnversioned(arrayRef->getObjectName()));

        if (arrayRef->getVersion() == ALL_VERSIONS) {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ASTERISK_USAGE2, _parameters[0]->getParsingContext());
        }
        ArrayDesc schema;
        const std::string &arrayNameOrg = arrayRef->getObjectName();
        std::string namespaceName;
        std::string arrayName;
        query->getNamespaceArrayNames(arrayNameOrg, namespaceName, arrayName);

        ArrayID arrayId = query->getCatalogVersion(namespaceName, arrayName);
        scidb::namespaces::Communicator::getArrayDesc(
            namespaceName,
            arrayName,
            arrayId,
            arrayRef->getVersion(),
            schema);

        schema.addAlias(arrayNameOrg);
        schema.setNamespaceName(namespaceName);

        // Trim if the user wishes to.
        if (_parameters.size() == 2 // the user provided a true/false clause
            &&                       // and it's true
            evaluate(
                    ((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                    query,
                    TID_BOOL
                    ).getBool()
            )
        {
            schema.trim();

            // Without this change, harness test other.between_sub2 may fail.
            //
            // Once you trim the schema, the array is not the original array anymore.
            // Some operators, such as concat(), may go to the system catalog to find schema for input arrays if named.
            // We should make sure they do not succeed.
            schema.setName("");
        }

        SCIDB_ASSERT(schema.getDistribution()->getPartitioningSchema() != psUninitialized);
        SCIDB_ASSERT(schema.getDistribution()->getPartitioningSchema() != psUndefined);

        return schema;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalScan, "scan")

} //namespace scidb

