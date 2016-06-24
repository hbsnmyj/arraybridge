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
 * LogicalScanHDF5.cpp
 *
 *  Created on: June 7 2016
 *      Author: Haoyuan Xing<hbsnmyj@gmail.com>
 */

#include <log4cxx/logger.h>
#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <usr_namespace/NamespacesCommunicator.h>
#include <usr_namespace/Permissions.h>

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.logical_scan_hdf5"));
/**
 * @brief Operator: scan_hdf5()
 */
class LogicalScanHDF5 : public LogicalOperator
{
public:
    LogicalScanHDF5(const std::string& logicalName, const std::string& alias):
            LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_IN_ARRAY_NAME2(PLACEHOLDER_ARRAY_NAME_VERSION | PLACEHOLDER_ARRAY_NAME_INDEX_NAME);
    }

    std::string inferPermissions(std::shared_ptr<Query>& query)
    {
        std::string permissions;
        permissions.push_back(scidb::permissions::namespaces::ReadArray);
        return permissions;
    }

    void inferArrayAccess(std::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);

        const std::string& arrayNameOrg =
                ((std::shared_ptr<OperatorParamReference>&)_parameters.front())->getObjectName();

        std::string namespaceName;
        std::string arrayName;
        query->getNamespaceArrayNames(arrayNameOrg, namespaceName, arrayName);
                ArrayDesc srcDesc;
        scidb::namespaces::Communicator::getArrayDesc(
            namespaceName, arrayName, SystemCatalog::ANY_VERSION, srcDesc);
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc> inputSchemas, std::shared_ptr<Query> query)
    {
        assert(inputSchemas.size() == 0);
        assert(_parameters.size() == 1);
        assert(_parameters[0]->getParamType() == PARAM_ARRAY_REF);

        std::shared_ptr<OperatorParamArrayReference>& arrayRef = (std::shared_ptr<OperatorParamArrayReference>&)_parameters[0];
        assert(arrayRef->getArrayName().find('@') == std::string::npos);
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
        //we set the distribution to psUndefined for the time being
        schema.setDistribution(ArrayDistributionFactory::getInstance()
                                       ->construct(psUndefined, DEFAULT_REDUNDANCY));
        return schema;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalScanHDF5, "scan_hdf5")
}// namespace scidb
