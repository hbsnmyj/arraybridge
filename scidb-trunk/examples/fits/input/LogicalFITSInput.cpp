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
 * @author miguel@spacebase.org
 *
 * @brief Input operator for loading data from an external FITS file.
 *        The operator syntax is:
 *        FITS_INPUT(<Array>, <File Path>, [ <HDU Number> [ , <Instance ID> ]] )
 */
#include <query/Operator.h>
#include <system/Cluster.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <usr_namespace/NamespacesCommunicator.h>


namespace scidb
{
using namespace std;


class LogicalFITSInput: public LogicalOperator
{
public:
    LogicalFITSInput(const string& logicalName, const string& alias) :
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_IN_ARRAY_NAME();
        ADD_PARAM_CONSTANT("string");
        ADD_PARAM_VARIES();
    }

    vector<std::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const vector< ArrayDesc> &schemas)
    {
        vector<std::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        switch (_parameters.size()) {
            case 2:
                res.push_back(PARAM_CONSTANT("uint32"));
                res.push_back(PARAM_CONSTANT("uint64"));
                break;
            case 3:
                res.push_back(PARAM_CONSTANT("uint64"));
                break;
            case 0:
            case 1:
                assert(false);
        }
        return res;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        if (_parameters.size() == 4) {  // Check for valid instance ID
            InstanceID instanceID = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&) _parameters[3])->getExpression(), query, TID_UINT64).getUint64();
            if (instanceID >= query->getInstancesCount()) {
                throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_INSTANCE_ID, _parameters[3]->getParsingContext()) << instanceID;
            }
        }

        const string& arrayName = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        ArrayDesc arrayDesc;
        std::string namespaceName = scidb::namespaces::Communicator::getNamespaceName(query);
        ArrayID arrayId = query->getCatalogVersion(namespaceName, arrayName);
        scidb::namespaces::Communicator::getArrayDesc(namespaceName, arrayName, arrayId, arrayDesc);

        stringstream ss;
        ss << query->getInstanceID(); // coordinator instance
        ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(psLocalInstance,
                                                                                    DEFAULT_REDUNDANCY,
                                                                                    ss.str());
        arrayDesc.setDistribution(localDist);
        arrayDesc.setResidency(query->getDefaultArrayResidency());

        return arrayDesc;
    }

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalFITSInput, "fits_input");

}
