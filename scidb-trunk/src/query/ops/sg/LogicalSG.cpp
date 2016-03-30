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
 * @file LogicalSG.cpp
 *
 * @author roman.simakov@gmail.com
 * @brief This file implement logical operator SCATTER/GATHER
 */

#include <query/Operator.h>
#include <smgr/io/Storage.h>
#include <usr_namespace/NamespacesCommunicator.h>
#include <usr_namespace/Permissions.h>

using namespace std;

namespace scidb
{

/**
 * @brief The operator: sg().
 *
 * @par Synopsis:
 *   sg( srcArray, partitionSchema, instanceId=-1, outputArray="", isStrict=false, offsetVector=null)
 *
 * @par Summary:
 *   SCATTER/GATHER distributes array chunks over the instances of a cluster.
 *   The result array is returned.
 *   It is the only operator that uses the network manager.
 *   Typically this operator is inserted by the optimizer into the physical plan.
 *
 * @par Input:
 *   - srcArray: the source array, with srcAttrs and srcDims.
 *   - partitionSchema:<br>
 *     0 = psReplication, <br>
 *     1 = psHashPartitioned,<br>
 *     2 = psLocalInstance,<br>
 *     3 = psByRow,<br>
 *     4 = psByCol,<br>
 *     5 = psUndefined.<br>
 *   - instanceId:<br>
 *     -2 = to coordinator (same with 0),<br>
 *     -1 = all instances participate,<br>
 *     0..#instances-1 = to a particular instance.<br>
 *     [TO-DO: The usage of instanceId, in calculating which instance a chunk should go to, requires further documentation.]
 *   - outputArray: if not empty, the result will be stored into this array
 *   - isStrict if true, enables the data integrity checks such as for data collisions and out-of-order input chunks, defualt=false. <br>
 *   - offsetVector: a vector of #dimensions values.<br>
 *     To calculate which instance a chunk belongs, the chunkPos is augmented with the offset vector before calculation.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *
 */
class LogicalSG: public LogicalOperator
{
public:
    LogicalSG(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_CONSTANT("uint32");
        ADD_PARAM_VARIES();
    }

    std::vector<std::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<std::shared_ptr<OperatorParamPlaceholder> > res;

        // sanity check: #parameters is at least 1 (i.e. partitionSchema), but no more than #dims+4.
        if (_parameters.size()==0 || _parameters.size() > schemas[0].getDimensions().size() + 4) {
            assert(false);
        }

        // the param is before the offset vector
        else if (_parameters.size() <= 3) {
            res.push_back(END_OF_VARIES_PARAMS());

            switch (_parameters.size()) {
            case 1:
                res.push_back(PARAM_CONSTANT("int64"));
                break;
            case 2:
                res.push_back(PARAM_OUT_ARRAY_NAME());
                break;
            case 3:
                res.push_back(PARAM_CONSTANT("bool"));
                break;
            default:
                ASSERT_EXCEPTION(false, "LogicalSG::nextVaryParamPlaceholder");
                break;
            }
        }

        // the param is in the offset vector
        else if (_parameters.size() < schemas[0].getDimensions().size() + 4) {
            // along with the first value in the offset, we say the vector is optional
            if (_parameters.size()==4) {
                res.push_back(END_OF_VARIES_PARAMS());
            }
            res.push_back(PARAM_CONSTANT("int64"));
        }

        // after the offset vector
        else {
            assert(_parameters.size() == schemas[0].getDimensions().size() + 4);
            res.push_back(END_OF_VARIES_PARAMS());
        }

        return res;
    }

    private:

    std::string getArrayNameForStore() const
    {
        std::string arrayName;
        if (_parameters.size() >= 3)
        {
            arrayName = static_cast<OperatorParamReference*>(_parameters[2].get())->getObjectName();
        }
        return arrayName;
    }

    /// @return  validated partitioning scheme specified by the user
    /// @throws scidb::SystemException if the scheme is unsupported
    /// @todo XXX TODO: change PartitioningSchema to DistributionId
    PartitioningSchema getPartitioningSchema(const std::shared_ptr<Query>& query) const
    {
        ASSERT_EXCEPTION(_parameters[0], "Partitioning schema is not specified by the user");
        OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[0].get());
        const PartitioningSchema ps = static_cast<PartitioningSchema>( evaluate(lExp->getExpression(), query, TID_INT32).getInt32());
        if (! isValidPartitioningSchema(ps, false)  && ps != psLocalInstance)
        {
            // Do not allow optional data associated with the partitioning schema
            throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_REDISTRIBUTE_ERROR);
        }
        return ps;
    }

    /// @return logical instance ID specified by the user, or ALL_INSTANCE_MASK if not specified
    InstanceID getInstanceId(const std::shared_ptr<Query>& query) const
    {
        InstanceID instanceId = ALL_INSTANCE_MASK;
        if (_parameters.size() >=2 )
        {
            OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[1].get());
            instanceId = static_cast<InstanceID>( evaluate(lExp->getExpression(), query, TID_INT64).getInt64());
        }
        instanceId = (instanceId==COORDINATOR_INSTANCE_MASK) ? query->getInstanceID() : instanceId;
        return instanceId;
    }

    /// @return the coordinate offset specified by the user, or empty offset if not specified
    DimensionVector getOffsetVector(const vector<ArrayDesc> & inputSchemas,
                                    const std::shared_ptr<Query>& query) const
    {
        if (_parameters.size() <= 4) {
            return DimensionVector();
        }

        const Dimensions&  dims = inputSchemas[0].getDimensions();
        DimensionVector result(dims.size());
        ASSERT_EXCEPTION(_parameters.size() == dims.size() + 4,
                         "Invalid coordinate offset is specified");

        for (size_t i = 0; i < result.numDimensions(); ++i) {
            OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[i+4].get());
            result[i] = static_cast<Coordinate>( evaluate(lExp->getExpression(), query, TID_INT64).getInt64());
        }
        return result;
    }

    public:

    std::string inferPermissions(std::shared_ptr<Query>& query)
    {
        // Ensure we have permissions to create the array in the namespace
        std::string permissions;
        permissions.push_back(scidb::permissions::namespaces::CreateArray);
        return permissions;
    }

    /**
     * The schema of output array is the same as input
     */
    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 1);
        ArrayDesc const& desc = inputSchemas[0];

        //validate the partitioning schema
        const PartitioningSchema ps = getPartitioningSchema(query) ;
        InstanceID localInstance = getInstanceId(query);
        DimensionVector offset = getOffsetVector(inputSchemas,query);

        // use the query live set because we dont know better
        ArrayResPtr arrRes = query->getDefaultArrayResidency();

        size_t redundancy = DEFAULT_REDUNDANCY;

        // get the name of the supplied result array
        std::string resultArrayName = getArrayNameForStore();
        if (resultArrayName.empty()) {
            resultArrayName = desc.getName();
        } else {
            // notice we are ignoring the catalog information
            // PhysicalUpdate::preSingleExecute() should correct that
            arrRes = query->getDefaultArrayResidencyForWrite();
            redundancy = Config::getInstance()->getOption<size_t>(CONFIG_REDUNDANCY);
        }
        if (isDebug()) {
            if (_parameters.size() >= 4) {
                assert(_parameters[3]->getParamType() == PARAM_LOGICAL_EXPRESSION);
                OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[3].get());
                SCIDB_ASSERT(lExp->isConstant());
                assert(lExp->getExpectedType()==TypeLibrary::getType(TID_BOOL));
            }
        }
        std::string distCtx;
        if (ps == psLocalInstance) {
            ASSERT_EXCEPTION((localInstance < query->getInstancesCount()),
                             "The specified instance is larger than total number of instances");
            stringstream ss;
            ss<<localInstance;
            distCtx = ss.str();
        }
        std::shared_ptr<CoordinateTranslator> translator;
        if (!offset.isEmpty()) {
            translator = OffsetCoordinateTranslator::createOffsetMapper(offset);
        }
        ArrayDistPtr arrDist = ArrayDistributionFactory::getInstance()->construct(ps,
                                                                                  redundancy,
                                                                                  distCtx,
                                                                                  translator,
                                                                                  0);
        return ArrayDesc(resultArrayName,
                         desc.getAttributes(),
                         desc.getDimensions(),
                         arrDist,
                         arrRes);
    }

    void inferArrayAccess(std::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);

        const std::string resultArrayName = getArrayNameForStore();
        if (resultArrayName.empty()) {
            return;
        }
        SCIDB_ASSERT(ArrayDesc::isNameUnversioned(resultArrayName));

        ArrayDesc srcDesc;
        SCIDB_ASSERT(!srcDesc.isTransient());
        const bool dontThrow(false);

        SystemCatalog::getInstance()->getArrayDesc(resultArrayName, SystemCatalog::ANY_VERSION, srcDesc, dontThrow);

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(resultArrayName, namespaceName, arrayName);

        const SystemCatalog::LockDesc::LockMode lockMode =
            srcDesc.isTransient() ? SystemCatalog::LockDesc::XCL : SystemCatalog::LockDesc::WR;

        std::shared_ptr<SystemCatalog::LockDesc>  lock(make_shared<SystemCatalog::LockDesc>(namespaceName, arrayName,
                                                                                       query->getQueryID(),
                                                                                       Cluster::getInstance()->getLocalInstanceId(),
                                                                                       SystemCatalog::LockDesc::COORD,
                                                                                       lockMode));
        std::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        SCIDB_ASSERT(resLock);
        SCIDB_ASSERT(resLock->getLockMode() >= SystemCatalog::LockDesc::WR);
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSG, "_sg")

} //namespace
