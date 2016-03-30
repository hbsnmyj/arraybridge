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
 * @file PhysicalResult.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief This file implements physical SCATTER/GATHER operator
 */

#include <memory>
#include <log4cxx/logger.h>

#include <array/DBArray.h>
#include <array/DelegateArray.h>
#include <array/TransientCache.h>
#include <query/Operator.h>
#include <query/QueryProcessor.h>
#include <query/QueryPlan.h>
#include <network/NetworkManager.h>
#include <network/BaseConnection.h>
#include <network/MessageUtils.h>
#include <system/SystemCatalog.h>
#include <smgr/io/Storage.h>

using namespace boost;
using namespace std;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.sg"));

/**
 * Physical implementation of SCATTER/GATHER operator.
 * This physical operator must be inserted into physical plan by optimizer
 * without any logical instance in logical plan.
 */
class PhysicalSG: public PhysicalUpdate
{
  private:

    static string getArrayName(const Parameters& parameters)
    {
        return PhysicalQueryPlanNode::getSgArrayName(parameters);
    }

    std::string getArrayNameForStore() const
    {
        std::string arrayName;
        std::string unvArrayName;
        if (!getArrayName(_parameters).empty()) {
            arrayName = _schema.getQualifiedArrayName();
            if(!arrayName.empty()) {
                unvArrayName = ArrayDesc::makeUnversionedName(arrayName);
            }
        }

        std::string paramArrayName = getArrayName(_parameters);
        bool equal =
            (paramArrayName == unvArrayName) ||
            (paramArrayName == ArrayDesc::getUnqualifiedArrayName(unvArrayName));

        SCIDB_ASSERT(arrayName.empty() || equal);

        return arrayName;
    }

    DimensionVector getOffsetVector(const vector<ArrayDesc> & inputSchemas) const
    {
        if (_parameters.size() <= 4)
        {
            return DimensionVector();
        }
        else
        {
            DimensionVector result(_schema.getDimensions().size());
            assert (_parameters.size() == _schema.getDimensions().size() + 4);
            for (size_t i = 0; i < result.numDimensions(); i++)
            {
                result[i] = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i+4])->getExpression()->evaluate().getInt64();
            }
            return result;
        }
    }

  public:

    PhysicalSG(const string& logicalName,
               const string& physicalName,
               const Parameters& parameters,
               const ArrayDesc& schema):
    PhysicalUpdate(logicalName,
                   physicalName,
                   parameters,
                   schema,
                   getArrayName(parameters))
    {
    }

    /// @return partitioning scheme specified as a parameter
    PartitioningSchema getPartitioningSchema() const
    {
        // NOTE:
        // The operator parameters at this point are not used to determine the behavior of SG.
        // _schema does; it should be correctly populated either by the optimizer or by LogicalSG.
        // Here, we just check _schema is consistent with the parameters.
        SCIDB_ASSERT(_parameters[0]);
        OperatorParamPhysicalExpression* pExp = static_cast<OperatorParamPhysicalExpression*>(_parameters[0].get());
        PartitioningSchema ps = static_cast<PartitioningSchema>(pExp->getExpression()->evaluate().getInt32());
        SCIDB_ASSERT(ps == _schema.getDistribution()->getPartitioningSchema());
        return ps;
    }

    /// @return logical instance ID specified by the user, or ALL_INSTANCE_MASK if not specified
    InstanceID getInstanceId(const std::shared_ptr<Query>& query) const
    {
        InstanceID instanceId = ALL_INSTANCE_MASK;
        if (_parameters.size() >=2 )
        {
            OperatorParamPhysicalExpression* pExp = static_cast<OperatorParamPhysicalExpression*>(_parameters[1].get());
            instanceId = static_cast<InstanceID>(pExp->getExpression()->evaluate().getInt64());
        }
        if (instanceId == COORDINATOR_INSTANCE_MASK) {
            instanceId = (query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID());
        }
        return instanceId;
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        if (getArrayName(_parameters).empty()) {
            return;
        }
        SCIDB_ASSERT(_schema.getDistribution()->getPartitioningSchema() ==
                     getPartitioningSchema());
        PhysicalUpdate::preSingleExecute(query);
    }

    bool outputFullChunks(std::vector< ArrayDesc> const&) const
    {
        return true;
    }

    RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                              const std::vector< ArrayDesc> & inputSchemas) const
    {
        if (isDebug()) {
            // Verify that _schema & _parameters are in sync
            ArrayDistPtr arrDist = _schema.getDistribution();
            SCIDB_ASSERT(arrDist);
            SCIDB_ASSERT(arrDist->getPartitioningSchema()!=psUninitialized);
            SCIDB_ASSERT(arrDist->getPartitioningSchema()!=psUndefined);
            std::shared_ptr<Query> query(_query);
            SCIDB_ASSERT(query);

            PartitioningSchema ps = getPartitioningSchema();
            SCIDB_ASSERT(arrDist->getPartitioningSchema() == ps);

            SCIDB_ASSERT(ps != psLocalInstance ||
                         getInstanceId(query) == InstanceID(atol(arrDist->getContext().c_str())));

            DimensionVector offset = getOffsetVector(inputSchemas);
            Coordinates schemaOffset;
            InstanceID  schemaShift;
            ArrayDistributionFactory::getTranslationInfo(_schema.getDistribution().get(),
                                                         schemaOffset,
                                                         schemaShift);
            SCIDB_ASSERT(offset == DimensionVector(schemaOffset));
        }
        RedistributeContext distro(_schema.getDistribution(),
                                   _schema.getResidency());
        LOG4CXX_TRACE(logger, "sg() output distro: "<< distro);
        return distro;
    }

    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                           const std::vector<ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        executionPreamble(inputArrays[0], query);

        ArrayDistPtr arrDist = _schema.getDistribution();
        SCIDB_ASSERT(arrDist);

        std::shared_ptr<Array> srcArray = inputArrays[0];
        std::string arrayName = getArrayNameForStore();

        bool enforceDataIntegrity=false;
        if (_parameters.size() >= 4)
        {
            assert(_parameters[3]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            OperatorParamPhysicalExpression* paramExpr = static_cast<OperatorParamPhysicalExpression*>(_parameters[3].get());
            enforceDataIntegrity = paramExpr->getExpression()->evaluate().getBool();
            assert(paramExpr->isConstant());
        }

        const bool storeResult = !arrayName.empty();

        if (!storeResult) {
            LOG4CXX_TRACE(logger, "sg() redistributing into distro: "<<
                          RedistributeContext(arrDist, query->getDefaultArrayResidency()));

            std::shared_ptr<Array> outputArray =
            redistributeToRandomAccess(srcArray,
                                       arrDist,
                                       _schema.getResidency(),
                                       query,
                                       enforceDataIntegrity);

            LOG4CXX_TRACE(logger, "sg() output array distro: "<<
                          RedistributeContext(outputArray->getArrayDesc().getDistribution(),
                                              outputArray->getArrayDesc().getResidency()));

            SCIDB_ASSERT(_schema.getResidency()->isEqual(outputArray->getArrayDesc().getResidency()));
            SCIDB_ASSERT(_schema.getDistribution()->checkCompatibility(outputArray->getArrayDesc().getDistribution()));
            inputArrays[0].reset(); // hopefully, drop the input array
            return outputArray;
        }

        // storing directly into a DB array

        SCIDB_ASSERT(!arrayName.empty());

        VersionID version = _schema.getVersionId();
        SCIDB_ASSERT(version == ArrayDesc::getVersionFromName(arrayName));

        arrayName  = ArrayDesc::makeUnversionedName(arrayName);
        const string & paramUnvArrayName = getArrayName(_parameters);
        bool equal =
            (paramUnvArrayName == arrayName) ||
            (paramUnvArrayName == ArrayDesc::getUnqualifiedArrayName(arrayName));
        SCIDB_ASSERT(equal);
        arrayName = ArrayDesc::getUnqualifiedArrayName(arrayName);

        if (!_lock)
        {
            SCIDB_ASSERT(!query->isCoordinator());
            const SystemCatalog::LockDesc::LockMode lockMode =
                _schema.isTransient() ? SystemCatalog::LockDesc::XCL : SystemCatalog::LockDesc::WR;

            _lock = std::shared_ptr<SystemCatalog::LockDesc>(
                make_shared<SystemCatalog::LockDesc>(
                    _schema.getNamespaceName(),
                    arrayName,
                    query->getQueryID(),
                    Cluster::getInstance()->getLocalInstanceId(),
                    SystemCatalog::LockDesc::WORKER,
                    lockMode));
            if (lockMode == SystemCatalog::LockDesc::WR) {
                SCIDB_ASSERT(!_schema.isTransient());
                _lock->setArrayVersion(version);
                std::shared_ptr<Query::ErrorHandler> ptr(make_shared<UpdateErrorHandler>(_lock));
                query->pushErrorHandler(ptr);
            }

           Query::Finalizer f = bind(&UpdateErrorHandler::releaseLock,_lock,_1);
           query->pushFinalizer(f);
           SystemCatalog::ErrorChecker errorChecker(bind(&Query::validate, query));
           if (!SystemCatalog::getInstance()->lockArray(_lock, errorChecker)) {
               throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK)<< _lock->toString();
           }
           SCIDB_ASSERT(_lock->getLockMode() == lockMode);
        }

        if (srcArray->getArrayDesc().getAttributes().size() != _schema.getAttributes().size())
        {
            srcArray = std::shared_ptr<Array>(new NonEmptyableArray(srcArray));
        }

        std::shared_ptr<Array> outputArray;
        {
            // New SG behavior introduced after 14.12.
            // it needs to be complimented by data collision checks, see #4332
            ArrayID outputArrayId(INVALID_ARRAY_ID);

            if (_schema.isTransient()) {
                SCIDB_ASSERT(_lock->getLockMode() == SystemCatalog::LockDesc::XCL);
                outputArray.reset(new MemArray(_schema,query));
                outputArrayId = _schema.getUAId();
                query->pushFinalizer(boost::bind(&PhysicalUpdate::recordTransient, this, outputArray,_1));
            } else  {
                // open persistent array
                outputArray = DBArray::newDBArray(_schema, query);
                outputArrayId = outputArray->getHandle();
                assert(outputArrayId >0);
                query->getReplicationContext()->enableInboundQueue(outputArrayId, outputArray);
            }

            // redistribute:
            SCIDB_ASSERT(_schema.getDistribution());
            SCIDB_ASSERT(_schema.getResidency());

            SCIDB_ASSERT(_schema.getDistribution()->checkCompatibility(outputArray->getArrayDesc().getDistribution()));
            SCIDB_ASSERT(_schema.getResidency()->isEqual(outputArray->getArrayDesc().getResidency()));

            set<Coordinates, CoordinatesLess> newChunkCoordinates;
            redistributeToArray(srcArray,
                                outputArray,
                                &newChunkCoordinates,
                                query,
                                enforceDataIntegrity);

            if (!_schema.isTransient()) {
                // insert tombstones:
                StorageManager::getInstance().removeDeadChunks(outputArray->getArrayDesc(), newChunkCoordinates, query);
                // stop replication
                query->getReplicationContext()->replicationSync(outputArrayId);
                query->getReplicationContext()->removeInboundQueue(outputArrayId);
                // commit to disk
                StorageManager::getInstance().flush();
            }

            PhysicalBoundaries bounds = PhysicalBoundaries::createFromChunkList(outputArray, newChunkCoordinates);

            // Update boundaries
            updateSchemaBoundaries(_schema, bounds, query);
        }
        getInjectedErrorListener().check();
        LOG4CXX_TRACE(logger, "sg() output array distro: "<<
                      RedistributeContext(outputArray->getArrayDesc().getDistribution(),
                                          outputArray->getArrayDesc().getResidency()));
        return outputArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSG, "_sg", "impl_sg")

} //namespace
