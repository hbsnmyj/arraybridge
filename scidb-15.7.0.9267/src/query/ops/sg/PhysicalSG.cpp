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

// Logger for network subsystem. static to prevent visibility of variable outside of file
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
        if (!getArrayName(_parameters).empty()) {
            arrayName = _schema.getName();
        }
        SCIDB_ASSERT(arrayName.empty() ||
                     ArrayDesc::makeUnversionedName(arrayName) == getArrayName(_parameters));

        return arrayName;
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


    PartitioningSchema getPartitioningSchema() const
    {
        assert(_parameters[0]);
        OperatorParamPhysicalExpression* pExp = static_cast<OperatorParamPhysicalExpression*>(_parameters[0].get());
        PartitioningSchema ps = static_cast<PartitioningSchema>(pExp->getExpression()->evaluate().getInt32());
        SCIDB_ASSERT(ps==_schema.getPartitioningSchema());
        return ps;
    }

    InstanceID getInstanceId() const
    {
        InstanceID instanceId = ALL_INSTANCE_MASK;
        if (_parameters.size() >=2 )
        {
            OperatorParamPhysicalExpression* pExp = static_cast<OperatorParamPhysicalExpression*>(_parameters[1].get());
            instanceId = static_cast<InstanceID>(pExp->getExpression()->evaluate().getInt64());
        }
        return instanceId;
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        if (getArrayName(_parameters).empty()) {
            return;
        }
        _schema.setPartitioningSchema(getPartitioningSchema());
        PhysicalUpdate::preSingleExecute(query);
    }

    bool outputFullChunks(std::vector< ArrayDesc> const&) const
    {
        return true;
    }

    RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                                 const std::vector< ArrayDesc> & inputSchemas) const
    {
        PartitioningSchema ps = getPartitioningSchema();

        DimensionVector offset = getOffsetVector(inputSchemas);

        std::shared_ptr<CoordinateTranslator> distMapper;

        if ( !offset.isEmpty() )
        {
            distMapper = CoordinateTranslator::createOffsetMapper(offset);
        }

        return RedistributeContext(ps,distMapper);
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

    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        const PartitioningSchema ps = getPartitioningSchema();
        const InstanceID instanceID = getInstanceId();
        DimensionVector offsetVector = getOffsetVector(vector<ArrayDesc>());
        std::shared_ptr<Array> srcArray = inputArrays[0];
        std::shared_ptr <CoordinateTranslator> distMapper;
        if (!offsetVector.isEmpty())
        {
            distMapper = CoordinateTranslator::createOffsetMapper(offsetVector);
        }
        const std::string arrayName = getArrayNameForStore();

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
            // XXX TODO: the returned array descriptor will have
            // XXX TODO: the incorrect PartitioningSchema (that of the input)
            return redistributeToRandomAccess(srcArray, query, ps,
                                              instanceID, distMapper, 0,
                                              std::shared_ptr<PartitioningSchemaData>(),
                                              enforceDataIntegrity);
        }

        // storing directly into a DB array

        assert(!arrayName.empty());

        VersionID version = _schema.getVersionId();
        SCIDB_ASSERT(version == ArrayDesc::getVersionFromName (arrayName));
        const string& unvArrayName = getArrayName(_parameters);
        SCIDB_ASSERT(unvArrayName == ArrayDesc::makeUnversionedName(arrayName));

        if (!_lock)
        {
            SCIDB_ASSERT(!query->isCoordinator());
            const SystemCatalog::LockDesc::LockMode lockMode =
                _schema.isTransient() ? SystemCatalog::LockDesc::XCL : SystemCatalog::LockDesc::WR;

            _lock = std::shared_ptr<SystemCatalog::LockDesc>(make_shared<SystemCatalog::LockDesc>(
                                                           unvArrayName,
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
            set<Coordinates, CoordinatesLess> newChunkCoordinates;
            redistributeToArray(srcArray, outputArray,  &newChunkCoordinates,
                                query, ps, instanceID, distMapper, 0,
                                std::shared_ptr<PartitioningSchemaData>(),
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

        return outputArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSG, "_sg", "impl_sg")

} //namespace
