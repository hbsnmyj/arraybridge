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
 * PhysicalStore.cpp
 *
 *  Created on: Apr 16, 2010
 *      Author: Knizhnik
 */

#include <boost/foreach.hpp>

#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "query/TypeSystem.h"
#include "array/Metadata.h"
#include "array/Array.h"
#include "array/DBArray.h"
#include "array/TransientCache.h"
#include "system/SystemCatalog.h"
#include "network/NetworkManager.h"
#include "smgr/io/Storage.h"
#include "query/Statistics.h"

#include "array/ParallelAccumulatorArray.h"
#include "array/DelegateArray.h"
#include "system/Config.h"
#include "system/SciDBConfigOptions.h"

using namespace std;
using namespace boost;

namespace scidb {

class PhysicalStore: public PhysicalUpdate
{
  private:

    static const string& getArrayName(const Parameters& parameters)
    {
        SCIDB_ASSERT(!parameters.empty());
        return ((std::shared_ptr<OperatorParamReference>&)parameters[0])->getObjectName();
    }

  public:
   PhysicalStore(const string& logicalName,
                 const string& physicalName,
                 const Parameters& parameters,
                 const ArrayDesc& schema):
        PhysicalUpdate(logicalName,
                       physicalName,
                       parameters,
                       schema,
                       getArrayName(parameters))
   {}

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc>         & inputSchemas) const
    {
        return inputBoundaries.front();
    }

    /**
     * If chunk sizes or overlaps differ, repartition the input array to match the target.
     */
    virtual void requiresRedimensionOrRepartition(
        vector<ArrayDesc> const& inputSchemas,
        vector<ArrayDesc const*>& modifiedPtrs) const
    {
        SCIDB_ASSERT(inputSchemas.size() == 1);
        SCIDB_ASSERT(modifiedPtrs.size() == 1);

        // If input matches target array schema, no problem.
        if (inputSchemas[0].samePartitioning(_schema)) {
            modifiedPtrs.clear();
            return;
        }

        // If input was manually repartitioned, we're not allowed to override
        // it, so you're scrod.
        //
        if (modifiedPtrs[0] != NULL) {
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_REPART_FORBIDDEN)
                << getLogicalName();
        }

        // Request a repartition to the target array's schema.
        modifiedPtrs[0] = &_schema;
    }

    virtual DistributionRequirement getDistributionRequirement(const std::vector< ArrayDesc> & inputSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder,
                                       vector<RedistributeContext>(1,RedistributeContext(defaultPartitioning())));
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        SCIDB_ASSERT(inputArrays.size() == 1);
        VersionID version = _schema.getVersionId();
        SCIDB_ASSERT(version == ArrayDesc::getVersionFromName (_schema.getName()));
        const string& unvArrayName = getArrayName(_parameters);
        SCIDB_ASSERT(unvArrayName == ArrayDesc::makeUnversionedName(_schema.getName()));

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
               throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK) << _lock->toString();
           }
           SCIDB_ASSERT(_lock->getLockMode() == lockMode);
        }

        if (_schema.isTransient())                       // Storing to transient?
        {
            SCIDB_ASSERT(_lock->getLockMode() == SystemCatalog::LockDesc::XCL);

            MemArrayPtr        outArray(make_shared<MemArray>(_schema,query)); // materialized copy
            PhysicalBoundaries bounds(PhysicalBoundaries::createEmpty(_schema.getDimensions().size()));

         /* Pick the best append mode that the source array will support...*/

            bool vertical = inputArrays[0]->getSupportedAccess() >= Array::MULTI_PASS;

            outArray->append(inputArrays[0],vertical);          // ...materialize it

         /* Run back over the chunks one more time to compute the physical bounds
            of the array...*/

            for (std::shared_ptr<ConstArrayIterator> i(outArray->getConstIterator(0)); !i->end(); ++(*i))
            {
                bounds.updateFromChunk(&i->getChunk());       // ...update bounds
            }

            updateSchemaBoundaries(_schema, bounds, query);
            query->pushFinalizer(boost::bind(&PhysicalUpdate::recordTransient, this, outArray,_1));
            getInjectedErrorListener().check();          // ...for error injection
            return outArray;                             // ...return the copy
        }

        std::shared_ptr<Array>  srcArray    (inputArrays[0]);
        ArrayDesc const&   srcArrayDesc(srcArray->getArrayDesc());
        std::shared_ptr<Array>  dstArray    (DBArray::newDBArray(_schema, query));
        ArrayDesc const&   dstArrayDesc(dstArray->getArrayDesc());
        SCIDB_ASSERT(dstArrayDesc == _schema);

        query->getReplicationContext()->enableInboundQueue(dstArrayDesc.getId(), dstArray);

        const size_t nAttrs = dstArrayDesc.getAttributes().size();

        if (nAttrs == 0)
        {
            return dstArray;
        }

        if (nAttrs > srcArrayDesc.getAttributes().size())
        {
            assert(nAttrs == srcArrayDesc.getAttributes().size()+1);
            srcArray = std::shared_ptr<Array>(make_shared<NonEmptyableArray>(srcArray));
        }

        // Perform parallel evaluation of aggregate
        std::shared_ptr<JobQueue> queue = PhysicalOperator::getGlobalQueueForOperators();
        size_t nJobs = false // until we have a trully multi-threaded storage manager and
                             // StoreJob that yields its thread, store() is single-threaded
            ? Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE)
            : 1;

        vector< std::shared_ptr<StoreJob> > jobs(nJobs);
        Dimensions const& dims = dstArrayDesc.getDimensions();
        const size_t nDims = dims.size();
        for (size_t i = 0; i < nJobs; i++) {
            jobs[i] = make_shared<StoreJob>(i, nJobs, dstArray, srcArray, nDims, nAttrs, query);
        }
        for (size_t i = 1; i < nJobs; i++) {
            queue->pushJob(jobs[i]);
        }

        jobs[0]->execute();

        PhysicalBoundaries bounds = PhysicalBoundaries::createEmpty(nDims);
        int errorJob = -1;
        for (size_t i = 0; i < nJobs; i++) {
            if (!jobs[i]->wait()) {
                errorJob = i;
            }
            else {
                bounds = bounds.unionWith(jobs[i]->bounds);
            }
        }
        if (errorJob >= 0) {
            jobs[errorJob]->rethrow();
        }

        //Destination array is mutable: collect the coordinates of all chunks created by all jobs
        set<Coordinates, CoordinatesLess> createdChunks;
        for(size_t i =0; i < nJobs; i++)
        {
            createdChunks.insert(jobs[i]->getCreatedChunks().begin(), jobs[i]->getCreatedChunks().end());
        }

        //Insert tombstone entries
        StorageManager::getInstance().removeDeadChunks(dstArrayDesc, createdChunks, query);

        // Update boundaries
        updateSchemaBoundaries(_schema, bounds, query);

        query->getReplicationContext()->replicationSync(dstArrayDesc.getId());
        query->getReplicationContext()->removeInboundQueue(dstArrayDesc.getId());

        StorageManager::getInstance().flush();
        getInjectedErrorListener().check();
        return dstArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalStore, "store", "physicalStore")

}  // namespace ops
