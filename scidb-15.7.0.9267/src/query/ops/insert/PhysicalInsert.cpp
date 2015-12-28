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
 * PhysicalInsert.cpp
 *
 *  Created on: Sep 14, 2012
 *      Author: poliocough@gmail.com
 */


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

namespace scidb
{

/**
 * Insert operator.
 */
class PhysicalInsert: public PhysicalUpdate
{
private:
    /**
     * Descriptor of previous version. Not initialized if not applicable.
     */
    ArrayDesc _previousVersionDesc;

    static const string& getArrayName(const Parameters& parameters)
    {
        SCIDB_ASSERT(!parameters.empty());
        return ((std::shared_ptr<OperatorParamReference>&)parameters[0])->getObjectName();
    }

public:
    /**
    * Vanilla. Same as most operators.
    */
    PhysicalInsert(const string& logicalName,
                   const string& physicalName,
                   const Parameters& parameters,
                   const ArrayDesc& schema):
    PhysicalUpdate(logicalName,
                   physicalName,
                   parameters,
                   schema,
                   getArrayName(parameters))
    {}

    /**
    * Find the descriptor for the previous version and populate placeHolder with it.
    * @param[out] placeholder the returned descriptor
    */
    void fillPreviousDesc(ArrayDesc& placeholder) const
    {
        //XXX TODO: avoid these catalog calls by getting the latest version in LogicalInsert
       const string& arrayName = getArrayName(_parameters);
       if(_schema.getId() == _schema.getUAId()) //new version (our version) was not created yet
       {
           SystemCatalog::getInstance()->getArrayDesc(arrayName,
                                                      SystemCatalog::ANY_VERSION,
                                                      LAST_VERSION,
                                                      placeholder, true);
       }
       else //new version was already created; locate the previous
       {
           assert(_schema.getId() > _schema.getUAId());
           VersionID ver = _schema.getVersionId() - 1;
           if (ver == 0)
           {
               return;
           }
           SystemCatalog::getInstance()->getArrayDesc(arrayName, SystemCatalog::ANY_VERSION,
                                                      ver, placeholder, true);
           assert(placeholder.getId() < _schema.getId());
       }
    }

    /**
    * Find the descriptor of the previous version if exists.
    * @return the descriptor of the previous version of the target array, NULL if we are inserting into version 1
    */
    ArrayDesc const* getPreviousDesc()
    {
       if(_previousVersionDesc.getUAId() == 0)
       {
           fillPreviousDesc(_previousVersionDesc);
       }

       if(_previousVersionDesc.getVersionId() == 0)
       {
           return NULL;
       }

       return &_previousVersionDesc;
    }

    /**
     * Get the estimated upper bound of the output array for the optimizer.
     * @param inputBoundaries the boundaries of the input arrays
     * @param inputSchemas the shapes of the input arrays
     * @return inputBoundaries[0] if we're inserting into version 1, else
     *         a union of inputBoundaries[0] with the boundaries of the previous version.
     */
    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        ArrayDesc prevVersionDesc;
        fillPreviousDesc(prevVersionDesc);
        if (prevVersionDesc.getVersionId() == 0)
        {
            return inputBoundaries[0];
        }
        else
        {
            Coordinates currentLo = prevVersionDesc.getLowBoundary();
            Coordinates currentHi = prevVersionDesc.getHighBoundary();
            PhysicalBoundaries currentBoundaries(currentLo, currentHi);
            return currentBoundaries.unionWith(inputBoundaries[0]);
        }
    }

    /**
     * If chunk sizes or overlaps differ, repartition the input array to match the target.
     */
    virtual void requiresRedimensionOrRepartition(
        vector<ArrayDesc> const&   inputSchemas,
        vector<ArrayDesc const*>&  modifiedPtrs) const
    {
        SCIDB_ASSERT(inputSchemas.size() == 1);
        SCIDB_ASSERT(modifiedPtrs.size() == 1);

        // If input matches target array schema, no problem.
        if (_schema.samePartitioning(inputSchemas[0])) {
            modifiedPtrs.clear();
            return;
        }

        // If input was manually repartitioned, we're not allowed to
        // override it, so you're scrod.
        //
        if (modifiedPtrs[0] != NULL) {
            throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_REPART_FORBIDDEN)
                << getLogicalName();
        }

        // Request a repartition to the target array's schema.
        modifiedPtrs[0] = &_schema;
    }

    /**
     * Get the distribution requirement.
     * @return a DistributionRquirement requiring defaultPartitioning()
     */
    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        vector<RedistributeContext> requiredDistribution;
        requiredDistribution.push_back(RedistributeContext(defaultPartitioning()));
        return DistributionRequirement(DistributionRequirement::SpecificAnyOrder, requiredDistribution);
    }

    /**
     * Internal helper: write a cell from sourceIter to outputIter at pos and set flag to true.
     * @param sourceIter a chunk iterator to write from
     * @param outputIter a chunk iterator to write to
     * @param pos the position where to write the element
     * @param flag variable that is set to true after writing
     */
    void writeFrom(std::shared_ptr<ConstChunkIterator>& sourceIter,
                   std::shared_ptr<ChunkIterator>& outputIter,
                   Coordinates const* pos, bool& flag)
    {
        outputIter->setPosition(*pos);
        outputIter->writeItem(sourceIter->getItem());
        flag = true;
    }

    /**
     * Merge previous version chunk with new chunk and insert result into the target chunk.
     * @param query the query context
     * @param materializedInputChunk a materialized chunk from input
     * @param existingChunk an existing chunk from the previous version
     * @param newChunk the newly created blank chunk to be written
     * @param nDims the number of dimensions
     */
    void insertMergeChunk(std::shared_ptr<Query>& query,
                          ConstChunk* materializedInputChunk,
                          ConstChunk const& existingChunk,
                          Chunk& newChunk,
                          size_t nDims)
    {
        std::shared_ptr<ConstChunkIterator> inputCIter =
            materializedInputChunk->getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);
        std::shared_ptr<ConstChunkIterator> existingCIter =
            existingChunk.getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);
        std::shared_ptr<ChunkIterator> outputCIter =
            newChunk.getIterator(query, ChunkIterator::NO_EMPTY_CHECK | ChunkIterator::SEQUENTIAL_WRITE);

        Coordinates const* inputPos = inputCIter->end() ? NULL : &inputCIter->getPosition();
        Coordinates const* existingPos = existingCIter->end() ? NULL : &existingCIter->getPosition();

        while ( inputPos || existingPos )
        {
            bool nextInput = false;
            bool nextExisting = false;
            if (inputPos == NULL)
            {
                writeFrom(existingCIter, outputCIter, existingPos, nextExisting);
            }
            else if (existingPos == NULL)
            {
                writeFrom(inputCIter, outputCIter, inputPos, nextInput);
            }
            else
            {
                int64_t res = coordinatesCompare(*inputPos, *existingPos);
                if ( res < 0 )
                {
                    writeFrom(inputCIter, outputCIter, inputPos, nextInput);
                }
                else if ( res > 0 )
                {
                    writeFrom(existingCIter, outputCIter, existingPos, nextExisting);
                }
                else
                {
                    writeFrom(inputCIter, outputCIter, inputPos, nextInput);
                    nextExisting = true;
                }
            }
            if(inputPos && nextInput)
            {
                ++(*inputCIter);
                inputPos = inputCIter->end() ? NULL : &inputCIter->getPosition();
            }
            if(existingPos && nextExisting)
            {
                ++(*existingCIter);
                existingPos = existingCIter->end() ? NULL : &existingCIter->getPosition();
            }
        }
        outputCIter->flush();
    }

    /**
     * Insert inputArray into a new version based on _schema, update catalog boundaries.
     * @param inputArray the input to insert
     * @param query the query context
     * @param currentLowBound the current lower-bound coordinates of the data in the previous version
     * @param currentHiBound the current hi-bound coordinates of the data in the previous version
     */
    std::shared_ptr<Array> performInsertion(std::shared_ptr<Array>& inputArray,
                                       std::shared_ptr<Query>& query,
                                       Coordinates const& currentLowBound,
                                       Coordinates const& currentHiBound,
                                       size_t const nDims)
    {
        const size_t nAttrs = _schema.getAttributes().size();
        std::shared_ptr<Array> dstArray;

        if (_schema.isTransient())
        {
            dstArray = transient::lookup(_schema,query);

            transient::remove(_schema);

            query->pushFinalizer(boost::bind(&PhysicalUpdate::recordTransient, this,
                                             static_pointer_cast<MemArray>(dstArray),_1));
        }
        else
        {
            dstArray = DBArray::newDBArray(_schema, query);
        }

        SCIDB_ASSERT(dstArray->getArrayDesc().getAttributes(true).size() ==
                     inputArray->getArrayDesc().getAttributes(true).size());
        assert(dstArray->getArrayDesc().getId()   == _schema.getId());
        assert(dstArray->getArrayDesc().getUAId() == _schema.getUAId());

        query->getReplicationContext()->enableInboundQueue(_schema.getId(), dstArray);

        PhysicalBoundaries bounds(currentLowBound, currentHiBound);
        if (inputArray->getArrayDesc().getEmptyBitmapAttribute() == NULL && _schema.getEmptyBitmapAttribute())
        {
            inputArray = make_shared<NonEmptyableArray>(inputArray);
        }

        vector<std::shared_ptr<ConstArrayIterator> > inputIters(nAttrs);    //iterators over the input array
        vector<std::shared_ptr<ConstArrayIterator> > existingIters(nAttrs); //iterators over the data already in the
                                                                       // output array
        vector<std::shared_ptr<ArrayIterator> > outputIters(nAttrs);        //write-iterators into the output array

        for(AttributeID i = 0; i < nAttrs; i++)
        {
            inputIters[i] = inputArray->getConstIterator(i);
            existingIters[i] = dstArray->getConstIterator(i);
            outputIters[i] = dstArray->getIterator(i);
        }

        while(!inputIters[0]->end())
        {
            Coordinates const& pos = inputIters[0]->getPosition();
            if (!_schema.contains(pos))
            {
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES)
                    << CoordsToStr(pos) << _schema.getDimensions();
            }

            bool haveExistingChunk = existingIters[0]->setPosition(pos);
            for(AttributeID i = 0; i < nAttrs; i++)
            {
                if ( haveExistingChunk && i != 0 )
                {
                    existingIters[i]->setPosition(pos);
                }

                ConstChunk const& inputChunk = inputIters[i]->getChunk();
                ConstChunk* matChunk = inputChunk.materialize();
                if(matChunk->count() == 0)
                {
                    break;
                }

                if(haveExistingChunk)
                {
                    insertMergeChunk(query, matChunk, existingIters[i]->getChunk(),
                                     getNewChunk(pos,outputIters[i]),
                                     nDims);
                }
                else
                {
                    outputIters[i]->copyChunk(*matChunk);
                }

                if (i == nAttrs-1)
                {
                    bounds.updateFromChunk(matChunk, _schema.getEmptyBitmapAttribute() == NULL);
                }
            }

            for(AttributeID i = 0; i < nAttrs; i++)
            {
                ++(*inputIters[i]);
            }
        }

        // Update boundaries
        updateSchemaBoundaries(_schema, bounds, query);

        if (!_schema.isTransient())
        {
            query->getReplicationContext()->replicationSync(_schema.getId());
            query->getReplicationContext()->removeInboundQueue(_schema.getId());
            StorageManager::getInstance().flush();
        }

        return dstArray;
    }

    Chunk&
    getNewChunk(const Coordinates& chunkPos,
                const std::shared_ptr<ArrayIterator> & outputIter)
    {
        Chunk* chunk = NULL;
        try {
            chunk = &outputIter->newChunk(chunkPos);
            assert(chunk);
        } catch (const SystemException& err) {
            if (err.getLongErrorCode() != SCIDB_LE_CHUNK_ALREADY_EXISTS ||
                !_schema.isTransient()) {
                throw;
            }
            bool rc = outputIter->setPosition(chunkPos);
            ASSERT_EXCEPTION(rc, "PhysicalInsert::getNewChunk");
            chunk = &outputIter->updateChunk();
            assert(chunk);
        }
        return *chunk;
    }

    /**
     * Runs the insert op.
     * @param inputArrays one-sized list containing the input
     * @param query the query context
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        SCIDB_ASSERT(inputArrays.size() == 1);
        VersionID version = _schema.getVersionId();
        SCIDB_ASSERT(version == ArrayDesc::getVersionFromName (_schema.getName()));
        const string& unvArrayName = getArrayName(_parameters);
        SCIDB_ASSERT(unvArrayName == ArrayDesc::makeUnversionedName(_schema.getName()));

        if (_schema.isTransient())
        {
            inputArrays[0] = make_shared<MemArray>(inputArrays[0],query);
        }

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
        }

        size_t nDims = _schema.getDimensions().size();
        Coordinates currentLo(nDims, CoordinateBounds::getMax());
        Coordinates currentHi(nDims, CoordinateBounds::getMin());

        if (const ArrayDesc* previousDesc = getPreviousDesc())
        {
            currentLo = previousDesc->getLowBoundary();
            currentHi = previousDesc->getHighBoundary();
        }

        std::shared_ptr<Array> dstArray =  performInsertion(inputArrays[0], query, currentLo, currentHi, nDims);

        getInjectedErrorListener().check();
        return dstArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalInsert, "insert", "physicalInsert")

}  // namespace ops
