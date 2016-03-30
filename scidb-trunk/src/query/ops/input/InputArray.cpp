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
 * InputArray.cpp
 *
 *  Created on: Sep 23, 2010
 */
#include "ChunkLoader.h"

#include <boost/format.hpp>
#include <boost/foreach.hpp>
#include <memory>

#include <sstream>

#include "InputArray.h"

#include "system/SystemCatalog.h"
#include "system/Utils.h"
#include "util/StringUtil.h"
#include "array/DBArray.h"

namespace scidb
{
    using namespace std;

    log4cxx::LoggerPtr InputArray::s_logger(log4cxx::Logger::getLogger("scidb.qproc.ops.inputarray"));
    static log4cxx::LoggerPtr& logger(InputArray::s_logger);

    void InputArray::resetShadowChunkIterators()
    {
        for (size_t i = 0, n = shadowChunkIterators.size(); i < n; i++) {
            shadowChunkIterators[i]->flush();
        }
        shadowChunkIterators.clear();
    }

    ArrayDesc InputArray::generateShadowArraySchema(ArrayDesc const& targetArray,
                                                    std::string const& shadowArrayName,
                                                    std::shared_ptr<Query> const& query)
    {
        Attributes const& srcAttrs = targetArray.getAttributes(true);
        size_t nAttrs = srcAttrs.size();
        Attributes dstAttrs(nAttrs+2);
        for (size_t i = 0; i < nAttrs; i++) {
            dstAttrs[i] = AttributeDesc(
                safe_static_cast<AttributeID>(i),
                srcAttrs[i].getName(),
                TID_STRING,  AttributeDesc::IS_NULLABLE, 0);
        }
        // FWIW row_offset is a misnomer.  According to the 14.8 docs and the implementation,
        // this value is really a file offset into the flat load file.
        dstAttrs[nAttrs] = AttributeDesc(
            safe_static_cast<AttributeID>(nAttrs), "row_offset", TID_INT64, 0, 0);
        dstAttrs[nAttrs+1] = AttributeDesc(
            safe_static_cast<AttributeID>(nAttrs+1), DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
            TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0);

        //Lets store shadow arrays in defaultPartitioning()
        //TODO: revisit this when we allow users to store arrays with specified distributions

        return ArrayDesc(shadowArrayName,
                         dstAttrs,
                         targetArray.getDimensions(),
                         defaultPartitioning(),
                         query->getDefaultArrayResidencyForWrite() );
    }

InputArray::InputArray(ArrayDesc const& array,
                       string const& format,
                       std::shared_ptr<Query>& query,
                       bool emptyMode,
                       bool enforceDataIntegrity,
                       int64_t maxCnvErrors,
                       const ArrayDesc& shadowArraySchema,
                       bool parallel)
:     SinglePassArray(array),
      _chunkLoader(ChunkLoader::create(format)),
      _currChunkIndex(0),
      strVal(TypeLibrary::getType(TID_STRING)),
      emptyTagAttrID(array.getEmptyBitmapAttribute() != NULL
                     ? array.getEmptyBitmapAttribute()->getId()
                     : INVALID_ATTRIBUTE_ID),
      nLoadedCells(0),
      nLoadedChunks(0),
      nErrors(0),
      maxErrors(maxCnvErrors),
      state(emptyMode ? S_Empty : S_Normal),
      nAttrs(array.getAttributes(true).size()),
      parallelLoad(parallel),
      _enforceDataIntegrity(enforceDataIntegrity)
    {
        SCIDB_ASSERT(query);
        _query=query;
        myInstanceID = query->getInstanceID();

        SCIDB_ASSERT(_chunkLoader);   // else inferSchema() messed up
        _chunkLoader->bind(this, query);

        // only after the bind() call the schema can be adjusted
        // (to match PhysicalInput::getOutputDistribution())
        // because some chunk loaders use the distribution specified by the target array
        // See LogicalInput
        if (getArrayDesc().getDistribution()->getPartitioningSchema() != psLocalInstance) {
            desc.setDistribution(createDistribution(psUndefined));
        }

        if (!shadowArraySchema.getName().empty()) {
            assert(shadowArraySchema.getId() > 0);
            assert(shadowArraySchema.getUAId() > 0);
            assert(shadowArraySchema.getId() > shadowArraySchema.getUAId());
            _shadowArray.reset(new MemArray(shadowArraySchema, query));
        }
    }

    bool InputArray::isSupportedFormat(string const& format)
    {
        std::unique_ptr<ChunkLoader> cLoader(ChunkLoader::create(format));
        return cLoader.get() != 0;
    }

    void InputArray::openFile(std::string const& fileName)
    {
        SCIDB_ASSERT(_chunkLoader);
        SCIDB_ASSERT(state != S_Empty); // Don't tell empty-mode InputArrays to be opening stuff!

        int rc = _chunkLoader->openFile(fileName);
        if (rc != 0) {
            LOG4CXX_WARN(logger, "Failed to open file " << fileName <<
                         " for input: " << ::strerror(rc) << " (" << rc << ')');
            state = S_Empty;
            if (!parallelLoad) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CANT_OPEN_FILE)
                    << fileName << ::strerror(rc) << rc;
            }
        }
    }

    void InputArray::openString(std::string const& dataString)
    {
        SCIDB_ASSERT(_chunkLoader);
        _chunkLoader->openString(dataString);
    }

    void InputArray::redistributeShadowArray(std::shared_ptr<Query>& query)
    {
        SCIDB_ASSERT(_shadowArray);

        const ArrayDesc& shadowArrayDesc = _shadowArray->getArrayDesc();

        assert(shadowArrayDesc.getId() > 0);
        assert(shadowArrayDesc.getUAId() > 0);
        LOG4CXX_DEBUG(logger, "Redistribute shadow array " << shadowArrayDesc.getName());

        if (! query->isCoordinator()) {
            // worker

            SCIDB_ASSERT(ArrayDesc::isNameVersioned(shadowArrayDesc.getName()));
            string shadowArrayName = ArrayDesc::makeUnversionedName(shadowArrayDesc.getName());

            std::shared_ptr<SystemCatalog::LockDesc> lock(new SystemCatalog::LockDesc(shadowArrayName,
                                                                                 query->getQueryID(),
                                                                                 Cluster::getInstance()->getLocalInstanceId(),
                                                                                 SystemCatalog::LockDesc::WORKER,
                                                                                 SystemCatalog::LockDesc::WR));

            std::shared_ptr<Query::ErrorHandler> ptr(new UpdateErrorHandler(lock));
            query->pushErrorHandler(ptr);

            Query::Finalizer f = bind(&UpdateErrorHandler::releaseLock, lock, _1);
            query->pushFinalizer(f);
            SystemCatalog::ErrorChecker errorChecker = bind(&Query::validate, query);
            if (!SystemCatalog::getInstance()->lockArray(lock, errorChecker)) {
                throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK) << shadowArrayName;
            }
            SCIDB_ASSERT(lock->getLockMode() == SystemCatalog::LockDesc::WR);
        }
        std::shared_ptr<Array> persistentShadowArray(DBArray::newDBArray(shadowArrayDesc, query));
        ArrayDesc dstArrayDesc = persistentShadowArray->getArrayDesc();

        query->getReplicationContext()->enableInboundQueue(dstArrayDesc.getId(),
                                                           persistentShadowArray);

        set<Coordinates, CoordinatesLess> newChunkCoordinates;
        redistributeToArray(_shadowArray,
                            persistentShadowArray,
                            &newChunkCoordinates,
                            query);

        StorageManager::getInstance().removeDeadChunks(dstArrayDesc, newChunkCoordinates, query);
        PhysicalBoundaries bounds = PhysicalBoundaries::createFromChunkList(persistentShadowArray,
                                                                            newChunkCoordinates);
        PhysicalUpdate::updateSchemaBoundaries(dstArrayDesc, bounds, query);

        query->getReplicationContext()->replicationSync(dstArrayDesc.getId());
        query->getReplicationContext()->removeInboundQueue(dstArrayDesc.getId());
        StorageManager::getInstance().flush();
        // XXX TODO: add: getInjectedErrorListener().check();
    }

    // Callback for deferred scatter/gather of the shadow array.
    void InputArray::sg()
    {
        SCIDB_ASSERT(_shadowArray);
        std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
        redistributeShadowArray(query);
    }

    // In the execution tree, this InputArray is the child of an
    // automatically inserted SG operator.  Two SG's may not be in
    // progress for the same query at the same time, so once the
    // InputArray's data has been fully consumed by the upstream SG
    // (denoted by state == S_Empty), we schedule an SG for the shadow
    // array.  Once scheduled, state == S_Done.
    //
    void InputArray::scheduleSG(std::shared_ptr<Query> const& query)
    {
        if (!_shadowArray) {
            return;
        }
        std::shared_ptr<Query::OperatorContext> sgCtx = query->getOperatorContext();
        resetShadowChunkIterators();
        shadowArrayIterators.clear();
        if (sgCtx) {
            boost::function<void()> cb = boost::bind(&InputArray::sg, shared_from_this());
            sgCtx->setCallback(cb);
        } else {
            sg();
        }
    }

    void InputArray::handleError(Exception const& x,
                                 std::shared_ptr<ChunkIterator> cIter,
                                 AttributeID i)
    {
        SCIDB_ASSERT(_chunkLoader);
        string const& msg = x.getErrorMessage();
        Attributes const& attrs = desc.getAttributes();
        LOG4CXX_ERROR(logger, "Failed to convert attribute " << attrs[i].getName()
                      << " at position " << _chunkLoader->getFileOffset()
                      << " line " << _chunkLoader->getLine()
                      << " column " << _chunkLoader->getColumn() << ": " << msg);

        if (++nErrors > maxErrors) {
            if (maxErrors) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR16);
            } else {
                // If no maxErrors limit was set, show the original error.
                x.raise();
            }
        }

        Value errVal;
        if (attrs[i].isNullable()) {
            errVal.setNull();
        } else {
            errVal.setSize(TypeLibrary::getType(attrs[i].getType()).byteSize());
            errVal = TypeLibrary::getDefaultValue(attrs[i].getType());
        }
        cIter->writeItem(errVal);

        if (_shadowArray) {
            if (shadowChunkIterators.empty()) {
                std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
                if (shadowArrayIterators.empty()) {
                    shadowArrayIterators.resize(nAttrs+1);
                    for (size_t j = 0; j < nAttrs; j++) {
                        shadowArrayIterators[j] =
                            _shadowArray->getIterator(safe_static_cast<AttributeID>(j));
                    }
                    shadowArrayIterators[nAttrs] =
                        _shadowArray->getIterator(safe_static_cast<AttributeID>(nAttrs));
                }
                shadowChunkIterators.resize(nAttrs+1);
                Coordinates const& chunkPos = _chunkLoader->getChunkPos();
                for (size_t j = 0; j < nAttrs; j++) {
                    shadowChunkIterators[j] =
                       shadowArrayIterators[j]->newChunk(chunkPos, 0).getIterator(query,
                                                                                  ChunkIterator::NO_EMPTY_CHECK|
                                                                                  ChunkIterator::SEQUENTIAL_WRITE);
                }
                shadowChunkIterators[nAttrs] =
                   shadowArrayIterators[nAttrs]->newChunk(chunkPos, 0).getIterator(query,
                                                                                   ChunkIterator::SEQUENTIAL_WRITE);
            }
            Coordinates const& currPos = cIter->getPosition();
            if (lastBadAttr < 0) {
                Value rowOffset;
                rowOffset.setInt64(_chunkLoader->getFileOffset());
                shadowChunkIterators[nAttrs]->setPosition(currPos);
                shadowChunkIterators[nAttrs]->writeItem(rowOffset);
            }
            strVal.setNull();
            while (AttributeID(++lastBadAttr) < i) {
                shadowChunkIterators[lastBadAttr]->setPosition(currPos);
                shadowChunkIterators[lastBadAttr]->writeItem(strVal);
            }
            shadowChunkIterators[i]->setPosition(currPos);
            strVal.setString(msg.c_str());
            shadowChunkIterators[i]->writeItem(strVal);
        }
    }

    void InputArray::completeShadowArrayRow()
    {
        if (lastBadAttr >= 0) {
            strVal.setNull();
            // rowOffset attribute should be already set
            Coordinates const& currPos =  shadowChunkIterators[nAttrs]->getPosition();
            while (AttributeID(++lastBadAttr) < nAttrs) {
                shadowChunkIterators[lastBadAttr]->setPosition(currPos);
                shadowChunkIterators[lastBadAttr]->writeItem(strVal);
            }
        }
    }

    bool InputArray::moveNext(size_t chunkIndex)
    {
        bool more = false;
        LOG4CXX_TRACE(logger, "InputArray::moveNext: chunkIndex= " << chunkIndex);
        try {

            if (chunkIndex > _currChunkIndex+1) {
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR1);
            }
            std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
            if (chunkIndex <= _currChunkIndex) {
                return true;
            }
            if (state == S_Empty) {
                state = S_Done;
                scheduleSG(query);
                return false;
            }
            if (state == S_Done) {
                return false;
            }

            more = _chunkLoader->loadChunk(query, chunkIndex);
            if (more) {
                resetShadowChunkIterators();
                nLoadedChunks += 1;
                LOG4CXX_TRACE(logger, "Loading of " << desc.getName()
                              << " is in progress: load at this moment " << nLoadedChunks
                              << " chunks and " << nLoadedCells
                              << " cells with " << nErrors << " errors");

                _currChunkIndex += 1;
            } else {
                state = S_Done;
                scheduleSG(query);
            }

            LOG4CXX_TRACE(logger, "Finished scan of chunk number " << _currChunkIndex << ", more=" << more);
        }
        catch(Exception const& x)
        {
            resetShadowChunkIterators();
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_FILE_IMPORT_FAILED)
                << _chunkLoader->filePath()
                << myInstanceID
                << getName()
                << _chunkLoader->getLine()
                << _chunkLoader->getColumn()
                << _chunkLoader->getFileOffset()
                << debugEncode(_chunkLoader->getBadField())
                << x.getErrorMessage();
        }
        catch(std::exception const& x)
        {
            resetShadowChunkIterators();
            throw USER_EXCEPTION(SCIDB_SE_IMPORT_ERROR, SCIDB_LE_FILE_IMPORT_FAILED)
                << _chunkLoader->filePath()
                << myInstanceID
                << getName()
                << _chunkLoader->getLine()
                << _chunkLoader->getColumn()
                << _chunkLoader->getFileOffset()
                << debugEncode(_chunkLoader->getBadField())
                << x.what();
        }
        return more;
    }

    InputArray::~InputArray()
    {
        delete _chunkLoader;
        LOG4CXX_INFO(logger, "Loading of " << desc.getName()
                     << " is completed: loaded " << nLoadedChunks
                     << " chunks and " << nLoadedCells
                     << " cells with " << nErrors << " errors");
    }


    ConstChunk const& InputArray::getChunk(AttributeID attr, size_t chunkIndex)
    {
        LOG4CXX_TRACE(logger, "InputArray::getChunk: currChunkIndex=" << _currChunkIndex
                      << " attr=" << attr
                      << " chunkIndex=" << chunkIndex);

        Query::getValidQueryPtr(_query);
        if (chunkIndex > _currChunkIndex
            || chunkIndex + ChunkLoader::LOOK_AHEAD <= _currChunkIndex)
        {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OP_INPUT_ERROR11);
        }
        MemChunk& chunk = _chunkLoader->getLookaheadChunk(attr, chunkIndex);
        if (emptyTagAttrID != attr && emptyTagAttrID != INVALID_ATTRIBUTE_ID) {
            MemChunk& bitmapChunk = _chunkLoader->getLookaheadChunk(emptyTagAttrID, chunkIndex);
            chunk.setBitmapChunk(&bitmapChunk);
        }

        LOG4CXX_TRACE(logger, "InputArray::getChunk: currChunkIndex=" << _currChunkIndex
                      << " attr=" << attr
                      << " chunkIndex=" << chunkIndex
                      << " pos=" << CoordsToStr(chunk.getFirstPosition(false)));

        return chunk;
    }
}
