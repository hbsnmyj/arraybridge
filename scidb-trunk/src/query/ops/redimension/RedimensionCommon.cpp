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

#include "RedimensionCommon.h"

#include "ChunkIdMap.h"
#include "CoordMetrics.h"
#include "../create_array/ChunkEstimator.h"
#include "OverlapRemapperArray.h"
#include "RemapperArray.h"
#include "Remapper.h"

#include <boost/assign/list_of.hpp>
#include <system/Config.h>
#include <array/SortArray.h>
#include <util/OverlappingChunksIterator.h>

using namespace std;

namespace scidb
{

using namespace arena;
using mgd::vector; // place as much of locally allocate memory on the operator
using mgd::map;    //  arena as possible, please...

const size_t redimMinChunkSize = 1*KiB;
const size_t redimMaxChunkSize = 1*MiB;

log4cxx::LoggerPtr RedimensionCommon::logger(log4cxx::Logger::getLogger("scidb.array.RedimensionCommon"));

void RedimensionCommon::setupMappings(ArrayDesc const&                  srcArrayDesc,
                                      PointerRange<AggregatePtr>        aggregates,
                                      PointerRange<size_t>              attrMapping,
                                      PointerRange<size_t>              dimMapping,
                                      PointerRange<AttributeDesc const> destAttrs,
                                      PointerRange<DimensionDesc const> destDims)
{
    assert(aggregates.size() == attrMapping.size());
    assert(_schema.getAttributes(true).size() == aggregates.size());
    assert(_schema.getAttributes().size() == aggregates.size()+1);

    PointerRange<AttributeDesc const> srcAttrs = srcArrayDesc.getAttributes(true);
    PointerRange<DimensionDesc const> srcDims = srcArrayDesc.getDimensions();

    for(size_t i =1; i<_parameters.size(); i++)
    {
        if(_parameters[i]->getParamType() == PARAM_AGGREGATE_CALL)
        {
            AttributeID inputAttId;
            string aggOutputName;
            AggregatePtr agg = resolveAggregate((std::shared_ptr<OperatorParamAggregateCall>&) _parameters[i],
                    srcArrayDesc.getAttributes(),
                    &inputAttId,
                    &aggOutputName);

            bool found = false;
            if (inputAttId == INVALID_ATTRIBUTE_ID)
            {
                inputAttId = 0;
            }

            for (size_t j = 0; j<_schema.getAttributes(true).size(); j++)
            {
                if (_schema.getAttributes()[j].getName() == aggOutputName)
                {
                    aggregates[j] = agg;
                    attrMapping[j] = inputAttId;
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OP_REDIMENSION_STORE_ERROR6) << aggOutputName;
            }
        } else {
            assert(_parameters[i]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            assert(i==1);
        }
    }

    for (size_t i = 0; i < destAttrs.size(); i++)
    {
        if (aggregates[i].get())
        {//already populated
            continue;
        }

        for (size_t j = 0; j < srcAttrs.size(); j++) {
            if (srcAttrs[j].getName() == destAttrs[i].getName()) {
                attrMapping[i] = j;
                goto NextAttr;
            }
        }
        for (size_t j = 0; j < srcDims.size(); j++) {
            if (srcDims[j].hasNameAndAlias(destAttrs[i].getName())) {
                attrMapping[i] = turnOn(j, FLIP);
                goto NextAttr;
            }
        }

        // A dest attribute either comes from a src dimension or a src attribute. Can't reach here.
        SCIDB_UNREACHABLE();

        NextAttr:;
    }
    for (size_t i = 0; i < destDims.size(); i++) {
        for (size_t j = 0; j < srcDims.size(); j++) {
            if (srcDims[j].hasNameAndAlias(destDims[i].getBaseName())) {
                dimMapping[i] = j;
                goto NextDim;
            }
        }
        for (size_t j = 0; j < srcAttrs.size(); j++) {
            if (destDims[i].hasNameAndAlias(srcAttrs[j].getName())) {
                dimMapping[i] = turnOn(j, FLIP);
                goto NextDim;
            }
        }
        dimMapping[i] = SYNTHETIC;
        NextDim:;
    }
}

std::shared_ptr<MemArray> RedimensionCommon::initializeRedimensionedArray(
    std::shared_ptr<Query> const&             query,
    PointerRange<AttributeDesc const>    srcAttrs,
    PointerRange<AttributeDesc const>    destAttrs,
    PointerRange<const size_t>           attrMapping,
    PointerRange<const AggregatePtr>     aggregates,
    vector< std::shared_ptr<ArrayIterator> >& redimArrayIters,
    vector< std::shared_ptr<ChunkIterator> >& redimChunkIters,
    size_t                               redimChunkSize)
{
    // Create a 1-D MemArray called 'redimensioned' to hold the redimensioned records.
    // Each cell in the array corresponds to a cell in the destination array,
    // where its position within the destination array is determined by two
    // additional attributes: the destination chunk identifier, and the
    // position within the destination chunk.

    // The schema is adapted from destArrayDesc, with the following differences:
    //    (a) An aggregate field's type is replaced with the source field type, but still uses the name of the dest attribute.
    //        The motivation is that multiple dest aggregate attribute may come from the same source attribute,
    //        in which case storing under the source attribute name would cause a conflict.
    //    (b) Two additional attributes are appended to the end:
    //        (1) 'tmpDestChunkPosition', that stores the location of the item in the dest chunk
    //        (2) 'tmpDestChunkId', that stores the id of the destination chunk
    //
    // The data is derived from the inputarray as follows.
    //    (a) They are "redimensioned".
    //    (b) Each record is stored as a distinct record in the MemArray. For an aggregate field, no aggregation is performed;
    //        For a synthetic dimension, just use dimStartSynthetic.
    //
    // Local aggregation will be performed at a later step, when generating the MemArray called 'beforeRedistribute'.
    // Global aggregation will be performed at the redistributeAggregate() step.
    //

    Dimensions dimsRedimensioned(1);
    Attributes attrsRedimensioned;
    // Avoid having to call safe_static_cast<> in each iteration of for loops
    // destAttrs is constant so there is no reason to accrue the overhead
    ASSERT_EXCEPTION(destAttrs.size() <= std::numeric_limits<AttributeID>::max(),
                     "Too many Destination Attributes");
    AttributeID destAttrsSize = safe_static_cast<AttributeID>(destAttrs.size());
    for (AttributeID i=0; i < destAttrsSize; ++i) {
        // For aggregate field, store the source data but under the name of the dest attribute.
        // The motivation is that multiple dest aggregate attribute may come from the same source attribute,
        // in which case storing under the source attribute name would cause conflict.
        //
        // An optimization is possible in this special case, to only store the source attribute once.
        // But some unintuitive bookkeeping would be needed.
        // We decide to skip the optimization at least for now.
        if (aggregates[i]) {
            AttributeDesc const& srcAttrForAggr = srcAttrs[ attrMapping[i] ];
            attrsRedimensioned.push_back(AttributeDesc(i,
                                                       destAttrs[i].getName(),
                                                       srcAttrForAggr.getType(),
                                                       srcAttrForAggr.getFlags(),
                                                       srcAttrForAggr.getDefaultCompressionMethod()));
        } else {
            attrsRedimensioned.push_back(destAttrs[i]);
        }
    }

    // TODO: Seems like leading '_' or some other scheme to prevent name collisions is called for.
    AttributeID destAttrSize = static_cast<AttributeID>(destAttrs.size());
    attrsRedimensioned.push_back(
        AttributeDesc(destAttrSize, "tmpDestPositionInChunk", TID_INT64, 0, 0));
    attrsRedimensioned.push_back(
        AttributeDesc(destAttrSize + 1, "tmpDestChunkId", TID_INT64, 0, 0));
    dimsRedimensioned[0] = DimensionDesc("Row", 0, CoordinateBounds::getMax(), redimChunkSize, 0);

    Attributes attrsRedimensionedWithET(attrsRedimensioned);
    attrsRedimensionedWithET.push_back(
        AttributeDesc(safe_static_cast<AttributeID>(attrsRedimensioned.size()),
                      DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
                      TID_INDICATOR,
                      AttributeDesc::IS_EMPTY_INDICATOR,
                      0));
    ArrayDesc schemaRedimensioned("",
                                  attrsRedimensionedWithET,
                                  dimsRedimensioned,
                                  createDistribution(psUndefined),
                                  _schema.getResidency());
    std::shared_ptr<MemArray> redimensioned(make_shared<MemArray>(schemaRedimensioned, query));

    // Initialize the iterators
    redimArrayIters.resize(attrsRedimensioned.size());
    redimChunkIters.resize(attrsRedimensioned.size());
    // Avoid having to call safe_static_cast<> in each iteration of for loops
    // attrsRedimensioned is constant so there is no reason to accrue the overhead
    ASSERT_EXCEPTION(attrsRedimensioned.size() <= std::numeric_limits<AttributeID>::max(),
                     "Too many Attributes in Redimension");
    for (AttributeID i = 0; i < static_cast<AttributeID>(attrsRedimensioned.size()); i++)
    {
        redimArrayIters[i] = redimensioned->getIterator(i);
    }

    return redimensioned;
}

void RedimensionCommon::appendItemToRedimArray(PointerRange<const Value> item,
                                               std::shared_ptr<Query> const& query,
                                               PointerRange< std::shared_ptr<ArrayIterator> const > redimArrayIters,
                                               PointerRange< std::shared_ptr<ChunkIterator> >        redimChunkIters,
                                               size_t& redimCount,
                                               size_t redimChunkSize)
{
    // if necessary, refresh the chunk iterators
    if (redimCount % redimChunkSize == 0)
    {
        Coordinates chunkPos(1);
        int chunkMode = ChunkIterator::SEQUENTIAL_WRITE;  // important for performance & mem usage
        chunkPos[0] = redimCount;
        for (size_t i = 0; i < redimArrayIters.size(); i++)
        {
            Chunk& chunk = redimArrayIters[i]->newChunk(chunkPos, 0);
            redimChunkIters[i] = chunk.getIterator(query, chunkMode);
            chunkMode |= ChunkIterator::NO_EMPTY_CHECK;  // creat iterator without this flag only for first attr
        }
    }

    // append the item to the current chunks
    for (size_t i = 0; i < item.size(); i++)
    {
        redimChunkIters[i]->writeItem(item[i]);
    }
    redimCount++;

    // flush the current chunks, or advance the iters
    if (redimCount % redimChunkSize == 0)
    {
        for (size_t i = 0; i < redimChunkIters.size(); i++)
        {
            redimChunkIters[i]->flush();
            redimChunkIters[i].reset();
        }
    }
    else
    {
        for (size_t i = 0; i < redimChunkIters.size(); i++)
        {
            ++(*redimChunkIters[i]);
        }
    }
}

// used during PHASE 3
bool RedimensionCommon::updateSyntheticDimForRedimArray(std::shared_ptr<Query> const& query,
                                                        ArrayCoordinatesMapper const& coordMapper,
                                                        ChunkIdMap& chunkIdMaps,
                                                        size_t dimSynthetic,
                                                        std::shared_ptr<Array>& redimensioned)
{
    using std::shared_ptr;
    // If there is a synthetic dimension, and if there are duplicates, modify the values
    // (so that the duplicates get distinct coordinates in the synthetic dimension).
    //
    
    queue< pair<position_t, position_t> > updates;
    bool needsResort = false;
    size_t currChunkId;
    size_t nextChunkId;
    position_t prevPosition;
    position_t currPosition;
    Coordinates currPosCoord(coordMapper.getDims().size());
    AttributeID chunkIdAttr = safe_static_cast<AttributeID>(
        redimensioned->getArrayDesc().getAttributes(true).size() - 1);
    AttributeID posAttr = chunkIdAttr - 1;
    shared_ptr<ConstArrayIterator> arrayChunkIdIter = redimensioned->getConstIterator(chunkIdAttr);
    shared_ptr<ArrayIterator> arrayPosIter = redimensioned->getIterator(posAttr);
    SCIDB_ASSERT(!arrayChunkIdIter->end());
    SCIDB_ASSERT(!arrayPosIter->end());
    shared_ptr<ConstChunkIterator> chunkChunkIdIter = arrayChunkIdIter->getChunk().getConstIterator();
    shared_ptr<ConstChunkIterator> chunkPosReadIter = arrayPosIter->getChunk().getConstIterator();
    shared_ptr<ChunkIterator> chunkPosWriteIter;
    Coordinates lows(coordMapper.getDims().size());
    Coordinates intervals(coordMapper.getDims().size());

    // initialize the previous position value, current chunk id, and lows and intervals
    prevPosition = chunkPosReadIter->getItem().getInt64();
    currChunkId = chunkChunkIdIter->getItem().getInt64();
    coordMapper.chunkPos2LowsAndIntervals(chunkIdMaps.mapIdToChunkPos(currChunkId),
                                          lows,
                                          intervals);
    coordMapper.pos2coordWithLowsAndIntervals(lows, intervals, prevPosition, currPosCoord);
    ++(*chunkPosReadIter);
    ++(*chunkChunkIdIter);

    // scan array from beginning to end
    Coordinates updatePos(1);                           // moved out of inner loop to avoid malloc
    while (!arrayChunkIdIter->end())
    {
        while (!chunkChunkIdIter->end())
        {
            // Are we processing a new output chunk id?
            nextChunkId = chunkChunkIdIter->getItem().getInt64();
            if (nextChunkId != currChunkId)
            {
                prevPosition = chunkPosReadIter->getItem().getInt64();
                currChunkId = nextChunkId;
                coordMapper.chunkPos2LowsAndIntervals(chunkIdMaps.mapIdToChunkPos(currChunkId),
                                                      lows,
                                                      intervals);
                coordMapper.pos2coordWithLowsAndIntervals(lows, intervals, prevPosition, currPosCoord);
                goto nextitem;
            }

            // Are we processing a run of identical positions?
            currPosition = chunkPosReadIter->getItem().getInt64();
            if (currPosition == prevPosition)
            {
                // found a duplicate --- add an update to the list
                pair<position_t, position_t> pu;

                currPosCoord[dimSynthetic]++;
                pu.first = chunkPosReadIter->getPosition()[0];
                pu.second = coordMapper.coord2posWithLowsAndIntervals(lows,
                                                                      intervals,
                                                                      currPosCoord);
                updates.push(pu);

                // make sure the number of duplicates is less than chunk interval (for the synthetic dim)
                if ((currPosCoord[dimSynthetic] - lows[dimSynthetic]) >=
                    intervals[dimSynthetic])
                {
                    throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_OP_REDIMENSION_STORE_ERROR7);
                }
            }
            else
            {
                prevPosition = currPosition;
                coordMapper.pos2coordWithLowsAndIntervals(lows, intervals, currPosition, currPosCoord);
            }

        nextitem:
            ++(*chunkPosReadIter);
            ++(*chunkChunkIdIter);
        }

        // At the end of a chunk, process any updates we have accumulated...
        if (!updates.empty())
        {
            needsResort = true;

            // OVERWRITING existing cells
            chunkPosWriteIter = arrayPosIter->updateChunk().getIterator(query,
                                                                        ChunkIterator::APPEND_CHUNK |
                                                                        ChunkIterator::APPEND_EMPTY_BITMAP |
                                                                        ChunkIterator::NO_EMPTY_CHECK);
            while (!updates.empty())
            {
                Value updateVal;

                updatePos[0] = updates.front().first;
                updateVal.setInt64(updates.front().second);
                chunkPosWriteIter->setPosition(updatePos);
                chunkPosWriteIter->writeItem(updateVal);

                updates.pop();
            }
            chunkPosWriteIter->flush();
            chunkPosWriteIter.reset();
        }

        // Goto next chunk
        ++(*arrayPosIter);
        ++(*arrayChunkIdIter);
        if (!arrayChunkIdIter->end())
        {
            chunkChunkIdIter = arrayChunkIdIter->getChunk().getConstIterator();
            chunkPosReadIter = arrayPosIter->getChunk().getConstIterator();
        }
    }

    return needsResort;
}


void RedimensionCommon::appendItemToBeforeRedistribution(
    ArrayCoordinatesMapper const& coordMapper,
    CoordinateCRange lows,
    CoordinateCRange intervals,
    Coordinates& tmp,
    position_t prevPosition,
    PointerRange< std::shared_ptr<ChunkIterator> const> chunkItersBeforeRedist,
    StateVector& stateVector)
{
    // Do nothing if stateVector has nothing in it
    if (stateVector.isValid())
    {
        coordMapper.pos2coordWithLowsAndIntervals(lows, intervals, prevPosition, tmp);

        PointerRange<const Value> destItem = stateVector.get();
        for (size_t a = 0, s=chunkItersBeforeRedist.size(); a < s; ++a) {
            bool rc = chunkItersBeforeRedist[a]->setPosition(tmp);
            if (!rc) {
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_REDIMENSION_POSITION) << CoordsToStr(tmp);
            }
            chunkItersBeforeRedist[a]->writeItem(destItem[a]);
        }
    }
}

/**
 * A template function that reads a scidb::Value of any integral type, and returns an int64_t value.
 * This is needed when turning an arbitrary integral type attribute to a dimension.
 *
 * @param v   a scidb::Value object, storing a value of type T.
 * @return    the value of type int64_t.
 *
 * @note T==int8_t || T==int16_t || T==int32_t || T==int64_t || T==uint8_t || T==uint16_t || T==uint32_t || T==uint64_t
 *
 */
template<class T>
int64_t integerTypeToInt64(Value const& v)
{
    // This assert holds, or else we should have thrown SCIDB_LE_OP_REDIMENSION_ERROR2 during
    // LogicalRedimension::inferSchema().
    static_assert(std::is_signed<T>::value || std::numeric_limits<T>::digits < 64,
                  "Cannot safely convert uint64 attribute to int64 dimension.");

    return static_cast<int64_t>(v.get<T>());
}

typedef int64_t (*IntegerCoercion)(const Value&);

std::shared_ptr<Array> RedimensionCommon::redimensionArray(std::shared_ptr<Array>& srcArray,
                                                      PointerRange<const size_t> attrMapping,
                                                      PointerRange<const size_t> dimMapping,
                                                      PointerRange<const AggregatePtr> aggregates,
                                                      std::shared_ptr<Query> const& query,
                                                      ElapsedMilliSeconds& timing,
                                                      RedistributeMode redistributeMode)
{
    // def of the meta data
    ArrayDesc const& srcArrayDesc = srcArray->getArrayDesc();
    Attributes const& srcAttrs = srcArrayDesc.getAttributes(true); // true = exclude the empty tag
    Attributes const& destAttrs = _schema.getAttributes(true);
    Dimensions const& destDims = _schema.getDimensions();

    // Does the dest array have a synthetic dimension?
    bool hasSynthetic = false;
    bool synthAutochunked = false;
    size_t dimSynthetic = 0;
    Coordinate dimStartSynthetic = CoordinateBounds::getMin();
    Coordinate dimEndSynthetic = CoordinateBounds::getMax();

    for (size_t i=0; i<dimMapping.size(); ++i) {
        if (dimMapping[i] == SYNTHETIC) {
            hasSynthetic = true;
            dimSynthetic = i;
            dimStartSynthetic = destDims[i].getStartMin();
            synthAutochunked = destDims[i].isAutochunked();
            if (!synthAutochunked) {
                dimEndSynthetic = dimStartSynthetic + destDims[i].getChunkInterval() - 1;
            }
            SCIDB_ASSERT(dimEndSynthetic>=dimStartSynthetic);
            break;
        }
    }

    // Does the dest array have any aggregate?
    bool hasAggregate = false;
    for (size_t i=0; i<aggregates.size(); ++i) {
        if (aggregates[i]) {
            hasAggregate = true;
            break;
        }
    }

    bool hasOverlap = false;
    for (size_t i = 0; i < destDims.size(); i++) {
        // Does the dest array have any overlap?
        if (destDims[i].getChunkOverlap() != 0) {
            hasOverlap = true;
        }

        // Any pass-thru intervals we should fix up?
        if (destDims[i].getRawChunkInterval() == DimensionDesc::PASSTHRU) {
            size_t srcDim = dimMapping[i];
            SCIDB_ASSERT(!isFlipped(srcDim));
            int64_t interval = srcArrayDesc.getDimensions()[srcDim].getChunkInterval();
            _schema.getDimensions()[i].setChunkInterval(interval);
            SCIDB_ASSERT(destDims[i].getChunkInterval() == interval);
        }
    }

    // Maybe create dimensions using provisional chunk intervals.
    //
    // When autochunking, provDims are the provisional dimensions, and we will eventually compute
    // the finalDims.  When *not* autochunking, provDims and finalDims are the same: it's a
    // convenience to have them both to avoid unnecessarily complicating code paths below.
    //
    Dimensions finalDims(destDims);
    Dimensions provDims;
    bool autochunk = makeProvisionalChunking(hasSynthetic, dimSynthetic, provDims);
    SCIDB_ASSERT(autochunk == !provDims.empty());
    if (autochunk) {
        // The destDims (and hence the finalDims) are autochunked.  "Metric exchange" will fill in
        // the missing intervals below!
        LOG4CXX_INFO(logger, "Using provisional chunk intervals, old: [" << destDims
                     << "], new: [" << provDims << ']');
        _schema.setDimensions(provDims); // Will set them to finalDims later.
    } else {
        provDims = destDims;
    }

    // Initialize 'redimensioned' array
    std::shared_ptr<Array> redimensioned;
    vector< std::shared_ptr<ArrayIterator> > redimArrayIters(_arena);
    vector< std::shared_ptr<ChunkIterator> > redimChunkIters(_arena);
    size_t redimChunkSize =
        Config::getInstance()->getOption<size_t>(CONFIG_REDIMENSION_CHUNKSIZE);

    if (redimChunkSize > redimMaxChunkSize)
        redimChunkSize = redimMaxChunkSize;
    if (redimChunkSize < redimMinChunkSize)
        redimChunkSize = redimMinChunkSize;

    redimensioned = initializeRedimensionedArray(query,
                                                 srcAttrs,
                                                 destAttrs,
                                                 attrMapping,
                                                 aggregates,
                                                 redimArrayIters,
                                                 redimChunkIters,
                                                 redimChunkSize);

    SCIDB_ASSERT(redimArrayIters.size() == destAttrs.size() + 2);
    SCIDB_ASSERT(redimChunkIters.size() == destAttrs.size() + 2);

    timing.logTiming(logger, "[RedimensionArray] PHASE 0 'redimensioned' initialized");
    // PHASE 1 - convert to redimensioned form (but not order)

    // Iterate through the input array, generate the output data, and append to the MemArray.
    // Note: For an aggregate field, its source value (in the input array) is used.
    // Note: The synthetic dimension is not handled here. That is, multiple records that will be
    //       differentiated along the synthetic dimension are all appended to the 'redimensioned'
    //       array with the same 'position'.
    //
    size_t iterAttr = 0;    // one of the attributes from the input array that needs to be iterated

    vector< std::shared_ptr<ConstArrayIterator> > srcArrayIterators(_arena,srcAttrs.size());
    vector< std::shared_ptr<ConstChunkIterator> > srcChunkIterators(_arena,srcAttrs.size());

    // A vector of functors, to be used to get value from an input array and return an int64_t value.
    vector<IntegerCoercion> functorsGetSourceValue(_arena,destDims.size());

    // Initialize the source array iters
    for (size_t i = 0; i < destAttrs.size(); i++) {
        size_t j = attrMapping[i];
        if (!isFlipped(j)) {
            if (!srcArrayIterators[iterAttr]) {
                iterAttr = j;
            }
            srcArrayIterators[j] = srcArray->getConstIterator(safe_static_cast<AttributeID>(j));
        }
    }
    for (size_t i = 0; i < destDims.size(); i++) {
        size_t j = dimMapping[i];
        if (isFlipped(j)) {
            j = turnOff(j, FLIP);
            if (!srcArrayIterators[iterAttr]) {
                iterAttr = j;
            }
            srcArrayIterators[j] = srcArray->getConstIterator(safe_static_cast<AttributeID>(j));

            // TODO:  Ugh.  Use FunctionLibrary::findConverter().  Example in ops/input/ChunkLoader.cpp.
            TypeId tid = srcAttrs[j].getType();
            if (tid == TID_INT8) {
                functorsGetSourceValue[i] = &integerTypeToInt64<int8_t>;
            }
            else if (tid == TID_INT16) {
                functorsGetSourceValue[i] = &integerTypeToInt64<int16_t>;
            }
            else if (tid == TID_INT32) {
                functorsGetSourceValue[i] = &integerTypeToInt64<int32_t>;
            }
            else if (tid == TID_INT64) {
                functorsGetSourceValue[i] = &integerTypeToInt64<int64_t>;
            }
            else if (tid == TID_UINT8) {
                functorsGetSourceValue[i] = &integerTypeToInt64<uint8_t>;
            }
            else if (tid == TID_UINT16) {
                functorsGetSourceValue[i] = &integerTypeToInt64<uint16_t>;
            }
            else if (tid == TID_UINT32) {
                functorsGetSourceValue[i] = &integerTypeToInt64<uint32_t>;
            }
            else if (tid == TID_UINT64) {
                // Should have caught this already in LogicalRedimension::inferSchema().
                ASSERT_EXCEPTION(false, "In RedimensionCommon::redimensionArray(), cannot safely convert"
                                 " uint64 attribute to int64 dimension.");
            }
            else {
                ASSERT_EXCEPTION(false, "In RedimensionCommon::redimensionArray(), src attr type must be "
                                 "of integer type.");
            }
        }
        else {
            functorsGetSourceValue[i] = &integerTypeToInt64<int64_t>;
        }
    }
    if (!srcArrayIterators[iterAttr]) {
        // If no src attribute needs to be scanned, open one anyways.
        assert(iterAttr == 0);
        srcArrayIterators[0] = srcArray->getConstIterator(0);
    }

    // Start scanning the input
    std::shared_ptr<ArrayCoordinatesMapper> arrayCoordinatesMapper =
        std::make_shared<ArrayCoordinatesMapper>(provDims); // == destDims if !autochunk
    std::shared_ptr<ChunkIdMap> arrayChunkIdMap = createChunkIdMap(provDims, _arena);
    size_t redimCount = 0;      // Actually a count of cells written to the 1-D.

    CoordMetrics coordMetrics(destDims.size(), synthAutochunked);  // local metrics for autochunking
    Coordinates destPos(destDims.size());                          // in outermost loop to avoid mallocs
    vector<Value> valuesInRedimArray(_arena,destAttrs.size()+2);   // in outermost loop to avoid mallocs

    while (!srcArrayIterators[iterAttr]->end())
    {
        // Initialize src chunk iterators
        for (size_t i = 0; i < srcAttrs.size(); i++) {
            if (srcArrayIterators[i]) {
                srcChunkIterators[i] = srcArrayIterators[i]->getChunk().getConstIterator();
            }
        }

        // Initialize the dest
        Coordinates chunkPos;

        // Loop through the chunks content
        while (!srcChunkIterators[iterAttr]->end()) {
            Coordinates const& srcPos = srcChunkIterators[iterAttr]->getPosition();

            // Get the destPos for this item -- for the SYNTHETIC dim, use the same value (dimStartSynthetic) for all.
            size_t nDims = destDims.size();
            for (size_t i = 0; i < nDims; i++) {
                size_t j = dimMapping[i];
                if (isFlipped(j)) {
                    Value const& value = srcChunkIterators[turnOff(j,FLIP)]->getItem();
                    if (value.isNull()) {
                        // a dimension is NULL. Just skip this item.
                        goto ToNextItem;
                    }
                    destPos[i] = (*functorsGetSourceValue[i])(value);
                } else if (j == SYNTHETIC) {
                    destPos[i] = dimStartSynthetic;
                } else {
                    destPos[i] = srcPos[j];
                }
            }

            // Sanity check, and accumulate local metrics.
            for (size_t i=0; i < nDims; ++i) {
                if (destPos[i]<destDims[i].getStartMin() || destPos[i]>destDims[i].getEndMax()) {
                    throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_INVALID_REDIMENSION_POSITION)
                        << CoordsToStr(destPos);
                }
                if (autochunk) {
                    coordMetrics.accumulate(i, destPos[i]);
                }
            }

            chunkPos = destPos;
            _schema.getChunkPositionFor(chunkPos);

            // Build data (except the last two fields, i.e. position/chunkid) to be written
            for (size_t i = 0; i < destAttrs.size(); i++) {
                size_t j = attrMapping[i];
                if ( isFlipped(j) ) { // if flipped from a dim
                    valuesInRedimArray[i].setInt64( srcPos[turnOff(j, FLIP)] );
                } else { // from an attribute
                    valuesInRedimArray[i] = srcChunkIterators[j]->getItem();
                }
            }

            // Set the last two fields of the data, and append to the redimensioned array.
            // When autochunking, the overlap regions will be generated later.
            if (hasOverlap && !autochunk) {
                // OverlappingChunksIterator iterates over the logical space.
                // Per THE REQUEST TO JUSTIFY LOGICAL-SPACE ITERATION (see RegionCoordinatesIterator.h),
                // here is why it is ok.
                // If chunkOverlap = 0, there is only one chunk in the space so it is ok.
                // With non-zero chunkOverlaps, the space includes only the neighbor chunks that need to store
                // a copy of this record. We have no option but to iterate over all of them.
                //
                OverlappingChunksIterator allChunks(destDims, destPos);
                while (!allChunks.end()) {
                    Coordinates const& overlappingChunkPos = allChunks.getPosition();
                    position_t pos = arrayCoordinatesMapper->coord2pos(overlappingChunkPos, destPos);
                    valuesInRedimArray[destAttrs.size()].setInt64(pos);
                    position_t chunkId = arrayChunkIdMap->mapChunkPosToId(overlappingChunkPos);
                    valuesInRedimArray[destAttrs.size()+1].setInt64(chunkId);
                    appendItemToRedimArray(valuesInRedimArray,
                                           query,
                                           redimArrayIters,
                                           redimChunkIters,
                                           redimCount,
                                           redimChunkSize);

                    // Must increment *after* overlappingChunkPos is no longer needed, because
                    // the increment will clobber overlappingChunkPos.
                    ++allChunks;
                }
            } else {
                CoordMetrics::PositionPair pp;
                pp.pos = arrayCoordinatesMapper->coord2pos(chunkPos, destPos);
                valuesInRedimArray[destAttrs.size()].setInt64(pp.pos);
                pp.chunkId = arrayChunkIdMap->mapChunkPosToId(chunkPos);
                valuesInRedimArray[destAttrs.size()+1].setInt64(pp.chunkId);
                appendItemToRedimArray(valuesInRedimArray,
                                       query,
                                       redimArrayIters,
                                       redimChunkIters,
                                       redimCount,
                                       redimChunkSize);
                if (autochunk) {
                    coordMetrics.accumulate(pp);
                }
            }

            // Advance chunk iterators
        ToNextItem:

            for (size_t i = 0; i < srcAttrs.size(); i++) {
                if (srcChunkIterators[i]) {
                    ++(*srcChunkIterators[i]);
                }
            }
        }

        // Advance array iterators
        for (size_t i = 0; i < srcAttrs.size(); i++) {
            if (srcArrayIterators[i]) {
                ++(*srcArrayIterators[i]);
            }
        }
    } // while

    // If there are leftover values, flush the output iters one last time
    if (redimCount % redimChunkSize != 0)
    {
        for (size_t i = 0; i < redimChunkIters.size(); ++i)
        {
            redimChunkIters[i]->flush();
            redimChunkIters[i].reset();
        }
    }
    for (size_t i = 0; i < redimArrayIters.size(); ++i)
    {
        redimArrayIters[i].reset();
    }

    timing.logTiming(logger, "[RedimensionArray] PHASE 1: conversion to redimensioned form (not order)");

    // If autochunking, we now have our partial <min,max,approxdc> metrics and can do "metric
    // exchange" to compute the final chunk intervals.
    if (autochunk) {
        try {
            exchangeMetrics(query,
                            coordMetrics,
                            (hasSynthetic ? dimSynthetic : -1),
                            finalDims);

            // Pretend like we knew them all along!
            _schema.setDimensions(finalDims);
            SCIDB_ASSERT(!_schema.isAutochunked());
        }
        catch (std::exception& ex) {
            LOG4CXX_DEBUG(logger, "Exception from exchangeMetrics: " << ex.what());
            throw;
        }
        LOG4CXX_DEBUG(logger, "Metric exchange yields " << finalDims);
        timing.logTiming(logger, "[RedimensionArray] PHASE 1A: autochunking metric exchange complete");
    }

    // LOG4CXX_DEBUG(logger, "[RedimensionArray] redimensioned values: ");
    // redimensioned->printArrayToLogger();

    // PHASE 2 - sort "redimensioned" to global order

    // drop the source array
    redimChunkIters.clear();
    redimArrayIters.clear();
    srcChunkIterators.clear();
    srcArrayIterators.clear();
    srcArray.reset();

    // If autochunking (and if we have anything to actually work on, i.e. redimCount > 0), here we
    // wrap the 1-D array with a "remapper" that will transform the provision coordinates into final
    // coordinates on the fly.
    std::shared_ptr<Remapper> remapper;
    if (autochunk && redimCount) {
        remapper.reset(new Remapper(*arrayCoordinatesMapper, arrayChunkIdMap, finalDims, _arena));
        std::shared_ptr<Array> remappedRedim;
        if (hasOverlap) {
            remappedRedim.reset(new OverlapRemapperArray(redimensioned, remapper, query));
        } else {
            remappedRedim.reset(new RemapperArray(redimensioned, remapper, query));
        }
        redimensioned = remappedRedim;
    }

    // Sort the redimensioned array based on the chunkid, followed by the position in the chunk
    //
    SortingAttributeInfos sortingAttributeInfos(2);
    SortingAttributeInfo k;
    k.columnNo = safe_static_cast<int>(destAttrs.size() + 1);
    k.ascent = true;
    sortingAttributeInfos[0] = k;
    k.columnNo = safe_static_cast<int>(destAttrs.size());
    k.ascent = true;
    sortingAttributeInfos[1] = k;

    const bool preservePositions = false;
    SortArray sorter(redimensioned->getArrayDesc(), _arena, preservePositions);
    std::shared_ptr<TupleComparator> tcomp(std::make_shared<TupleComparator>(sortingAttributeInfos, redimensioned->getArrayDesc()));
    if (redimCount)
    {
        std::shared_ptr<MemArray> sortedRedimensioned = sorter.getSortedArray(redimensioned, query, tcomp);
        redimensioned = sortedRedimensioned;
    }

    // Sort done, and the resulting array has the final (chunkId,cellPos) pairs.  Extract and use
    // the corresponding ChunkIdMap.
    if (autochunk && redimCount) {
        SCIDB_ASSERT(remapper);
        arrayChunkIdMap = remapper->getFinalChunkIdMap();
        arrayCoordinatesMapper = std::make_shared<ArrayCoordinatesMapper>(finalDims);
        remapper.reset();       // releases provisional ChunkIdMap memory
    } else {
        SCIDB_ASSERT(!remapper);
    }
    
    // reverse the direction of the chuk pos <=> id bijection
    arrayChunkIdMap->reverse();

    timing.logTiming(logger, "[RedimensionArray] PHASE 2A: redimensioned sort pass 1");

    // LOG4CXX_DEBUG(logger, "[RedimensionArray] redimensioned sorted values: ");
    // redimensioned->printArrayToLogger();

    // If hasSynthetic, each record with the same position get assigned a distinct value in the synthetic dimension, effectively
    // assigning a distinct position to every record.  After updating the redimensioned array, it will need to be re-sorted.
    //
    if (hasSynthetic && redimCount)
    {
        bool updated = updateSyntheticDimForRedimArray(query,
                                                       *arrayCoordinatesMapper,
                                                       *arrayChunkIdMap,
                                                       dimSynthetic,
                                                       redimensioned);
        timing.logTiming(logger, "[RedimensionArray] PHASE 2B: redimensioned updateSynthetic");
        if(updated)
        {
            // LOG4CXX_DEBUG(logger, "[RedimensionArray] redimensioned after update synthetic before sort: ");
            // redimensioned->printArrayToLogger();

            std::shared_ptr<MemArray> sortedRedimSynthetic = sorter.getSortedArray(redimensioned, query, tcomp);
            redimensioned = sortedRedimSynthetic;
            timing.logTiming(logger, "[RedimensionArray] PHASE 2C: redimensioned sort pass 2");
        }
        // LOG4CXX_DEBUG(logger, "[RedimensionArray] redimensioned after update synthetic after sort2: ");
        // redimensioned->printArrayToLogger();
    }

    timing.logTiming(logger, "[RedimensionArray] PHASE 2: complete");
    // PHASE 3 - aggregate into 'beforeRedistribution'

    // Create a MemArray call 'beforeRedistribution'.
    //
    // The schema is adapted from destArrayDesc as follows:
    //    (a) For an aggregate field, the type is the 'State' of the aggregate, rather than the destination field type.
    //
    // The data is computed as follows:
    //    (a) For an aggregate field, the aggregate state, among all records with the same position, is stored.
    //    (b) If !hasAggregate and !hasSynthetic, for duplicates, only one record is kept.
    //
    // Also, the MemArray has the empty tag, regardless to what the input array has.
    //

    // TODO We need to create the beforeRedistribution array with the finalized schema.  Above
    // we sent around our partial metrics and computed the final chunk intervals.  Now we can
    // know the "finalizing transform", and we can create the beforeRedistribution array with
    // the correct schema.

    Attributes attrsBeforeRedistribution;
    attrsBeforeRedistribution.reserve(destAttrs.size());

    if (hasAggregate) {
        for (AttributeID i=0; i < safe_static_cast<AttributeID>(destAttrs.size()); ++i) {
            if (aggregates[i]) {
                attrsBeforeRedistribution.push_back(
                    AttributeDesc(i,
                                  destAttrs[i].getName(),
                                  aggregates[i]->getStateType().typeId(),
                                  destAttrs[i].getFlags(),
                                  destAttrs[i].getDefaultCompressionMethod()));
            } else {
                attrsBeforeRedistribution.push_back(destAttrs[i]);
            }
        }
    } else {
        attrsBeforeRedistribution = destAttrs;
    }

    SCIDB_ASSERT(finalDims == _schema.getDimensions());
    std::shared_ptr<MemArray> beforeRedistribution =
       std::make_shared<MemArray>(ArrayDesc(_schema.getName(),
                                            addEmptyTagAttribute(attrsBeforeRedistribution),
                                            _schema.getDimensions(),
                                            createDistribution(psUndefined),
                                            _schema.getResidency()
                                            ),
                                  query);

    // Write data from the 'redimensioned' array to the 'beforeRedistribution' array
    //

    // Initialize iterators
    //
    vector<std::shared_ptr<ArrayIterator> > arrayItersBeforeRedistribution(_arena,attrsBeforeRedistribution.size());
    vector<std::shared_ptr<ChunkIterator> > chunkItersBeforeRedistribution(_arena,attrsBeforeRedistribution.size());
    for (AttributeID i=0; i < safe_static_cast<AttributeID>(destAttrs.size()); ++i)
    {
        arrayItersBeforeRedistribution[i] = beforeRedistribution->getIterator(i);
    }
    vector< std::shared_ptr<ConstArrayIterator> > redimArrayConstIters(_arena,destAttrs.size() + 2);
    vector< std::shared_ptr<ConstChunkIterator> > redimChunkConstIters(_arena,destAttrs.size() + 2);
    for (AttributeID i = 0; i < safe_static_cast<AttributeID>(redimArrayConstIters.size()); ++i)
    {
        redimArrayConstIters[i] = redimensioned->getConstIterator(i);
    }

    // Initialize current chunk id to a value that is never in the map
    //
    size_t chunkIdAttr = redimArrayConstIters.size() - 1;
    size_t positionAttr = redimArrayConstIters.size() - 2;
    size_t nDestDims = _schema.getDimensions().size();
    size_t chunkId = arrayChunkIdMap->getUnusedId();

    // Coordinates outside of loops to reduce number of mallocs
    Coordinates lows(nDestDims);
    Coordinates intervals(nDestDims);
    Coordinates tmp(nDestDims);
    Coordinates outputCoord(nDestDims);

    // Init state vector and prev position
    StateVector stateVector(_arena,aggregates, 0);
    position_t prevPosition = -1;

    // Scan through the items, aggregate (if apply), and write to the MemArray.
    //
    vector<Value> destItem(_arena,destAttrs.size());  // moved outside inner loop to avoid repeated malloc
    while (!redimArrayConstIters[0]->end())
    {
        // Set up chunk iters for the input chunk
        for (size_t i = 0; i < redimChunkConstIters.size(); ++i)
        {
            redimChunkConstIters[i] = redimArrayConstIters[i]->getChunk().getConstIterator();
        }

        while (!redimChunkConstIters[0]->end())
        {
            // Have we found a new output chunk?
            //
            size_t nextChunkId = redimChunkConstIters[chunkIdAttr]->getItem().getInt64();
            // TODO Need to transform this /provisional/ (chunkId,chunkPos) pair into /final/ form!
            if (chunkId != nextChunkId)
            {
                // Write the left-over stateVector
                //
                appendItemToBeforeRedistribution(*arrayCoordinatesMapper,
                                                 lows, intervals, tmp,
                                                 prevPosition,
                                                 chunkItersBeforeRedistribution,
                                                 stateVector);

                // Flush current output iters
                //
                for (size_t i = 0; i < destAttrs.size(); ++i)
                {
                    if (chunkItersBeforeRedistribution[i].get())
                    {
                        chunkItersBeforeRedistribution[i]->flush();
                        chunkItersBeforeRedistribution[i].reset();
                    }
                }

                // Init the coordinate mapper for the new chunk
                //
                chunkId = nextChunkId;
                arrayCoordinatesMapper->chunkPos2LowsAndIntervals(arrayChunkIdMap->mapIdToChunkPos(chunkId),
                                                                  lows,
                                                                  intervals);

                // Create new chunks and get the iterators.
                // The first non-empty-tag attribute does NOT use NO_EMPTY_CHECK (so as to help
                // take care of the empty tag); others do.
                //
                int iterMode = ConstChunkIterator::SEQUENTIAL_WRITE;
                for (size_t i=0; i<destAttrs.size(); ++i)
                {
                    CoordinateCRange pos = arrayChunkIdMap->mapIdToChunkPos(chunkId);
                    tmp.assign(pos.begin(),pos.end());

                    Chunk& chunk = arrayItersBeforeRedistribution[i]->newChunk(tmp);
                    chunkItersBeforeRedistribution[i] = chunk.getIterator(query, iterMode);
                    iterMode |= ConstChunkIterator::NO_EMPTY_CHECK;
                }

                // Update prevPosition, reset state vector
                //
                prevPosition = -1;
                stateVector.init();
            }

            // When seeing the first item with a new position, the attribute values in the item are
            // populated into the destItem as follows.
            //  - For a scalar field, the value is copied.
            //  - For an aggregate field, the value is initialized and accumulated.
            //
            // When seeing subsequent items with the same position, the attribute values in the item
            // are populated as follows.
            //  - For a scalar field, the value is ignored (just select the first item).
            //  - For an aggregate field, the value is accumulated.
            //
            for (size_t i = 0; i < destAttrs.size(); ++i)
            {
                destItem[i] = redimChunkConstIters[i]->getItem();
            }

            position_t currPosition = redimChunkConstIters[positionAttr]->getItem().getInt64();
            if (currPosition == prevPosition)
            {
                if (!hasAggregate) {
                    if (redistributeMode==VALIDATED) {
                        arrayCoordinatesMapper->pos2coordWithLowsAndIntervals(lows, intervals, currPosition, outputCoord);
                        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_DATA_COLLISION)
                                                                    << CoordsToStr(outputCoord);
                    }
                    if (!_hasDataIntegrityIssue && logger->isWarnEnabled()) {
                        arrayCoordinatesMapper->pos2coordWithLowsAndIntervals(lows, intervals, currPosition, outputCoord);
                        LOG4CXX_WARN(logger, "RedimensionCommon::redimensionArray: "
                                     << "Data collision is detected at cell position "
                                     << CoordsToStr(outputCoord)
                                     <<  " for attribute ID = " << positionAttr
                                     << ". Add log4j.logger.scidb.array.RedimensionCommon=TRACE to the log4cxx config file for more");
                        _hasDataIntegrityIssue=true;
                    } else if (_hasDataIntegrityIssue && logger->isTraceEnabled()) {
                        arrayCoordinatesMapper->pos2coordWithLowsAndIntervals(lows, intervals, currPosition, outputCoord);
                        LOG4CXX_TRACE(logger, "RedimensionCommon::redimensionArray: "
                                      << "Data collision is detected at cell position "
                                      << CoordsToStr(outputCoord)
                                      <<  " for attribute ID = " << positionAttr);
                    }
                }
                stateVector.accumulate(destItem);
            }
            else
            {
                // Output the previous state vector.
                appendItemToBeforeRedistribution(*arrayCoordinatesMapper,
                                                 lows, intervals, tmp,
                                                 prevPosition,
                                                 chunkItersBeforeRedistribution,
                                                 stateVector);

                // record the new prevPosition
                prevPosition = currPosition;

                // Init and accumulate with the current item.
                stateVector.init();
                stateVector.accumulate(destItem);
            }

            // Advance chunk iterators
            for (size_t i = 0; i < redimChunkConstIters.size(); ++i)
            {
                ++(*redimChunkConstIters[i]);
            }
        } // while chunk iterator

        // Advance array iterators
        for (size_t i = 0; i < redimArrayConstIters.size(); ++i)
        {
            ++(*redimArrayConstIters[i]);
        }
    } // while array iterator

    arrayChunkIdMap->clear(); // ok, we're done with this now - release memory

    // Flush the leftover statevector
    appendItemToBeforeRedistribution(*arrayCoordinatesMapper,
                                     lows, intervals, tmp,
                                     prevPosition,
                                     chunkItersBeforeRedistribution,
                                     stateVector);

    // Flush the chunks one last time
    for (size_t i=0; i<destAttrs.size(); ++i)
    {
        if (chunkItersBeforeRedistribution[i].get())
        {
            chunkItersBeforeRedistribution[i]->flush();
        }
        chunkItersBeforeRedistribution[i].reset();
    }

    for (size_t i=0; i<destAttrs.size(); ++i) {
        arrayItersBeforeRedistribution[i].reset();
        chunkItersBeforeRedistribution[i].reset();
    }

    timing.logTiming(logger, "[RedimensionArray] PHASE 3: [aggregate] and build 'BeforeRedistribution'");

    // PHASE 4 redistribute

    // drop redimensioned
    redimChunkConstIters.clear();
    redimArrayConstIters.clear();
    redimensioned.reset();

    if( !hasAggregate && redistributeMode!=AGGREGATED) {
        SCIDB_ASSERT(!hasSynthetic);
        SCIDB_ASSERT(_schema.getResidency()->isEqual(beforeRedistribution->getArrayDesc().getResidency()));
        SCIDB_ASSERT(_schema.getDistribution()->checkCompatibility(beforeRedistribution->getArrayDesc().getDistribution()));

        // return without redistributing : optimizer will have to insert SG
        timing.logTiming(logger, "[RedimStore] PHASE 4: redistribution: non-aggregate early return");
        return beforeRedistribution;
    }
    SCIDB_ASSERT(redistributeMode!=VALIDATED);

    ArrayDesc outSchema(_schema.getName(),
                        _schema.getAttributes(),
                        _schema.getDimensions(),
                        _schema.getDistribution(),
                        _schema.getResidency());

    SCIDB_ASSERT(_schema.getResidency()->isEqual(query->getDefaultArrayResidency()));

    std::shared_ptr<Array> afterRedistribution;

    if ( hasSynthetic) {
        SyntheticDimChunkMerger::RedimInfo redimInfo(hasSynthetic,
                                                     safe_static_cast<AttributeID>(dimSynthetic),
                                                     finalDims[dimSynthetic]);
        std::shared_ptr<Array> input(beforeRedistribution);
        afterRedistribution = redistributeWithSynthetic(input, query, &redimInfo);
    } else {
        SCIDB_ASSERT(hasAggregate);
        const bool enforceDataIntegrity = (redistributeMode == VALIDATED);
        SCIDB_ASSERT(!enforceDataIntegrity);
        std::shared_ptr<Array> input(beforeRedistribution);

        afterRedistribution = redistributeWithAggregates(input,
                                                         outSchema,
                                                         query,
                                                         enforceDataIntegrity,
                                                         hasOverlap,
                                                         aggregates);
    }
    // drop beforeRedistribution
    chunkItersBeforeRedistribution.clear();
    arrayItersBeforeRedistribution.clear();
    beforeRedistribution.reset();

    timing.logTiming(logger, "[RedimStore] PHASE 4: redistribution: full redistribution");
    assert(outSchema == afterRedistribution->getArrayDesc());
    return afterRedistribution;
}

std::shared_ptr<Array>
RedimensionCommon::redistributeWithSynthetic(std::shared_ptr<Array>& inputArray,
                                             const std::shared_ptr<Query>& query,
                                             const SyntheticDimChunkMerger::RedimInfo* redimInfo)
{
    const ArrayDesc& desc = inputArray->getArrayDesc();
    size_t numAttrs = desc.getAttributes().size();
    PartialChunkMergerList chunkMergers(numAttrs);
    for (AttributeID a=0; a < numAttrs; ++a) {
        std::shared_ptr<MultiStreamArray::PartialChunkMerger> merger =
        std::make_shared<SyntheticDimChunkMerger>(redimInfo, query->getInstancesCount());
        chunkMergers[a] = merger;
    }

    SCIDB_ASSERT(_schema.getResidency()->isEqual(query->getDefaultArrayResidency()));

    // regardless of user settings there should be no data collisions with a synthetic dimension
    const bool enforceDataIntegrity = true;
    SCIDB_ASSERT(_schema.getResidency()->isEqual(query->getDefaultArrayResidency()));
    return redistributeToRandomAccess(inputArray,
                                      _schema.getDistribution(),
                                      ArrayResPtr(), //default query residency
                                      query,
                                      chunkMergers,
                                      enforceDataIntegrity );
}

/**
 * A partial chunk merger which uses an aggregate function to form a complete chunk.
 * It expects the partial chunks to contain aggreagte state values suitable for using with the Aggregate methods.
 */
class FinalAggregateChunkMerger : public AggregateChunkMerger
{
    const bool _hasOverlap;
    const ArrayDesc* _desc;
public:
    /// Constructor
    FinalAggregateChunkMerger(AggregatePtr const& agg,
                              const ArrayDesc*  desc,
                              bool isEmptyable,
                              bool hasOverlap)
    : AggregateChunkMerger(agg, isEmptyable), _hasOverlap(hasOverlap), _desc(desc) { assert(desc); }

    /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk
    virtual std::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                       const std::shared_ptr<Query>& query)
    {
        std::shared_ptr<MemChunk> chunk = AggregateChunkMerger::getMergedChunk(attId, query);
        std::shared_ptr<MemChunk> finalChunk = std::make_shared<MemChunk>();

        LOG4CXX_TRACE(RedimensionCommon::logger, "FinalAggregateChunkMerger::getMergedChunk: "
                      << "attId=" << attId
                      <<" old desc= " << chunk->getArrayDesc()
                      <<" new desc=" << (*_desc)
                      <<" pos=" << chunk->getFirstPosition(false));

        Address addr(chunk->getAttributeDesc().getId(), chunk->getFirstPosition(false));
        finalChunk->initialize(&chunk->getArray(),
                   _desc,
                   addr,
                   chunk->getCompressionMethod());

        // src chunk
        std::shared_ptr<ConstChunkIterator> src = chunk->getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);

        const size_t ebmSize(chunk->getBitmapSize());
        if (ebmSize>0) {
            const size_t off = chunk->getSize() - ebmSize;
            std::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap =
               std::make_shared<ConstRLEEmptyBitmap>(static_cast<char*>(chunk->getData()) + off);
            finalChunk->setEmptyBitmap(emptyBitmap);
        } else {
            ASSERT_EXCEPTION(false, "Merged chunk has no emptybitmap");
            // Technically, it is not a problem if finalChunk is not a "closure" (i.e. emptybitmap is not appended to the data)
            // and the result of redistribute(redimension()) is materialized (into a MemArray).
            // However, we would like it to also work without full array materialization.
        }

        // dest chunk
        int destMode = ConstChunkIterator::SEQUENTIAL_WRITE |
                       ConstChunkIterator::NO_EMPTY_CHECK |
                       ConstChunkIterator::APPEND_EMPTY_BITMAP;
        std::shared_ptr<ChunkIterator> dst = finalChunk->getIterator(query, destMode);

        // copy
        Value result;
        size_t count = 0;
        while (!src->end()) {
            ++ count;
            Coordinates const& destPos = src->getPosition();
            bool rc = dst->setPosition(destPos);
            SCIDB_ASSERT(rc);
            _aggregate->finalResult(result, src->getItem());
            dst->writeItem(result);
            ++(*src);
        }
        src.reset();
        dst->flush();
        dst.reset();

        finalChunk->setEmptyBitmap(std::shared_ptr<ConstRLEEmptyBitmap>());
        finalChunk->setBitmapChunk(NULL);
        assert(finalChunk->getBitmapSize()>0);

        if (!_hasOverlap) { // the count should not include overlapped items; just leave as 0.
            finalChunk->setCount(count);
        } else {
            finalChunk->setCount(0);
        }
        return finalChunk;
    }
};

class FinalETChunkMerger : public MultiStreamArray::DefaultChunkMerger
{
    const ArrayDesc *_desc;
public:
    /// Constructor
    FinalETChunkMerger(const ArrayDesc* desc,
                       bool enforceDataIntegrity)
    : DefaultChunkMerger(enforceDataIntegrity), _desc(desc) { assert(desc); }

    /// @see MultiStreamArray::PartialChunkMerger::getMergedChunk
    virtual std::shared_ptr<MemChunk> getMergedChunk(AttributeID attId,
                                                       const std::shared_ptr<Query>& query)
    {
        assert(attId == _desc->getAttributes().size()-1);
        std::shared_ptr<MemChunk> chunk = MultiStreamArray::DefaultChunkMerger::getMergedChunk(attId, query);
        LOG4CXX_TRACE(RedimensionCommon::logger, "FinalETChunkMerger::getMergedChunk: "
                      << "attId=" << attId
                      <<" old desc= " << chunk->getArrayDesc()
                      <<" new desc=" << (*_desc)
                      <<" pos=" << chunk->getFirstPosition(false));

        chunk->setArrayDesc(_desc);
        return chunk;
    }
};

std::shared_ptr<Array>
RedimensionCommon::redistributeWithAggregates(std::shared_ptr<Array>& inputArray,
                                              ArrayDesc const& outSchema,
                                              const std::shared_ptr<Query>& query,
                                              bool enforceDataIntegrity,
                                              bool hasOverlap,
                                              PointerRange<const AggregatePtr> aggregates)

{
    const ArrayDesc& desc = inputArray->getArrayDesc();
    const size_t numAttrs = desc.getAttributes().size();
    assert(numAttrs == (aggregates.size()+1));
    const bool isEmptyable = (desc.getEmptyBitmapAttribute() != NULL);

    PartialChunkMergerList chunkMergers(numAttrs);

    std::shared_ptr<Array> withAggregatesArray = make_shared<MemArray>(outSchema,query);

    for (AttributeID a=0; a < (numAttrs-1); ++a) {
        if (aggregates[a]) {
            std::shared_ptr<MultiStreamArray::PartialChunkMerger> merger =
            std::make_shared<FinalAggregateChunkMerger>(aggregates[a], &outSchema, isEmptyable, hasOverlap);
            chunkMergers[a] = merger;
        }
    }
    assert(chunkMergers.size() == numAttrs);
    assert(!chunkMergers[numAttrs-1]);
    chunkMergers[numAttrs-1] = make_shared<FinalETChunkMerger>(&outSchema, enforceDataIntegrity);

    redistributeToArray(inputArray,
                        withAggregatesArray,
                        chunkMergers,
                        NULL,
                        query,
                        enforceDataIntegrity );
     return withAggregatesArray;
}

/**
 * If any @c _schema dimension has an unspecified chunk interval, set up @c provDims with provisional chunk intervals.
 *
 * Per wiki:Development/components/Rearrange_Ops/RedimWithAutoChunkingHLD, here we detect whether we
 * are doing autochunking, and if so we "make up from thin air" provisional chunk intervals to
 * (temporarily) use in place of the unspecified ones.  These are stored in @c provDims .
 *
 * @return true iff we saw an unspecified interval (i.e. we are in autochunking mode)
 */
bool RedimensionCommon::makeProvisionalChunking(bool hasSynth, size_t synthIndex, Dimensions& provDims)
{
    provDims.clear();

    // Arbitrary, so keep it simple.  Make 'em look funny for easy identification while debugging.
    const int64_t PROVISIONAL_CHUNK_INTERVAL = 1012101;
    const int64_t PROVISIONAL_SYNTH_INTERVAL = 121;

    Dimensions const& destDims = _schema.getDimensions();
    for (size_t i = 0, n = destDims.size(); i < n; ++i) {
        DimensionDesc const& dim = destDims[i];
        int64_t chunkInterval = dim.getRawChunkInterval();
        if (chunkInterval == DimensionDesc::AUTOCHUNKED) {
            if (provDims.empty()) {
                // Late initialization, only do it if we must.
                provDims = destDims;
            }
            if (hasSynth && synthIndex == i) {
                provDims[i].setChunkInterval(PROVISIONAL_SYNTH_INTERVAL);
            } else {
                provDims[i].setChunkInterval(PROVISIONAL_CHUNK_INTERVAL);
            }
        }
    }

    return !provDims.empty();
}


/**
 *  Exchange and aggregate locally gathered shape metrics to obtain cluster-wide metrics.
 *
 *  @description We have seen the full coordinates of every locally produced output cell, and
 *  gathered metrics along each dimension: min, max, and approximate distinct count.  We also have
 *  local values for distinct count across *all* dimensions and an estimate of collisions along the
 *  synthetic dimension (if any).  Now we replicate these local findings, using the aggregation
 *  facility of SG, to come up with global values for these metrics.  The global metrics will be fed
 *  to the ChunkEstimator to arrive at reasonable, automatically computed chunk interval values.
 *
 *  We exchange a 1-D array containing the metrics.  Attributes and shape of the 1-D metrics array
 *  we will replicate:
 *
 *    dmin/AI_MIN   dmax/AI_MAX  dcnt/AI_ADC      ovdc/AI_ODC      cols/AI_COL
 *    ------------------------------------------------------------------------
 *    <minState0,   maxState0,   approxdcState0,  overalldcState,  collisionState> [0]
 *    <minState1,   maxState1,   approxdcState1,  (dcIdentity),    (zero)> [1]
 *    ...           ...          ...              ...              ...
 *    <minStateN,   maxStateN,   approxdcStateN,  (dcIdentity),    (zero)> [N]
 */
void RedimensionCommon::exchangeMetrics(std::shared_ptr<Query> const& query,
                                        CoordMetrics const& coordMetrics,
                                        ssize_t synthDim,
                                        Dimensions& inOutDims)
{
    size_t const N_DEST_DIMS = _schema.getDimensions().size();
    SCIDB_ASSERT(coordMetrics.size() == N_DEST_DIMS);
    SCIDB_ASSERT(inOutDims.size() == N_DEST_DIMS);

    size_t const N_INSTS = _schema.getResidency()->size();
    SCIDB_ASSERT(query->getInstanceID() < N_INSTS);
    SCIDB_ASSERT(query->getInstancesCount() == N_INSTS);

    // The redistribute call below will aggregate attributes when there are collisions, so by making
    // all instances' metrics cells collide along the "destDim" dimension, we'll get the aggregated
    // values that the ChunkEstimator wants.  Cool!
    //
    // We build the aggregates vector first, so that we can use the correct "state types" in the
    // attributes list.
    //
    std::vector<AggregatePtr> aggregates(AI_MAX_ATTRS - 1); // no aggregate for empty bitmap
    AggregateLibrary* aggLib = AggregateLibrary::getInstance();
    // Second arg to createAggregate() is its input type.
    // Note: When aggregating cluster-wide, AI_COL aggregate is sum(), *not* _maxrepcnt().
    aggregates[AI_MIN] = aggLib->createAggregate("min", TypeLibrary::getType(TID_INT64));
    aggregates[AI_MAX] = aggLib->createAggregate("max", TypeLibrary::getType(TID_INT64));
    aggregates[AI_ADC] = aggLib->createAggregate("approxdc", TypeLibrary::getType(TID_INT64));
    aggregates[AI_ODC] = aggLib->createAggregate("approxdc", TypeLibrary::getType(TID_BINARY));
    aggregates[AI_COL] = aggLib->createAggregate("sum", TypeLibrary::getType(TID_UINT64));

    // Build MemArray to hold metric states.
    int16_t const NULLABLE = AttributeDesc::IS_NULLABLE; // Chunk::aggregateMerge() insists
    Attributes attrs = boost::assign::list_of
        (AttributeDesc(AI_MIN, "dmin", aggregates[AI_MIN]->getStateType().typeId(), NULLABLE, 0))
        (AttributeDesc(AI_MAX, "dmax", aggregates[AI_MAX]->getStateType().typeId(), NULLABLE, 0))
        (AttributeDesc(AI_ADC, "dcnt", aggregates[AI_ADC]->getStateType().typeId(), NULLABLE, 0))
        (AttributeDesc(AI_ODC, "ovdc", aggregates[AI_ODC]->getStateType().typeId(), NULLABLE, 0))
        (AttributeDesc(AI_COL, "cols", aggregates[AI_COL]->getStateType().typeId(), NULLABLE, 0))
        (AttributeDesc(AI_EBM, DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
                       TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0));
    SCIDB_ASSERT(AI_MAX_ATTRS == attrs.size());

    // There are N_DEST_DIMS entries.  The "overall" metric attributes "ovdc" and "cols" are only
    // set for the zero-th cell (all other cells use the identity for the particular aggregate).
    Dimensions dims = boost::assign::list_of
        (DimensionDesc("destDim", 0, N_DEST_DIMS-1, N_DEST_DIMS, 0));

    ArrayDesc metricsInDesc("_metrics", attrs, dims, _schema.getDistribution(), _schema.getResidency());
    std::shared_ptr<MemArray> metrics = std::make_shared<MemArray>(metricsInDesc, query);

    // Load MemArray with local metric state.  First, get array write-iterators.
    vector<std::shared_ptr<ArrayIterator> > outAIters(AI_MAX_ATTRS);
    for (AttributeID aid = 0; aid < AI_MAX_ATTRS; ++aid) {
        outAIters[aid] = metrics->getIterator(aid);
    }

    // Now get chunk write-iterators.
    Coordinates currPos(1);
    assert(currPos[0] == 0);
    unsigned chunkMode = ChunkIterator::SEQUENTIAL_WRITE;
    vector<std::shared_ptr<ChunkIterator> > outCIters(AI_MAX_ATTRS);
    for (AttributeID aid = 0; aid < AI_MAX_ATTRS; ++aid) {
        Chunk& chunk = outAIters[aid]->newChunk(currPos);
        outCIters[aid] = chunk.getIterator(query, chunkMode);
        chunkMode |= ChunkIterator::NO_EMPTY_CHECK;
    }

    // Some values we need...
    Value emptyBitmapValue;
    emptyBitmapValue.setBool(true);
    Value zero;
    zero.setUint64(0UL);                // Identity for sum()

    // Fill in all local metric state.
    for (size_t i = 0, n = coordMetrics.size(); i < n; ++i) {
        outCIters[AI_MIN]->writeItem(coordMetrics.getState(i, AI_MIN));
        outCIters[AI_MAX]->writeItem(coordMetrics.getState(i, AI_MAX));
        outCIters[AI_ADC]->writeItem(coordMetrics.getState(i, AI_ADC));
        outCIters[AI_ODC]->writeItem(coordMetrics.getState(i, AI_ODC));
        if (i == 0) {
            outCIters[AI_COL]->writeItem(coordMetrics.getFinalResult(i, AI_COL));
        } else {
            outCIters[AI_COL]->writeItem(zero);
        }

        // Write empty bitmap attribute and bump all chunk iterators.
        outCIters[AI_EBM]->writeItem(emptyBitmapValue);
        for (auto& cIter : outCIters) {
            ++(*cIter);
        }

    }
    for (auto& cIter : outCIters) {
        cIter->flush();
        cIter->reset();
    }
    for (auto& aIter : outAIters) {
        aIter->reset();
    }

    // The redistribute call needs an output schema slightly modified from the metricsInDesc input
    // schema to (a) specify replication and (b) use the aggregates' *output* types rather than
    // their state types.
    //
    Attributes outAttrs = boost::assign::list_of
        (AttributeDesc(AI_MIN, "dmin", aggregates[AI_MIN]->getResultType().typeId(), NULLABLE, 0))
        (AttributeDesc(AI_MAX, "dmax", aggregates[AI_MAX]->getResultType().typeId(), NULLABLE, 0))
        (AttributeDesc(AI_ADC, "dcnt", aggregates[AI_ADC]->getResultType().typeId(), NULLABLE, 0))
        (AttributeDesc(AI_ODC, "ovdc", aggregates[AI_ODC]->getResultType().typeId(), NULLABLE, 0))
        (AttributeDesc(AI_COL, "cols", aggregates[AI_COL]->getResultType().typeId(), NULLABLE, 0))
        (AttributeDesc(AI_EBM, DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,
                       TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0));
    ArrayDesc metricsOutDesc(metricsInDesc.getName(),
                             outAttrs,
                             metricsInDesc.getDimensions(),
                             createDistribution(psReplication),
                             _schema.getResidency());

    // Replicate metrics, aggregating them as we go.
    std::shared_ptr<Array> tmp = metrics;
    std::shared_ptr<Array> allMetricsArray =
        redistributeWithAggregates(tmp,
                                   metricsOutDesc,
                                   query,
                                   true, // enforceDataIntegrity
                                   false, // hasOverlap
                                   aggregates);

    // Pull all metrics.

    // First, get hold of attribute iterators.
    std::shared_ptr<ConstArrayIterator> rdArrayIterators[AI_MAX_ATTRS];
    std::shared_ptr<ConstChunkIterator> rdChunkIterators[AI_MAX_ATTRS];
    for (AttributeID i = 0; i < attrs.size(); ++i) {
        rdArrayIterators[i] = allMetricsArray->getConstIterator(i);
    }

    // This is a full-blown multi-chunk iteration loop, but really we know to only expect one chunk.
    ChunkEstimator estimator(inOutDims, ChunkEstimator::forRedim_t());
    estimator.setLogger(logger);
    while (!rdArrayIterators[0]->end()) {

        // Get chunk iterators...
        for (size_t i = 0; i < AI_MAX_ATTRS; ++i) {
            ConstChunk const& chunk = rdArrayIterators[i]->getChunk();
            rdChunkIterators[i] = chunk.getConstIterator();
        }

        // Read back (the one and only) chunk of per-dimension tuples.
        for (int64_t dimNum = 0; !rdChunkIterators[0]->end(); ++dimNum) {

            SCIDB_ASSERT(rdChunkIterators[0]->getPosition()[0] == dimNum);

            // Unrolled attribute loop.
            ChunkEstimator::Statistics stats;
            stats[ChunkEstimator::minimum] = rdChunkIterators[AI_MIN]->getItem();
            stats[ChunkEstimator::maximum] = rdChunkIterators[AI_MAX]->getItem();
            stats[ChunkEstimator::distinct] = rdChunkIterators[AI_ADC]->getItem();
            if (dimNum == 0) {
                // This row contains the statistics not tied to any particular dimension.
                estimator.setOverallDistinct(rdChunkIterators[AI_ODC]->getItem().getInt64());
                if (synthDim >= 0) {
                    estimator.setSyntheticInterval(synthDim, rdChunkIterators[AI_COL]->getItem().getUint64());
                } else {
                    // No synthetic dimension.  If there *were* collisions, we don't care---it's
                    // just an estimate and errs on the high side.
                    uint64_t nCollisions = rdChunkIterators[AI_COL]->getItem().getUint64();
                    if (nCollisions) {
                        LOG4CXX_DEBUG(logger, "" << __FUNCTION__ << ": Estimated " << nCollisions
                                      << " collisions but there is no synthetic dimension.");
                    }
                }
            }
            estimator.addStatistics(stats);

            for (auto& cIter : rdChunkIterators) {
                ++(*cIter);
            }
        }

        // Bump the array iterators to get the next set of chunks.
        for (size_t i = 0; i < AI_MAX_ATTRS; ++i) {
            ++(*rdArrayIterators[i]);
        }
    }

    // Finally, estimate the chunk size(s).
    estimator.go();
}

} // namespace scidb
