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
 * @file CoordMetrics.cpp
 */

#include "CoordMetrics.h"
#include "RedimensionCommon.h"

namespace scidb {

CoordMetrics::CoordMetrics(size_t nDims, bool wantMaxRepeats)
    : _N_DIMS(nDims)
    , _N_AGGS_PER_DIM(AI_MAX_ATTRS - 1) // -1: no aggregate for empty bitmap attribute
    , _wantRepeats(wantMaxRepeats)
    , _coordValue(sizeof(Coordinate))
    , _ppValue(sizeof(CoordMetrics::PositionPair))
    , _aggregates(_N_DIMS * _N_AGGS_PER_DIM)
{
    // The min() and max() aggregates need to accumulate at least one value.  Otherwise if the local
    // 1-D array is empty, they'll yield null Values to the ChunkEstimator, which won't be happy.
    Value big, small;
    big.setInt64(std::numeric_limits<int64_t>::max());
    small.setInt64(std::numeric_limits<int64_t>::min());

    // Initialize all the aggregates.
    AggregateLibrary* agLib = AggregateLibrary::getInstance();
    AggState* asp;
    for (size_t dim = 0; dim < nDims; ++dim) {
        asp = &_aggregates[indexOf(dim, AI_MIN)];
        asp->agg = agLib->createAggregate("min", TypeLibrary::getType(TID_INT64));
        asp->agg->initializeState(asp->state);
        asp->agg->accumulateIfNeeded(asp->state, big);

        asp = &_aggregates[indexOf(dim, AI_MAX)];
        asp->agg = agLib->createAggregate("max", TypeLibrary::getType(TID_INT64));
        asp->agg->initializeState(asp->state);
        asp->agg->accumulateIfNeeded(asp->state, small);

        asp = &_aggregates[indexOf(dim, AI_ADC)];
        asp->agg = agLib->createAggregate("approxdc", TypeLibrary::getType(TID_INT64));
        asp->agg->initializeState(asp->state);

        asp = &_aggregates[indexOf(dim, AI_ODC)];
        asp->agg = agLib->createAggregate("approxdc", TypeLibrary::getType(TID_BINARY));
        asp->agg->initializeState(asp->state);

        asp = &_aggregates[indexOf(dim, AI_COL)];
        asp->agg = agLib->createAggregate("_maxrepcnt", TypeLibrary::getType(TID_BINARY));
        asp->agg->initializeState(asp->state);
    }
}

void CoordMetrics::accumulate(size_t dim, Coordinate c)
{
    _coordValue.setInt64(c);
    AggState* asp = &_aggregates[indexOf(dim, AI_MIN)];
    for (AttributeID aid = AI_MIN; aid < AI_MAX_PER_DIM_ATTRS; ++aid, ++asp) {
        asp->agg->accumulateIfNeeded(asp->state, _coordValue);
    }
}

void CoordMetrics::accumulate(CoordMetrics::PositionPair const& pp)
{
    SCIDB_ASSERT(_ppValue.size() == sizeof(PositionPair));
    ::memcpy(_ppValue.data(), &pp, sizeof(PositionPair));

    AggState* asp = &_aggregates[indexOf(0, AI_ODC)];
    asp->agg->accumulateIfNeeded(asp->state, _ppValue);
    if (_wantRepeats) {
        asp = &_aggregates[indexOf(0, AI_COL)];
        asp->agg->accumulateIfNeeded(asp->state, _ppValue);
    }
}

Value const& CoordMetrics::getState(size_t dim, MetricAttrIndex aid) const
{
    return _aggregates[indexOf(dim, aid)].state;
}

Value CoordMetrics::getFinalResult(size_t dim, MetricAttrIndex which) const
{
    Value result;
    AggState const* asp = &_aggregates[indexOf(dim, which)];
    asp->agg->finalResult(result, asp->state);
    return result;
}

} // namespace scidb
