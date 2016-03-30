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
 * @file CoordMetrics.h
 *
 * @description A container for accumulating local dimension metrics,
 * both per-dimension and overall.
 */

#ifndef COORD_METRICS_H
#define COORD_METRICS_H

#include "RedimensionCommon.h"

#include <array/Coordinate.h>
#include <query/Aggregate.h>
#include <system/Exceptions.h>

namespace scidb {

/**
 * Indeces of attributes in an autochunking metric exchange array.
 *
 * @note WARNING, the CoordMetrics class is intimately dependent
 * on the ordering of this enum.
 */
enum MetricAttrIndex {
    AI_MIN,                 // Min of local coordinate in this dimension
    AI_MAX,                 // Max of local coordinate in this dimension
    AI_ADC,                 // Approxdc of local etc.
    AI_MAX_PER_DIM_ATTRS,   // Count of per-dimension attributes above
    AI_ODC = AI_MAX_PER_DIM_ATTRS, // Overall distinct count (not per-dim)
    AI_COL,                        // Overall collision count (not per-dim)
    AI_EBM,                 // Empty bitmap attribute
    AI_MAX_ATTRS            // MUST be last
};
/**
 * Specialized container for holding coordinate metrics.
 */
class CoordMetrics {
public:

    /// Assures contiguous Value data for hashing and distinct counts.
    struct PositionPair {
        position_t pos;
        position_t chunkId;
    };

    /**
     * Construct a container to hold metrics for nDims dimensions (plus overall metrics).
     *
     * @param nDims             number of dimensions
     * @param wantMaxRepeats    true iff we care about collisions, that is, how often a PositionPair is repeated
     */
    CoordMetrics(size_t nDims, bool wantMaxRepeats);

    /// Accumulate @c c for all per-dimension metrics of dimension @c dimNum .
    void        accumulate(size_t dimNum, Coordinate c);

    // Accumulate (pos,id) pair for overall (non-dimensional) metrics.
    void        accumulate(PositionPair const& pp);

    /**
     *  Return the intermediate state of the desired metric.
     *
     *  @note The overall metrics AI_ODC and AI_COL only return their true states for dimension
     *  zero.  When other dimensions are requested, an appropriate identity value is returned.
     */
    Value const& getState(size_t dim, MetricAttrIndex which) const;

    /**
     * Compute the final result for the metric at given dimension of given type.
     */
    Value getFinalResult(size_t dim, MetricAttrIndex which) const;

    size_t size() const { return _N_DIMS; }

private:
    size_t const    _N_DIMS;
    size_t const    _N_AGGS_PER_DIM;
    bool            _wantRepeats;
    Value           _coordValue; // Allocate once to avoid frequent reallocation
    Value           _ppValue;    // Ditto

    struct AggState {
        AggregatePtr agg;
        Value        state;
    };
    std::vector<AggState> _aggregates;

    size_t indexOf(size_t dim, MetricAttrIndex aid) const
    {
        SCIDB_ASSERT(dim < _N_DIMS);
        SCIDB_ASSERT(size_t(aid) < _N_AGGS_PER_DIM);
        return (dim * _N_AGGS_PER_DIM) + aid;
    }
};

} // namespace scidb

#endif /* COORD_METRICS_H */
