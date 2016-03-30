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
 * @file ChunkEstimator.h
 *
 * @description Common chunk size computation logic for CreateArrayUsing and for autochunking
 * redimension().  The meat of this code was originally written by Jonathon based on a prototype by
 * Donghui.  Wrapped up inside a nice, warm class by MJL.
 *
 * @author Jonathon Bell <jbell@paradigm4.com>
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#ifndef CHUNK_ESTIMATOR_H
#define CHUNK_ESTIMATOR_H

#include <array/Metadata.h>

#include <array>
#include <vector>

namespace scidb {

class ChunkEstimator
{
public:

    enum Statistic
    {
        loBound,
        hiBound,
        interval,
        overlap,
        minimum,
        maximum,
        distinct
    };

    typedef std::array<Value,distinct+1> Statistics;
    typedef std::vector<Statistics>      StatsVector;
    struct forRedim_t {};

    /// Constructor used by create_array_using
    ChunkEstimator(Dimensions& inOutDims);

    /// Constructor used by redimension().  Indicates that the statistics vector will need some
    /// pre-processing to set it up in accordance with @c dims .
    ChunkEstimator(Dimensions& inOutDims, forRedim_t const&);

    /// Setters, "named parameter idiom"
    /// @{
    ChunkEstimator& setOverallDistinct(uint64_t v)         { _overallDistinctCount = v; return *this; }
    ChunkEstimator& setTargetCellCount(uint64_t v)         { _desiredValuesPerChunk = v; return *this; }
    ChunkEstimator& setSyntheticInterval(ssize_t dimNum, uint64_t nCollisions);
    ChunkEstimator& setAllStatistics(StatsVector const& v) { _statsVec = v; return *this; }
    ChunkEstimator& addStatistics(Statistics const& stats) { _statsVec.push_back(stats); return *this; }
    ChunkEstimator& setLogger(log4cxx::LoggerPtr lp)       { _logger = lp; return *this; }
    /// @}

    /// Getters, for benefit of operator<<()
    /// @{
    StatsVector const& getAllStatistics() const { return _statsVec; }
    uint64_t    getCollisionCount() const { return _collisionCount; }
    uint64_t    getOverallDistinct() const { return _overallDistinctCount; }
    uint64_t    getTargetCellCount() const { return _desiredValuesPerChunk; }
    /// @}

    /// When all parameters are loaded, go compute estimates and fill in the [in,out] dimensions.
    void go();

private:
    void inferStatsFromSchema();

    Dimensions&     _dims;
    uint64_t        _overallDistinctCount;
    uint64_t        _collisionCount;
    uint64_t        _desiredValuesPerChunk;
    bool            _inRedim;
    StatsVector     _statsVec;
    log4cxx::LoggerPtr _logger;
};

std::ostream& operator<< (std::ostream& os, ChunkEstimator::Statistics const&);
std::ostream& operator<< (std::ostream& os, ChunkEstimator const&);

} // namespace scidb

#endif /* ! CHUNK_ESTIMATOR_H */
