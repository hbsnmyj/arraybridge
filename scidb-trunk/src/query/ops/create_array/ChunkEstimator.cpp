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
 * @file ChunkEstimator.cpp
 *
 * @description Common chunk size computation logic for CreateArrayUsing and for autochunking
 * redimension().  The meat of this code was originally written by Jonathon based on a prototype by
 * Donghui.  Wrapped up inside a nice, warm class by MJL.
 *
 * @author Jonathon Bell <jbell@paradigm4.com>
 * @author Mike Leibensperger <mjl@paradigm4.com>
 */

#include "ChunkEstimator.h"

#include <system/Config.h>

#include <sstream>
#include <string>

using namespace std;

namespace scidb {

ChunkEstimator::ChunkEstimator(Dimensions& dims)
    : _dims(dims)
    , _overallDistinctCount(0UL)
    , _collisionCount(0UL)
    , _desiredValuesPerChunk(0UL)
    , _inRedim(false)
{ }

ChunkEstimator::ChunkEstimator(Dimensions& dims, ChunkEstimator::forRedim_t const&)
    : _dims(dims)
    , _overallDistinctCount(0UL)
    , _collisionCount(0UL)
    , _desiredValuesPerChunk(0UL)
    , _inRedim(true)
{ }

ChunkEstimator& ChunkEstimator::setSyntheticInterval(ssize_t dimNum, uint64_t nCollisions)
{
    SCIDB_ASSERT(dimNum >= 0);

    if (!_dims[dimNum].isAutochunked()) {
        LOG4CXX_TRACE(_logger, "Synthetic interval length estimate ignored");
        return *this;
    }

    if (nCollisions == 0) {
        // They did ask for a synthetic dimension after all.
        nCollisions = 1;
    }

    Config* cfg = Config::getInstance();
    uint64_t maxSynthInterval = 
        cfg->getOption<uint64_t>(CONFIG_AUTOCHUNK_MAX_SYNTHETIC_INTERVAL);
    if (nCollisions > maxSynthInterval) {
        if (_logger) {
            LOG4CXX_WARN(_logger, "Estimated " << nCollisions
                         << " collisions, but capping synthetic interval at "
                         << maxSynthInterval
                         << " (autochunk-max-synthetic-interval)");
        }
        nCollisions = maxSynthInterval;
    }

    // Leave startMin alone.
    int64_t startMin = _dims[dimNum].getStartMin();
    _dims[dimNum].setEndMax(startMin + nCollisions - 1);
    _dims[dimNum].setChunkInterval(nCollisions);
    _dims[dimNum].setChunkOverlap(0);

    return *this;
}

/**
 * The first four Statistics elements (loBound,hiBound,interval,overlap)
 * are bool Values that are true iff the corresponding value is
 * pegged... so, false if the value is to be inferred by the go()
 * routine.  These bools are set up automatically by CreateArrayUsing,
 * but for redimension we have to generate them appropriately.
 */
void ChunkEstimator::inferStatsFromSchema()
{
    assert(_inRedim);
    assert(_statsVec.size() == _dims.size());

    if (_logger) {
        LOG4CXX_DEBUG(_logger, "" << __PRETTY_FUNCTION__ << ": " << *this);
    }

    for (size_t i = 0, n = _dims.size(); i < n; ++i) {
        _statsVec[i][loBound].setBool(true);
        _statsVec[i][hiBound].setBool(true);
        _statsVec[i][interval].setBool(!_dims[i].isAutochunked());
        _statsVec[i][overlap].setBool(true);
    }
}

/**
 *  Round the proposed chunk interval to the nearest power of two,  where
 *  'nearest' means that the base two logarithm is rounded to the nearest
 *  integer.
 */
static int64_t roundLog2(int64_t ci)
{
    assert(ci != 0);                                 // Validate argument
    return static_cast<int64_t>(pow(2.0,round(log2(static_cast<double>(ci)))));
}

void ChunkEstimator::go()
{
    using std::max;
    using std::min;

    // First, are we ready?
    SCIDB_ASSERT(!_dims.empty());
    SCIDB_ASSERT(_dims.size() == _statsVec.size());
    if (!_desiredValuesPerChunk) {
        Config* cfg = Config::getInstance();
        _desiredValuesPerChunk =
            cfg->getOption<uint64_t>(CONFIG_REDIM_TARGET_CELLS_PER_CHUNK);
        if (!_desiredValuesPerChunk) {
            _desiredValuesPerChunk = 1000000UL;
        }
    }
    if (_inRedim) {
        inferStatsFromSchema();
    }

    if (_logger) {
        LOG4CXX_DEBUG(_logger, "" << __PRETTY_FUNCTION__ << ": " << *this);
    }

    size_t         numChunksFromN(max(_overallDistinctCount / _desiredValuesPerChunk,1UL));
    size_t                      N(0);            // Inferred intervals
    Dimensions::iterator        d(_dims.begin());// Dimension iterator
    int64_t                remain(CoordinateBounds::getMaxLength());// Cells to play with

    for (Statistics& s : _statsVec)                  // For each dimension
    {
        if (!s[interval].getBool())                  // ...infer interval?
        {
            N += 1;                                  // ....seen another
        }
        else                                         // ...user specified
        {
            numChunksFromN *= d->getChunkInterval();
            numChunksFromN /= s[distinct].getInt64();

            remain /= (d->getChunkInterval() + d->getChunkOverlap());
        }

        ++d;
    }

    double const chunksPerDim = max(
        pow(static_cast<double>(numChunksFromN),1.0/static_cast<double>(N)),
        1.0);

    d = _dims.begin();                               // Reset dim iterator

    for (Statistics& s : _statsVec)                  // For each dimension
    {
        if (!s[loBound].getBool())                   // ...infer loBound?
        {
            d->setStartMin(s[minimum].getInt64());   // ....assign default
        }

        if (!s[hiBound].getBool())                   // ...infer hiBound?
        {
            d->setEndMax(s[maximum].getInt64());     // ....assign default
        }

        if (!s[overlap].getBool())                   // ...infer overlap?
        {
            d->setChunkOverlap(0);                   // ....assign default
        }

        if (!s[interval].getBool())                   // ...infer interval?
        {
            int64_t hi = s[maximum].getInt64();      // ....highest actual
            int64_t lo = s[minimum].getInt64();      // ....lowest  actual
            int64_t ci  = static_cast<int64_t>(
                static_cast<double>((hi - lo + 1))/chunksPerDim);     // ....chunk interval

            ci = max(ci,1L);                         // ....clamp at one
            ci = roundLog2(ci);                      // ....nearest power
            ci = min(ci,remain);                     // ....clamp between
            ci = max(ci,1L);                         // ....one and remain

            d->setChunkInterval(ci);                 // ....update schema

            remain /= (d->getChunkInterval() + d->getChunkOverlap());
        }

        ++d;                                         // ...next dimension
    }
    
}

static const char* bool_or_null(Value const& v)
{
    return v.isNull() ? "null" : v.getBool() ? "T" : "F";
}

static string int64_or_null(Value const& v)
{
    if (v.isNull()) {
        return "null";
    } else {
        stringstream ss;
        ss << v.getInt64();
        return ss.str();
    }
}

std::ostream& operator<< (std::ostream& os, ChunkEstimator::Statistics const& stats)
{
    os << "stat([" << bool_or_null(stats[ChunkEstimator::loBound])
       << ',' << bool_or_null(stats[ChunkEstimator::hiBound])
       << ',' << bool_or_null(stats[ChunkEstimator::interval])
       << ',' << bool_or_null(stats[ChunkEstimator::overlap])
       << "],min=" << int64_or_null(stats[ChunkEstimator::minimum])
       << ",max=" << int64_or_null(stats[ChunkEstimator::maximum])
       << ",dc=" << int64_or_null(stats[ChunkEstimator::distinct])
       << ')';
    return os;
}

std::ostream& operator<< (std::ostream& os, ChunkEstimator const& ce)
{
    os << "ChunkEstimator(odc=" << ce.getOverallDistinct()
       << ",target=" << ce.getTargetCellCount()
       << ",coll=" << ce.getCollisionCount()
       << ",stats=[";
    for (auto const& s : ce.getAllStatistics()) {
        os << "\n  " << s;
    }
    os << "])";
    return os;
}

} // namespace scidb
