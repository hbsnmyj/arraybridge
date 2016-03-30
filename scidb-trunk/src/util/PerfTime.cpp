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

/**
 * @file PerfTime.cpp
 *
 * @brief Implementation of PerfTime.h
 */

#include <atomic>
#include <cmath>

#include <query/Query.h>
#include <util/PerfTime.h>

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.logger"));

namespace scidb
{

double perfTimeGetElapsed() noexcept
{
    try {
        struct timeval tv;
        auto result = ::gettimeofday(&tv, NULL);
        assert(result==0);  // never fails in practice
        return double(tv.tv_sec) + double(tv.tv_usec) * 1.0e-6;         // slow, initial implementation
    }
    catch (...){;} // timing should not change the code path above

    return NAN; // NaN will keep things going while still invalidating the timing
}

double perfTimeGetCPU() noexcept
{
    try {
        struct rusage rUsage;
        auto result = ::getrusage(RUSAGE_THREAD, &rUsage);
        assert(result==0);  // never fails in practice

        struct timeval cpuSum;
        timeradd(&rUsage.ru_utime, &rUsage.ru_stime, &cpuSum);      // linux macro
        return double(rUsage.ru_utime.tv_sec) +
               double(rUsage.ru_utime.tv_usec) * 1.0e-6 +
               double(rUsage.ru_stime.tv_sec) +
               double(rUsage.ru_stime.tv_usec) * 1.0e-6;
    }
    catch (...){;} // timing should not change the code path above
    return NAN; // NaN will keep things going while still invalidating the timing
}

const char* tcString(const perfTimeCategory_t tc)
{
    switch(tc) {
    case PTC_ACTIVE:    return "PTC_ACTIVE" ;
    case PTC_CPU:    return "PTC_CPU" ;
    case PTCW_PG:    return "PTCW_PG" ;
    //case PTCW_LCK:    return "PTCW_LCK" ;
    case PTCW_EXT:    return "PTCW_EXT" ;
    case PTCW_FS_RD:    return "PTCW_FS_RD" ;
    case PTCW_FS_WR:    return "PTCW_FS_WR" ;
    case PTCW_FS_WR_SYNC:    return "PTCW_FS_WR_SYNC" ;
    case PTCW_FS_FL:    return "PTCW_FS_FL" ;
    //case PTCW_SG_SND:    return "PTCW_SG_SND" ;
    case PTCW_SG_RCV:    return "PTCW_SG_RCV" ;
    case PTCW_NET_SND:    return "PTCW_NET_SND" ;
    case PTCW_NET_RCV:    return "PTCW_NET_RCV" ;
    case PTCW_BAR:       return "PTCW_BAR";
    case PTCW_REP:       return "PTCW_REP";
    case PTCW_MUT_OTHER:    return "PTCW_MUT_OTHER" ;
    case PTCW_SEM_OTHER:     return "PTCW_SEM_OTHER" ;
    case PTCW_EVENT_OTHER:    return "PTCW_EVENT_OTHER" ;
    default:                 return "UNKNOWN";
    }
}

void logNoqueryWaits(const perfTimeCategory_t tc, const double sec)
{
    LOG4CXX_DEBUG(logger, "perfTimeAdd: didn't log " << tcString(tc) << " " << sec);
}

void perfTimeAdd(const perfTimeCategory_t tc, const double sec)
{
    const int DEBUG_VERBOSE=0;
    const int DEBUG_EXTRA_VERBOSE=0;

    std::shared_ptr<Query> query = Query::getQueryPerThread();
    if(query) {
        if(DEBUG_VERBOSE) {
            if(tc == PTC_CPU || tc == PTC_ACTIVE) {
                LOG4CXX_DEBUG(logger, "perfTimeAdd: " << query.get() << " " << tcString(tc) << " " << sec);
            }
        }
        query->perfTimeAdd(tc, sec);
    } else if (tc == PTC_CPU || tc == PTC_ACTIVE) {
        // these are done explicitly at the top level,
        // so they should not happen without query being set
        // unless there is an error in the code
        LOG4CXX_ERROR(logger, "perfTimeAdd: didn't log " << tcString(tc) << " " << sec);
    } else if (DEBUG_EXTRA_VERBOSE) {
        logNoqueryWaits(tc, sec);
    }
}

thread_local std::atomic_uint_fast32_t ScopedWaitTimer::_nestingDepth(0);

ScopedWaitTimer::ScopedWaitTimer(perfTimeCategory_t tc, float weight) noexcept
:
    _secStartElapsed(perfTimeGetElapsed()),
    _secStartCPU(perfTimeGetCPU()),
    _tc(tc),
    _weight(weight)
{
    try {
        assert(_nestingDepth.is_lock_free()); // assumption that_nestingDepth is fast
        assert(tc != PTC_CPU && tc != PTC_ACTIVE); // only correct for PTCW_* (wait categories)

        static int NESTING_ALLOWED=1 ; // changed to 0 to debug unintended nesting
                                       // TODO: this may become a ctor parameter
                                       //   so that "top-level" ScopedWaitTimer won't be
                                       //   accidentally nested, but its use inside
                                       //   semaphore, event, etc can be more permissive
        if (!NESTING_ALLOWED) {
            assert(_nestingDepth==0);       // check that there is only one object at a time per thread
                                            // to avoid nesting these on the stack, which would
                                            // result in over-measurement.
        }
        _nestingDepth++ ;
    }
    catch (...){;} // timing should not change the code path above
}

ScopedWaitTimer::~ScopedWaitTimer() noexcept
{
    // note: surround CPU mesurement with elapsed measurement
    //       so that secBlocked has less of a chance
    //       of being negative.  This does introduce
    //       an positive bias of elapsed vs cpu time
    //       but tends to keep secBlocked from going negative
    //       as much, which is a little less astonishing
    //       This is subject to change as we learn more
    //       about the clocks involved and their relative
    //       precisions
    try {
        assert(_nestingDepth > 0);

        if(_nestingDepth==1) {   // only the outermost nesting does the timing
            auto secCPU =     perfTimeGetCPU()     - _secStartCPU;
            auto secElapsed = perfTimeGetElapsed() - _secStartElapsed;
            auto secBlocked =   secElapsed - secCPU ;
            perfTimeAdd(_tc, secBlocked*_weight);
        }
        _nestingDepth-- ;
    }
    catch (...){;} // timing should not change the code path above
}

} // namespace

