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
 * @file PerfTime.h
 *
 * @brief timing methods for performance reporting
 */

#ifndef PERF_TIME_H_
#define PERF_TIME_H_

#include <algorithm>
#include <atomic>

#include <sys/time.h>             // linux specific
#include <sys/resource.h>         // linux specific


namespace scidb
{

/**
 * various accumulated performance timings, in microseconds
 * atomic variables, so no locking needed
 */
enum perfTimeCategory { PTC_FIRST=0,
                        PTC_ACTIVE=PTC_FIRST,               // while the job thread runs, not while client is not requesting
                        PTC_CPU,                            // using the CPU (obtained from OS)
                        PTCW_PG,                            // waiting on PostGres
                        PTCW_EXT,                           // waiting on an EXTernal task to finish (e.g. MPI, R) 
                        PTCW_FS_RD, PTCW_FS_WR,             // waiting on File System read / write
                        PTCW_FS_WR_SYNC, PTCW_FS_FL,        // waiting on File System write syncronous / flush
                        PTCW_SG_RCV, /*PTCW_SG_SND,*/       // waiting on cells sent/received vi Scatter Gather
                        PTCW_BAR,                           // waiting on BARrier (BSP primitive)
                        PTCW_REP,                           // waiting on REPlication
                        PTCW_NET_RCV, PTCW_NET_SND,         // waiting on NETwork bytes sent/received vi send/recv primitives
                        PTCW_MUT_OTHER,                     // non-specific MUTexes
                        PTCW_SEM_OTHER,                     // non-specific SEMaphores
                        PTCW_EVENT_OTHER,                   // non-specific EVENTs
                        PTC_NUM };

typedef enum perfTimeCategory perfTimeCategory_t ;

/**
 * most generally used type of timing
 */
double perfTimeGetElapsed() noexcept ;


/**
 * just for the single CPU time statistic
 */
double perfTimeGetCPU() noexcept ;

/**
 * how time used is reported on a per-category basis
 */
void perfTimeAdd(const perfTimeCategory_t tc, const double sec);

// TODO: this might need to be renamed ScopedWaitTiming
//       and it should only take enums with W in them.
class ScopedWaitTimer {
private:
    double _secStartElapsed;
    double _secStartCPU;
    perfTimeCategory_t  _tc;
    float  _weight;
    static thread_local std::atomic_uint_fast32_t _nestingDepth;  // int64 would be unreasonably large
public:
    ScopedWaitTimer(perfTimeCategory_t tc, float weight=1.0f) noexcept ;
    ~ScopedWaitTimer() noexcept ;
};

} //namespace

#endif /* PERF_TIME_H_ */
