/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2015 SciDB, Inc.
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
 * @file ScopedQueryThread.h
 */

#ifndef SCOPED_QUERY_THREAD_H
#define SCOPED_QUERY_THREAD_H

#include <query/Query.h>

namespace scidb {



class ScopedActiveQueryThread {
private:
    // NOTE can't implement in terms of ScopedWaitTimer
    double _secStartElapsed;
    double _secStartCPU;
public:
    ScopedActiveQueryThread(std::shared_ptr<Query>& query)
    :
        _secStartElapsed(perfTimeGetElapsed()),
        _secStartCPU(perfTimeGetCPU())
    {
        // TODO: before setting QPT, should we check to see if QPT is already set?
        //       this might be important on coordinators, wehre the
        //       server thread might run as soon as enabled by client thread
        Query::setQueryPerThread(query);
    }

    ~ScopedActiveQueryThread() {
        perfTimeAdd(PTC_CPU, perfTimeGetCPU() - _secStartCPU);
        perfTimeAdd(PTC_ACTIVE, perfTimeGetElapsed() - _secStartElapsed);
        Query::resetQueryPerThread();
    }
};

} // namespace scidb

#endif // SCOPED_QUERY_THREAD_H

