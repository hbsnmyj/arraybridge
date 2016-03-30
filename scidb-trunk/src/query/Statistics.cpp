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
 * @file Statistic.cpp
 *
 * @brief Implementation of statistic gatharing class
 *
 * NOTE: For performance timing, see PerfTiming.{h,cpp}
 *       Timing aspects of this code are not currently maintained
 *       and are being considered for removal.
 */

#include <memory>
#include <log4cxx/logger.h>

#include "query/Statistics.h"
#include "query/Query.h"

#include <util/Pqxx.h>

using namespace std;
using namespace boost;
using namespace pqxx;
using namespace pqxx::prepare;

namespace scidb
{

Statistics StatisticsScope::systemStatistics;

__thread Statistics* currentStatistics = &StatisticsScope::systemStatistics;

inline size_t printSize(size_t size)
{
    if (size < 2*KiB) {
        return size;
    }
    if (size < 2*MiB) {
        return size / KiB;
    }
    return size / MiB;
}

inline const char* printSizeUnit(size_t size)
{
    if (size < 2*KiB) {
        return "B";
    }
    if (size < 2*MiB) {
        return "KiB";
    }
    return "MiB";
}

#ifndef SCIDB_CLIENT
std::ostream& writeStatistics(std::ostream& os, const Statistics& s, size_t tab)
{
    string tabStr(tab*4, ' ');
    os <<
        tabStr << "Sent " << printSize(s.sentSize) << printSizeUnit(s.sentSize) << " (" << s.sentMessages << " messages)" << endl <<
        tabStr << "Recieved " << printSize(s.receivedSize) << printSizeUnit(s.receivedSize) << " (" << s.receivedMessages << " messages)" << endl <<
        tabStr << "Written " << printSize(s.writtenSize) << printSizeUnit(s.writtenSize) << " (" << s.writtenChunks << " chunks)" << endl <<
        tabStr << "Read " << printSize(s.readSize) << printSizeUnit(s.readSize) << " (" << s.readChunks << " chunks)" << endl <<
        tabStr << "Pinned " << printSize(s.pinnedSize) << printSizeUnit(s.pinnedSize) << " (" << s.pinnedChunks << " chunks)" << endl <<
        tabStr << "Allocated " << printSize(s.allocatedSize) << printSizeUnit(s.allocatedSize) << " (" << s.allocatedChunks << " chunks)" << endl;

    return os;
}

// S T A T I S T I C S   M O N I T O R

class LoggerStatisticsMonitor: public StatisticsMonitor
{
public:
    virtual ~LoggerStatisticsMonitor() {}
    LoggerStatisticsMonitor(const string& loggerName):
        _logger(log4cxx::Logger::getLogger(loggerName == "" ? "scidb.statistics" : loggerName))
    {
    }

    void pushStatistics(const Query& query)
    {
        stringstream ss;
        query.writeStatistics(ss);
        LOG4CXX_INFO(_logger, "Statistics of query " << query.getQueryID() << ": " << ss.str())
    }

private:
    log4cxx::LoggerPtr _logger;
};

std::shared_ptr<StatisticsMonitor> StatisticsMonitor::create(size_t type, const string& params)
{
    switch (type)
    {
    case smLogger:
    default:
        return std::shared_ptr<StatisticsMonitor>(new LoggerStatisticsMonitor(params));
    }
}

#endif

} // namespace
