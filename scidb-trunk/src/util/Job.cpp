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
 * @file Job.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Job class
 */

#include <log4cxx/logger.h>
#include <util/WorkQueue.h>
#include <query/Query.h>
#include <util/Job.h>

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.common.thread"));

namespace scidb
{
    void Job::executeOnQueue(std::weak_ptr<WorkQueue>& wq,
                             std::shared_ptr<SerializationCtx>& sCtx)
    {
        _wq=wq;
        _wqSCtx = sCtx;
        run();
        // the state of this object must not be modified after the call to run()
        // or it has to be protected by a mutex
    }

    void Job::execute()
    {
        if (!_removed) {
            const char *err_msg = "Job::execute: unhandled exception";
            try {
                run();

            } catch (Exception const& x) {
                _error = x.copy();
                LOG4CXX_ERROR(logger, err_msg
                              << "\ntype: " << typeid(x).name()
                              << "\njobType: " << typeid(*this).name()
                              << "\nmesg: " << x.what()
                              << "\nqueryID = "<<(_query ? _query->getQueryID() : INVALID_QUERY_ID));
            } catch (const std::exception& e) {
                try {
                    _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_EXECUTION, SCIDB_LE_UNKNOWN_ERROR) << e.what();
                    LOG4CXX_ERROR(logger, err_msg
                                  << "\ntype: " << typeid(e).name()
                                  << "\njobType: " << typeid(*this).name()
                                  << "\nmesg: " << e.what()
                                  << "\nqueryID = "<<(_query ? _query->getQueryID() : INVALID_QUERY_ID));
                } catch (...) {}
                throw;
            } catch (...) {
                try {
                    _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_EXECUTION, SCIDB_LE_UNKNOWN_ERROR) << err_msg;
                    LOG4CXX_ERROR(logger, err_msg);
                } catch (...) {}
                throw;
            }
        }
        _query.reset();
        _done.release();
    }

    // Waits until job is done
    bool Job::wait(bool propagateException, bool allowMultipleWaits)
    {
        _done.enter();
        if (allowMultipleWaits) {
            _done.release(); // allow multiple waits
        }
        if (_error && _error->getShortErrorCode() != SCIDB_E_NO_ERROR) {
            if (propagateException)
            {
                _error->raise();
            }
            return false;
        }
        return true;
    }

    void Job::rethrow()
    {
        _error->raise();
    }
} //namespace
