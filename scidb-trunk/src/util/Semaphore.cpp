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
 * @file Semaphore.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Semaphore class
 */

#include "assert.h"
#include <stdio.h>
#include <string>
#include <sstream>
#include <system/Exceptions.h>
#include <util/Semaphore.h>

namespace scidb
{

Semaphore::Semaphore()
{
	if (sem_init(_sem, 0, 0) == -1)
	{
        throw SYSTEM_EXCEPTION(
            SCIDB_SE_INTERNAL,
            SCIDB_LE_SEMAPHORE_ERROR)
                << "initialize" << ::strerror(errno) << errno;
	}
}

Semaphore::~Semaphore()
{
	if (sem_destroy(_sem) == -1)
	{
        throw SYSTEM_EXCEPTION(
            SCIDB_SE_INTERNAL, SCIDB_LE_SEMAPHORE_ERROR)
                << "destroy" << ::strerror(errno) << errno;
	}
}

void Semaphore::enter(perfTimeCategory_t tc /*= PTCW_SEM_OTHER*/)
{
    {
        ScopedWaitTimer timer(tc);         // destruction updates the timing of tc
        do
        {
            if (sem_wait(_sem) == 0) {
                return;
            }
        } while (errno == EINTR);
    }

    throw SYSTEM_EXCEPTION(
        SCIDB_SE_INTERNAL,
        SCIDB_LE_SEMAPHORE_ERROR)
            << "wait" << ::strerror(errno) << errno;
}

void Semaphore::enter(size_t count, perfTimeCategory_t tc /*= TCW_SEM_OTHER*/)
{
    for (size_t i = 0; i < count; i++)
    {
        enter(tc);
    }
}

bool Semaphore::enter(
    ErrorChecker&   errorChecker,
    time_t          timeoutSeconds, /* = 10 */
    perfTimeCategory_t tc /*= TCW_SEM_OTHER*/)
{
    if (errorChecker && !errorChecker()) {
        return false;
    }

    timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
        assert(false);
        throw SYSTEM_EXCEPTION(
            SCIDB_SE_INTERNAL,
            SCIDB_LE_CANT_GET_SYSTEM_TIME);
    }
    ts.tv_sec += timeoutSeconds;

    ScopedWaitTimer timer(tc);         // destruction updates the timing of tc
    while (true)
    {
        if (sem_timedwait(_sem, &ts) == 0) {
            return true;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != ETIMEDOUT) {
            assert(false);
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_INTERNAL,
                SCIDB_LE_SEMAPHORE_ERROR)
                    << "timedwait" << ::strerror(errno) << errno;
        }
        if (errorChecker && !errorChecker()) {
           return false;
        }
        if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
            assert(false);
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_INTERNAL,
                SCIDB_LE_CANT_GET_SYSTEM_TIME);
        }
        ts.tv_sec += timeoutSeconds;
    }
}

bool Semaphore::enter(
    size_t          count,
    ErrorChecker&   errorChecker,
    time_t          timeoutSeconds, /* = 10 */
    perfTimeCategory_t tc /*= TCW_SEM_OTHER*/)
{
    for (size_t i = 0; i < count; i++) {
        if (!enter(errorChecker, timeoutSeconds)) {
            return false;
        }
    }
    return true;
}

bool Semaphore::tryEnter()
{
	// Instant try
	do
	{
		if (sem_trywait(_sem) != -1)
			return true;
	} while (errno == EINTR);

	if (errno == EAGAIN)
		return false;

        throw SYSTEM_EXCEPTION(
            SCIDB_SE_INTERNAL,
            SCIDB_LE_SEMAPHORE_ERROR)
                << "trywait" << ::strerror(errno) << errno;

	return false;	// Shutdown warning
}

void Semaphore::release(int count /* = 1 */)
{
    for (int i = 0; i < count; i++)
    {
        if (sem_post(_sem) == -1)
        {
            // XXX TODO: this must be an error
            ASSERT_EXCEPTION_FALSE(false);
        }
    }
}

void Semaphore::release(bool &result, int count /* = 1 */)
{
    result = false;
    for (int i = 0; i < count; i++)
    {
        if (sem_post(_sem) == 0)
        {
            result = true;
            return;
        }
    }
}

} //namespace
