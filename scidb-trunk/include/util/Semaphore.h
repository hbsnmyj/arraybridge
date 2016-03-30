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
 * @file Semaphore.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Semaphore class for synchronization
 */

#ifndef SEMAPHORE_H_
#define SEMAPHORE_H_

#include <errno.h>
#include <boost/function.hpp>
#include <string>

#include <util/PerfTime.h>

#include <semaphore.h>
#include <time.h>
#include <fcntl.h>

namespace scidb
{

class Semaphore
{
private:
    sem_t _sem[1];

public:
    /**
     * @throws a scidb::Exception if necessary
     */
    typedef boost::function<bool()> ErrorChecker;
    Semaphore();

    ~Semaphore();

    /**
     * @brief Try to enter semaphore
     * @throws scidb::SCIDB_SE_INTERNAL::SCIDB_LE_SEMAPHORE_ERROR
     */
    void enter(perfTimeCategory_t tc = PTCW_SEM_OTHER);

    /**
     * @brief Try to enter semaphore count times
     * @param[in] count - how many times should we attempt to enter
     * @throws scidb::SCIDB_SE_INTERNAL::SCIDB_LE_SEMAPHORE_ERROR
     */
    void enter(size_t count, perfTimeCategory_t tc = PTCW_SEM_OTHER);

    /**
     * @brief Try to enter semaphore with time out and check error state.
     * If error state is set it returns.
     *
     * @param[in] errorChecker - error checking functor
     * @parampin] timeoutSeconds - when should sem_timedwait expire
     */
    bool enter(
        ErrorChecker& errorChecker,
        time_t timeoutSeconds = 10,
        perfTimeCategory_t tc = PTCW_SEM_OTHER);

    /**
     * @brief Try to enter semaphore with time out and check error state.
     * Note:  The sem_wait method will be called using timeoutSeconds
     * to timeout.  If after the timeout a check for errors will be
     * performed.  If there is an error the routine will return false.
     * If no error exists, another attempt at sem_wait will be made.
     * This wait/check_errors will iterate count times.
     *
     * @param[in] count - how many times should we attempt to enter
     * @param[in] errorChecker - error checking functor
     * @parampin] timeoutSeconds - when should sem_timedwait expire
     */
    bool enter(
        size_t count,
        ErrorChecker& errorChecker,
        time_t timeoutSeconds = 10,
        perfTimeCategory_t tc = PTCW_SEM_OTHER);

    /**
     * @brief Attempt to release the semaphore count times.
     * @param[in] - count how many times to attempt the release
     *
     * Note:  if an error occurs an assertion will occur
     */
    void release(int count = 1);

    /**
     * @brief Attempt to release the semaphore count times.
     * @param[out] - result true=released, false=otherwise
     * @param[in] - count how many times to attempt the release
     */
    void release(bool &result, int count = 1);

    /**
     * @brief Enter a semaphore. Throw an exception if not successful.
     * @throws scidb::SCIDB_SE_INTERNAL::SCIDB_LE_SEMAPHORE_ERROR
     */
    bool tryEnter();
};


class ReleaseOnExit
{
    Semaphore& sem;

  public:
    /**
     * Constructor
     */
    ReleaseOnExit(Semaphore& s) : sem(s) {}

    /**
     * Follows the RAII (resource-acquisition-is-initialization) paradigm
     * and releases the semaphore when "this" object goes out of scope.
     */
    ~ReleaseOnExit()
    {
        sem.release();
    }
};

} //namespace



#endif /* SEMAPHORE_H_ */
