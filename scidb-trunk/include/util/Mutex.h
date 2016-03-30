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
 * @file Mutex.h
 *
 * @author roman.simakov@gmail.com
 *
 * @brief The Mutex class for synchronization
 */

#ifndef MUTEX_H_
#define MUTEX_H_

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <pthread.h>

#include <sys/time.h>             // linux specific
#include <sys/resource.h>         // linux specific

#include <util/Platform.h>
#include <util/PerfTime.h>

#include <system/Exceptions.h>


namespace scidb
{

class Event;

class Mutex
{
friend class Event;
friend class Semaphore;
private:
 class PAttrEraser
 {
 public:
 PAttrEraser(pthread_mutexattr_t *attrPtr) : _attrPtr(attrPtr)
     {
         assert(_attrPtr!=NULL);
     }
     ~PAttrEraser()
     {
         pthread_mutexattr_destroy(_attrPtr);
     }
 private:
     pthread_mutexattr_t *_attrPtr;
 };

    pthread_mutex_t _mutex;
    static const int _mutexType = PTHREAD_MUTEX_RECURSIVE;

  public:
    void checkForDeadlock() {
        assert(_mutex.__data.__count == 1);
    }

    Mutex()
    {
        pthread_mutexattr_t mutexAttr;
        if (pthread_mutexattr_init(&mutexAttr)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutexattr_init";
        }
        PAttrEraser onStack(&mutexAttr);

        if (pthread_mutexattr_settype(&mutexAttr, _mutexType)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutexattr_settype";
        }
        if (pthread_mutex_init(&_mutex, &mutexAttr)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutex_init";
        }
    }

    ~Mutex()
    {
        if (pthread_mutex_destroy(&_mutex)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutex_destroy";
        }
    }

    void lock(scidb::perfTimeCategory_t tc = PTCW_MUT_OTHER)
    {
        {
            ScopedWaitTimer timer(tc);     // destruction updates the timing of tc
            if (pthread_mutex_lock(&_mutex)) {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutex_lock";
            }
        }
    }

    void unlock()
    {
        if (pthread_mutex_unlock(&_mutex)) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_OPERATION_FAILED) << "pthread_mutex_unlock";
        }
    }

    /// @return true if the mutex is locked by this thread; otherwise, false
    /// @note Works only in DEBUG mode
    /// @note Specific to Linux implementation of pthreads
    bool isLockedByThisThread()
    {
        bool result = false;
        if (isDebug())  {
            assert(_mutexType == PTHREAD_MUTEX_RECURSIVE);

            int locked = pthread_mutex_trylock(&_mutex);
            if (locked == 0) {
                result = (_mutex.__data.__count > 1);
                unlock();
            }
            result = result || (locked == EAGAIN) || (locked == EDEADLK);
        }
        return result;
    }
};


/***
 * RAII class for holding Mutex in object visible scope.
 */
class ScopedMutexLock
{
private:
	Mutex& _mutex;

public:
	ScopedMutexLock(Mutex& mutex, perfTimeCategory_t tc = PTCW_SEM_OTHER): _mutex(mutex)
	{
		_mutex.lock(tc);
	}

	~ScopedMutexLock()
	{
		_mutex.unlock();
	}
};


} //namespace

#endif /* MUTEX_H_ */
