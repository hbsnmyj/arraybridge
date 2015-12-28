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

/****************************************************************************/

#include <pthread.h>                                     // For pthread_mutex
#include <errno.h>                                       // For the code EBUSY
#include "Platform.h"                                    // For getBlockSize()
#include "ArenaDetails.h"                                // For implementation

/****************************************************************************/
namespace scidb { namespace arena { namespace {
/****************************************************************************/

size_t          _bytes  = 0;                             // Bytes  allocated
size_t          _blocks = 0;                             // Blocks allocated
size_t          _peak   = 0;                             // High water mark
size_t          _limit  = SIZE_MAX;                      // Max memory limit
pthread_mutex_t _mutex  = PTHREAD_MUTEX_INITIALIZER;     // Guards counters

/**
 *  Acquire and release the mutex that guards our various counters.
 */
struct Lock : noncopyable, stackonly
{
    Lock() {if (pthread_mutex_lock  (&_mutex)) abort();} // Acquire the mutex
   ~Lock() {if (pthread_mutex_unlock(&_mutex)) abort();} // Release the mutex
};

/****************************************************************************/
}
/****************************************************************************/

/**
 *  Release the (copy of) the root arena mutex when forking a child process.
 *
 *  Call from the one and only child process thread immediately upon forking.
 *
 *  @see http://cppwisdom.quora.com/Why-threads-and-fork-dont-mix
 */
void onForkOfChild()
{
    int e = pthread_mutex_trylock(&_mutex);              // But do not block

    if (e==0 || e==EBUSY)                                // So we acquired it?
    {
        if (pthread_mutex_unlock(&_mutex) == 0)          // ...we released it?
        {
         /* So we now know for sure that the (copy of) the root arena _mutex
            is definately not locked within the one and only thread that now
            runs in the forked child process...*/

            _bytes  = 0;                                 // ...bytes allocated
            _blocks = 0;                                 // ...blocks allocated
            _peak   = 0;                                 // ...high water mark
            _limit  = SIZE_MAX;                          // ...max memory limit
            return;                                      // ...and we're good
        }
    }

    abort();                                             // Malloc is unusable
}

/**
 *  Return the maximum memory limit in bytes. An attempt to allocate more than
 *  this number of bytes should fail; malloc() and friends return 0, while the
 *  root arena and operator new throw a std::bad_alloc exception.
 */
size_t getMemoryLimit()
{
    return _limit;                                       // The current limit
}

/**
 *  Assign the maximum memory limit in bytes. An attempt to allocate more than
 *  this number of bytes should fail; malloc() and friends return 0, while the
 *  root arena and operator new throw a std::bad_alloc exception.
 */
bool setMemoryLimit(size_t limit)
{
    Lock l;                                              // Lock the counters

    if (_bytes <= limit)                                 // Not yet exceeded?
    {
        _limit = limit;                                  // ...ok, then set it

        return true;                                     // ...set succeeded
    }

    return false;                                        // Assignment failed
}

/**
 *  An arena-compatible version of the standard freestore function.
 *
 *  @see http://www.cplusplus.com/reference/cstdlib/malloc
 */
void* malloc(size_t size)
{
    Lock l;                                              // Lock the counters

    if (_bytes + size > _limit)                          // Exceeds the limit?
    {
        return NULL;                                     // ...don't even try
    }

    if (void* p = std::malloc(size))                     // Allocate as normal
    {
        size_t n = getBlockSize(p);                      // ...the actual size

        _bytes  += n;                                    // ...current bytes
        _blocks += 1;                                    // ...current blocks
        _peak    = std::max(_peak,_bytes);               // ...record the max

        return p;                                        // ...the allocation
    }

    return NULL;                                         // Failed to allocate
}

/**
 *  An arena-compatible version of the standard freestore function.
 *
 *  @see http://www.cplusplus.com/reference/cstdlib/calloc
 */
void* calloc(count_t count,size_t size)
{
    assert(count < SIZE_MAX/size);                       // Beware of overlow

    Lock l;                                              // Lock the counters

    if (_bytes + count * size > _limit)                  // Exceeds the limit?
    {
        return NULL;                                     // ...don't even try
    }

    if (void* p = std::calloc(count,size))               // Allocate as normal
    {
        size_t n = getBlockSize(p);                      // ...the actual size

        _bytes  += n;                                    // ...current bytes
        _blocks += 1;                                    // ...current blocks
        _peak    = std::max(_peak,_bytes);               // ...record the max

        return p;                                        // ...the allocation
    }

    return NULL;                                         // Failed to allocate
}

/**
 *  An arena-compatible version of the standard freestore function.
 *
 *  @see http://www.cplusplus.com/reference/cstdlib/realloc
 */
void* realloc(void* payload,size_t size)
{
    Lock l;                                              // Lock the counters

    size_t f = getBlockSize(payload);                    // Fetch current size

    assert(f <= _bytes);                                 // Verify accounting

    if (_bytes - f + size > _limit)                      // Exceeds the limit?
    {
        return NULL;                                     // ...don't even try
    }

    if (void* p = std::realloc(payload,size))            // Allocate as normal
    {
        size_t n = getBlockSize(p);                      // ...the actual size

        _bytes -= f;                                     // ...bytes reclaimed
        _bytes += n;                                     // ...bytes consumed
        _blocks+= size_t(f==0) - size_t(size==0);        // ...is allocation?
        _peak   = std::max(_peak,_bytes);                // ...record the max

        return p;                                        // ....the allocation
    }

    return NULL;                                         // Failed to allocate
}

/**
 *  An arena-compatible version of the standard freestore function.
 *
 *  @see http://www.cplusplus.com/reference/cstdlib/free
 */
void free(void* payload)
{
    if (size_t n = getBlockSize(payload))                // Fetch actual size
    {
        std::free(payload);                              // ...free as normal

        Lock l;                                          // ...lock counters

        assert(_blocks>0 && n<=_bytes);                  // ...check counters

        _bytes  -= n;                                    // ...current bytes
        _blocks -= 1;                                    // ...current blocks
    }
}

/**
 *  Return the root arena,  the singleton %arena from which every other %arena
 *  ultimately obtains its memory.
 *
 *  The root arena forms the root of a parent-child tree that runs through all
 *  other arenas up to this singleton instance,  and is automatically supplied
 *  as the  default when constructing an %arena for  which no other parent has
 *  been specified.
 *
 *  It's implemented as a stateless wrapper around the global counters defined
 *  above, delegating to arena::malloc() and arena::free() to manipulate these
 *  counters and so adhere to the maximum memory limit installed at startup.
 */
ArenaPtr getRootArena()
{
    static struct RootArena : Arena
    {
        name_t      name()                         const {return "root";}
        size_t      available()                    const {return _limit - _bytes;}
        size_t      allocated()                    const {return _bytes;}
        size_t      peakusage()                    const {return _peak;}
        size_t      allocations()                  const {return _blocks;}
        features_t  features()                     const {return finalizing|recycling|threading;}

        void* doMalloc(size_t size)
        {
            assert(size != 0);                           // Validate arguments

            if (void* p = arena::malloc(size))           // Use our own malloc
            {
                return p;                                // ...yes, succeeded
            }

            this->exhausted(size);                       // Failed to allocate
        }

        void doFree(void* payload,size_t size)
        {
            assert(aligned(payload));                    // Validate argument
            assert(size <= getBlockSize(payload));       // Validate its size

            arena::free(payload);                        // Use our own free

            (void)size;                                  // Unused in release
        }

    } theRootArena;                                      // The singleton root

    return ArenaPtr(static_cast<Arena*>(&theRootArena),null_deleter());
}

/****************************************************************************/
}}
/****************************************************************************/
