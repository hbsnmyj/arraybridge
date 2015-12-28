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

#include <array/Array.h>
#include <array/ArrayDistribution.h>
#include <query/Operator.h>             // for ALL_INSTANCE_MASK

/**
 * @file ArrayDistribution.cpp
 *
 * This file contains implementation for ArrayDistribution.h
 *
 * Note that changes to this file could potentially affect array storage formats
 * depending on whether any of this information is stored into those formats.
 * Currently that is true of the PartitioniningSchema enumeration itself, which
 * is currently being left in Metadata.h
 *
 */

/**
 * Moved from Metadata.h so this file could hold all definitions of the primary
 * mapping of chunks to instances.  However, we want to hide the direct use
 * of this function, because some code was assuming arrays were in psHashDistribution
 * when they can no longer be guaranteed to be.
 *
 * This means that all code that was calling this directly must now call
 * getPrimaryInstanceForChunk() below, which handles the more general case
 * and is the new standard for all PartitioningSchema values that are persistable.
 *
 * @param dims Dimensions of the array in question
 * @param pos Coordinates of the chunk in question
 * @return a hashed value. (historical version, not guaranteed less than the number of instances)
 *
 * NOTE: the code was moved from Metadata.cpp  The method was called
 *       ArrayDesc::getHashedChunkNumber(chunkPosition) const
 *
 * NOTE: static rather than anonymous namespace in order that even the
 * address of this function cannot leave this module.  This is 100% private,
 * and sometimes permits more aggressive compiler-elected inlining.
 * This is one of the few times that its worth doing
 */
static uint64_t getHashedChunkNumber(scidb::Dimensions const& dims, scidb::Coordinates const& pos)
{
    uint64_t no = 0;
    /// The goal here is to produce a good hash function without using array
    /// dimension sizes (which can be changed in case of unboundary arrays)
    for (size_t i = 0, n = pos.size(); i < n; i++)
    {
        // 1013 is prime number close to 1024. 1024*1024 is assumed to be optimal chunk size for 2-d array.
        // For 1-d arrays value of this constant is not important, because we are multiplying it on 0.
        // 3-d arrays and arrays with more dimensions are less common and using prime number and XOR should provide
        // well enough (uniform) mixing of bits.
        no = (no * 1013) ^ ((pos[i] - dims[i].getStartMin()) / dims[i].getChunkInterval());
    }

    return no;
}

namespace scidb {

 /**
 * TODO: after initial check-in, lines marked
 * "to make meld diff well. remove after first check-in"
 * should be fixed: re-indent and re-brace, remove dummy variables, etc.
 *
 */

InstanceID getPrimaryInstanceForChunk(PartitioningSchema ps, Coordinates const& chunkPosition,
                                      Dimensions const& dims, size_t nInstancesOriginal)
{
    const size_t instanceCount = nInstancesOriginal;   // to make meld diff well. remove after first check-in

    InstanceID destInstanceId;
    switch (ps) {
    //
    // persistable cases: valid cases for ArrayDesc::getPrimaryInstanceForChunk()
    // these do, or soon will, return true from ArrayDesc::isPersistable() and
    // so can have their arrays entered in the catalog, and stored in TEMP and DB-Arrays.
    // Constrast with non-persistable cases further below.
    //
    case psReplication:
    {
        destInstanceId = ALL_INSTANCE_MASK;
    }
    case psHashPartitioned:
    {
        destInstanceId = getHashedChunkNumber(dims, chunkPosition) % instanceCount;
        break;
    }
    case psByRow:
    {
        uint64_t dim0Length = dims[0].getLength();
        destInstanceId = ((chunkPosition)[0] - dims[0].getStartMin()) / dims[0].getChunkInterval()
        / (((dim0Length + dims[0].getChunkInterval() - 1) / dims[0].getChunkInterval() + instanceCount - 1) / instanceCount);
        break;
    }
    case psByCol:
    {
        uint64_t dim1Length = dims.size() > 1 ? dims[1].getLength() : 0;
        if (dims.size() > 1)
        {
            destInstanceId = ((chunkPosition)[1] - dims[1].getStartMin()) / dims[1].getChunkInterval()
            / (((dim1Length + dims[1].getChunkInterval() - 1) / dims[1].getChunkInterval() + instanceCount - 1) / instanceCount);
        }
        else
        {
            destInstanceId = 0;  //XXX TODO Tigor ; you wanted a comment because you wanted to look at this line
        }
        break;
    }
    //
    // Non-persistable cases.
    // These are not mappable to instanceIds given the currently persisted array information.
    // They are used as arguments to redistributeXxxx() and their mapping typically
    // involves the use of additional "psData".  They are not compatible with this
    // function.
    // TODO: clean up the bracing and verbosity after first checkin
    //       it is "to make meld diff well. remove after first check-in"
    case psScaLAPACK:
    {
        ASSERT_EXCEPTION_FALSE("getPrimaryInstanceForChunk: psScaLAPACK not permitted in stored arrays");
        break; // NOTREACHED
    }
    case psLocalInstance:
    {
        ASSERT_EXCEPTION_FALSE("getPrimaryInstanceForChunk: psLocalInstance not permitted in stored arrays");
        break; // NOTREACHED
    }
    case psGroupby:
    {
        ASSERT_EXCEPTION_FALSE("getPrimaryInstanceForChunk: psGroupby not permitted in stored arrays");
        break; // NOTREACHED
    }
    case psUndefined:
    {
        ASSERT_EXCEPTION_FALSE("getPrimaryInstanceForChunk: psUndefined not permitted in stored arrays");
        break; // NOTREACHED
    }
    case psUninitialized:
    default:
    {
        ASSERT_EXCEPTION_FALSE("getPrimaryInstanceForChunk: internal error, an unknown PartitioningSchema was supplied.");
        break; // NOTREACHED
    }
    } // TODO: end region of bracing, tabbing, and verbosity cleanup.

    return destInstanceId;
}
} // namespace scidb
