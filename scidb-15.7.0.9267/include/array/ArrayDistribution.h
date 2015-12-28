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
#ifndef ARRAY_DISTRIBUTION_H_
#define ARRAY_DISTRIBUTION_H_

#include <array/Array.h>
#include <array/Metadata.h>

namespace scidb
{

/**
 * @file ArrayDistribution.h
 *
 * This file contains code needed by by several components: the Operators,
 * the Arrays and Storage, the query compiler and optimization, and probably others.
 *
 * It was factored from Operator.cpp to support some generalization of PartitioningSchema,
 * it is not new code.
 *
 * Note that changes to this file could potentially affect array storage formats
 * depending on whether any of this information is stored into those formats.
 * Currently that is true of the PartitioniningSchema enumeration itself, which
 * is currently being left in Metadata.h
 *
 * A word about SciDB data distribution [originally written for the uniq() operator
 * by Alex, but generally applicable.]
 * <br>
 * <br>
 * The default distribution scheme that SciDB uses is called "psHashPartitioned". In reality, it is a hash of the chunk
 * coordinates, modulo the number of instances. In the one-dimensional case, if data starts at 1 with a chunk size
 * of 10 on 3 instances, then chunk 1 goes to instance 0,  chunk 11 to instance 1, chunk 21 to instance 2, chunk 31 to
 * instance 0, and on...
 * <br>
 * <br>
 * In the two-plus dimensional case, the hash is not so easy to describe. For the exact definition, read
 * getInstanceForChunk() in Operator.cpp.
 * <br>
 * <br>
 * All data is currently stored with this distribution. But operators emit data in different distributions quite often.
 * For example, ops like cross, cross_join and some linear algebra routines will output data in a completely different
 * distribution. Worse, ops like slice, subarray, repart may emit "partially filled" or "ragged" chunks - just like
 * we do in the algorithm example above.
 * <br>
 * <br>
 * Data whose distribution is so "violated" must be redistributed before it is stored or processed by other ops that
 * need a particular distribution. The function redistribute() is available and is sometimes called directly by the
 * operator (see PhysicalIndexLookup.cpp for example). Other times, the operator simply tells the SciDB optimizer that
 * it requires input data in a particular distribution or outputs data in a particular distribtuion.
 * The optimizer then inserts the appropriate SG() operators.
 * That approach is more advanatageous, as the optimizer is liable to get smarter about delaying or waiving the
 * call to redistribute(). For this purpose, the functions
 * <br> getOutputDistribution(),
 * <br> changedDistribution() and
 * <br> outputFullChunks()
 * are provided. See their use in the Operator class.
 */

 /**
 * @param ps the kind of PartitioningScheme
 * @param chunkPosition the Coordinates of the chunk in the Array
 * @param dims the Dimensions of the Array
 * @param nInstancesOriginal number of instances at the time the Array was created
 *        (which is not always the current number of instances, even though it often
 *         is, so be careful: use the value that was persisted with the Array).
 * @return if positive, the InstanceID where the primary copy of the chunk belongs
 *         if negative, a special value like ALL_INSTANCES_MASK which is not associated
 *         with a single instance, so it is often incorrect to directly compare the result
 *         to another InstanceID directly.  A matching predicate should be used.
 */
InstanceID getPrimaryInstanceForChunk(PartitioningSchema ps, Coordinates const& chunkPosition,
                                      Dimensions const& dims, size_t nInstancesOriginal);

} // namespace scidb

#endif // ARRAY_DISTRIBUTION_H_
