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

/*
 * HDF5Array.h
 *
 *  Created on: June 8 2016
 *      Author: Haoyuan Xing<hbsnmyj@gmail.com>
 */
#include <array/Array.h>
#include <array/MemChunk.h>
#include <util/Arena.h>
#include <query/Parser.h>
#include "HDF5ArrayDesc.h"

#ifndef SCIDB_HDF5ARRAY_H
#define SCIDB_HDF5ARRAY_H

namespace scidb
{
namespace hdf5gateway
{

/**
 * @brief A read-only HDF5 array.
 * This array allocate the chunks to the instances at run time, hence
 * assumes that the HDF5 file could be accessed by all the instances.
 */
class HDF5Array : public Array, public std::enable_shared_from_this<HDF5Array>
{
public:
    /*
     * @param desc SciDB Array Description.
     * @param h5desc HDF5 meta data for the array.
     * @param query Query context.
     */
    HDF5Array(const ArrayDesc& desc, HDF5ArrayDesc& h5desc,
              const std::shared_ptr<Query> &query, arena::ArenaPtr arena);
    HDF5Array(const HDF5Array &other) = delete;
    HDF5Array(HDF5Array &&other) = delete;
    HDF5Array &operator=(const HDF5Array &array) = delete;
    HDF5Array&& operator=(HDF5Array&& array) = delete;

    virtual ~HDF5Array() {}

    /* inherited methods */
    virtual const ArrayDesc& getArrayDesc() const override { return _desc; }
    virtual bool hasChunkPositions() const override { return true; }
    virtual std::shared_ptr<CoordinateSet> getChunkPositions() const override;
    virtual bool isCountKnown() const override { return true; }
    virtual size_t count() const override;
    virtual std::shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const override;

    /**
     * @brief return the HDF5-only metadata.
     */
    const HDF5ArrayDesc& getHDF5ArrayDesc() const { return _h5desc; }

    /**
     * @brief return the upper bound of elements in a chunk.
     */
    int64_t getMaxElementsInChunk(bool withOverlap = false) const;

    /**
     * @brief return the number of elements in a chunk starting at start
     */
    int64_t getMaxElementsInChunk(Coordinates start, bool withOverlap = false) const;

    /* As HDF5 library is not thread-safe as of version 1.10, a mtuex is used
     * to synchroize HDF5 calls. */
    Mutex& getMutex() const { return _mutex; }
private:
    arena::ArenaPtr _arena;
    ArrayDesc _desc;
    HDF5ArrayDesc _h5desc;
    std::weak_ptr<Query> _query;
    /* At array construction time, we calculate all the position of chunks
     * according to the dimension, and save the local instances to the set
     * for iteration. */
    std::shared_ptr<CoordinateSet> _coordinateSet;
    Mutex mutable _mutex;

private:
    /* check if a coordinate is within the dimensions provided. */
    inline static bool lessThanOrEqual(Coordinates coordinates, const Dimensions &dims);

    inline static void getCoordinatesOfNextChunk(Coordinates& coordinates, const Dimensions &dims);

};

inline bool HDF5Array::lessThanOrEqual(const Coordinates coordinates, const Dimensions &dims)
{
    SCIDB_ASSERT(dims.size() != 0);
    SCIDB_ASSERT(coordinates.size() == dims.size());
    for(size_t i=0; i<dims.size(); i++) {
        if(coordinates[i] > dims[i].getEndMax())
            return false;
    }
    return true;
}

inline void HDF5Array::getCoordinatesOfNextChunk(Coordinates& coordinates, const Dimensions &dims)
{
    SCIDB_ASSERT(dims.size() != 0);
    SCIDB_ASSERT(coordinates.size() == dims.size());
    bool carry = false;
    long i = coordinates.size();
    do {
        --i;
        coordinates[i] += dims[i].getChunkInterval();
        if(i > 0 && coordinates[i] > dims[i].getEndMax()) {
            coordinates[i] = dims[i].getStartMin();
            carry = true;
        }
    } while (carry && i > 0);
}

} //namespace hdf5gateway
} //namespace scidb

#endif //SCIDB_HDF5ARRAY_H
