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
 * HDF5Array.cpp
 *
 *  Created on: June 8 2016
 *      Author: Haoyuan Xing<hbsnmyj@gmail.com>
 */

#include "HDF5Array.h"
#include <query/Query.h>
#include <system/Exceptions.h>

namespace scidb
{
namespace hdf5gateway
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.physical_scan_hdf5"));

std::string to_string(const Coordinates& coordniates) {
    std::stringstream ss;
    std::ostream_iterator<Coordinates::value_type> oIter(ss, ", ");
    std::copy(coordniates.begin(), coordniates.end(), oIter);
    return ss.str();
}

class HDF5ArrayIterator : public ConstArrayIterator
{
public:
    HDF5ArrayIterator(std::weak_ptr<const HDF5Array> array,
                      AttributeID attId,
                      const std::weak_ptr<Query> query);
    ~HDF5ArrayIterator(){};

    HDF5ArrayIterator(const HDF5ArrayIterator&) = delete;
    HDF5ArrayIterator(HDF5ArrayIterator&&) = default;
    HDF5ArrayIterator& operator=(const HDF5ArrayIterator&) = delete;
    HDF5ArrayIterator& operator=(HDF5ArrayIterator&&) = default;

    virtual ConstChunk const& getChunk() override;
    virtual bool end() override;
    virtual void operator++() override;
    virtual Coordinates const& getPosition() override;
    virtual bool setPosition(Coordinates const& pos) override;
    virtual void reset() override;
private:
    std::weak_ptr<const HDF5Array> _array;
    std::weak_ptr<Query> _query;

    std::unique_ptr<MemChunk> _chunk;
    std::shared_ptr<CoordinateSet> _coordinateSet;
    CoordinateSet::iterator _coordinateIter;

    std::unique_ptr<HDF5File> _hdf5File;
    std::unique_ptr<HDF5Dataset> _hdf5Dataset;
    AttributeID _attrId;
    bool _needRead; /* check if we need to read the data from disk */
    bool _isEmptyBitmap;

    /*
     * returns the first chunk that contains position pos.
     */
    std::set<std::vector<long>, scidb::CoordinatesLess>::iterator findChunk(Coordinates pos);

    bool isCoordinatesInChunk(const Coordinates &chunkStart, const Coordinates &pos);

    void readChunk();
};

HDF5ArrayIterator::HDF5ArrayIterator(std::weak_ptr<const HDF5Array> array, AttributeID attId,
                                     const std::weak_ptr<Query> query)
    : _array(array), _query(query), _attrId(attId), _needRead(true)
{
    auto arrayPtr = _array.lock();
    LOG4CXX_TRACE(logger, "HDF5ArrayIterator: Initializing Iterator array = "
                          << arrayPtr->getName() <<" attrId=" << attId << "\n");

    _coordinateSet = arrayPtr->getChunkPositions();
    _coordinateIter = arrayPtr->getChunkPositions()->begin();
    _isEmptyBitmap = (_attrId == arrayPtr->getArrayDesc().getEmptyBitmapAttribute()->getId());
    if(_isEmptyBitmap == false) {
        const HDF5ArrayAttribute& h5Attr = arrayPtr->getHDF5ArrayDesc().getAttribute(_attrId);
        ScopedMutexLock lock(arrayPtr->getMutex());

        _hdf5File = std::make_unique<HDF5File>(h5Attr.getFilePath(), HDF5File::OpenOption::kRDONLY);
        _hdf5Dataset = std::make_unique<HDF5Dataset>(*_hdf5File, h5Attr.getDatasetName());
    }
}

ConstChunk const& HDF5ArrayIterator::getChunk()
{
    LOG4CXX_TRACE(logger, "HDF5ArrayIterator: getting chunk " << to_string(*_coordinateIter)
                          << " attrId=" << _attrId << "\n");
    if(_needRead)
        readChunk();

    return *_chunk;
}

void HDF5ArrayIterator::readChunk()
{
    auto arrayPtr = _array.lock();
    bool isEmptyBitmap = (_attrId == arrayPtr->getArrayDesc().getEmptyBitmapAttribute()->getId());

    _chunk.reset(new MemChunk);

    auto attrDesc = arrayPtr->getArrayDesc().getAttributes()[_attrId];
    int64_t nElems = arrayPtr->getMaxElementsInChunk(getPosition());
    Address address(_attrId, getPosition());
    _chunk->initialize(arrayPtr.get(), &arrayPtr->getArrayDesc(), address, 0);



    if(isEmptyBitmap) {
        /* Just create a dense bitmap */
        auto emptyBitmap = std::make_shared<RLEEmptyBitmap>(nElems);
        _chunk->allocate(emptyBitmap->packedSize());
        auto data = _chunk->getData();
        emptyBitmap->pack((char *)data);
    } else {
        /* initalize a fully dense chunk, allocate the space, and then read data */
        size_t elemSize = attrDesc.getSize();
        size_t dataSize = elemSize * nElems;
        size_t rawSize = ConstRLEPayload::perdictPackedize(1, dataSize);
        _chunk->allocate(rawSize);
        auto data = _chunk->getData();
        auto rawData = ConstRLEPayload::initDensePackedChunk((char *) data, dataSize, 0, elemSize, nElems, false);
        auto firstCoordinates = _chunk->getFirstPosition(true);
        H5Coordinates target_pos(firstCoordinates.begin(), firstCoordinates.end());

        ScopedMutexLock lock(arrayPtr->getMutex());
        _hdf5Dataset->readChunk(*_chunk, target_pos, (void*)rawData);
    }
    _needRead = false;
}

bool HDF5ArrayIterator::end()
{
    return _coordinateIter == _coordinateSet->end();
}

void HDF5ArrayIterator::operator++()
{
    ++_coordinateIter;
    _needRead = true;
}

Coordinates const& HDF5ArrayIterator::getPosition()
{
    return *_coordinateIter;
}

bool HDF5ArrayIterator::setPosition(Coordinates const& pos)
{
    LOG4CXX_TRACE(logger, "HDF5ArrayIterator: setting pos " << to_string(*_coordinateIter)
                          << " attrId=" << _attrId << "\n");
    auto chunkPosIter = findChunk(pos);
    if (isCoordinatesInChunk(*chunkPosIter, pos)) {
        _coordinateIter = chunkPosIter;
        _needRead = true;
        return true;
    } else {
        return false;
    }
}

void HDF5ArrayIterator::reset()
{
    _coordinateIter = _coordinateSet->begin();
    _needRead = true;
}

std::set<std::vector<long>, scidb::CoordinatesLess>::iterator HDF5ArrayIterator::findChunk(
        Coordinates pos)
{
    return _coordinateSet->lower_bound(pos);
}

bool HDF5ArrayIterator::isCoordinatesInChunk(const Coordinates &chunkStart, const Coordinates &pos)
{
    auto arrayPtr = _array.lock();
    auto dims = arrayPtr->getArrayDesc().getDimensions();
    for(unsigned i=0;i<chunkStart.size();++i) {
        if(chunkStart[i] > pos[i] || pos[i] >= chunkStart[i] + dims[i].getChunkInterval()) {
            return false;
        }
    }
    return true;
}

HDF5Array::HDF5Array(scidb::ArrayDesc const& desc,
                     scidb::hdf5gateway::HDF5ArrayDesc& h5desc,
                     const std::shared_ptr<Query>& query,
                     arena::ArenaPtr arena)
        : _arena(arena), _desc(desc), _h5desc(h5desc), _coordinateSet(new CoordinateSet)
{
    setQuery(query);
    auto& dims = getArrayDesc().getDimensions();
    auto instance_id = query->getInstanceID();
    auto total_instances = query->getInstancesCount();
    auto chunk_id = 0;

    /* Generates all the chunks, and save the chunks that are allocated to this instance. */
    /* Right now we use a simple round-robin algorithm. */
    Coordinates coordinates(dims.size(), 0);
    while(lessThanOrEqual(coordinates, dims)) {
        if(chunk_id % total_instances == instance_id) {
            _coordinateSet->insert(coordinates);
        }
        getCoordinatesOfNextChunk(coordinates, dims);
        ++chunk_id;
    }
}

std::shared_ptr<CoordinateSet> HDF5Array::getChunkPositions() const
{
    return _coordinateSet;
}

size_t HDF5Array::count() const
{
    return _coordinateSet->size();
}

std::shared_ptr<ConstArrayIterator> HDF5Array::getConstIterator(AttributeID attr) const
{
    auto shared_ptr = shared_from_this();
    return std::make_shared<HDF5ArrayIterator>
            (std::weak_ptr<const HDF5Array>(shared_ptr), attr, _query);
}

int64_t HDF5Array::getMaxElementsInChunk(bool withOverlap) const
{
    using namespace std;
    auto& dims = getArrayDesc().getDimensions();
    size_t result = 1;
    for(auto& dim : dims) {
        if(withOverlap) {
            result *= dim.getChunkInterval();
        } else {
            result *= dim.getChunkInterval() + dim.getChunkOverlap();
        }
    }
    return result;
}

int64_t HDF5Array::getMaxElementsInChunk(Coordinates start, bool withOverlap) const
{
    using namespace std;
    auto& dims = getArrayDesc().getDimensions();
    size_t result = 1;
    auto i = 0;
    for(auto& dim : dims) {
        auto dimInterval = dim.getChunkInterval();
        if(withOverlap) dimInterval += dim.getChunkOverlap();
        if(dim.getEndMax() - start[i] + 1 < dimInterval)
            dimInterval = dim.getEndMax() - start[i] + 1;
        result *= dimInterval;
        ++i;
    }
    return result;
}

}
} // namespace scidb

