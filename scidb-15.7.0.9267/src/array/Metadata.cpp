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
 * @file Metadata.cpp
 *
 * @brief Structures for fetching and updating cluster metadata.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */
#include <sstream>
#include <boost/foreach.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem/path.hpp>
#include <log4cxx/logger.h>

#ifndef SCIDB_CLIENT
#include <system/Config.h>
#endif

#include <util/PointerRange.h>
#include <system/SciDBConfigOptions.h>
#include <query/TypeSystem.h>
#include <array/ArrayDistribution.h>
#include <array/Metadata.h>
#include <system/SystemCatalog.h>
#include <system/Utils.h>
#include <smgr/io/Storage.h>
#include <array/Compressor.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.metadata"));

ObjectNames::ObjectNames()
{}

ObjectNames::ObjectNames(const std::string &baseName):
    _baseName(baseName)
{
    addName(baseName);
}

ObjectNames::ObjectNames(const std::string &baseName, const NamesType &names):
    _names(names),
    _baseName(baseName)
{}

void ObjectNames::addName(const std::string &name)
{
    string trimmedName = name;
    boost::algorithm::trim(trimmedName);
    assert(trimmedName != "");

    if (hasNameAndAlias(name))
        return;

    _names[name] = set<string>();
}

void ObjectNames::addAlias(const std::string &alias, const std::string &name)
{
    if (!alias.empty())
    {
        string trimmedAlias = alias;
        boost::algorithm::trim(trimmedAlias);
        assert(trimmedAlias != "");

        string trimmedName = name;
        boost::algorithm::trim(trimmedName);
        assert(trimmedName != "");

        _names[name].insert(alias);
    }
}

void ObjectNames::addAlias(const std::string &alias)
{
    if (!alias.empty())
    {
        string trimmedAlias = alias;
        boost::algorithm::trim(trimmedAlias);
        assert(trimmedAlias != "");

        BOOST_FOREACH(const NamesPairType &nameAlias, _names)
        {
            _names[nameAlias.first].insert(alias);
        }
    }
}

bool ObjectNames::hasNameAndAlias(const std::string &name, const std::string &alias) const
{
    NamesType::const_iterator nameIt = _names.find(name);

    if (nameIt != _names.end())
    {
        if (alias.empty())
            return true;
        else
            return ( (*nameIt).second.find(alias) != (*nameIt).second.end() );
    }

    return false;
}

const ObjectNames::NamesType& ObjectNames::getNamesAndAliases() const
{
    return _names;
}

const std::string& ObjectNames::getBaseName() const
{
    return _baseName;
}

bool ObjectNames::operator==(const ObjectNames &o) const
{
    return _names == o._names;
}

std::ostream& operator<<(std::ostream& stream,const ObjectNames::NamesType::value_type& pair)
{
    stream << pair.first;
    return insertRange(stream,pair.second,", ");
}

std::ostream& operator<<(std::ostream& stream, const ObjectNames::NamesType &ob)
{
    return insertRange(stream,ob,", ");
}

void printNames (std::ostream& stream, const ObjectNames::NamesType &ob)
{
    for (ObjectNames::NamesType::const_iterator nameIt = ob.begin(); nameIt != ob.end(); ++nameIt)
    {
        if (nameIt != ob.begin())
        {
            stream << ", ";
        }
        stream << (*nameIt).first;
    }
}

/*
 * Class DimensionVector
 */

DimensionVector& DimensionVector::operator+=(const DimensionVector& rhs)
{
    if (isEmpty())
    {
        _data = rhs._data;
    }
    else
    if (!rhs.isEmpty())
    {
        assert(numDimensions() == rhs.numDimensions());

        for (size_t i=0, n=numDimensions(); i!=n; ++i)
        {
            _data[i] += rhs._data[i];
        }
    }

    return *this;
}

DimensionVector& DimensionVector::operator-=(const DimensionVector& rhs)
{
    if (!isEmpty() && !rhs.isEmpty())
    {
        assert(numDimensions() == rhs.numDimensions());

        for (size_t i=0, n=numDimensions(); i!=n; ++i)
        {
            _data[i] -= rhs._data[i];
        }
    }

    return *this;
}
/**
 * Retrieve a human-readable description.
 * Append a human-readable description of this onto str. Description takes up
 * one or more lines. Append indent spacer characters to the beginning of
 * each line.
 * @param[out] str buffer to write to
 * @param[in] indent number of spacer characters to start every line with.
 */
void DimensionVector::toString (std::ostringstream &str, int indent) const
{
    if (indent > 0)
    {
        str << std::string(indent,' ');
    }

    if (isEmpty())
    {
        str << "[empty]";
    }
    else
    {
        str << '[';
        insertRange(str,_data,' ');
        str << ']';
    }
}

/*
 * Class ArrayDesc
 */

ArrayDesc::ArrayDesc() :
    _arrId(0),
    _uAId(0),
    _versionId(0),
    _bitmapAttr(NULL),
    _flags(0),
    _ps(psUninitialized)
{}


ArrayDesc::ArrayDesc(const std::string &name,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     PartitioningSchema ps,
                     int32_t flags) :
    _arrId(0),
    _uAId(0),
    _versionId(0),
    _name(name),
    _attributes(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _ps(ps)
{
    assert(isPermitted(ps)); // temporary restriction while scaffolding erected for #4546

    locateBitmapAttribute();
    initializeDimensions();
}

ArrayDesc::ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId,
                     const std::string &name,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     PartitioningSchema ps,
                     int32_t flags)
:
    _arrId(arrId),
    _uAId(uAId),
    _versionId(vId),
    _name(name),
    _attributes(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _ps(ps)
{
    assert(isPermitted(ps)); // temporary restriction while scaffolding erected for #4546

    // either both 0 or not...
    assert(arrId == 0 || uAId != 0);

    locateBitmapAttribute();
    initializeDimensions();
}

ArrayDesc::ArrayDesc(ArrayDesc const& other) :
    _arrId(other._arrId),
    _uAId(other._uAId),
    _versionId(other._versionId),
    _name(other._name),
    _attributes(other._attributes),
    _attributesWithoutBitmap(other._attributesWithoutBitmap),
    _dimensions(other._dimensions),
    _bitmapAttr(other._bitmapAttr != NULL ? &_attributes[other._bitmapAttr->getId()] : NULL),
    _flags(other._flags),
    _ps(other._ps) // TODO: this my be invoked with other._ps = psUninitialized
{
    initializeDimensions();
}

bool ArrayDesc::operator==(ArrayDesc const& other) const
{
    return
        _name == other._name &&
        _ps == other._ps &&
        _attributes == other._attributes &&
        _dimensions == other._dimensions &&
        _flags == other._flags;
}

ArrayDesc& ArrayDesc::operator=(ArrayDesc const& other)
{
    _arrId = other._arrId;
    _uAId = other._uAId;
    _versionId = other._versionId;
    _name = other._name;
    _attributes = other._attributes;
    _attributesWithoutBitmap = other._attributesWithoutBitmap;
    _dimensions = other._dimensions;
    _bitmapAttr = (other._bitmapAttr != NULL) ? &_attributes[other._bitmapAttr->getId()] : NULL;
    _flags = other._flags;
    initializeDimensions();
    _ps = other._ps; // TODO: this does get invoked with other._ps = psUninitialized
    return *this;
}

bool ArrayDesc::isNameVersioned(std::string const& name)
{
    if (name.empty())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "calling isNameVersioned on an empty string";
    }

    size_t const locationOfAt = name.find('@');
    size_t const locationOfColon = name.find(':');
    return locationOfAt > 0 && locationOfAt < name.size() && locationOfColon == std::string::npos;
}

bool ArrayDesc::isNameUnversioned(std::string const& name)
{
    if (name.empty())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "calling isNameUnversioned on an empty string";
    }

    size_t const locationOfAt = name.find('@');
    size_t const locationOfColon = name.find(':');
    return locationOfAt == std::string::npos && locationOfColon == std::string::npos;
}

void ArrayDesc::initializeDimensions()
{
    Coordinate logicalChunkSize = 1;
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        _dimensions[i]._array = this;
        Coordinate chunkLength = _dimensions[i].getChunkInterval();
        Coordinate t = chunkLength + _dimensions[i].getChunkOverlap();
        if ( t < chunkLength ) //overflow check
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }
        chunkLength = t;
        t = chunkLength + _dimensions[i].getChunkOverlap();
        if ( t < chunkLength) //overflow check again
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }

        t = logicalChunkSize * chunkLength;
        if (chunkLength != 0 && t / chunkLength != logicalChunkSize) //overflow check again
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }
        logicalChunkSize = t;
    }
}

void ArrayDesc::trim()
{
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        DimensionDesc& dim = _dimensions[i];
        if (dim._startMin == CoordinateBounds::getMin() && dim._currStart != CoordinateBounds::getMax()) {
            dim._startMin = dim._currStart;
        }
        if (dim._endMax == CoordinateBounds::getMax() && dim._currEnd != CoordinateBounds::getMin()) {
            dim._endMax = (dim._startMin + (dim._currEnd - dim._startMin + dim._chunkInterval) / dim._chunkInterval * dim._chunkInterval + dim._chunkOverlap - 1);
        }
    }
}

Coordinates ArrayDesc::getLowBoundary() const
{
    assert(!_dimensions.empty());
    Coordinates low(_dimensions.size());
    for (size_t i = 0, n = _dimensions.size(); i < n; ++i) {
        const DimensionDesc& dim = _dimensions[i];
        low[i] = dim.getCurrStart();
    }
    return low;
}

Coordinates ArrayDesc::getHighBoundary() const
{
    assert(!_dimensions.empty());
    Coordinates high(_dimensions.size());
    for (size_t i = 0, n = _dimensions.size(); i < n; ++i) {
        const DimensionDesc& dim = _dimensions[i];
        high[i] = dim.getCurrEnd();
    }
    return high;
}


InstanceID ArrayDesc::getPrimaryInstanceId(Coordinates const& chunkPosition,
                                           size_t nInstances) const      // const critical
{
    // redirect to the master definition of the chunk mapping function for persistent arrays

    assert(isPermitted(_ps));

    // TODO: psLocalInstance is set by e.g. LogicalInput.
    //       but we don't want it in ArrayDesc() because the
    //       ability to evaluate this method then depends on Query.
    //       Storage manager does not have Query in all cases where it calls
    //       this method, ergo, psLocalInstance better not be persisted.
    //       At the moment, ArrayDesc::getPrimaryInstanceId() isn't called
    //       on ArrayDescs with psLocalInstance, so we have some time to sort
    //       this out:
    //          a) should all callers have query? [problem for storage manager?]
    //          b) should current instance be stored in ArrayDesc with _ps ?
    //             [problem for sending ArrayDesc to other instances]
    //          c) avoid putting psLocalInstance into ArrayDesc? [problem for LogicalInput.cpp]
    //          d) another solution.
    assert(_ps != psLocalInstance);

    assert(_ps != psUndefined);

    //
    // NOTE: the current method is const.  that is essential so that the
    // third argument to getPrimaryInstancesForChunk() will call
    // getDimensions() const, not getDimensions() [which has unacceptable malloc overhead]
    // next line it is getDimensions() const" that is called, otherwise this call would
    //
    return getPrimaryInstanceForChunk(_ps, chunkPosition, getDimensions(), nInstances);
}

ssize_t ArrayDesc::findDimension(const std::string& name, const std::string& alias) const
{
    const ssize_t N_DIMS = _dimensions.size();
    for (ssize_t i = 0; i < N_DIMS; ++i) {
        if (_dimensions[i].hasNameAndAlias(name, alias)) {
            return i;
        }
    }
    return -1;
}

bool ArrayDesc::contains(Coordinates const& pos) const
{
    Dimensions const& dims = _dimensions;
    for (size_t i = 0, n = pos.size(); i < n; i++) {
        if (pos[i] < dims[i].getStartMin() || pos[i] > dims[i].getEndMax()) {
            return false;
        }
    }
    return true;
}

void ArrayDesc::getChunkPositionFor(Coordinates& pos) const
{
    Dimensions const& dims = _dimensions;
    for (size_t i = 0, n = pos.size(); i < n; i++) {
        if ( dims[i].getChunkInterval() != 0) {
            Coordinate diff = (pos[i] - dims[i].getStartMin()) % dims[i].getChunkInterval();

            // The code below ensures the correctness of this code, in case pos[i] < dims[i].getStartMin().
            // Example:
            //   - Suppose dimStart=0, chunkInterval=5. All chunkPos should be a multiple of 5.
            //   - Given pos[i]=-9, we desire to reduce it to -10.
            //   - The calculated diff = -4.
            //   - The step below changes diff to a non-negative number of 1, bedore using it to decrease pos[i].
            if (diff < 0) {
                diff += dims[i].getChunkInterval();
            }

            pos[i] -= diff;
        }
    }
}

bool ArrayDesc::isAChunkPosition(Coordinates const& pos) const
{
    Coordinates chunkPos = pos;
    getChunkPositionFor(chunkPos);
    return coordinatesCompare(pos, chunkPos) == 0;
}

bool ArrayDesc::isCellPosInChunk(Coordinates const& cellPos, Coordinates const& chunkPos) const
{
    Coordinates chunkPosForCell = cellPos;
    getChunkPositionFor(chunkPosForCell);
    return coordinatesCompare(chunkPosForCell, chunkPos) == 0;
}

void ArrayDesc::getChunkBoundaries(Coordinates const& chunkPosition,
                                   bool withOverlap,
                                   Coordinates& lowerBound,
                                   Coordinates& upperBound) const
{
#ifndef NDEBUG
    do
    {
        Coordinates alignedChunkPosition = chunkPosition;
        getChunkPositionFor(alignedChunkPosition);
        SCIDB_ASSERT(alignedChunkPosition == chunkPosition);
    }
    while(false);
#endif /* NDEBUG */
    Dimensions const& d = getDimensions();
    Dimensions::size_type const n = d.size();
    SCIDB_ASSERT(n == chunkPosition.size());
    lowerBound = chunkPosition;
    upperBound = chunkPosition;
    for (size_t i = 0; i < n; i++) {
        upperBound[i] += d[i].getChunkInterval() - 1;
    }
    if (withOverlap) {
        for (size_t i = 0; i < n; i++) {
            lowerBound[i] -= d[i].getChunkOverlap();
            upperBound[i] += d[i].getChunkOverlap();
        }
    }
    for (size_t i = 0; i < n; ++i) {
        lowerBound[i] = std::max(lowerBound[i], d[i].getStartMin());
        upperBound[i] = std::min(upperBound[i], d[i].getEndMax());
    }
}

void ArrayDesc::locateBitmapAttribute()
{
    _bitmapAttr = NULL;
    _attributesWithoutBitmap = _attributes;
    for (size_t i = 0, n = _attributes.size(); i < n; i++) {
        if (_attributes[i].getType() ==  TID_INDICATOR) {
            _bitmapAttr = &_attributes[i];
            _attributesWithoutBitmap.erase(_attributesWithoutBitmap.begin() + i);
        }
    }
}

uint64_t ArrayDesc::getSize() const
{
    uint64_t size = 1;
    //uint64_t max = std::numeric_limits<uint64_t>::max();
    for (size_t i = 0, n = _dimensions.size(); i < n; i++)
    {
        uint64_t length = _dimensions[i].getLength();
        //check for uint64_t overflow

        // As soon as we encounter one dimension with a '*' give up
        // and return maxLength.
        // Or, when length * size > getMaxLength() return getMaxLength()
        if (_dimensions[i].isMaxStar() || length > CoordinateBounds::getMaxLength() / size)
        {
            return CoordinateBounds::getMaxLength();
        }
        size *= length;
    }
    return size;
}


void ArrayDesc::cutOverlap()
{
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        _dimensions[i]._chunkOverlap = 0;
    }
}

bool ArrayDesc::hasOverlap() const
{
    for (size_t i = 0, n = _dimensions.size(); i < n; i++) {
        if (_dimensions[i].getChunkOverlap() != 0) {
            return true;
        }
    }
    return false;
}

Dimensions ArrayDesc::grabDimensions(VersionID version) const
{
    Dimensions dims(_dimensions.size());
    for (size_t i = 0; i < dims.size(); i++) {
        DimensionDesc const& dim = _dimensions[i];
        dims[i] = dim;
    }
    return dims;
}

void ArrayDesc::addAlias(const std::string &alias)
{
    BOOST_FOREACH(AttributeDesc &attr, _attributes)
    {
        attr.addAlias(alias);
    }

    BOOST_FOREACH(AttributeDesc &attr, _attributesWithoutBitmap)
    {
        attr.addAlias(alias);
    }

    BOOST_FOREACH(DimensionDesc &dim, _dimensions)
    {
        dim.addAlias(alias);
    }
}

bool ArrayDesc::coordsAreAtChunkStart(Coordinates const& coords) const
{
    if (coords.size() != _dimensions.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DIMENSIONS_MISMATCH);

    for (size_t i = 0; i < coords.size(); i++ )
    {
       if ( coords[i] < _dimensions[i].getStartMin() ||
            coords[i] > _dimensions[i].getEndMax() ||
            (coords[i] - _dimensions[i].getStartMin()) % _dimensions[i].getChunkInterval() != 0 )
       {
           return false;
       }
    }
    return true;
}

bool ArrayDesc::coordsAreAtChunkEnd(Coordinates const& coords) const
{
    if (coords.size() != _dimensions.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DIMENSIONS_MISMATCH);

    for (size_t i = 0; i < coords.size(); i++ )
    {
        if ( coords[i] != _dimensions[i].getEndMax() &&
             (coords[i] < _dimensions[i].getStartMin() ||
              coords[i] > _dimensions[i].getEndMax() ||
              (coords[i] + 1 - _dimensions[i].getStartMin()) % _dimensions[i].getChunkInterval() != 0 ))
        {
            return false;
        }
    }
    return true;
}

void ArrayDesc::addAttribute(AttributeDesc const& newAttribute)
{
    assert(newAttribute.getId() == _attributes.size());
    for (size_t i = 0; i< _dimensions.size(); i++)
    {
        if (_dimensions[i].getBaseName() == newAttribute.getName() || newAttribute.hasAlias(_dimensions[i].getBaseName()))
        {
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME) << newAttribute.getName();
        }
    }

    for (size_t i = 0; i < _attributes.size(); i++)
    {
        if (_attributes[i].getName() == newAttribute.getName())
        {
            throw USER_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_DUPLICATE_ATTRIBUTE_NAME) << newAttribute.getName();
        }
    }
    _attributes.push_back(newAttribute);
    if (newAttribute.getType() == TID_INDICATOR)
    {
        assert(_bitmapAttr == NULL);
        _bitmapAttr = &_attributes[_attributes.size()-1];
    }
    else
    {
        _attributesWithoutBitmap.push_back(newAttribute);
    }
}

double ArrayDesc::getNumChunksAlongDimension(size_t dimension, Coordinate start, Coordinate end) const
{
    assert(dimension < _dimensions.size());
    if(start==CoordinateBounds::getMax() && end ==CoordinateBounds::getMin())
    {
        start = _dimensions[dimension].getStartMin();
        end = _dimensions[dimension].getEndMax();
    }
    return ceil((end * 1.0 - start + 1.0) / _dimensions[dimension].getChunkInterval());
}

bool ArrayDesc::isPermitted(PartitioningSchema ps) const
{

    return (ps == psHashPartitioned ||
            ps == psByRow ||
            ps == psByCol ||
            ps == psReplication ||
            ps == psUndefined ||
            ps == psLocalInstance);
    // TODO: Deal with psLocalInstance differently.
    //       It is currently set on ArrayDesc
    //       by e.g. LogicalInput(), but is incompatible with
    //       ArrayDesc::getPrimaryInstanceId() [see that for more details]
    //       Note special assert in that method to disallow it
}

size_t getChunkNumberOfElements(Coordinates const& low, Coordinates const& high)
{
    size_t M = size_t(-1);
    size_t ret = 1;
    assert(low.size()==high.size());
    for (size_t i=0; i<low.size(); ++i) {
        assert(high[i] >= low[i]);
        size_t interval = high[i] - low[i] + 1;
        if (M/ret < interval) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE);
        }
        ret *= interval;
    }
    return ret;
}

// Can src array be stored/inserted into dst array?
void ArrayDesc::checkConformity(ArrayDesc const& srcDesc, ArrayDesc const& dstDesc, unsigned options)
{
    if (!(options & IGNORE_PSCHEME) &&
        srcDesc.getPartitioningSchema() != dstDesc.getPartitioningSchema())
    {
        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
            << "Target of INSERT/STORE must have same distribution as the source";
    }

    Dimensions const& srcDims = srcDesc.getDimensions();
    Dimensions const& dstDims = dstDesc.getDimensions();

    if (srcDims.size() != dstDims.size())
    {
        //TODO: this will get lifted when we allow redimension+insert in the same op
        //and when we DO implement redimension+insert - we will need to match
        // attributes/dimensions by name, not position.
        LOG4CXX_DEBUG(logger, "Source and target array descriptors are not conformant:"
                      << srcDesc.toString() << ", " << dstDesc.toString());
        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSION_COUNT_MISMATCH)
              << "INSERT/STORE" << srcDims.size() << dstDims.size();
    }

    const bool checkOverlap = !(options & IGNORE_OVERLAP);
    const bool checkInterval = !(options & IGNORE_INTERVAL);
    for (size_t i = 0, n = srcDims.size(); i < n; i++)
    {
        if( (checkInterval && srcDims[i].getChunkInterval() != dstDims[i].getChunkInterval()) ||
            (checkOverlap  && srcDims[i].getChunkOverlap()  != dstDims[i].getChunkOverlap()) )
        {
            LOG4CXX_ERROR(logger, "Source and target array descriptors are not conformant"
                          <<" in chunk interval or overlap:"
                          << srcDesc.toString() << ", " << dstDesc.toString());

            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_DIMENSIONS_DONT_MATCH)
                  << srcDims[i].getBaseName() << dstDims[i].getBaseName();
        }
        if (srcDims[i].getStartMin() != dstDims[i].getStartMin()) {
            ostringstream oss;
            oss << '[' << srcDims[i] << "] != [" << dstDims[i] << ']';
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_START_INDEX_MISMATCH)
                  << oss.str();
        }
        if (srcDims[i].getEndMax() == dstDims[i].getEndMax()) {
            // Dimension matches, all is cool.
            continue;
        }
        else if (srcDims[i].getEndMax() > dstDims[i].getEndMax()) {
            if (srcDims[i].getEndMax() == CoordinateBounds::getMax()) {
                // Go ahead and try to inject S[i=0:*,...] into D[j=0:N,...] on the expectation
                // that S doesn't actually have any data beyond N.  (If it does, fail when you
                // know that for sure, not now.)
                continue;
            }
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INCORRECT_DIMENSION_BOUNDARY)
                << dstDims[i].getStartMin() << dstDims[i].getEndMax();
        }
        else if ((options & SHORT_OK_IF_EBM) && srcDesc.getEmptyBitmapAttribute() != NULL) {
            // The source dimension can be shorter if the source array has an empty bitmap.
            continue;

            // One day all arrays will have empty bitmaps.  Then the question becomes, is the
            // check for partial source chunks (below) ever useful?
        }
        else if (srcDims[i].getLength() % srcDims[i].getChunkInterval() != 0) {
            // The source dimension can be shorter if there are no partial source chunks.
            ostringstream oss;
            oss << "Source array for INSERT/STORE must have uniform chunk sizes";
            if (options & SHORT_OK_IF_EBM) {
                oss << " (for non-emptyable arrays)";
            }
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
                << oss.str();
        }
    }

    Attributes const& srcAttrs = srcDesc.getAttributes(true);
    Attributes const& dstAttrs = dstDesc.getAttributes(true);

    if (srcAttrs.size() != dstAttrs.size())
    {
        LOG4CXX_ERROR(logger, "Source and target array descriptors are not conformant:"
                      << srcDesc.toString() << ", " << dstDesc.toString());

        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT)
        << "Target of INSERT/STORE must have same attributes as the source";
    }
    for (size_t i = 0, n = srcAttrs.size(); i < n; i++)
    {
        if(srcAttrs[i].getType() != dstAttrs[i].getType())
        {
            LOG4CXX_ERROR(logger, "Source and target array descriptors are not conformant:"
                          << srcDesc.toString() << ", " << dstDesc.toString());

            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ATTRIBUTE_TYPE)
            << srcAttrs[i].getName() << srcAttrs[i].getType() << dstAttrs[i].getType();
        }

        //can't store nulls into a non-nullable attribute
        if(!dstAttrs[i].isNullable() && srcAttrs[i].isNullable())
        {
            LOG4CXX_ERROR(logger, "Source and target array descriptors are not conformant:"
                          << srcDesc.toString() << ", " << dstDesc.toString());
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_ATTRIBUTE_FLAGS)
            << srcAttrs[i].getName();
        }
    }
}

// Determine if 'this' and the 'other' schema match based on the
// selection critera.
bool ArrayDesc::sameSchema(ArrayDesc const& other, SchemaFieldSelector const &sel) const
{
    Dimensions::const_iterator itOther, itThis;
    Dimensions const& dimsOther = other.getDimensions();

    // Same dimension count, or else some inferSchema() method failed to forbid this!
    SCIDB_ASSERT(dimsOther.size() == _dimensions.size());

    for(itOther = dimsOther.begin(),      itThis = _dimensions.begin();
        (itOther != dimsOther.end())  &&  (itThis != _dimensions.end());
        itOther++,                        itThis++)
    {
        if(sel.chunkOverlap()  && ((*itOther).getChunkInterval() != (*itThis).getChunkInterval())) return false;
        if(sel.chunkInterval() && ((*itOther).getChunkOverlap()  != (*itThis).getChunkOverlap()))  return false;
        if(sel.startMin()      && ((*itOther).getStartMin()      != (*itThis).getStartMin()))      return false;
        if(sel.endMax()        && ((*itOther).getEndMax()        != (*itThis).getEndMax()))        return false;
    }

    return true;
}

// Replace this schema's dimension values with those from another schema
void ArrayDesc::replaceDimensionValues(ArrayDesc const& other)
{
    Dimensions::iterator        itThis;
    Dimensions::const_iterator  itOther;
    Dimensions const& dimsOther = other.getDimensions();

    // Same dimension count, or else some inferSchema() method failed to forbid this!
    SCIDB_ASSERT(dimsOther.size() == _dimensions.size());

    for(itOther = dimsOther.begin(),      itThis = _dimensions.begin();
        (itOther != dimsOther.end())  &&  (itThis != _dimensions.end());
        itOther++,                        itThis++)
    {
        (*itThis).replaceValues((*itOther));
    }
}

void printSchema(std::ostream& stream,const ArrayDesc& ob)
{
#ifndef SCIDB_CLIENT
    if (Config::getInstance()->getOption<bool>(CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT)) {
        if (ob.getEmptyBitmapAttribute() == NULL) {
            stream << "not empty ";
        }
    } else {
        if (ob.getEmptyBitmapAttribute() != NULL) {
            stream << "empty ";
        }
    }
#endif
    stream << ob.getName()
           << '<' << ob.getAttributes(true)
           << "> [";
    printSchema(stream, ob.getDimensions());
    stream << ']';
}

std::ostream& operator<<(std::ostream& stream,const ArrayDesc& ob)
{
#ifndef SCIDB_CLIENT
    if (Config::getInstance()->getOption<bool>(CONFIG_ARRAY_EMPTYABLE_BY_DEFAULT)) {
        if (ob.getEmptyBitmapAttribute() == NULL) {
            stream << "not empty ";
        }
    } else {
        if (ob.getEmptyBitmapAttribute() != NULL) {
            stream << "empty ";
        }
    }
#endif

    stream << ob.getName()
           << '<' << ob.getAttributes(true)
           << "> [" << ob.getDimensions() << ']';

#ifndef SCIDB_CLIENT
    stream << " ArrayId: " << ob.getId();
    stream << " UnversionedArrayId: " << ob.getUAId();
    stream << " Version: " << ob.getVersionId();
    stream << " Flags: " << ob.getFlags();
    stream << " PartitioningSchema: " << ob.getPartitioningSchema();
    stream << " <" << ob.getAttributes(false) << ">" ;
#endif

    return stream;
}

std::string ArrayDesc::toString () const
{
    stringstream ss;
    ss << (*this);
    return ss.str();
}

/*
 * Class AttributeDesc
 */
AttributeDesc::AttributeDesc() :
    _id(0),
    _type( TypeId( TID_VOID)),
    _flags(0),
    _defaultCompressionMethod(0),
    _reserve(
#ifndef SCIDB_CLIENT
        Config::getInstance()->getOption<int>(CONFIG_CHUNK_RESERVE)
#else
        0
#endif
     ),
    _varSize(0)
{}

AttributeDesc::AttributeDesc(AttributeID id,
                             const std::string &name,
                             TypeId type,
                             int16_t flags,
                             uint16_t defaultCompressionMethod,
                             const std::set<std::string> &aliases,
                             int16_t reserve,
                             Value const* defaultValue,
                             const string &defaultValueExpr,
                             size_t varSize):
    _id(id),
    _name(name),
    _aliases(aliases),
    _type(type),
    _flags(flags | (type ==  TID_INDICATOR ? IS_EMPTY_INDICATOR : 0)),
    _defaultCompressionMethod(defaultCompressionMethod),
    _reserve(reserve),
    _varSize(varSize),
    _defaultValueExpr(defaultValueExpr)
{
    if (defaultValue != NULL) {
        _defaultValue = *defaultValue;
    } else {
        _defaultValue = Value(TypeLibrary::getType(type));
        if (flags & IS_NULLABLE) {
            _defaultValue.setNull();
        } else {
            _defaultValue = TypeLibrary::getDefaultValue(type);
        }
    }
    if(name == DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME &&  (_flags & AttributeDesc::IS_EMPTY_INDICATOR) == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Empty tag attribute name misuse";
    }
    if(name != DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME && (_flags & AttributeDesc::IS_EMPTY_INDICATOR))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Empty tag attribute not named properly";
    }
}

AttributeDesc::AttributeDesc(AttributeID id, const std::string &name,  TypeId type, int16_t flags,
        uint16_t defaultCompressionMethod, const std::set<std::string> &aliases,
        Value const* defaultValue,
        const string &defaultValueExpr,
        size_t varSize) :
    _id(id),
    _name(name),
    _aliases(aliases),
    _type(type),
    _flags(flags | (type ==  TID_INDICATOR ? IS_EMPTY_INDICATOR : 0)),
    _defaultCompressionMethod(defaultCompressionMethod),
    _reserve(
#ifndef SCIDB_CLIENT
        Config::getInstance()->getOption<int>(CONFIG_CHUNK_RESERVE)
#else
        0
#endif
    ),
    _varSize(varSize),
    _defaultValueExpr(defaultValueExpr)
{
    if (defaultValue != NULL) {
        _defaultValue = *defaultValue;
    } else {
        _defaultValue = Value(TypeLibrary::getType(type));
        if (flags & IS_NULLABLE) {
            _defaultValue.setNull();
        } else {
            _defaultValue = TypeLibrary::getDefaultValue(type);
        }
    }
    if(name == DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME &&  (_flags & AttributeDesc::IS_EMPTY_INDICATOR) == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Empty tag attribute name misuse";
    }
    if(name != DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME && (_flags & AttributeDesc::IS_EMPTY_INDICATOR))
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Empty tag attribute not named properly";
    }
}

bool AttributeDesc::operator==(AttributeDesc const& other) const
{
    return
        _id == other._id &&
        _name == other._name &&
        _aliases == other._aliases &&
        _type == other._type &&
        _flags == other._flags &&
        _defaultCompressionMethod == other._defaultCompressionMethod &&
        _reserve == other._reserve &&
        _defaultValue == other._defaultValue &&
        _varSize == other._varSize &&
        _defaultValueExpr == other._defaultValueExpr;
}

AttributeID AttributeDesc::getId() const
{
    return _id;
}

const std::string& AttributeDesc::getName() const
{
    return _name;
}

const std::set<std::string>& AttributeDesc::getAliases() const
{
    return _aliases;
}

void AttributeDesc::addAlias(const string& alias)
{
    string trimmedAlias = alias;
    boost::algorithm::trim(trimmedAlias);
    _aliases.insert(trimmedAlias);
}

bool AttributeDesc::hasAlias(const std::string& alias) const
{
    if (alias.empty())
        return true;
    else
        return (_aliases.find(alias) != _aliases.end());
}

TypeId AttributeDesc::getType() const
{
    return _type;
}

int AttributeDesc::getFlags() const
{
    return _flags;
}

bool AttributeDesc::isNullable() const
{
    return (_flags & IS_NULLABLE) != 0;
}

bool AttributeDesc::isEmptyIndicator() const
{
    return (_flags & IS_EMPTY_INDICATOR) != 0;
}

uint16_t AttributeDesc::getDefaultCompressionMethod() const
{
    return _defaultCompressionMethod;
}

Value const& AttributeDesc::getDefaultValue() const
{
    return _defaultValue;
}

int16_t AttributeDesc::getReserve() const
{
    return _reserve;
}

size_t AttributeDesc::getSize() const
{
    Type const& type = TypeLibrary::getType(_type);
    return type.byteSize() > 0 ? type.byteSize() : getVarSize();
}

size_t AttributeDesc::getVarSize() const
{
    return _varSize;
}

const std::string& AttributeDesc::getDefaultValueExpr() const
{
    return _defaultValueExpr;
}

/**
 * Retrieve a human-readable description.
 * Append a human-readable description of this onto str. Description takes up
 * one or more lines. Append indent spacer characters to the beginning of
 * each line. Call toString on interesting children. Terminate with newline.
 * @param[out] str buffer to write to
 * @param[in] indent number of spacer characters to start every line with.
 */
void AttributeDesc::toString(std::ostringstream &str, int indent) const
{
    if (indent > 0)
    {
        str<<std::string(indent,' ');
    }

    str<< "[attDesc] id " << _id
       << " name " << _name
       << " aliases {";

    BOOST_FOREACH(const std::string& alias,_aliases)
    {
        str << _name << "." << alias << ", ";
    }

    str<< "} type " << _type
       << " flags " << _flags
       << " compression " << _defaultCompressionMethod
       << " reserve " << _reserve
       << " default " << _defaultValue.toString(_type);
}

std::ostream& operator<<(std::ostream& stream,const Attributes& atts)
{
    return insertRange(stream,atts,',');
}

std::ostream& operator<<(std::ostream& stream, const AttributeDesc& att)
{
    //don't print NOT NULL because it default behaviour
    stream << att.getName() << ':' << att.getType()
           << (att.getFlags() & AttributeDesc::IS_NULLABLE ? " NULL" : "");
    try
    {
        if (!isDefaultFor(att.getDefaultValue(),att.getType()))
        {
            stream << " DEFAULT " << att.getDefaultValue().toString(att.getType());
        }
    }
    catch (const SystemException &e)
    {
        if (e.getLongErrorCode() != SCIDB_LE_TYPE_NOT_REGISTERED)
        {
            e.raise();
        }

        stream << " DEFAULT UNKNOWN";
    }
    if (att.getDefaultCompressionMethod() != CompressorFactory::NO_COMPRESSION)
    {
        stream << " COMPRESSION '" << CompressorFactory::getInstance().getCompressors()[att.getDefaultCompressionMethod()]->getName() << "'";
    }
    return stream;
}

/*
 * Class DimensionDesc
 */

DimensionDesc::DimensionDesc() :
    ObjectNames(),

    _startMin(0),
    _currStart(0),
    _currEnd(0),
    _endMax(0),

    _chunkInterval(0),
    _chunkOverlap(0),
    _array(NULL)
{
    validate();
}

DimensionDesc::DimensionDesc(const std::string &name, Coordinate start, Coordinate end, int64_t chunkInterval,
                             int64_t chunkOverlap) :
    ObjectNames(name),

    _startMin(start),
    _currStart(CoordinateBounds::getMax()),
    _currEnd(CoordinateBounds::getMin()),
    _endMax(end),

    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap),
    _array(NULL)
{
    validate();
}

DimensionDesc::DimensionDesc(const std::string &baseName, const NamesType &names, Coordinate start, Coordinate end,
                             int64_t chunkInterval, int64_t chunkOverlap) :
    ObjectNames(baseName, names),

    _startMin(start),
    _currStart(CoordinateBounds::getMax()),
    _currEnd(CoordinateBounds::getMin()),
    _endMax(end),

    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap),
    _array(NULL)
{
    validate();
}

DimensionDesc::DimensionDesc(const std::string &name, Coordinate startMin, Coordinate currStart, Coordinate currEnd,
                             Coordinate endMax, int64_t chunkInterval, int64_t chunkOverlap) :
    ObjectNames(name),

    _startMin(startMin),
    _currStart(currStart),
    _currEnd(currEnd),
    _endMax(endMax),
    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap),
    _array(NULL)
{
    validate();
}

DimensionDesc::DimensionDesc(const std::string &baseName, const NamesType &names, Coordinate startMin,
                             Coordinate currStart, Coordinate currEnd, Coordinate endMax, int64_t chunkInterval, int64_t chunkOverlap) :
    ObjectNames(baseName, names),

    _startMin(startMin),
    _currStart(currStart),
    _currEnd(currEnd),
    _endMax(endMax),
    _chunkInterval(chunkInterval),
    _chunkOverlap(chunkOverlap),
    _array(NULL)
{
    validate();
}

bool DimensionDesc::operator==(DimensionDesc const& other) const
{
    return
        _names == other._names &&
        _startMin == other._startMin &&
        _endMax == other._endMax &&
        _chunkInterval == other._chunkInterval &&
        _chunkOverlap == other._chunkOverlap;
}

uint64_t DimensionDesc::getCurrLength() const
{
    Coordinate low = _startMin;
    Coordinate high = _endMax;
#ifndef SCIDB_CLIENT
    if (_startMin == CoordinateBounds::getMin() || _endMax == CoordinateBounds::getMax()) {
        if (_array->getId() != 0) {
            size_t index = this - &_array->_dimensions[0];
            if (_startMin == CoordinateBounds::getMin()) {
                low = SystemCatalog::getInstance()->getLowBoundary(_array->getId())[index];
            }
            if (_endMax == CoordinateBounds::getMax()) {
                high = SystemCatalog::getInstance()->getHighBoundary(_array->getId())[index];
            }
        } else {
            low = _currStart;
            high = _currEnd;
        }
    }
#endif
    /*
     * check for empty array - according to informal agreement,
     * high boundary for empty array is CoordinateBounds::getMax()
     */
    if (low == CoordinateBounds::getMax() || high == CoordinateBounds::getMin()) {
        return 0;
    } else {
        return high - low + 1;
    }
}

/**
 * Retrieve a human-readable description.
 * Append a human-readable description of this onto str. Description takes up
 * one or more lines. Append indent spacer characters to the beginning of
 * each line. Call toString on interesting children. Terminate with newline.
 * @param[out] str buffer to write to
 * @param[in] indent number of spacer characters to start every line with.
 */
void DimensionDesc::toString (std::ostringstream &str,int indent) const
{
    if (indent > 0)
    {
        str<<std::string(indent,' ');
    }

    str<<"[dimDesc] names "<<_names
       <<" startMin "<<_startMin
       <<" currStart "<<_currStart
       <<" currEnd "<<_currEnd
       <<" endMax "<<_endMax
       <<" chnkInterval "<<_chunkInterval
       <<" chnkOverlap "<<_chunkOverlap
       << "\n";
}

void DimensionDesc::validate() const
{
    if (_startMin > _endMax)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_HIGH_SHOULDNT_BE_LESS_LOW);
    }

    if(_startMin < CoordinateBounds::getMin())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_MIN_TOO_SMALL)
            << _startMin
            << CoordinateBounds::getMin();
    }

    if(_endMax > CoordinateBounds::getMax())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYNTAX, SCIDB_LE_MAX_TOO_LARGE)
            << _endMax
            << CoordinateBounds::getMax();
    }
}

void printSchema(std::ostream& stream,const Dimensions& dims)
{
    for (size_t i=0,n=dims.size(); i<n; i++)
    {
        printSchema(stream, dims[i]);
        if (i != n-1)
        {
            stream << ',';
        }
    }
}

std::ostream& operator<<(std::ostream& stream,const Dimensions& dims)
{
    return insertRange(stream,dims,',');
}

std::ostream& operator<<(std::ostream& stream,const DimensionDesc& dim)
{
    Coordinate start = dim.getStartMin();
    stringstream ssstart;
    ssstart << start;

    Coordinate end = dim.getEndMax();
    stringstream ssend;
    ssend << end;

    Coordinate currStart = dim.getCurrStart();
    stringstream scstart;
    scstart << currStart;

    Coordinate currEnd = dim.getCurrEnd();
    stringstream scend;
    scend << currEnd;

   // Note:  The negative side does not know about '*' per design
   stream << dim.getNamesAndAliases() << '='
           << ssstart.str() << ':'
           << (end ==  CoordinateBounds::getMax()  ? "*" : ssend.str()) << " "
           << "("
           << scstart.str() << ':'
           << (currEnd ==  CoordinateBounds::getMax()  ? "*" : scend.str())
           << ")"
           << ","
           << dim.getChunkInterval() << "," << dim.getChunkOverlap();
    return stream;
}

void printSchema(std::ostream& stream,const DimensionDesc& dim)
{
    Coordinate start = dim.getStartMin();
    stringstream ssstart;
    ssstart << start;

    Coordinate end = dim.getEndMax();
    stringstream ssend;
    ssend << end;

    printNames(stream, dim.getNamesAndAliases());

   // Note:  The negative side does not know about '*' per design
    stream << '='
           << ssstart.str() << ':'
           << (end == CoordinateBounds::getMax() ? "*" : ssend.str()) << ","
           << dim.getChunkInterval() << "," << dim.getChunkOverlap();
}

void printDimNames(std::ostream& os, Dimensions const& dims)
{
    const size_t N = dims.size();
    for (size_t i = 0; i < N; ++i) {
        if (i) {
            // Alias separator used by printNames is a comma, so use semi-colon.
            os << ';';
        }
        printNames(os, dims[i].getNamesAndAliases());
    }
}

/*
 * Class InstanceDesc
 */
InstanceDesc::InstanceDesc() :
    _instance_id(0),
    _port(0),
    _online(~0)
{}

InstanceDesc::InstanceDesc(const std::string &host, uint16_t port, const std::string &path) :
        _host(host),
        _port(port),
        _online(~0)
{
     boost::filesystem::path p(path);
     _path = p.normalize().string();
}

InstanceDesc::InstanceDesc(uint64_t instance_id, const std::string &host,
                           uint16_t port, uint64_t online, const std::string &path) :
        _instance_id(instance_id),
        _host(host),
        _port(port),
        _online(online)
{
    boost::filesystem::path p(path);
    _path = p.normalize().string();
}

std::ostream& operator<<(std::ostream& stream,const InstanceDesc& instance)
{
    stream << "instance { id = " << instance.getInstanceId()
           << ", host = " << instance.getHost()
           << ", port = " << instance.getPort()
           << ", has been on-line since " << instance.getOnlineSince()
           << ", path = " << instance.getPath();
    return stream;
}

} // namespace
