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
#include <util/BitManip.h>

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

        for (const NamesPairType &nameAlias : _names)
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
        else {
            std::string qualifiedAlias;
            if(!ArrayDesc::isQualifiedArrayName(alias))
            {
                qualifiedAlias = ArrayDesc::makeQualifiedArrayName("public", alias);
            }

            return
                ((*nameIt).second.find(alias) != (*nameIt).second.end()) ||
                ((*nameIt).second.find(qualifiedAlias) != (*nameIt).second.end());
        }
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
 * Class ArrayDesc
 */

ArrayDesc::ArrayDesc() :
    _arrId(0),
    _uAId(0),
    _versionId(0),
    _bitmapAttr(NULL),
    _flags(0)
{}


ArrayDesc::ArrayDesc(const std::string &namespaceName,
                     const std::string &arrayName,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     const ArrayDistPtr& arrDist,
                     const ArrayResPtr& arrRes,
                     int32_t flags) :
    _arrId(0),
    _uAId(0),
    _versionId(0),
    _namespaceName(namespaceName),
    _arrayName(arrayName),
    _attributes(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _distribution(arrDist),
    _residency(arrRes)
{
    splitQualifiedArrayName(arrayName, _namespaceName, _arrayName);

#ifndef SCIDB_CLIENT
    SCIDB_ASSERT(arrDist);
    SCIDB_ASSERT(arrRes);
    SCIDB_ASSERT(isValidPartitioningSchema(arrDist->getPartitioningSchema())); // temporary restriction while scaffolding erected for #4546
#endif
    locateBitmapAttribute();
    initializeDimensions();
}

ArrayDesc::ArrayDesc(const std::string &arrayName,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     const ArrayDistPtr& arrDist,
                     const ArrayResPtr& arrRes,
                     int32_t flags) :
    _arrId(0),
    _uAId(0),
    _versionId(0),
    _namespaceName("public"),
    _arrayName(arrayName),
    _attributes(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _distribution(arrDist),
    _residency(arrRes)
{
    splitQualifiedArrayName(arrayName, _namespaceName, _arrayName);

#ifndef SCIDB_CLIENT
    SCIDB_ASSERT(arrDist);
    SCIDB_ASSERT(arrRes);
    SCIDB_ASSERT(isValidPartitioningSchema(arrDist->getPartitioningSchema())); // temporary restriction while scaffolding erected for #4546
#endif
    locateBitmapAttribute();
    initializeDimensions();
}

ArrayDesc::ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId,
                     const std::string &namespaceName,
                     const std::string &arrayName,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     const ArrayDistPtr& arrDist,
                     const ArrayResPtr& arrRes,
                     int32_t flags)
:
    _arrId(arrId),
    _uAId(uAId),
    _versionId(vId),
    _namespaceName(namespaceName),
    _arrayName(arrayName),
    _attributes(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _distribution(arrDist),
    _residency(arrRes)
{
    splitQualifiedArrayName(arrayName, _namespaceName, _arrayName);

#ifndef SCIDB_CLIENT
    SCIDB_ASSERT(arrDist);
    SCIDB_ASSERT(arrRes);
    SCIDB_ASSERT(isValidPartitioningSchema(arrDist->getPartitioningSchema())); // temporary restriction while scaffolding erected for #4546
#endif
    // either both 0 or not...
    assert(arrId == 0 || uAId != 0);

    locateBitmapAttribute();
    initializeDimensions();
}


ArrayDesc::ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId,
                     const std::string &arrayName,
                     const Attributes& attributes,
                     const Dimensions &dimensions,
                     const ArrayDistPtr& arrDist,
                     const ArrayResPtr& arrRes,
                     int32_t flags)
:
    _arrId(arrId),
    _uAId(uAId),
    _versionId(vId),
    _namespaceName("public"),
    _arrayName(arrayName),
    _attributes(attributes),
    _dimensions(dimensions),
    _flags(flags),
    _distribution(arrDist),
    _residency(arrRes)
{
    splitQualifiedArrayName(arrayName, _namespaceName, _arrayName);

#ifndef SCIDB_CLIENT
    SCIDB_ASSERT(arrDist);
    SCIDB_ASSERT(arrRes);
    SCIDB_ASSERT(isValidPartitioningSchema(arrDist->getPartitioningSchema())); // temporary restriction while scaffolding erected for #4546
#endif
    // either both 0 or not...
    assert(arrId == 0 || uAId != 0);

    locateBitmapAttribute();
    initializeDimensions();
}

ArrayDesc::ArrayDesc(ArrayDesc const& other) :
    _arrId(other._arrId),
    _uAId(other._uAId),
    _versionId(other._versionId),
    _namespaceName(other._namespaceName),
    _arrayName(other._arrayName),
    _attributes(other._attributes),
    _attributesWithoutBitmap(other._attributesWithoutBitmap),
    _dimensions(other._dimensions),
    _bitmapAttr(other._bitmapAttr != NULL ? &_attributes[other._bitmapAttr->getId()] : NULL),
    _flags(other._flags),
    _distribution(other._distribution),
    _residency(other._residency)
{
    splitQualifiedArrayName(other._arrayName, _namespaceName, _arrayName);
    initializeDimensions();
}

void ArrayDesc::splitQualifiedArrayName(
    const std::string &     qualifiedArrayName,
    std::string &           namespaceName,
    std::string &           arrayName)
{
    std::string::size_type pos = qualifiedArrayName.find(".");
    if(pos != std::string::npos) {
        namespaceName = qualifiedArrayName.substr(0, pos);
        arrayName = qualifiedArrayName.substr(pos+1, qualifiedArrayName.length());
    } else {
        // The namespaceName is not set if the qualifiedArrayName is not qualified.
        arrayName = qualifiedArrayName;
    }
}

std::string ArrayDesc::getUnqualifiedArrayName(
    const std::string & arrayName)
{
    std::string::size_type pos = arrayName.find(".");
    if(pos != std::string::npos) {
        return arrayName.substr(pos+1, arrayName.length());
    }

    return arrayName;
}

std::string ArrayDesc::makeQualifiedArrayName(
    const std::string &           namespaceNameIn,
    const std::string &           arrayNameIn)
{
	if(arrayNameIn == "") return arrayNameIn;
    std::string arrayName = arrayNameIn;

    if(isQualifiedArrayName(arrayName))
    {
        std::string namespaceName = namespaceNameIn;
        splitQualifiedArrayName(arrayNameIn, namespaceName, arrayName);
        if(namespaceName != namespaceNameIn)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ARRAY_NAME_ALREADY_QUALIFIED)
                  << namespaceName << arrayName;
        }

        return arrayNameIn;
    }

    std::stringstream ss;
    ss << namespaceNameIn << std::string(".") << arrayName;
    return ss.str();
}

bool ArrayDesc::isQualifiedArrayName(
    const std::string &           arrayName)
{
    return (arrayName.find('.') != std::string::npos);
}

std::string ArrayDesc::getQualifiedArrayName() const
{
    if(isQualifiedArrayName(_arrayName) || (_namespaceName==""))
    {
        return _arrayName;
    }

    return makeQualifiedArrayName(_namespaceName, _arrayName);
}

bool ArrayDesc::operator==(ArrayDesc const& other) const
{
    bool same =
       _namespaceName == other._namespaceName &&
       _arrayName == other._arrayName &&
       _attributes == other._attributes &&
       _dimensions == other._dimensions &&
       _flags == other._flags;

    if (!same) {
        return false;
    }
    if (_distribution && other._distribution) {
        same = _distribution->checkCompatibility(other._distribution);
    } else if (_distribution || other._distribution) {
        return false;
    }
    if (!same) {
        return false;
    }
    if (_residency && other._residency) {
        same = _residency->isEqual(other._residency);
    } else if (_residency || other._residency) {
        return false;
    }
    return same;
}

ArrayDesc& ArrayDesc::operator=(ArrayDesc const& other)
{
    _arrId = other._arrId;
    _uAId = other._uAId;
    _versionId = other._versionId;
    _namespaceName = other._namespaceName;
    _arrayName = other._arrayName;
    _attributes = other._attributes;
    _attributesWithoutBitmap = other._attributesWithoutBitmap;
    _dimensions = other._dimensions;
    _bitmapAttr = (other._bitmapAttr != NULL) ? &_attributes[other._bitmapAttr->getId()] : NULL;
    _flags = other._flags;
    initializeDimensions();
    _distribution = other._distribution;
    _residency = other._residency;
    return *this;
}

bool ArrayDesc::isNameVersioned(std::string const& name)
{
    if (name.empty())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
              << "calling isNameVersioned on an empty string";
    }

    size_t const locationOfAt = name.find('@');
    size_t const locationOfColon = name.find(':');
    return locationOfAt > 0 && locationOfAt < name.size() && locationOfColon == std::string::npos;
}

bool ArrayDesc::isNameUnversioned(std::string const& name)
{
    if (name.empty())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
              << "calling isNameUnversioned on an empty string";
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
        // If unknown (autochunked) interval, use 1 so the calculation is as close as it can be.
        Coordinate chunkLength = _dimensions[i].getChunkIntervalIfAutoUse(1);
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
        SCIDB_ASSERT(dim._chunkInterval > 0);
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

InstanceID ArrayDesc::getPrimaryInstanceId(Coordinates const& pos,
                                           size_t instanceCount) const
{
    SCIDB_ASSERT(_distribution);
    return _distribution->getPrimaryChunkLocation(pos, _dimensions, instanceCount);
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
    getChunkPositionFor(_dimensions, pos);
}

void ArrayDesc::getChunkPositionFor(Dimensions const& dims, Coordinates& pos)
{
    for (size_t i = 0, n = pos.size(); i < n; i++) {
        if ( dims[i].getRawChunkInterval() > 0) {
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

bool ArrayDesc::isAutochunked() const
{
    for (DimensionDesc const& dim : _dimensions) {
        if (dim.getRawChunkInterval() == DimensionDesc::AUTOCHUNKED) {
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

void ArrayDesc::addAlias(const std::string& alias)
{
    for (AttributeDesc& attr : _attributes)
    {
        attr.addAlias(alias);
    }

    for (AttributeDesc& attr : _attributesWithoutBitmap)
    {
        attr.addAlias(alias);
    }

    for (DimensionDesc& dim : _dimensions)
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
             coords[i] > _dimensions[i].getEndMax() )
        {
            return false;
        }

        if (_dimensions[i].isAutochunked())
        {
            if (coords[i] != _dimensions[i].getStartMin())
            {
                // We have no way of knowing, unless the coordinate is
                // at the very beginning of the dimension range.
                return false;
            }
        }
        else
        {
            if ( (coords[i] - _dimensions[i].getStartMin()) % _dimensions[i].getChunkInterval() != 0 )
            {
                return false;
            }
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
    return ceil(
        (static_cast<double>(end) - static_cast<double>(start) + 1.0)
        / static_cast<double>(_dimensions[dimension].getChunkInterval()));
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
        !(srcDesc.getDistribution()->checkCompatibility(dstDesc.getDistribution()) &&
          srcDesc.getResidency()->isEqual(dstDesc.getResidency())))
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

bool ArrayDesc::areNamesUnique() const
{
    set<string> names;
    for (auto const& attr : _attributes) {
        auto iter_and_inserted = names.insert(attr.getName());
        if (!iter_and_inserted.second) {
            return false;
        }
    }
    for (auto const& dim : _dimensions) {
        auto iter_and_inserted = names.insert(dim.getBaseName());
        if (!iter_and_inserted.second) {
            return false;
        }
    }
    return true;
}

// Determine if 'this' and the 'other' schema match based on the
// selection critera.  So far, only dimension-based criteria are implemented.
bool ArrayDesc::sameSchema(ArrayDesc const& other, SchemaFieldSelector const &sel) const
{
    Dimensions::const_iterator itOther, itThis;
    Dimensions const& dimsOther = other.getDimensions();

    // Same dimension count, or else some inferSchema() method failed to forbid this!
    ASSERT_EXCEPTION(dimsOther.size() == _dimensions.size(), "Dimension count mismatch");

    for(itOther = dimsOther.begin(),      itThis = _dimensions.begin();
        (itOther != dimsOther.end());
        ++itOther,                        ++itThis)
    {
        assert(itThis != _dimensions.end());
        if(sel.chunkOverlap()  && ((*itOther).getChunkOverlap()     != (*itThis).getChunkOverlap()))     return false;
        if(sel.chunkInterval() && ((*itOther).getRawChunkInterval() != (*itThis).getRawChunkInterval())) return false;
        if(sel.startMin()      && ((*itOther).getStartMin()         != (*itThis).getStartMin()))         return false;
        if(sel.endMax()        && ((*itOther).getEndMax()           != (*itThis).getEndMax()))           return false;
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
    ASSERT_EXCEPTION(dimsOther.size() == _dimensions.size(), "Dimension count mismatch");

    for(itOther = dimsOther.begin(),      itThis = _dimensions.begin();
        (itOther != dimsOther.end())  &&  (itThis != _dimensions.end());
        ++itOther,                        ++itThis)
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

    stream << ob.getQualifiedArrayName()
           << '<' << ob.getAttributes(true)
           << "> [" << ob.getDimensions() << ']';

#ifndef SCIDB_CLIENT
    stream << " ArrayId: " << ob.getId();
    stream << " UnversionedArrayId: " << ob.getUAId();
    stream << " Version: " << ob.getVersionId();
    stream << " Flags: " << ob.getFlags();
    stream << " Distro: " << RedistributeContext(ob.getDistribution(),
                                                 ob.getResidency());
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
        static_cast<int16_t>(Config::getInstance()->getOption<int>(CONFIG_CHUNK_RESERVE))
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
    _flags(static_cast<int16_t>(flags | (type ==  TID_INDICATOR ? IS_EMPTY_INDICATOR : 0))),
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
    _flags(static_cast<int16_t>(flags | (type ==  TID_INDICATOR ? IS_EMPTY_INDICATOR : 0))),
    _defaultCompressionMethod(defaultCompressionMethod),
    _reserve(
#ifndef SCIDB_CLIENT
        safe_static_cast<int16_t>(Config::getInstance()->getOption<int>(CONFIG_CHUNK_RESERVE))
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
    else {
        std::string qualifiedAlias;
        if(!ArrayDesc::isQualifiedArrayName(alias))
        {
            qualifiedAlias = ArrayDesc::makeQualifiedArrayName("public", alias);
        }

        return
            (_aliases.find(alias)          != _aliases.end()) ||
            (_aliases.find(qualifiedAlias) != _aliases.end());
    }
}

TypeId AttributeDesc::getType() const
{
    return _type;
}

int16_t AttributeDesc::getFlags() const
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
 * @param[out] output stream to write to
 * @param[in] indent number of spacer characters to start every line with.
 */
void AttributeDesc::toString(std::ostream &os, int indent) const
{
    if (indent > 0)
    {
        os << std::string(indent,' ');
    }

    os << "[attDesc] id " << _id
       << " name " << _name
       << " aliases {";

    for (const std::string& alias : _aliases)
    {
        os << _name << "." << alias << ", ";
    }

    os << "} type " << _type
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
    //Don't print NULL because nullable is the default behavior.
    stream << att.getName() << ':' << att.getType()
           << (isAnyOn<int>(att.getFlags(), AttributeDesc::IS_NULLABLE) ? "" : " NOT NULL");
    try
    {
        if (!isAnyOn<int>(att.getFlags(), AttributeDesc::IS_NULLABLE) &&  // not nullable, and
            !isDefaultFor(att.getDefaultValue(),att.getType()))           // not equal to the type's default value
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
        stream << " COMPRESSION '"
               << CompressorFactory::getInstance().getCompressors()[att.getDefaultCompressionMethod()]->getName()
               << "'";
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
     * Check for empty array.  According to informal agreement, high
     * boundary for empty array is CoordinateBounds::getMin() and low
     * is ...::getMax().
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
 * @param[out] os output stream to write to
 * @param[in] indent number of spacer characters to start every line with.
 */
void DimensionDesc::toString (std::ostream& os, int indent) const
{
    if (indent > 0)
    {
        os << std::string(indent, ' ');
    }

    os << "[dimDesc] names " << _names
       << " startMin " << _startMin
       << " currStart " << _currStart
       << " currEnd " << _currEnd
       << " endMax " << _endMax
       << " chnkInterval " << _chunkInterval
       << " chnkOverlap " << _chunkOverlap
       << '\n';
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
    // XXX Use ';' to separate?
    return insertRange(stream,dims,',');
}

std::ostream& operator<<(std::ostream& os, const DimensionDesc& dim)
{
    printSchema(os, dim, /*verbose:*/true);
    return os;
}

void printSchema(std::ostream& os, const DimensionDesc& dim, bool verbose)
{
    // Stringify numbers that may need special representation like '*'.
    // (Low bounds do not use any such notation, by design.)

    stringstream end;
    if (dim.getEndMax() == CoordinateBounds::getMax()) {
        end << '*';
    } else {
        end << dim.getEndMax();
    }

    stringstream currEnd;
    if (verbose) {
        if (dim.getCurrEnd() == CoordinateBounds::getMax()) {
            currEnd << '*';
        } else {
            currEnd << dim.getCurrEnd();
        }
    }

    // Emit the dimension name.  In verbose mode we want the aliases too.
    if (verbose) {
        os << dim.getNamesAndAliases();
    } else {
        printNames(os, dim.getNamesAndAliases());
    }

    os << '=' << dim.getStartMin() << ':' << end.str();
    if (verbose) {
        os << " (" << dim.getCurrStart() << ':' << currEnd.str() << ')';
    }
    os << ',';

    int64_t interval = dim.getRawChunkInterval();
    if (interval == DimensionDesc::AUTOCHUNKED) {
        os << "*,";
    } else {
        os << interval << ',';
    }

    os << dim.getChunkOverlap();
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

} // namespace
