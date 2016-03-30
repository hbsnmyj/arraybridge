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

#include <array/Metadata.h>
#include <array/ArrayDistribution.h>
#include <util/PointerRange.h>
#include <query/Operator.h>

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

namespace scidb {

InstanceID getInstanceForChunk(Coordinates const& chunkPosition,
                               Dimensions const& dims,
                               ArrayDistPtr const& outputAdist,
                               ArrayResPtr const& outputAres,
                               std::shared_ptr<Query> const& query)
{
    InstanceID location = outputAdist->getPrimaryChunkLocation(chunkPosition,
                                                               dims,
                                                               outputAres->size());
    if (location == ALL_INSTANCE_MASK) {
        //XXX TODO: this only works if the residency is the same as
        //XXX TODO: the default query residency because
        //XXX TODO: SG interprets ALL_INSTANCE_MASK wrt the default residency
        //XXX TODO: However, it should cause only extra data movement (to non-participating instances),
        //XXX TODO: but still preserve correctness.
        return location;
    }
    InstanceID physicalInstanceId = outputAres->getPhysicalInstanceAt(location);

    InstanceID logicalInstanceId = query->mapPhysicalToLogical(physicalInstanceId);
    return logicalInstanceId;
}

InstanceID getInstanceForChunk(Coordinates const& chunkPosition,
                               Dimensions const& dims,
                               ArrayDistPtr const& outputAdist,
                               std::shared_ptr<Query> const& query)
{
    InstanceID queryLogicalInstance = outputAdist->getPrimaryChunkLocation(chunkPosition,
                                                                           dims,
                                                                           query->getInstancesCount());
    return queryLogicalInstance;
}

uint64_t
HashedArrayDistribution::getHashedChunkNumber(scidb::Dimensions const& dims,
                                              scidb::Coordinates const& pos)
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


InstanceID
ByColumnArrayDistribution::getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                                   Dimensions const& dims,
                                                   size_t nInstances) const
{
    InstanceID destInstanceId = 0;
    if (dims.size() > 1)
    {
        SCIDB_ASSERT(nInstances >0);
        SCIDB_ASSERT(dims[1].getChunkInterval() >0);

        const uint64_t dim1Length = dims[1].getLength();
        destInstanceId = ((chunkPosition)[1] - dims[1].getStartMin()) / dims[1].getChunkInterval()
        / (((dim1Length + dims[1].getChunkInterval() - 1) / dims[1].getChunkInterval() + nInstances - 1) / nInstances);
    }

    return destInstanceId % nInstances;
}

InstanceID
ByRowArrayDistribution::getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                                Dimensions const& dims,
                                                size_t nInstances) const
{

    const uint64_t dim0Length = dims[0].getLength();
    InstanceID destInstanceId = ((chunkPosition)[0] - dims[0].getStartMin()) / dims[0].getChunkInterval()
    / (((dim0Length + dims[0].getChunkInterval() - 1) / dims[0].getChunkInterval() + nInstances - 1) / nInstances);

    return destInstanceId % nInstances;
}

namespace dist {
/**
 * ArrayDistribution wrapper distribution which applies transformation(s) on the inputs and/or outputs of another distribution.
 * It is not intended as a public interface because it might introduce an unnecessary level of complexity.
 * Currently, it is used to support the "offsetable" property used by/for SubArray distribution.
 * The hope is that the need for this class will disappear when we overhaul SubArray.
 */
class TranslatedArrayDistribution : public ArrayDistribution
{
 public:
    /**
     * Constructor
     * @param arrDist input distribution for which to use translation
     * @param instanceShift the output of the input distribution is (modulo)added this amount
     * @param ct the coordinate position is translated using this object  before being fed
     *        into the input distribution
     */
    TranslatedArrayDistribution (ArrayDistPtr& arrDist,
                                 size_t instanceShift,
                                 const std::shared_ptr<CoordinateTranslator>& ct)
    :  ArrayDistribution(arrDist->getPartitioningSchema(), arrDist->getRedundancy()),
       _inputArrDist(arrDist), _instanceShift(instanceShift), _coordinateTranslator(ct)
    {
    }

    virtual InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                               Dimensions const& dims,
                                               size_t nInstances) const ;

    virtual bool checkCompatibility(const ArrayDistPtr& otherArrDist) const ;

    virtual std::string getContext() const
    {
        // FYI: SystemCatalog guards against storing TranslatedArrayDistribution
        return _inputArrDist->getContext();
    }

    const CoordinateTranslator* getTranslator() const
    {
        return _coordinateTranslator.get();
    }
    size_t getInstanceShift() const { return _instanceShift;  }

 private:
    ArrayDistPtr _inputArrDist;
    size_t _instanceShift;
    std::shared_ptr<CoordinateTranslator> _coordinateTranslator;
    mutable Coordinates _mappedChunkPos;
};

InstanceID
TranslatedArrayDistribution::getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                                     Dimensions const& dims,
                                                     size_t nInstances) const
{
    const Coordinates *chunkPos = &chunkPosition;

    if (_coordinateTranslator)
    {
        _mappedChunkPos = _coordinateTranslator->translate(chunkPosition);
        chunkPos = &_mappedChunkPos;
    }

    InstanceID destInstanceId = _inputArrDist->getPrimaryChunkLocation(*chunkPos,
                                                                       dims,
                                                                       nInstances);
    return (destInstanceId + _instanceShift) % nInstances;
}

bool
TranslatedArrayDistribution::checkCompatibility(const ArrayDistPtr& otherArrDist) const
{
    SCIDB_ASSERT(otherArrDist);
    const TranslatedArrayDistribution* other =
       dynamic_cast<const TranslatedArrayDistribution*>(otherArrDist.get());
    if (other==NULL) {
        return false;
    }

    if (!_inputArrDist->checkCompatibility(other->_inputArrDist)) {
        return false;
    }

    if (_instanceShift != other->_instanceShift) {
        return false;
    }
    const OffsetCoordinateTranslator* thisTranslator  =
       dynamic_cast<const OffsetCoordinateTranslator*>(_coordinateTranslator.get());
    const OffsetCoordinateTranslator* otherTranslator =
       dynamic_cast<const OffsetCoordinateTranslator*>(other->_coordinateTranslator.get());
    return ((thisTranslator == NULL &&
            otherTranslator == NULL)  ||
            (thisTranslator != NULL &&
             otherTranslator != NULL &&
             *thisTranslator == *otherTranslator));
}

} // namespace dist

void ArrayDistributionFactory::getTranslationInfo(const ArrayDistribution* arrDist,
                                                  Coordinates& offset,
                                                  InstanceID& instanceShift)
{
    instanceShift = 0;
    offset.clear();
    const dist::TranslatedArrayDistribution *translated =
       dynamic_cast<const dist::TranslatedArrayDistribution*>(arrDist);
    if (translated) {
        instanceShift = translated->getInstanceShift();
        if (translated->getTranslator()) {
            const OffsetCoordinateTranslator* translator =
            safe_dynamic_cast<const OffsetCoordinateTranslator*>(translated->getTranslator());
            const DimensionVector& coords = translator->getOffsetVector();
            offset = coords;
        }
    }
}

std::shared_ptr<CoordinateTranslator>
ArrayDistributionFactory::createOffsetTranslator(const Coordinates& offset)
{
    if (offset.empty()) {
        return std::shared_ptr<CoordinateTranslator>();
    }
    return OffsetCoordinateTranslator::createOffsetMapper(offset);
}

ArrayResPtr createDefaultResidency(PointerRange<InstanceID> instances)
{
    ArrayResPtr residency = std::make_shared<MapArrayResidency>(instances.begin(),instances.end());
    return residency;
}

std::ostream& operator<<(std::ostream& stream, const ArrayDistribution& dist)
{
    Coordinates offset;
    InstanceID instanceShift(0);
    ArrayDistributionFactory::getTranslationInfo(&dist, offset, instanceShift);

    stream << " ps: " << dist.getPartitioningSchema();
    stream << " ctx: " << dist.getContext();
    stream << " redun: " << dist.getRedundancy();
    stream << " off: " << offset;
    stream << " shift: " << instanceShift;
    return stream;
}

std::ostream& operator<<(std::ostream& stream, const ArrayResidency& res)
{
    stream<<"[";
    const size_t nInstances = res.size();
    for (size_t i = 0; i < nInstances; ++i) {
        if (i>0) {
            stream << ", ";
        }
        stream << res.getPhysicalInstanceAt(i);
    }
    stream<<"]";
    return stream;
}

ArrayDistPtr ArrayDistributionFactory::construct(PartitioningSchema ps,
                                                 size_t redundancy,
                                                 const std::string& ctx,
                                                 const std::shared_ptr<CoordinateTranslator>& ct,
                                                 size_t instanceShift)
{
    SCIDB_ASSERT (!_constructors.empty());

    const KeyType key(ps);
    FactoryMap::iterator iter = _constructors.find(key);

    if (iter == _constructors.end()) {
        ASSERT_EXCEPTION_FALSE("Unknown array distirbution type");
        return ArrayDistPtr();
    }

    ArrayDistPtr arrDist = (iter->second)(ps, redundancy, ctx);

    if (instanceShift!=0 || ct) {
        return std::make_shared<dist::TranslatedArrayDistribution> (arrDist, instanceShift, ct);
    }
    return arrDist;
}

void
ArrayDistributionFactory::registerConstructor(PartitioningSchema ps,
                                              const ArrayDistributionConstructor& constructor)
{
    SCIDB_ASSERT(constructor);
    SCIDB_ASSERT(ps>=psMIN && ps<psEND );

    const KeyType key(ps);

    std::pair<FactoryMap::iterator, bool> res =
    _constructors.insert(FactoryMap::value_type(key, constructor));
    if (!res.second) {
        SCIDB_ASSERT(false);
        std::stringstream ss;
        ss << "ArrayDistributionFactory::registerConstructor(" << ps <<")";
        throw SYSTEM_EXCEPTION(SCIDB_SE_TYPESYSTEM, SCIDB_LE_OPERATION_FAILED) << ss.str();
    }
}

void
ArrayDistributionFactory::registerBuiltinDistributions()
{
    ArrayDistributionConstructor adc(boost::bind(&ArrayDistributionFactory::defaultConstructor <
                                                 HashedArrayDistribution >, _1, _2, _3));
    registerConstructor(psHashPartitioned, adc);

    adc = boost::bind(&ArrayDistributionFactory::defaultConstructor <
                      ReplicatedArrayDistribution >, _1, _2, _3);
    registerConstructor(psReplication, adc);

    adc = boost::bind(&ArrayDistributionFactory::defaultConstructor <
                      LocalArrayDistribution >, _1, _2, _3);
    registerConstructor(psLocalInstance, adc);

    adc = boost::bind(&ArrayDistributionFactory::defaultConstructor <
                      ByRowArrayDistribution >, _1, _2, _3);
    registerConstructor(psByRow, adc);

    adc = boost::bind(&ArrayDistributionFactory::defaultConstructor <
                      ByColumnArrayDistribution >, _1, _2, _3);
    registerConstructor(psByCol, adc);

    adc = boost::bind(&ArrayDistributionFactory::defaultConstructor <
                      GroupByArrayDistribution >, _1, _2, _3);
    registerConstructor(psGroupBy, adc);

    adc = boost::bind(&ArrayDistributionFactory::defaultConstructor <
                      UndefinedArrayDistribution >, _1, _2, _3);
    registerConstructor(psUndefined, adc);
}

RedistributeContext::RedistributeContext(const ArrayDistPtr& arrDist,
                                         const ArrayResPtr& arrRes)
  : _arrDistribution(arrDist), _arrResidency(arrRes), _translated(NULL)
{

    ASSERT_EXCEPTION(_arrDistribution, "Invalid array distribution");
    ASSERT_EXCEPTION(_arrResidency,  "Invalid array residency");

    if(!isValidPartitioningSchema(_arrDistribution->getPartitioningSchema(), true)) {
        ASSERT_EXCEPTION_FALSE("RedistributeContext: invalid PartitioningSchema");
    }
    _translated = dynamic_cast<const dist::TranslatedArrayDistribution*>(_arrDistribution.get());

    SCIDB_ASSERT(_arrDistribution->getPartitioningSchema() != psUndefined || _translated == NULL);
}

RedistributeContext::RedistributeContext(const RedistributeContext& other)
: _arrDistribution(other._arrDistribution), _arrResidency(other._arrResidency)
{
    ASSERT_EXCEPTION(_arrDistribution, "Invalid array distribution");
    ASSERT_EXCEPTION(_arrResidency,  "Invalid array residency");

    _translated = dynamic_cast<const dist::TranslatedArrayDistribution*>(_arrDistribution.get());
    SCIDB_ASSERT(_translated == other._translated);

    SCIDB_ASSERT(_arrDistribution->getPartitioningSchema() != psUndefined || _translated == NULL);
}

RedistributeContext&
RedistributeContext::operator= (const RedistributeContext& rhs)
{
    if (this != &rhs) {
        ASSERT_EXCEPTION(rhs._arrDistribution, "Invalid array distribution");
        ASSERT_EXCEPTION(rhs._arrResidency, "Invalid array distribution");

        _arrDistribution = rhs._arrDistribution;
        _arrResidency = rhs._arrResidency;
        _translated = dynamic_cast<const dist::TranslatedArrayDistribution*>(_arrDistribution.get());
        SCIDB_ASSERT(_translated == rhs._translated);
    }
    ASSERT_EXCEPTION(_arrDistribution, "Invalid array distribution");
    ASSERT_EXCEPTION(_arrResidency,  "Invalid array residency");

    return *this;
}

bool
RedistributeContext::operator== (RedistributeContext const& other) const
{
    if (this != &other) {
        SCIDB_ASSERT(other._arrDistribution);
        if (!_arrDistribution->checkCompatibility(other._arrDistribution)) {
            return false;
        }
        if (_arrResidency && !other._arrResidency) { return false; }
        if (!_arrResidency && other._arrResidency) { return false; }
        if (_arrResidency && other._arrResidency &&
            !_arrResidency->isEqual(other._arrResidency)) { return false; }
    }
    return true;
}

const CoordinateTranslator*
RedistributeContext::getMapper() const
{
    if (_translated!=NULL) {
        return static_cast<const dist::TranslatedArrayDistribution* const>(_translated)->getTranslator();
    }
    return NULL;
}

std::ostream& operator<<(std::ostream& stream, const RedistributeContext& dist)
{
    stream<<"dist: ";

    if (dist._arrDistribution) {

        switch (dist.getArrayDistribution()->getPartitioningSchema())
        {
        case psReplication:     stream<<"repl";
                                break;
        case psHashPartitioned: stream<<"hash";
                                break;
        case psLocalInstance:   stream<<"loca";
                                break;
        case psByRow:           stream<<"byro";
                                break;
        case psByCol:           stream<<"byco";
                                break;
        case psUndefined:       stream<<"undefined";
                                break;
        case psGroupBy:         stream<<"groupby";
                                break;
        case psScaLAPACK:       stream<<"ScaLAPACK";
                                break;
        default:
        SCIDB_ASSERT( false);
        }

        stream << (*dist.getArrayDistribution());

    } else {
        stream << "default";
    }

    stream << " res: ";

    if (dist._arrResidency) {
        stream << (*dist.getArrayResidency());
    } else {
        stream << "default";
    }

    return stream;
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

} // namespace scidb
