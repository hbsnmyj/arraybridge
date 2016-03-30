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

#include <iostream>
#include <memory>
#include <sstream>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/operators.hpp>

#include <array/ArrayDistributionInterface.h>
#include <array/ArrayResidency.h>
#include <array/ArrayDistributionUtils.h>
#include <array/Coordinate.h>
#include <array/Metadata.h>
#include <util/Utility.h>

/**
 * @file ArrayDistribution.h
 *
 * This file contains code needed by by several components: the Operators,
 * the Arrays and Storage, the query compiler and optimization, and probably others.
 *
 * It was factored from Operator.cpp to support some generalization of PartitioningSchema.
 * Some of it is old code.
 *
 * Note that changes to this file could potentially affect array storage formats
 * depending on whether any of this information is stored into those formats.
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
 * HashedArrayDistribution::getPrimaryChunkLocation()
 * <br>
 * <br>
 * All data is currently stored with this distribution. But operators emit data in different distributions quite often.
 * For example, ops like cross, cross_join and some linear algebra routines will output data in a completely different
 * distribution. Worse, ops like slice, subarray, repart may emit "partially filled" or "ragged" chunks - just like
 * we do in the algorithm example above.
 * <br>
 * <br>
 * Data whose distribution is so "violated" must be redistributed before it is stored or processed by other ops that
 * need a particular distribution. The functions [pull]RedistributeXXX() are available and are sometimes
 * called directly by the operator (see PhysicalIndexLookup.cpp for example).
 * Other times, the operator simply tells the SciDB optimizer that
 * it requires input data in a particular distribution or outputs data in a particular distribtuion.
 * The optimizer then inserts the appropriate SG() operators.
 * That approach is more advanatageous, as the optimizer is liable to get smarter about delaying or waiving the
 * call to redistributeXXX(). For this purpose, the functions
 * <br> getOutputDistribution(),
 * <br> changedDistribution() and
 * <br> outputFullChunks()
 * are provided. See their use in the Operator class.
 */

namespace scidb
{
    class Query;

// { SG APIs

/// Compute a logical destination instance ID to which a given array chunk belongs.
/// The logical set of instance IDs is the consecutive set of natural numbers
/// {0,...,#live_instances_participating_in_query-1}
/// This method performs two steps:
/// 1. It identifies the physical instance ID from the array residency using the array distribution.
/// 2. It maps the physical instance ID to a logical instance ID using the query liveness set.
/// The physical instances of the destination array residency must be mappable
/// to a subset of the logical instance IDs via the query liveness set.
/// In other words, the physical instances of the array residency must be live.
/// @param chunkPosition chunk coordinates
/// @param dims array dimensions
/// @param outputArrDistribution destination array distribution
/// @param outputArrResidency destination array residency set
/// @param query current query context
/// @return destination instance ID
/// @throws SystemException if the destination instance ID is not live

InstanceID getInstanceForChunk(Coordinates const& chunkPosition,
                               Dimensions const& dims,
                               ArrayDistPtr const& outputArrDistribution,
                               ArrayResPtr const& outputArrResidency,
                               std::shared_ptr<Query> const& query);

/// Compute a logical destination instance ID to which a given array chunk belongs.
/// The logical set of instance IDs is the consecutive set of natural numbers
/// {0,...,#live_instances_participating_in_query-1}
/// This method uses the array distribution to identify the destination instance ID from the query liveness set.
/// @param chunkPosition chunk coordinates
/// @param dims array dimensions
/// @param outputArrDistribution destination array distribution
/// @param query current query context
/// @return destination instance ID
/// @throws SystemException if the destination instance ID is not live

InstanceID getInstanceForChunk(Coordinates const& chunkPosition,
                               Dimensions const& dims,
                               ArrayDistPtr const& outputArrDistribution,
                               std::shared_ptr<Query> const& query);
// SG APIs }

/**
 * Whether a partitioning schema has optional data.
 */
static inline bool doesPartitioningSchemaHaveData(PartitioningSchema ps)
{
    return ps==psGroupBy || ps==psScaLAPACK || ps==psLocalInstance ;
}

/**
 * Whether a partitioning schema is valid
 */
inline bool isValidPartitioningSchema(PartitioningSchema ps, bool allowOptionalData=true)
{
    bool isLegalRange = ((psMIN <= ps) && (ps < psEND));
    if (!isLegalRange)
    {
        return false;
    }

    if (!allowOptionalData && doesPartitioningSchemaHaveData((PartitioningSchema)ps))
    {
        return false;
    }

    return true;
}

/**
 * Syntactic sugar to represent an n-dimensional vector.
 * @deprecated use Coordinates instead
 */
class DimensionVector : boost::equality_comparable<DimensionVector>,
                        boost::additive<DimensionVector>
{
public:
    /**
     * Create a zero-length vector in numDims dimensions.
     * @param[in] numDims number of dimensions
     */
    DimensionVector(size_t numDims = 0)
        : _data(numDims,0)
    {}

    /**
     * Create a vector based on values.
     * @param[in] vector values
     */
    DimensionVector(const Coordinates& values)
        : _data(values)
    {}

    DimensionVector(const DimensionVector& other)
    : _data(other._data)
    {}

    /**
     * Check if this is a "NULL" vector.
     * @return true iff the vector has 0 dimensions.
     */
    bool isEmpty() const
    {
        return _data.empty();
    }

    /**
     * Get the number of dimensions.
     * @return the number of dimensions
     */
    size_t numDimensions() const
    {
        return _data.size();
    }

    Coordinate& operator[] (size_t index)
    {
        return isDebug() ? _data.at(index) : _data[index];
    }

    const Coordinate& operator[] (size_t index) const
    {
        return isDebug() ? _data.at(index) : _data[index];
    }

    DimensionVector& operator+= (const DimensionVector&);
    DimensionVector& operator-= (const DimensionVector&);

    friend bool operator== (const DimensionVector & a, const DimensionVector & b)
    {
        return a._data == b._data;
    }

    void clear()
    {
        _data.clear();
    }

    operator const Coordinates& () const
    {
        return _data;
    }

    template<class Archive>
    void serialize(Archive& a,unsigned version)
    {
        a & _data;
    }

    DimensionVector& operator=(const DimensionVector& other)
    {
       if (this != &other) {
          _data = other._data;
       }
       return (*this);
    }
private:
    Coordinates _data;
};

/**
 * OffsetCoordinateTranslator stores a DimensionVector, and helps shifting a Coordinates by adding the offset.
 * @example
 * Let the offset vector be <4,6>. We have: translate(<1,1>) = <5,7>.
 *
 */
class OffsetCoordinateTranslator : public CoordinateTranslator
{
private:
    // The offset vector.
    DimensionVector _distOffsetVector;

public:

    OffsetCoordinateTranslator(DimensionVector const& offset)
    : _distOffsetVector(offset)
    {
    }

    OffsetCoordinateTranslator(Coordinates const& offset)
    : _distOffsetVector(offset)
    {
    }

    virtual ~OffsetCoordinateTranslator()
    {}

    const DimensionVector& getOffsetVector() const
    {
        return _distOffsetVector;
    }

    Coordinates translate (Coordinates const& input) const
    {
        assert(input.size() == _distOffsetVector.numDimensions());
        Coordinates result;
        result.reserve(input.size());

        for (size_t i = 0 ; i < input.size(); i++ )
        {
            result.push_back(input[i] + _distOffsetVector[i]);
        }

        return result;
    }

    static std::shared_ptr<OffsetCoordinateTranslator> createOffsetMapper(Coordinates const& offset)
    {
        return std::make_shared<OffsetCoordinateTranslator>(offset);
    }

    //careful: this is not commutative
    std::shared_ptr<OffsetCoordinateTranslator> combine(const std::shared_ptr<OffsetCoordinateTranslator>& previous)
    {
        if (previous.get() == NULL)
        {
            return createOffsetMapper(_distOffsetVector);
        }

        DimensionVector newOffset = _distOffsetVector + previous->_distOffsetVector;
        return createOffsetMapper(newOffset);
    }

    friend bool operator== (const OffsetCoordinateTranslator& lhs, const OffsetCoordinateTranslator& rhs)
    {
        return lhs._distOffsetVector == rhs._distOffsetVector;
    }

    friend bool operator!= (const OffsetCoordinateTranslator& lhs, const OffsetCoordinateTranslator& rhs)
    {
        return ! (lhs == rhs);
    }

    friend std::ostream& operator<<(std::ostream& stream, const OffsetCoordinateTranslator& dm)
    {
        stream << "offset [" ;
        for (size_t i = 0; i < dm._distOffsetVector.numDimensions(); i++)
        {
            stream<<dm._distOffsetVector[i]<<" ";
        }
        stream << "]";

        return stream;
    }
};

/// Helper class used by the optimizer for placing the necessary SG nodes into the physical plan.
/// The necessity for the SG nodes is determined by the ArrayDistribution & ArrayResidency reported
/// by each operator in the plan.
class RedistributeContext
{
private:

    ArrayDistPtr _arrDistribution;
    ArrayResPtr _arrResidency;
    const ArrayDistribution* _translated;

    const CoordinateTranslator* getMapper() const;
public:

    /// Default constructor
    RedistributeContext() : _translated(NULL) {}

    /// Destructor
    virtual ~RedistributeContext() {}

    /// Constructor
    RedistributeContext(const ArrayDistPtr& arrDist,
                        const ArrayResPtr& arrRes);

    /// Copy constructor
    RedistributeContext(const RedistributeContext& other);

    /// Assignment operators
    RedistributeContext& operator= (const RedistributeContext& rhs);

    bool operator== (RedistributeContext const& other) const;

    bool operator!= (RedistributeContext const& other) const
    {
        return (! operator==(other));
    }

    /// @return internal array distribution
    ArrayDistPtr getArrayDistribution() const
    {
        ASSERT_EXCEPTION(_arrDistribution, "Attempt to use NULL ArrayDistribution");
        return _arrDistribution;
    }

    /// @return internal array residency
    ArrayResPtr getArrayResidency() const
    {
        ASSERT_EXCEPTION(_arrResidency, "Attempt to use NULL ArrayResidency");
        return _arrResidency;
    }

    /// @return true if the internal distribution is a coordinate-translating one; false otherwise
    /// A coordinate translating distribution uses a CoordinateTranslator to transform the input coordinates
    /// to the getPrimaryChunkLocation() function (corresponding to the partitioning scheme).
    bool hasMapper() const
    {
        return (getMapper()!=NULL);
    }

    /// @return true if the partitioning scheme of the internal array distribution is psUndefined;
    ///         false otherwise
    bool isUndefined() const
    {
        return (getPartitioningSchema() == psUndefined);
    }

    /// @return true if the partitioning scheme of the internal array distribution is psUndefined OR
    ///         if it is a coordinate translating distribution; false otherwise
    bool isViolated() const
    {
        return isUndefined() || hasMapper();
    }

    /// @return the partitioning scheme of the internal array distribution
    PartitioningSchema getPartitioningSchema() const
    {
        return getArrayDistribution()->getPartitioningSchema();
    }

    /// Stream output operator for RedistributeContext
    /// @todo XXX remove friendship
    friend std::ostream& operator<<(std::ostream& stream, const RedistributeContext& dist);
};

/// Stream output operator RedistributeContext
std::ostream& operator<<(std::ostream& stream, const RedistributeContext& dist);

// ArrayDistribution Implementations

/// An ArrayDistribution where the system does not know, in advance, the location of chunks.
/// I.e. there is no defined distribution pattern.
/// This is useful in cases such as importing unorganized data or anywhere
/// where it will be useful to force a redistribution by declaring the current one to be undefined.
class UndefinedArrayDistribution : public ArrayDistribution
{
public:

    UndefinedArrayDistribution(size_t redundancy, const std::string& ctx)
    : ArrayDistribution(psUndefined, DEFAULT_REDUNDANCY)
    {
        SCIDB_ASSERT(redundancy==DEFAULT_REDUNDANCY);
        SCIDB_ASSERT(ctx.empty());
    }

    InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                       Dimensions const& dims,
                                       size_t nInstances) const
    {
        ASSERT_EXCEPTION_FALSE("UndefinedArrayDistribution cannot map chunks to instances");
        return  InstanceID(INVALID_INSTANCE);
    }

    std::string getContext() const
    {
        return std::string();
    }

    virtual bool checkCompatibility(const ArrayDistPtr& otherArrDist) const
    {
        if (!ArrayDistribution::checkPs(otherArrDist)) {
            return false;
        }
        return (dynamic_cast<const UndefinedArrayDistribution*>(otherArrDist.get()) != NULL);
    }
};

/// ArrayDistribution which determines chunk locations
/// by applying a hash function on the chunk coordinates
class HashedArrayDistribution : public ArrayDistribution
{
   public:
    HashedArrayDistribution(size_t redundancy, const std::string& ctx)
    : ArrayDistribution(psHashPartitioned, redundancy)
    {
        SCIDB_ASSERT(ctx.empty());
    }

    virtual InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                               Dimensions const& dims,
                                               size_t nInstances) const
    {
        InstanceID destInstanceId = getHashedChunkNumber(dims, chunkPosition) % nInstances;
        return destInstanceId;
    }

    virtual std::string getContext() const
    {
        return std::string();
    }

    virtual bool checkCompatibility(const ArrayDistPtr& otherArrDist) const
    {
        if (!ArrayDistribution::checkPs(otherArrDist)) {
            return false;
        }
        return (dynamic_cast<const HashedArrayDistribution*>(otherArrDist.get()) != NULL);
    }

    /// @return the hash value corresponding to a hash function
    ///         applied on a given chunk position as follows:
    /// The chunk coordinates are converted
    /// to the chunk numbers along each dimension (relative to the dimension start).
    /// i.e. eanch coordinate is first offset by the dimension start
    /// and divided by the chunk size along that coordinate's dimension.
    /// XXX TODO (for JHM?): If this behavior is changed (e.g. by hashing the coordinates directly),
    /// XXX TODO: The replica hashing code (fib64()) in Storage.cpp should be updated accordingly.
    static uint64_t getHashedChunkNumber(scidb::Dimensions const& dims,
                                         scidb::Coordinates const& pos);
};

/// ArrayDistribution suitable for two-dimensional arrays
/// It uses the second dimension (column) to distribute chunks in a round-robin fashion
class ByColumnArrayDistribution : public ArrayDistribution
{
   public:
    ByColumnArrayDistribution(size_t redundancy, const std::string& ctx)
    : ArrayDistribution(psByCol, redundancy)
    {
        SCIDB_ASSERT(ctx.empty());
    }

    virtual InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                               Dimensions const& dims,
                                               size_t nInstances) const ;
    virtual std::string getContext() const
    {
        return std::string();
    }

    virtual bool checkCompatibility(const ArrayDistPtr& otherArrDist) const
    {
        if (!ArrayDistribution::checkPs(otherArrDist)) {
            return false;
        }
        return (dynamic_cast<const ByColumnArrayDistribution*>(otherArrDist.get())!=NULL);
    }
};


/// ArrayDistribution suitable for two-dimensional arrays
/// It uses the first dimension (row) to distribute chunks in a round-robin fashion
class ByRowArrayDistribution : public ArrayDistribution
{
public:
   ByRowArrayDistribution(size_t redundancy, const std::string& ctx)
   : ArrayDistribution(psByRow, redundancy)
    {
        SCIDB_ASSERT(ctx.empty());
    }

    virtual InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                               Dimensions const& dims,
                                               size_t nInstances) const ;
    virtual std::string getContext() const
    {
        return std::string();
    }

    virtual bool checkCompatibility(const ArrayDistPtr& otherArrDist) const
    {
        if (!ArrayDistribution::checkPs(otherArrDist)) {
            return false;
        }
        return (dynamic_cast<const ByRowArrayDistribution*>(otherArrDist.get())!=NULL);
    }
};

/// ArrayDistribution that maps each chunk to ALL instances
class ReplicatedArrayDistribution : public ArrayDistribution
{
   public:
    ReplicatedArrayDistribution(size_t redundancy, const std::string& ctx)
    : ArrayDistribution(psReplication, DEFAULT_REDUNDANCY)
    {
        SCIDB_ASSERT(redundancy==DEFAULT_REDUNDANCY);
        SCIDB_ASSERT(ctx.empty());
    }

    /// @return ALL_INSTANCE_MASK to indicate
    ///         that a chunk with a given position belongs on all instances
    virtual InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                               Dimensions const& dims,
                                               size_t nInstances) const
    {
       return InstanceID(ALL_INSTANCE_MASK);
    }

    virtual std::string getContext() const
    {
        return std::string();
    }

    virtual bool checkCompatibility(const ArrayDistPtr& otherArrDist) const
    {
        if (!ArrayDistribution::checkPs(otherArrDist)) {
            return false;
        }
        return (dynamic_cast<const ReplicatedArrayDistribution*>(otherArrDist.get())!=NULL);
    }
};

/// ArrayDistribution that uses groups to assign chunks to positions
/// @todo XXX define groups
class GroupByArrayDistribution : public ArrayDistribution
{
public:

    GroupByArrayDistribution(size_t redundancy, const std::vector<bool>& arrIsGroupbyDim)
    : ArrayDistribution(psGroupBy, DEFAULT_REDUNDANCY),
    _arrIsGroupbyDim(arrIsGroupbyDim)
    {
        SCIDB_ASSERT(redundancy == DEFAULT_REDUNDANCY);
        ASSERT_EXCEPTION((!_arrIsGroupbyDim.empty()),
                         "GroupByArrayDistribution: invalid context");
    }

    GroupByArrayDistribution(size_t redundancy, const std::string& ctx)
    : ArrayDistribution(psGroupBy, DEFAULT_REDUNDANCY)
    {
        SCIDB_ASSERT(redundancy == DEFAULT_REDUNDANCY);
        std::stringstream sin(ctx);
        dist::readInts(sin, _arrIsGroupbyDim);

        ASSERT_EXCEPTION((!_arrIsGroupbyDim.empty()),
                         "GroupByArrayDistribution: invalid context");
    }

    /// @return a string represenation of the internal state, in this case the
    /// a bitmask representaion of the group-by dimensions
    virtual std::string getContext() const
    {
        std::stringstream sout;
        dist::writeInts(sout, _arrIsGroupbyDim);
        return sout.str();
    }

    virtual InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                               Dimensions const& dims,
                                               size_t nInstances) const
    {
        ASSERT_EXCEPTION(chunkPosition.size() == _arrIsGroupbyDim.size(), "EMPTY GroupByDim");

        InstanceID destInstanceId = hashForGroupBy(chunkPosition, _arrIsGroupbyDim);

        return destInstanceId % nInstances;
    }

    virtual ArrayDistPtr adjustForNewResidency(size_t nInstances)
    {
        ASSERT_EXCEPTION_FALSE("GroupByArrayDistribution::adjustForNewResidency "
                               "not implemented");
        return ArrayDistPtr();
    }

    virtual bool checkCompatibility(const ArrayDistPtr& otherArrDist) const
    {
        if (!ArrayDistribution::checkPs(otherArrDist)) {
            return false;
        }
        const GroupByArrayDistribution *gbard =
           dynamic_cast<const GroupByArrayDistribution*>(otherArrDist.get());
        if (gbard == NULL) {
            return false;
        }
        return (_arrIsGroupbyDim == gbard->_arrIsGroupbyDim);
    }

private:

    /**
     * Compute hash over the groupby dimensions.
     * @param   allDims   Coordinates containing all the dims.
     * @param   isGroupby   For every dimension, whether it is a groupby dimension.
     *
     * @note The result can be larger than #instances!!! The caller should mod it.
     */
    InstanceID
    hashForGroupBy(const Coordinates& allDims,
                   const std::vector<bool>& isGroupby ) const
    {
        SCIDB_ASSERT(allDims.size()==isGroupby.size());

        _groups.clear();
        _groups.reserve(allDims.size()/2);

        for (size_t i=0; i<allDims.size(); ++i) {
            if (isGroupby[i]) {
                _groups.push_back(allDims[i]);
            }
        }
        return VectorHash<Coordinate>()(_groups);
    }

private:

    // XXX TODO: We really don't want to be using vector<bool>
    // XXX TODO: see http://www.informit.com/guides/content.aspx?g=cplusplus&seqNum=98
    // XXX TODO: Suggest vector<char> or (if N is known) bitset<N>.
    std::vector<bool> _arrIsGroupbyDim;
    mutable Coordinates _groups; //temp to avoid mallocs
};

/// ArrayDistribution which assigns all chunks to the same instances
class LocalArrayDistribution : public ArrayDistribution
{
public:

    LocalArrayDistribution(size_t redundancy, const std::string& ctx)
    : ArrayDistribution(psLocalInstance, DEFAULT_REDUNDANCY),
    _localInstance(INVALID_INSTANCE)
    {
        SCIDB_ASSERT(redundancy == DEFAULT_REDUNDANCY);
        std::stringstream sin(ctx);
        sin >> _localInstance;
        SCIDB_ASSERT(_localInstance != ALL_INSTANCE_MASK);
        ASSERT_EXCEPTION(_localInstance != INVALID_INSTANCE, "Invalid local instance");
    }

    LocalArrayDistribution(InstanceID i)
    : ArrayDistribution(psLocalInstance, DEFAULT_REDUNDANCY), _localInstance(i)
    {
        SCIDB_ASSERT(i != ALL_INSTANCE_MASK);
        SCIDB_ASSERT(i != INVALID_INSTANCE);
    }

    virtual InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                               Dimensions const& dims,
                                               size_t nInstances) const
    {
        ASSERT_EXCEPTION(_localInstance < nInstances,
                         "LocalArrayDistribution::getPrimaryChunkLocation "
                         "invalid local instance or instance number");
        return _localInstance;
    }

    virtual ArrayDistPtr adjustForNewResidency(size_t nInstances)
    {
        ASSERT_EXCEPTION(_localInstance < nInstances,
                         "LocalArrayDistribution::adjustForNewResidency "
                         "invalid local instance or instance number");
        return ArrayDistribution::adjustForNewResidency(nInstances);
    }

    virtual bool checkCompatibility(const ArrayDistPtr& otherArrDist) const
    {
        if (!ArrayDistribution::checkPs(otherArrDist)) {
            return false;
        }
        const LocalArrayDistribution *lard =
           dynamic_cast<const LocalArrayDistribution*>(otherArrDist.get());
        if (lard == NULL) {
            return false;
        }
        return (_localInstance == lard->_localInstance);
    }

    /// @return a string represenation of the internal state, in this case the
    /// logical ID of the instance to which all array chunks are mapped
    virtual std::string getContext() const
    {
        std::stringstream sout;
        sout << _localInstance;
        return sout.str();
    }

    /// @return the logical instance ID to which all array chunks are mapped
    InstanceID getLogicalInstanceId() const { return _localInstance; }

   private:
    InstanceID _localInstance;
};

} // namespace scidb

#endif // ARRAY_DISTRIBUTION_H_
