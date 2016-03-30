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

#ifndef ARRAY_DISTRIBUTION_INTERFACE_H_
#define ARRAY_DISTRIBUTION_INTERFACE_H_

#include <iostream>
#include <memory>
#include <unordered_map>

#include <boost/function.hpp>

#include <array/Coordinate.h>
#include <query/InstanceID.h>
#include <system/Utils.h>
#include <util/PointerRange.h>
#include <util/Singleton.h>
#include <util/Utility.h>

namespace scidb
{
    class DimensionDesc;
    typedef std::vector<DimensionDesc> Dimensions;
    class Coordiantes;
    class CoordianteCRange;
    class Query;

/**
 * Partitioning schema shows how an array is distributed among the SciDB instances.
 *
 * Guidelines for introducing a new partitioning schema:
 *   - Add to enum PartitioningSchema (right above psMax).
 *   - Modify the doxygen comments in LogicalSG.cpp.
 *   - Provider an implementation of ArrayDistribution intreface, register it with ArrayDistributionFactory
 *   - Modify std::ostream& operator<<(std::ostream& stream, const RedistributeContext& dist). (See Operator.cpp)
 *   - If the partitioning schema uses extra data:
 *   -    Modify doesPartitioningSchemaHaveOptionalData.
 */
enum PartitioningSchema
{
    psUninitialized = -1, // e.g. ps after ArrayDesc::ArrayDesc()
    psMIN = 0,
    psReplication = psMIN,
    psHashPartitioned,
    psLocalInstance,
    psByRow,
    psByCol,
    psUndefined,        // a range of meanings, including sometimes "wildcard"
                        // TODO: replace with psWildcard and others as required
    psGroupBy,
    psScaLAPACK,
    // A newly introduced partitioning schema should be added before this line.
    psEND
};

class ArrayDistribution;

typedef std::shared_ptr<const ArrayDistribution> ArrayDistPtr;

/**
 * Array distribution interface abstracts a method for assigning array chunks to
 * the consecutive set of size N of natural numbers [0,...,N-1].
 * Such a method uses the chunk coordinates and the array dimensions to produce
 * an integer to which the chunk is assigned. The natural number can be used to compute
 * the SciDB instances to which a given chunk belongs.
 * ArrayDistirbution is part of array metadata and provides some additional information
 * such as array redundancy. Redundancy is the number of *additional* chunk copies
 * stored by the system for a given array. The original chunk is called the primary chunk
 * and the copies are its replicas.
 */
class ArrayDistribution : public std::enable_shared_from_this<ArrayDistribution>, boost::noncopyable
{
public:

    /// Compute a destination integer in the range [0..nInstances-1] for the primary chunk
    /// for a given chunk position and array dimensions
    /// @param chunkPosition chunk position coordinates
    /// @param dims chunk's array dimensions
    /// @return destination integer
    virtual InstanceID getPrimaryChunkLocation(Coordinates const& chunkPosition,
                                               Dimensions const& dims,
                                               size_t nInstances) const = 0;

    /// Produce a new distribution for a given residency set size.
    /// Some distributions may need to mutate when the number of instances changes.
    /// @param nInstances cardinality of the new residency set
    /// @return a new array distribution that fits a given residency
    virtual ArrayDistPtr adjustForNewResidency(size_t nInstances)
    {
        return shared_from_this();
    }

    /// Check if this distribution is compatible with another distribution
    /// Two distributions are compatible if they are interchangible.
    /// In other words, an SG converting an array from one distribution to the other
    /// would NOT change the results of any operation on the array.
    /// It must be commutative, i.e. A.checkCompatibility(B) == B.checkCompatibility(A)
    /// @deprecated USE isCompatibleWith()
    /// @todo XXX remove this method
    /// @param otherArrDist array distribution to check
    /// @return true if the distributions are interchangible; false otherwise
    virtual bool checkCompatibility(const ArrayDistPtr& otherArrDist) const = 0;

    /// Check if this distribution is compatible with another distribution
    /// Two distributions are compatible if they are interchangible.
    /// In other words, an SG converting an array from one distribution to the other
    /// would NOT change the results of any operation on the array.
    /// It must be commutative, i.e. A.checkCompatibility(B) == B.checkCompatibility(A)
    /// @param otherArrDist array distribution to check
    /// @return true if the distributions are interchangible; false otherwise
    virtual bool isCompatibleWith(const ArrayDistPtr& otherArrDist) const
    {
        return checkCompatibility(otherArrDist);
    }

    /// Get an opaque string representation of the internal state used by the concrete implementation.
    /// This string context is used for construction of the objects of that concrete type.
    /// @see ArrayDistributionFactory::construct()
    virtual std::string getContext() const = 0;

    /// Get the partitioning schema ID which uniquely identifies this type of distribution
    PartitioningSchema getPartitioningSchema() const
    {
        return _ps;
    }

    /// Get array redundancy, where redundancy is the number
    /// of additional copies of array stored by the system.
    /// E.g. redundancy==2 means that the system stores 3 copies
    /// of each array chunk, and no 2 copies are stored on the
    /// same physical instance.
    size_t getRedundancy() const
    {
        return _redundancy;
    }

protected:

    /// Constructor
    ArrayDistribution(PartitioningSchema ps, size_t redundancy)
    : _ps(ps), _redundancy(redundancy)
    {
    }

    /// @return true if the partitioning scheme of the input distribution
    ///         is the same as that of this distribution; false otherwise
    bool checkPs(const ArrayDistPtr& otherArrDist) const
    {
        return (getPartitioningSchema() == otherArrDist->getPartitioningSchema());
    }

private:
    ArrayDistribution();

    PartitioningSchema _ps;
    size_t _redundancy;
};

/// Stream output operator for ArrayDistribution
std::ostream& operator<<(std::ostream& stream, const ArrayDistribution& res);

/**
 * Array distribution interface abstracts a set of physical SciDB instances
 * on which an array is stored. Each physical instance is referred to by its index (or rank)
 * from the consecutive set of size N of natural numbers [0,...,N-1], where N is the size of the residency.
 * @todo XXX TODO: factor in its own file
 */
class ArrayResidency;
typedef std::shared_ptr<const ArrayResidency> ArrayResPtr;
class ArrayResidency
{
public:

    /// @return the physical instance ID at a given index
    virtual InstanceID getPhysicalInstanceAt(InstanceID index) const = 0;

    /// @return residency size (i.e. number of physical instances)
    virtual size_t size() const = 0;

    /// @return true if the instances in this and the other residencies are the same
    ///         false otherwise
    /// The functionality is logically equivalent operator==(),
    /// but needs to work with a virtual interface
    bool isEqual(const ArrayResPtr& ar) const
    {
        if (ar.get() == this) {
            return true;
        }
        const size_t nInstances = size();
        if (ar->size() != nInstances) {
            return false;
        }
        for (size_t i = 0; i < nInstances; ++i) {
            if ( ar->getPhysicalInstanceAt(i) != getPhysicalInstanceAt(i)) {
                return false;
            }
        }
        return true;
    }
    /// @return true if the instances in this residencie
    ///         and the collection specified by the input iterators are the same;
    ///         false otherwise
    template<class Iterator>
    bool isEqual(Iterator begin, Iterator end) const
    {
        const size_t nInstances = size();
        size_t i = 0;

        for (; i < nInstances && begin != end; ++i, ++begin) {
            if ( (*begin) != getPhysicalInstanceAt(i)) {
                return false;
            }
        }
        if (begin != end || i != nInstances) {
            return false;
        }
        return true;
    }

protected:
    ArrayResidency() {}
};

/// Stream output operator for ArrayResidency
std::ostream& operator<<(std::ostream& stream, const ArrayResidency& res);

/// An interface for coordinate translation
class CoordinateTranslator
{
protected:
    CoordinateTranslator() {}

public:
    virtual Coordinates translate (Coordinates const& input) const = 0 ;
    virtual ~CoordinateTranslator() {}

private:
    CoordinateTranslator(CoordinateTranslator const& );
    CoordinateTranslator& operator=(CoordinateTranslator const&);
};

/**
 * A concrete factory for creating ArrayDistributions based on
 * partitioning schemas and any additional context, typically
 * stored with array metadata in SystemCatalog.
 */
class ArrayDistributionFactory : public scidb::Singleton<ArrayDistributionFactory>
{
public:
    /// Constructor
    explicit ArrayDistributionFactory ()
    {
        registerBuiltinDistributions(); //must be non-virtual
    }

    /// Destructor
    virtual ~ArrayDistributionFactory () {}

    /**
     * A functor type for constructing ArrayDistributions
     * @param ps partitioning scheme
     * @param redundancy
     * @param context
     */
    typedef boost::function< ArrayDistPtr(PartitioningSchema ps,
                                          size_t redundancy,
                                          const std::string& ctx) >
    ArrayDistributionConstructor;

    /**
     * Register a construction functor for a distribution identified
     * by a partitioning scheme
     * @param ps partitioning scheme
     * @param constructor for the distribution
     */
    void registerConstructor(PartitioningSchema ps,
                             const ArrayDistributionConstructor& constructor);

    /**
     * Construct an ArrayDistribution
     *
     * @param ps partitioning scheme
     * @param redundancy
     * @param context, "" by default
     * @param ct coordinate translator to be applied on the coordinates
     *        before they are fed into getPrimaryChunkLocation()
     * @param instanceShift shift to be added to the chunk location
     *        before it is returned from getPrimaryChunkLocation()
     */
    ArrayDistPtr construct(PartitioningSchema ps,
                           size_t redundancy,
                           const std::string& ctx=std::string(),
                           const std::shared_ptr<CoordinateTranslator>& ct =
                           std::shared_ptr<CoordinateTranslator>(),
                           size_t instanceShift=0);


    /**
     * Some ArrayDistribution implementations are wrappers which only perform translations
     * on the inputs and/or the output of getPrimaryChunkLocation() of a different distribution.
     * This method provides the information about the translations if any.
     * @param arrDist [in]
     * @param offset [out] added to the coordinates
     *        before they are fed into getPrimaryChunkLocation()
     * @param instanceShift [out] added to the chunk location
     *        before it is returned from getPrimaryChunkLocation()
     * @note This is somewhat of a HACK to make some subarray() optimizations to work.
     * Hopefully, we will get rid of the optimizations or the need for them
     * (e.g. by distributing arrays based on the actual coordinates rather than on the relative ones)
     * and the need for the "CoordianteTranslator" hackery will disappear.
     */
    static void getTranslationInfo(const ArrayDistribution* arrDist,
                                   Coordinates& offset,
                                   InstanceID& instanceShift);

    /**
     * Create a translator that adds an offset to the coordinates
     * @param offset to add
     * @return translator
     */
    static std::shared_ptr<CoordinateTranslator> createOffsetTranslator(const Coordinates& offset);


    /**
     * Default implementation of the ArrayDistribution construction functor
     */
    template<class ArrayDistType>
    static ArrayDistPtr defaultConstructor(PartitioningSchema ps,
                                           size_t redundancy,
                                           const std::string& ctx)
    {
        return std::make_shared<ArrayDistType>(redundancy, ctx);
    }

private:

    void registerBuiltinDistributions();

    typedef int KeyType;
    typedef ArrayDistributionConstructor ValueType;
    typedef std::unordered_map<KeyType, ValueType > FactoryMap;
    FactoryMap _constructors;
};

/// Persistent arrays may have non-default redundancy (returned by scan() and written by store(), ...)
/// Arrays existing only during query execution have the default redundancy of 0.
static const size_t DEFAULT_REDUNDANCY=0;

/**
 * Historical context for  defaultPartitioning()
 *
 * Most of the code was assuming that arrays are scanned and stored in
 * psHashPartitioned Partitioning (currently a category of ArrayDistribution).
 * Therefore there were many "psHashPartitioned" scatter about the code that
 * logically mean "whatever the default PartitioningSchema is".  We are moving on
 * a path toward generalizing what distributions can be stored.  The first attempt
 * will be to extend tempArrays to psReplication, psByRow, and psByCol.  When that
 * is working we will enable it for dbArrays.  Later we will move on to distributions
 * which require additional parameterizations to be stored, such as the 2D
 * ScaLAPACK.
 *
 * As scaffolding for that effort, we are making the "default" PartitioningSchema
 * configurable.  This will allow us to (1) experiment with performance of alternate
 * distributions for certain workflows and (2) start fixing the query compiler/optimizer
 * to insert SGs without assuming that psHashPartitioned is the universal goal.
 *
 * Then we can add a create array parameterization for particular distributions
 * and drive the query tree's output toward that. (e.g. right now, for iterated spgemm,
 * psByRow and psByCol are far more efficient than psHashed)
 *
 * Once this is working for TEMP arrays, we can allow it for general arrays
 *
 * Once this all works correctly via create statements, we will no longer be managing
 * "ps" outside of ArrayDesc's and ArrayDistributions, this function should no longer be referenced
 * and this scaffolding removed.
 */

/**
 * Get the default array distribution (with a given redundancy).
 * Currently, all persistent and temp arrays are stored in that distribution.
 */
inline ArrayDistPtr defaultPartitioning(size_t redundancy=DEFAULT_REDUNDANCY)
{
    // The (old) statement below is most likely not true any more,
    // but there are no tests to prove it, so ...
    // you MAY NOT change this from psHashPartitioned at this time
    // this is scaffolding code that is locating all the places where
    // psHashPartitioned is assumed which need to be generalized.
    // there are further changes in the optimizer that must be planned first
    // and then we can decide which PartitioningSchema variable is the
    // definitive one. Until then, the code will read "defaultParitioning()"
    // and the reader should think "psHashPartitioned"

    ArrayDistPtr dist =
       ArrayDistributionFactory::getInstance()->construct(psHashPartitioned,
                                                          redundancy);
    return dist;
}

/// Helper method for creating array distributions which do not require any context
/// @param ps partitioning schema
/// @param redundancy, DEFAULT_REDUNDANCY by default
inline ArrayDistPtr createDistribution(PartitioningSchema ps,
                                       size_t redundancy=DEFAULT_REDUNDANCY)
{
    if (ps == psHashPartitioned) {
        return defaultPartitioning(redundancy);
    }
    return ArrayDistributionFactory::getInstance()->construct(ps,
                                                              redundancy);
}

/**
 * Create a default implementation of ArrayResidency with the specified physical instances
 * @param physInstances an ordered collection of physical instance IDs,
 *        the order in which they are specified determines their ranks
 *        mapping the output of ArrayDistribution::getPrimaryChunkLocation()
 *        to the physical instances
 * @return ArrayResidency
 */
ArrayResPtr createDefaultResidency(PointerRange<InstanceID> physInstances);

} // namespace scidb

#endif // ARRAY_DISTRIBUTION_INTERAFCE_H_
