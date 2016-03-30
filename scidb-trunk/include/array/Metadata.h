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
 * @file Metadata.h
 *
 * @brief Structures for fetching and updating metadata of cluster.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 * @author poliocough@gmail.com
 */

#ifndef METADATA_H_
#define METADATA_H_

#include <stdint.h>
#include <string>
#include <vector>
#include <iosfwd>
#include <assert.h>
#include <set>

#include <boost/operators.hpp>
#include <boost/serialization/level.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/set.hpp>

#include <array/ArrayDistributionInterface.h>
#include <array/Coordinate.h>
#include <query/TypeSystem.h>
#include <system/Cluster.h>

namespace scidb
{

class AttributeDesc;
class DimensionDesc;
class InstanceDesc;
class LogicalOpDesc;
class ObjectNames;
class PhysicalOpDesc;
class Value;
class ArrayDistribution;
class ArrayResidency;

/**
 * Vector of AttributeDesc type
 */
typedef std::vector<AttributeDesc> Attributes;

/**
 * Vector of DimensionDesc type
 */
typedef std::vector<DimensionDesc> Dimensions;

typedef std::vector<LogicalOpDesc> LogicalOps;

typedef std::vector<PhysicalOpDesc> PhysicalOps;

/**
 * Array identifier
 */
typedef uint64_t ArrayID;

/**
 * Unversioned Array identifier
 */
typedef uint64_t ArrayUAID;

/**
 * Identifier of array version
 */
typedef uint64_t VersionID;

/**
 * Attribute identifier (attribute number in array description)
 */
typedef uint32_t AttributeID;

typedef uint64_t OpID;

const VersionID   LAST_VERSION          = (VersionID)-1;
const VersionID   ALL_VERSIONS          = (VersionID)-2;
const ArrayID     INVALID_ARRAY_ID      = ~0;
const AttributeID INVALID_ATTRIBUTE_ID  = static_cast<uint32_t>(~0);
const size_t      INVALID_DIMENSION_ID  = ~0;
const std::string DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME = "EmptyTag";


/**
 * Coordinates mapping mode
 */
enum CoordinateMappingMode
{
    cmUpperBound,
    cmLowerBound,
    cmExact,
    cmTest,
    cmLowerCount,
    cmUpperCount
};

/**
 * @brief Class containing all possible object names
 *
 * During array processing schemas can be merged in many ways. For example NATURAL JOIN contain all
 * attributes from both arrays and dimensions combined. Attributes in such example received same names
 * as from original schema and also aliases from original schema name if present, so it can be used
 * later for resolving ambiguity. Dimensions in output schema received not only aliases, but also
 * additional names, so same dimension in output schema can be referenced by old name from input schema.
 *
 * Despite object using many names and aliases catalog storing only one name - base name. This name
 * will be used also for returning in result schema. So query processor handling all names but storage
 * and user API using only one.
 *
 * @note Alias this is not full name of object! Basically it prefix received from schema name or user
 * defined alias name.
 */
class ObjectNames : boost::equality_comparable<ObjectNames>
{
public:
    typedef std::set<std::string>               AliasesType;
    typedef std::map<std::string,AliasesType>   NamesType;
    typedef std::pair<std::string,AliasesType>  NamesPairType;

    ObjectNames();

    /**
     * Constructing initial name without aliases and/or additional names. This name will be later
     * used for returning to user or storing to catalog.
     *
     * @param baseName base object name
     */
    ObjectNames(const std::string &baseName);

    /**
     * Constructing full name
     *
     * @param baseName base object name
     * @param names other names and aliases
     */
    ObjectNames(const std::string &baseName, const NamesType &names);

    /**
     * Add new object name
     *
     * @param name object name
     */
    void addName(const std::string &name);

    /**
     * Add new alias name to object name
     *
     * @param alias alias name
     * @param name object name
     */
    void addAlias(const std::string &alias, const std::string &name);

    /**
     * Add new alias name to all object names
     *
     * @param alias alias name
     */
    void addAlias(const std::string &alias);

    /**
     * Check if object has such name and alias (if given).
     *
     * @param name object name
     * @param alias alias name
     * @return true if has
     */
    bool hasNameAndAlias(const std::string &name, const std::string &alias = "") const;

    /**
     * Get all names and aliases of object
     *
     * @return names and aliases map
     */
    const NamesType& getNamesAndAliases() const;

    /**
     * Get base name of object.
     *
     * @return base name of object
     */
    const std::string& getBaseName() const;

    bool operator==(const ObjectNames &o) const;

    friend std::ostream& operator<<(std::ostream& stream, const ObjectNames &ob);
    friend void printSchema (std::ostream& stream, const ObjectNames &ob);

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _baseName;
        ar & _names;
    }

protected:
    NamesType   _names;
    std::string _baseName;
};


/**
 * Descriptor of array. Used for getting metadata of array from catalog.
 */
class ArrayDesc : boost::equality_comparable<ArrayDesc>
{
    friend class DimensionDesc;
public:
    /**
     * Various array qualifiers
     */
    enum ArrayFlags
    {
        TRANSIENT    = 0x10,    ///< Represented as a MemArray held in the transient array cache: see TransientCache.h for details.
        INVALID      = 0x20     ///< The array is no longer in a consistent state and should be removed from the database.
    };

    /**
     * Construct empty array descriptor (for receiving metadata)
     */
    ArrayDesc();

    /**
     * Construct partial array descriptor (without id, for adding to catalog)
     *
     * @param namespaceName The name of the namespace the array is in
     * @param arrayName array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     * @param arrDist array distribution
     * @param arrRes array residency
     * @param flags array flags from ArrayDesc::ArrayFlags
     */
    ArrayDesc(const std::string &namespaceName,
              const std::string &arrayName,
              const Attributes& attributes,
              const Dimensions &dimensions,
              const ArrayDistPtr& arrDist,
              const ArrayResPtr& arrRes,
              int32_t flags = 0);

    /**
     * Construct partial array descriptor (without id, for adding to catalog)
     *
     * @param arrayName array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     * @param arrDist array distribution
     * @param arrRes array residency
     * @param flags array flags from ArrayDesc::ArrayFlags
     */
    ArrayDesc(const std::string &arrayName,
              const Attributes& attributes,
              const Dimensions &dimensions,
              const ArrayDistPtr& arrDist,
              const ArrayResPtr& arrRes,
              int32_t flags = 0);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param arrId the unique array ID
     * @param uAId the unversioned array ID
     * @param vId the version number
     * @param namespaceName The name of the namespace the array is in
     * @param arrayName array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     * @param arrDist array distribution
     * @param arrRes array residency
     * @param flags array flags from ArrayDesc::ArrayFlags
     */
    ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId,
              const std::string &namespaceName,
              const std::string &arrayName,
              const Attributes& attributes,
              const Dimensions &dimensions,
              const ArrayDistPtr& arrDist,
              const ArrayResPtr& arrRes,
              int32_t flags = 0);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param arrId the unique array ID
     * @param uAId the unversioned array ID
     * @param vId the version number
     * @param arrayName array name
     * @param attributes vector of attributes
     * @param dimensions vector of dimensions
     * @param arrDist array distribution
     * @param arrRes array residency
     * @param flags array flags from ArrayDesc::ArrayFlags
     */
    ArrayDesc(ArrayID arrId, ArrayUAID uAId, VersionID vId,
              const std::string &arrayName,
              const Attributes& attributes,
              const Dimensions &dimensions,
              const ArrayDistPtr& arrDist,
              const ArrayResPtr& arrRes,
              int32_t flags = 0);

    /**
     * Copy constructor
     */
    ArrayDesc(ArrayDesc const& other);

   ~ArrayDesc() { }

    /**
     * Given a fully qualified array name of the form "namespace_name.array_name"
     * split it into its constituent parts.  If the qualifiedArrayName is not qualified 
     * then namespaceName will not be modified.
     *
     * @param[in] qualifiedArrayName    Potentially qualified array name of the form 
     *                                  "namespace_name.array_name"
     * @param[out] namespaceName        If qualifiedArrayName is qualified, 'namespace_name' from 
     *                                  qualifiedArrayName.  Otherwise, not modified
     * @param[out] arrayName            'array_name' portion of the fullyQualifiedArrayName
     */
    static void splitQualifiedArrayName(
        const std::string &     qualifiedArrayName,
        std::string &           namespaceName,
        std::string &           arrayName);


    /**
     * Given a potentially fully qualified array name of the form "namespace_name.array_name"
     * retrieve the arrayName portion.
     *
     * @param[in] arrayName A potentially fully qualified array name
     * @return The unqualified portion of the array name
     */
    static std::string getUnqualifiedArrayName(
        const std::string &           arrayName);

    /**
     * Make a fully qualified array name from a namespace name and an array name
     * If arrayName is already fully-qualified it is returned without modification
     * @param namespaceName The name of the namespace to use in the fully-qualified name
     * @param arrayName The name of the array to use in the fully-qualified name
     */
    static std::string makeQualifiedArrayName(
        const std::string &           namespaceName,
        const std::string &           arrayName);

    /**
     * Determine if an array name is qualified or not
     * @return true qualified, false otherwise
     */
    static bool isQualifiedArrayName(
        const std::string &           arrayName);

    /**
     * Make a fully qualified array name for this instance of namespaceName and arrayName.
     */
    std::string getQualifiedArrayName() const;

    /**
     * Assignment operator
     */
    ArrayDesc& operator = (ArrayDesc const&);

    /**
     * Get the unversioned array id (id of parent array)
     * @return unversioned array id
     */
    ArrayUAID getUAId() const
    {
        return _uAId;
    }

    /**
     * Get the unique versioned array id.
     * @return the versioned array id
     */
    ArrayID getId() const
    {
        return _arrId;
    }

    /**
     * Get the array version number.
     * @return the version number
     */
    VersionID getVersionId() const
    {
        return _versionId;
    }

    /**
     * Set array identifiers
     * @param [in] arrId the versioned array id
     * @param [in] uAId the unversioned array id
     * @param [in] vId the version number
     */
    void setIds(ArrayID arrId, ArrayUAID uAId, VersionID vId)
    {
        _arrId = arrId;
        _uAId = uAId;
        _versionId = vId;
    }

    /**
     * Get name of array
     * @return array name
     */
    const std::string& getName() const
    {
        return _arrayName;
    }

    /**
     * Set name of array
     * @param arrayName array name
     */
    void setName(const std::string& arrayName)
    {
        _arrayName = arrayName;
    }

    /**
     * Get name of the namespace the array belongs to
     * @return namespace name
     */
    const std::string& getNamespaceName() const
    {
        return _namespaceName;
    }

    /**
     * Set name of namespace the array belongs to
     * @param namespaceName The namespace name
     */
    void setNamespaceName(const std::string& namespaceName)
    {
        _namespaceName = namespaceName;
    }

    /**
     * Find out if an array name is for a versioned array.
     * In our current naming scheme, in order to be versioned, the name
     * must contain the "@" symbol, as in "myarray@3". However, NID
     * array names have the form "myarray@3:dimension1" and those arrays
     * are actually NOT versioned.
     * @param[in] name the name to check. A nonempty string.
     * @return true if name contains '@' at position 1 or greater and does not contain ':'.
     *         false otherwise.
     */
    static bool isNameVersioned(std::string const& name);

    /**
     * Find out if an array name is for an unversioned array - not a NID and not a version.
     * @param[in] the name to check. A nonempty string.
     * @return true if the name contains neither ':' nor '@'. False otherwise.
     */
    static bool isNameUnversioned(std::string const& name);

    /**
     * Given the versioned array name, extract the corresponing name for the unversioned array.
     * In other words, compute the name of the "parent" array. Or, simply put, given "foo@3" produce "foo".
     * @param[in] name
     * @return a substring of name up to and excluding '@', if isNameVersioned(name) is true.
     *         name otherwise.
     */
    static std::string makeUnversionedName(std::string const& name)
    {
        if (isNameVersioned(name))
        {
            size_t const locationOfAt = name.find('@');
            return name.substr(0, locationOfAt);
        }
        return name;
    }

    /**
    * Given the versioned array name, extract the version id.
    * Or, simply put, given "foo@3" produce 3.
    * @param[in] name
    * @return a substring of name after and excluding '@', converted to a VersionID, if
    *         isVersionedName(name) is true.
    *         0 otherwise.
    */
    static VersionID getVersionFromName(std::string const& name)
    {
        if(isNameVersioned(name))
        {
           size_t locationOfAt = name.find('@');
           return atol(&name[locationOfAt+1]);
        }
        return 0;
    }

    /**
     * Given an unversioned array name and a version ID, stitch the two together.
     * In other words, given "foo", 3 produce "foo@3".
     * @param[in] name must be a nonempty unversioned name
     * @param[in] version the version number
     * @return the concatenation of name, "@" and version
     */
    static std::string makeVersionedName(std::string const& name, VersionID const version)
    {
        assert(!isNameVersioned(name));
        std::stringstream ss;
        ss << name << "@" << version;
        return ss.str();
    }

    /**
     * Get static array size (number of elements within static boundaries)
     * @return array size
     */
    uint64_t getSize() const;

    /**
     * Get bitmap attribute used to mark empty cells
     * @return descriptor of the empty indicator attribute or NULL is array is regular
     */
    AttributeDesc const* getEmptyBitmapAttribute() const
    {
        return _bitmapAttr;
    }

    /**
     * Get vector of array attributes
     * @return array attributes
     */
    Attributes const& getAttributes(bool excludeEmptyBitmap = false) const
    {
        return excludeEmptyBitmap ? _attributesWithoutBitmap : _attributes;
    }

    /** Set vector of array attributes */
    ArrayDesc& setAttributes(Attributes const& attributes)
    {
        _attributes = attributes;
        locateBitmapAttribute();
        return *this;
    }

    /**
     * Get vector of array dimensions
     * @return array dimensions
     */
    Dimensions const& getDimensions() const {return _dimensions;}
    Dimensions&       getDimensions()       {return _dimensions;}
    /**
     * Get the current low boundary of an array
     * @note This is NOT reliable.
     *       The only time the value can be trusted is when the array schema is returned by scan().
     *       As soon as we add other ops into the mix (e.g. filter(scan()), the value is not trustworthy anymore.
     */
    Coordinates getLowBoundary() const ;
    /**
     * Get the current high boundary of an array
     * @note This is NOT reliable.
     *       The only time the value can be trusted is when the array schema is returned by scan().
     *       As soon as we add other ops into the mix (e.g. filter(scan()), the value is not trustworthy anymore.
     */
    Coordinates getHighBoundary() const ;

    /** Set vector of array dimensions */
    ArrayDesc& setDimensions(Dimensions const& dims)
    {
        _dimensions = dims;
        initializeDimensions();
        return *this;
    }

    /**
     * Find the index of a DimensionDesc by name and alias.
     * @return index of desired dimension or -1 if not found
     */
    ssize_t findDimension(const std::string& name, const std::string& alias) const;

    /**
     * Check if position belongs to the array boundaries
     */
    bool contains(Coordinates const& pos) const;

    /**
     * Get position of the chunk for the given coordinates
     * @param[in,out] pos  an element position goes in, a chunk position goes out (not including overlap).
     */
    void getChunkPositionFor(Coordinates& pos) const;

    /**
     * Get position of the chunk for the given coordinates from a supplied Dimensions vector
     * @param[in] dims     dimensions to use in computing a chunk position from an element position
     * @param[in,out] pos  an element position goes in, a chunk position goes out (not including overlap).
     */
    static void getChunkPositionFor(Dimensions const& dims, Coordinates& pos);

    /**
     * @return whether a given position is a chunk position.
     * @param[in] pos  a cell position.
     */
    bool isAChunkPosition(Coordinates const& pos) const;

    /**
     * @return whether a cellPos belongs to a chunk specified with a chunkPos.
     * @param[in] cellPos  a cell position.
     * @param[in] chunkPos a chunk position.
     */
    bool isCellPosInChunk(Coordinates const& cellPos, Coordinates const& chunkPos) const;

    /**
      * Get boundaries of the chunk
      * @param chunkPosition - position of the chunk (should be aligned (for example, by getChunkPositionFor)
      * @param withOverlap - include or not include chunk overlap to result
      * @param lowerBound - lower bound of chunk area
      * @param upperBound - upper bound of chunk area
      */
    void getChunkBoundaries(Coordinates const& chunkPosition,
                            bool withOverlap,
                            Coordinates& lowerBound,
                            Coordinates& upperBound) const;
   /**
     * Get assigned instance for chunk for the given coordinates.
     * This function is unaware of the current query liveness; therefore,
     * the returned instance may not be identified by the same logical instanceID in the current query.
     * To get the corresponding logical instance ID use @see Query::mapPhysicalToLogical()
     * @param pos chunk position
     * @param originalInstanceCount query->getNumInstances() from the creating/storing query
     *        this was not saved in the ArrayDesc, so it is supplied from without
     *        at this time.  [For a more precise rules of whether this value changes on
     *        a per-version basis or not, one must investigate how the storage manager
     *        currently persists this value.]
     *
     * TODO: consider a design where the original instance count is managed alongside _ps
     *       to alleviate a potential source of errors that happen only during failover
     */
    InstanceID getPrimaryInstanceId(Coordinates const& pos, size_t instanceCount) const;

    /**
     * Get flags associated with array
     * @return flags
     */
    int32_t getFlags() const
    {
        return _flags;
    }

    /**
     * Trim unbounded array to its actual boundaries
     */
    void trim();

    /**
     * Checks if array has non-zero overlap in any dimension
     */
    bool hasOverlap() const;

    /**
     * Checks if any dimension has an unspecified (autochunked) chunk interval.
     */
    bool isAutochunked() const;

    /**
     * Return true if the array is marked as being 'transient'. See proposal
     * 'TransientArrays' for more details.
     */
    bool isTransient() const
    {
        return _flags & TRANSIENT;
    }

    /**
     * Mark or unmark the array as being 'transient'. See proposal
     * 'TransientArrays' for more details.
     */
    ArrayDesc& setTransient(bool transient)
    {
        if (transient)
        {
            _flags |= TRANSIENT;
        }
        else
        {
            _flags &= (~TRANSIENT);
        }

        return *this;
    }

    /**
     * Return true if the array is marked as being 'invalid', that is,
     * is pending removal from the database.
     */
    bool isInvalid() const
    {
        return _flags & INVALID;
    }

    /**
     * Add alias to all objects of schema
     * tigor: Whenever the AFL/AQL keyword 'as ABC',
     * tigor: ABC is attached to all? the names of the metadata as aliases.
     * tigor: The algorith for assigning aliases is not formal/consistent
     * tigor: (e.i. propagation through a binary op) and mostly works as implemented.
     * @param alias alias name
     */
    void addAlias(const std::string &alias);

    /**
     * @return a string representation of the ArrayDescriptor
     * including the attributes, dimensions, array IDs and version, flags, and the PartitioningSchema
     */
    std::string toString () const;

    template<class Archive>
    void serialize(Archive& ar,unsigned version)
    {
        ar & _arrId;
        ar & _uAId;
        ar & _versionId;
        ar & _namespaceName;  // Note:  deal with backwards compatability of the save format
        ar & _arrayName;
        ar & _attributes;
        ar & _dimensions;
        ar & _flags;

        // XXX TODO: improve serialization of residency and distribution
        // XXX TODO: the default residency does not need to be sent out ? it is the liveness set.

        PartitioningSchema ps;
        std::string psCtx;
        size_t redundancy;
        size_t instanceShift;
        Coordinates offset;
        std::vector<InstanceID> residency;

        // serialize distribution + residency

        if (Archive::is_loading::value)
        {
            // de-serializing data
            ar & ps;
            ar & psCtx;
            ar & redundancy;
            ar & instanceShift;
            ar & offset;
            ar & residency;

            SCIDB_ASSERT(!_residency);
            SCIDB_ASSERT(!_distribution);

            std::shared_ptr<CoordinateTranslator> translator =
               ArrayDistributionFactory::createOffsetTranslator(offset);

            ASSERT_EXCEPTION(instanceShift < residency.size(),
                             "Serialized distribution instance shift is "
                             "greater than residency size");

            _distribution = ArrayDistributionFactory::getInstance()->construct(ps,
                                                                               redundancy,
                                                                               psCtx,
                                                                               translator,
                                                                               instanceShift);
            ASSERT_EXCEPTION(_distribution,
                             "Serialized array descriptor has no distribution");

            _residency = createDefaultResidency(PointerRange<InstanceID>(residency));

            locateBitmapAttribute();

        } else {
            // serializing data
            ps = _distribution->getPartitioningSchema();
            psCtx = _distribution->getContext();
            redundancy = _distribution->getRedundancy();
            ArrayDistributionFactory::getTranslationInfo(_distribution.get(), offset, instanceShift);

            residency.reserve(_residency->size());
            for (size_t i=0; i < _residency->size(); ++i) {
                residency.push_back(_residency->getPhysicalInstanceAt(i));
            }
            ar & ps;
            ar & psCtx;
            ar & redundancy;
            ar & instanceShift;
            ar & offset;
            ar & residency;
        }
    }

    bool operator ==(ArrayDesc const& other) const;

    void cutOverlap();
    Dimensions grabDimensions(VersionID version) const;

    bool coordsAreAtChunkStart(Coordinates const& coords) const;
    bool coordsAreAtChunkEnd(Coordinates const& coords) const;

    void addAttribute(AttributeDesc const& newAttribute);

    double getNumChunksAlongDimension(size_t dimension,
                                      Coordinate start = CoordinateBounds::getMax(),
                                      Coordinate end = CoordinateBounds::getMin()) const;

    /**
     * Check if two array descriptors are conformant, i.e. if srcDesc one can be "stored" into dstDesc.
     * It checks the number and types of the attributes, the number of dimensions, partitioning schema, flags.
     * @throws scidb::SystemException with SCIDB_LE_ARRAYS_NOT_CONFORMANT or a more precise error code
     * @param srcDesc source array schema
     * @param dstDesc target array schema
     * @param options bit mask of options to toggle certain conformity checks
     */
    static void checkConformity(ArrayDesc const& srcDesc, ArrayDesc const& dstDesc, unsigned options = 0);

    /**
     * Check if all dimension names and attribute names are unique.
     */
    bool areNamesUnique() const;

    /** Option flags for use with checkConformity() */
    enum ConformityOptions {
        IGNORE_PSCHEME  = 0x01, /**< Array distributions and residencies need not match */
        IGNORE_OVERLAP  = 0x02, /**< Chunk overlaps need not match */
        IGNORE_INTERVAL = 0x04, /**< Chunk intervals need not match */
        SHORT_OK_IF_EBM = 0x08  /**< Src dim can be shorter if src array has empty bitmap */
    };

    /**
     *  @brief      A union of the various possible schema filter attributes.
     *
     *  @details    Modelled after the Arena Options class, class
     *              SchemaFieldSelector provides a sort of union of the
     *              many parameters with which an schema can be filtered.
     *              It uses the 'named parameter idiom' to enable these
     *              options to be supplied by name in any convenient
     *              order. For example:
     *
     *  @code
     *              sameSchema(
     *                  schemaInstance,
     *                  SchemaFieldSelector()
     *                      .startMin(true)
     *                      .endMax(true));
     *  @endcode
     *
     *  @see        http://www.parashift.com/c++-faq/named-parameter-idiom.html for
     *              a description of the 'named parameter idiom'.
     *
     *  @author     mcorbett@paradigm4.com.
     */

    class SchemaFieldSelector
    {
    public:

        // Construction
        SchemaFieldSelector()
            : _startMin(false)
            , _endMax(false)
            , _chunkInterval(false)
            , _chunkOverlap(false)
        {

        }

        // Attributes
        bool startMin()       const { return _startMin;      }
        bool endMax()         const { return _endMax;        }
        bool chunkInterval()  const { return _chunkInterval; }
        bool chunkOverlap()   const { return _chunkOverlap;  }

        // Operations
        SchemaFieldSelector & startMin(bool b)      { _startMin=b;       return *this; }
        SchemaFieldSelector & endMax(bool b)        { _endMax=b;         return *this; }
        SchemaFieldSelector & chunkInterval(bool b) { _chunkInterval=b;  return *this; }
        SchemaFieldSelector & chunkOverlap(bool b)  { _chunkOverlap=b;   return *this; }

    private:
        // Representation
        unsigned  _startMin      : 1;  // true = startMin selected
        unsigned  _endMax        : 1;  // true = endMax selected
        unsigned  _chunkInterval : 1;  // true = chunkInterval selected
        unsigned  _chunkOverlap  : 1;  // true = chunkOverlap selected
    };

    /**
     * Determine whether two schemas are equivalent, based on a field selector
     * @returns true IFF the selected fields within the schema match
     * @throws internal error if dimension sizes do not match.
     */
    bool sameSchema(ArrayDesc const& other, SchemaFieldSelector const &sel) const;

    /**
     * Determine whether two schemas have the same dimension parameters
     * @returns true IFF the schemas match
     * @throws internal error if dimension sizes do not match.
     */
    bool sameShape(ArrayDesc const& other) const
    {
        return sameSchema(other,
            SchemaFieldSelector()
                .startMin(true)
                .endMax(true)
                .chunkInterval(true)
                .chunkOverlap(true));
    }

    /**
     * Determine whether two arrays have the same partitioning.
     * @returns true IFF all dimensions have same chunk sizes and overlaps
     * @throws internal error if dimension sizes do not match.
     */
    bool samePartitioning(ArrayDesc const& other) const
    {
        return sameSchema(other,
            SchemaFieldSelector()
                .chunkInterval(true)
                .chunkOverlap(true));
    }

    /**
     * Compare the dimensions of this array descriptor with those of "other"
     * @returns true IFF all dimensions have same startMin and endMax
     * @throws internal error if dimension sizes do not match.
     */
    bool sameDimensionRanges(ArrayDesc const& other) const
    {
        return sameSchema(other,
            SchemaFieldSelector()
                .startMin(true)
                .endMax(true));
    }

    void replaceDimensionValues(ArrayDesc const& other);

    //XXX TODO: we should globally replace getDistribution()->getPartitioningSchema() with getDistributionCode()
    ArrayDistPtr getDistribution() const { SCIDB_ASSERT(_distribution); return _distribution; }
    void setDistribution(ArrayDistPtr const& dist) { SCIDB_ASSERT(dist); _distribution=dist; }

    ArrayResPtr getResidency() const { SCIDB_ASSERT(_residency); return _residency; }
    void setResidency(ArrayResPtr const& res) { SCIDB_ASSERT(res); _residency=res; }

private:
    void locateBitmapAttribute();
    void initializeDimensions();

    /**
     * The Versioned Array Identifier - unique ID for every different version of a named array.
     * This is the most important number, returned by ArrayDesc::getId(). It is used all over the system -
     * to map chunks to arrays, for transaction semantics, etc.
     */
    ArrayID _arrId;

    /**
     * The Unversioned Array Identifier - unique ID for every different named array.
     * Used to relate individual array versions to the "parent" array. Some arrays are
     * not versioned. Examples are IMMUTABLE arrays as well as NID arrays.
     * For those arrays, _arrId is is equal to _uAId (and _versionId is 0)
     */
    ArrayUAID _uAId;

    /**
     * The Array Version Number - simple, aka the number 3 in "myarray@3".
     */
    VersionID _versionId;

    std::string _namespaceName;
    std::string _arrayName;
    Attributes _attributes;
    Attributes _attributesWithoutBitmap;
    Dimensions _dimensions;
    AttributeDesc* _bitmapAttr;
    int32_t _flags;
    /// Method for distributing chunks across instances
    ArrayDistPtr _distribution;
    /// Instance over which array chunks are distributed
    ArrayResPtr _residency;
};

/**
 * Attribute descriptor
 */
class AttributeDesc
{
public:
    enum AttributeFlags
    {
        IS_NULLABLE        = 1,
        IS_EMPTY_INDICATOR = 2
    };

    /**
     * Construct empty attribute descriptor (for receiving metadata)
     */
    AttributeDesc();
    ~AttributeDesc() {}         // value class, non-virtual d'tor

    /**
     * Construct full attribute descriptor
     *
     * @param id attribute identifier
     * @param name attribute name
     * @param type attribute type
     * @param flags attribute flags from AttributeDesc::AttributeFlags
     * @param defaultCompressionMethod default compression method for this attribute
     * @param aliases attribute aliases
     * @param defaultValue default attribute value (if NULL, then use predefined default value: zero for scalar types, empty for strings,...)
     * @param comment documentation comment
     * @param varSize size of variable size type
     */
    AttributeDesc(AttributeID id, const std::string &name, TypeId type, int16_t flags,
                  uint16_t defaultCompressionMethod,
                  const std::set<std::string> &aliases = std::set<std::string>(),
                  Value const* defaultValue = NULL,
                  const std::string &defaultValueExpr = std::string(),
                  size_t varSize = 0);


    /**
     * Construct full attribute descriptor
     *
     * @param id attribute identifier
     * @param name attribute name
     * @param type attribute type
     * @param flags attribute flags from AttributeDesc::AttributeFlags
     * @param defaultCompressionMethod default compression method for this attribute
     * @param aliases attribute aliases
     * @param reserve percent of chunk space reserved for future updates
     * @param defaultValue default attribute value (if NULL, then use predefined default value: zero for scalar types, empty for strings,...)
     * @param comment documentation comment
     * @param varSize size of variable size type
     */
    AttributeDesc(AttributeID id, const std::string &name, TypeId type, int16_t flags,
                  uint16_t defaultCompressionMethod,
                  const std::set<std::string> &aliases,
                  int16_t reserve, Value const* defaultValue = NULL,
                  const std::string &defaultValueExpr = std::string(),
                  size_t varSize = 0);

    bool operator == (AttributeDesc const& other) const;
    bool operator != (AttributeDesc const& other) const
    {
        return !(*this == other);
    }

    /**
     * Get attribute identifier
     * @return attribute identifier
     */
    AttributeID getId() const;

    /**
     * Get attribute name
     * @return attribute name
     */
    const std::string& getName() const;

    /**
     * Get attribute aliases
     * @return attribute aliases
     */
    const std::set<std::string>& getAliases() const;

    /**
     * Assign new alias to attribute
     * @alias alias name
     */
    void addAlias(const std::string& alias);

    /**
     * Check if such alias present in aliases
     * @alias alias name
     * @return true if such alias present
     */
    bool hasAlias(const std::string& alias) const;

    /**
     * Get chunk reserved space percent
     * @return reserved percent of chunk size
     */
    int16_t getReserve() const;

    /**
     * Get attribute type
     * @return attribute type
     */
    TypeId getType() const;

    /**
     * Check if this attribute can have NULL values
     */
    bool isNullable() const;

    /**
     * Check if this arttribute is empty cell indicator
     */
    bool isEmptyIndicator() const;

    /**
     * Get default compression method for this attribute: it is possible to specify explicitly different
     * compression methods for each chunk, but by default one returned by this method is used
     */
    uint16_t getDefaultCompressionMethod() const;

    /**
     * Get default attribute value
     */
    Value const& getDefaultValue() const;

    /**
     * Get attribute flags
     * @return attribute flags
     */
    int16_t getFlags() const;

    /**
     * Return type size or var size (in bytes) or 0 for truly variable size.
     */
    size_t getSize() const;

    /**
     * Get the optional variable size.v
     */
    size_t getVarSize() const;

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] output stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    void toString (std::ostream&, int indent = 0) const;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & _id;
        ar & _name;
        ar & _aliases;
        ar & _type;
        ar & _flags;
        ar & _defaultCompressionMethod;
        ar & _reserve;
        ar & _defaultValue;
        ar & _varSize;
        ar & _defaultValueExpr;
    }

    /**
     * Return expression string which used for default value.
     *
     * @return expression string
     */
    const std::string& getDefaultValueExpr() const;

private:
    AttributeID _id;
    std::string _name;
    std::set<std::string> _aliases;
    TypeId _type;
    int16_t _flags;
    uint16_t _defaultCompressionMethod;
    int16_t _reserve;
    Value _defaultValue;
    size_t _varSize;

    /**
     * Compiled and serialized expression for evaluating default value. Used only for storing/retrieving
     * to/from system catalog. Default value evaluated once after fetching metadata or during schema
     * construction in parser. Later only Value field passed between schemas.
     *
     * We not using Expression object because this class used on client.
     * actual value.
     */
    //TODO: May be good to have separate Metadata interface for client library
    std::string _defaultValueExpr;
};

/**
 * Descriptor of dimension
 */
class DimensionDesc : public ObjectNames, boost::equality_comparable<DimensionDesc>
{
public:
    /**
     * Special (non-positive) chunk interval values.
     */
    enum SpecialIntervals {
        UNINITIALIZED = 0,      //< chunkInterval value not yet initialized.
        AUTOCHUNKED   = -1,     //< chunkInterval will (eventually) be automatically computed.
        PASSTHRU      = -2,     //< redimension uses chunkInterval from another corresponding input dimension
        LAST_SPECIAL = -3,     // All enums must be greater than this. This MUST be the last.
    };

    /**
     * Construct empty dimension descriptor (for receiving metadata)
     */
    DimensionDesc();

    virtual ~DimensionDesc() final {} // value class; no more subclassing

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param name dimension name
     * @param start dimension start
     * @param end dimension end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     */
    DimensionDesc(const std::string &name,
                  Coordinate start, Coordinate end,
                  int64_t chunkInterval, int64_t chunkOverlap);

    /**
     *
     * @param baseName name of dimension derived from catalog
     * @param names dimension names and/ aliases collected during query compilation
     * @param start dimension start
     * @param end dimension end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     */
    DimensionDesc(const std::string &baseName, const NamesType &names,
                  Coordinate start, Coordinate end,
                  int64_t chunkInterval, int64_t chunkOverlap);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param name dimension name
     * @param startMin dimension minimum start
     * @param currSart dimension current start
     * @param currMax dimension current end
     * @param endMax dimension maximum end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     */
    DimensionDesc(const std::string &name,
                  Coordinate startMin, Coordinate currStart,
                  Coordinate currEnd, Coordinate endMax,
                  int64_t chunkInterval, int64_t chunkOverlap);

    /**
     * Construct full descriptor (for returning metadata from catalog)
     *
     * @param baseName dimension name derived from catalog
     * @param name dimension names and/ aliases collected during query compilation
     * @param startMin dimension minimum start
     * @param currStart dimension current start
     * @param currEnd dimension current end
     * @param endMax dimension maximum end
     * @param chunkInterval chunk size in this dimension
     * @param chunkOverlap chunk overlay in this dimension
     */
    DimensionDesc(const std::string &baseName, const NamesType &names,
                  Coordinate startMin, Coordinate currStart,
                  Coordinate currEnd, Coordinate endMax,
                  int64_t chunkInterval, int64_t chunkOverlap);

    bool operator == (DimensionDesc const&) const;

    /**
     * @brief replace the values, excluding the name, with those from another descriptor
     */
    void replaceValues(DimensionDesc const &other)
    {
        setStartMin(other.getStartMin());
        setEndMax(other.getEndMax());

        setRawChunkInterval(other.getRawChunkInterval());
        setChunkOverlap(other.getChunkOverlap());

        setCurrStart(other.getCurrStart());
        setCurrEnd(other.getCurrEnd());
    }

    /**
     * @return minimum start coordinate.
     * @note This is reliable. The value is independent of the data in the array.
     */
    Coordinate getStartMin() const
    {
        return _startMin;
    }

    /**
     * @return current start coordinate.
     * @note In an array with no data, getCurrStart()=CoordinateBounds::getMax() and getCurrEnd()=CoordinateBounds::getMin().
     * @note This is NOT reliable.
     *       The only time the value can be trusted is right after the array schema is generated by scan().
     *       As soon as we add other ops into the mix (e.g. filter(scan()), the value is not trustworthy anymore.
     */
    Coordinate getCurrStart() const
    {
        return _currStart;
    }

    /**
     * @return current end coordinate.
     * @note This is NOT reliable. @see getCurrStart().
     */
    Coordinate getCurrEnd() const
    {
        return _currEnd;
    }

    /**
     * @return maximum end coordinate.
     * @note This is reliable. @see getStartMin().
     */
    Coordinate getEndMax() const
    {
        return _endMax;
    }

    /**
     * @return dimension length
     * @note This is reliable. @see getStartMin().
     */
    uint64_t getLength() const
    {
        return _endMax - _startMin + 1;
    }

    /**
     * @brief Determine if the max value matches '*'
     * @return true={max=='*'}, false otherwise
     */
    bool isMaxStar() const
    {
        return CoordinateBounds::isMaxStar(getEndMax());
    }

    /**
     * @return current dimension length.
     * @note This is NOT reliable. @see getCurrStart().
     * @note This may read from the system catalog.
     */
    uint64_t getCurrLength() const;

    /**
     * Get the chunk interval (during query execution)
     *
     * @return the chunk interval in this dimension, not including overlap.
     * @throws ASSERT_EXCEPTION if interval is unspecified (aka autochunked)
     * @see getRawChunkInterval
     * @see https://trac.scidb.net/wiki/Development/components/Rearrange_Ops/RedimWithAutoChunkingFS
     *
     * @description
     * Ordinarily #getChunkInterval() would be a simple accessor, but due to autochunking, chunk
     * intervals may not be knowable until actual query execution.  Specifically, operators must be
     * prepared to handle unknown chunk intervals right up until the inputArrays[] are received by
     * their PhysicalFoo::execute() method.  The input arrays are guaranteed to have fully resolved
     * chunk intervals, so it is safe to #getChunkInterval() in execute() methods and in the Array
     * subclass objects that they build.
     *
     * @p At all other times (parsing, schema inference, optimization, maybe elsewhere), code most
     * be prepared to Do The Right Thing(tm) when it encounters a DimensionDesc::AUTOCHUNKED chunk
     * interval.  Specifically, code must either call #getRawChunkInterval() and take appropriate
     * action if the return value is AUTOCHUNKED, or else guard calls to #getChunkInterval() by
     * first testing the #DimensionDesc::isAutochunked() predicate.  What a particular operator does
     * for autochunked dimensions depends on the operator.  Many operators merely need to propragate
     * the autochunked interval up the inferSchema() tree using getRawChunkInterval(), but other
     * code may require more thought.
     *
     * @p There was no #getRawChunkInterval() before autochunking, so by placing an ASSERT_EXCEPTION
     * in this "legacy" #getChunkInterval() method we intend to catch all the places in the code
     * that need to change (hopefully during development rather than during QA).
     *
     * @note Only redimension() and repart() may have autochunked *schema parameters*, all other
     *       operators must disallow that---either by using #getRawChunkInterval() or by testing
     *       with isAutochunked(), and possibly throwing SCIDB_LE_AUTOCHUNKING_NOT_SUPPORTED.
     *       Autochunked input schemas (as opposed to parameter schemas) are permitted everywhere in
     *       the logical plan, and their intervals should be propagated up the query tree with
     *       #getRawChunkInterval().  If an operator has particular constraints on input chunk
     *       intervals (as gemm() and svd() do), it should @i try to make the checks at logical
     *       inferSchema() time, but if it cannot because one or more intervals are unspecified, it
     *       @b must do the checks at physical execute() time, when all unknown autochunked
     *       intervals will have been resolved.  During query execution it's safe to call
     *       #getChunkInterval() without any checking.
     */
    int64_t getChunkInterval() const
    {
        ASSERT_EXCEPTION(isIntervalResolved(), "Caller not yet modified for autochunking.");
        return _chunkInterval;
    }

    /**
     * Get the possibly-not-yet-specified chunk interval (during query planning)
     *
     * @return the raw chunk interval in this dimension (not including overlap), or AUTOCHUNKED.
     * @see getChunkInterval
     * @note Callers that are autochunk-aware should call this version rather
     *       than getChunkInterval().  Other callers would presumably not be
     *       prepared for a return value of AUTOCHUNKED.
     */
    int64_t getRawChunkInterval() const
    {
        return _chunkInterval;
    }

    /**
     * @return the chunk interval in this dimension, or useMe if the interval is autochunked.
     */
    int64_t getChunkIntervalIfAutoUse(int64_t useMe) const
    {
        return _chunkInterval == AUTOCHUNKED ? useMe : _chunkInterval;
    }

    /**
     * @return true iff interval in this dimension is autochunked.
     */
    bool isAutochunked() const
    {
        return _chunkInterval == AUTOCHUNKED;
    }

    bool isIntervalResolved() const
    {
        SCIDB_ASSERT(_chunkInterval > LAST_SPECIAL);
        // UNINITIALIZED counts as "resolved" because there is no other value
        // that is going to 'magically' replace it, whereas, these two will be
        // replaced at execution time.
        return _chunkInterval != AUTOCHUNKED && _chunkInterval != PASSTHRU;
    }

    /**
     * @return chunk overlap in this dimension.
     * @note Given base coordinate Xi, a chunk stores data with coordinates in
     *       [Xi-getChunkOverlap(), Xi+getChunkInterval()+getChunkOverlap()].
     */
    int64_t getChunkOverlap() const
    {
        return _chunkOverlap;
    }

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] os output stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     */
    void toString (std::ostream&, int indent = 0) const;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<ObjectNames>(*this);
        ar & _startMin;
        ar & _currStart;
        ar & _currEnd;
        ar & _endMax;
        ar & _chunkInterval;
        ar & _chunkOverlap;
    }

    void setCurrStart(Coordinate currStart)
    {
        _currStart = currStart;
    }

    void setCurrEnd(Coordinate currEnd)
    {
        _currEnd = currEnd;
    }

    void setStartMin(Coordinate startMin)
    {
        _startMin = startMin;
    }

    void setEndMax(Coordinate endMax)
    {
        _endMax = endMax;
    }

    void setChunkInterval(int64_t i)
    {
        assert(i > 0);
        _chunkInterval = i;
    }

    void setRawChunkInterval(int64_t i)
    {
        assert(i > 0 || i == AUTOCHUNKED || i == PASSTHRU);
        _chunkInterval = i;
    }

    void setChunkOverlap(int64_t i)
    {
        assert(i >= 0);
        _chunkOverlap = i;
    }

private:
    void validate() const;

private:
    friend class ArrayDesc;

    Coordinate _startMin;
    Coordinate _currStart;

    Coordinate _currEnd;
    Coordinate _endMax;

    /**
     * The length of the chunk along this dimension, excluding overlap.
     *
     * Chunk Interval is often used as part of coordinate math and coordinates are signed int64. To
     * make life easier for everyone, chunk interval is also signed for the moment. Same with
     * position_t in RLE.h.
     */
    int64_t _chunkInterval;

    /**
     * The length of just the chunk overlap along this dimension.
     * Signed to make coordinate math easier.
     */
    int64_t _chunkOverlap;

    ArrayDesc* _array;
};

/**
 * Descriptor of pluggable logical operator
 */
class LogicalOpDesc
{
public:
    /**
     * Default constructor
     */
    LogicalOpDesc()
    {}

    /**
     * Construct descriptor for adding to catalog
     *
     * @param name Operator name
     * @param module Operator module
     * @param entry Operator entry in module
     */
    LogicalOpDesc(const std::string& name,
                  const std::string& module,
                  const std::string& entry) :
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Construct full descriptor
     *
     * @param logicalOpId Logical operator identifier
     * @param name Operator name
     * @param module Operator module
     * @param entry Operator entry in module
     */
    LogicalOpDesc(OpID logicalOpId, const std::string& name, const std::string& module,
                    const std::string& entry) :
        _logicalOpId(logicalOpId),
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Get logical operator identifier
     *
     * @return Operator identifier
     */
    OpID getLogicalOpId() const
    {
        return _logicalOpId;
    }

    /**
     * Get logical operator name
     *
     * @return Operator name
     */
    const std::string& getName() const
    {
        return _name;
    }

    /**
     * Get logical operator module
     *
     * @return Operator module
     */
    const std::string& getModule() const
    {
        return _module;
    }

    /**
     * Get logical operator entry in module
     *
     * @return Operator entry
     */
    const std::string& getEntry() const
    {
        return _entry;
    }

private:
    OpID        _logicalOpId;
    std::string _name;
    std::string _module;
    std::string _entry;
};

class PhysicalOpDesc
{
public:
    /**
     * Default constructor
     */
    PhysicalOpDesc()
    {}

    PhysicalOpDesc(const std::string& logicalOpName, const std::string& name,
                const std::string& module, const std::string& entry) :
        _logicalOpName(logicalOpName),
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Construct full descriptor
     *
     * @param physicalOpId Operator identifier
     * @param logicalOpName Logical operator name
     * @param name Physical operator name
     * @param module Operator module
     * @param entry Operator entry in module
     * @return
     */
    PhysicalOpDesc(OpID physicalOpId, const std::string& logicalOpName,
                   const std::string& name, const std::string& module,
                   const std::string& entry) :
        _physicalOpId(physicalOpId),
        _logicalOpName(logicalOpName),
        _name(name),
        _module(module),
        _entry(entry)
    {}

    /**
     * Get physical operator identifier
     *
     * @return Operator identifier
     */
    OpID getId() const
    {
        return _physicalOpId;
    }

    /**
     * Get logical operator name
     *
     * @return Operator name
     */
    const std::string& getLogicalName() const
    {
        return _logicalOpName;
    }

    /**
     * Get physical operator name
     *
     * @return Operator name
     */
    const std::string& getName() const
    {
        return _name;
    }

    /**
     * Get physical operator module
     *
     * @return Operator module
     */
    const std::string& getModule() const
    {
        return _module;
    }

    /**
     * Get physical operator entry in module
     *
     * @return Operator entry
     */
    const std::string& getEntry() const
    {
        return _entry;
    }

  private:
    OpID        _physicalOpId;
    std::string _logicalOpName;
    std::string _name;
    std::string _module;
    std::string _entry;
};

class VersionDesc
{
  public:
    VersionDesc(ArrayID a = 0,VersionID v = 0,time_t t = 0)
        : _arrayId(a),
          _versionId(v),
          _timestamp(t)
    {}

    ArrayID getArrayID() const
    {
        return _arrayId;
    }

    VersionID getVersionID() const
    {
        return _versionId;
    }

    time_t getTimeStamp() const
    {
        return _timestamp;
    }

  private:
    ArrayID   _arrayId;
    VersionID _versionId;
    time_t    _timestamp;
};


/**
 * Helper function to add the empty tag attribute to Attributes,
 * if the empty tag attribute did not already exist.
 *
 * @param   attributes  the original Attributes
 * @return  the new Attributes
 */
inline Attributes addEmptyTagAttribute(const Attributes& attributes) {
    size_t size = attributes.size();
    assert(size>0);
    if (attributes[size-1].isEmptyIndicator()) {
        return attributes;
    }
    Attributes newAttributes = attributes;
    newAttributes.push_back(AttributeDesc((AttributeID)newAttributes.size(),
            DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,  TID_INDICATOR, AttributeDesc::IS_EMPTY_INDICATOR, 0));
    return newAttributes;
}

/**
 * Helper function to add the empty tag attribute to ArrayDesc,
 * if the empty tag attribute did not already exist.
 *
 * @param   desc    the original ArrayDesc
 * @return  the new ArrayDesc
 */
inline ArrayDesc addEmptyTagAttribute(ArrayDesc const& desc)
{
    //XXX: This does not check to see if some other attribute does not already have the same name
    //     and it would be faster to mutate the structure, not copy. See also ArrayDesc::addAttribute
    return ArrayDesc(desc.getName(),
                     addEmptyTagAttribute(desc.getAttributes()),
                     desc.getDimensions(),
                     desc.getDistribution(),
                     desc.getResidency());
}

/**
 * Compute the first position of a chunk, given the chunk position and the dimensions info.
 * @param[in]  chunkPos      The chunk position (not including overlap)
 * @param[in]  dims          The dimensions.
 * @param[in]  withOverlap   Whether overlap is respected.
 * @return the first chunk position
 */
inline Coordinates computeFirstChunkPosition(Coordinates const& chunkPos,
                                             Dimensions const& dims,
                                             bool withOverlap = true)
{
    assert(chunkPos.size() == dims.size());
    if (!withOverlap) {
        return chunkPos;
    }

    Coordinates firstPos = chunkPos;
    for (size_t i=0; i<dims.size(); ++i) {
        assert(chunkPos[i]>=dims[i].getStartMin());
        assert(chunkPos[i]<=dims[i].getEndMax());

        firstPos[i] -= dims[i].getChunkOverlap();
        if (firstPos[i] < dims[i].getStartMin()) {
            firstPos[i] = dims[i].getStartMin();
        }
    }
    return firstPos;
}

/**
 * Compute the last position of a chunk, given the chunk position and the dimensions info.
 * @param[in]  chunkPos      The chunk position (not including overlap)
 * @param[in]  dims          The dimensions.
 * @param[in]  withOverlap   Whether overlap is respected.
 * @return the last chunk position
 */
inline Coordinates computeLastChunkPosition(Coordinates const& chunkPos,
                                            Dimensions const& dims,
                                            bool withOverlap = true)
{
    assert(chunkPos.size() == dims.size());

    Coordinates lastPos = chunkPos;
    for (size_t i=0; i<dims.size(); ++i) {
        assert(chunkPos[i]>=dims[i].getStartMin());
        assert(chunkPos[i]<=dims[i].getEndMax());

        lastPos[i] += dims[i].getChunkInterval()-1;
        if (withOverlap) {
            lastPos[i] += dims[i].getChunkOverlap();
        }
        if (lastPos[i] > dims[i].getEndMax()) {
            lastPos[i] = dims[i].getEndMax();
        }
    }
    return lastPos;
}

/**
 * Get the logical space size of a chunk.
 * @param[in]  low   the low position of the chunk
 * @param[in]  high  the high position of the chunk
 * @return     #cells in the space that the chunk covers
 * @throw      SYSTEM_EXCEPTION(SCIDB_SE_METADATA, SCIDB_LE_LOGICAL_CHUNK_SIZE_TOO_LARGE)
 */
size_t getChunkNumberOfElements(Coordinates const& low, Coordinates const& high);

/**
 * Get the logical space size of a chunk.
 * @param[in]  chunkPos      The chunk position (not including overlap)
 * @param[in]  dims          The dimensions.
 * @param[in]  withOverlap   Whether overlap is respected.
 * @return     #cells in the space the cell covers
 */
inline size_t getChunkNumberOfElements(Coordinates const& chunkPos,
                                       Dimensions const& dims,
                                       bool withOverlap = true)
{
    Coordinates lo(computeFirstChunkPosition(chunkPos,dims,withOverlap));
    Coordinates hi(computeLastChunkPosition (chunkPos,dims,withOverlap));
    return getChunkNumberOfElements(lo,hi);
}


/**
 * Print only the pertinent part of the relevant object.
 */
void printDimNames(std::ostream&, const Dimensions&);
void printSchema(std::ostream&,const Dimensions&);
void printSchema(std::ostream&,const DimensionDesc&,bool verbose=false);
void printSchema(std::ostream&,const ArrayDesc&);
void printNames (std::ostream&,const ObjectNames::NamesType&);
std::ostream& operator<<(std::ostream&,const Attributes&);
std::ostream& operator<<(std::ostream&,const ArrayDesc&);
std::ostream& operator<<(std::ostream&,const AttributeDesc&);
std::ostream& operator<<(std::ostream&,const DimensionDesc&);
std::ostream& operator<<(std::ostream&,const Dimensions&);
std::ostream& operator<<(std::ostream&,const ObjectNames::NamesType&);

} // namespace

#endif /* METADATA_H_ */
