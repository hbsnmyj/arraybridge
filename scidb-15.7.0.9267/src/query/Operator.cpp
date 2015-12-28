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
 * @file Operator.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of basic operator methods.
 */

#include <boost/foreach.hpp>
#include <memory>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <unordered_set>
#include <log4cxx/logger.h>

#include <query/QueryPlanUtilites.h>
#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <network/NetworkManager.h>
#include <network/BaseConnection.h>
#include <network/MessageUtils.h>
#include <network/NetworkManager.h>
#include <system/SystemCatalog.h>
#include <array/DBArray.h>
#include <array/TransientCache.h>
#include <query/QueryProcessor.h>
#include <system/BlockCyclic.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>
#include <smgr/io/Storage.h>
#include <boost/functional/hash.hpp>
#include <util/Hashing.h>
#include <util/Timing.h>
#include <util/MultiConstIterators.h>
#include <util/session/Session.h>

using namespace std;
using namespace boost;

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.operator"));

template <typename T>
inline static T min( T const& lhs, T const& rhs)
{
    return lhs < rhs ? lhs : rhs;
}

template <typename T>
inline static T max( T const& lhs, T const& rhs)
{
    return lhs > rhs ? lhs : rhs;
}


#define stringify(name) #name

static const char *OperatorParamPlaceholderTypeNames[] =
{
        stringify(PLACEHOLDER_INPUT),
        stringify(PLACEHOLDER_ARRAY_NAME),
        stringify(PLACEHOLDER_ATTRIBUTE_NAME),
        stringify(PLACEHOLDER_DIMENSION_NAME),
        stringify(PLACEHOLDER_CONSTANT),
        stringify(PLACEHOLDER_EXPRESSION),
        stringify(PLACEHOLDER_VARIES),
        stringify(PLACEHOLDER_SCHEMA),
        stringify(PLACEHOLDER_AGGREGATE_CALL),
        stringify(PLACEHOLDER_END_OF_VARIES)
};

void OperatorParamPlaceholder::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    int typeIndex = 0, type = _placeholderType;
    while ((type >>= 1) != 0) {
        typeIndex += 1;
    }
    out << "[opParamPlaceholder] " << OperatorParamPlaceholderTypeNames[typeIndex];
    out << " requiredType " <<_requiredType.name();
    out <<" ischeme "<<_inputSchema<<"\n";
}

void OperatorParam::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out <<"[param] type "<<_paramType<<"\n";
}

void OperatorParamReference::toString(std::ostream &out, int /*indent*/) const
{
    out << "object "<<_objectName;
    out << " inputNo " <<_inputNo;
    out << " objectNo " <<_objectNo;
    out << " inputScheme "<<_inputScheme;
    out << "\n";
}

void OperatorParamArrayReference::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramArrayReference] ";
    OperatorParamReference::toString(out, indent);
}

void OperatorParamAttributeReference::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramAttributeReference] ";
    OperatorParamReference::toString(out,indent);
}

void OperatorParamDimensionReference::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramDimensionReference] ";
    OperatorParamReference::toString(out,indent);
}

void OperatorParamLogicalExpression::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramLogicalExpression] type "<<_expectedType.name();
    out << " const " << _constant << "\n";
    _expression->toString(out, indent+1);
}

void OperatorParamPhysicalExpression::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramPhysicalExpression] const " << _constant;
    out << "\n";
    _expression->toString(out, indent+1);
}

void OperatorParamSchema::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out <<"[paramSchema] " << _schema <<"\n";
}

void OperatorParamAggregateCall::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramAggregateCall] " << _aggregateName << "\n" ;

    out << prefix(' ', false);
    out <<"input: ";
    _inputAttribute->toString(out);

    if (_alias.size() )
    {
        out << prefix(' ');
        out << "alias " << _alias << "\n";
    }
}

void OperatorParamAsterisk::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "[paramAsterisk] *" << "\n";
}

void LogicalOperator::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[lOperator] "<<_logicalName;
    out << " ddl "<< _properties.ddl << "\n";

    for (size_t i = 0; i < _parameters.size(); i++)
    {
        _parameters[i]->toString(out, indent+1);
    }

    for ( size_t i = 0; i < _paramPlaceholders.size(); i++)
    {
        _paramPlaceholders[i]->toString(out,indent+1);
    }

    out << prefix('>', false);
    out << "schema: " << _schema << "\n";
}

void PhysicalOperator::setQuery(const std::shared_ptr<Query>& query)
{
    arena::Options options;                              // Arena ctor args

    options.name  (_physicalName.c_str());               // Use operator name
    options.parent(query->getArena());                   // Attach to query
    options.threading(true);                             // Assume threaded

    _query = query;                                      // Save the query
    _arena = arena::newArena(options);                   // Create new Arena
}

void PhysicalOperator::toString(std::ostream &out, int indent) const
{
    Indent prefix(indent);
    out << prefix(' ', false);
    out << "schema " <<_schema<<"\n";
}

void PhysicalOperator::dumpArrayToLog(std::shared_ptr<Array> const& input, log4cxx::LoggerPtr& logger)
{
    ArrayDesc const& schema = input->getArrayDesc();
    Attributes const& attrs = schema.getAttributes(true);
    size_t const nAttrs = attrs.size();
    vector<FunctionPointer> converters(nAttrs,NULL);
    FunctionLibrary *functionLib = FunctionLibrary::getInstance();
    vector<std::shared_ptr<ConstArrayIterator> > aiters (nAttrs);
    vector<std::shared_ptr<ConstChunkIterator> > citers (nAttrs);
    for (size_t i =0; i<nAttrs; ++i)
    {
        TypeId const& typeId = attrs[i].getType();
        converters[i] = functionLib->findConverter(typeId, TID_STRING, false, false, NULL);
        aiters[i] = input->getConstIterator(i);
    }
    while (!aiters[0]->end())
    {
        for(size_t i=0; i<nAttrs; ++i)
        {
            citers[i] = aiters[i]->getChunk().getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);
        }
        while (!citers[0]->end())
        {
            Coordinates const& position = citers[0]->getPosition();
            ostringstream out;
            out<<CoordsToStr(position)<<" ";
            for(size_t i=0; i<nAttrs; ++i)
            {
                Value const& v = citers[i]->getItem();
                if (v.isNull())
                {
                    if(v.getMissingReason() == 0)
                    {
                        out<<"[null]";
                    }
                    else
                    {
                        out<<"[?"<<v.getMissingReason()<<"]";
                    }
                }
                else if (converters[i])
                {
                    Value const* input = &v;
                    Value result;
                    converters[i](&input, &result, NULL);
                    out<<result.getString();
                }
                else
                {
                    out<<"[nct]";
                }
                out<<",";
            }
            LOG4CXX_DEBUG(logger, out.str());
            for(size_t i=0; i<nAttrs; ++i)
            {
                ++(*citers[i]);
            }
        }
        for(size_t i=0; i<nAttrs; ++i)
        {
            ++(*aiters[i]);
        }
    }
}

std::shared_ptr<Array>
PhysicalOperator::ensureRandomAccess(std::shared_ptr<Array>& input,
                                     std::shared_ptr<Query> const& query)
{
    if (input->getSupportedAccess() == Array::RANDOM)
    {
        return input;
    }
    LOG4CXX_DEBUG(logger, "Query "<<query->getQueryID()<<
                  " materializing input "<<input->getArrayDesc());
    bool vertical = (input->getSupportedAccess() == Array::MULTI_PASS);
    std::shared_ptr<MemArray> memCopy(new MemArray(input, query, vertical));
    input.reset();

    return memCopy;
}

/*
 * PhysicalBoundaries methods
 */
PhysicalBoundaries::PhysicalBoundaries(Coordinates const& start, Coordinates const& end, double density):
    _startCoords(start), _endCoords(end), _density(density)
{
    if (_startCoords.size() != _endCoords.size()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
            << "number of dimensions";
    }

    if (density < 0.0 || density > 1.0) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
            << "density not between 0.0 and 1.0";
    }

    for (size_t i = 0; i< _startCoords.size(); i++)
    {
        if (_startCoords[i] < CoordinateBounds::getMin())
        {
            _startCoords[i] = CoordinateBounds::getMin();
        }
        else if (_startCoords[i] > CoordinateBounds::getMax())
        {
            _startCoords[i] = CoordinateBounds::getMax();
        }

        if (_endCoords[i] < CoordinateBounds::getMin())
        {
            _endCoords[i] = CoordinateBounds::getMin();
        }
        else if (_endCoords[i] > CoordinateBounds::getMax())
        {
            _endCoords[i] = CoordinateBounds::getMax();
        }
    }
}

PhysicalBoundaries PhysicalBoundaries::createFromFullSchema(ArrayDesc const& schema )
{
    Coordinates resultStart, resultEnd;

    for (size_t i =0; i<schema.getDimensions().size(); i++)
    {
        resultStart.push_back(schema.getDimensions()[i].getStartMin());
        resultEnd.push_back(schema.getDimensions()[i].getEndMax());
    }

    return PhysicalBoundaries(resultStart, resultEnd);
}

PhysicalBoundaries PhysicalBoundaries::createEmpty(size_t numDimensions)
{
    Coordinates resultStart, resultEnd;

    for (size_t i =0; i<numDimensions; i++)
    {
        resultStart.push_back(CoordinateBounds::getMax());
        resultEnd.push_back(CoordinateBounds::getMin());
    }

    return PhysicalBoundaries(resultStart, resultEnd);
}

bool PhysicalBoundaries::isEmpty() const
{
    if (_startCoords.size() == 0)
    {
        return true;
    }

    for (size_t i = 0; i<_startCoords.size(); i++)
    {
        if (_startCoords[i] > _endCoords[i])
        {
            return true;
        }
    }
    return false;
}

uint64_t PhysicalBoundaries::getCellNumber (Coordinates const& coords, Dimensions const& dims)
{
    if (dims.size() != coords.size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
            << "number of dimensions";
    }
    uint64_t result = 0;
    for ( size_t i = 0, n = dims.size(); i < n; i++)
    {
        if (dims[i].getLength()==0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
                << "dimension has zero length";
        }
        uint64_t t = result * dims[i].getLength();
        if (t / dims[i].getLength() != result) //overflow check multiplication
        {
            return CoordinateBounds::getMaxLength();
        }
        result = t;
        t = result + coords[i] - dims[i].getStartMin();
        if (t < result) //overflow check addition
        {
            return CoordinateBounds::getMaxLength();
        }
        result = t;
    }
    if (!CoordinateBounds::isValidLength(result))
    {
        return CoordinateBounds::getMaxLength();
    }
    return result;
}

Coordinates PhysicalBoundaries::getCoordinates(uint64_t& cellNum, Dimensions const& dims, bool strictCheck)
{
    if(cellNum >= CoordinateBounds::getMaxLength())
    {
        return Coordinates(dims.size(), CoordinateBounds::getMax());
    }
    Coordinates result (dims.size(), 0);
    for (int i = dims.size(); --i >= 0;)
    {
        result[i] = dims[i].getStartMin() + (cellNum % dims[i].getLength());
        cellNum /= dims[i].getLength();
    }
    if (strictCheck && cellNum != 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
            << "non-zero cell number";
    }
    return result;
}

Coordinates PhysicalBoundaries::reshapeCoordinates (Coordinates const& in,
                                                    Dimensions const& currentDims,
                                                    Dimensions const& newDims)
{
    uint64_t cellNum = getCellNumber(in, currentDims);
    if ( cellNum >= CoordinateBounds::getMaxLength() )
    {
        return Coordinates(newDims.size(), CoordinateBounds::getMax());
    }
    return getCoordinates(cellNum, newDims);
}

uint64_t PhysicalBoundaries::getNumCells (Coordinates const& start, Coordinates const& end)
{
    if (PhysicalBoundaries(start,end).isEmpty())
    {
        return 0;
    }
    uint64_t result = 1;
    for ( size_t i = 0; i < end.size(); i++)
    {
        if (start[i] <= CoordinateBounds::getMin() || end[i] >= CoordinateBounds::getMax())
        {
            return CoordinateBounds::getMaxLength();
        }
        else if(end[i] >= start[i])
        {
            uint64_t t = result * (end[i] - start[i] + 1);
            if ( t / (end[i] - start[i] + 1) != result) //overflow check multiplication
            {
                return CoordinateBounds::getMaxLength();
            }
            result = t;
        }
        else
        {
            result *= 0;
        }
    }
    if (!CoordinateBounds::isValidLength(result))
    {
        return CoordinateBounds::getMaxLength();
    }
    return result;
}

uint64_t PhysicalBoundaries::getNumCells() const
{
    return getNumCells(_startCoords, _endCoords);
}

uint64_t PhysicalBoundaries::getCellsPerChunk (Dimensions const& dims)
{
    uint64_t cellsPerChunk = 1;
    for (size_t i = 0; i<dims.size(); i++)
    {
        //assume the dimensions that are passed in come from an array, therefore this is overflow-safe
        cellsPerChunk *= dims[i].getChunkInterval();
    }
    return cellsPerChunk;
}

uint64_t PhysicalBoundaries::getNumChunks(Dimensions const& dims) const
{
    if (_startCoords.size() != dims.size())
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
            << "number of dimensions";
    }

    if (isEmpty())
    {
        return 0;
    }

    uint64_t result = 1;
    for (size_t i =0; i < _endCoords.size(); i++)
    {
        if (_startCoords[i]<=CoordinateBounds::getMin() || _endCoords[i]>=CoordinateBounds::getMax())
        {
            return CoordinateBounds::getMaxLength();
        }

        DimensionDesc const& dim = dims[i];
        if (_startCoords[i] < dim.getStartMin() || _endCoords[i] > dim.getEndMax())
        {
            stringstream ss;
            ss << "target dimension too small";
            if (_startCoords[i] < dim.getStartMin()) {
                ss << std::endl << "----- _startCoords=" << _startCoords[i] << " is less than " << "getStartMin=" << dim.getStartMin();
            }

            if (_endCoords[i] > dim.getEndMax()) {
                ss << std::endl << "----- _endCoords=" << _endCoords[i] << " is greater than " << "getEndMax=" << dim.getEndMax();
            }

            // Note: *not* an internal error, this can happen for example if we insert(unbounded, bounded)
            // and the data in the unbounded (i.e. <...>[i=0:*,...]) array does not fit inside the bounded one.
            // See ticket #2016.
            throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_MISMATCHED_COORDINATES_IN_PHYSICAL_BOUNDARIES)
                << ss.str();
        }

        Coordinate arrayStart = dim.getStartMin();
        int64_t chunkInterval = dim.getChunkInterval();
        Coordinate physStart = _startCoords[i]; //TODO:OPTAPI (- overlap) ?
        Coordinate physEnd = _endCoords[i];
        int64_t numChunks = chunkInterval == 0 ? 0 :
            ((physEnd - arrayStart + chunkInterval) / chunkInterval) - ((physStart - arrayStart) / chunkInterval);

        uint64_t t = result * numChunks;
        if ( numChunks && t / numChunks != result) //overflow check multiplication
        {
            return CoordinateBounds::getMaxLength();
        }
        result = t;
    }
    return result;
}

std::ostream& operator<< (std::ostream& stream, const PhysicalBoundaries& bounds)
{
    stream<<"start "<<bounds._startCoords<<" end "<<bounds._endCoords<<" density "<<bounds._density;
    return stream;
}

bool PhysicalBoundaries::isInsideBox (Coordinate const& in, size_t const& dimensionNum) const
{
    if (dimensionNum >= _startCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_WRONG_NUMBER_OF_DIMENSIONS);

    Coordinate start = _startCoords[dimensionNum];
    Coordinate end = _endCoords[dimensionNum];

    return in >= start && in <= end;
}

PhysicalBoundaries PhysicalBoundaries::intersectWith (PhysicalBoundaries const& other) const
{
    if (_startCoords.size() != other._startCoords.size()
            || _startCoords.size() != other._endCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_BOUNDARIES);

    if (isEmpty() || other.isEmpty())
    {
        return createEmpty(_startCoords.size());
    }

    Coordinates start;
    for (size_t i =0; i<_startCoords.size(); i++)
    {
        Coordinate myStart = _startCoords[i];
        Coordinate otherStart = other._startCoords[i];
        start.push_back( myStart < otherStart ? otherStart : myStart );
    }

    Coordinates end;
    for (size_t i =0; i<_endCoords.size(); i++)
    {
        Coordinate myEnd = _endCoords[i];
        Coordinate otherEnd = other._endCoords[i];
        end.push_back( myEnd > otherEnd ? otherEnd : myEnd);
    }

    double myCells = getNumCells();
    double otherCells = other.getNumCells();
    double intersectionCells = getNumCells(start,end);

    double resultDensity = 1.0;
    if (intersectionCells > 0)
    {
        double maxMyDensity = min( _density * myCells / intersectionCells, 1.0 );
        double maxOtherDensity = min ( other._density * otherCells / intersectionCells, 1.0);
        resultDensity = min (maxMyDensity, maxOtherDensity);
    }

    return PhysicalBoundaries(start,end, resultDensity);
}

PhysicalBoundaries PhysicalBoundaries::unionWith (PhysicalBoundaries const& other) const
{
    if (_startCoords.size() != other._startCoords.size()
            || _startCoords.size() != other._endCoords.size())
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_MISMATCHED_BOUNDARIES);

    if (isEmpty())
    {
        return other;
    }

    else if (other.isEmpty())
    {
        return *this;
    }

    Coordinates start;
    for (size_t i =0; i<_startCoords.size(); i++)
    {
        Coordinate myStart = _startCoords[i];
        Coordinate otherStart = other._startCoords[i];
        start.push_back( myStart > otherStart ? otherStart : myStart );
    }

    Coordinates end;
    for (size_t i =0; i<_endCoords.size(); i++)
    {
        Coordinate myEnd = _endCoords[i];
        Coordinate otherEnd = other._endCoords[i];
        end.push_back( myEnd < otherEnd ? otherEnd : myEnd);
    }

    double myCells = getNumCells();
    double otherCells = other.getNumCells();
    double resultCells = getNumCells(start, end);
    double maxDensity = min ( (myCells * _density + otherCells * other._density ) / resultCells, 1.0);

    return PhysicalBoundaries(start,end, maxDensity);
}

PhysicalBoundaries PhysicalBoundaries::crossWith (PhysicalBoundaries const& other) const
{
    if (isEmpty() || other.isEmpty())
    {
        return createEmpty(_startCoords.size()+other._startCoords.size());
    }

    Coordinates start, end;
    for (size_t i=0; i<_startCoords.size(); i++)
    {
        start.push_back(_startCoords[i]);
        end.push_back(_endCoords[i]);
    }
    for (size_t i=0; i<other.getStartCoords().size(); i++)
    {
        start.push_back(other.getStartCoords()[i]);
        end.push_back(other.getEndCoords()[i]);
    }

    return PhysicalBoundaries(start,end, _density * other._density);
}

PhysicalBoundaries PhysicalBoundaries::reshape(Dimensions const& oldDims, Dimensions const& newDims) const
{
    if (isEmpty())
    {
        return createEmpty(newDims.size());
    }

    Coordinates start = reshapeCoordinates(_startCoords, oldDims, newDims);
    Coordinates end = reshapeCoordinates(_endCoords, oldDims, newDims);

    if (newDims.size() > oldDims.size())
    {
        bool dimensionFull = false;

        for (size_t i = 0; i < start.size(); i++)
        {
            if (dimensionFull)
            {
                if (newDims[i].getStartMin() <= CoordinateBounds::getMin() || newDims[i].getEndMax() >= CoordinateBounds::getMax())
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_CANT_CREATE_BOUNDARIES_FROM_INFINITE_ARRAY);

                start[i] = newDims[i].getStartMin();
                end[i] = newDims[i].getEndMax();
            }
            else if(end[i] > start[i])
            {
                dimensionFull = true;
            }
        }
    }

    double startingCells = getNumCells();
    double resultCells = getNumCells(start, end);

    return PhysicalBoundaries(start,end, _density * startingCells / resultCells);
}

std::shared_ptr<SharedBuffer> PhysicalBoundaries::serialize() const
{
    size_t totalSize = sizeof(size_t) + sizeof(double) + _startCoords.size()*sizeof(Coordinate) + _endCoords.size()*sizeof(Coordinate);
    MemoryBuffer* buf = new MemoryBuffer(NULL, totalSize);

    size_t* sizePtr = (size_t*)buf->getData();
    *sizePtr = _startCoords.size();
    sizePtr++;

    double* densityPtr = (double*) sizePtr;
    *densityPtr = _density;
    densityPtr++;

    Coordinate* coordPtr = (Coordinate*) densityPtr;

    for(size_t i = 0; i<_startCoords.size(); i++)
    {
        *coordPtr = _startCoords[i];
        coordPtr++;
    }

    for(size_t i = 0; i<_endCoords.size(); i++)
    {
        *coordPtr = _endCoords[i];
        coordPtr++;
    }

    assert((char*) coordPtr == (char*)buf->getData() + buf->getSize());
    return std::shared_ptr<SharedBuffer> (buf);
}

PhysicalBoundaries PhysicalBoundaries::deSerialize(std::shared_ptr<SharedBuffer> const& buf)
{
    size_t* numCoordsPtr = (size_t*) buf->getData();
    size_t numCoords = *numCoordsPtr;
    numCoordsPtr++;

    double* densityPtr = (double*) buf->getData();
    double density = *densityPtr;
    densityPtr++;

    Coordinates start, end;
    Coordinate* coordPtr = (Coordinate*) densityPtr;

    for(size_t i =0; i<numCoords; i++)
    {
        start.push_back(*coordPtr);
        coordPtr++;
    }

    for(size_t i =0; i<numCoords; i++)
    {
        end.push_back(*coordPtr);
        coordPtr++;
    }

    return PhysicalBoundaries(start,end,density);
}


uint32_t PhysicalBoundaries::getCellSizeBytes(const Attributes& attrs )
{
    uint32_t totalBitSize = 0;
    Config* cfg = Config::getInstance();

    for (size_t i = 0; i < attrs.size(); i++)
    {
        const AttributeDesc attr = attrs[i];
        Type cellType = TypeLibrary::getType(attr.getType());
        uint32_t bitSize = cellType.bitSize();

        if (bitSize == 0)
        {
            bitSize =  cfg->getOption<int>(CONFIG_STRING_SIZE_ESTIMATION) * 8;
        }
        if (attr.isNullable())
        {
            bitSize += 1;
        }
        totalBitSize += bitSize;
    }
    return (totalBitSize + 7)/8;
}

double PhysicalBoundaries::getSizeEstimateBytes(const ArrayDesc& schema) const
{
    uint64_t numCells = getNumCells();
    uint64_t numChunks = getNumChunks(schema.getDimensions());
    size_t numDimensions = schema.getDimensions().size();
    size_t numAttributes = schema.getAttributes().size();

    uint32_t cellSize = getCellSizeBytes(schema.getAttributes());

    //we assume that every cell is part of sparse chunk
    cellSize += numAttributes * (numDimensions  * sizeof(Coordinate) + sizeof(int));
    double size = numCells * 1.0 * cellSize ;

    //Assume all chunks are sparse and add header
    size += numChunks *
            numAttributes;

    return size * _density;
}


/*
 * RedistributeContext methods
 */
bool operator== (RedistributeContext const& lhs, RedistributeContext const& rhs)
{
    return lhs._partitioningSchema == rhs._partitioningSchema &&
           (lhs._partitioningSchema != psLocalInstance || lhs._instanceId == rhs._instanceId) &&
           ((!lhs.hasMapper() && !rhs.hasMapper()) || ( lhs.hasMapper() && rhs.hasMapper() && *lhs._distMapper.get() == *rhs._distMapper.get()));
}

std::ostream& operator<<(std::ostream& stream, const RedistributeContext& dist)
{
    switch (dist._partitioningSchema)
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
        case psGroupby:         stream<<"groupby";
                                break;
        case psScaLAPACK:       stream<<"ScaLAPACK";
                                break;
    default:
            assert(0);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "operator<<(std::ostream& stream, const RedistributeContext& dist)";
    }

    if (dist._partitioningSchema == psLocalInstance)
    {
        stream<<" instance "<<dist._instanceId;
    }
    if (dist._distMapper.get() != NULL)
    {
        stream<<" "<<*dist._distMapper;
    }
    return stream;
}

/**
 * Implementation of SCATTER/GATHER method.
 */
void sync(NetworkManager* networkManager, const std::shared_ptr<Query>& query, uint64_t instanceCount, bool isSendLocal=false)
{
    std::shared_ptr<MessageDesc> msg = std::make_shared<MessageDesc>(mtSyncRequest);
    std::shared_ptr<scidb_msg::DummyQuery> record = msg->getRecord<scidb_msg::DummyQuery>();
    msg->setQueryID(query->getQueryID());
    networkManager->broadcastLogical(msg);

    if (isSendLocal) {
         networkManager->sendLocal(query, msg);
        ++instanceCount;
    }

    LOG4CXX_DEBUG(logger, "Sending sync to every one and waiting for " << instanceCount - 1 << " sync confirmations")
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    query->syncSG.enter(instanceCount - 1, ec);
    LOG4CXX_DEBUG(logger, "All confirmations received - continuing")
}


void barrier(uint64_t barrierId, NetworkManager* networkManager, const std::shared_ptr<Query>& query, uint64_t instanceCount)
{
    std::shared_ptr<MessageDesc> barrierMsg = std::make_shared<MessageDesc>(mtBarrier);
    std::shared_ptr<scidb_msg::DummyQuery> barrierRecord = barrierMsg->getRecord<scidb_msg::DummyQuery>();
    barrierMsg->setQueryID(query->getQueryID());
    barrierRecord->set_payload_id(barrierId);
    networkManager->broadcastLogical(barrierMsg);

    LOG4CXX_DEBUG(logger, "Sending barrier to every one and waiting for " << instanceCount - 1 << " barrier messages")
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    query->semSG[barrierId].enter(instanceCount - 1, ec);
    LOG4CXX_DEBUG(logger, "All barrier messages received - continuing")
}

/**
 * This can be used as a barrier mechanism across the cluster in a blocking/materializing operator
 * Note: redistributeXXX() uses the same mechanism.
 */
void syncBarrier(uint64_t barrierId, const std::shared_ptr<Query>& query)
{
    LOG4CXX_DEBUG(logger, "syncBarrier: barrierId = " << barrierId);
    assert(query);
    NetworkManager* networkManager = NetworkManager::getInstance();
    assert(networkManager);
    const uint64_t instanceCount = query->getInstancesCount();
    assert(instanceCount>0);
    barrier(barrierId%MAX_BARRIERS, networkManager, query, instanceCount);
}

void syncSG(const std::shared_ptr<Query>& query)
{
    assert(query);
    LOG4CXX_DEBUG(logger, "syncSG: queryID="<<query->getQueryID());
    NetworkManager* networkManager = NetworkManager::getInstance();
    assert(networkManager);
    const uint64_t instanceCount = query->getInstancesCount();
    assert(instanceCount>0);
    sync(networkManager, query, instanceCount, true);
}

/**
 * Compute hash over the groupby dimensions.
 * @param   allDims   Coordinates containing all the dims.
 * @param   isGroupby   For every dimension, whether it is a groupby dimension.
 *
 * @note The result can be larger than #instances!!! The caller should mod it.
 *
 */
InstanceID hashForGroupby(const Coordinates& allDims, const vector<bool>& isGroupby ) {
    assert(allDims.size()==isGroupby.size());
    Coordinates groups;
    for (size_t i=0; i<allDims.size(); ++i) {
        if (isGroupby[i]) {
            groups.push_back(allDims[i]);
        }
    }
    return VectorHash<Coordinate>()(groups);
}

/**
 * Compute the instanceID for a group.
 * For psGroupby, if the logic is modified, also change PhysicalQuantile.cpp::GroupbyQuantileArrayIterator::getInstanceForChunk.
 */
InstanceID getInstanceForChunk(std::shared_ptr<Query> const& query,
                               Coordinates const& chunkPos,
                               ArrayDesc const& desc,
                               PartitioningSchema ps,
                               const std::shared_ptr<CoordinateTranslator>& distMapper,
                               uint64_t instanceIdShift,
                               InstanceID psLocal_DestInstanceId, // TODO: eliminate special case arg -- pass via psData
                               PartitioningSchemaData* psData)
{
    static const char *funcName = "getInstanceForChunk: ";

    if(ps==psReplication) {
        return ALL_INSTANCE_MASK;       // magic (negative) number, NEVER remapped, shifted, or %instanceCount
    }

    // NOTE: be careful distinguishing ArrayDesc::_ps and the argument ps.
    // The former is from the existing array that is going to have its data redistributed
    // and the latter is the new distribution for the data.
    // because of this, one may not call ArrayDesc::getPrimaryInstanceId(pos, icount)
    // but instead the lower-level getPrimaryInstanceForChunk(ps, pos, dims, icount)

    Dimensions const& dims = desc.getDimensions();
    const uint64_t instanceCount = query->getInstancesCount();

    // handle optional pos translation
    const Coordinates *chunkPosition = &chunkPos;

    Coordinates mappedChunkPos;
    if (distMapper)
    {
        mappedChunkPos = distMapper->translate(chunkPos);
        chunkPosition = &mappedChunkPos;
    }

    InstanceID destInstanceId = 0 ;
    switch (ps)
    {

    //
    // persistable cases: no psData, no psLocal_DestInstanceId
    //
    case psHashPartitioned:
    case psByRow:
    case psByCol:
    {
        // use the factored standard for non "psData" cases
        // note: (these calculations can't be modified across releases anyway, so they don't need to be here)
        destInstanceId =  getPrimaryInstanceForChunk(ps, *chunkPosition, dims, instanceCount);
        break;
    }

    //
    // the non persistable cases (at this time): ones that rely on parameters other than
    // (ps, pos, dims, originalInstanceCount)
    // + the additional info is not persisted at this time
    // + because its not persisted, their parameterization has not be carefully specd
    // + requires generalization where ArrayDesc::_ps is currently  persisted
    //
    case psScaLAPACK:
    {
        if ( dims.size() < 1 ||  // distribution is defined only for vectors
             dims.size() > 2) {  // and matrices, (not tensors, etc)
            throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_REDISTRIBUTE_ERROR);
        }
        PartitioningSchemaDataForScaLAPACK* repartInfo = dynamic_cast<PartitioningSchemaDataForScaLAPACK*>(psData);
        if (!repartInfo) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_REDISTRIBUTE_ERROR);
        }
        destInstanceId = repartInfo->getInstanceID((*chunkPosition), *(query.get()));
        break;
    }
    case psLocalInstance:
    {
        // a specified instanceId with a shift

        // TODO: eliminate psLocal_InstanceId, use psData like the others
        destInstanceId = psLocal_DestInstanceId ;
        ASSERT_EXCEPTION((destInstanceId < instanceCount), funcName);
        break;
    }
    case psGroupby:
    {
        PartitioningSchemaDataGroupby* pIsGroupbyDim = dynamic_cast<PartitioningSchemaDataGroupby*>(psData);
        if (pIsGroupbyDim!=NULL) {
            destInstanceId = hashForGroupby((*chunkPosition), pIsGroupbyDim->_arrIsGroupbyDim);
        } else {
            stringstream ss;
            ss << funcName << "psGroupby";
            ASSERT_EXCEPTION(false, ss.str());
        }
        break;
    }
    case psReplication:
        ASSERT_EXCEPTION(false, "getInstanceForChunk: internal error: psReplication should not reach this switch statement");
    case psUndefined:
        ASSERT_EXCEPTION(false, "getInstanceForChunk: caller error: psUndefined should never be used as an argument");
    default:
    {
        stringstream ss;
        ss << funcName << "internal error: unknown PartitioningSchema "<< ps; // print the number
        ASSERT_EXCEPTION(false, ss.str());
    }
    }
    return (destInstanceId + instanceIdShift) % instanceCount;
}

void sendToRemoteInstance(
        bool cachingLastEmptyBitmap,
        bool cachingReceivedChunks,
        bool isEmptyable,
        bool isEmptyIndicator,
        AttributeID attrId,
        std::shared_ptr<ConstRLEEmptyBitmap>& sharedEmptyBitmap,
        const ConstChunk& chunk,
        Coordinates const& coordinates,
        size_t& totalBytesSent,
        size_t& totalBytesSynced,
        std::shared_ptr<Query>& query,
        NetworkManager* networkManager,
        MessageType mt,
        InstanceID instanceID,
        size_t instanceCount,
        size_t networkBufferLimit
        )
{
    std::shared_ptr<CompressedBuffer> buffer = std::make_shared<CompressedBuffer>();
    std::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
    if (isEmptyable && !cachingLastEmptyBitmap && !cachingReceivedChunks && !isEmptyIndicator) {
        emptyBitmap = sharedEmptyBitmap;
        assert(emptyBitmap);
    }
    chunk.compress(*buffer, emptyBitmap);
    assert(buffer && buffer->getData());

    std::shared_ptr<MessageDesc> chunkMsg = std::make_shared<MessageDesc>(mt, buffer);
    std::shared_ptr<scidb_msg::Chunk> chunkRecord = chunkMsg->getRecord<scidb_msg::Chunk>();
    chunkRecord->set_eof(false);
    chunkRecord->set_compression_method(buffer->getCompressionMethod());
    chunkRecord->set_attribute_id(attrId);
    chunkRecord->set_decompressed_size(buffer->getDecompressedSize());
    chunkRecord->set_count(chunk.isCountKnown() ? chunk.count() : 0);
    chunkMsg->setQueryID(query->getQueryID());
    for (size_t i = 0; i < coordinates.size(); i++)
    {
        chunkRecord->add_coordinates(coordinates[i]);
    }

    networkManager->send(instanceID, chunkMsg);
    LOG4CXX_TRACE(logger, "Sending chunk with att=" << attrId << " to instance=" << instanceID);
    totalBytesSent += buffer->getDecompressedSize();
    if (totalBytesSent > totalBytesSynced + networkBufferLimit) {
        sync(networkManager, query, instanceCount);
        totalBytesSynced = totalBytesSent;
    }
}

void mergeToLocalInstance(
        bool isEmptyable,
        bool isEmptyIndicator,
        AttributeID attrId,
        std::shared_ptr<Query>& query,
        std::shared_ptr<Array> outputArray,
        vector<std::shared_ptr<ArrayIterator> >& outputIters,
        std::shared_ptr<ConstRLEEmptyBitmap>& sharedEmptyBitmap,
        const ConstChunk& chunk,
        Coordinates const& coordinates,
        vector <AggregatePtr> const& aggs,
        ArrayDesc const& desc
)
{
    ScopedMutexLock cs(query->resultCS);
    if (!outputIters[attrId]) {
        outputIters[attrId] = outputArray->getIterator(attrId);
    }
    if (outputIters[attrId]->setPosition(coordinates))
    {
        Chunk& dstChunk = outputIters[attrId]->updateChunk();
        if (aggs[attrId].get())
        {
            if (desc.getEmptyBitmapAttribute() == NULL)
            {
                dstChunk.nonEmptyableAggregateMerge(chunk, aggs[attrId], query);
            }
            else
            {
                dstChunk.aggregateMerge(chunk, aggs[attrId], query);
            }
        }
        else
        {
            dstChunk.merge(chunk, query);
        }
    }
    else
    {
        std::shared_ptr<ConstRLEEmptyBitmap> emptyBitmap;
        if (isEmptyable && !isEmptyIndicator) {
            emptyBitmap = sharedEmptyBitmap;
        }
        outputIters[attrId]->copyChunk(chunk, emptyBitmap);
    }
    LOG4CXX_TRACE(logger, "Storing chunk with att=" << attrId << " locally")
}

AggregatePtr resolveAggregate(std::shared_ptr <OperatorParamAggregateCall>const& aggregateCall,
                              Attributes const& inputAttributes,
                              AttributeID* inputAttributeID,
                              string* outputName)
{
    const std::shared_ptr<OperatorParam> &acParam = aggregateCall->getInputAttribute();

    try
    {
        if (PARAM_ASTERISK == acParam->getParamType())
        {
            AggregatePtr agg = AggregateLibrary::getInstance()->createAggregate( aggregateCall->getAggregateName(), TypeLibrary::getType(TID_VOID));

            if (inputAttributeID)
            {
                *inputAttributeID = INVALID_ATTRIBUTE_ID;
            }
            if (outputName)
            {
                *outputName = aggregateCall->getAlias().size() ? aggregateCall->getAlias() : agg->getName();
            }
            return agg;
        }
        else if (PARAM_ATTRIBUTE_REF == acParam->getParamType())
        {
            const std::shared_ptr<OperatorParamAttributeReference> &ref = (const std::shared_ptr<OperatorParamAttributeReference>&) acParam;

            AttributeDesc const& inputAttr = inputAttributes[ref->getObjectNo()];
            Type const& inputType = TypeLibrary::getType(inputAttr.getType());
            AggregatePtr agg = AggregateLibrary::getInstance()->createAggregate( aggregateCall->getAggregateName(), inputType);

            if (inputAttributeID)
            {
                *inputAttributeID = inputAttr.getId();
            }
            if (outputName)
            {
                *outputName = aggregateCall->getAlias().size() ? aggregateCall->getAlias() : inputAttr.getName() + "_" + agg->getName();
            }
            return agg;
        }
        else
        {
            // All other cases must have been thrown already during translation.
            assert(0);
        }
    }
    catch(const UserException &e)
    {
        if (SCIDB_LE_AGGREGATE_NOT_FOUND == e.getLongErrorCode())
        {
            throw CONV_TO_USER_QUERY_EXCEPTION(e, acParam->getParsingContext());
        }

        throw;
    }

    assert(false);
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "resolveAggregate";
    return AggregatePtr();
}

void addAggregatedAttribute (
        std::shared_ptr <OperatorParamAggregateCall>const& aggregateCall,
        ArrayDesc const& inputDesc,
        ArrayDesc& outputDesc,
        bool operatorDoesAggregationInOrder)
{
    string outputName;

    AggregatePtr agg = resolveAggregate(aggregateCall, inputDesc.getAttributes(), 0, &outputName);

    if ( !operatorDoesAggregationInOrder && agg->isOrderSensitive() ) {
        throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_AGGREGATION_ORDER_MISMATCH) << agg->getName();
    }

    outputDesc.addAttribute( AttributeDesc(outputDesc.getAttributes().size(),
                                           outputName,
                                           agg->getResultType().typeId(),
                                           AttributeDesc::IS_NULLABLE,
                                           0));
}

void PhysicalBoundaries::updateFromChunk(ConstChunk const* chunk, bool chunkShapeOnly)
{
    size_t nDims = _startCoords.size();
    if (chunk == NULL)
    {   return; }

    //chunk iteration is expensive - only perform if needed
    Coordinates const& chunkFirstPos = chunk->getFirstPosition(false), chunkLastPos = chunk->getLastPosition(false);
    bool updateLowBound = false, updateHiBound = false;
    for (size_t i = 0; i < nDims; i++)
    {
       if (chunkFirstPos[i] < _startCoords[i])
       {
           if ( chunkShapeOnly )
           {    _startCoords[i] = chunkFirstPos[i]; }
           else
           {   updateLowBound = true; }
       }

       if (chunkLastPos[i] > _endCoords[i])
       {
           if ( chunkShapeOnly)
           {    _endCoords[i] = chunkLastPos[i]; }
           else
           {    updateHiBound = true; }
       }
    }

    //The chunk is inside the box and cannot expand the bounds. Early exit.
    if (!updateLowBound && !updateHiBound)
    {   return; }

    //TODO: there is a further optimization opportunity here. The given chunk *should* always be a bitmap
    //chunk. Once we verify that, we can iterate over bitmap segments and compute coordinates. Committing
    //this due to timing constraints - should revisit and optimize this when possible.
    std::shared_ptr<ConstChunkIterator> citer = chunk->materialize()->getConstIterator();
    while (!citer->end() && (updateLowBound || updateHiBound))
    {
        Coordinates const& pos = citer->getPosition();
        bool updated = false;
        for (size_t j = 0; j < nDims; j++)
        {
            if (updateHiBound && pos[j] > _endCoords[j])
            {
                _endCoords[j] = pos[j];
                updated = true;
            }
            if (updateLowBound && pos[j] < _startCoords[j])
            {
                _startCoords[j] = pos[j];
                updated = true;
            }
        }
        if(updated) //it's likely that no further update can come from this chunk
        {
            if(updateHiBound)
            {
                size_t k=0;
                while (k<nDims && _endCoords[k]>=chunkLastPos[k])
                { k++; }
                if (k==nDims) //no more useful data for hi bound could come from this chunk!
                {   updateHiBound=false; }
            }
            if(updateLowBound)
            {
                size_t k=0;
                while (k<nDims && _startCoords[k]<=chunkFirstPos[k])
                { k++; }
                if (k==nDims) //no more useful data for lo bound could come from this chunk!
                {   updateLowBound=false; }
            }
        }
        ++(*citer);
    }
}

PhysicalBoundaries PhysicalBoundaries::trimToDims(Dimensions const& dims) const
{
    SCIDB_ASSERT(_startCoords.size() == dims.size());
    size_t nDims = dims.size();

    Coordinates resStart(nDims);
    Coordinates resEnd(nDims);

    for(size_t i=0; i<nDims; i++)
    {
        resStart[i] = std::max<Coordinate> (dims[i].getStartMin(), _startCoords[i]);
        resEnd[i] = std::min<Coordinate> (dims[i].getEndMax(), _endCoords[i]);
    }

    return PhysicalBoundaries(resStart, resEnd, _density);
}

PhysicalBoundaries PhysicalBoundaries::createFromChunkList(std::shared_ptr<Array>& inputArray,
                                                           const std::set<Coordinates, CoordinatesLess>& chunkCoordinates)
{
    ASSERT_EXCEPTION((inputArray->getSupportedAccess() == Array::RANDOM),
                     "PhysicalBoundaries::createFromChunkList: ");

    const ArrayDesc& arrayDesc = inputArray->getArrayDesc();
    const size_t nDims = arrayDesc.getDimensions().size();

    if (chunkCoordinates.empty()) {
        return PhysicalBoundaries::createEmpty(nDims);
    }

    typedef set<Coordinates, CoordinatesLess> ChunkCoordinates;
    typedef std::unordered_set<const Coordinates*> CoordHashSet;
    CoordHashSet chunksToExamine;

    if (nDims>1) {

        typedef std::vector<std::pair<int64_t, std::deque<const Coordinates*> > > ChunkListByExtremeDim;
        ChunkListByExtremeDim chunksWithMinDim(nDims,make_pair(CoordinateBounds::getMax(),std::deque<const Coordinates*>()));
        assert(nDims == chunksWithMinDim.size());
        ChunkListByExtremeDim chunksWithMaxDim(nDims,make_pair(CoordinateBounds::getMin(),std::deque<const Coordinates*>()));
        assert(nDims == chunksWithMaxDim.size());

        // find all boundary chunks
        for (ChunkCoordinates::const_iterator iter = chunkCoordinates.begin();
             iter != chunkCoordinates.end(); ++iter) {
            const Coordinates& chunkCoords = *iter;
            assert(chunkCoords.size() == nDims);
            for (size_t d=0; d < nDims; ++d) {
                pair<int64_t, std::deque<const Coordinates*> >& minChunks = chunksWithMinDim[d];
                if (chunkCoords[d] < minChunks.first) {
                    minChunks.second.clear();
                    minChunks.first = chunkCoords[d];
                    minChunks.second.push_back(&chunkCoords);
                }  else if (chunkCoords[d] == minChunks.first) {
                    minChunks.second.push_back(&chunkCoords);
                } else { }

                pair<int64_t, std::deque<const Coordinates*> >& maxChunks = chunksWithMaxDim[d];
                if (chunkCoords[d] > maxChunks.first) {
                    maxChunks.second.clear();
                    maxChunks.first = chunkCoords[d];
                    maxChunks.second.push_back(&chunkCoords);
                }  else if (chunkCoords[d] == maxChunks.first) {
                    maxChunks.second.push_back(&chunkCoords);
                } else { }
            }
        }

        // throw away duplicates
        for (size_t d=0; d < nDims; ++d) {
            std::deque<const Coordinates*>& minList = chunksWithMinDim[d].second;
            for (std::deque<const Coordinates*>::const_iterator iter = minList.begin();
                 iter != minList.end(); ++iter) {
                chunksToExamine.insert(*iter).second;
            }

            std::deque<const Coordinates*>& maxList = chunksWithMaxDim[d].second;
            for (std::deque<const Coordinates*>::const_iterator iter = maxList.begin();
                 iter != maxList.end(); ++iter) {
                chunksToExamine.insert(*iter).second;
            }
        }
    } else {
        assert(nDims==1);
        const Coordinates& firstCoord = *chunkCoordinates.begin();
        chunksToExamine.insert(&firstCoord);
        const Coordinates& lastCoord = *(--chunkCoordinates.end());
        chunksToExamine.insert(&lastCoord);
    }

    // update bounds using the boundary chunks
    PhysicalBoundaries bounds = PhysicalBoundaries::createEmpty(nDims);

    const AttributeID attr = arrayDesc.getAttributes().size() - 1;
    const bool isNoEmptyTag = (arrayDesc.getEmptyBitmapAttribute() == NULL);
    std::shared_ptr<ConstArrayIterator> arrayIter = inputArray->getConstIterator(attr);
    for (CoordHashSet::const_iterator iter = chunksToExamine.begin();
         iter != chunksToExamine.end(); ++iter) {
        const Coordinates* chunkCoords = *iter;
        bool rc = arrayIter->setPosition(*chunkCoords);
        SCIDB_ASSERT(rc);
        const ConstChunk& chunk = arrayIter->getChunk();
        bounds.updateFromChunk(&chunk, isNoEmptyTag);
    }
    return bounds;
}

void BaseLogicalOperatorFactory::registerFactory()
{
    OperatorLibrary::getInstance()->addLogicalOperatorFactory(this);
}

void BasePhysicalOperatorFactory::registerFactory()
{
    OperatorLibrary::getInstance()->addPhysicalOperatorFactory(this);
}

VersionID OperatorParamArrayReference::getVersion() const
{
    return _version;
}

void LogicalOperator::inferArrayAccess(std::shared_ptr<Query>& query)
{
    for (size_t i=0, end=_parameters.size(); i<end; ++i) {
        const std::shared_ptr<OperatorParam>& param = _parameters[i];
        string arrayName;
        if (param->getParamType() == PARAM_ARRAY_REF) {
            arrayName = ((std::shared_ptr<OperatorParamReference>&)param)->getObjectName();
        } else if (param->getParamType() == PARAM_SCHEMA) {
            arrayName = ((std::shared_ptr<OperatorParamSchema>&)param)->getSchema().getName();
        }
        if (arrayName.empty()) {
            continue;
        }
        const string baseName = ArrayDesc::makeUnversionedName(arrayName);
        std::shared_ptr<SystemCatalog::LockDesc> lock(make_shared<SystemCatalog::LockDesc>(baseName,
        query->getQueryID(),
        Cluster::getInstance()->getLocalInstanceId(),
        SystemCatalog::LockDesc::COORD,
        SystemCatalog::LockDesc::RD));
        query->requestLock(lock);
    }
}

InjectedErrorListener<OperatorInjectedError> PhysicalOperator::_injectedErrorListener;

std::shared_ptr<ThreadPool> PhysicalOperator::_globalThreadPoolForOperators;
std::shared_ptr<JobQueue> PhysicalOperator::_globalQueueForOperators;
Mutex PhysicalOperator::_mutexGlobalQueueForOperators;

std::shared_ptr<JobQueue> PhysicalOperator::getGlobalQueueForOperators()
{
    ScopedMutexLock cs(_mutexGlobalQueueForOperators);
    if (!_globalThreadPoolForOperators) {
        _globalQueueForOperators = std::shared_ptr<JobQueue>(new JobQueue());
        _globalThreadPoolForOperators = std::shared_ptr<ThreadPool>(
                new ThreadPool(Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_THREADS), _globalQueueForOperators));
        _globalThreadPoolForOperators->start();
    }
    return _globalQueueForOperators;
}

void PhysicalOperator::repartByLeftmost(
    vector<ArrayDesc> const& inputSchemas,
    vector<ArrayDesc const*>& modifiedPtrs) const
{
    const size_t N_SCHEMAS = inputSchemas.size();
    assert(N_SCHEMAS > 1); // ... else you are calling the wrong canned implementation.
    assert(N_SCHEMAS == modifiedPtrs.size());

    _redimRepartSchemas.clear();
    ArrayDesc const&    leftSchema  = inputSchemas[0];
    const size_t        nDimensions = leftSchema.getDimensions().size();
    modifiedPtrs[0] = 0;       // Do not repartition leftmost input array.

    for (size_t nSchema = 1; nSchema < N_SCHEMAS; ++nSchema)
    {
        ArrayDesc const& rightSchema = inputSchemas[nSchema];
        assert(rightSchema.getDimensions().size() == nDimensions);

        if (leftSchema.samePartitioning(rightSchema)) {
            // Already has right chunkSize and overlap, do nothing.
            modifiedPtrs[nSchema] = 0;
        } else {
            // If explicit repart is present, we're forbidden to change it---yet it *must* match!!!
            if (modifiedPtrs[nSchema]) {
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_BAD_EXPLICIT_REPART)
                    << getLogicalName()
                    << leftSchema.getDimensions()
                    << rightSchema.getDimensions();
            }

            // Clone this schema and adjust dimensions according to leftmost.
            Dimensions const& leftDimensions = leftSchema.getDimensions();
            Dimensions mergedDimensions(rightSchema.getDimensions());
            for (size_t nDimension = 0; nDimension < nDimensions; ++nDimension)
            {
                DimensionDesc const & leftDimension   = leftDimensions[nDimension];
                DimensionDesc       & mergedDimension = mergedDimensions[nDimension];

                mergedDimension.setChunkInterval(leftDimension.getChunkInterval());

                // Take smallest overlap since we can't (easily) conjure up cells that aren't there.
                mergedDimension.setChunkOverlap(min(
                    leftDimension.getChunkOverlap(),
                    mergedDimension.getChunkOverlap()));
            }

            _redimRepartSchemas.push_back(make_shared<ArrayDesc>(
                rightSchema.getName(),
                rightSchema.getAttributes(),
                mergedDimensions,
                defaultPartitioning()));
            modifiedPtrs[nSchema] = _redimRepartSchemas.back().get();
        }
    }
    if (_redimRepartSchemas.empty()) {
        // Assertions elsewhere hate an all-NULLs vector here.
        modifiedPtrs.clear();
    }
}

void
PhysicalOperator::assertLastVersion(std::string const& arrayName,
                                    bool arrayExists,
                                    ArrayDesc const& desc)
{
    if (isDebug()) {
        ArrayDesc tmpDesc;
        assert(arrayExists == SystemCatalog::getInstance()->getArrayDesc(arrayName,
                                                                         SystemCatalog::ANY_VERSION,
                                                                         tmpDesc, false));
        if (arrayExists) {
            assert(desc == tmpDesc);
        }
    }
}

void StoreJob::run()
{
    ArrayDesc const& dstArrayDesc = _dstArray->getArrayDesc();
    size_t nAttrs = dstArrayDesc.getAttributes().size();
    Query::setCurrentQueryID(_query->getQueryID());

    for (size_t i = _shift; i != 0 && !_srcArrayIterators[0]->end(); --i)
    {
        for (size_t j = 0; j < nAttrs; j++)
        {
            ++(*_srcArrayIterators[j]);
        }
    }

    while (!_srcArrayIterators[0]->end())
    {
        bool chunkHasElems(true);
        for (size_t i = 0; i < nAttrs; i++)
        {
            ConstChunk const& srcChunk = _srcArrayIterators[i]->getChunk();
            Coordinates srcPos = _srcArrayIterators[i]->getPosition();
            if (!dstArrayDesc.contains(srcPos)) {
                throw USER_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES)
                    << CoordsToStr(srcPos) << dstArrayDesc.getDimensions();
            }
            if (i==0) {
                chunkHasElems = hasValues(srcChunk);
                if (chunkHasElems) {
                    createdChunks.insert(_srcArrayIterators[0]->getPosition());
                }
            } else {
                assert(chunkHasElems == hasValues(srcChunk));
            }
            if (chunkHasElems) {
                if (i == nAttrs - 1)
                {
                    bounds.updateFromChunk(&srcChunk, dstArrayDesc.getEmptyBitmapAttribute() == NULL);
                }
                _dstArrayIterators[i]->copyChunk(srcChunk);
            }
            Query::validateQueryPtr(_query);
            for (size_t j = _step; j != 0 && !_srcArrayIterators[i]->end(); ++(*_srcArrayIterators[i]), --j)
                ;
        }
    }
}

bool StoreJob::hasValues(ConstChunk const& srcChunk)
{
    ArrayDesc const& srcArrayDesc = _srcArray->getArrayDesc();
    bool isSrcEmptyable = (srcArrayDesc.getEmptyBitmapAttribute() != NULL);

    // XXX TODO: until we have the single RLE? data format,
    //           we need to filter out other depricated formats (e.g. dense/sparse/nonempyable)
    bool chunkHasVals = (!isSrcEmptyable) || (!srcChunk.isEmpty());
    return chunkHasVals;
}

PhysicalUpdate::PhysicalUpdate(const string& logicalName,
                               const string& physicalName,
                               const Parameters& parameters,
                               const ArrayDesc& schema,
                               const std::string& catalogArrayName):
PhysicalOperator(logicalName, physicalName, parameters, schema),
_unversionedArrayName(catalogArrayName),
_arrayUAID(0),
_arrayID(0),
_lastVersion(0)
{}

void PhysicalUpdate::preSingleExecute(std::shared_ptr<Query> query)
{
    std::shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
    SCIDB_ASSERT(membership);
    if ((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
        (membership->getInstances().size() != query->getInstancesCount())) {
        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
    }

    // Figure out the array descriptor for the new array
    // Several options:
    // 1. _schema represents the unversioned array already in the catalog (obtained by the LogicalOperator)
    // 2. _schema represents the input array and the output array does not yet exists in the catalog
    // 3. _schema represents the input array and the output array may/not exist in the catalog (in case of PhysicalInput)
    // Transient arrays do not have versions; for the persistent arrays we need to update _schema to represent a new version array
    bool rc = false;
    PartitioningSchema ps = _schema.getPartitioningSchema();

    SystemCatalog* catalog = SystemCatalog::getInstance();
    SCIDB_ASSERT(catalog);
    SCIDB_ASSERT(!_unversionedArrayName.empty());

    // throw away the alias name, and restore the catalog name
    _schema.setName(_unversionedArrayName);

    LOG4CXX_DEBUG(logger, "PhysicalUpdate::preSingleExecute: schema: "<< _schema);

    bool isArrayInCatalog = (_schema.getId() > 0);
    if (!isArrayInCatalog) {
        isArrayInCatalog = catalog->getArrayDesc(_schema.getName(),
                                                 query->getCatalogVersion(_schema.getName()),
                                                 _unversionedSchema,
                                                 false);
        assertLastVersion(_schema.getName(), isArrayInCatalog, _unversionedSchema);
    } else {
        SCIDB_ASSERT(_schema.getId() != INVALID_ARRAY_ID);
        SCIDB_ASSERT(_schema.getId() == _schema.getUAId());
        // LogicalOperator already obtained the unversioned schema
        _unversionedSchema = _schema;
    }

    // set up error handling
    const SystemCatalog::LockDesc::LockMode lockMode =
        _unversionedSchema.isTransient() ? SystemCatalog::LockDesc::XCL : SystemCatalog::LockDesc::WR;
    _lock = make_shared<SystemCatalog::LockDesc>(_schema.getName(),
                                                 query->getQueryID(),
                                                 Cluster::getInstance()->getLocalInstanceId(),
                                                 SystemCatalog::LockDesc::COORD,
                                                 lockMode);

    {  //XXX HACK to make sure we got the right (more restrictive) lock
        _lock->setLockMode(SystemCatalog::LockDesc::RD);
        std::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(_lock);
        SCIDB_ASSERT(resLock);
        _lock->setLockMode(resLock->getLockMode());
        if (_lock->getLockMode() !=  lockMode) {
            throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_ARRAYS_NOT_CONFORMANT) <<
            string("Transient array with name: ") + _lock->getArrayName() +
            string(" cannot be removed/inserted concurrently with another store/insert");
        }
    }

    if (lockMode == SystemCatalog::LockDesc::WR) {
        std::shared_ptr<Query::ErrorHandler> ptr(make_shared<UpdateErrorHandler>(_lock));
        query->pushErrorHandler(ptr);
    }

    if (!isArrayInCatalog) {
        // so, we need to add the unversioned array as well
        // NOTE: that transient arrays are only added by the user (via create array ...)
        SCIDB_ASSERT(_schema.getId() == 0);

        _lock->setLockMode(SystemCatalog::LockDesc::CRT);
        rc = catalog->updateArrayLock(_lock);
        SCIDB_ASSERT(rc);
        SCIDB_ASSERT(ps == defaultPartitioning());
        _schema.setPartitioningSchema(ps);
        _unversionedSchema = _schema;
        ArrayID uAId = catalog->getNextArrayId();
        _unversionedSchema.setIds(uAId, uAId, VersionID(0));

    } else if (_unversionedSchema.isTransient()) {

        _schema = _unversionedSchema;

        _lock->setArrayId       (_arrayUAID   = _unversionedSchema.getUAId());
        _lock->setArrayVersion  (_lastVersion = 0);
        _lock->setArrayVersionId(_arrayID     = _unversionedSchema.getId());
        rc = catalog->updateArrayLock(_lock);
        SCIDB_ASSERT(rc);
        return;

    } else {
        // in the catalog but not transient, so get its latest version
        _lastVersion = catalog->getLastVersion(_unversionedSchema.getId());
    }
    SCIDB_ASSERT(_unversionedSchema.getUAId() == _unversionedSchema.getId());

    // all aspects of conformity should be checked at the physical stage
    ArrayDesc::checkConformity(_schema, _unversionedSchema);

    _arrayUAID = _unversionedSchema.getId();

    _schema = ArrayDesc(ArrayDesc::makeVersionedName(_schema.getName(), _lastVersion+1),
                        _unversionedSchema.getAttributes(),
                        _unversionedSchema.getDimensions(),
                        _unversionedSchema.getPartitioningSchema());

    BOOST_FOREACH (DimensionDesc& d, _schema.getDimensions())
    {
        d.setCurrStart(CoordinateBounds::getMax());
        d.setCurrEnd(CoordinateBounds::getMin());
    }
    SCIDB_ASSERT(_schema.getPartitioningSchema() == defaultPartitioning());

    _arrayID = catalog->getNextArrayId();

    _schema.setIds(_arrayID, _arrayUAID,(_lastVersion+1));

    _lock->setArrayId(_arrayUAID);
    _lock->setArrayVersion(_lastVersion+1);
    _lock->setArrayVersionId(_arrayID);
    rc = catalog->updateArrayLock(_lock);
    SCIDB_ASSERT(rc);
}

void PhysicalUpdate::postSingleExecute(std::shared_ptr<Query> query)
{
    if (_arrayID != 0 && !_unversionedSchema.isTransient()) {
        SCIDB_ASSERT(_lock);
        SCIDB_ASSERT(_arrayID > 0);
        SCIDB_ASSERT(_arrayUAID < _arrayID);
        // NOTE: the physical operator must outlive the finalizer
        // (and it does because the physical plan is never destroyed)
        query->pushFinalizer(boost::bind(&PhysicalUpdate::recordPersistent,this,_1));
    }
}

/**
 * Implements a callback that is suitable for use as a query finalizer.
 * Adds the new array/version to the catalog transactionally.
 */
void PhysicalUpdate::recordPersistent(const std::shared_ptr<Query>& query)
{
    if (!query->wasCommitted()) {
        return;
    }
    SCIDB_ASSERT(!_schema.isTransient());
    SCIDB_ASSERT(_arrayUAID == _unversionedSchema.getId());
    SCIDB_ASSERT(_arrayUAID == _schema.getUAId());
    SCIDB_ASSERT(_arrayID   == _schema.getId());
    SCIDB_ASSERT(query->getSession());

    // if the catalog update fails (for a reason other than disconnect)
    // the finalizer code exercising this routine will abort() the process
    if (_lock->getLockMode()==SystemCatalog::LockDesc::CRT) {
        SystemCatalog::getInstance()->addArrayVersion(
            query->getSession()->getNamespace(),
            &_unversionedSchema, _schema);
    } else {
        SCIDB_ASSERT(_lock->getLockMode()==SystemCatalog::LockDesc::WR);
        SystemCatalog::getInstance()->addArrayVersion(
            query->getSession()->getNamespace(),
            NULL, _schema);
    }
}

void PhysicalUpdate::recordTransient(const std::shared_ptr<Array>& t,
                                     const std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(_schema.isTransient());
    SCIDB_ASSERT(_schema.getId() == _schema.getUAId());
    if (query->wasCommitted()) {

        PhysicalBoundaries bounds(_schema.getLowBoundary(),
                                  _schema.getHighBoundary());
        transient::record(t);
        // 2/25/2015 NOTE: this is the last usage of updateArrayBoundaries()
        // if the catalog update fails (for a reason other than disconnect)
        // the finalizer code exercising this routine will abort() the process
        SystemCatalog::getInstance()->updateArrayBoundaries(_schema, bounds);
    }
}

void
PhysicalUpdate::updateSchemaBoundaries(ArrayDesc& schema,
                                       PhysicalBoundaries& bounds,
                                       std::shared_ptr<Query>& query)
{
    SCIDB_ASSERT(schema.getId()>0);
    Dimensions const& dims = schema.getDimensions();
    SCIDB_ASSERT(!dims.empty());

    // These conditions should have been caught by now.
    SCIDB_ASSERT(bounds.isEmpty() ||
                 (schema.contains(bounds.getStartCoords()) &&
                  schema.contains(bounds.getEndCoords())));

    if (query->isCoordinator()) {

        SCIDB_ASSERT(schema.getUAId() <= schema.getId());

        LOG4CXX_DEBUG(logger, "PhysicalUpdate::updateSchemaBoundaries: schema on coordinator: "<< schema);

        Coordinates start(dims.size());
        Coordinates end(dims.size());

        for (InstanceID fromInstance=0; fromInstance < query->getInstancesCount(); ++fromInstance) {

            if (fromInstance == query->getInstanceID()) {
                continue;
            }
            std::shared_ptr<MessageDesc> resultMessage;
            NetworkManager::getInstance()->receive(fromInstance, resultMessage, query);
            assert(resultMessage);
            ASSERT_EXCEPTION(resultMessage->getMessageType() == mtUpdateQueryResult, "UNKNOWN MESSAGE");

            std::shared_ptr<scidb_msg::QueryResult> queryResultRecord = resultMessage->getRecord<scidb_msg::QueryResult>();
            ASSERT_EXCEPTION(queryResultRecord->array_name() == schema.getName(),
                             string("Message for unknown array ")+queryResultRecord->array_name());

            namespace gp = google::protobuf;
            const gp::RepeatedPtrField<scidb_msg::QueryResult_DimensionDesc>& entries = queryResultRecord->dimensions();
            ASSERT_EXCEPTION(static_cast<size_t>(entries.size()) == dims.size(), "INVALID MESSAGE");

            size_t i=0;
            for(gp::RepeatedPtrField<scidb_msg::QueryResult_DimensionDesc>::const_iterator iter = entries.begin();
                iter != entries.end();
                ++iter, ++i) {

                const scidb_msg::QueryResult_DimensionDesc& dimension = (*iter);
                assert(dimension.has_curr_start());
                start[i] = dimension.curr_start();
                assert(dimension.has_curr_end());
                end[i] = dimension.curr_end();
                if (isDebug()) {
                    assert(dimension.has_name());
                    assert(dimension.name() == dims[i].getBaseName());
                    assert(dimension.has_start_min());
                    assert(dimension.start_min() == dims[i].getStartMin());
                    assert(dimension.has_end_max());
                    assert(dimension.end_max() == dims[i].getEndMax());
                    assert(dimension.has_chunk_interval());
                    assert(dimension.chunk_interval() == dims[i].getChunkInterval());
                    assert(dimension.has_chunk_overlap());
                    assert(dimension.chunk_overlap() == dims[i].getChunkOverlap());
                }
            }
            PhysicalBoundaries currBounds(start, end);
            bounds = bounds.unionWith(currBounds);
        }

        SCIDB_ASSERT(start.size() == end.size());
        SCIDB_ASSERT(start.size() == dims.size());
        Dimensions newDims(dims);
        for (size_t i=0, n=newDims.size(); i<n; ++i) {
            newDims[i].setCurrStart(bounds.getStartCoords()[i]);
            newDims[i].setCurrEnd(bounds.getEndCoords()[i]);
        }
        schema.setDimensions(newDims);
        LOG4CXX_DEBUG(logger, "Dimension boundaries updated: "<< schema);

    } else {

        LOG4CXX_DEBUG(logger, "PhysicalUpdate::updateSchemaBoundaries: schema on worker: "<< schema);

        // Creating message with result for sending to coordinator
        std::shared_ptr<MessageDesc> resultMessage = make_shared<MessageDesc>(mtUpdateQueryResult);
        std::shared_ptr<scidb_msg::QueryResult> queryResultRecord = resultMessage->getRecord<scidb_msg::QueryResult>();
        resultMessage->setQueryID(query->getQueryID());

        queryResultRecord->set_array_name(schema.getName());

        const Coordinates & start = bounds.getStartCoords();
        const Coordinates & end = bounds.getEndCoords();
        assert(start.size() == end.size());
        assert(start.size() == dims.size());

        for (size_t i = 0; i < start.size(); i++)
        {
            scidb_msg::QueryResult_DimensionDesc* dimension = queryResultRecord->add_dimensions();

            dimension->set_name(dims[i].getBaseName());
            dimension->set_start_min(dims[i].getStartMin());
            dimension->set_end_max(dims[i].getEndMax());
            dimension->set_chunk_interval(dims[i].getChunkInterval());
            dimension->set_chunk_overlap(dims[i].getChunkOverlap());

            dimension->set_curr_start(start[i]);
            dimension->set_curr_end(end[i]);
        }
        const InstanceID coordLogicalId = query->getCoordinatorID();
        NetworkManager::getInstance()->send(coordLogicalId, resultMessage);
    }
}

} //namespace
