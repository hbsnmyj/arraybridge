/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2015 SciDB, Inc.
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
 * @author poliocough@gmail.com
 */

#include <limits>
#include <limits>
#include <sstream>

#include <unordered_map>
#include <query/Operator.h>
#include <util/Platform.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <system/Sysinfo.h>
#include "ParseSettings.h"

#include <boost/algorithm/string.hpp>

namespace scidb
{
using namespace boost;
using namespace std;

class OutputWriter : public boost::noncopyable
{
private:
    std::shared_ptr<Array> const _output;
    Coordinates _outputPosition;
    size_t const _numLiveAttributes;
    char _lineDelimiter;
    char _attributeDelimiter;
    size_t const _outputLineSize;
    vector<Value> _outputLine;
    size_t const _outputChunkSize;
    vector<std::shared_ptr<ArrayIterator> > _outputArrayIterators;
    vector<std::shared_ptr<ChunkIterator> > _outputChunkIterators;
    bool _splitOnDimension;

public:
    OutputWriter(ArrayDesc const& schema, std::shared_ptr<Query>& query, char lineDelimiter, char attributeDelimiter, bool splitOnDimension):
        _output(std::make_shared<MemArray>(schema,query)),
        _outputPosition( splitOnDimension ? 4 : 3, 0),
        _numLiveAttributes(schema.getAttributes(true).size()),
        _lineDelimiter(lineDelimiter),
        _attributeDelimiter(attributeDelimiter),
        _outputLineSize(splitOnDimension ? schema.getDimensions()[3].getChunkInterval() : _numLiveAttributes),
        _outputLine(_outputLineSize),
        _outputChunkSize(schema.getDimensions()[2].getChunkInterval()),
        _outputArrayIterators(_numLiveAttributes),
        _outputChunkIterators(_numLiveAttributes),
        _splitOnDimension(splitOnDimension)
    {
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            _outputArrayIterators[i] = _output->getIterator(i);
        }
    }

    void processChunk(Coordinates const& inputChunkPosition, Value const& value, std::shared_ptr<Query>& query)
    {
        _outputPosition[0] = inputChunkPosition[0];
        _outputPosition[1] = inputChunkPosition[1];
        _outputPosition[2] = 0;
        if(_splitOnDimension)
        {
            _outputPosition[3] = 0;
        }
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            _outputChunkIterators[i] = _outputArrayIterators[i]->newChunk(_outputPosition).getIterator(query,
                i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        }
        const char *data = value.getString();
        if(data[ value.size() - 1] != 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Encountered a string that is not null-terminated; bailing";
        }
        string input(data);
        vector <string> lines;
        split(lines, input, is_from_range(_lineDelimiter, _lineDelimiter));
        size_t const nLines = lines.size();
        if (nLines > _outputChunkSize)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Encountered a string with more lines than the chunk size; bailing";
        }
        for ( size_t l = 0; l<nLines; ++l)
        {
            string const& line = lines[l];
            parseLine(line);
            if(_splitOnDimension)
            {
                _outputPosition[3] = 0;
            }
            for(AttributeID i =0; i<_outputLineSize; ++i)
            {
                _outputChunkIterators[ _splitOnDimension ? AttributeID(0) : i]->setPosition(_outputPosition);
                _outputChunkIterators[ _splitOnDimension ? AttributeID(0) : i]->writeItem(_outputLine[i]);
               if(_splitOnDimension)
               {
                   ++(_outputPosition[3]);
               }
            }
            ++(_outputPosition[2]);
        }
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            _outputChunkIterators[i]->flush();
            _outputChunkIterators[i].reset();
        }
    }

    /**
     * Flush the last chunk and return the resulting array object. After this, the class is invalidated.
     * @return the output array
     */
    std::shared_ptr<Array> finalize()
    {
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            _outputChunkIterators[i].reset();
            _outputArrayIterators[i].reset();
        }
        return _output;
    }


private:
    void parseLine(string const& line)
    {
        vector<string> tokens;
        split(tokens, line, is_from_range(_attributeDelimiter, _attributeDelimiter));
        size_t const nTokens = tokens.size();
        //set the error string
        if(nTokens < _outputLineSize -1 )
        {
            _outputLine[_outputLineSize-1].setString("short");
        }
        else if (nTokens >_outputLineSize -1 )
        {
            ostringstream error;
            error<<"long";
            for(size_t t = _outputLineSize - 1; t<nTokens; ++t)
            {
                error << _attributeDelimiter << tokens[t];
            }
            _outputLine[_outputLineSize-1].setString(error.str().c_str());
        }
        else
        {
            _outputLine[_outputLineSize-1].setNull();
        }
        for(size_t t= 0; t<_outputLineSize -1; ++t)
        {
            if(t<nTokens)
            {
                _outputLine[t].setString(tokens[t].c_str());
            }
            else
            {
                _outputLine[t].setNull();
            }
        }
    }
};

class PhysicalParse : public PhysicalOperator
{
public:
    PhysicalParse(std::string const& logicalName,
                     std::string const& physicalName,
                     Parameters const& parameters,
                     ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const&,
                                                      std::vector<ArrayDesc> const&) const
    {
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        ParseSettings settings (_parameters, false, query);
        OutputWriter writer(_schema, query, settings.getLineDelimiter(), settings.getAttributeDelimiter(), settings.getSplitOnDimension());
        std::shared_ptr<Array>& input = inputArrays[0];
        std::shared_ptr<ConstArrayIterator> inputIterator = input->getConstIterator(0);
        while(!inputIterator-> end())
        {
            Coordinates const& pos = inputIterator->getPosition();
            std::shared_ptr<ConstChunkIterator> inputChunkIterator = inputIterator->getChunk().getConstIterator();
            if(!inputChunkIterator->end()) //just 1 value in chunk
            {
                Value const& v = inputChunkIterator->getItem();
                writer.processChunk(pos, v, query);
            }
            ++(*inputIterator);
        }
        return writer.finalize();
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalParse, "parse", "PhysicalParse");


} // end namespace scidb
