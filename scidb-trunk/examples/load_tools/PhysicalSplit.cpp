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
#include <memory>

#include <query/Operator.h>
#include <util/Platform.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <system/Sysinfo.h>
#include "SplitSettings.h"

namespace scidb
{
using namespace boost;
using namespace std;


/**
 * A wrapper around an open file (or pipe) that may iterate over the data once and split it into blocks, each
 * block containing a number of lines. Returns one block at a time.
 */
class FileSplitter
{
private:
    size_t const _linesPerBlock;
    size_t       _bufferSize;
    vector<char> _buffer;
    char*        _dataStartPos;
    size_t       _dataSize;
    bool         _endOfFile;
    FILE*        _inputFile;
    char         _delimiter;

public:
    FileSplitter(string const& filePath,
                 size_t numLinesPerBlock,
                 size_t bufferSize,
                 char delimiter,
                 int64_t header):
        _linesPerBlock(numLinesPerBlock),
        _bufferSize(bufferSize),
        _buffer(0),
        _dataStartPos(0),
        _dataSize(0),
        _endOfFile(false),
        _inputFile(0),
        _delimiter(delimiter)
    {
        try
        {
            _buffer.resize(_bufferSize);
        }
        catch(...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitter() cannot allocate memory";
        }
        _inputFile = fopen(filePath.c_str(), "r");
        if(header>0)
        {
            char *line = NULL;
            size_t linesize = 0;
            ssize_t nread = 0;
            for(int64_t j=0; j<header && nread>=0; ++j)
            {
                nread = getdelim(&line, &linesize, (int)_delimiter, _inputFile);
            }
            free(line);
        }
        if (_inputFile == NULL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitter() cannot open file";
        }
        _dataSize = scidb::fread_unlocked(&_buffer[0], 1, _bufferSize, _inputFile);
        if (_dataSize != _bufferSize)
        {
            _endOfFile= true;
            scidb::fclose(_inputFile);
            _inputFile =0;
        }
        _dataStartPos = &_buffer[0];
    }

    ~FileSplitter()
    {
        if(_inputFile!=0)
        {
            scidb::fclose(_inputFile);
        }
    }

    /**
     * Get a pointer to the next block of data which shall contain no more than numLinesPerBlock delimiter characters,
     * may contain less if we are at the end of the file. Also advances the position and reads more data from the file
     * if needed.
     * @param[out] numCharacters the size of returned data block, 0 if there is no more data
     * @return pointer to the data, not valid if numCharacters is 0
     */
    char* getBlock(size_t& numCharacters)
    {
        size_t lineCounter = _linesPerBlock;
        char* ch = _dataStartPos;
        numCharacters = 0;
        while (1)
        {
            while (numCharacters < _dataSize && lineCounter != 0)
            {
                if(*ch == _delimiter)
                {
                    lineCounter --;
                }
                ++ch;
                ++numCharacters;
            }
            if(lineCounter == 0 || _endOfFile)
            {
                break;
            }
            else
            {
                ch = eatMoreData(); //this call changes _dataStartPos and _dataSize
            }
        }
        char* res = _dataStartPos;
        _dataStartPos = ch;
        _dataSize = _dataSize - numCharacters;
        return res;
    }

private:
    char* eatMoreData()
    {
        char *bufStart = &_buffer[0];
        if (_dataStartPos != bufStart)   //we have a block of data at the end of the buffer, move it to the beginning, then read more
        {
            memmove(bufStart, _dataStartPos, _dataSize);
        }
        else
        {
            if(_dataSize != _bufferSize) //invariant check: entire buffer must be full; double the size of the buffer, then read more
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitter()::eatMoreData internal error";
            }
            _bufferSize = _bufferSize * 2;
            try
            {
                _buffer.resize(_bufferSize);
            }
            catch(...)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitter()::eatMoreData cannot allocate memory";
            }
            bufStart = &_buffer[0];
        }
        char *newDataStart = bufStart + _dataSize;
        size_t remainderSize = _bufferSize - _dataSize;
        size_t bytesRead = scidb::fread_unlocked( newDataStart, 1, remainderSize, _inputFile);
        if(bytesRead != remainderSize)
        {
            _endOfFile = true;
            scidb::fclose(_inputFile);
            _inputFile =0;
        }
        _dataStartPos = bufStart;
        _dataSize = _dataSize + bytesRead;
        return newDataStart;
    }
};

/**
 * Placeholder for instances that don't have a file to load. Returning an empty MemArray no longer works.
 */
class EmptySinglePass : public SinglePassArray
{
private:
    typedef SinglePassArray super;
    MemChunk _dummy;

public:
    EmptySinglePass(ArrayDesc const& schema):
            super(schema)
    {
        super::setEnforceHorizontalIteration(true);
    }

    virtual ~EmptySinglePass()
    {}

    size_t getCurrentRowIndex() const
    {
        return 0;
    }

    bool moveNext(size_t rowIndex)
    {
        return false;
    }

    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
    {
        return _dummy;
    }
};

class FileSplitArray : public SinglePassArray
{
private:
    typedef SinglePassArray super;
    size_t _rowIndex;
    Address _chunkAddress;
    MemChunk _chunk;
    std::weak_ptr<Query> _query;
    FileSplitter _splitter;
    char*  _buffer;
    size_t _bufferSize;
    char   _delimiter;

public:
    FileSplitArray(ArrayDesc const& schema, std::shared_ptr<Query>& query, std::shared_ptr<SplitSettings> const& settings):
        super(schema),
        _rowIndex(0),
        _chunkAddress(0, Coordinates(2,0)),
        _query(query),
        _splitter(settings->getInputFilePath(),
                  settings->getLinesPerChunk(),
                  settings->getBufferSize(),
                  settings->getDelimiter(),
                  settings->getHeader()),
        _delimiter(settings->getDelimiter())
    {
        super::setEnforceHorizontalIteration(true);
    }

    virtual ~FileSplitArray()
    {}

    size_t getCurrentRowIndex() const
    {
        return _rowIndex;
    }

    bool moveNext(size_t rowIndex)
    {
        _buffer = _splitter.getBlock(_bufferSize);
        if(_bufferSize > 0)
        {
            ++_rowIndex;
            return true;
        }
        else
        {
            return false;
        }
    }

    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
    {
        _chunkAddress.coords[1] = _rowIndex  -1;
        std::shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        _chunk.initialize(this, &super::getArrayDesc(), _chunkAddress, 0);
        std::shared_ptr<ChunkIterator> chunkIt = _chunk.getIterator(query, ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        Value v;
        if(_buffer[_bufferSize-1] == _delimiter) //add the null-termination character; replace the last delimiter character if one is present
        {
            v.setSize(_bufferSize);
            char *d = (char*) v.data();
            memcpy(d, _buffer, _bufferSize);
            d[_bufferSize-1] = 0;
        }
        else
        {
            v.setSize(_bufferSize+1);
            char *d = (char*) v.data();
            memcpy(d, _buffer, _bufferSize);
            d[_bufferSize] = 0;
        }
        chunkIt->writeItem(v);
        chunkIt->flush();
        return _chunk;
    }
};

class PhysicalSplit : public PhysicalOperator
{
public:
    PhysicalSplit(std::string const& logicalName,
                     std::string const& physicalName,
                     Parameters const& parameters,
                     ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const&, std::vector<ArrayDesc> const&) const
    {
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        std::shared_ptr<SplitSettings> settings (new SplitSettings (_parameters, false, query));
        if (settings->getSourceInstanceId() != 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "At the moment, the source_instance_id flag is not supported";
        }
        std::shared_ptr<Array> result;
        if(query->isCoordinator())
        {
            result = std::shared_ptr<FileSplitArray>(new FileSplitArray(_schema, query, settings));
        }
        else
        {
            result = std::shared_ptr<EmptySinglePass>(new EmptySinglePass(_schema));
        }
        SCIDB_ASSERT(_schema.getResidency()->isEqual(query->getDefaultArrayResidency()));

        result = redistributeToRandomAccess(result,
                                            _schema.getDistribution(),
                                            ArrayResPtr(), // default query residency
                                            query);

        return result;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalSplit, "split", "PhysicalSplit");


} // end namespace scidb
