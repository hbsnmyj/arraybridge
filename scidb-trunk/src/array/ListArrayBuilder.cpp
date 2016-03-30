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

/****************************************************************************/

#include <boost/assign/list_of.hpp>                      // For list_of()
#include <boost/foreach.hpp>                             // For FOREACH
#include "array/ListArrayBuilder.h"

using namespace std;

/****************************************************************************/
namespace scidb {
/****************************************************************************/

using boost::assign::list_of;

/****************************************************************************/

ListArrayBuilder::ListArrayBuilder(const std::string &listName)
                : _nAttrs(0),
                  _dimIdOff(0),
                  _listName(listName)
{}

ArrayDesc ListArrayBuilder::getSchema(std::shared_ptr<Query> const& query) const
{
    ArrayDesc desc(_listName,
                   getAttributes(),
                   getDimensions(query),
                   defaultPartitioning(),
                   query->getDefaultArrayResidency());
    return desc;
}

Dimensions ListArrayBuilder::getDimensions(std::shared_ptr<Query> const& query) const
{
    size_t n = query->getCoordinatorLiveness()->getNumInstances();

    return list_of
        (DimensionDesc("inst",0,0,n-1,n-1,1,0))
        (DimensionDesc("n",   0,0,CoordinateBounds::getMax(),CoordinateBounds::getMax(),LIST_CHUNK_SIZE,0));
}

void ListArrayBuilder::initialize(std::shared_ptr<Query> const& query)
{
    ArrayDesc  const schema = getSchema(query);
    _dimIdOff               = 0;
    _nAttrs                 = schema.getAttributes().size() - 1;
    _array                  = make_shared<MemArray>(schema,query);

    _currPos.resize(schema.getDimensions().size(),0);

    if (_currPos.size() > 1)
    {
        // adding the instance coordinate
        _currPos[0] = query->getInstanceID();
        _dimIdOff = 1;
    }

    _outAIters.reserve(_nAttrs);

    for (AttributeID i =0; i<_nAttrs; ++i)
    {
        _outAIters.push_back(_array->getIterator(i));
    }

    _outCIters.reserve(_nAttrs);

    for (AttributeID i=0; i<_nAttrs; ++i)
    {
        Chunk& ch = _outAIters[i]->newChunk(_currPos);
        _outCIters.push_back(ch.getIterator(query, i==0 ? ChunkIterator::SEQUENTIAL_WRITE :
                                                          ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK));
    }

    _nextChunkPos = _currPos;
    _nextChunkPos[_dimIdOff] += LIST_CHUNK_SIZE;
    _query = query;
}

std::shared_ptr<MemArray> ListArrayBuilder::getArray()
{
    assert(_query);

    BOOST_FOREACH(std::shared_ptr<ChunkIterator> i,_outCIters)
    {
        i->flush();
    }

    return _array;
}

void ListArrayBuilder::beginElement()
{
    assert(_query);

    if (_currPos[_dimIdOff] == _nextChunkPos[_dimIdOff])
    {
        for (AttributeID i=0; i<_nAttrs; ++i)
        {
            _outCIters[i]->flush();
            Chunk& ch = _outAIters[i]->newChunk(_currPos);
            _outCIters[i] = ch.getIterator(_query,i==0 ? ChunkIterator::SEQUENTIAL_WRITE :
                                                         ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        }

        _nextChunkPos[_dimIdOff] += LIST_CHUNK_SIZE;
    }

    BOOST_FOREACH(std::shared_ptr<ChunkIterator> i,_outCIters)
    {
        i->setPosition(_currPos);
    }
}

void ListArrayBuilder::endElement()
{
    assert(_query);

    ++_currPos[_dimIdOff];
}

AttributeDesc ListArrayBuilder::emptyBitmapAttribute(AttributeID id)
{
    return AttributeDesc(id,DEFAULT_EMPTY_TAG_ATTRIBUTE_NAME,TID_INDICATOR,AttributeDesc::IS_EMPTY_INDICATOR,0);
}

/****************************************************************************/
}
/****************************************************************************/
