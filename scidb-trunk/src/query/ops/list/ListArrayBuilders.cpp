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
#include <iostream>
#include <boost/assign/list_of.hpp>                      // For list_of()
#include "ListArrayBuilders.h"

using namespace std;

/****************************************************************************/
namespace scidb {
/****************************************************************************/

using namespace boost::assign;                           // For list_of()

Attributes ListChunkDescriptorsArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(STORAGE_VERSION,   "svrsn",TID_UINT32,0,0))
    (AttributeDesc(INSTANCE_ID,       "insn", TID_UINT64,0,0))
    (AttributeDesc(DATASTORE_GUID,    "dguid",TID_UINT64,0,0))
    (AttributeDesc(DISK_HEADER_POS,   "dhdrp",TID_UINT64,0,0))
    (AttributeDesc(DISK_OFFSET,       "doffs",TID_UINT64,0,0))
    (AttributeDesc(V_ARRAY_ID,        "arrid",TID_UINT64,0,0))
    (AttributeDesc(ATTRIBUTE_ID,      "attid",TID_UINT64,0,0))
    (AttributeDesc(COORDINATES,       "coord",TID_STRING,0,0))
    (AttributeDesc(COMPRESSION,       "comp", TID_INT8,  0,0))
    (AttributeDesc(FLAGS,             "flags",TID_UINT8, 0,0))
    (AttributeDesc(NUM_ELEMENTS,      "nelem",TID_UINT64,0,0))
    (AttributeDesc(COMPRESSED_SIZE,   "csize",TID_UINT64,0,0))
    (AttributeDesc(UNCOMPRESSED_SIZE, "usize",TID_UINT64,0,0))
    (AttributeDesc(ALLOCATED_SIZE,    "asize",TID_UINT64,0,0))
    (AttributeDesc(FREE,              "free", TID_BOOL,  0,0))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListChunkDescriptorsArrayBuilder::list(const ChunkDescriptor& desc,bool free)
{
    std::ostringstream s;
    s << take(CoordinateCRange(desc.coords),desc.hdr.nCoordinates);

    beginElement();
    write(STORAGE_VERSION,  desc.hdr.storageVersion);
    write(INSTANCE_ID    ,  desc.hdr.instanceId);
    write(DATASTORE_GUID,   desc.hdr.pos.dsGuid);
    write(DISK_HEADER_POS,  desc.hdr.pos.hdrPos);
    write(DISK_OFFSET,      desc.hdr.pos.offs);
    write(V_ARRAY_ID,       desc.hdr.arrId);
    write(ATTRIBUTE_ID,     desc.hdr.attId);
    write(COORDINATES,      s.str());
    write(COMPRESSION,      desc.hdr.compressionMethod);
    write(FLAGS,            desc.hdr.flags);
    write(NUM_ELEMENTS,     desc.hdr.nElems);
    write(COMPRESSED_SIZE,  desc.hdr.compressedSize);
    write(UNCOMPRESSED_SIZE,desc.hdr.size);
    write(ALLOCATED_SIZE,   desc.hdr.allocatedSize);
    write(FREE,             free);
    endElement();
}

/****************************************************************************/

Attributes ListChunkMapArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(STORAGE_VERSION,  "svrsn",TID_UINT32,0,0))
    (AttributeDesc(INSTANCE_ID,      "instn",TID_UINT64,0,0))
    (AttributeDesc(DATASTORE_GUID,   "dguid",TID_UINT64,0,0))
    (AttributeDesc(DISK_HEADER_POS,  "dhdrp",TID_UINT64,0,0))
    (AttributeDesc(DISK_OFFSET,      "doffs",TID_UINT64,0,0))
    (AttributeDesc(U_ARRAY_ID,       "uaid", TID_UINT64,0,0))
    (AttributeDesc(V_ARRAY_ID,       "arrid",TID_UINT64,0,0))
    (AttributeDesc(ATTRIBUTE_ID,     "attid",TID_UINT64,0,0))
    (AttributeDesc(COORDINATES,      "coord",TID_STRING,0,0))
    (AttributeDesc(COMPRESSION,      "comp", TID_INT8,  0,0))
    (AttributeDesc(FLAGS,            "flags",TID_UINT8, 0,0))
    (AttributeDesc(NUM_ELEMENTS,     "nelem",TID_UINT64,0,0))
    (AttributeDesc(COMPRESSED_SIZE,  "csize",TID_UINT64,0,0))
    (AttributeDesc(UNCOMPRESSED_SIZE,"usize",TID_UINT64,0,0))
    (AttributeDesc(ALLOCATED_SIZE,   "asize",TID_UINT64,0,0))
    (AttributeDesc(ADDRESS,          "addrs",TID_UINT64,0,0))
    (AttributeDesc(CLONE_OF,         "clnof",TID_UINT64,0,0))
    (AttributeDesc(CLONES,           "clons",TID_STRING,0,0))
    (AttributeDesc(NEXT,             "next", TID_UINT64,0,0))
    (AttributeDesc(PREV,             "prev", TID_UINT64,0,0))
    (AttributeDesc(DATA,             "data", TID_UINT64,0,0))
    (AttributeDesc(ACCESS_COUNT,     "accnt",TID_INT32, 0,0))
    (AttributeDesc(N_WRITERS,        "nwrit",TID_INT32, 0,0))
    (AttributeDesc(TIMESTAMP,        "tstmp",TID_UINT64,0,0))
    (AttributeDesc(RAW,              "raw",  TID_BOOL,  0,0))
    (AttributeDesc(WAITING,          "waitn",TID_BOOL,  0,0))
    (AttributeDesc(LAST_POS,         "lpos", TID_STRING,0,0))
    (AttributeDesc(FIRST_POS_OVERLAP,"fposo",TID_STRING,0,0))
    (AttributeDesc(LAST_POS_OVERLAP, "lposo",TID_STRING,0,0))
    (AttributeDesc(STORAGE,          "strge",TID_UINT64,0,0))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListChunkMapArrayBuilder::list(const ArrayUAID& uaid,
                                    const StorageAddress& addr,
                                    const PersistentChunk* chunk,
                                    uint64_t tombstonePos,
                                    bool valid)
{
    std::ostringstream s;

    beginElement();
    write(STORAGE_VERSION,  chunk==NULL ? -1 : chunk->_hdr.storageVersion);
    write(INSTANCE_ID,      chunk==NULL ? -1 : chunk->_hdr.instanceId);
    write(DATASTORE_GUID,   chunk==NULL ? -1 : chunk->_hdr.pos.dsGuid);
    write(DISK_HEADER_POS,  chunk==NULL ? tombstonePos : chunk->_hdr.pos.hdrPos);
    write(DISK_OFFSET,      chunk==NULL ? -1 : chunk->_hdr.pos.offs);
    write(U_ARRAY_ID,       uaid);
    write(V_ARRAY_ID,       addr.arrId);
    write(ATTRIBUTE_ID,     addr.attId);
    s << addr.coords;
    write(COORDINATES,      s.str());
    write(COMPRESSION,      chunk==NULL ?  int8_t(-1) : chunk->_hdr.compressionMethod);
    write(FLAGS,
          chunk==NULL ?
          (valid ?
           uint8_t(ChunkHeader::TOMBSTONE) :
           uint8_t(ChunkHeader::INVALID)) :
          chunk->_hdr.flags);
    write(NUM_ELEMENTS,     chunk==NULL ? -1 : chunk->_hdr.nElems);
    write(COMPRESSED_SIZE,  chunk==NULL ? -1 : chunk->_hdr.compressedSize);
    write(UNCOMPRESSED_SIZE,chunk==NULL ? -1 : chunk->_hdr.size);
    write(ALLOCATED_SIZE,   chunk==NULL ? -1 : chunk->_hdr.allocatedSize);
    write(ADDRESS,          uint64_t(chunk));
    write(CLONE_OF,         -1);    //XXX TODO: remove this field (used to be _cloneOf)
    write(CLONES,           "[]");  //XXX TODO: remove this field (used to be _clones)
    write(NEXT,             chunk==NULL ? -1 : uint64_t(chunk->_next));
    write(PREV,             chunk==NULL ? -1 : uint64_t(chunk->_prev));
    write(DATA,             chunk==NULL ? -1 : uint64_t(chunk->_data));
    write(ACCESS_COUNT,     chunk==NULL ? -1 : chunk->_accessCount);
    write(N_WRITERS,        -1);    //XXX tigor TODO: remove _nWrite from the schema
    write(TIMESTAMP,        chunk==NULL ? -1 : chunk->_timestamp);
    write(RAW,              chunk==NULL ? false : chunk->_raw);
    write(WAITING,          chunk==NULL ? false : chunk->_waiting);
    s.str("");
    if (chunk)
    {
        s << chunk->_lastPos;
    }
    write(LAST_POS,         s.str());
    s.str("");
    if (chunk)
    {
        s << chunk->_firstPosWithOverlaps;
    }
    write(FIRST_POS_OVERLAP,s.str());
    s.str("");
    if (chunk)
    {
        s << chunk->_lastPosWithOverlaps;
    }
    write(LAST_POS_OVERLAP, s.str());
    write(STORAGE,          chunk==NULL ? -1 : uint64_t(chunk->_storage));
    endElement();
}

/****************************************************************************/

Attributes ListLibrariesArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(PLUGIN_NAME,"name",      TID_STRING,0,0))
    (AttributeDesc(MAJOR,      "major",     TID_UINT32,0,0))
    (AttributeDesc(MINOR,      "minor",     TID_UINT32,0,0))
    (AttributeDesc(PATCH,      "patch",     TID_UINT32,0,0))
    (AttributeDesc(BUILD,      "build",     TID_UINT32,0,0))
    (AttributeDesc(BUILD_TYPE, "build_type",TID_STRING,AttributeDesc::IS_NULLABLE, 0))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListLibrariesArrayBuilder::list(const PluginManager::Plugin& e)
{
    beginElement();
    write(PLUGIN_NAME,e._name);
    write(MAJOR,      e._major);
    write(MINOR,      e._minor);
    write(BUILD,      e._build);
    write(PATCH,      e._patch);
    if (e._buildType.empty())
        write(BUILD_TYPE,Value());
    else
        write(BUILD_TYPE,e._buildType);
    endElement();
}

/****************************************************************************/

ListAggregatesArrayBuilder::ListAggregatesArrayBuilder(size_t cellCount, bool showSys)
 : ListArrayBuilder("aggregates")
 , _cellCount(cellCount)
 , _showSys(showSys)
{}

Dimensions ListAggregatesArrayBuilder::getDimensions(std::shared_ptr<Query> const& query) const
{
    size_t cellCount = std::max(_cellCount,1UL);

    return Dimensions(1,DimensionDesc("No",0,0,cellCount-1,cellCount-1,cellCount,0));
}

Attributes ListAggregatesArrayBuilder::getAttributes() const
{
    Attributes attrs = list_of
        (AttributeDesc(0, "name",   TID_STRING,0,0))
        (AttributeDesc(1, "typeid", TID_STRING,0,0))
        (AttributeDesc(2, "library",TID_STRING,0,0));
    if (_showSys) {
        attrs.push_back(AttributeDesc(3, "internal", TID_BOOL,0,0));
        attrs.push_back(emptyBitmapAttribute(4));
    } else {
        attrs.push_back(emptyBitmapAttribute(3));
    }
    return attrs;
}

void ListAggregatesArrayBuilder::list(const std::string& name,const TypeId& type,const std::string& lib)
{
    bool isSys = (name.find('_') == 0);
    if (!isSys || _showSys) {
        beginElement();
        write(0, name);
        write(1, type);
        write(2, lib);
        if (_showSys) {
            write(3, isSys);
        }
        endElement();
    }
}

/****************************************************************************/

Attributes ListDataStoresArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(GUID,           "uaid",           TID_UINT64,0,0))
    (AttributeDesc(FILE_BYTES,     "file_bytes",     TID_UINT64,0,0))
    (AttributeDesc(FILE_BLOCKS_512,"file_blocks_512",TID_UINT64,0,0))
    (AttributeDesc(FILE_FREE,      "file_free_bytes",TID_UINT64,0,0))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListDataStoresArrayBuilder::list(DataStore const& item)
{
    off_t    filesize = 0;
    blkcnt_t fileblks = 0;
    off_t    filefree = 0;

    item.getSizes(filesize, fileblks, filefree);

    beginElement();
    write(GUID,           item.getGuid());
    write(FILE_BYTES,     filesize);
    write(FILE_BLOCKS_512,fileblks);
    write(FILE_FREE ,     filefree);
    endElement();
}

/****************************************************************************/

Attributes ListQueriesArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(QUERY_ID,     "query_id",     TID_STRING,  0,0))
    (AttributeDesc(COORDINATOR,  "coordinator",  TID_UINT64,  0,0))
    (AttributeDesc(QUERY_STR,    "query_string", TID_STRING,  0,0))
    (AttributeDesc(CREATION_TIME,"creation_time",TID_DATETIME,0,0))
    (AttributeDesc(ERROR_CODE,   "error_code",   TID_INT32,   0,0))
    (AttributeDesc(ERROR,        "error",        TID_STRING,  0,0))
    (AttributeDesc(IDLE,         "idle",         TID_BOOL,    0,0))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListQueriesArrayBuilder::list(std::shared_ptr<Query> const& query)
{
    const bool resolveLocalInstanceID = true;
    std::stringstream qs;
    beginElement();
    qs << query->getQueryID();

    write(QUERY_STR,    query->queryString);
    write(QUERY_ID,     qs.str());
    write(COORDINATOR,  query->getPhysicalCoordinatorID(resolveLocalInstanceID));
    write(CREATION_TIME,query->getCreationTime());

    std::shared_ptr<Exception> error(query->getError());

    write(ERROR_CODE,   error ? error->getLongErrorCode() : 0);
    write(ERROR,        error ? error->getErrorMessage() : "");
    write(IDLE,         query->idle());
    endElement();
}

/****************************************************************************/

Attributes ListCounterArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(NAME,       "name",       TID_STRING,0,0))
    (AttributeDesc(TOTAL,      "total",      TID_UINT64,0,0))
    (AttributeDesc(TOTAL_MSECS,"total_msecs",TID_UINT64,0,0))
    (AttributeDesc(AVG_MSECS,  "avg_msecs",  TID_FLOAT, 0,0))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListCounterArrayBuilder::list(const CounterState::Entry& item)
{
    float avg_msecs =
      item._num ?
      static_cast<float>(item._msecs) / static_cast<float>(item._num) :
      0;

    beginElement();
    write(NAME,       CounterState::getInstance()->getName(item._id));
    write(TOTAL,      item._num);
    write(TOTAL_MSECS,item._msecs);
    write(AVG_MSECS,  avg_msecs);
    endElement();
}

/****************************************************************************/

Dimensions ListArraysArrayBuilder::getDimensions(const std::shared_ptr<Query>& query) const
{
    return Dimensions(
        1,DimensionDesc("No",0,0,
        CoordinateBounds::getMax(),
        CoordinateBounds::getMax(),
        LIST_CHUNK_SIZE,0));
}

Attributes ListArraysArrayBuilder::getAttributes() const
{
    return list_of
    (AttributeDesc(ARRAY_NAME,        "name",        TID_STRING,0,0))
    (AttributeDesc(ARRAY_UAID,        "uaid",        TID_INT64, 0,0))
    (AttributeDesc(ARRAY_ID,          "aid",         TID_INT64, 0,0))
    (AttributeDesc(ARRAY_SCHEMA,      "schema",      TID_STRING,0,0))
    (AttributeDesc(ARRAY_IS_AVAILABLE,"availability",TID_BOOL,  0,0))
    (AttributeDesc(ARRAY_IS_TRANSIENT,"temporary",   TID_BOOL,  0,0))
    (emptyBitmapAttribute(EMPTY_INDICATOR));
}

void ListArraysArrayBuilder::list(const ArrayDesc& desc)
{
    stringstream s;
    printSchema(s,desc);

    beginElement();
    write(ARRAY_NAME,        desc.getName());
    write(ARRAY_UAID,        desc.getUAId());
    write(ARRAY_ID,          desc.getId());
    write(ARRAY_SCHEMA,      s.str());
    write(ARRAY_IS_AVAILABLE,!desc.isInvalid());
    write(ARRAY_IS_TRANSIENT,desc.isTransient());
    endElement();
}

/****************************************************************************/
}
/****************************************************************************/
