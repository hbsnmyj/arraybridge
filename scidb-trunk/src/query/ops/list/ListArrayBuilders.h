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

#ifndef QUERY_LIST_ARRAY_BUILDER_H_
#define QUERY_LIST_ARRAY_BUILDER_H_

/****************************************************************************/

#include <array/ListArrayBuilder.h>
#include <smgr/io/InternalStorage.h>
#include <query/Aggregate.h>
#include <util/PluginManager.h>
#include <util/DataStore.h>
#include <util/Counter.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

/**
 *  A ListArrayBuilder for listing ChunkDescriptor objects.
 */
struct ListChunkDescriptorsArrayBuilder : ListArrayBuilder
{
    enum
    {
        STORAGE_VERSION,
        INSTANCE_ID,
        DATASTORE_GUID,
        DISK_HEADER_POS,
        DISK_OFFSET,
        V_ARRAY_ID,
        ATTRIBUTE_ID,
        COORDINATES,
        COMPRESSION,
        FLAGS,
        NUM_ELEMENTS,
        COMPRESSED_SIZE,
        UNCOMPRESSED_SIZE,
        ALLOCATED_SIZE,
        FREE,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void       list(const ChunkDescriptor&,bool free);
    Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing PersistentChunk objects.
 */
struct ListChunkMapArrayBuilder : ListArrayBuilder
{
    enum
    {
        STORAGE_VERSION,
        INSTANCE_ID,
        DATASTORE_GUID,
        DISK_HEADER_POS,
        DISK_OFFSET,
        U_ARRAY_ID,
        V_ARRAY_ID,
        ATTRIBUTE_ID,
        COORDINATES,
        COMPRESSION,
        FLAGS,
        NUM_ELEMENTS,
        COMPRESSED_SIZE,
        UNCOMPRESSED_SIZE,
        ALLOCATED_SIZE,
        ADDRESS,
        CLONE_OF,
        CLONES,
        NEXT,
        PREV,
        DATA,
        ACCESS_COUNT,
        N_WRITERS,
        TIMESTAMP,
        RAW,
        WAITING,
        LAST_POS,
        FIRST_POS_OVERLAP,
        LAST_POS_OVERLAP,
        STORAGE,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void       list(const ArrayUAID&,
                    const StorageAddress&,
                    const PersistentChunk*,
                    uint64_t,
                    bool);
    Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing loaded library information.
 */
struct ListLibrariesArrayBuilder : ListArrayBuilder
{
    enum
    {
        PLUGIN_NAME,
        MAJOR,
        MINOR,
        PATCH,
        BUILD,
        BUILD_TYPE,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void       list(const PluginManager::Plugin&);
    Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing aggregates information.
 */
struct ListAggregatesArrayBuilder : ListArrayBuilder
{
    size_t const _cellCount;
    bool const   _showSys;

    void       list(const std::string&,const TypeId&,const std::string&);
    Attributes getAttributes()                                const;
    Dimensions getDimensions(std::shared_ptr<Query> const&) const;

    ListAggregatesArrayBuilder(size_t cellCount, bool showSys);
};

/**
 *  A ListArrayBuilder for listing datastore information.
 */
struct ListDataStoresArrayBuilder : ListArrayBuilder
{
    enum
    {
        GUID,
        FILE_BYTES,
        FILE_BLOCKS_512,
        FILE_FREE,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void       list(const DataStore&);
    Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing Query objects.
 */
struct ListQueriesArrayBuilder : ListArrayBuilder
{
    enum
    {
        QUERY_ID,
        COORDINATOR,
        QUERY_STR,
        CREATION_TIME,
        ERROR_CODE,
        ERROR,
        IDLE,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void       list(const std::shared_ptr<Query>&);
    Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing counter values.
 */
struct ListCounterArrayBuilder : ListArrayBuilder
{
    enum
    {
        NAME,
        TOTAL,
        TOTAL_MSECS,
        AVG_MSECS,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void       list(const CounterState::Entry&);
    Attributes getAttributes() const;
};

/**
 *  A ListArrayBuilder for listing array information.
 */
struct ListArraysArrayBuilder : ListArrayBuilder
{
    enum
    {
        ARRAY_NAME,
        ARRAY_UAID,
        ARRAY_ID,
        ARRAY_SCHEMA,
        ARRAY_IS_AVAILABLE,
        ARRAY_IS_TRANSIENT,
        EMPTY_INDICATOR,
        NUM_ATTRIBUTES
    };

    void       list(const ArrayDesc&);
    Attributes getAttributes()                                const;
    Dimensions getDimensions(std::shared_ptr<Query> const&) const;
};

/****************************************************************************/
}
/****************************************************************************/
#endif
/****************************************************************************/
