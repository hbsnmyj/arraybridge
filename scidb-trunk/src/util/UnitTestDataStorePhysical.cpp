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

#include <smgr/io/Storage.h>
#include <util/DataStore.h>
#include <query/Operator.h>
#include <array/Metadata.h>
#include <system/Cluster.h>
#include <query/Query.h>
#include <memory>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <log4cxx/logger.h>
#include <util/NetworkMessage.h>
#include <array/RLE.h>

using namespace boost;
using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.unittest"));

class UnitTestDataStorePhysical: public PhysicalOperator
{
public:

    UnitTestDataStorePhysical(const string& logicalName,
                           const string& physicalName,
                           const Parameters& parameters,
                           const ArrayDesc& schema)
    : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    /* Allocate power-of-two sized blocks in the datastore from size 2^baselow
       up to 2^basehigh, and record the offsets in the blockmap
     */
    void allocatePowerOfTwos(uint32_t baselow,
                             uint32_t basehigh,
                             std::shared_ptr<DataStore> ds,
                             map<size_t, off_t>& blockmap)
    {
        /* Verify params
         */
        if (basehigh < baselow)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestDataStorePhysical" << "invalid argument to allocate";
        }
        if (!ds)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestDataStorePhysical" << "invalid datastore";
        }

        /* do the allocations
         */
        size_t size = 1 << baselow;
        for(uint32_t i = baselow; i < basehigh; ++i, size <<= 1)
        {
            size_t alloc = 0;

            blockmap[size] = ds->allocateSpace(size, alloc);
            if (alloc != size*2)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                    << "UnitTestDataStorePhysical" << "unexpected allocation";
            }
        }
    }

    /* Test the basic functionality of the DataStore class
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        /* Test:

           1) create a datastore for a dummy guid (-1)
           2) allocate a series of blocks, check the size of the store
           3) free all the blocks
           4) check the size of the store and the free bytes
           5) close the store
           6) re-open the store
           7) allocate the same series of blocks, ensure the size of the store is unchanged
           8) write to each block
           9) read back data from each block, verify
           10) remove the store
           11) re-create the store
           12) test allocation/ freeing of blocks with block-splitting (bug 4389)
           13) remove the store
         */

        /* 1)
         */
        std::shared_ptr<DataStore> ds =
            StorageManager::getInstance().getDataStores().getDataStore(
                static_cast<DataStore::Guid>(-1));

        if (!ds)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestDataStorePhysical" << "failed to open data store";
        }

        /* 2)
         */
        map<size_t, off_t> blockmap;
        off_t    size = 0;
        blkcnt_t blocks = 0;
        off_t    freebytes = 0;

        allocatePowerOfTwos(8, 16, ds, blockmap);
        ds->getSizes(size, blocks, freebytes);

        /* 3)
         */
        map<size_t, off_t>::iterator it;

        for (it = blockmap.begin(); it != blockmap.end(); ++it)
        {
            ds->freeChunk(it->second, it->first*2);
        }

        /* 4)
         */
        off_t    size1 = 0;
        blkcnt_t blocks1 = 0;
        off_t    freebytes1 = 0;

        ds->getSizes(size1, blocks1, freebytes1);
        if (size1 != size || blocks1 != blocks ||
            freebytes1 != size1)
        {
            stringstream failstring;
            failstring << "incorrect number of free bytes reported: " << freebytes1;
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestDataStorePhysical" << failstring.str();
        }


        /* 5)
         */
        StorageManager::getInstance().getDataStores().closeDataStore(static_cast<DataStore::Guid>(-1), false);
        ds.reset();

        /* 6)
         */
        ds = StorageManager::getInstance().getDataStores().getDataStore(static_cast<DataStore::Guid>(-1));

        if (!ds)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestDataStorePhysical" << "failed to open data store 2";
        }

        /* 7)
         */
        off_t    size2 = 0;
        blkcnt_t blocks2 = 0;
        off_t    freebytes2 = 0;

        allocatePowerOfTwos(8, 16, ds, blockmap);
        ds->getSizes(size2, blocks2, freebytes2);
        if (size2 != size || blocks2 != blocks || freebytes2 != freebytes)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestDataStorePhysical" << "unexpected change in store size";
        }

        /* 8)
         */
        for (it = blockmap.begin(); it != blockmap.end(); ++it)
        {
            char* buf = new char[it->first];

            for (uint32_t* p = reinterpret_cast<uint32_t*>(buf);
                 p < reinterpret_cast<uint32_t*>(buf + it->first);
                 ++p)
            {
                *p = safe_static_cast<uint32_t>(it->first);
            }
            ds->writeData(it->second, buf, it->first, it->first*2);
            delete [] buf;
        }

        /* 9)
         */
        for (it = blockmap.begin(); it != blockmap.end(); ++it)
        {
            char* buf = new char[it->first];

            ds->readData(it->second, buf, it->first);
            for (uint32_t* p = reinterpret_cast<uint32_t*>(buf);
                 p < reinterpret_cast<uint32_t*>(buf + it->first);
                 ++p)
            {
                if (*p != it->first)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                        << "UnitTestDataStorePhysical" << "mismatch in data read from store";
                }
            }
            delete [] buf;
        }

        /* 10)
         */
        StorageManager::getInstance().getDataStores().closeDataStore(static_cast<DataStore::Guid>(-1), true);
        ds.reset();

        /* 11)
         */
        ds = StorageManager::getInstance().getDataStores().getDataStore(
                static_cast<DataStore::Guid>(-1));
        if (!ds)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestDataStorePhysical" << "failed to open data store2";
        }

        /* 12) Special test for bug 4389.  Make sure freeing a block that is
           already in the freelist works.  Even if the block in the freelist
           has a different size or offset (it is a parent of the block to be
           freed)
         */
        size_t my_alloc = 0;
        off_t my_off = 0;

        my_off = ds->allocateSpace(8 * KiB, my_alloc);
        if (my_alloc != 16 * KiB)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNITTEST_FAILED)
                << "UnitTestDataStorePhysical" << "unexpected allocation";
        }
        ds->freeChunk(my_off, 16 * KiB);
        ds->freeChunk(my_off, 8 * KiB);
        ds->freeChunk(my_off + 8 * KiB, 4 * KiB);
        ds->verifyFreelist();

        /* 13)
         */
        StorageManager::getInstance().getDataStores().closeDataStore(
            static_cast<DataStore::Guid>(-1), true);
        ds.reset();

        return std::shared_ptr<Array> (new MemArray(_schema,query));
    }

};

REGISTER_PHYSICAL_OPERATOR_FACTORY(UnitTestDataStorePhysical, "test_datastores", "UnitTestDataStorePhysical");
}
