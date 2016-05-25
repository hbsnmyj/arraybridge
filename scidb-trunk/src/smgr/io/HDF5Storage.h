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
 * @file HDF5Storage.h
 *
 *  Created on: 19.02.2010
 *      Author: xing.136@osu.edu
 * Description: HDF5-related storage implementations.
 */

#ifndef SCIDB_HDF5WRITER_H_
#define SCIDB_HDF5WRITER_H_

#include <string>
#include <memory>
#include <vector>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>
#include <hdf5.h>
#include <array/Coordinate.h>
#include <array/Array.h>
#include <fcntl.h>
#include <chrono>
#include <thread>

namespace scidb {
namespace hdf5gateway {

    using H5Coordinates = std::vector<hsize_t>;

    inline bool existsFile(std::string const& filename) {
        boost::filesystem::path path(filename);
        return boost::filesystem::exists(path);
    }

    inline void lock_file(std::string const& filename) {
        using namespace std::chrono_literals;
        int fd = 0;
        do {
            fd = open(filename.c_str(), O_CREAT | O_EXCL | O_RDWR, 0666);
            std::this_thread::sleep_for(2ms);
        } while (fd < 0);
    }


    inline void unlock(std::string const& filename) {
        remove(filename.c_str());
    }

    /**
     * a class to represent the type in HDF5 file.
     */
    class HDF5Type {
      public:
        HDF5Type(std::string const& typeName);
        HDF5Type(const hid_t& typeName);
        hid_t getHDF5Type() { return _hdf5_type; }
      private:
        hid_t _hdf5_type;
    };

    class HDF5File
    {
      public:
        enum class CreateOption {
            kTrunc,
            kExcl,
        };
        enum class OpenOption {
            kRDWR,
            kRDONLY,
        };
        struct CreateOrOpenParam
        {
            const std::string& filename;
        };

      public:
        HDF5File(std::string const& filename, CreateOption const& param);
        HDF5File(std::string const& filename, OpenOption const& option);
        HDF5File(CreateOrOpenParam const& option);

        HDF5File(const HDF5File&) = delete;
        HDF5File(HDF5File&&) = delete;
        HDF5File& operator=(const HDF5File&) = delete;
        HDF5File& operator=(HDF5File&&) = delete;
        ~HDF5File();
        hid_t getFileID() { return _fileID; }

      private:
        hid_t _fileID;
    };

    inline bool existsLink(HDF5File& file, std::string const& linkname) {
        return H5Lexists(file.getFileID(), linkname.c_str(), H5P_DEFAULT) > 0;
    }

    class HDF5Dataset {
      public:
        /*
         * create dataset in hdf5 file
         * @param file file to be created
         * @param datasetName dataset name
         * @param type the type of the dataset
         * @param dims the total dimension of the dataset
         * @param chunk dimension of the dataset
         */
        HDF5Dataset(HDF5File& file, std::string const& datasetName, std::string const& type,
                    H5Coordinates const & dims, H5Coordinates const& chunk_dims);
        HDF5Dataset(HDF5File& file, std::string const& datasetName);
        HDF5Dataset(const HDF5Dataset&) = delete;
        HDF5Dataset(HDF5Dataset&&) = delete;
        HDF5Dataset& operator=(const HDF5Dataset&) = delete;
        HDF5Dataset& operator=(HDF5Dataset&&) = delete;
        ~HDF5Dataset();

        int writeData(void* data, H5Coordinates const& target_pos, H5Coordinates const& block);
        int writeChunk(ConstChunk const& chunk, H5Coordinates const& target_pos);
      private:
        hid_t _datasetId;
        hid_t _fspaceId;
        H5Coordinates _stride;
        H5Coordinates _count;
        HDF5Type _type;
    };

}
}


#endif //SCIDB_HDF5WRITER_H
