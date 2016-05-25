#include "HDF5Storage.h"

#include <algorithm>
#include <functional>
#include "array/Array.h"

namespace scidb {
namespace hdf5gateway
{

    HDF5File::HDF5File(CreateOrOpenParam const& param)
    {
        if(existsFile(param.filename)) {
            _fileID = H5Fopen(param.filename.c_str(), H5F_ACC_RDWR, H5P_DEFAULT);
        } else {
            _fileID = H5Fcreate(param.filename.c_str(), H5F_ACC_EXCL, H5P_DEFAULT,
                                H5P_DEFAULT);
        }
        assert(_fileID >= 0);
    }

    HDF5File::HDF5File(std::string const& filename, CreateOption const& option)
    {
        unsigned flags = (option == CreateOption::kTrunc)? H5F_ACC_TRUNC : H5F_ACC_EXCL;
        _fileID = H5Fcreate(filename.c_str(), flags, H5P_DEFAULT, H5P_DEFAULT);
        assert(_fileID >= 0);
    }


    HDF5File::HDF5File(std::string const& filename, OpenOption const& param)
    {
        unsigned flags = ( param == OpenOption::kRDONLY) ? H5F_ACC_RDONLY: H5F_ACC_RDWR;
        _fileID = H5Fopen(filename.c_str(), flags, H5P_DEFAULT);
        assert(_fileID >= 0);
    }

    HDF5File::~HDF5File()
    {
        if (_fileID >= 0)
            H5Fclose(_fileID);
    }

    HDF5Dataset::HDF5Dataset(HDF5File &file, std::string const &datasetName, std::string const& typeName,
                                                 const scidb::hdf5gateway::H5Coordinates &dims,
                                                 const scidb::hdf5gateway::H5Coordinates &chunk_dims) :
            _stride(dims.size(), 1), _count(dims.size(), 1), _type(typeName)
    {
        hid_t file_id = file.getFileID();
        auto rank = static_cast<int>(dims.size());
        assert(dims.size() == chunk_dims.size());
        hid_t dspace_id = H5Screate_simple(rank, dims.data(), NULL);
        hid_t dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
        H5Pset_chunk(dcpl_id, rank, chunk_dims.data());

        _datasetId = H5Dcreate(file_id, datasetName.c_str(), _type.getHDF5Type(), dspace_id,
                H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
        assert(_datasetId >= 0);
        _fspaceId = H5Dget_space(_datasetId);
        _mspaceId = H5Screate_simple(rank, chunk_dims.data(), NULL);
    }

    HDF5Dataset::~HDF5Dataset()
    {
        if (_datasetId >= 0)
            H5Dclose(_datasetId);
    }

    int HDF5Dataset::writeData(void* data, H5Coordinates const& target_pos, H5Coordinates const& block)
    {
        H5Sselect_hyperslab(_fspaceId, H5S_SELECT_SET,
                            target_pos.data(), _stride.data(), _count.data(), block.data());
        hid_t memspace = H5Screate_simple((int)block.size(), block.data(), NULL);
        H5Dwrite(_datasetId, _type.getHDF5Type(), memspace, _fspaceId, H5P_DEFAULT, data);
        H5Sclose(memspace);
        return 0;
    }

    int HDF5Dataset::writeChunk(ConstChunk const& chunk, H5Coordinates const& target_pos)
    {
        std::vector<char> buffer;
        buffer.reserve(chunk.count() * chunk.getAttributeDesc().getSize());
        int iterationMode = ConstChunkIterator::IterationMode::IGNORE_OVERLAPS;
        for(auto it = chunk.getConstIterator(iterationMode); !it->end(); ++(*it)) {
            auto& value = it->getItem();
            buffer.insert(buffer.end(),(char*)value.data(),
                          (char*)value.data() + value.size());
        }

        /* FIXME: decompress the payload. */
        auto first = chunk.getFirstPosition(false);
        auto last = chunk.getLastPosition(false);
        H5Coordinates block;
        block.reserve(first.size());
        std::transform(last.begin(), last.end(), first.begin(), std::back_inserter(block),
                [](hsize_t a, hsize_t b){ return a - b + 1;});
        writeData(buffer.data(), target_pos, block);
        return 0;
    }

    HDF5Type::HDF5Type(std::string const& typeName)
    {
        if(typeName == "float") {
            _hdf5_type = H5T_NATIVE_FLOAT;
        }
    }

}
}



