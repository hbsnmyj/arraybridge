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
 * HDF5ArrayDesc.h
 *
 *  Created on: June 8 2016
 *      Author: Haoyuan Xing<hbsnmyj@gmail.com>
 */

#ifndef SCIDB_HDF5ARRAYDESC_H
#define SCIDB_HDF5ARRAYDESC_H

#include <string>
#include <smgr/io/HDF5Storage.h>

namespace scidb
{
namespace hdf5gateway
{

class HDF5ArrayAttribute
{
public:
    HDF5ArrayAttribute(std::string filePath, std::string datasetName, HDF5Type type)
            : _filePath(filePath), _datasetName(datasetName), _type(type) { }
    const std::string& getFilePath() const { return _filePath; }
    const std::string& getDatasetName() const { return _datasetName; }
    const HDF5Type& getType() const { return _type; }
private:
    std::string _filePath;
    std::string _datasetName;
    HDF5Type _type;
};

class HDF5ArrayDesc
{
public:
    HDF5ArrayDesc() {}
    int addAttribute(HDF5ArrayAttribute&& attribute) {
        int id = (int) _attributes.size();
        _attributes.push_back(std::move(attribute));
        return id;
    }
    template<typename ...Args>
    int addAttribute(Args&& ...args) {
        int id = (int) _attributes.size();
        _attributes.emplace_back(std::forward<Args>(args)...);
        return id;
    }
    const HDF5ArrayAttribute& getAttribute(int attrId) const {
        return _attributes[attrId];
    }
private:
    std::vector<HDF5ArrayAttribute> _attributes;
};

} // namespace hdf5gateway
} // namespace scidb
#endif //SCIDB_HDF5ARRAYDESC_H
