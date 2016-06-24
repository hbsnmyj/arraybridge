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
 * PhysicalScanHDF5.cpp
 *
 *  Created on: June 8 2016
 *      Author: Haoyuan Xing<hbsnmyj@gmail.com>
 */


#include <memory>
#include <log4cxx/logger.h>
#include <query/Operator.h>
#include <fstream>
#include "HDF5Array.h"

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.physical_scan_hdf5"));

class PhysicalScanHDF5: public PhysicalOperator
{
public:
    PhysicalScanHDF5(const std::string& logicalName,
                     const std::string& physicalName,
                     const Parameters& parameters,
                     const ArrayDesc& schema):
            PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
        _arrayName = std::dynamic_pointer_cast<OperatorParamReference>(parameters[0])->getObjectName();
        LOG4CXX_DEBUG(logger, "hdf5gateway::scan_hdf5() array = " << _arrayName.c_str() << "\n" );
    }

    virtual RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                                      const std::vector< ArrayDesc> & inputSchemas) const
    {
        return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries>& inputBoundaries,
                                                   const std::vector<ArrayDesc>& inputSchemas) {
        Coordinates lowBoundary = _schema.getLowBoundary();
        Coordinates highBoundary = _schema.getHighBoundary();

        return PhysicalBoundaries(lowBoundary, highBoundary);
    }

    std::shared_ptr<Array> execute(std::vector<std::shared_ptr<Array>>& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        using hdf5gateway::HDF5Array;
        using hdf5gateway::HDF5ArrayDesc;
        using hdf5gateway::HDF5Type;
        HDF5ArrayDesc h5desc;
        /* Right now we read the filename using $HOME/scidb_hdf5.config */
        char* home = getenv("HOME");
        std::string config_file = std::string(home) + "/scidb_hdf5.config";
        std::ifstream infile(config_file);
        std::string hdf5_file;
        infile >> hdf5_file;

        auto attributes = _schema.getAttributes(true);
        for(auto& attribute: attributes) {
            h5desc.addAttribute(hdf5_file, attribute.getName(), HDF5Type(attribute.getType()));
        }

        return std::make_shared<HDF5Array>(_schema, h5desc, query, _arena);
    }

private:
    std::string _arrayName;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalScanHDF5, "scan_hdf5", "impl_hdf5_scan")

} // namespace scidb