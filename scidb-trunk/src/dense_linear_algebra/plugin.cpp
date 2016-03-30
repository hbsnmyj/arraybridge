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
 * @file plugin.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of some plugin functions.
 */

#include <vector>

#include <SciDBAPI.h>
#include <mpi/MPIManager.h>
#include <system/BlockCyclic.h>
#include <system/ErrorsLibrary.h>
#include "DLAErrors.h"

using namespace scidb;

/**
 * EXPORT FUNCTIONS
 * Functions from this section will be used by LOAD LIBRARY operator.
 */
EXPORTED_FUNCTION void GetPluginVersion(uint32_t& major, uint32_t& minor, uint32_t& patch, uint32_t& build)
{
    // MUSTFIX: prior to Cheshire
    // TODO: this is wrong, because the version number should NEVER be a function ... it should be a constant
    //       so that the constant gets put into the C file objects that use it, at the time they are compiled.
    //       But that's not exposed, what is exposed is something that will change at link time.
    //       That's plain wrong.   It should be SCIDB_VERSION_MAJOR, not SCIDB_VERSION_MAJOR().
    //
    //       to fix this it should be include/system/Constants.h that is generated from a .in file ...
    //       not Constants.cpp
    //
    major = SCIDB_VERSION_MAJOR();
    minor = SCIDB_VERSION_MINOR();
    patch = SCIDB_VERSION_PATCH();
    build = SCIDB_VERSION_BUILD();

    scidb::ArrayDistributionFactory::ArrayDistributionConstructor adc(
                                    boost::bind(&scidb::ArrayDistributionFactory::defaultConstructor <
                                                scidb::ScaLAPACKArrayDistribution >, _1, _2, _3));
    scidb::ArrayDistributionFactory::getInstance()->registerConstructor(scidb::psScaLAPACK, adc);

    // SciDB networking should already be set up, let's configure MPI and run its initialization
    scidb::MpiManager::getInstance()->init();
}

class Instance
{
public:
    Instance()
    {
        //register error messages
#       define X(_name, _error)   _msg[_name] = _error ;
#       include "DLAErrors.inc"
#       undef X
        scidb::ErrorsLibrary::getInstance()->registerErrors(DLANameSpace, &_msg);
    }

    ~Instance()
    {
        scidb::ErrorsLibrary::getInstance()->unregisterErrors(DLANameSpace);
    }

private:
    scidb::ErrorsLibrary::ErrorsMessages _msg;
} _instance;
