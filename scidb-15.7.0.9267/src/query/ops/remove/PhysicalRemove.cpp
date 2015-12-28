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
 * PhysicalRemove.cpp
 *
 *  Created on: Apr 16, 2010
 *      Author: Knizhnik
 */

#include <boost/foreach.hpp>
#include <deque>

#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "array/DBArray.h"
#include "array/TransientCache.h"
#include "smgr/io/Storage.h"
#include "system/SystemCatalog.h"

using namespace std;
using namespace boost;

namespace scidb {

class PhysicalRemove: public PhysicalOperator
{
public:
   PhysicalRemove(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
   PhysicalOperator(logicalName, physicalName, parameters, schema)
   {
   }

   void preSingleExecute(std::shared_ptr<Query> query)
   {
       std::shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
       assert(membership);
       if (((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
            (membership->getInstances().size() != query->getInstancesCount()))) {
           throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
       }
       const string &arrayName = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
       _lock = std::shared_ptr<SystemCatalog::LockDesc>(new SystemCatalog::LockDesc(arrayName,
                                                                                      query->getQueryID(),
                                                                                      Cluster::getInstance()->getLocalInstanceId(),
                                                                                      SystemCatalog::LockDesc::COORD,
                                                                                      SystemCatalog::LockDesc::RM));
       std::shared_ptr<Query::ErrorHandler> ptr(new RemoveErrorHandler(_lock));
       query->pushErrorHandler(ptr);

       bool arrayExists = SystemCatalog::getInstance()->getArrayDesc(arrayName,
                                                                     query->getCatalogVersion(arrayName),
                                                                     _schema, true);
       SCIDB_ASSERT(arrayExists);
       assert(_schema.getName() == arrayName);
   }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        getInjectedErrorListener().check();

        assert(_schema.getUAId() != 0);
        StorageManager::getInstance().removeVersions(query->getQueryID(), _schema.getUAId(), 0);
        transient::remove(_schema);
        return std::shared_ptr<Array>();
    }

    void postSingleExecute(std::shared_ptr<Query> query)
    {
        bool rc = RemoveErrorHandler::handleRemoveLock(_lock, true);
        if (!rc) assert(false);
    }

private:

   std::shared_ptr<SystemCatalog::LockDesc> _lock;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRemove, "remove", "physicalRemove")

}  // namespace ops
