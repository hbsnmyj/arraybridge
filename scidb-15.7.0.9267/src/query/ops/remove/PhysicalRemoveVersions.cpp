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
 * PhysicalRemoveVersions.cpp
 *
 *  Created on: Jun 11, 2014
 *      Author: sfridella
 */

#include <boost/foreach.hpp>
#include <deque>

#include "query/Operator.h"
#include "query/QueryProcessor.h"
#include "array/DBArray.h"
#include "smgr/io/Storage.h"
#include "system/SystemCatalog.h"

using namespace std;
using namespace boost;

namespace scidb {

class PhysicalRemoveVersions: public PhysicalOperator
{
public:
   PhysicalRemoveVersions(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
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


       const string &arrayName =
           ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
       VersionID targetVersion =
           ((std::shared_ptr<OperatorParamPhysicalExpression>&)
            _parameters[1])->getExpression()->evaluate().getInt64();

       bool arrayExists = SystemCatalog::getInstance()->getArrayDesc(arrayName,
                                                                     query->getCatalogVersion(arrayName),
                                                                     targetVersion,
                                                                     _schema, true);
       SCIDB_ASSERT(arrayExists);
       assert(_schema.getVersionId() == targetVersion);

       _lock = std::shared_ptr<SystemCatalog::LockDesc>(
           new SystemCatalog::LockDesc(arrayName,
                                       query->getQueryID(),
                                       Cluster::getInstance()->getLocalInstanceId(),
                                       SystemCatalog::LockDesc::COORD,
                                       SystemCatalog::LockDesc::RM)
           );
       _lock->setArrayId(_schema.getUAId());
       _lock->setArrayVersion(targetVersion);

       SystemCatalog::getInstance()->updateArrayLock(_lock);
       std::shared_ptr<Query::ErrorHandler> ptr(new RemoveErrorHandler(_lock));
       query->pushErrorHandler(ptr);
   }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                     std::shared_ptr<Query> query)
    {
        getInjectedErrorListener().check();

        /* Remove target versions from storage
         */
        StorageManager::getInstance().removeVersions(query->getQueryID(),
                                                     _schema.getUAId(),
                                                     _schema.getId());
        return std::shared_ptr<Array>();
    }

    void postSingleExecute(std::shared_ptr<Query> query)
    {
       RemoveErrorHandler::handleRemoveLock(_lock, true);
    }

private:


   std::shared_ptr<SystemCatalog::LockDesc> _lock;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRemoveVersions, "remove_versions", "physicalRemoveVersions")

}  // namespace ops
