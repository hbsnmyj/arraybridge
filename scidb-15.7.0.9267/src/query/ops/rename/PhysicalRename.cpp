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
 * PhysicalRename.cpp
 *
 *  Created on: Apr 16, 2010
 *      Author: Knizhnik
 */

#include <boost/foreach.hpp>
#include <memory>
#include <boost/bind.hpp>
#include "query/Operator.h"
#include "array/DBArray.h"
#include "system/SystemCatalog.h"
#include "smgr/io/Storage.h"


namespace scidb {

using namespace std;
using namespace boost;

class PhysicalRename: public PhysicalOperator
{
    public:
    PhysicalRename(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        getInjectedErrorListener().check();
        return std::shared_ptr<Array>();
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        std::shared_ptr<const InstanceMembership> membership(Cluster::getInstance()->getInstanceMembership());
        assert(membership);
        if (((membership->getViewId() != query->getCoordinatorLiveness()->getViewId()) ||
             (membership->getInstances().size() != query->getInstancesCount()))) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
        }
        const string& oldArrayName = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        _oldArrayName = oldArrayName;
        assert(!_oldArrayName.empty());
    }

    void postSingleExecute(std::shared_ptr<Query> query)
    {
        assert(!_oldArrayName.empty());
        const string& newArrayName = ((std::shared_ptr<OperatorParamReference>&)_parameters[1])->getObjectName();
        SystemCatalog::getInstance()->renameArray(_oldArrayName, newArrayName);
    }

    private:
    std::string _oldArrayName;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRename, "rename", "physicalRename")

}  // namespace scidb
