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
 * \file PhysicalCancel.cpp
 *
 * \author roman.simakov@gmail.com
 */

#include <iostream>
#include <query/QueryID.h>
#include <query/Operator.h>
#include <query/executor/SciDBExecutor.h>

#include <SciDBAPI.h>

using namespace std;

namespace scidb {

class PhysicalCancel: public PhysicalOperator
{
public:
    PhysicalCancel(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
	    PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    std::shared_ptr<Array> execute(vector<std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        const scidb::SciDB& scidb = getSciDBExecutor();
        std::stringstream queryIdS (dynamic_pointer_cast<OperatorParamPhysicalExpression>(_parameters[0])->getExpression()->evaluate().getString());
        QueryID queryID;
        queryIdS >> queryID;
        scidb.cancelQuery(queryID);

        return std::shared_ptr<Array>();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCancel, "cancel", "cancel_impl")

}  // namespace ops
