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
 * @file LogicalReduceDistro.cpp
 * @author poliocough@gmail.com
 */

//#include <regex.h>
#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/TypeSystem.h"

namespace scidb
{

using namespace std;
using namespace boost;

/**
 * @brief The operator: reduce_distro().
 *
 * @par Synopsis:
 *   reduce_distro( replicatedArray, partitioningSchema )
 *
 * @par Summary:
 *   Makes a replicated array appear as if it has the required partitioningSchema.
 *
 * @par Input:
 *   - replicatedArray: an source array which is replicated across all the instances.
 *   - partitioningSchema: the desired partitioning schema.
 *
 * @par Output array:
 *        <
 *   <br>   same attributes as in replicatedArray
 *   <br> >
 *   <br> [
 *   <br>   same dimensions as in replicatedArray
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *
 */
class LogicalReduceDistro: public  LogicalOperator
{
public:
    LogicalReduceDistro(string const& logicalName, string const& alias):
        LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        ADD_PARAM_INPUT();
        ADD_PARAM_CONSTANT("int32");
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        SCIDB_ASSERT(_parameters.size()>=1);

        ArrayDesc outSchema(schemas[0]);

        // keep the residency but change distribution
        OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[0].get());
        const PartitioningSchema ps = static_cast<PartitioningSchema>( evaluate(lExp->getExpression(), query, TID_INT32).getInt32());

        outSchema.setDistribution(createDistribution(ps));

        return outSchema;
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalReduceDistro, "_reduce_distro")

} //namespace
