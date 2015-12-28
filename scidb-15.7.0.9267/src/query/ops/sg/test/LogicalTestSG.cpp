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
 * @file LogicalTestSG.cpp
 *
 * @author
 *
 *
 */

#include <query/Operator.h>

using namespace std;
using namespace boost;

namespace scidb
{

/**
 * @brief The operator: test_sg().
 *
 * @par Synopsis:
 *   test_sg( srcArray, partitionSchema, instanceId=-1, sgMode, isStrict=false, offsetVector=null)
 *
 * @par Summary:
 *   SCATTER/GATHER distributes array chunks over the instances of a cluster.
 *   The result array is returned.
 *
 * @par Input:
 *   - srcArray: the source array, with srcAttrs and srcDims.
 *   - partitionSchema:<br>
 *     0 = psReplication, <br>
 *     1 = psHashPartitioned,<br>
 *     2 = psLocalInstance,<br>
 *     3 = psByRow,<br>
 *     4 = psByCol,<br>
 *     5 = psUndefined.<br>
 *   - instanceId:<br>
 *     -2 = to coordinator (same with 0),<br>
 *     -1 = all instances participate,<br>
 *     0..#instances-1 = to a particular instance.<br>
 *   - sgMode: <br>
 *     'serial'-all attributes are redistributed in order; <br>
 *     'parallel' - all attributes are redistributed concurrently; <br>
 *     ''='parallel'<br>
 *   - isStrict if true, enables the data integrity checks such as for data collisions and out-of-order input chunks, defualt=false. <br>
 *   - offsetVector: a vector of #dimensions values.<br>
 *     To calculate which instance a chunk belongs, the chunkPos is augmented with the offset vector before calculation.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
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
class LogicalTestSG: public LogicalOperator
{
public:
    LogicalTestSG(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_CONSTANT("uint32");
        ADD_PARAM_VARIES();
    }

    std::vector<std::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<std::shared_ptr<OperatorParamPlaceholder> > res;

        // sanity check: #parameters is at least 1 (i.e. partitionSchema), but no more than #dims+4.
        if (_parameters.size()==0 || _parameters.size() > schemas[0].getDimensions().size() + 4) {
            assert(false);
        }

        // the param is before the offset vector
        else if (_parameters.size() <= 3) {
            res.push_back(END_OF_VARIES_PARAMS());

            switch (_parameters.size()) {
            case 1:
                res.push_back(PARAM_CONSTANT("int64"));
                break;
            case 2:
                res.push_back(PARAM_CONSTANT("string"));
                break;
            case 3:
                res.push_back(PARAM_CONSTANT("bool"));
                break;
            default:
                ASSERT_EXCEPTION(false, "LogicalSG::nextVaryParamPlaceholder");
                break;
            }
        }

        // the param is in the offset vector
        else if (_parameters.size() < schemas[0].getDimensions().size() + 4) {
            // along with the first value in the offset, we say the vector is optional
            if (_parameters.size()==4) {
                res.push_back(END_OF_VARIES_PARAMS());
            }
            res.push_back(PARAM_CONSTANT("int64"));
        }

        // after the offset vector
        else {
            assert(_parameters.size() == schemas[0].getDimensions().size() + 4);
            res.push_back(END_OF_VARIES_PARAMS());
        }

        return res;
    }

    public:
    /**
     * The schema of output array is the same as input
     */
    ArrayDesc inferSchema(std::vector< ArrayDesc> inputSchemas, std::shared_ptr< Query> query)
    {
        assert(inputSchemas.size() == 1);
        ArrayDesc const& desc = inputSchemas[0];

        //validate the partitioning schema
        const PartitioningSchema ps = (PartitioningSchema)
            evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(), query, TID_INT32).getInt32();
        if (! isValidPartitioningSchema(ps, false)) // false = not allow optional data associated with the partitioning schema
        {
            throw USER_EXCEPTION(SCIDB_SE_REDISTRIBUTE, SCIDB_LE_REDISTRIBUTE_ERROR);
        }

        // get the name of the supplied result array
        const std::string& resultArrayName = desc.getName();

        if (isDebug()) {
            if (_parameters.size() >= 4) {
                assert(_parameters[3]->getParamType() == PARAM_LOGICAL_EXPRESSION);
                OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[3].get());
                SCIDB_ASSERT(lExp->isConstant());
                assert(lExp->getExpectedType()==TypeLibrary::getType(TID_BOOL));
            }
        }
        return ArrayDesc(resultArrayName, desc.getAttributes(), desc.getDimensions(), defaultPartitioning());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalTestSG, "test_sg");

} //namespace
