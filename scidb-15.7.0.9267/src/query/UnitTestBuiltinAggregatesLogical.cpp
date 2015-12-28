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
 * @file UnitTestDeepChunkMergeLogical.cpp
 *
 * @brief The logical operator interface for testing deep-chunk merge.
 */

#include <query/Query.h>
#include <array/Array.h>
#include <query/Operator.h>

#include <log4cxx/logger.h>



namespace scidb
{
using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.unittest"));

/**
 * @brief The operator: test_builtin_aggregates().
 *
 * @par Synopsis:
 *   test_builtin_aggregates([filename, expected_result])
 *
 * @par Summary:
 *   This operator performs unit tests for the builtin aggregates. It returns an empty string. Upon failures exceptions are thrown.
 *
 * @par Input:
 *   The input parameters may be N/A in which case the test will auto-run with random data
 *   If filename & expected_result are passed the filename is a table with 16 columns and 8192 rows
 *   of values to be fed as the srcState to approxdc->finalResult.  The expected_result should
 *   be equal to the value expected to be returned in dstState from approxdc->finalResult.
 *
 * @par Output array:
 *        <
 *   <br>   dummy_attribute: string
 *   <br> >
 *   <br> [
 *   <br>   dummy_dimension: start=end=chunk_interval=0.
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
 *
 * @author mcorbett@paradigm4.com
 *
 */
class UnitTestBuiltinAggregatesLogical: public LogicalOperator
{
public:
    UnitTestBuiltinAggregatesLogical(const string& logicalName, const std::string& alias):
    LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_VARIES();
    }

   /**
     * @brief Get a description for the next variable parameter needed
     * Allow either 0 parameters, or 2 parameters where the first is
     * a string and the second is a uint64.
     *
     * @param[in]    schemas
     *
     * @author mcorbett@paradigm4.com
     */
    vector<std::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const vector< ArrayDesc> &schemas)
    {
        vector<std::shared_ptr<OperatorParamPlaceholder> > res;

        switch(_parameters.size())
        {
            case 0:
                res.push_back(END_OF_VARIES_PARAMS());
                res.push_back(PARAM_CONSTANT(TID_STRING));
                break;

            case 1:
                res.push_back(PARAM_CONSTANT(TID_UINT64));
                break;

            case 2:
                res.push_back(END_OF_VARIES_PARAMS());
                break;
        }

        return res;
    }


   /**
     * @brief Create an array descriptor for this query
     *
     * @param[in]    schemas
     * @param[in]    query
     *
     * @return Array descriptor
     *
     * @author mcorbett@paradigm4.com
     */
    ArrayDesc inferSchema(std::vector<ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        static const Coordinate     in_start            = 0;
        static const Coordinate     in_end              = 1;
        static const int64_t        in_chunkInterval    = 1;
        static TypeId const &       type                = TID_UINT64;

        vector<AttributeDesc> attributes(1);
        attributes[0] = AttributeDesc((AttributeID)0, "val_approxdc",  type, 0, 0);
        vector<DimensionDesc> dimensions(1);
        dimensions[0] = DimensionDesc(string("i"), in_start, in_end, in_chunkInterval, uint32_t(0));
        return ArrayDesc("dummy_array", attributes, dimensions, defaultPartitioning());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(UnitTestBuiltinAggregatesLogical, "test_builtin_aggregates");
}  // namespace scidb
