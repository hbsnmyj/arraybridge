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

#include <query/Operator.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

using namespace std;
using namespace boost;

/**
 * @brief The operator: test_root_arena().
 *
 * @par Synopsis:
 *   test_root_arena( size )
 *
 * @par Summary:
 *   Allocates - and leaks - a block of memory from the root arena of each instance.
 *
 * @par Input:
 *      - size: the number of bytes to allocate from the root arena.
 *
 * @par Output array:
 *        <
 *   <br>   dummy_attribute: bool
 *   <br> >
 *   <br> [
 *   <br>   dummy_dimension = 0:0,0,0
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
struct UnitTestRootArenaLogical : LogicalOperator
{
    UnitTestRootArenaLogical(const string& name,const string& alias)
        : LogicalOperator(name,alias)
    {
        ADD_PARAM_CONSTANT(TID_UINT64);                  // Allocation size
    }

    ArrayDesc inferSchema(vector<ArrayDesc>,std::shared_ptr<Query>)
    {
        return ArrayDesc("array",
            Attributes(1,AttributeDesc(0,"a",TID_BOOL,0,0)),
            Dimensions(1,DimensionDesc("i",0,0,0,0)),
            defaultPartitioning());
    }
};

struct UnitTestRootArenaPhysical : PhysicalOperator
{
    UnitTestRootArenaPhysical(const string& lname,const string& pname,const Parameters& params,const ArrayDesc& schema)
        : PhysicalOperator(lname,pname,params,schema)
    {}

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >&,std::shared_ptr<Query> query)
    {
        size_t size = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().get<size_t>();

        arena::getArena()->malloc(size);

        return make_shared<MemArray>(_schema,query);
    }
};

/****************************************************************************/

REGISTER_LOGICAL_OPERATOR_FACTORY (UnitTestRootArenaLogical, "test_root_arena");
REGISTER_PHYSICAL_OPERATOR_FACTORY(UnitTestRootArenaPhysical,"test_root_arena","UnitTestRootArenaPhysical");

/****************************************************************************/
}
/****************************************************************************/
