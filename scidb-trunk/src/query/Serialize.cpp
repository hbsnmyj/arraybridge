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


/**
 * @file
 *
 * @brief Routines for serializing physical plans to strings.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <query/Serialize.h>

#include <query/Expression.h>
#include <query/LogicalExpression.h>
#include <query/Operator.h>
#include <query/QueryPlan.h>

#include <boost/format.hpp>
#include <boost/foreach.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include <sstream>

using namespace std;
using namespace boost::archive;

namespace scidb
{

string serializePhysicalPlan(const std::shared_ptr<PhysicalPlan> &plan)
{
    stringstream ss;
    text_oarchive oa(ss);

    const std::shared_ptr<PhysicalQueryPlanNode> &queryRoot = plan->getRoot();

    registerLeafDerivedOperatorParams<text_oarchive>(oa);

    PhysicalQueryPlanNode* n = queryRoot.get();
    oa & n;

    return ss.str();
}

//
// There is no deserializePhysicalPlan here.  Deserialization of
// physical plans happens in QueryProcessor::parsePhysical(), by way
// of an mtPreparePhysicalPlan message and
// ServerMessageHandleJob::handlePreparePhysicalPlan().
//

string serializePhysicalExpression(const Expression &expr)
{
    stringstream ss;
    text_oarchive oa(ss);

    oa & expr;

    return ss.str();
}

Expression deserializePhysicalExpression(const string &str)
{
    Expression expr;
    stringstream ss;
    ss << str;
    text_iarchive ia(ss);
    ia & expr;

    return expr;
}


} //namespace scidb
