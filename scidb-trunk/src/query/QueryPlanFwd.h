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
#ifndef QUERYPLANFWD_H
#define QUERYPLANFWD_H

namespace std {
template<typename _Tp> class shared_ptr;
}  // std

namespace scidb {
class LogicalPlan;
class PhysicalPlan;
class PhysicalQueryPlanNode;
class LogicalQueryPlanNode;
typedef std::shared_ptr<PhysicalPlan> PhysPlanPtr;
typedef std::shared_ptr<PhysicalQueryPlanNode> PhysNodePtr;

}  // scidb

#endif
