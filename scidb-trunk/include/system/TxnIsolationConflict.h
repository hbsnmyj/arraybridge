/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2010-2015 SciDB, Inc.
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
 * TxnIsolationConflict.h
 */


#ifndef __TXN_ISOLATION_CONFLICT_H__
#define __TXN_ISOLATION_CONFLICT_H__

#include <pqxx/except>


class TxnIsolationConflict : public pqxx::sql_error
{
public:
    explicit TxnIsolationConflict(
        const std::string& what,
        const std::string& query)
        : sql_error(what, query)
    {

    }
};

#endif // __TXN_ISOLATION_CONFLICT_H__

