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
 * GetNamespaceIdFromArrayUAId.cpp
 *
 *  Created on: May 27, 2015
 *      Author: mcorbett@paradigm4.com
 */
#include <system/GetNamespaceIdFromArrayUAId.h>


#include <string>
#include <boost/bind.hpp>
#include <usr_namespace/NamespaceDesc.h>
#include <log4cxx/logger.h>
#include <system/ErrorsLibrary.h>
#include <system/SystemCatalog.h>
#include <util/Pqxx.h>

using namespace pqxx;
using namespace pqxx::prepare;
//using namespace scidb;
//using namespace std;


namespace scidb
{
    GetNamespaceIdFromArrayUAId::GetNamespaceIdFromArrayUAId(
        const scidb::ArrayUAID &          arrayUAId)
        : _arrayUAId(arrayUAId)
    {

    }

    void GetNamespaceIdFromArrayUAId::execute(bool serialize /* = true */)
    {
        SystemCatalog::Action boost_operator = boost::bind(
            &GetNamespaceIdFromArrayUAId::operator(), this, _1, _2);

        scidb::SystemCatalog::getInstance()->execute(
            boost_operator, serialize);
    }

    void GetNamespaceIdFromArrayUAId::operator()(
        pqxx::connection *              connection,
        pqxx::basic_transaction *       tr)
    {
        std::string sql = "select namespace_id from \"namespace_members\" where array_id = $1";
        connection->prepare(sql, sql) PQXX_DECL("bigint", treat_direct);

        result query_res = tr->prepared(sql)(_arrayUAId).exec();
        if (query_res.size() <= 0)
        {
            sql = "select id from \"array\" where id = $1";
            connection->prepare(sql, sql)
                PQXX_DECL("bigint", treat_direct);
            query_res = tr->prepared(sql)(_arrayUAId).exec();
            if (query_res.size() <= 0) {
                throw SYSTEM_EXCEPTION(
                    SCIDB_SE_SYSCAT,
                    SCIDB_LE_ARRAYID_DOESNT_EXIST)
                        << _arrayUAId;
            }

            _namespaceId = scidb::NamespaceDesc::getPublicNamespaceId();
        } else {
            _namespaceId =
                query_res[0].at("namespace_id").as(int64_t());
        }
    }
} // namespace scidb
