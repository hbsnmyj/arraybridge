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
 * GetNamespaceIdFromArrayId.h
 *
 *  Created on: May 27, 2015
 *      Author: mcorbett@paradigm4.com
 */
#ifndef _GET_NAMESPACE_ID_FROM_ARRAY_UAID_
#define _GET_NAMESPACE_ID_FROM_ARRAY_UAID_

#include <string>
#include <array/Metadata.h>
#include <usr_namespace/NamespaceDesc.h>

#include <pqxx/transaction>
#include <pqxx/connection>


using namespace pqxx;
using namespace pqxx::prepare;


namespace scidb
{
    class GetNamespaceIdFromArrayUAId
    {
    private:
        const scidb::ArrayUAID          _arrayUAId;
        scidb::NamespaceDesc::ID        _namespaceId;

    public:
        /**
         * @brief Constructor
         * @param arrayUAId - The unique id for the array
         */
        GetNamespaceIdFromArrayUAId(
            const scidb::ArrayUAID &          arrayUAId);

        /**
         * Retrieve the id of the namespace.
         */
        const scidb::NamespaceDesc::ID &getNamespaceId() const
        {
            return _namespaceId;
        }

        /**
         * @brief execute the sql function
         * @param serialize - true=serialize the sql transaction, false=otherwise
         */
        virtual void execute(bool serialize = true);

        /**
         * @brief Interacts with the system catalog to retrieve the id
         * @param connection - postgres connection
         * @param tr - postgres transaction handle
         */
         virtual void operator()(
                pqxx::connection *          connection,
                pqxx::basic_transaction *   tr);
    };
} //namespace scidb


#endif // _GET_NAMESPACE_ID_FROM_ARRAY_UAID_
