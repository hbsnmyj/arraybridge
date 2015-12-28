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
 * NamespacesCommunicator.h
 *
 *  Created on: May 8, 2015
 *      Author: mcorbett@paradigm4.com
 */

#ifndef NAMESPACE_PLUGIN_COMMUNICATOR_H_
#define NAMESPACE_PLUGIN_COMMUNICATOR_H_

#include <memory>


#include "log4cxx/logger.h"
#include <string>
#include <util/PluginManager.h>
#include <boost/assign.hpp>
#include <memory>
#include <pqxx/transaction>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <usr_namespace/NamespaceDesc.h>
#include <util/session/Session.h>
#include <system/SystemCatalog.h>

namespace pqxx
{
// forward declaration of pqxx::connection
    class connect_direct;
    template<typename T> class basic_connection;
    typedef basic_connection<connect_direct> connection;
}

namespace scidb
{
    class Session;

    typedef uint64_t ArrayID;


    namespace namespaces
    {
        static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

        class Communicator
        {
        private:

        // -------------------------------------------------------------
        public:  // Methods

            /**
             * Constructor
             */
            Communicator() { }

            static void updateNamespaceId(
                NamespaceDesc &  namespaceDesc)
            {
                NamespaceDesc::ID namespaceID = namespaceDesc.getId();
                if(-1 == namespaceID)
                {
                    SystemCatalog::getInstance()->findNamespace(
                        namespaceDesc.getName(),
                        namespaceID,
                        false);  // do not throw on error

                    namespaceDesc.setId(namespaceID);
                }
            }


            static bool checkArrayAccess(
                std::shared_ptr<Session> &session,
                ArrayID arrayId )
            {
                Session *pSession = session.get();
                ASSERT_EXCEPTION(pSession!=nullptr, "NULL session");

                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "checkArrayAccess",     // const std::string& name
                    boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                        (TID_UINT64)        //   arrayID
                        (TID_BINARY),       //   Session *
                    func,                   // FunctionDescription& funcDescription
                    convs,                  // std::vector<FunctionPointer>& converters
                    false);                 // bool tile );
                if(!func.getFuncPtr())
                {
                    try
                    {
                        // If we are able to convert the arrayId into
                        // a namespaceId then the array exists.
                        NamespaceDesc::ID namespaceId;
                        SystemCatalog::getInstance()->getNamespaceIdFromArrayId(
                            arrayId, namespaceId);
                        return true;
                    }  catch (SystemException& e) {
                        if (e.getLongErrorCode() == SCIDB_LE_ARRAYID_DOESNT_EXIST)
                        {
                            return false;
                        }
                        throw;
                    }
                }

                Value inputParams[2] = {
                    Value(TypeLibrary::getType(TID_UINT64)),
                    Value(TypeLibrary::getType(TID_BINARY))};



                inputParams[0].setUint64(arrayId);
                inputParams[1].setData(&pSession,sizeof(Session *));

                const Value* vInputParams[2] = {
                    &inputParams[0],
                    &inputParams[1]};

                Value returnParams(TypeLibrary::getType(TID_INT32));
                func.getFuncPtr()(vInputParams, &returnParams, NULL);

                // If the return from libnamespaces.checkArrayAccess is 0
                // then it succeeded.  Otherwise, it failed.
                int retval = returnParams.getInt32();
                return ((0 == retval) ? true : false);
            }

            static bool findNamespaceTr(
                pqxx::connection *          connection,
                const NamespaceDesc &       namespaceDesc,
                NamespaceDesc::ID &         namespaceID,
                pqxx::basic_transaction *   tr)
            {
                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "findNamespaceTr",      // const std::string& name
                    boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                        (TID_BINARY)                     //   in  - pqxx::connnection *
                        (TID_BINARY)                     //   in  - const NamespaceDesc *
                        (TID_BINARY)                     //   out - NamespaceDesc::ID *
                        (TID_BINARY),                    //   in  - pqxx::basic_transaction *
                    func,                   // FunctionDescription& funcDescription
                    convs,                  // std::vector<FunctionPointer>& converters
                    false);                 // bool tile );
                if(!func.getFuncPtr())
                {
                    return false;
                }

                Value inputParams[4] = {
                    Value(TypeLibrary::getType(TID_BINARY)),
                    Value(TypeLibrary::getType(TID_BINARY)),
                    Value(TypeLibrary::getType(TID_BINARY)),
                    Value(TypeLibrary::getType(TID_BINARY))};


                const NamespaceDesc *   pNamespaceDesc  = &namespaceDesc;
                NamespaceDesc::ID *     pNamespaceId    = &namespaceID;

                inputParams[0].setData(&connection,     sizeof(connection));
                inputParams[1].setData(&pNamespaceDesc, sizeof(pNamespaceDesc));
                inputParams[2].setData(&pNamespaceId,   sizeof(pNamespaceId));
                inputParams[3].setData(&tr,             sizeof(tr));

                const Value* vInputParams[4] = {
                    &inputParams[0],
                    &inputParams[1],
                    &inputParams[2],
                    &inputParams[3]};

                Value returnParams(TypeLibrary::getType(TID_INT32));
                func.getFuncPtr()(vInputParams, &returnParams, NULL);

                // If the return from libnamespaces.checkArrayAccess is 0
                // then it succeeded.  Otherwise, it failed.
                int retval = returnParams.getInt32();
                return ((0 == retval) ? true : false);
            }


            static bool addArrayToNamespaceTr(
                pqxx::connection *          connection,
                const NamespaceDesc &       namespaceDesc,
                NamespaceDesc::ID &         namespaceID,
                const std::string &         arrayName,
                const ArrayID               arrayId,
                pqxx::basic_transaction *   tr)
            {
                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "addArrayToNamespaceTr",      // const std::string& name
                    boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                        (TID_INT64)           //   in - NamespaceDesc::ID     namespaceId
                        (TID_UINT64)          //   in - const ArrayID         arrayId
                        (TID_BINARY)          //   in - pqxx::connection *    connection
                        (TID_BINARY)          //   in - const NamespaceDesc * pNamespaceDesc
                        (TID_BINARY)          //   in - const std::string *   pArrayName
                        (TID_BINARY),         //   in - pqxx::basic_transaction * tr
                    func,                   // FunctionDescription& funcDescription
                    convs,                  // std::vector<FunctionPointer>& converters
                    false);                 // bool tile );

                if(!func.getFuncPtr())
                {
                    return false;
                }

                Value inputParams[6] = {
                    Value(TypeLibrary::getType(TID_INT64)),   // namespaceId
                    Value(TypeLibrary::getType(TID_UINT64)),  // arrayId
                    Value(TypeLibrary::getType(TID_BINARY)),  // connection
                    Value(TypeLibrary::getType(TID_BINARY)),  // pNamespaceDesc
                    Value(TypeLibrary::getType(TID_BINARY)),  // pArrayName
                    Value(TypeLibrary::getType(TID_BINARY))}; // tr

                const NamespaceDesc *   pNamespaceDesc  = &namespaceDesc;
                const std::string *     pArrayName      = &arrayName;
                inputParams[0].setInt64(namespaceID);
                inputParams[1].setUint64(arrayId);
                inputParams[2].setData(&connection,     sizeof(connection));
                inputParams[3].setData(&pNamespaceDesc, sizeof(pNamespaceDesc));
                inputParams[4].setData(&pArrayName,     sizeof(pArrayName));
                inputParams[5].setData(&tr,             sizeof(tr));

                const Value* vInputParams[6] = {
                    &inputParams[0],
                    &inputParams[1],
                    &inputParams[2],
                    &inputParams[3],
                    &inputParams[4],
                    &inputParams[5]};

                Value returnParams(TypeLibrary::getType(TID_INT32));
                func.getFuncPtr()(vInputParams, &returnParams, NULL);

                // If the return from libnamespaces.checkArrayAccess is 0
                // then it succeeded.  Otherwise, it failed.
                int retval = returnParams.getInt32();
                return ((0 == retval) ? true : false);
            }

            static bool createNamespaceTr(
                pqxx::connection *          connection,
                const NamespaceDesc &       namespaceDesc,
                pqxx::basic_transaction *   tr)
            {
                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "createNamespaceTr",      // const std::string& name
                    boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                        (TID_BINARY)          //   in - pqxx::connection *    connection
                        (TID_BINARY)          //   in - const NamespaceDesc * pNamespaceDesc
                        (TID_BINARY),         //   in - pqxx::basic_transaction * tr
                    func,                   // FunctionDescription& funcDescription
                    convs,                  // std::vector<FunctionPointer>& converters
                    false);                 // bool tile );

                if(!func.getFuncPtr())
                {
                    return false;
                }

                Value inputParams[3] = {
                    Value(TypeLibrary::getType(TID_BINARY)),  // connection
                    Value(TypeLibrary::getType(TID_BINARY)),  // pNamespaceDesc
                    Value(TypeLibrary::getType(TID_BINARY))}; // tr

                const NamespaceDesc *pNamespaceDesc = &namespaceDesc;
                inputParams[0].setData(&connection,     sizeof(connection));
                inputParams[1].setData(&pNamespaceDesc, sizeof(pNamespaceDesc));
                inputParams[2].setData(&tr,             sizeof(tr));

                const Value* vInputParams[3] = {
                    &inputParams[0],
                    &inputParams[1],
                    &inputParams[2]};

                Value returnParams(TypeLibrary::getType(TID_INT32));
                func.getFuncPtr()(vInputParams, &returnParams, NULL);

                // If the return from libnamespaces.checkArrayAccess is 0
                // then it succeeded.  Otherwise, it failed.
                int retval = returnParams.getInt32();
                return ((0 == retval) ? true : false);
            }

            static bool dropNamespaceTr(
                pqxx::connection *          connection,
                const NamespaceDesc &       namespaceDesc,
                pqxx::basic_transaction *   tr)
            {
                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "dropNamespaceTr",        // const std::string& name
                    boost::assign::list_of    // const std::vector<TypeId>& inputArgTypes
                        (TID_BINARY)          //   in - pqxx::connection *    connection
                        (TID_BINARY)          //   in - const NamespaceDesc * pNamespaceDesc
                        (TID_BINARY),         //   in - pqxx::basic_transaction * tr
                    func,                   // FunctionDescription& funcDescription
                    convs,                  // std::vector<FunctionPointer>& converters
                    false);                 // bool tile );

                if(!func.getFuncPtr())
                {
                    return false;
                }

                Value inputParams[3] = {
                    Value(TypeLibrary::getType(TID_BINARY)),  // connection
                    Value(TypeLibrary::getType(TID_BINARY)),  // pNamespaceDesc
                    Value(TypeLibrary::getType(TID_BINARY))}; // tr

                const NamespaceDesc *pNamespaceDesc = &namespaceDesc;
                inputParams[0].setData(&connection,     sizeof(connection));
                inputParams[1].setData(&pNamespaceDesc, sizeof(pNamespaceDesc));
                inputParams[2].setData(&tr,             sizeof(tr));

                const Value* vInputParams[3] = {
                    &inputParams[0],
                    &inputParams[1],
                    &inputParams[2]};

                Value returnParams(TypeLibrary::getType(TID_INT32));
                func.getFuncPtr()(vInputParams, &returnParams, NULL);

                // If the return from libnamespaces.checkArrayAccess is 0
                // then it succeeded.  Otherwise, it failed.
                int retval = returnParams.getInt32();
                return ((0 == retval) ? true : false);
            }


            static bool getNamespaceIdFromArrayIdTr(
                pqxx::connection *              connection,
                const ArrayID                   arrayId,
                scidb::NamespaceDesc::ID &      namespaceId,
                pqxx::basic_transaction*        tr)
            {
                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "getNamespaceIdFromArrayIdTr",      // const std::string& name
                    boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                        (TID_BINARY)                     //   in  - pqxx::connnection *
                        (TID_UINT64)                     //   in  - const ArrayID
                        (TID_BINARY)                     //   out - NamespaceDesc::ID *
                        (TID_BINARY),                    //   in  - pqxx::basic_transaction *
                    func,                   // FunctionDescription& funcDescription
                    convs,                  // std::vector<FunctionPointer>& converters
                    false);                 // bool tile );
                if(!func.getFuncPtr())
                {
                    return false;
                }

                Value inputParams[4] = {
                    Value(TypeLibrary::getType(TID_BINARY)),
                    Value(TypeLibrary::getType(TID_BINARY)),
                    Value(TypeLibrary::getType(TID_BINARY)),
                    Value(TypeLibrary::getType(TID_BINARY))};



                NamespaceDesc::ID *pNamespaceId = &namespaceId;

                inputParams[0].setData(&connection,     sizeof(connection));
                inputParams[1].setUint64(arrayId);
                inputParams[2].setData(&pNamespaceId,   sizeof(pNamespaceId));
                inputParams[3].setData(&tr,             sizeof(tr));

                const Value* vInputParams[4] = {
                    &inputParams[0],
                    &inputParams[1],
                    &inputParams[2],
                    &inputParams[3]};

                Value returnParams(TypeLibrary::getType(TID_INT32));
                func.getFuncPtr()(vInputParams, &returnParams, NULL);

                // If the return from libnamespaces.checkArrayAccess is 0
                // then it succeeded.  Otherwise, it failed.
                int retval = returnParams.getInt32();
                return ((0 == retval) ? true : false);
            }

            static bool getNamespacesTr(
                pqxx::connection *              connection,
                std::vector<NamespaceDesc> &    namespaces,
                pqxx::basic_transaction *       tr)
            {
                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "getNamespacesTr",        // const std::string& name
                    boost::assign::list_of    // const std::vector<TypeId>& inputArgTypes
                        (TID_BINARY)          //   in - pqxx::connection *    connection
                        (TID_BINARY)          //   out - std::vector<NamespaceDesc> *
                        (TID_BINARY),         //   in - pqxx::basic_transaction * tr
                    func,                   // FunctionDescription& funcDescription
                    convs,                  // std::vector<FunctionPointer>& converters
                    false);                 // bool tile );

                if(!func.getFuncPtr())
                {
                    return false;
                }

                Value inputParams[3] = {
                    Value(TypeLibrary::getType(TID_BINARY)),  // connection
                    Value(TypeLibrary::getType(TID_BINARY)),  // pNamespaceDesc
                    Value(TypeLibrary::getType(TID_BINARY))}; // tr


                std::vector<NamespaceDesc> *pNamespaces = &namespaces;
                inputParams[0].setData(&connection,     sizeof(connection));
                inputParams[1].setData(&pNamespaces,    sizeof(pNamespaces));
                inputParams[2].setData(&tr,             sizeof(tr));

                const Value* vInputParams[3] = {
                    &inputParams[0],
                    &inputParams[1],
                    &inputParams[2]};

                Value returnParams(TypeLibrary::getType(TID_INT32));
                func.getFuncPtr()(vInputParams, &returnParams, NULL);

                // If the return from libnamespaces.checkArrayAccess is 0
                // then it succeeded.  Otherwise, it failed.
                int retval = returnParams.getInt32();
                return ((0 == retval) ? true : false);
            }
        };
    } // namespace namespaces
} // namespace scidb

#endif // NAMESPACE_PLUGIN_COMMUNICATOR_H_
