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
 * SecurityCommunicator.h
 *
 *  Created on: May 8, 2015
 *      Author: mcorbett@paradigm4.com
 */

#ifndef SECURITY_PLUGIN_COMMUNICATOR_H_
#define SECURITY_PLUGIN_COMMUNICATOR_H_

#include <memory>
#include <inttypes.h>
#include <string>
#include <vector>

#include <boost/assign.hpp>
#include <log4cxx/logger.h>
#include <pqxx/transaction>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <usr_namespace/UserDesc.h>


namespace pqxx
{
// forward declaration of pqxx::connection
    class connect_direct;
    template<typename T> class basic_connection;
    typedef basic_connection<connect_direct> connection;
}


namespace scidb
{
    class ClientCommunicator;
    class Session;

    namespace security
    {
        static log4cxx::LoggerPtr _logger(log4cxx::Logger::getLogger("scidb.ops.securityPluginComm"));

        class Communicator
        {
        // -------------------------------------------------------------
        public:    // Enumerations
            typedef enum
            {
                STATE_LOGIN_REQUEST = 0,
                STATE_AUTHENTICATE_SUCCEEDED
            } LOGIN_STATE;

        // -------------------------------------------------------------
        private:  // Variables
            LOGIN_STATE           _currentState;

        // -------------------------------------------------------------
        public:  // Methods

            /**
             * Constructor
             */
            Communicator();

            /**
             * Destructor
             */
            virtual ~Communicator();

            /**
             * A verification method to determine if the user was authenticated
             * @return true - authenticated, false - otherwise
             */
            bool isAuthenticated() const;

            /**
             * Communicates with the client to get the user information
             * then verifies the user and feeds back the userName and
             * the userPermissions that are allowed by that user in
             * the SciDB system.
             *
             * @param session - the current working session
             * @param maxTries - how many times should the client be queried for the pw before giving up.
             *
             * throws an exception upon failure
             */
            void getAuthorization(
                std::shared_ptr<Session> &session,
                int maxTries = 1);


            static bool createUserTr(
                pqxx::connection *          connection,
                UserDesc &                  userDesc,
                pqxx::basic_transaction *   tr)
            {
                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "createUserTr",         // const std::string& name
                    boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                        (TID_BINARY)        //   in - pqxx::connection * connection
                        (TID_BINARY)        //   in - UserDesc * pUserDesc
                        (TID_BINARY),       //   in - pqxx::basic_transaction * tr
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

                UserDesc *pUserDesc = &userDesc;
                inputParams[0].setData(&connection, sizeof(connection));
                inputParams[1].setData(&pUserDesc,  sizeof(pUserDesc));
                inputParams[2].setData(&tr,         sizeof(tr));

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

            static bool dropUserTr(
                pqxx::connection *          connection,
                const UserDesc &            userDesc,
                pqxx::basic_transaction *   tr)
            {
                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "dropUserTr",           // const std::string& name
                    boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                        (TID_BINARY)        //   in - pqxx::connection * connection
                        (TID_BINARY)        //   in - const UserDesc * pUserDesc
                        (TID_BINARY),       //   in - pqxx::basic_transaction * tr
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

                const UserDesc *pUserDesc = &userDesc;
                inputParams[0].setData(&connection, sizeof(connection));
                inputParams[1].setData(&pUserDesc,  sizeof(pUserDesc));
                inputParams[2].setData(&tr,         sizeof(tr));

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

            static bool changeUserTr(
                pqxx::connection *          connection,
                UserDesc &                  userDesc,
                const std::string &         whatToChange,
                pqxx::basic_transaction *   tr)
            {
                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "changeUserTr",         // const std::string& name
                    boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                        (TID_BINARY)        //   in - pqxx::connection * connection
                        (TID_BINARY)        //   in - UserDesc * pUserDesc
                        (TID_BINARY)        //   in - std::string * whatToChange
                        (TID_BINARY),       //   in - pqxx::basic_transaction * tr
                    func,                   // FunctionDescription& funcDescription
                    convs,                  // std::vector<FunctionPointer>& converters
                    false);                 // bool tile );

                if(!func.getFuncPtr())
                {
                    return false;
                }

                Value inputParams[4] = {
                    Value(TypeLibrary::getType(TID_BINARY)),  // connection
                    Value(TypeLibrary::getType(TID_BINARY)),  // pNamespaceDesc
                    Value(TypeLibrary::getType(TID_BINARY)),  // pWhatToChange
                    Value(TypeLibrary::getType(TID_BINARY))}; // tr

                UserDesc *pUserDesc = &userDesc;
                const std::string *pWhatToChange = &whatToChange;
                inputParams[0].setData(&connection,     sizeof(connection));
                inputParams[1].setData(&pUserDesc,      sizeof(pUserDesc));
                inputParams[2].setData(&pWhatToChange,  sizeof(pWhatToChange));
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

            static bool findUserTr(
                pqxx::connection *          connection,
                UserDesc &                  userDesc,
                pqxx::basic_transaction *   tr)
            {
                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "findUserTr",           // const std::string& name
                    boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                        (TID_BINARY)        //   in - pqxx::connection * connection
                        (TID_BINARY)        //   in - const UserDesc * pUserDesc
                        (TID_BINARY),       //   in - pqxx::basic_transaction * tr
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

                UserDesc *pUserDesc = &userDesc;
                inputParams[0].setData(&connection, sizeof(connection));
                inputParams[1].setData(&pUserDesc,  sizeof(pUserDesc));
                inputParams[2].setData(&tr,         sizeof(tr));

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

            static bool getUsersTr(
                pqxx::connection *              connection,
                std::vector<scidb::UserDesc> &  usersDescs,
                pqxx::basic_transaction *       tr)
            {
                std::vector<FunctionPointer> convs;
                FunctionDescription func;

                FunctionLibrary::getInstance()->findFunction(
                    "getUsersTr",           // const std::string& name
                    boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                        (TID_BINARY)        //   in - pqxx::connection * connection
                        (TID_BINARY)        //   in - const UserDesc * pUserDesc
                        (TID_BINARY),       //   in - pqxx::basic_transaction * tr
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

                std::vector<scidb::UserDesc> *pUserDescs = &usersDescs;
                inputParams[0].setData(&connection, sizeof(connection));
                inputParams[1].setData(&pUserDescs, sizeof(pUserDescs));
                inputParams[2].setData(&tr,         sizeof(tr));

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
        };  // class Communicator
    } // namespace security
} // namespace scidb

#endif /* SECURITY_PLUGIN_COMMUNICATOR_H_ */
