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
 * SecurityCommunicator.cpp
 *
 *  Created on: May 19, 2015
 *      Author: mcorbett@paradigm4.com
 */

#include <usr_namespace/SecurityCommunicator.h>


#include "log4cxx/logger.h"
#include <string>
#include "util/PluginManager.h"
#include <memory>
#include <boost/assign.hpp>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <usr_namespace/ClientCommunicator.h>
#include <usr_namespace/UserDesc.h>
#include <util/session/Session.h>

namespace scidb
{

    namespace security
    {
        static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.securityPluginComm"));

        // -------------------------------------------------------------
        Communicator::Communicator()
            : _currentState(STATE_LOGIN_REQUEST)
        {

        }

        // -------------------------------------------------------------
        Communicator::~Communicator()
        {
            _currentState = STATE_LOGIN_REQUEST;
        }

        // -------------------------------------------------------------
        bool Communicator::isAuthenticated() const
        {
            return (_currentState == STATE_AUTHENTICATE_SUCCEEDED)
                ? true : false;
        }


        // -------------------------------------------------------------
        void Communicator::getAuthorization(
            std::shared_ptr<Session> &session,
            int maxTries /* = 1 */)
        {
            int retval;

            ASSERT_EXCEPTION(session.get()!=nullptr, "NULL session");

            if(isAuthenticated())
            {
                return;
            }

            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_getAuthorization",    // const std::string& name
                boost::assign::list_of  // const std::vector<TypeId>& inputArgTypes
                    (TID_UINT32)        //   maxTries
                    (TID_BINARY),       //   *session
                func,                   // FunctionDescription& funcDescription
                convs,                  // std::vector<FunctionPointer>& converters
                false);                 // bool tile );
            if(!func.getFuncPtr()) {
                throw SYSTEM_EXCEPTION(
                    SCIDB_SE_NETWORK,
                    SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                    << "namespaces";
            }

            Value inputParams[2] = {
                Value(TypeLibrary::getType(TID_UINT32)),
                Value(TypeLibrary::getType(TID_BINARY))};

            ASSERT_EXCEPTION(
                session.get()!=nullptr,
                "NULL session");

            LOG4CXX_DEBUG(logger,
                "PluginCommunicator::getAuthorization"
                << "  maxTries=" << maxTries
                << "  session *=" << session.get() );

            Session *pSession = session.get();
            ASSERT_EXCEPTION(pSession, "NULL pSession");

            inputParams[0].setUint32(maxTries);
            inputParams[1].setData(
                &pSession,
                sizeof(Session *));

            const Value* vInputParams[2] = {
                &inputParams[0],
                &inputParams[1]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);

            // If the return from libnamespaces.getAuthorization is 0
            // then it succeeded.  Otherwise, it failed.
            retval = returnParams.getInt32();
            if(0 == retval)
            {
                 _currentState = STATE_AUTHENTICATE_SUCCEEDED;
            } else {
                session->invalidateUser();

                LOG4CXX_DEBUG(logger,
                    "PluginCommunicator::getAuthorization failed"
                        << " retval=" << retval);

                throw SYSTEM_EXCEPTION(
                    SCIDB_SE_NETWORK,
                    SCIDB_LE_AUTHENTICATION_ERROR);
            }
        }

        bool Communicator::checkSecurityPermissions(
            const std::shared_ptr<scidb::Session> &         session,
            const std::string &                             permissions)
        {
            std::vector<FunctionPointer> convs;
            FunctionDescription func;

            FunctionLibrary::getInstance()->findFunction(
                "_checkSecurityPermissions",// const std::string& name
                boost::assign::list_of      // const std::vector<TypeId>& inputArgTypes
                    (TID_BINARY)            //   in - const std::shared_ptr<scidb::Session> *
                    (TID_BINARY),           //   in - std::string *
                func,                       // FunctionDescription& funcDescription
                convs,                      // std::vector<FunctionPointer>& converters
                false);                     // bool tile );

            if(!func.getFuncPtr())
            {
                return true;
            }

            Value inputParams[] = {
                Value(TypeLibrary::getType(TID_BINARY)),    // pSession
                Value(TypeLibrary::getType(TID_BINARY))};   // pPermissions


            const std::shared_ptr<scidb::Session> * pSession        = &session;
            const std::string *                     pPermissions    = &permissions;

            inputParams[0].setData(&pSession,       sizeof(pSession));
            inputParams[1].setData(&pPermissions,   sizeof(pPermissions));


            const Value* vInputParams[] = {
                &inputParams[0],
                &inputParams[1]};

            Value returnParams(TypeLibrary::getType(TID_INT32));
            func.getFuncPtr()(vInputParams, &returnParams, NULL);

            // If the return from libnamespaces.checkArrayAccess is 0
            // then it succeeded.  Otherwise, it failed.
            int retval = returnParams.getInt32();
            return ((0 == retval) ? true : false);
        }

    } // namespace security
} // namespace scidb
