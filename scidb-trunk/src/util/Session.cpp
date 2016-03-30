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
 * Session.cpp
 *
 *  Created on: May 20, 2015
 *      Author: mcorbett@paradigm4.com
 */

#include <util/session/Session.h>

#include <log4cxx/logger.h>
#include <system/SystemCatalog.h>
#include <usr_namespace/NamespacesCommunicator.h>
#include <usr_namespace/SecurityCommunicator.h>
#include <util/Mutex.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger( log4cxx::Logger::getLogger("scidb.session"));

    Session::Session()
        : _securityMode("Uninitialized")
        , _currentNamespaceDesc(NamespaceDesc("public"))
        , _defaultNamespaceDesc(NamespaceDesc("public"))
        , _authenticationState(AUTHENTICATION_STATE_E_NOT_INITIALIZED)
    {
        _validSecurityModes.insert("trust");
        _validSecurityModes.insert("password");
    }

    bool Session::isValidSecurityMode(const std::string & mode) const
    {
        ValidSecurityModes::const_iterator it =
            _validSecurityModes.find(mode);

        if(it == _validSecurityModes.end())
        {
            return false;
        }

        return true;
    }

    void Session::setSecurityMode(const std::string & securityMode)
    {
        if(isValidSecurityMode(securityMode))
        {
            _securityMode = securityMode;
            return;
        }
        else
        {
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_NETWORK,
                SCIDB_LE_AUTHENTICATION_ERROR)
                  << "securityMode";
        }
    }

    const std::string &Session::getSecurityMode() const
    {
        return _securityMode;
    }


    void Session::setAuthenticatedState(AUTHENTICATION_STATE_E newState)
    {
        ScopedMutexLock lock(_lockAuthenticationState);

        AUTHENTICATION_STATE_E &currState = _authenticationState;
        switch(newState)
        {
            case AUTHENTICATION_STATE_E_NOT_INITIALIZED:
            {
                setAuthenticatedState(
                    AUTHENTICATION_STATE_E_NOT_AUTHORIZED);

                throw SYSTEM_EXCEPTION(
                    SCIDB_SE_NETWORK,
                    SCIDB_LE_AUTHENTICATION_ERROR)
                        << "Not initialized";
            } break;

            case AUTHENTICATION_STATE_E_AUTHORIZING:
            {
                if(currState != AUTHENTICATION_STATE_E_NOT_INITIALIZED)
                {
                    setAuthenticatedState(
                        AUTHENTICATION_STATE_E_NOT_AUTHORIZED);

                    throw SYSTEM_EXCEPTION(
                        SCIDB_SE_NETWORK,
                        SCIDB_LE_AUTHENTICATION_ERROR)
                            << "currentState !Init";
                }
                currState = newState;
            } break;

            case AUTHENTICATION_STATE_E_AUTHORIZED:
            {
                if(currState != AUTHENTICATION_STATE_E_AUTHORIZING)
                {
                    setAuthenticatedState(
                        AUTHENTICATION_STATE_E_NOT_AUTHORIZED);

                    throw SYSTEM_EXCEPTION(
                        SCIDB_SE_NETWORK,
                        SCIDB_LE_AUTHENTICATION_ERROR)
                            << "currentState !Authorizing";
                }
                currState = newState;
            } break;

            case AUTHENTICATION_STATE_E_NOT_AUTHORIZED:
            {
                invalidateUser();
                currState = newState;
                return;
            } break;
        }
    }

    Session::AUTHENTICATION_STATE_E Session::getAuthenticatedState()
    {
        ScopedMutexLock lock(_lockAuthenticationState);
        return _authenticationState;
    }

    void Session::setNamespace(
        const NamespaceDesc &namespaceDesc)
    {
        _currentNamespaceDesc = namespaceDesc;
        NamespaceDesc::ID namespaceID;

        SystemCatalog::getInstance()->findNamespace(
            _currentNamespaceDesc, namespaceID);
        _currentNamespaceDesc.setId(namespaceID);
    }

    NamespaceDesc &Session::getNamespace()
    {
        return _currentNamespaceDesc;
    }

    const NamespaceDesc &Session::getDefaultNamespace() const
    {
        return _defaultNamespaceDesc;
    }

    void Session::invalidateNamespace()
    {
        _currentNamespaceDesc = NamespaceDesc();
    }



    void Session::setUser(const scidb::UserDesc &userDesc)
    {
        _userDesc = userDesc;

        scidb::RoleDesc desc(userDesc.getName());
        setRole(desc);
    }

    const scidb::UserDesc &Session::getUser() const
    {
        return _userDesc;
    }

    void Session::changeUser(const scidb::UserDesc &newUser)
    {
        setUser(newUser);
    }

    void Session::invalidateUser()
    {
        scidb::UserDesc userDesc = scidb::UserDesc();
        setUser(userDesc);
    }


    void Session::setRole(const scidb::RoleDesc &roleDesc)
    {
        // todo:  determine if role is valid first?
        LOG4CXX_DEBUG(logger, "Session::setRole(" << roleDesc.getName() << ")");
        _roleDesc = roleDesc;
    }

    const scidb::RoleDesc &Session::getRole() const
    {
        return _roleDesc;
    }


    void Session::setSecurityCommunicator(
        const std::shared_ptr<security::Communicator> &
            securityCommunicator)
    {
        ScopedMutexLock lock(_lockAuthenticationState);

        if( getAuthenticatedState() !=
            AUTHENTICATION_STATE_E_AUTHORIZING )
        {
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_NETWORK,
                SCIDB_LE_AUTHENTICATION_ERROR)
                  << "getAuthenticatedState() !Authorizing";
        }


        if( _securityCommunicator != NULL) // test old value
        {
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_NETWORK,
                SCIDB_LE_AUTHENTICATION_ERROR)
                   << "_ security communicator";
       }

        if(securityCommunicator == NULL)    // test new value
        {
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_NETWORK,
                SCIDB_LE_AUTHENTICATION_ERROR)
                   << "security communicator";
       }

        _securityCommunicator = securityCommunicator;
    }

    std::shared_ptr<security::Communicator>
        Session::getSecurityCommunicator() const
    {
        return _securityCommunicator;
    }


    void Session::setNamespacesCommunicator(
        const std::shared_ptr<namespaces::Communicator> &
            namespacesCommunicator)
    {
        ScopedMutexLock lock(_lockAuthenticationState);

        if( getAuthenticatedState() !=
            AUTHENTICATION_STATE_E_AUTHORIZING )
        {
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_NETWORK,
                SCIDB_LE_AUTHENTICATION_ERROR)
                   << "getAuthenticatedState() !Authorizing";
        }


        if( _namespacesCommunicator != NULL) // test old value
        {
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_NETWORK,
                SCIDB_LE_AUTHENTICATION_ERROR)
                  << "_ namespaces communicator";
        }

        if(namespacesCommunicator == NULL)    // test new value
        {
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_NETWORK,
                SCIDB_LE_AUTHENTICATION_ERROR)
                  << "namespaces communicator";
        }

        _namespacesCommunicator = namespacesCommunicator;

        // -- Initialize namespace IDs
        NamespaceDesc::ID namespaceID;

        SystemCatalog::getInstance()->findNamespace(
            _defaultNamespaceDesc, namespaceID, false);
        _defaultNamespaceDesc.setId(namespaceID);

        SystemCatalog::getInstance()->findNamespace(
            _currentNamespaceDesc, namespaceID, false);
        _currentNamespaceDesc.setId(namespaceID);
    }

    std::shared_ptr<namespaces::Communicator>
        Session::getNamespacesCommunicator() const
    {
        return _namespacesCommunicator;
    }


    void Session::setClientCommunicator(
        const std::shared_ptr<ClientCommunicator> &
            clientCommunicator)
    {
        if( getAuthenticatedState() !=
            AUTHENTICATION_STATE_E_AUTHORIZING )
        {
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_NETWORK,
                SCIDB_LE_AUTHENTICATION_ERROR)
                   << "getAuthenticatedState() !Authorizing";
       }

        if(_clientCommunicator != NULL)    // test old value
        {
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_NETWORK,
                SCIDB_LE_AUTHENTICATION_ERROR)
                    << "_ client communicator";
        }

        if(clientCommunicator == NULL)    // test new value
        {
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_NETWORK,
                SCIDB_LE_AUTHENTICATION_ERROR)
                    << "_ client communicator";
        }

       _clientCommunicator = clientCommunicator;
    }

    std::shared_ptr<ClientCommunicator>
        Session::getClientCommunicator() const
    {
        return _clientCommunicator;
    }

}  // namespace scidb
