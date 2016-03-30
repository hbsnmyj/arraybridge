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
 * Session.h
 *
 *  Created on: May 20, 2015
 *      Author: mcorbett@paradigm4.com
 */

#ifndef SESSION_H_
#define SESSION_H_

#include <memory>
#include <string>
#include <vector>
#include <set>

#include <usr_namespace/NamespaceDesc.h>
#include <usr_namespace/RoleDesc.h>
#include <usr_namespace/UserDesc.h>
#include <util/Mutex.h>


namespace scidb
{
    class ClientCommunicator;
    class Connection;

    namespace security   { class Communicator; }
    namespace namespaces { class Communicator; }

    /**
     * The Session object is normally stored in the connection and keeps
     * track of things that are associated with the lifetime of the
     * connection.
     */
    class Session
    {
    public:
        typedef enum
        {
            AUTHENTICATION_STATE_E_NOT_INITIALIZED = 0,
            AUTHENTICATION_STATE_E_AUTHORIZING     = 1,
            AUTHENTICATION_STATE_E_AUTHORIZED      = 2,
            AUTHENTICATION_STATE_E_NOT_AUTHORIZED  = 3
        } AUTHENTICATION_STATE_E;

        typedef std::set<std::string> ValidSecurityModes;

    private:
        std::string             _securityMode;
        NamespaceDesc           _currentNamespaceDesc;
        NamespaceDesc           _defaultNamespaceDesc;
        scidb::UserDesc         _userDesc;
        scidb::RoleDesc         _roleDesc;
        AUTHENTICATION_STATE_E  _authenticationState;
        class Mutex             _lockAuthenticationState;
        ValidSecurityModes      _validSecurityModes;

        std::shared_ptr<namespaces::Communicator>
            _namespacesCommunicator;

        std::shared_ptr<security::Communicator>
            _securityCommunicator;

        std::shared_ptr<ClientCommunicator>
            _clientCommunicator;

    public:
        /**
         * Constructor
         */
        Session();


        void lockAuthenticationState(bool locked);


        /**
         * Determine if the security mode is valid
         * @param mode - value to check for valility
         */
        bool isValidSecurityMode(const std::string & mode) const;

        /**
         * Sets the security mode.
         * @param securityMode - must be one of  {"trust", "password" }
         */
        void setSecurityMode(const std::string & securityMode);

        /**
         * Currently should return one of:
         *   {"Uninitialized", "trust", "password" }
         */
        const std::string &getSecurityMode() const;

        /**
         * Saves the current AUTHENTICATION state of the session
         * @param state - the new state
         */
        void setAuthenticatedState(AUTHENTICATION_STATE_E state);

        /**
         *  Retrieves the AUTHENTICATION state of the session
         */
        Session::AUTHENTICATION_STATE_E getAuthenticatedState();

        /**
         * Saves the current namespace info for later retrieval
         * @param namespaceDesc - the namespace's info
         */
        void setNamespace(const NamespaceDesc &namespaceDesc);

        /**
         *  Retrieves the current namespace's descriptor
         */
        NamespaceDesc &getNamespace();


        /**
         *  Retrieves the default (normally public) namespace's descriptor
         */
        const NamespaceDesc &getDefaultNamespace() const;

        /**
         *  Set the namespace info to an invalid state
         */
        void invalidateNamespace();

        /**
         * Saves the AUTHENTICATED user info for later retrieval
         * @param userDesc - the AUTHHENTICATED user's info
         */
        void setUser(const scidb::UserDesc &userDesc);

        /**
         *  Retrieves the AUTHENTICATED user info
         */
        const scidb::UserDesc &getUser() const;

        /**
         *  Allows switching the current user.
         */
        void changeUser(const scidb::UserDesc &newUser);

        /**
         *  Set the user info to an invalid state
         */
        void invalidateUser();

        /**
         * Sets the current role to roleDesc
         * @param roleDesc - the descriptor for which role to switch to
         */
        void setRole(const scidb::RoleDesc &roleDesc);

        /**
         *  Retrieves the current role descriptor
         */
        const scidb::RoleDesc &getRole() const;

        /**
         * Saves a communicator to the Security for later retrieval
         * Currently the only security library is "libnamespaces.so"
         *
         * @param securityCommunicator - the communicator to the
         *   security
         *
         * @throws on error
         */
        void setSecurityCommunicator(
            const std::shared_ptr<security::Communicator> &
                securityCommunicator);

        /**
         * Retrieves a communicator to the Security
         * Currently the only security library is "libnamespaces.so"
         */
        std::shared_ptr<security::Communicator>
            getSecurityCommunicator() const;

        /**
         * Saves a communicator to the Namespace for later
         * retrieval.
         *
         * @param namespacesCommunicator - the communicator to the
         *   namespace library
         *
         * @throws on error
         */
        void setNamespacesCommunicator(
            const std::shared_ptr<namespaces::Communicator> &
                namespacesCommunicator);

        /**
         * Retrieves a communicator to the Namespaces library
         */
        std::shared_ptr<namespaces::Communicator>
            getNamespacesCommunicator() const;

        /**
         * Saves a communicator to the Client for later retrieval
         *
         * @param clientCommunicator - the communicator to the
         *   security libary
         *
         * @throws on error
         */
        void setClientCommunicator(
            const std::shared_ptr<ClientCommunicator> &
                clientCommunicator);

        /**
         * Retrieves a communicator to the Client
         */
        std::shared_ptr<ClientCommunicator>
            getClientCommunicator() const;

        const ValidSecurityModes & getValidSecurityModes() const
        {
            return _validSecurityModes;
        }
    };
}  // namespace scidb

#endif /* SESSION_H_ */
