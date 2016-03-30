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
#include <string>
#include <boost/assign.hpp>
#include <log4cxx/logger.h>
#include <pqxx/transaction>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <usr_namespace/NamespaceDesc.h>
#include <usr_namespace/Permissions.h>
#include <usr_namespace/RoleDesc.h>
#include <util/PluginManager.h>
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
        static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.namespacesPluginComm"));

        /**
         * Communication interface between the namespaces plugin and SciDB.
         */
        class Communicator
        {
        private:

        // -------------------------------------------------------------
        public:  // Methods

            /**
             * Constructor
             */
            Communicator() { }


            /**
             * Retrieve the namespace name from the query (defaulting to public if necessary)
             * @param query The query that contains the namespaceName
             */
            static std::string getNamespaceName(
                const std::shared_ptr<Query>& query);

            /**
             * For the given namespace name retrieve the id.
             * First the routine attempts to get the id from the desc.
             * If the id is not valid in the desc, the routine retrieves
             * it from the catalog, updates the desc, and returns the id.
             *
             * @param - namespaceDesc - contains the name of the namespace
             * @param - namespaceID - the id to be returned
             */
            static void getNamespaceId(
                NamespaceDesc &             namespaceDesc,
                scidb::NamespaceDesc::ID &  namespaceID);

            /**
             * Deterine if the environment is such that the specified array can be accessed.
             *
             * @param namespaceName - The name of the array to check in
             * @param arrayName - The name of the array to check for
             */
            static void checkArrayAccess(
                const std::string &namespaceName,
                const std::string &arrayName );

            /**
             * Given a namespace descriptor retrieve the corresponding id.
             * @param namespaceDesc - holds the name of the namespace
             * @param namespaceID - the namespace id to be returned
             * @param throwOnErr - allows turning on/off exception throwing
             */
            static bool findNamespace(
                const NamespaceDesc &       namespaceDesc,
                NamespaceDesc::ID &         namespaceID,
                bool                        throwOnErr = true);

            /**
             * Retrieves a vector of namespace descriptors
             * @param namespaces - the vector of namespaces to be returned
             */
            static bool getNamespaces(
                std::vector<NamespaceDesc> &    namespaces);

            /**
             * Retrieves a vector of role descriptors
             * @param roleDescs - the vector of roles to be returned
             */
            static bool getRoles(
                std::vector<scidb::RoleDesc> &  roleDescs);


            /**
             * Retrieve the id for the specified namespace.
             * Assums a connection and a transaction have been established.
             * @param namespaceDesc - holds the name of the namespace
             * @param naemspaceID - the namespace id to be returned
             * @param connection - the connection to use
             * @param tr - the transaction to use
             */
            static bool findNamespaceWithTransaction(
                const NamespaceDesc &       namespaceDesc,
                NamespaceDesc::ID &         namespaceID,
                pqxx::connection *          connection,
                pqxx::basic_transaction *   tr);

            /**
             * Add an array to a given namespace
             * @param namespaceDesc - holds the name of the namespace (used for logging purposes only)
             * @param namespaceID - the id of the namespace to add the array to
             * @param arrayName - holds the name of the array (used for logging purposes only)
             * @param arrayId - the id of the array to add to the namespace
             * @param connection - the connection to use
             * @param tr - the transaction to use
             */
            static bool addArrayToNamespaceWithTransaction(
                const NamespaceDesc &       namespaceDesc,
                NamespaceDesc::ID &         namespaceID,
                const std::string &         arrayName,
                const ArrayID               arrayId,
                pqxx::connection *          connection,
                pqxx::basic_transaction *   tr);

            /**
             * Check to see if the specified permissions are granted.
             * @param session - the session to check the permissions against
             * @param namespaceDesc - the descriptor of the namespace to check the permissions on
             * @param permissions - the set of permissions to check
             * @throws One of the following:
             * PLUGIN_SYSTEM_EXCEPTION(NAMESPACE_LIB_NAME, SCIDB_SE_UDO, NAMESPACE_E_INVALID_ARGUMENTS)
             * PLUGIN_USER_EXCEPTION(NAMESPACE_LIB_NAME, SCIDB_SE_UDO, NAMESPACE_E_UNRECOGNIZED_PARAMETER)
             * PLUGIN_SYSTEM_EXCEPTION(NAMESPACE_LIB_NAME, SCIDB_SE_UDO, NAMESPACE_E_UNABLE_TO_GET_PERMISSIONS)
             * PLUGIN_USER_EXCEPTION(NAMESPACE_LIB_NAME, SCIDB_SE_UDO, NAMESPACE_E_ROOT_PRIVILEGE)
             * PLUGIN_USER_EXCEPTION(NAMESPACE_LIB_NAME, SCIDB_SE_UDO, NAMESPACE_E_ADMINISTRATE_PRIVILEGE)
             * PLUGIN_USER_EXCEPTION(NAMESPACE_LIB_NAME, SCIDB_SE_UDO, NAMESPACE_E_CREATE_PRIVILEGE)
             * PLUGIN_USER_EXCEPTION(NAMESPACE_LIB_NAME, SCIDB_SE_UDO, NAMESPACE_E_READ_PRIVILEGE)
             * PLUGIN_USER_EXCEPTION(NAMESPACE_LIB_NAME, SCIDB_SE_UDO, NAMESPACE_E_UPDATE_PRIVILEGE)
             * PLUGIN_USER_EXCEPTION(NAMESPACE_LIB_NAME, SCIDB_SE_UDO, NAMESPACE_E_DELETE_PRIVILEGE)
             * PLUGIN_USER_EXCEPTION(NAMESPACE_LIB_NAME, SCIDB_SE_UDO, NAMESPACE_E_LIST_PRIVILEGE)
             */
            static void checkNamespacePermissions(
                const std::shared_ptr<scidb::Session> &         session,
                const NamespaceDesc &                           namespaceDesc,
                const  std::string &                            permissions);

            /**
             * Get array metadata for the array name as of a given catalog version.
             * The metadata provided by this method corresponds to an array with id <= catalogVersion
             * @param[in] namespaceName The namespace the array is contained in
             * @param[in] arrayName Array name
             * @param[in] catalogVersion as previously returned by getCurrentVersion().
             *            If catalogVersion == SystemCatalog::ANY_VERSION,
             *            the result metadata array ID is not bounded by catalogVersion
             * @param[out] arrayDesc Array descriptor
             * @exception scidb::SystemException
             * @see SystemCatalog::getCurrentVersion()
             */
            static void getArrayDesc(
                const std::string&      namespaceName,
                const std::string&      arrayName,
                const ArrayID           catalogVersion,
                ArrayDesc&              arrayDesc);

            /**
             * Get array metadata for the array name as of a given catalog version.
             * The metadata provided by this method corresponds to an array with id <= catalogVersion
             * @param[in] namespaceName The namespace the array is contained in
             * @param[in] arrayName Array name
             * @param[in] catalogVersion as previously returned by getCurrentVersion().
             *            If catalogVersion == SystemCatalog::ANY_VERSION,
             *            the result metadata array ID is not bounded by catalogVersion
             * @param[out] arrayDesc Array descriptor
             * @param[in] throwException throw exception if array with specified name is not found
             * @return true if array is found, false if array is not found and throwException is false
             * @exception scidb::SystemException
             */
            static bool getArrayDesc(
                const std::string&      namespaceName,
                const std::string &     arrayName,
                const ArrayID           catalogVersion,
                ArrayDesc &             arrayDesc,
                const bool              throwException);


            /**
             * Get array metadata for the array name as of a given catalog version.
             * The metadata provided by this method corresponds to an array with id <= catalogVersion
             * @param[in] namespaceName The namespace the array is contained in
             * @param[in] arrayName Array name
             * @param[in] catalogVersion as previously returned by getCurrentVersion().
             *            If catalogVersion == SystemCatalog::ANY_VERSION,
             *            the result metadata array ID is not bounded by catalogVersion
             * @param[in] arrayVersion version identifier or LAST_VERSION
             * @param[out] arrayDesc Array descriptor
             * @param[in] throwException throw exception if array with specified name is not found
             * @return true if array is found, false if array is not found and throwException is false
             * @exception scidb::SystemException
             */
            static bool getArrayDesc(
                const std::string &         namespaceName,
                const std::string &         arrayName,
                const ArrayID               catalogVersion,
                VersionID                   arrayVersion,
                ArrayDesc &                 arrayDesc,
                const bool                  throwException = true);

            /**
             * Fills vector with array descriptors from the persistent catalog manager.
             * @param[in] namespaceName The namespace the arrays are contained in
             * @param arrayDescs Vector of ArrayDesc objects
             * @param ignoreOrphanAttributes whether to ignore attributes whose UDT/UDF are not available
             * @param ignoreVersions whether to ignore version arrays (i.e. of the name <name>@<version>)
             * @throws scidb::SystemException on error
             */
            static void getArrays(
                const std::string &         namespaceName,
                std::vector<ArrayDesc> &    arrays,
                bool                        ignoreOrphanAttributes,
                bool                        ignoreVersions);

            /**
             * Retrieve the id of the array in the catalog.
             *
             * @param[in] namespaceName The namespace the arrays are contained in
             * @param[in] arrayName Array name
             * @return if the array exists the id is returned, otherwise INVALID_ARRAY_ID is returned
             */
            static ArrayID getArrayId(
                const std::string &         namespaceName,
                const std::string &         arrayName);

            /**
             * Checks if there is array with specified name in the storage. First
             * check the local instance's list of arrays. If the array is not present
             * in the local catalog management, check the persistent catalog manager.
             *
             * @param[in] namespaceName The namespace the arrays are contained in
             * @param[in] arrayName Array name
             * @return true if there is array with such name in the storage, false otherwise
             */
            static bool containsArray(
                const std::string &         namespaceName,
                const std::string &         arrayName);

            /**
             * Rename old array (and all of its versions) to the new name
             * @param[in] namespaceName The namespace the arrays are contained in
             * @param[in] oldArrayName The current name of the array
             * @param[in] newArrayName The desired name of the array
             * @throws SystemException(SCIDB_LE_ARRAY_DOESNT_EXIST) if oldArrayName does not exist
             * @throws SystemException(SCIDB_LE_ARRAY_ALREADY_EXISTS) if newArrayName already exists
             */
            static void renameArray(
                const std::string &         namespaceName,
                const std::string &         oldArrayName,
                const std::string &         newArrayName);

            /**
             * Delete array from catalog by its name and all of its versions if this is the base array.
             * @param[in] namespaceName The namespace the arrays are contained in
             * @param[in] arrayName Array name
             * @return true if array was deleted, false if it did not exist
             */
            static bool deleteArray(
                const std::string &         namespaceName,
                const std::string &         arrayName);

            /**
             * Delete all versions prior to given version from array with given name
             * @param[in] namespaceName The namespace the arrays are contained in
             * @param[in] arrayName Array name
             * @param[in] arrayVersion Array version prior to which all versions should be deleted.
             * @return true if array versions were deleted, false if array did not exist
             */
            static bool deleteArrayVersions(
                const std::string &         namespaceName,
                const std::string &         arrayName,
                const VersionID             arrayVersion);

        };
    } // namespace namespaces
} // namespace scidb

#endif // NAMESPACE_PLUGIN_COMMUNICATOR_H_
