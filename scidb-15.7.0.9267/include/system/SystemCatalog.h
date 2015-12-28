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

/**
 *      @file
 *
 *      @brief API for fetching and updating system catalog metadata.
 *
 *      @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#ifndef SYSTEMCATALOG_H_
#define SYSTEMCATALOG_H_

#include <string>
#include <vector>
#include <map>
#include <list>
#include <assert.h>
#include <memory>
#include <pqxx/transaction>

#include <query/TypeSystem.h>
#include <array/Metadata.h>
#include <util/Singleton.h>
#include <system/Cluster.h>

#include <usr_namespace/NamespaceDesc.h>


namespace pqxx
{
// forward declaration of pqxx::connection
    class connect_direct;
    template<typename T> class basic_connection;
    typedef basic_connection<connect_direct> connection;
}

namespace scidb
{
class Mutex;
class NamespaceDesc;
class NamespaceObject;
class PhysicalBoundaries;
class UserDesc;

/**
 * @brief Global object for accessing and manipulating cluster's metadata.
 *
 * On first access catalog object will be created and as result private constructor of SystemCatalog
 * will be called where connection to PostgreSQL will be created. After this
 * cluster can be initialized. Instance must add itself to SC or mark itself as online,
 * and it ready to work (though we must wait other instances online, this can be
 * implemented as PostgreSQL event which will be transparently transported to
 * NetworkManager through callback for example).
 *
 * @note for developers:
 *   If you want to change array_dimension or array_attribute tables,
 *   such as to add the capability to rename attributes or dimensions,
 *   you should make sure an array's dimension names and attribute names do not collide.
 *   @see Donghui Zhang's comment inside SystemCatalog::_addArray().
 *
 */
class SystemCatalog : public Singleton<SystemCatalog>
{
public:

    class LockDesc
    {
    public:
        typedef enum {INVALID_ROLE=0, COORD, WORKER} InstanceRole;
        typedef enum {INVALID_MODE=0, RD, WR, CRT, RM, XCL, RNF} LockMode;

        LockDesc(const std::string& arrayName,
                QueryID  queryId,
                InstanceID   instanceId,
                InstanceRole instanceRole,
                LockMode lockMode);

        virtual ~LockDesc() {}
        const std::string& getArrayName() const { return _arrayName; }
        ArrayID   getArrayId() const { return _arrayId; }
        QueryID   getQueryId() const { return _queryId; }
        InstanceID    getInstanceId() const { return _instanceId; }
        VersionID getArrayVersion() const { return _arrayVersion; }
        ArrayID   getArrayVersionId() const { return _arrayVersionId; }
        ArrayID   getArrayCatalogId() const { assert(isLocked()); return _arrayCatalogId; }
        InstanceRole  getInstanceRole() const { return _instanceRole; }
        LockMode  getLockMode() const { return _lockMode; }
        bool  isLocked() const { return _isLocked; }
        void setArrayId(ArrayID arrayId) { _arrayId = arrayId; }
        void setArrayVersionId(ArrayID versionId) { _arrayVersionId = versionId; }
        void setArrayCatalogId(ArrayID catalogId) { _arrayCatalogId = catalogId; }
        void setArrayVersion(VersionID version) { _arrayVersion = version; }
        void setLockMode(LockMode mode) { _lockMode = mode; }
        void setLocked(bool isLocked) { _isLocked = isLocked; }
        std::string toString();

    private:
        LockDesc(const LockDesc&);
        LockDesc& operator=(const LockDesc&);
        bool operator== (const LockDesc&);
        bool operator!= (const LockDesc&);

        std::string _arrayName;
        ArrayID  _arrayId;
        QueryID  _queryId;
        InstanceID   _instanceId;
        ArrayID  _arrayVersionId;
        ArrayID  _arrayCatalogId; // highest (version) array ID for _arrayId;
                                  // right after all the query locks are acquired, the state of the catalog should be such that
                                  // _arrayId <= _arrayCatalogId < _arrayVersionId (where 0 means non-existent)
        VersionID _arrayVersion;
        InstanceRole _instanceRole; // 1-coordinator, 2-worker
        LockMode  _lockMode; // {1=read, write, remove, renameto, renamefrom}
        bool _isLocked;
    };

    /**
     * This exception is thrown when an array is already locked (by a different query).
     */
    class LockBusyException: public SystemException
    {
    public:
        LockBusyException(const char* file, const char* function, int32_t line)
        : SystemException(file, function, line, "scidb",
                          SCIDB_SE_EXECUTION, SCIDB_LE_RESOURCE_BUSY,
                          "SCIDB_SE_EXECUTION", "SCIDB_LE_RESOURCE_BUSY", uint64_t(0))
        {
        }
        ~LockBusyException() throw () {}
        void raise() const { throw *this; }
        virtual Exception::Pointer copy() const
        {
            std::shared_ptr<LockBusyException> ep =
               std::make_shared<LockBusyException>(_file.c_str(),
                                                     _function.c_str(),
                                                     _line);
            ep->_what_str = _what_str;
            ep->_formatter = _formatter;
            return ep;
        }
    };

    /**
     * Log errors produced by sql
     */
    void _logSqlError(
        const std::string &t,
        const std::string &w);

    /**
     * Add the 'INVALID' flag to all array entries in the catalog currently
     * marked as being 'TRANSIENT'.
     */
    void invalidateTempArrays();

    /**
     * Rename old array (and all of its versions) to the new name
     * @param[in] old_array_name
     * @param[in] new array_name
     * @throws SystemException(SCIDB_LE_ARRAY_DOESNT_EXIST) if old_array_name does not exist
     * @throws SystemException(SCIDB_LE_ARRAY_ALREADY_EXISTS) if new_array_name already exists
     */
    void renameArray(const std::string &old_array_name, const std::string &new_array_name);

    /**
     * @throws a scidb::Exception if necessary
     */
    typedef boost::function<bool()> ErrorChecker;

    /**
     * Acquire a lock in the catalog. On a coordinator the method will block until the lock can be acquired.
     * On a worker instance, the lock will not be acquired unless a corresponding coordinator lock exists.
     * @param[in] lockDesc the lock descriptor
     * @param[in] errorChecker that is allowed to interrupt the lock acquisition
     * @return true if the lock was acquired, false otherwise
     */
    bool lockArray(const std::shared_ptr<LockDesc>&  lockDesc, ErrorChecker& errorChecker);

    /**
     * Release a lock in the catalog.
     * @param[in] lockDesc the lock descriptor
     * @return true if the lock was released, false if it did not exist
     */
    bool unlockArray(const std::shared_ptr<LockDesc>& lockDesc);

    /**
     * Update the lock with new fields. Array name, query ID, instance ID, instance role
     * cannot be updated after the lock acquisition.
     * @param[in] lockDesc the lock descriptor
     * @return true if the lock was released, false if it did not exist
     */
    bool updateArrayLock(const std::shared_ptr<LockDesc>& lockDesc);

    /**
     * Get all arrays locks from the catalog for a given instance.
     * @param[in] instanceId
     * @param[in] coordLocks locks acquired as in the coordinator role
     * @param[in] workerLocks locks acquired as in the worker role
     */
    void readArrayLocks(const InstanceID instanceId,
            std::list< std::shared_ptr<LockDesc> >& coordLocks,
            std::list< std::shared_ptr<LockDesc> >& workerLocks);

    /**
     * Delete all arrays locks created as coordinator from the catalog on a given instance.
     * @param[in] instanceId
     * @return number of locks deleted
     */
    uint32_t deleteCoordArrayLocks(InstanceID instanceId);

    /**
     * Delete all arrays locks created as coordinator from the catalog for a given query on a given instance.
     * @param[in] instanceId
     * @param[in] queryId
     * @return number of locks deleted
     */
    uint32_t deleteWorkerArrayLocks(InstanceID instanceId);

    /**
     * Delete all arrays locks from the catalog for a given query on a given instance, and role
     * @param[in] instanceId
     * @param[in] queryId
     * @param[in] instance role (coord or worker), if equals LockDesc::INVALID_ROLE, it is ignored
     * @return number of locks deleted
     */
    uint32_t deleteArrayLocks(InstanceID instanceId, QueryID queryId,
                              LockDesc::InstanceRole role = LockDesc::INVALID_ROLE);

    /**
     * Check if a coordinator lock for given array name and query ID exists in the catalog
     * @param[in] arrayName
     * @param[in] queryId
     * @return the lock found in the catalog possibly empty
     */
    std::shared_ptr<LockDesc> checkForCoordinatorLock(const std::string& arrayName,
                                                        QueryID queryId);

    /**
     * Populate PostgreSQL database with metadata, generate cluster UUID and return
     * it as result
     *
     * @return Cluster UUID
     */
    const std::string& initializeCluster();

    /**
     * @return is cluster ready to work?
     */
    bool isInitialized() const;


    /**
     * @return UUID if cluster initialized else - void string
     */
    const std::string& getClusterUuid() const;

    /**
     * @note IMPORTANT: Array updates must not use this interface, @see SystemCatalog::addArrayVersion().
     * Add new array to the catalog by descriptor.
     * @param[in] namespace_desc describes the namespace to add the array into
     * @param[in] array_desc fully populated descriptor
     */
    void addArray(
        const NamespaceDesc &namespaceDesc,
        const ArrayDesc &array_desc);

    /**
     * Transactionally add a new array version.
     * Basically, this is how all array updates become visible to other queries.
     * @param[in] namespace_desc describes the namespace to add the array into
     * @param unversionedDesc schema for the unversioned array if not already in the catalog, NULL if it is
     *        Must have the array ID already filled in.
     * @param versionedDesc schema for the new versioned array to be added
     *        Must have the array ID already filled in.
     * @see scidb::SystemCatalog::getNextArrayId()
     * @throws scidb::SystemException if the catalog state is not consistent with this operation
     */
    void addArrayVersion(
        const NamespaceDesc &namespaceDesc,
        const ArrayDesc* unversionedDesc,
        const ArrayDesc& versionedDesc);

    /**
     * Fills vector with array names from the persistent catalog manager.
     * @param arrays Vector of strings
     */
    void getArrays(std::vector<std::string> &arrays);

    /**
     * Fills vector with array descriptors from the persistent catalog manager.
     * @param arrayDescs Vector of ArrayDesc objects
     * @param ignoreOrphanAttributes whether to ignore attributes whose UDT/UDF are not available
     * @param ignoreVersions whether to ignore version arrays (i.e. of the name <name>@<version>)
     * @throws scidb::SystemException on error
     */
    void getArrays(std::vector<ArrayDesc>& arrayDescs,
                   bool ignoreOrphanAttributes,
                   bool ignoreVersions);

    /**
     * Checks if there is array with specified name in the storage. First
     * check the local instance's list of arrays. If the array is not present
     * in the local catalog management, check the persistent catalog manager.
     *
     * @param[in] array_name Array name
     * @return true if there is array with such name in the storage, false otherwise
     */
    bool containsArray(const std::string &array_name);


    /**
     * Retrieves information about a user in users table
     *
     * @param user - Information about the user
     */
    void findUser(UserDesc &user);


    /**
     * Adds a new entry to the users table if the user does not already exist.
     *
     * @param user - Information about the user
     */
    void createUser(UserDesc &user);

    /**
     * Changes the setting for the specified user in users.
     *
     * @param user - Information about the user
     * @param whatToChange - what to change about the user
     */
    void changeUser(
        UserDesc &user,
        const std::string &whatToChange);

    /**
     * Removes an entry representing a user in the users table.
     *
     * @param name - the name of the user to remove
     */
    void dropUser(const UserDesc &user);

    /**
     * Fills vector with user names from the persistent catalog manager.
     * @param users Vector of desriptors describing user
     */
    void getUsers(std::vector<UserDesc> &users);


    // -------------------------------------------------------------- //


    /**
     * Retrieves information about a namespace in scidb_namespaces table
     *
     * @param namespaceDesc - the descriptor of the namespace to find
     * @param namespaceId - the return id of the namespace
     * @param throwOnErr - throws if the namespace library is not loaded
     */
    void findNamespace(
        const NamespaceDesc &   namespaceDesc,
        NamespaceDesc::ID &     namespaceId,
        bool                    throwOnErr = true);

    /**
     * Adds a new entry to the scidb_namespaces table if the namespace
     * does not already exist.
     *
     * @param namespaceDesc - the descriptor of the namespace to add
     */
    void createNamespace(
        const NamespaceDesc & namespaceDesc);

    /**
     * Removes an entry representing a namespace in the
     * scidb_namespaces table.
     *
     * @param namespaceDesc - the descriptor of the namespace to remove
     */
    void dropNamespace(
        const NamespaceDesc & namespaceDesc);

    /**
     * Fills vector with Namespace names from the persistent catalog
     * manager.
     *
     * @param namespaceDescs - Vector of namespace descriptors
     */
    void getNamespaces(
        std::vector<NamespaceDesc> &namespaceDescs);

    /**
     * Retrieve the namespace id for a given array id
     *
     * @param[in] arrayId - the id of the array to determine the namespace for
     * @param[out] namespaceId - the id of the namespace associated with the arrayId
     */
    void getNamespaceIdFromArrayId(
        const ArrayID           arrayId,
        NamespaceDesc::ID &     namespaceId);


    /// Unrestricted catalog version
    static const ArrayID ANY_VERSION;
    static const ArrayID MAX_ARRAYID;
    static const VersionID MAX_VERSIONID;


    /**
     * Get array metadata for the array name as of a given catalog version.
     * The metadata provided by this method corresponds to an array with id <= catalogVersion
     * @param[in] array_name Array name
     * @param[in] catalogVersion as previously returned by getCurrentVersion().
     *            If catalogVersion == SystemCatalog::ANY_VERSION,
     *            the result metadata array ID is not bounded by catalogVersion
     * @param[out] array_desc Array descriptor
     * @exception scidb::SystemException
     * @see SystemCatalog::getCurrentVersion()
     */
    void getArrayDesc(const std::string& array_name,
                      const ArrayID catalogVersion,
                      ArrayDesc& array_desc);

    /**
     * Get array metadata for the array name as of a given catalog version.
     * The metadata provided by this method corresponds to an array with id <= catalogVersion
     * @param[in] array_name Array name
     * @param[in] catalogVersion as previously returned by getCurrentVersion().
     *            If catalogVersion == SystemCatalog::ANY_VERSION,
     *            the result metadata array ID is not bounded by catalogVersion
     * @param[out] array_desc Array descriptor
     * @param[in] throwException throw exception if array with specified name is not found
     * @return true if array is found, false if array is not found and throwException is false
     * @exception scidb::SystemException
     */
    bool getArrayDesc(const std::string &array_name,
                      const ArrayID catalogVersion,
                      ArrayDesc &array_desc,
                      const bool throwException);

    /**
     * Get array metadata for the array name as of a given catalog version.
     * The metadata provided by this method corresponds to an array with id <= catalogVersion
     * @param[in] array_name Array name
     * @param[in] catalogVersion as previously returned by getCurrentVersion().
     *            If catalogVersion == SystemCatalog::ANY_VERSION,
     *            the result metadata array ID is not bounded by catalogVersion
     * @param[in] array_version version identifier or LAST_VERSION
     * @param[out] array_desc Array descriptor
     * @param[in] throwException throw exception if array with specified name is not found
     * @return true if array is found, false if array is not found and throwException is false
     * @exception scidb::SystemException
     */
    bool getArrayDesc(const std::string &array_name,
                      const ArrayID catalogVersion,
                      VersionID version,
                      ArrayDesc &array_desc,
                      const bool throwException = true);

    /**
     * Returns array metadata by its ID
     * @param[in] id array identifier
     * @param[out] array_desc Array descriptor
     */
    void getArrayDesc(const ArrayID id, ArrayDesc &array_desc);

    /**
     * Returns array metadata by its ID
     * @param[in] id array identifier
     * @return Array descriptor
     */
    std::shared_ptr<ArrayDesc> getArrayDesc(const ArrayID id);

    /**
     * Delete array from catalog by its name and all of its versions if this is the base array.
     * @param[in] array_name Array name
     * @return true if array was deleted, false if it did not exist
     */
    bool deleteArray(const std::string &array_name);

    /**
     * Delete all versions prior to given version from array with given name
     * @param[in] array_name Array name
     * @param[in] array_version Array version prior to which all versions should be deleted.
     * @return true if array versions were deleted, false if array did not exist
     */
    bool deleteArrayVersions(const std::string &array_name, const VersionID array_version);

    /**
     * Delete array from persistent system catalog manager by its ID
     * @param[in] id array identifier
     */
    void deleteArray(const ArrayID id);

    /**
     * Get an array ID suitable for using in a schema(ArrayDesc) for a persistent array,
     * the one stored in the catalog (including the temp arrays).
     */
    ArrayID getNextArrayId();

    /**
     * Delete version of the array
     * @param[in] arrayID array ID
     * @param[in] versionID version ID
     */
    void deleteVersion(const ArrayID arrayID, const VersionID versionID);

    /**
     * Get last version of an array.
     * The version provided by this method corresponds to an array with id <= catalogVersion
     * @param[in] unvAId unversioned array ID
     * @param[in] catalogVersion as previously returned by getCurrentVersion().
     *            If catalogVersion == SystemCatalog::ANY_VERSION,
     *            the array ID corresponding to the result is not bounded by catalogVersion
     * @return identifier of last array version or 0 if this array has no versions
     */
    VersionID getLastVersion(const ArrayID unvAId,
                             const ArrayID catlogVersion=ANY_VERSION);

    /**
     * Get array id of oldest version of array
     * @param[in] id array ID
     * @return array id of oldest version of array or 0 if array has no versions
     */
    ArrayID getOldestArrayVersion(const ArrayID id);

    /**
     * Get the latest version preceeding specified timestamp
     * @param[in] id array ID
     * @param[in] timestamp string with timestamp
     * @return identifier ofmost recent version of array before specified timestamp or 0 if there is no such version
     */
    VersionID lookupVersionByTimestamp(const ArrayID id, const uint64_t timestamp);

    /**
     * Get list of updatable array's versions
     * @param[in] arrayId array identifier
     * @return vector of VersionDesc
     */
    std::vector<VersionDesc> getArrayVersions(const ArrayID array_id);

    /**
     * Get array actual upper boundary
     * @param[in] id array ID
     * @return array of maximal coordinates of array elements
     */
    Coordinates getHighBoundary(const ArrayID array_id);

    /**
     * Get array actual low boundary
     * @param[in] id array ID
     * @return array of minimum coordinates of array elements
     */
    Coordinates getLowBoundary(const ArrayID array_id);

    /**
     * Update array high and low boundaries
     * @param[in] desc the array descriptor
     * @param[in] bounds the boundaries of the array
     */
    void updateArrayBoundaries(ArrayDesc const& desc, PhysicalBoundaries const& bounds);

    /**
     * Get number of registered instances
     * return total number of instances registered in catalog
     */
    uint32_t getNumberOfInstances();

    /**
     * Add new instance to catalog
     * @param[in] instance Instance descriptor
     * @return Identifier of instance (ordinal number actually)
     */
    uint64_t addInstance(const InstanceDesc &instance);

    /**
     * Return all instances registered in catalog.
     * @param[out] instances Instances vector
     */
    void getInstances(Instances &instances);

    /**
     * Get instance metadata by its identifier
     * @param[in] instance_id Instance identifier
     * @param[out] instance Instance metadata
     */
    void getClusterInstance(InstanceID instance_id, InstanceDesc &instance);

    /**
     * Switch instance to online and update its host and port
     * @param[in] instance_id Instance identifier
     * @param[in] host Instance host
     * @param[in] port Instance port
     */
    void markInstanceOnline(InstanceID instance_id, const std::string& host, uint16_t port);

    /**
     * Switch instance to offline
     * @param[in] instance_id Instance identifier
     */
    void markInstanceOffline(InstanceID instance_id);

    /**
     * Temporary method for connecting to PostgreSQL database used as metadata
     * catalog
     *
     * @param[in] doUpgrade run upgrade scripts depending on metadata version
     */
    void connect(bool doUpgrade);

    /**
     * Temporary method for checking connection to catalog's database.
     *
     * @return is connection established
     */
    bool isConnected() const;

    /**
     * Load library, and record loaded library in persistent system catalog
     * manager.
     *
     * @param[in] library name
     */
    void addLibrary(const std::string& libraryName);

    /**
     * Get info about loaded libraries from the persistent system catalog
     * manager.
     *
     * @param[out] libraries vector of library names
     */
    void getLibraries(std::vector< std::string >& libraries);

    /**
     * Unload library.
     *
     * @param[in] library name
     */
    void removeLibrary(const std::string& libraryName);

    /**
     * Returns version of loaded catalog metadata
     *
     * @return[out] metadata version
     */
    int getMetadataVersion() const;

    struct LockPtrLess : std::binary_function <const std::shared_ptr<scidb::SystemCatalog::LockDesc>,
                                               const std::shared_ptr<scidb::SystemCatalog::LockDesc>, bool>
    {
        bool operator() (const std::shared_ptr<scidb::SystemCatalog::LockDesc>& l,
                         const std::shared_ptr<scidb::SystemCatalog::LockDesc>& r) const
        {
            if (!l || !r) {
                ASSERT_EXCEPTION_FALSE("LockPtrLess: NULL argument");
                return false;
            }
            return (l->getArrayName() < r->getArrayName());
        }
    };

    typedef std::set<std::shared_ptr<LockDesc>, LockPtrLess > QueryLocks;

    /**
     * Updates the query array locks with the highest array ID committed to the catalog for each array
     * in the list of ACQUIRED locks.
     * @param locks [in/out] all already ACQUIRED array locks for the current query
     */
    void getCurrentVersion(QueryLocks& locks);


private:  // Definitions

    struct StringPtrLess : std::binary_function <const std::string*, const std::string*, bool>
    {
        bool operator() (const std::string* const& l, const std::string* const& r) const
        {
            if (!l || !r) {
                assert(false);
                return false;
            }
            return ((*l) < (*r));
        }
    };

private:  // Methods

    /**
     * Helper method to get an appropriate SQL string for a given lock
     */
    static std::string getLockInsertSql(const std::shared_ptr<LockDesc>& lockDesc);

    /// SQL to garbage-collect unused mapping arrays
    static const std::string cleanupMappingArraysSql;

    /**
     * Default constructor for SystemCatalog()
     */
    SystemCatalog();
    virtual ~SystemCatalog();

    void _invalidateTempArray(const std::string& arrayName);
    void _renameArray(const std::string &old_array_name, const std::string &new_array_name);
    bool _lockArray(const std::shared_ptr<LockDesc>&  lockDesc, ErrorChecker& errorChecker);
    bool _unlockArray(const std::shared_ptr<LockDesc>& lockDesc);
    bool _updateArrayLock(const std::shared_ptr<LockDesc>& lockDesc);
    void _readArrayLocks(const InstanceID instanceId,
            std::list< std::shared_ptr<LockDesc> >& coordLocks,
            std::list< std::shared_ptr<LockDesc> >& workerLocks);
    uint32_t _deleteArrayLocks(InstanceID instanceId, QueryID queryId, LockDesc::InstanceRole role);
    std::shared_ptr<LockDesc> _checkForCoordinatorLock(const std::string& arrayName,
            QueryID queryId);
    std::string _findCredParam(const std::string& creds, const std::string& what);
    std::string _makeCredentials();
    void _initializeCluster();


    void _findUser(UserDesc &user);

    void _createUser(UserDesc &user);

    void _changeUser(
        UserDesc &user,
        const std::string &whatToChange);

    void _dropUser(const UserDesc &user);

    void _getUsers(std::vector<UserDesc> &users);

    void _findNamespace(
        const NamespaceDesc &       name,
        NamespaceDesc::ID &         namespaceId,
        bool                        throwOnErr = true);

    void _createNamespace(
        const NamespaceDesc &       namespaceDesc);

    void _dropNamespace(
        const NamespaceDesc &       namespaceDesc);

    void _getNamespaces(std::vector<NamespaceDesc> &namespaces);

    void _getNamespaceIdFromArrayId(
        const ArrayID               arrayId,
        NamespaceDesc::ID &         namespaceId);
    void _getNamespaceIdFromArrayId(
        const scidb::ArrayID            arrayId,
        NamespaceDesc::ID &             namespaceId,
        pqxx::basic_transaction*        tr);


    void _addArray(
        const NamespaceDesc &namespaceDesc,
        const ArrayDesc &array_desc);
    void _addArray(
        const NamespaceDesc &namespaceDesc,
        const ArrayDesc &array_desc,
        pqxx::basic_transaction* tr);
    void _addArrayVersion(
        const NamespaceDesc &namespaceDesc,
        const ArrayDesc* unversionedDesc,
        const ArrayDesc& versionedDesc);
    void _getArrays(std::vector<std::string> &arrays);
    void _getArrays(std::vector<ArrayDesc>& arrayDescs,
                    bool ignoreOrphanAttributes,
                    bool ignoreVersions);
    bool _containsArray(const ArrayID array_id);
    ArrayID _findArrayByName(const std::string &array_name);
    void _getArrayDesc(const std::string &array_name,
                       const ArrayID catalogVersion,
                       const bool ignoreOrphanAttributes,
                       ArrayDesc &array_desc);
    void _getArrayDesc(const std::string &array_name,
                       const ArrayID catalogVersion,
                       const bool ignoreOrphanAttributes,
                       ArrayDesc &array_desc,
                       pqxx::basic_transaction* tr);
    void getArrayInfo(const std::string &array_name,
                      const ArrayID catalogVersion,
                      ArrayID& arrId,
                      std::string& arrName,
                      int& arrPs,
                      int& arrFlags,
                      pqxx::basic_transaction* tr);
    std::shared_ptr<ArrayDesc> _getArrayDesc(const ArrayID id);
    bool _deleteArrayByName(const std::string &array_name);
    bool _deleteArrayVersions(const std::string &array_name, const VersionID array_version);
    void _deleteArrayById(const ArrayID id);
    ArrayID _getNextArrayId();
    ArrayID _getNextArrayId(pqxx::basic_transaction* tr);
    VersionID _createNewVersion(const ArrayID id, const ArrayID version_array_id,
                                pqxx::basic_transaction* tr);
    void _deleteVersion(const ArrayID arrayID, const VersionID versionID);
    VersionID _getLastVersion(const ArrayID id, const ArrayID catlogVersion);
    ArrayID _getOldestArrayVersion(const ArrayID id);
    VersionID _lookupVersionByTimestamp(const ArrayID id, const uint64_t timestamp);
    std::vector<VersionDesc> _getArrayVersions(const ArrayID array_id);
    Coordinates _getHighBoundary(const ArrayID array_id);
    Coordinates _getLowBoundary(const ArrayID array_id);
    void _updateArrayBoundaries(ArrayDesc const& desc, PhysicalBoundaries const& bounds);
    uint32_t _getNumberOfInstances();
    uint64_t _addInstance(const InstanceDesc &instance);
    void _getInstances(Instances &instances);
    void _getClusterInstance(InstanceID instance_id, InstanceDesc &instance);
    void _markInstanceOnline(InstanceID instance_id, const std::string& host, uint16_t port);
    void _markInstanceOffline(InstanceID instance_id);
    void _addLibrary(const std::string& libraryName);
    void _getLibraries(std::vector< std::string >& libraries);
    void _removeLibrary(const std::string& libraryName);
    void _getCurrentVersion(QueryLocks& locks);

private:  // Variables

    bool _initialized;
    pqxx::connection *_connection;
    std::string _uuid;
    int _metadataVersion;
    const NamespaceDesc::ID PUBLIC_NS_ID = 1;

    //FIXME: libpq don't have ability of simultaneous access to one connection from
    // multiple threads even on read-only operatinos, so every operation must
    // be locked with this mutex while system catalog using PostgreSQL as storage.
    static Mutex _pgLock;

    friend class Singleton<SystemCatalog>;
    /// number of attempts to reconnect to PG
    int _reconnectTries;
    /// number of attempts to re-execute a conflicting/concurrent serialized txn
    int _serializedTxnTries;
    static const int DEFAULT_SERIALIZED_TXN_TRIES =10;

    class TxnIsolationConflict : public pqxx::sql_error
    {
    public:
        explicit TxnIsolationConflict(const std::string& what,
				      const std::string& query)
        : sql_error(what, query) {}
    };
    void throwOnSerializationConflict(const pqxx::sql_error& e);
};

} // namespace scidb

#endif /* SYSTEMCATALOG_H_ */
