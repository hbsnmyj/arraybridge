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

/*!
 *      @file
 *
 *      @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 *      @brief API for fetching and updating system catalog metadata.
 */

#include <system/SystemCatalog.h>

#include <query/Operator.h>

#include <vector>
#include <stdint.h>
#include <unistd.h>
#include <assert.h>
#include <fstream>
#include <sstream>
#include <iostream>
#include <limits.h>
#include <pwd.h>
#include <sys/stat.h>

#include <boost/random.hpp>

#include <log4cxx/logger.h>

#include <array/Metadata.h>
#include <query/Expression.h>
#include <query/Query.h>
#include <query/Serialize.h>
#include <system/Config.h>
#include <system/ErrorCodes.h>
#include <system/Exceptions.h>
#include <system/SciDBConfigOptions.h>
#include <system/catalog/data/CatalogMetadata.h>
#include <smgr/io/Storage.h>
#include <usr_namespace/NamespaceDesc.h>
#include <usr_namespace/NamespaceObject.h>
#include <usr_namespace/NamespacesCommunicator.h>
#include <usr_namespace/SecurityCommunicator.h>
#include <usr_namespace/UserDesc.h>
#include <util/Mutex.h>
#include <util/Pqxx.h>

using namespace std;
using namespace pqxx;
using namespace pqxx::prepare;

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.catalog"));

    Mutex SystemCatalog::_pgLock;

    const ArrayID SystemCatalog::ANY_VERSION = ArrayID(std::numeric_limits<int64_t>::max());
    const ArrayID SystemCatalog::MAX_ARRAYID = ArrayID(std::numeric_limits<int64_t>::max());
    const VersionID SystemCatalog::MAX_VERSIONID = VersionID(std::numeric_limits<int64_t>::max());

     SystemCatalog::LockDesc::LockDesc(const std::string& namespaceName,
                                      const std::string& arrayName,
                                      const QueryID&  queryId,
                                      InstanceID   instanceId,
                                      InstanceRole instanceRole,
                                      LockMode lockMode)
    : _namespaceName(namespaceName),
      _arrayName(arrayName),
      _fullArrayName(
            ArrayDesc::makeQualifiedArrayName(
                _namespaceName, _arrayName)),
      _arrayId(0),
      _queryId(queryId),
      _instanceId(instanceId),
      _arrayVersionId(0),
      _arrayCatalogId(0),
      _arrayVersion(0),
      _lockMode(lockMode),
      _isLocked(false)
     {

         SCIDB_ASSERT(isValidPhysicalInstance(instanceId));
         SCIDB_ASSERT(isValidPhysicalInstance(_queryId.getCoordinatorId()));

         ASSERT_EXCEPTION((instanceRole == COORD && _queryId.getCoordinatorId() == instanceId) ||
                          (instanceRole == WORKER && _queryId.getCoordinatorId() != instanceId),
                          "Invalid query ID or instance ID");
     }

     SystemCatalog::LockDesc::LockDesc(const std::string& arrayName,
                                       const QueryID&  queryId,
                                       InstanceID   instanceId,
                                       InstanceRole instanceRole,
                                       LockMode lockMode)
    : LockDesc(
        "public", arrayName, queryId,
        instanceId, instanceRole,lockMode)
    {
         SCIDB_ASSERT(isValidPhysicalInstance(instanceId));
         SCIDB_ASSERT(isValidPhysicalInstance(_queryId.getCoordinatorId()));

         ASSERT_EXCEPTION((instanceRole == COORD && _queryId.getCoordinatorId() == instanceId) ||
                          (instanceRole == WORKER && _queryId.getCoordinatorId() != instanceId),
                          "Invalid query ID or instance ID");
    }

     std::string SystemCatalog::LockDesc::toString()
     {
        std::ostringstream out;
        out << "Lock: "
            << "namespaceName="
            << _namespaceName
            << ", arrayName="
            << _arrayName
            << ", arrayId="
            << _arrayId
            << ", queryId="
            << _queryId
            << ", instanceId="
            << _instanceId
            << ", instanceRole="
            << (_queryId.getCoordinatorId() == _instanceId ? "COORD" : "WORKER")
            << ", lockMode="
            << _lockMode
            << ", arrayVersion="
            << _arrayVersion
            << ", arrayVersionId="
            << _arrayVersionId
            << ", arrayCatalogId="
            << _arrayCatalogId;

        return out.str();
    }

    void SystemCatalog::_logSqlError(
        const std::string &t,
        const std::string &w)
    {
        LOG4CXX_ERROR(logger,
            "sql_error"
            << " name=" << t
            << " what=" << w);
    }

    void SystemCatalog::_invalidateTempArray(const std::string& arrayName)
    {
        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);

        LOG4CXX_TRACE(logger, "SystemCatalog::_removeTempArray()");

        try
        {
         /* Add the 'INVALID' flag to all entries of the 'array' table whose
          * 'flags' field currently has the 'TRANSIENT' bit set... */

            string sql("update \"array\" set flags = (flags | $1) where (flags & $2)!=0");

            if (!arrayName.empty()) {
                sql += " and name=$3";
            }

            pqxx::transaction<pqxx::serializable> tr(*_connection);


            if (needPreparedParamDecls()) {
                PQXX_DECL_USE (
                pqxx::prepare::declaration decl = _connection->prepare(sql,sql)
                        ("int",treat_direct)
                        ("int",treat_direct);
                if (!arrayName.empty()) {
                    decl("varchar", treat_string);
                } )
            } else {
                _connection->prepare(sql,sql);
            }
            pqxx::prepare::invocation invc = tr.prepared(sql)
            (int(ArrayDesc::INVALID))
            (int(ArrayDesc::TRANSIENT));

            if (!arrayName.empty()) {
                invc(arrayName);
            }

            invc.exec();
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);

            LOG4CXX_ERROR(logger, "SystemCatalog::_invalidateTempArray: postgress exception:"<< e.what());
            LOG4CXX_ERROR(logger, "SystemCatalog::_invalidateTempArray: query:"              << e.query());
            if (isDebug()) {
                const string t = typeid(e).name();
                LOG4CXX_ERROR(logger, "SystemCatalog::_invalidateTempArray: postgress exception type:"<< t);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT,SCIDB_LE_PG_QUERY_EXECUTION_FAILED)       << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                SCIDB_ASSERT(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }

        LOG4CXX_TRACE(logger, "Invalidated temp arrays");
    }

    void SystemCatalog::invalidateTempArrays()
    {
        const string allArrays;
        boost::function<void()> work1 = boost::bind(&SystemCatalog::_invalidateTempArray, this, boost::cref(allArrays));
        boost::function<void()> work2 = boost::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                                    work1, _serializedTxnTries);
        Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
    }

    const std::string& SystemCatalog::initializeCluster()
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_initializeCluster, this);
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
        return _uuid;
    }

    void SystemCatalog::_initializeCluster()
    {
        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);

        LOG4CXX_TRACE(logger, "SystemCatalog::initializeCluster()");

        try
        {
            work tr(*_connection);
            tr.exec(string(CURRENT_METADATA));

            result query_res = tr.exec("select get_cluster_uuid as uuid from get_cluster_uuid()");
            _uuid = query_res[0].at("uuid").as(string());
            query_res = tr.exec("select get_metadata_version as version from get_metadata_version()");
            _metadataVersion = query_res[0].at("version").as(int());
            assert(METADATA_VERSION == _metadataVersion);
            _initialized = true;

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }

        LOG4CXX_TRACE(logger, "Initialized cluster uuid = " << _uuid << ", metadata version = " << _metadataVersion);
}

    bool SystemCatalog::isInitialized() const
    {
        return _initialized;
    }

    const std::string& SystemCatalog::getClusterUuid() const
    {
        return _uuid;
    }

    ArrayID SystemCatalog::getNextArrayId()
    {
        boost::function<ArrayID()> work = boost::bind(&SystemCatalog::_getNextArrayId, this);
        return Query::runRestartableWork<ArrayID, broken_connection>(work, _reconnectTries);
    }
    ArrayID SystemCatalog::_getNextArrayId()
    {
        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);
        ArrayID arrId(0);
        try {
            assert(_connection);
            work tr(*_connection);
            arrId = _getNextArrayId(&tr);
            tr.commit();
            LOG4CXX_TRACE(logger, "SystemCatalog::_getNextArrayId(): " << arrId);
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        SCIDB_ASSERT(arrId >0);
        return arrId;
    }
    ArrayID SystemCatalog::_getNextArrayId(pqxx::basic_transaction* tr)
    {
        result query_res = tr->exec("select nextval from nextval('array_id_seq')");
        const ArrayID arrId = query_res[0].at("nextval").as(int64_t());
        return arrId;
    }

    //Not thread safe. Must be called with active connection under _pgLock.
    void SystemCatalog::fillArrayIdentifiers(
        pqxx::connection *          connection,
        pqxx::basic_transaction*    tr,
        string const&               arrayName,
        ArrayID                     arrId,
        ArrayUAID&                  uaid,
        VersionID&                  vid)
    {
        assert(tr);
        uaid = arrId;
        vid = 0;

        if(ArrayDesc::isNameVersioned(arrayName))
        {
            vid = ArrayDesc::getVersionFromName(arrayName);

            string sql_u =
               "select array_id, version_id from \"array_version\" where version_array_id = $1";

            connection->prepare(sql_u, sql_u) PQXX_DECL("bigint", treat_direct);  // arrayId
            result query_res_u = tr->prepared(sql_u)(arrId).exec();
            if (query_res_u.size() <= 0)
            {
                string unversionedName = ArrayDesc::makeUnversionedName(arrayName);
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST)
                    << unversionedName;
            }
            ASSERT_EXCEPTION((query_res_u.size() == 1),
                             "SysCatalog is inconsistent, too many array id rows");

            uaid = query_res_u[0].at("array_id").as(int64_t());

            const uint64_t versionId = static_cast<const uint64_t>(
                query_res_u[0].at("version_id").as(int64_t()));
            ASSERT_EXCEPTION(vid == versionId, "Array version does not match array ID");
        }
    }

    void SystemCatalog::addArrayVersion(
        const ArrayDesc* unversionedDesc,
        const ArrayDesc& versionedDesc)
    {
        boost::function<void()> work1 = boost::bind(
            &SystemCatalog::_addArrayVersion,
            this,
            unversionedDesc,
            boost::cref(versionedDesc));

        boost::function<void()> work2 = boost::bind(
            &Query::runRestartableWork<void, TxnIsolationConflict>,
            work1, _serializedTxnTries);

        Query::runRestartableWork<void, broken_connection>(
            work2, _reconnectTries);
    }

    void SystemCatalog::_addArrayVersion(
        const ArrayDesc* unversionedDesc,
        const ArrayDesc& versionedDesc)
    {
        assert(versionedDesc.getUAId()>0);
        assert(versionedDesc.getUAId()<versionedDesc.getId());
        assert(versionedDesc.getVersionId()>0);

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);

        try {
            pqxx::transaction<pqxx::serializable> tr(*_connection);

            if (unversionedDesc != NULL) {
                SCIDB_ASSERT(unversionedDesc->getId() == versionedDesc.getUAId());
                SCIDB_ASSERT(unversionedDesc->getUAId() == unversionedDesc->getId());
                _addArray(*unversionedDesc, &tr);
            }
            _addArray(versionedDesc, &tr);
            _createNewVersion(versionedDesc.getUAId(), versionedDesc.getId(), &tr);

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::addArray(const ArrayDesc &arrayDesc)
    {
        boost::function<void()> work = boost::bind(
            &SystemCatalog::_addArray,
            this,
            boost::cref(arrayDesc));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_addArray(
        const ArrayDesc &arrayDesc)
    {
        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);
        try
        {
            work tr(*_connection);
            _addArray(arrayDesc, &tr);
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED)
                << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::_addArray(
        const ArrayDesc &array_desc,
        pqxx::basic_transaction* tr)
    {
        // caller sets the timing scope
        LOG4CXX_DEBUG(logger, "SystemCatalog::_addArray array_desc: "<< array_desc );

        const ArrayID arrId = array_desc.getId();
        const string arrayName = array_desc.getName();
        const ArrayUAID uaid = array_desc.getUAId();
        const VersionID vid = array_desc.getVersionId();
        ArrayResPtr residency = array_desc.getResidency();
        ArrayDistPtr distribution = array_desc.getDistribution();

        ASSERT_EXCEPTION(isValidPartitioningSchema(distribution->getPartitioningSchema()),
                         string("Invalid array descriptor: ")+array_desc.toString());
        ASSERT_EXCEPTION(distribution->getPartitioningSchema() != psUndefined,
                         string("Invalid array descriptor: ")+array_desc.toString());
        ASSERT_EXCEPTION(distribution->getPartitioningSchema() != psUninitialized,
                         string("Invalid array descriptor: ")+array_desc.toString());
        {
            size_t instanceShift(0);
            Coordinates offset;
            ArrayDistributionFactory::getTranslationInfo(distribution.get(), offset, instanceShift);
            ASSERT_EXCEPTION((offset.empty() && instanceShift==0),
                             string("Invalid array descriptor: ")+array_desc.toString());
        }
        ASSERT_EXCEPTION(arrId>0, string("Invalid array descriptor: ")+array_desc.toString());
        ASSERT_EXCEPTION( ( (ArrayDesc::isNameUnversioned(arrayName) && uaid==arrId) ||
                            (vid == ArrayDesc::getVersionFromName(arrayName) &&
                             uaid > 0 &&
                             uaid < arrId) ),
                          string("Invalid array version descriptor: ")+array_desc.toString() );

        //XXX TODO: for now we will use the uaid to identify distributions,
        //XXX TODO: at some point we might want to reuse distributions across many arrays
        uint64_t arrDistId = uaid;

        if (uaid == arrId) {
            // inserting an unversioned array
            insertArrayDistribution(arrDistId, distribution, tr);
        }


        // --- Get the current namespace id ---
        NamespaceDesc namespaceDesc(array_desc.getNamespaceName());
        NamespaceDesc::ID namespaceId;
        if(namespaceDesc.getName().compare("public") == 0)
        {
            namespaceId = scidb::NamespaceDesc::getPublicNamespaceId();
        } else {
            if(false == scidb::namespaces::Communicator::findNamespaceWithTransaction(
                namespaceDesc, namespaceId, _connection, tr))
            {
                throw SYSTEM_EXCEPTION(
                    SCIDB_SE_SYSCAT,
                    SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                        << "namespaces";
            }
        }

        // Note from Donghui Zhang 2015-6-23:
        // The logic below makes sure attribute names and dimension names do not collide,
        // as a more efficient implementation of the trigger removed in ticket:3676.
        //
        // Additional note from Donghui Zhang 2015-12-18:
        // As revealed in ticket:5068, we cannot rely on a user exception here because it will be too late.
        // In the scenario described in the ticket, the engine crashed.
        // We fixed the problem by making store() produce unique names.
        // Here instead of throwing a user exception, we assert that there should not be conflicts.
        ASSERT_EXCEPTION(array_desc.areNamesUnique(),
            "In SystemCatalog::_addArray(), duplicate dimension/attribute names are found.");

        string sql1 =
            "insert into \"array\" (name, id, distribution_id, flags) "
            "select $2::VARCHAR, $3, $4, $5 "
            "where not exists("
            "    select 1 from namespace_arrays as NA "
            "    where NA.namespace_name = $1 "
            "    and NA.array_name = $2)";

        _connection->prepare(sql1, sql1)
        PQXX_DECL("varchar", treat_string)  // namespaceName
        PQXX_DECL("varchar", treat_string)  // arrayName
        PQXX_DECL("bigint", treat_direct)   // id
        PQXX_DECL("integer", treat_direct)  // distribution_id
        PQXX_DECL("integer", treat_direct); // flags
        result queryRes = tr->prepared(sql1)
        (array_desc.getNamespaceName())
        (array_desc.getName())
        (arrId)
        (arrDistId)
        (array_desc.getFlags()).exec();
        if (queryRes.affected_rows() != 1)
				{
            LOG4CXX_ERROR(logger, "SystemCatalog::_addArray duplicate array "
                << " queryRes.affected_rows()=" << queryRes.affected_rows());

            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT,SCIDB_LE_NOT_UNIQUE)
                 << array_desc.getQualifiedArrayName();
        }

        if (uaid == arrId) {
            // inserting an unversioned array
            insertArrayResidency(uaid, residency, tr);
        }

        string sql2 = "insert into \"array_attribute\"(array_id, id, name, type, flags, "
        " default_compression_method, reserve, default_missing_reason, default_value) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
        _connection->prepare(sql2, sql2)
            PQXX_DECL("bigint", treat_direct)
            PQXX_DECL("int", treat_direct)
            PQXX_DECL("varchar", treat_string)
            PQXX_DECL("bigint", treat_direct)
            PQXX_DECL("int", treat_direct)
            PQXX_DECL("int", treat_direct)
            PQXX_DECL("int", treat_direct)
            PQXX_DECL("int", treat_direct)
            PQXX_DECL("varchar", treat_string);

        Attributes const& attributes = array_desc.getAttributes();
        Attributes cachedAttributes(attributes.size());
        for (size_t i = 0, n = attributes.size(); i < n; i++)
        {
            AttributeDesc const& attr = attributes[i];
            tr->prepared(sql2)
                (arrId)
                (i)
                (attr.getName())
                (attr.getType())
                (attr.getFlags())
                (attr.getDefaultCompressionMethod())
                (attr.getReserve())
                (static_cast<int>(attr.getDefaultValue().getMissingReason()))
                (attr.getDefaultValueExpr()).exec();

            //Attribute in descriptor has no some data before adding to catalog so build it manually for
            //caching
            cachedAttributes[i] =
                AttributeDesc(AttributeID(i), attr.getName(), attr.getType(), attr.getFlags(),
                          attr.getDefaultCompressionMethod(), std::set<std::string>(), attr.getReserve(),
                          &attr.getDefaultValue(), attr.getDefaultValueExpr());
        }

        string sql3 = "insert into \"array_dimension\"(array_id, id, name,"
        " startMin, currStart, currEnd, endMax, chunk_interval, chunk_overlap) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
        _connection->prepare(sql3, sql3)
            PQXX_DECL("bigint", treat_direct)
            PQXX_DECL("int", treat_direct)
            PQXX_DECL("varchar", treat_string)
            PQXX_DECL("bigint", treat_direct)
            PQXX_DECL("bigint", treat_direct)
            PQXX_DECL("bigint", treat_direct)
            PQXX_DECL("bigint", treat_direct)
            PQXX_DECL("bigint", treat_direct)
            PQXX_DECL("bigint", treat_direct);

        Dimensions const& dims = array_desc.getDimensions();
        for (size_t i = 0, n = dims.size(); i < n; i++)
        {
            DimensionDesc const& dim = dims[i];
            tr->prepared(sql3)
                (arrId)
                (i)
                (dim.getBaseName())
                (dim.getStartMin())
                (dim.getCurrStart())
                (dim.getCurrEnd())
                (dim.getEndMax())
                (dim.getChunkInterval())
                (dim.getChunkOverlap()).exec();
            //TODO: If DimensionDesc will store IDs in future, here must be building vector of
            //dimensions for caching as for attributes.
        }

        //  namespace_id bigint references "namespaces" (id) on delete cascade,
        // array_id bigint references "array" (id) on delete cascade,
        LOG4CXX_DEBUG(logger,
            "SystemCatalog::_addArray(name="
                <<  arrayName << " id=" << arrId << ")");


        if(false == scidb::namespaces::Communicator::addArrayToNamespaceWithTransaction(
            namespaceDesc, namespaceId,
            arrayName,     arrId,
            _connection,   tr))
        {
            // Here it is okay if the namespace library does not exist.
        }
    }

    void SystemCatalog::getArrays(std::vector<std::string> &arrays)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_getArrays,
                this, boost::ref(arrays));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_getArrays(std::vector<std::string> &arrays)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrays()");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);

        try
        {
            work tr(*_connection);

            result query_res = tr.exec(
                "select name from public_arrays where name is not null order by name");

            arrays.clear();
            arrays.reserve(query_res.size());

            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                arrays.push_back(i.at("name").c_str());
            }

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        LOG4CXX_TRACE(logger, "Retrieved " << arrays.size() << " arrays from catalogs");
    }

    ArrayID SystemCatalog::getArrayId(const string &arrayName)
    {
        boost::function<ArrayID()> work = boost::bind(
            &SystemCatalog::_findArrayByName, this, boost::cref(arrayName));

        return Query::runRestartableWork<ArrayID, broken_connection>(work, _reconnectTries);
    }

    bool SystemCatalog::containsArray(const string &arrayName)
    {
        return (getArrayId(arrayName) != INVALID_ARRAY_ID);
    }

    void SystemCatalog::execute(Action& action, bool serialize /* = true */)
    {

        boost::function<void()> work1 = boost::bind(
            &SystemCatalog::_execute,
            this,
            boost::ref(action),
            serialize);

        if(serialize)
        {
            boost::function<void()> work2 = boost::bind(
                &Query::runRestartableWork<void, TxnIsolationConflict>,
                work1, _serializedTxnTries);

            Query::runRestartableWork<void, pqxx::broken_connection>(
                work2, _reconnectTries);
        } else {
            Query::runRestartableWork<void, pqxx::broken_connection>(
                work1, _reconnectTries);
        }
    }

    void SystemCatalog::_execute(Action &action, bool serialize /* = true */)
    {
        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);

        try {
            if(serialize)
            {
                pqxx::transaction<pqxx::serializable> tr(*_connection);
                action(_connection, &tr);
                tr.commit();
            } else {
                work tr(*_connection);
                action(_connection, &tr);
                tr.commit();
            }
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const unique_violation& e)
        {
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_SYSCAT,
                SCIDB_LE_NOT_UNIQUE)
                     << e.what();
        }
        catch (const sql_error &e)
        {
            if(serialize) {
                throwOnSerializationConflict(e);
            }

            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                _logSqlError(t, w);
                assert(false);
            }
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_SYSCAT,
                SCIDB_LE_PG_QUERY_EXECUTION_FAILED)
                    << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_SYSCAT,
                SCIDB_LE_UNKNOWN_ERROR)
                    << e.what();
        }
    }

    void SystemCatalog::findNamespace(
        const NamespaceDesc &   namespaceDesc,
        NamespaceDesc::ID &     namespaceId,
        bool                    throwOnErr /* = true */)
    {
        boost::function<void()> work1 = boost::bind(
            &SystemCatalog::_findNamespace,
            this,
            cref(namespaceDesc),
            ref(namespaceId),
            throwOnErr);

        boost::function<void()> work2 = boost::bind(
            &Query::runRestartableWork<void, TxnIsolationConflict>,
            work1, _serializedTxnTries);

        Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
    }

    void SystemCatalog::_findNamespace(
        const NamespaceDesc &   namespaceDesc,
        NamespaceDesc::ID &     namespaceId,
        bool                    throwOnErr /* = true */)
    {
        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);

        try {
            pqxx::transaction<pqxx::serializable> tr(*_connection);

            if(false == scidb::namespaces::Communicator::findNamespaceWithTransaction(
                namespaceDesc, namespaceId, _connection, &tr))
            {
                // This cannot throw because it would break "list()"
                if(true == throwOnErr)
                {
                    throw SYSTEM_EXCEPTION(
                        SCIDB_SE_SYSCAT,
                        SCIDB_LE_PLUGIN_FUNCTION_ACCESS)
                            << "namespaces";
                }

                namespaceId =
					scidb::NamespaceDesc::getPublicNamespaceId();
            }

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);
            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                _logSqlError(t, w);
                assert(false);
            }
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_SYSCAT,
                SCIDB_LE_PG_QUERY_EXECUTION_FAILED)
                << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            if (isDebug()) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(
                SCIDB_SE_SYSCAT,
                SCIDB_LE_UNKNOWN_ERROR)
                << e.what();
        }
    }


    ArrayID SystemCatalog::_findArrayByName(const std::string &arrayName)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::_findArrayByName( name = " << arrayName << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        LOG4CXX_TRACE(logger, "Failed to find arrayName = " << arrayName << " locally.");

        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql1 = "select id from public_arrays where name = $1";
            _connection->prepare(sql1, sql1) PQXX_DECL("varchar", treat_string);
            result query_res1 = tr.prepared(sql1)(arrayName).exec();
            if (query_res1.size() != 0) {
                return query_res1[0].at("id").as(int64_t());
            }
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return INVALID_ARRAY_ID;
    }

    bool SystemCatalog::getArrayDesc(const string &array_name,
                                     const ArrayID catalogVersion,
                                     VersionID version,
                                     ArrayDesc &array_desc,
                                     const bool throwException)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDesc( array_name= " << array_name
                      <<", version="<< version
                      <<", catlogVersion="<< catalogVersion
                      << " )");
        std::stringstream ss;

        if (version != LAST_VERSION) {
            ss << array_name << "@" << version;
            LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDesc(): array_name= " << ss.str());
            return getArrayDesc(ss.str(), catalogVersion,
                                array_desc, throwException);
        }

        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDesc(): array_name= " << array_name);
        bool rc = getArrayDesc(array_name, catalogVersion,
                               array_desc, throwException);
        if (!rc) {
            return false;
        }

        version = getLastVersion(array_desc.getId(), catalogVersion);
        if (version == 0) {
            return true;
        }

        ss << array_name << "@" << version;
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDesc(): array_name= " << ss.str());
        return getArrayDesc(ss.str(), catalogVersion,
                            array_desc, throwException);
    }

    bool SystemCatalog::getArrayDesc(
        const string &      arrayName,
        const ArrayID       catalogVersion,
        ArrayDesc &         arrayDesc,
        const bool          throwException)
    {
        try {
            getArrayDesc(arrayName, catalogVersion, arrayDesc);
        } catch (const Exception& e) {
            if (!throwException &&
                e.getLongErrorCode() == SCIDB_LE_ARRAY_DOESNT_EXIST) {
                return false;
            }
            throw;
        }
        return true;
    }

    void SystemCatalog::getArrayDesc(
        const std::string & arrayName,
        const ArrayID       catalogVersion,
        ArrayDesc &         arrayDesc)
    {
        const bool ignoreOrphanAttributes = false;

        boost::function<void()> work1 = boost::bind(
            &SystemCatalog::_getArrayDesc,
            this,
            boost::cref(arrayName),
            catalogVersion,
            ignoreOrphanAttributes,
            boost::ref(arrayDesc));

        boost::function<void()> work2 = boost::bind(
            &Query::runRestartableWork<void, TxnIsolationConflict>,
            work1, _serializedTxnTries);

        Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
    }



void SystemCatalog::_getArrayDesc(const std::string &arrayName,
                                  const ArrayID catalogVersion,
                                  const bool ignoreOrphanAttributes,
                                  ArrayDesc &arrayDesc)
{
    LOG4CXX_TRACE(logger, "SystemCatalog::_getArrayDesc( name = " << arrayName << ")");

    ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
    ScopedWaitTimer timer(PTCW_PG);

    assert(_connection);

    try {
        pqxx::transaction<pqxx::serializable> tr(*_connection);

        _getArrayDesc(arrayName, catalogVersion, ignoreOrphanAttributes, arrayDesc, &tr);

        tr.commit();
    }
    catch (const broken_connection &e)
    {
        throw;
    }
    catch (const sql_error &e)
    {
        throwOnSerializationConflict(e);
        if (isDebug()) {
            const string t = typeid(e).name();
            const string w = e.what();
            _logSqlError(t, w);
            assert(false);
        }
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
    }
    catch (const pqxx::failure &e)
    {
        if (isDebug()) {
            const string t = typeid(e).name();
            const string w = e.what();
            assert(false);
        }
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
    }
}

void SystemCatalog::insertArrayDistribution(uint64_t arrDistId,
                                            const ArrayDistPtr& distribution,
                                            pqxx::basic_transaction* tr)
{
    if (isDebug()) {
        Coordinates offset;
        InstanceID instanceShift(0);
        ArrayDistributionFactory::getTranslationInfo(distribution.get(), offset, instanceShift);
        SCIDB_ASSERT(offset.empty() && instanceShift == 0);
    }

    string sql = "insert into \"array_distribution\" "
    "(id, partition_function, partition_state, redundancy) values ($1, $2, $3, $4)";
    _connection->prepare(sql, sql)
    PQXX_DECL("bigint", treat_direct)
    PQXX_DECL("integer", treat_direct)
    PQXX_DECL("varchar", treat_string)
    PQXX_DECL("integer", treat_direct);

    tr->prepared(sql)
    (arrDistId)
    ((int)distribution->getPartitioningSchema())
    (distribution->getContext())
    ((int)distribution->getRedundancy()).exec();
}

ArrayDistPtr SystemCatalog::getArrayDistribution(uint64_t arrDistId,
                                                 pqxx::basic_transaction* tr)
{
    string sql = "select partition_function, partition_state, redundancy"
    " from \"array_distribution\" where id = $1";

    _connection->prepare(sql, sql) PQXX_DECL("bigint", treat_direct);

    result query_res = tr->prepared(sql)(arrDistId).exec();
    ASSERT_EXCEPTION(query_res.size()>0, "No array distribution found");

    PartitioningSchema ps =
    static_cast<PartitioningSchema>(query_res[0].at("partition_function").as(int()));

    SCIDB_ASSERT(isValidPartitioningSchema(ps));

    std::string state = query_res[0].at("partition_state").as(std::string());

    int redundancy = query_res[0].at("redundancy").as(int());
    SCIDB_ASSERT(redundancy>=0);

    ArrayDistPtr arrDist = ArrayDistributionFactory::getInstance()->construct(ps,redundancy,state);
    ASSERT_EXCEPTION(arrDist, "Unknown array distribution in SysCatalog");
    return arrDist;
}

void SystemCatalog::insertArrayResidency(ArrayID uaid,
                                         const ArrayResPtr& residency,
                                         pqxx::basic_transaction* tr)
{
    const string sql2 = "insert into \"array_residency\" (array_id, instance_id) values ";
    const size_t resSize = residency->size();
    SCIDB_ASSERT(resSize>0);
    static const size_t MAX_ROWS = 500;

    stringstream ss;
    ss << "(" << uaid << ",";
    const string prefix = ss.str();
    for (size_t i = 0; i < resSize; ) {
        size_t j=0;
        ss.str("");
        ss.clear();
        for (; j < MAX_ROWS && i < resSize; ++j, ++i) {
            const InstanceID inst = residency->getPhysicalInstanceAt(i);
            ss << prefix << inst <<")";
            if (j+1 < MAX_ROWS && i+1 < resSize) {
                ss << ",";
            }
        }
        result query_res = tr->exec(sql2 + ss.str());
        ASSERT_EXCEPTION((query_res.affected_rows()==j), "Unable to update array residency");
    }
}

ArrayResPtr SystemCatalog::getArrayResidency(ArrayID uaid,
                                             pqxx::basic_transaction* tr)
{
    string sql = "select instance_id "
    " from \"array_residency\" where array_id = $1 order by instance_id";

    _connection->prepare(sql, sql) PQXX_DECL("bigint", treat_direct);
    result query_res = tr->prepared(sql)(uaid).exec();

    std::vector<InstanceID> instances;

    ASSERT_EXCEPTION(query_res.size()>0, "ArrayResidency is not found in SysCatalog");
    instances.reserve(query_res.size());

    for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
    {
        instances.push_back(i.at("instance_id").as(int64_t()));
    }
    ArrayResPtr residency = createDefaultResidency(PointerRange<InstanceID>(instances));
    return residency;
}

void SystemCatalog::getArrayInfo(const std::string &arrayName,
                                 const ArrayID catalogVersion,
                                 ArrayID& arrId,
                                 string& arrName,
                                 uint64_t& arrDistId,
                                 int& arrFlags,
                                 pqxx::basic_transaction* tr)
{
    assert(_connection);

    string sql = "select id, name, distribution_id, flags from public_arrays where name = $1 and id <= $2";

    _connection->prepare(sql, sql)
        PQXX_DECL("varchar", treat_string)
        PQXX_DECL("bigint", treat_direct);

    result query_res = tr->prepared(sql)
        (arrayName)
        (catalogVersion).exec();
    if (query_res.size() <= 0) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << arrayName;
    }
    arrId    = query_res[0].at("id").as(int64_t());
    assert(arrId<=catalogVersion);
    arrName  = query_res[0].at("name").as(string());
    arrFlags = query_res[0].at("flags").as(int());
    arrDistId   = query_res[0].at("distribution_id").as(uint64_t());
}

void SystemCatalog::_getArrayDesc(const std::string &array_name,
                                  const ArrayID catalogVersion,
                                  const bool ignoreOrphanAttributes,
                                  ArrayDesc &array_desc,
                                  pqxx::basic_transaction* tr)
{
    assert(_connection);
    ArrayID array_id(0);
    ArrayUAID uaid(0);
    VersionID vid(0);
    int flags(0);
    string metadataArrName;
    uint64_t distId(0);
    getArrayInfo(array_name,
                 catalogVersion,
                 array_id,
                 metadataArrName,
                 distId, //should be the same as that of the unversioned array
                 flags,
                 tr);
    assert(metadataArrName == array_name);

    fillArrayIdentifiers(_connection, tr, array_name, array_id, uaid, vid);

    string sql2 = "select id, name, type, flags, default_compression_method, reserve, default_missing_reason, default_value"
        " from \"array_attribute\" where array_id = $1 order by id";
    _connection->prepare(sql2, sql2) PQXX_DECL("bigint", treat_direct);
    result query_res2 = tr->prepared(sql2)(array_id).exec();

    Attributes attributes;
    attributes.reserve(query_res2.size());
    for (result::const_iterator i = query_res2.begin(); i != query_res2.end(); ++i)
    {
        Value defaultValue;
        int missingReason = i.at("default_missing_reason").as(int());
        string defaultValueExpr;
        if (missingReason >= 0) {
            defaultValue.setNull(safe_static_cast<Value::reason>(missingReason));
        } else {
            defaultValueExpr = i.at("default_value").as(string());
            try {
                // Do type check before constructor check in the next if.
                TypeId typeId = i.at("type").as(TypeId());
                defaultValue = Value(TypeLibrary::getType(typeId));

                // Evaluate expression if present and set default value
                if (defaultValueExpr != "")
                {
                    Expression expr = deserializePhysicalExpression(defaultValueExpr);
                    defaultValue = expr.evaluate();
                }
                // Else fallback to null or zero as before
                else
                {
                    if (i.at("flags").as(int16_t()) & AttributeDesc::IS_NULLABLE)
                    {
                        defaultValue.setNull();
                    }
                    else
                    {
                        defaultValue = TypeLibrary::getDefaultValue(typeId);
                    }
                }
            } catch(const scidb::Exception& e) {
                if (ignoreOrphanAttributes &&
                    (e.getLongErrorCode() == SCIDB_LE_TYPE_NOT_REGISTERED ||
                     e.getLongErrorCode() == SCIDB_LE_FUNCTION_NOT_FOUND)) {

                    flags |= ArrayDesc::INVALID;
                    Value tmp;
                    defaultValue.swap(tmp);

                } else { throw; }
            }
        }
        AttributeDesc att(
                          i.at("id").as(AttributeID()),
                          i.at("name").as(string()),
                          i.at("type").as(TypeId()),
                          i.at("flags").as(int16_t()),
                          i.at("default_compression_method").as(uint16_t()),
                          std::set<std::string>(),
                          i.at("reserve").as(int16_t()),
                          &defaultValue,
                          defaultValueExpr
                          );
        attributes.push_back(att);
    }

    // string sql3 = "select name, start, length, chunk_interval, chunk_overlap "
    //          " from \"array_dimension\" where array_id = $1 order by id";
    string sql3 = "select name, startmin, currstart, currend, endmax, chunk_interval, chunk_overlap "
    " from \"array_dimension\" where array_id = $1 order by id";

    _connection->prepare(sql3, sql3) PQXX_DECL("bigint", treat_direct);
    result query_res3 = tr->prepared(sql3)(array_id).exec();

    Dimensions dimensions;
    dimensions.reserve(query_res3.size());
    for (result::const_iterator i = query_res3.begin(); i != query_res3.end(); ++i)
    {
        dimensions.push_back(
                             DimensionDesc(
                                           i.at("name").as(string()),
                                           i.at("startmin").as(int64_t()),
                                           i.at("currstart").as(int64_t()),
                                           i.at("currend").as(int64_t()),
                                           i.at("endmax").as(int64_t()),
                                           i.at("chunk_interval").as(int64_t()),
                                           i.at("chunk_overlap").as(int64_t())
                                           ));
    }

    ArrayDistPtr distribution = getArrayDistribution(distId, tr);
    ArrayResPtr residency = getArrayResidency(uaid, tr);

    ArrayDesc newDesc(array_id, uaid, vid,
                      metadataArrName,
                      attributes,
                      dimensions,
                      distribution,
                      residency,
                      flags);
    array_desc = newDesc;

    SCIDB_ASSERT(array_desc.getUAId()!=0);
    SCIDB_ASSERT(array_desc.getId() <= catalogVersion);
    SCIDB_ASSERT(array_desc.getUAId() <= catalogVersion);
}

    void SystemCatalog::getArrayDesc(const ArrayID array_id, ArrayDesc &array_desc)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDesc( id = " << array_id << ", array_desc )");
        std::shared_ptr<ArrayDesc> desc = getArrayDesc(array_id);
        array_desc = *desc;
    }

    std::shared_ptr<ArrayDesc> SystemCatalog::getArrayDesc(const ArrayID array_id)
    {
        boost::function<std::shared_ptr<ArrayDesc>()> work =
                boost::bind(&SystemCatalog::_getArrayDesc, this, array_id);
        return Query::runRestartableWork<std::shared_ptr<ArrayDesc>, broken_connection>(work, _reconnectTries);
    }

    std::shared_ptr<ArrayDesc> SystemCatalog::_getArrayDesc(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayDesc( id = " << array_id << ")");
        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        LOG4CXX_TRACE(logger, "Failed to find array_id = " << array_id << " locally.");
        assert(_connection);
        std::shared_ptr<ArrayDesc> newDesc;
        try
        {
            work tr(*_connection);
            string sql1 = "select id, name, distribution_id, flags from \"array\" where id = $1";
            _connection->prepare("find-by-id", sql1) PQXX_DECL("bigint", treat_direct);
            result query_res1 = tr.prepared("find-by-id")(array_id).exec();
            if (query_res1.size() <= 0)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) << array_id;
            }

            assert(array_id ==  query_res1[0].at("id").as(uint64_t()));
            string array_name = query_res1[0].at("name").as(string());
            ArrayUAID uaid;
            VersionID vid;
            fillArrayIdentifiers(_connection, &tr, array_name, array_id, uaid, vid);

            string key="GET_ATTR";
            string sql2 = "select id, name, type, flags, default_compression_method, reserve, default_missing_reason, default_value"
                " from \"array_attribute\" where array_id = $1 order by id";
            _connection->prepare(key + sql2, sql2) PQXX_DECL("integer", treat_direct);
            result query_res2 = tr.prepared(key + sql2)(array_id).exec();

            Attributes attributes;
            if (query_res2.size() > 0)
            {
                attributes.reserve(query_res2.size());
                for (result::const_iterator i = query_res2.begin(); i != query_res2.end(); ++i)
                {
                    Value defaultValue;
                    string defaultValueExpr = "";
                    int missingReason = i.at("default_missing_reason").as(int());
                    if (missingReason >= 0) {
                        defaultValue.setNull(safe_static_cast<Value::reason>(missingReason));
                    } else {
                        defaultValueExpr = i.at("default_value").as(string());

                        // Evaluate expression if present and set default value
                        if (defaultValueExpr != "")
                        {
                            Expression expr = deserializePhysicalExpression(defaultValueExpr);
                            defaultValue = expr.evaluate();
                        }
                        // Else fallback to null or zero as before
                        else
                        {
                            TypeId typeId = i.at("type").as(TypeId());
                            defaultValue = Value(TypeLibrary::getType(typeId));
                            if (i.at("flags").as(int16_t()) & AttributeDesc::IS_NULLABLE)
                            {
                                defaultValue.setNull();
                            }
                            else
                            {
                                defaultValue = TypeLibrary::getDefaultValue(typeId);
                            }
                        }
                    }
                    AttributeDesc att(
                        i.at("id").as(AttributeID()),
                        i.at("name").as(string()),
                        i.at("type").as (TypeId()),
                        i.at("flags").as(int16_t()),
                        i.at("default_compression_method").as(uint16_t()),
                        std::set<std::string>(),
                        i.at("reserve").as(int16_t()),
                        &defaultValue,
                        defaultValueExpr
                        );

                    attributes.push_back(att);
                }
            }

            // string sql3 = "select name, start, length, chunk_interval, chunk_overlap "
            //      " from \"array_dimension\" where array_id = $1 order by id";

            string sql3 = "select name, startmin, currstart, currend, endmax, chunk_interval, chunk_overlap "
                " from \"array_dimension\" where array_id = $1 order by id";
            _connection->prepare(sql3, sql3) PQXX_DECL("integer", treat_direct);
            result query_res3 = tr.prepared(sql3)(array_id).exec();

            Dimensions dimensions;
            if (query_res3.size() > 0)
            {
                attributes.reserve(query_res3.size());
                for (result::const_iterator i = query_res3.begin(); i != query_res3.end(); ++i)
                {
                    dimensions.push_back(
                        DimensionDesc(
                            i.at("name").as(string()),
                            i.at("startmin").as(int64_t()),
                            i.at("currstart").as(int64_t()),
                            i.at("currend").as(int64_t()),
                            i.at("endmax").as(int64_t()),
                            i.at("chunk_interval").as(int64_t()),
                            i.at("chunk_overlap").as(int64_t())));
                }
            }

            //array distribution should be the same as that of the unversioned array
            uint64_t arrDistId = query_res1[0].at("distribution_id").as(uint64_t());

            ArrayDistPtr distribution = getArrayDistribution(arrDistId, &tr);
            ArrayResPtr residency = getArrayResidency(uaid, &tr);

            newDesc = std::shared_ptr<ArrayDesc>(new ArrayDesc(array_id, uaid, vid,
                                                               query_res1[0].at("name").as(string()),
                                                               attributes,
                                                               dimensions,
                                                               distribution,
                                                               residency,
                                                               query_res1[0].at("flags").as(int())));
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        assert(newDesc->getUAId()!=0);

        return newDesc;
    }

    bool SystemCatalog::deleteArray(const string &array_name)
    {
        boost::function<bool()> work1 = boost::bind(&SystemCatalog::_deleteArrayByName,
                                                    this, boost::cref(array_name));
        boost::function<bool()> work2 = boost::bind(&Query::runRestartableWork<bool, TxnIsolationConflict>,
                                                    work1, _serializedTxnTries);
        return Query::runRestartableWork<bool, broken_connection>(work2, _reconnectTries);
    }

    bool SystemCatalog::_deleteArrayByName(const string &array_name)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteArray( name = " << array_name << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);
        bool rc = false;
        try
        {
            pqxx::transaction<pqxx::serializable> tr(*_connection);

            const string deleteArraySql =
               "delete from \"array\" where name = $1 or (name like $1||'@%' and name not like '%:%')";

            _connection->prepare("delete-array-name", deleteArraySql) PQXX_DECL("varchar", treat_string);
            result query_res = tr.prepared("delete-array-name")(array_name).exec();
            rc = (query_res.affected_rows() > 0);
            if (rc) {
                // XXX TODO: these queries are a bit heavy, need to optimize
                const string deleteResidencySql =
                "delete from \"array_residency\" as RES where not exists "
                "( select 1 from \"array\" as ARR where ARR.id=RES.array_id)";

                _connection->prepare(deleteResidencySql, deleteResidencySql);
                result query_res_res = tr.prepared(deleteResidencySql).exec();
                bool rc_res = (query_res_res.affected_rows() > 0);
                SCIDB_ASSERT(!ArrayDesc::isNameUnversioned(array_name) || rc_res);

                const string deleteDistributionSql =
                "delete from \"array_distribution\" as DIST where not exists "
                "( select 1 from \"array\" as ARR where ARR.distribution_id=DIST.id)";

                _connection->prepare(deleteDistributionSql, deleteDistributionSql);
                result query_res_dist = tr.prepared(deleteDistributionSql).exec();
                bool rc_dist = (query_res_dist.affected_rows() > 0);
                //XXX TODO: until we use the uaid to identify distributions
                SCIDB_ASSERT(!ArrayDesc::isNameUnversioned(array_name) || rc_dist);
            }
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);
            if (isDebug() ) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return rc;
    }

    bool SystemCatalog::deleteArrayVersions(const std::string &array_name, const VersionID array_version)
    {
        boost::function<bool()> work1 = boost::bind(&SystemCatalog::_deleteArrayVersions,
                                                   this, boost::cref(array_name), array_version);
        boost::function<bool()> work2 = boost::bind(&Query::runRestartableWork<bool, TxnIsolationConflict>,
                                                    work1, _serializedTxnTries);
        return Query::runRestartableWork<bool, broken_connection>(work2, _reconnectTries);
    }

    bool SystemCatalog::_deleteArrayVersions(const std::string &array_name, const VersionID array_version)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteArrayVersions( array_name = " <<
                      array_name << ", array_version = " << array_version << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);
        bool rc = false;
        try
        {
            pqxx::transaction<pqxx::serializable> tr(*_connection);
            stringstream ss;
            ss << "delete from \"array\" where name like $1||'@%' and id < "
               << "(select id from \"array\" where name like $1||'@'||$2)";
            _connection->prepare("delete-array-versions", ss.str())
                PQXX_DECL("varchar", treat_string)
                PQXX_DECL("integer", treat_direct);
            result query_res =
                tr.prepared("delete-array-versions")(array_name)(array_version).exec();
            rc = (query_res.affected_rows() > 0);
            tr.commit();
        }
        catch (const pqxx::broken_connection &e)
        {
            throw;
        }
        catch (const pqxx::sql_error &e)
        {
            LOG4CXX_ERROR(logger, "SystemCatalog::deleteArrayVersions: postgress exception:"<< e.what());
            LOG4CXX_ERROR(logger, "SystemCatalog::deleteArrayVersions: query:"<< e.query());
            LOG4CXX_ERROR(logger, "SystemCatalog::deleteArrayVersions: "
                          << array_name << " version:" << array_version);

            throwOnSerializationConflict(e);
            if (isDebug() ) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT,
                                   SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return rc;
    }

    void SystemCatalog::deleteArray(const ArrayID array_id)
    {
        boost::function<void()> work1 = boost::bind(&SystemCatalog::_deleteArrayById,
                                                    this, array_id);
        boost::function<void()> work2 = boost::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                                    work1, _serializedTxnTries);
        Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
    }

    void SystemCatalog::_deleteArrayById(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::_deleteArrayById( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);
        try
        {
            pqxx::transaction<pqxx::serializable> tr(*_connection);

            const string sql1 = "delete from \"array\" where id = $1";
            _connection->prepare("delete-array-id", sql1) PQXX_DECL("integer", treat_direct);
            result query_res = tr.prepared("delete-array-id")(array_id).exec();

            if (query_res.affected_rows() > 0) {
                // XXX TODO: these queries are a bit heavy, need to optimize
                const string deleteResidencySql =
                   "delete from \"array_residency\" as RES where not exists "
                   "( select 1 from \"array\" as ARR where ARR.id=RES.array_id)";

                _connection->prepare(deleteResidencySql, deleteResidencySql);
                result query_res_res = tr.prepared(deleteResidencySql).exec();
                LOG4CXX_TRACE(logger, "SystemCatalog::_deleteArrayById(): removed residency rows="
                              << query_res_res.affected_rows());

                const string deleteDistributionSql =
                   "delete from \"array_distribution\" as DIST where not exists "
                   "( select 1 from \"array\" as ARR where ARR.distribution_id=DIST.id)";

                _connection->prepare(deleteDistributionSql, deleteDistributionSql);
                result query_res_dist = tr.prepared(deleteDistributionSql).exec();
                LOG4CXX_TRACE(logger, "SystemCatalog::_deleteArrayById(): removed distribution rows="
                              << query_res_dist.affected_rows());
            }
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);
            if (isDebug() ) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    VersionID SystemCatalog::_createNewVersion(const ArrayID array_id,
                                               const ArrayID version_array_id,
                                               pqxx::basic_transaction* tr)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::_createNewVersion( array_id = " << array_id << ")");
        VersionID version_id = (VersionID)-1;

        string sql = "select COALESCE(max(version_id),0) as vid from \"array_version\" where array_id=$1";
        _connection->prepare(sql, sql)
        PQXX_DECL("bigint", treat_direct);
        result query_res = tr->prepared(sql)(array_id).exec();
        version_id = query_res[0].at("vid").as(uint64_t());

        version_id += 1;

        string sql1 = "insert into \"array_version\"(array_id, version_array_id, version_id, time_stamp)"
        " values ($1, $2, $3, $4)";
        int64_t timestamp = time(NULL);
        _connection->prepare(sql1, sql1)
        PQXX_DECL("bigint", treat_direct)
        PQXX_DECL("bigint", treat_direct)
        PQXX_DECL("bigint", treat_direct)
        PQXX_DECL("bigint", treat_direct);
        tr->prepared(sql1)(array_id)(version_array_id)(version_id)(timestamp).exec();

        return version_id;
    }

    void SystemCatalog::deleteVersion(const ArrayID array_id, const VersionID version_id)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_deleteVersion,
                this, array_id, version_id);
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_deleteVersion(const ArrayID array_id, const VersionID version_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::deleteVersion( array_id = " << array_id << ", version_id = " << version_id << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);

        try
        {
            work tr(*_connection);

            _connection->prepare("delete-version", "delete from \"array_version\" where array_id=$1 and version_id = $2")
                PQXX_DECL("bigint", treat_direct)
                PQXX_DECL("bigint", treat_direct);
            tr.prepared("delete-version")(array_id)(version_id).exec();
            _connection->unprepare("delete-version");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    VersionID SystemCatalog::getLastVersion(const ArrayID array_id,
                                            const ArrayID catalogVersion)
    {
        boost::function<VersionID()> work = boost::bind(&SystemCatalog::_getLastVersion,
                                                        this, array_id, catalogVersion);
        return Query::runRestartableWork<VersionID, broken_connection>(work, _reconnectTries);
    }

    ArrayID SystemCatalog::getOldestArrayVersion(const ArrayID id)
    {
        boost::function<VersionID()> work = boost::bind(&SystemCatalog::_getOldestArrayVersion,
                this, id);
        return Query::runRestartableWork<VersionID, broken_connection>(work, _reconnectTries);
    }

/*
** TODO: We will need to rework this so that we only need to go back to the
**       persistent meta-data store when the local cache is invalidated
**       due to a new version being created with an UPDATE. This can be
**       piggy-backed on a "heartbeat" message, which contains the latest
**       lamport clock value. Meta-data additions to the persistent store
**       will increment the lamport clock value, which will be propogated
**       throughout the instances. When the local instance's clock value < the
**       'global', and the local is obliged to check it's local catalogs,
**       then it can reload meta-data from the persistent store.
*/
VersionID SystemCatalog::_getLastVersion(const ArrayID array_id,
                                         const ArrayID catalogVersion)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getLastVersion( array_id = " << array_id
                      <<", catalogVersion = "<<catalogVersion
                      << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);
        try
        {
            work tr(*_connection);

            string sql = "select COALESCE(max(version_id),0) as vid from \"array_version\" where array_id=$1 and version_array_id<=$2";
            _connection->prepare("select-last-version", sql)
            PQXX_DECL("bigint", treat_direct)
            PQXX_DECL("bigint", treat_direct);
            result query_res = tr.prepared("select-last-version")
            (array_id)
            (catalogVersion).exec();
            VersionID version_id = query_res[0].at("vid").as(uint64_t());
            tr.commit();
            return version_id;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return 0;
    }

    void
    SystemCatalog::getCurrentVersion(QueryLocks& locks)
    {
        boost::function<void()> work1 = boost::bind(&SystemCatalog::_getCurrentVersion,
                                                    this, boost::ref(locks));
        boost::function<void()> work2 = boost::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                                    work1, _serializedTxnTries);
        Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
    }

namespace
{
        /// In Postgres '_' is a special character if used with 'like' & 'similar', so
        /// we need to escape it. Dont need to escape '%', because it is SciDB-reserved.
        void appendEscaped(ostream& ss, const string& str)
        {
            for (string::const_iterator ch=str.begin(); ch != str.end(); ++ch) {
                if (*ch == '_') { ss << "\\"; }
                ss << *ch;
            }
        }
}
    void SystemCatalog::_getCurrentVersion(QueryLocks& locks)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getCurrentVersion()");

        assert(locks.size() > 0);

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);
        try
        {
            string sql =
                "select "
                "  substring(ARR.name,'([^@]+).*') as arr_name, "
                "  max(ARR.id) as max_arr_id "
                "from \"array\" as ARR "
                "where "
                "  ARR.name similar to $1::VARCHAR "
                "group by arr_name";

            // prepare a regexp to find all the array names: (NAME1(@%)*)|(NAME2(@%)*)|...
            typedef map<const string*, LockDesc*, StringPtrLess> Str2LockDescMap;
            Str2LockDescMap name2lock;

            const string arrRegexSuffix = "(@%)*)";
            stringstream ss;
            QueryLocks::iterator l = locks.begin();
            if (l != locks.end()) {
                LockDesc* lock = (*l).get();
                const string& arrayName = lock->getArrayName();
                ss << "(";
                appendEscaped(ss, arrayName);
                ss << arrRegexSuffix;
                ++l;
                name2lock[&arrayName] = lock;
            }
            for (; l != locks.end(); ++l) {
                LockDesc* lock = (*l).get();
                const string& arrayName = lock->getArrayName();
                ss << "|(";
                appendEscaped(ss, arrayName);
                ss << arrRegexSuffix;
                name2lock[&arrayName] = lock;
            }

            const string arrayList = ss.str();

            LOG4CXX_DEBUG(logger, "SystemCatalog::_getCurrentVersion(): regexp = " << arrayList);

            // prepare & execute PG txn
            pqxx::transaction<pqxx::serializable> tr(*_connection);
            _connection->prepare(sql, sql) PQXX_DECL("varchar", treat_string);
            result query_res = tr.prepared(sql)(arrayList).exec();

            // update the query locks with the max catalog array ids
            assert(locks.size()>=query_res.size());
            assert(name2lock.size()==locks.size());

            for (result::const_iterator i = query_res.begin(); i != query_res.end();  ++i)
            {
                const ArrayID maxArrayId = i.at("max_arr_id").as(uint64_t());
                assert(maxArrayId>0);
                const string& arrName    = i.at("arr_name").as(string());

                LOG4CXX_TRACE(logger, "SystemCatalog::_getCurrentVersion(): arr_name= " << arrName);
                LOG4CXX_TRACE(logger, "SystemCatalog::_getCurrentVersion(): max_arr_id= " << maxArrayId);

                Str2LockDescMap::const_iterator iter = name2lock.find(&arrName);
                ASSERT_EXCEPTION(iter != name2lock.end(), "SystemCatalog::_getCurrentVersion(): invalid array name");

                LockDesc* lock = (*iter).second;
                assert(lock);
                LOG4CXX_TRACE(logger, "SystemCatalog::_getCurrentVersion(): lock name= " << lock->getArrayName());

                ASSERT_EXCEPTION(lock->isLocked(), "Array lock is not locked");
                ASSERT_EXCEPTION(lock->getArrayCatalogId() == 0, "Array lock catalog ID !=0");
                ASSERT_EXCEPTION(arrName == lock->getArrayName(),
                                 "Catalog array name does not match the name in LockDesc");

                lock->setArrayCatalogId(maxArrayId);
            }
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throwOnSerializationConflict(e);
            if (isDebug() ) {
                const string t = typeid(e).name();
                const string w = e.what();
                _logSqlError(t, w);
                assert(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            if (isDebug() ) {
                const string t = typeid(e).name();
                const string w = e.what();
                assert(false);
            }
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    ArrayID SystemCatalog::_getOldestArrayVersion(const ArrayID id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getOldestArrayVersion( id = " << id << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);
        try
        {
            work tr(*_connection);

            string sql =
                "select COALESCE(min(version_array_id),0) as vid from \"array_version\" where array_id=$1";
            _connection->prepare("select-oldest-version", sql)
                PQXX_DECL("bigint", treat_direct);
            result query_res = tr.prepared("select-oldest-version")(id).exec();
            ArrayID array_version_id = query_res[0].at("vid").as(uint64_t());
            tr.commit();
            return array_version_id;
        }
        catch (const pqxx::broken_connection &e)
        {
            throw;
        }
        catch (const pqxx::sql_error &e)
        {
            LOG4CXX_ERROR(logger, "SystemCatalog::getOldestArrayVersion: postgress exception:"<< e.what());
            LOG4CXX_ERROR(logger, "SystemCatalog::getOldestArrayVersion: query:"<< e.query());
            LOG4CXX_ERROR(logger, "SystemCatalog::getOldestArrayVersion: "
                          << " arrayId:" << id);
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT,
                                   SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return 0;
    }

    VersionID SystemCatalog::lookupVersionByTimestamp(const ArrayID array_id, const uint64_t timestamp)
    {
        boost::function<VersionID()> work = boost::bind(&SystemCatalog::_lookupVersionByTimestamp,
                this, array_id, timestamp);
        return Query::runRestartableWork<VersionID, broken_connection>(work, _reconnectTries);
    }

    VersionID SystemCatalog::_lookupVersionByTimestamp(const ArrayID array_id, const uint64_t timestamp)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::lookupVersionByTimestamp( array_id = " << array_id << ", timestamp = " << timestamp << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        try
        {
            work tr(*_connection);

            string sql = "select COALESCE(max(version_id),0) as vid from \"array_version\" where array_id=$1 and time_stamp <= $2";
            _connection->prepare("select-version-by-timestamp", sql)
                PQXX_DECL("bigint", treat_direct)
                PQXX_DECL("bigint", treat_direct);
            result query_res = tr.prepared("select-version-by-timestamp")(array_id)(timestamp).exec();
            VersionID version_id = query_res[0].at("vid").as(uint64_t());
            tr.commit();
            return version_id;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return 0;
    }

    std::vector<VersionDesc> SystemCatalog::getArrayVersions(const ArrayID array_id)
    {
        boost::function<std::vector<VersionDesc>()> work = boost::bind(&SystemCatalog::_getArrayVersions,
                this, array_id);
        return Query::runRestartableWork<std::vector<VersionDesc>, broken_connection>(work, _reconnectTries);
    }

    std::vector<VersionDesc> SystemCatalog::_getArrayVersions(const ArrayID array_id)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getArrayVersions( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);
        try
        {
            work tr(*_connection);

            string sql = "select \"version_array_id\", \"version_id\", \"time_stamp\" from \"array_version\" where \"array_id\"=$1 order by \"version_id\";";
            _connection->prepare("select-all-versions", sql)
                PQXX_DECL("bigint", treat_direct);
            result query_res = tr.prepared("select-all-versions")(array_id).exec();
            std::vector<VersionDesc> versions(query_res.size());
            int j = 0;
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                versions[j++] = VersionDesc(i.at("version_array_id").as(uint64_t()),
                                            i.at("version_id").as(uint64_t()),
                                            i.at("time_stamp").as(time_t()));
            }
            tr.commit();
            return versions;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return std::vector<VersionDesc>();
    }

    Coordinates SystemCatalog::getHighBoundary(const ArrayID array_id)
    {
        boost::function<Coordinates()> work = boost::bind(&SystemCatalog::_getHighBoundary,
                this, array_id);
        return Query::runRestartableWork<Coordinates, broken_connection>(work, _reconnectTries);
    }

    Coordinates SystemCatalog::_getHighBoundary(const ArrayID array_id)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::getHighBoundary( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);
        try
        {
            work tr(*_connection);

            string sql = "select currEnd from \"array_dimension\" where array_id=$1 order by id";
            _connection->prepare("select-high-boundary", sql)
                PQXX_DECL("bigint", treat_direct);
            result query_res = tr.prepared("select-high-boundary")(array_id).exec();
            Coordinates highBoundary(query_res.size());
            int j = 0;
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                highBoundary[j++] = i.at("currEnd").as(int64_t());
            }

            if (0 == j) {
                throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) <<array_id ;
            }
            tr.commit();
            return highBoundary;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return Coordinates();
    }

    Coordinates SystemCatalog::getLowBoundary(const ArrayID array_id)
    {
        boost::function<Coordinates()> work = boost::bind(&SystemCatalog::_getLowBoundary,
                this, array_id);
        return Query::runRestartableWork<Coordinates, broken_connection>(work, _reconnectTries);
    }

    Coordinates SystemCatalog::_getLowBoundary(const ArrayID array_id)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::getLowBoundary( array_id = " << array_id << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);
        try
        {
            work tr(*_connection);

            string sql = "select currStart from \"array_dimension\" where array_id=$1 order by id";
            _connection->prepare("select-low-boundary", sql)
                PQXX_DECL("bigint", treat_direct);
            result query_res = tr.prepared("select-low-boundary")(array_id).exec();
            Coordinates lowBoundary(query_res.size());
            int j = 0;
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
            {
                lowBoundary[j++] = i.at("currStart").as(int64_t());
            }

            if (0 == j) {
                throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAYID_DOESNT_EXIST) <<array_id ;
            }
            tr.commit();
            return lowBoundary;
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return Coordinates();
    }

    void SystemCatalog::updateArrayBoundaries(ArrayDesc const& desc, PhysicalBoundaries const& bounds)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_updateArrayBoundaries,
                this, boost::cref(desc), boost::ref(bounds));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_updateArrayBoundaries(ArrayDesc const& desc, PhysicalBoundaries const& bounds)
    {
        PhysicalBoundaries trimmed = bounds.trimToDims(desc.getDimensions());
        Coordinates const& low = trimmed.getStartCoords();
        Coordinates const& high = trimmed.getEndCoords();
        ArrayID array_id = desc.getId();

        LOG4CXX_DEBUG(logger, "SystemCatalog::updateArrayBoundaries( array_id = " << desc.getId()
                      << ", low = [" << low << "], high = [" << high << "])");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);
        try
        {
            work tr(*_connection);

            string sql1 = "update \"array_dimension\" set currStart=$1 where array_id=$2 and id=$3 and currStart>$1";
            _connection->prepare("update-low-boundary", sql1)
                PQXX_DECL("bigint", treat_direct)
                PQXX_DECL("bigint", treat_direct)
                PQXX_DECL("int", treat_direct);
            string sql2 = "update \"array_dimension\" set currEnd=$1 where array_id=$2 and id=$3 and currEnd<$1";
            _connection->prepare("update-high-boundary", sql2)
                PQXX_DECL("bigint", treat_direct)
                PQXX_DECL("bigint", treat_direct)
                PQXX_DECL("int", treat_direct);
            for (size_t i = 0, n = low.size(); i < n; i++) {
                tr.prepared("update-low-boundary")(low[i])(array_id)(i).exec();
                tr.prepared("update-high-boundary")(high[i])(array_id)(i).exec();
            }
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    uint32_t SystemCatalog::getNumberOfInstances()
    {
        boost::function<uint32_t()> work = boost::bind(&SystemCatalog::_getNumberOfInstances,
                this);
        return Query::runRestartableWork<uint32_t, broken_connection>(work, _reconnectTries);
    }

    uint32_t SystemCatalog::_getNumberOfInstances()
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getNumberOfInstances()");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);
        uint32_t n_instances;
        try
        {
            work tr(*_connection);

            result query_res = tr.exec("select count(*) as cnt from \"instance\"");
            n_instances = query_res[0].at("cnt").as(uint32_t());
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return n_instances;
    }

    InstanceID SystemCatalog::addInstance(const InstanceDesc &instance,
                                          const std::string& online)
    {
        boost::function<InstanceID()> work = boost::bind(&SystemCatalog::_addInstance,
                                                         this, boost::cref(instance),
                                                         boost::cref(online));
        return Query::runRestartableWork<InstanceID, broken_connection>(work, _reconnectTries);
    }

    InstanceID SystemCatalog::_addInstance(const InstanceDesc &instance,
                                           const std::string& online)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::addInstance( " << instance << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);

        int64_t instance_id = 0;
        try
        {
            work tr(*_connection);

            string sql1 = "insert into \"instance\" (instance_id, host, port, online_since, base_path, server_id, server_instance_id) "
                    " values ( (($5::bigint << 32) | (nextval('instance_id_seq') & (x'00FFFFFFFF'::bigint))), $1, $2, $3, $4, $5, $6)";

            _connection->prepare(sql1, sql1)
                    PQXX_DECL("varchar", treat_string)
                    PQXX_DECL("int", treat_direct)
                    PQXX_DECL("varchar", treat_string)
                    PQXX_DECL("varchar", treat_string)
                    PQXX_DECL("int", treat_direct)
                    PQXX_DECL("int", treat_direct);

            result query_res = tr.prepared(sql1)
                    (instance.getHost())
                    (instance.getPort())
                    (online)
                    (instance.getBasePath())
                    (instance.getServerId())
                    (instance.getServerInstanceId()).exec();

            size_t numRows = query_res.affected_rows();

            ASSERT_EXCEPTION( numRows == 1, "Instance not added");

            string sql2 = "select instance_id from \"instance\" where server_id=$1 and server_instance_id=$2";

            _connection->prepare(sql2, sql2)
                    PQXX_DECL("int", treat_direct)
                    PQXX_DECL("int", treat_direct);

            query_res = tr.prepared(sql2)
                    (instance.getServerId())
                    (instance.getServerInstanceId()).exec();

            numRows = query_res.size();

            ASSERT_EXCEPTION( numRows == 1, "Instance not added");

            instance_id = query_res[0].at("instance_id").as(int64_t());

            ASSERT_EXCEPTION(isValidPhysicalInstance(static_cast<InstanceID>(instance_id)),
                             "Invalid instance ID inserted");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        return instance_id;
    }

    MembershipID SystemCatalog::getInstances(Instances &instances)
    {
        boost::function<MembershipID()> work = boost::bind(&SystemCatalog::_getInstances,
                                                           this, boost::ref(instances), false);
        return Query::runRestartableWork<MembershipID, broken_connection>(work, _reconnectTries);
    }

    MembershipID SystemCatalog::getAllInstances(Instances &instances)
    {
        boost::function<MembershipID()> work = boost::bind(&SystemCatalog::_getInstances,
                                                           this, boost::ref(instances), true);
        return Query::runRestartableWork<MembershipID, broken_connection>(work, _reconnectTries);
    }

    MembershipID SystemCatalog::_getInstances(Instances &instances, bool all)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::getInstances()");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);
        MembershipID membershipId(0);
        try
        {
            work tr(*_connection); //Dont need serializability because of the table lock

            const string lockTableSql = "LOCK TABLE instance";
            tr.exec(lockTableSql);

            const string sql1 = "select id from membership";
            result query_res = tr.exec(sql1);
            ASSERT_EXCEPTION(query_res.size() == 1, "Membership table has more/less than 1 row");
            membershipId = query_res[0].at("id").as(int64_t());

            string sql2 = "select instance_id, membership_id, host, port, date_part('epoch', online_since)::bigint as ts,"
                    " isfinite(online_since) as fin, base_path, server_id, server_instance_id from \"instance\" ";
            if (!all) {
                sql2 += " where membership_id>=0 and isfinite(online_since)";
            }
            sql2 += " order by instance_id";
            query_res = tr.exec(sql2);
            ASSERT_EXCEPTION(query_res.size() > 0, "No instances online");

            if (query_res.size() > 0)
            {
                instances.reserve(query_res.size());
                for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i)
                {
                    double ts = InstanceDesc::INFINITY_TS;
                    if (i.at("fin").as(bool())) {
                        ts = i.at("ts").as(double());
                    }

                    MembershipID mId = i.at("membership_id").as(int64_t());
                    SCIDB_ASSERT (mId < std::numeric_limits<int64_t>::max() ||
                                  mId == InstanceDesc::INVALID_MEMBERSHIP_ID);

                    instances.push_back(
                        InstanceDesc(
                            i.at("instance_id").as(InstanceID()),
                            mId,
                            i.at("host").as(string()),
                            i.at("port").as(uint16_t()),
                            ts,
                            i.at("base_path").as(string()),
                            i.at("server_id").as(uint32_t()),
                            i.at("server_instance_id").as(uint32_t())
                                ));

                }
            }

            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED)
                  << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        LOG4CXX_TRACE(logger, "Retrieved " << instances.size()
                      << " instances from catalogs. MembershipID="
                      << membershipId);
        return membershipId;
    }

    /**
     * Find a particular parameter within a Postgres connection string.
     *
     * @param creds connection string to examine
     * @param key parameter to search for, including trailing '='
     * @return value of parameter
     *
     * @note
     * We don't support spaces around = in key = value pairs.  We also brashly assume that the
     * embedded values won't be quoted and won't contain funny characters... which *should*
     * certainly be true for host, port, dbname, and user parameters.
     */
    string SystemCatalog::_findCredParam(const string& creds, const string& key)
    {
        assert(key.at(key.size() - 1) == '=');
        string::size_type pos = creds.find(key);
        if (pos == string::npos) {
            LOG4CXX_DEBUG(logger, __FUNCTION__ << ": '" << key << "' not found");
            return string();
        }
        string rest(creds.substr(pos));         // "key1=v1 key2=v2 ..."
        rest = rest.substr(rest.find('=') + 1); // "v1 key2=v2 ..."
        if (rest.empty()) {
            // Hilarious.
            LOG4CXX_DEBUG(logger, __FUNCTION__ << ": '" << key << "' is empty");
            return string("''");
        }
        pos = rest.find_first_of(" \t");
        return rest.substr(0, pos); // Logged below in _makeCredentials().
    }

    /**
     * Build a Postgres connection string.
     */
    string SystemCatalog::_makeCredentials()
    {
        // RESIST THE TEMPTATION TO WRITE PASSWORDS INTO THE LOG!!!

        // (We could easily cache the result of all this work in a
        // static: at Postgres connection time we are still running
        // single-threaded, so no locking needed.  But... we are only
        // called once, so why bother.)

        // Backward compatibility: if given a password, use it... but complain.
        string creds = Config::getInstance()->getOption<string>(CONFIG_CATALOG);
        if (creds.find("password=") != string::npos) {
            // Password in cleartext on the command line?!  BAD!!!
            LOG4CXX_WARN(logger, "Postgres password provided in cleartext on command line, how embarrassing!");
            return creds;
        }

        // We must find the password in $HOME/.pgpass ... which *must* have proper access mode.
        struct passwd* pw = ::getpwuid(::getuid());
        if (pw == NULL) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG)
                << "Cannot find my own /etc/passwd entry?!";
        }
        assert(pw->pw_dir);
        string pgpass_file(pw->pw_dir);
        pgpass_file += "/.pgpass";
        struct stat stat;
        int rc = ::stat(pgpass_file.c_str(), &stat);
        if (rc < 0) {
            stringstream ss;
            ss << "Cannot stat('" << pgpass_file << "'): " << ::strerror(errno);
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << ss.str();
        }
        if (!S_ISREG(stat.st_mode) || (stat.st_mode & (S_IRWXG|S_IRWXO))) {
            stringstream ss;
            ss << "Permission check failed on " << pgpass_file;
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << ss.str();
        }

        // Parse the partial creds to figure out what to look for in ~/.pgpass .
        string host(_findCredParam(creds, "host="));
        string port(_findCredParam(creds, "port="));
        string dbname(_findCredParam(creds, "dbname="));
        string user(_findCredParam(creds, "user="));

        // Build a partial .pgpass line to search for.
        // See http://www.postgresql.org/docs/9.3/interactive/libpq-pgpass.html
        stringstream srch;
        srch << host << ':' << port << ':' << dbname << ':' << user << ':';
        string search(srch.str());
        LOG4CXX_DEBUG(logger, __FUNCTION__ << ": Search for '" << search << '\'');

        // Search for it!
        FILE *fp = ::fopen(pgpass_file.c_str(), "r");
        if (fp == NULL) {
            stringstream ss;
            ss << "Cannot fopen('" << pgpass_file << "', 'r'): " << ::strerror(errno);
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << ss.str();
        }
        size_t len = 128;
        char* buf = static_cast<char*>(::malloc(len));
        SCIDB_ASSERT(buf);
        ssize_t nread = 0;
        string password;
        while ((nread = ::getline(&buf, &len, fp)) != EOF) {
            if (buf[nread-1] == '\n') {
                buf[nread-1] = '\0';
            }
            if (0 == ::strncmp(buf, search.c_str(), search.size())) {
                password = &buf[search.size()];
                if (password.empty()) {
                    password = "''";
                } else if (password.find_first_of(" \t") != string::npos) {
                    stringstream ss;
                    ss << '\'' << password << '\'';
                    password = ss.str();
                }
                break;
            }
        }
        if (buf) {
            ::free(buf);
        }
        ::fclose(fp);

        // Did we lose?
        if (password.empty()) {
            stringstream ss;
            ss << "Cannot find " << pgpass_file << " entry for '" << creds << '\'';
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << ss.str();
        }

        // Win!
        creds += " password=" + password;
        return creds;
    }

    void SystemCatalog::connect(bool doUpgrade)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::connect(doUpgrade = " << doUpgrade << ")");

        if (!PQisthreadsafe())
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_LIBPQ_NOT_THREADSAFE);

        try
        {
            _connection = new pqxx::connection(_makeCredentials());

            work tr(*_connection);
            result query_res = tr.exec("select count(*) from pg_tables where tablename = 'cluster'");
            _initialized = query_res[0].at("count").as(bool());

            if (_initialized)
            {
                result query_res = tr.exec("select get_cluster_uuid as uuid from get_cluster_uuid()");
                _uuid = query_res[0].at("uuid").as(string());

                query_res = tr.exec("select count(*) from pg_proc where proname = 'get_metadata_version'");
                if (query_res[0].at("count").as(bool()))
                {
                    query_res = tr.exec("select get_metadata_version as version from get_metadata_version()");
                    _metadataVersion = query_res[0].at("version").as(int());
                }
                else
                {
                    LOG4CXX_WARN(logger, "Cannot find procedure get_metadata_version in catalog. "
                                 "Assuming catalog metadata version is 0");
                    _metadataVersion = 0;
                }
            }
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query()
                << e.what();
        }
        catch (const PGSTD::runtime_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_CONNECT_PG) << e.what();
        }

        if (_initialized && doUpgrade)
        {
            if (_metadataVersion > METADATA_VERSION)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CATALOG_NEWER_THAN_SCIDB)
                        << METADATA_VERSION << _metadataVersion;
            }
            else if (_metadataVersion < METADATA_VERSION)
            {
                if (Config::getInstance()->getOption<bool>(CONFIG_ENABLE_CATALOG_UPGRADE) == false)
                {
                    string const& configName = Config::getInstance()->getOptionName(CONFIG_ENABLE_CATALOG_UPGRADE);
                    ostringstream message;
                    message <<"In order to proceed, SciDB needs to perform an upgrade of the system "<<
                              "catalog. This is not reversible. To confirm, please restart the system "<<
                              "with the setting \'"<<configName<<"\' set to 'true'";
                    LOG4CXX_ERROR(logger, message.str());
                    std::cerr<<message.str()<<"\n";
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_NEED_UPGRADE_CONFIRMATION);
                }

                LOG4CXX_WARN(logger, "Catalog metadata version (" << _metadataVersion
                        << ") lower than SciDB metadata version (" << METADATA_VERSION
                        << "). Trying to upgrade catalog...");

                try
                {
                    work tr(*_connection);
                    sleep(5); //XXX why sleep ?
                    for(int ver = _metadataVersion + 1; ver <= METADATA_VERSION; ++ver)
                    {
                        LOG4CXX_WARN(logger, "Upgrading metadata from " << ver-1 << " to " << ver);
                        string upgradeScript(METADATA_UPGRADES_LIST[ver]);
                        tr.exec(string(METADATA_UPGRADES_LIST[ver]));
                    }
                    tr.commit();
                    _metadataVersion = METADATA_VERSION;
                }
                catch (const sql_error &e)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
                }
                catch (const pqxx::failure &e)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
                }
            }
        }
    }

    bool SystemCatalog::isConnected() const
    {
        if (_connection)
            return _connection->is_open();
        return false;
    }

    void SystemCatalog::addLibrary(const string& libraryName)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_addLibrary,
                this, boost::cref(libraryName));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_addLibrary(const string& libraryName)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::addLibrary( libraryName ='" << libraryName << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);

        try
        {
            work tr(*_connection);

            result query_res = tr.exec("select nextval from nextval('libraries_id_seq')");
            int64_t lid = query_res[0].at("nextval").as(int64_t());

            string sql1 = "insert into \"libraries\"(id, name)"
                " values ($1, $2)";
            _connection->prepare("addLibrary", sql1)
                PQXX_DECL("bigint", treat_direct)
                PQXX_DECL("varchar", treat_string);
            tr.prepared("addLibrary")
                (lid)
                (libraryName).exec();
            _connection->unprepare("addLibrary");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const unique_violation& e)
        {
            // we allow double insertions, to support the case:
            // load_library()
            // unload_library()
            // load_library()
            LOG4CXX_TRACE(logger, "SystemCatalog::addLibrary: unique constraint violation:"
                          << e.what() << ", lib name="<< libraryName);
            return;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }

    void SystemCatalog::getLibraries(vector<string >& libraries)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_getLibraries,
                this, boost::ref(libraries));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_getLibraries(vector< string >& libraries)
    {

        LOG4CXX_TRACE(logger, "SystemCatalog::getLibraries ( &libraries )");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);

        try
        {
            work tr(*_connection);

            string sql1 = "select name from \"libraries\"";
            _connection->prepare("getLibraries", sql1);
            result query_res = tr.prepared("getLibraries").exec();
            for (result::const_iterator i = query_res.begin(); i != query_res.end(); ++i) {
                libraries.push_back(i.at("name").as(string()));
            }
            _connection->unprepare("getLibraries");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
        LOG4CXX_TRACE(logger, "Loaded " << libraries.size() << " libraries.");
    }

    void SystemCatalog::removeLibrary(const string& libraryName)
    {
        boost::function<void()> work = boost::bind(&SystemCatalog::_removeLibrary,
                this, boost::cref(libraryName));
        Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
    }

    void SystemCatalog::_removeLibrary(const string& libraryName)
    {
        LOG4CXX_TRACE(logger, "SystemCatalog::removeLibrary ( " << libraryName << ")");

        ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
        ScopedWaitTimer timer(PTCW_PG);

        assert(_connection);

        try
        {
            work tr(*_connection);
            string sql1 = "delete from \"libraries\" where name = $1";
            _connection->prepare("removeLibrary", sql1)
                PQXX_DECL("varchar", treat_string);
            tr.prepared("removeLibrary")
                (libraryName).exec();
            _connection->unprepare("removeLibrary");
            tr.commit();
        }
        catch (const broken_connection &e)
        {
            throw;
        }
        catch (const sql_error &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
        }
        catch (const pqxx::failure &e)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
        }
    }


    SystemCatalog::SystemCatalog() :
    _initialized(false),
    _connection(NULL),
    _uuid(""),
    _metadataVersion(-1),
    _reconnectTries(Config::getInstance()->getOption<int>(CONFIG_CATALOG_RECONNECT_TRIES)),
    _serializedTxnTries(DEFAULT_SERIALIZED_TXN_TRIES)
    {

    }

    SystemCatalog::~SystemCatalog()
    {
        if (_connection)
        {
            try
            {
                if (isConnected())
                {
                    _connection->disconnect();
                }
                delete _connection;
                _connection = NULL;
            }
            catch (...)
            {
                LOG4CXX_DEBUG(logger, "Error when disconnecting from PostgreSQL.");
            }
        }
    }

int SystemCatalog::getMetadataVersion() const
{
    return _metadataVersion;
}

std::string SystemCatalog::getLockInsertSql(const std::shared_ptr<LockDesc>& lockDesc)
{
   assert(lockDesc);
   ASSERT_EXCEPTION( (lockDesc->getInstanceRole() == LockDesc::COORD ||
                      lockDesc->getInstanceRole() == LockDesc::WORKER),
                     string("Invalid lock role requested: ")+lockDesc->toString());

   string lockInsertSql;
   bool isInvalidRequest(false);

   if (lockDesc->getLockMode() == LockDesc::RD) {
      if (lockDesc->getInstanceRole() == LockDesc::COORD) {
         // COORD_RD
         lockInsertSql = "insert into array_version_lock"
         " (namespace_name, array_name, array_id, query_id, instance_id, array_version_id, "
         "  array_version, coordinator_id, lock_mode)"
         "(select $1::VARCHAR,$2::VARCHAR,$3,$4,$5,$6,$7,$8,$9 where not exists"
         "  (select AVL.array_name from array_version_lock as AVL where"
         "   AVL.namespace_name=$1::VARCHAR and "
         "   AVL.array_name=$2::VARCHAR and "
         "   AVL.lock_mode>$10 and "
         "   AVL.instance_id=AVL.coordinator_id))";

      } else {
          isInvalidRequest = true;
      }
   } else if (lockDesc->getLockMode() == LockDesc::WR ||
              lockDesc->getLockMode() == LockDesc::CRT) {
      if (lockDesc->getInstanceRole() == LockDesc::COORD) {
         // COORD_WR
         lockInsertSql = "insert into array_version_lock"
         " (namespace_name, array_name, array_id, query_id, instance_id, array_version_id, array_version, "
         "  coordinator_id, lock_mode)"
         "(select $1::VARCHAR,$2::VARCHAR,$3,$4,$5,$6,$7,$8,$9 where not exists"
         "  (select AVL.array_name from array_version_lock as AVL where"
         "   AVL.namespace_name=$1::VARCHAR and "
         "   AVL.array_name=$2::VARCHAR and "
         "  (AVL.query_id<>$4 or AVL.coordinator_id<>$8) and "
         "   AVL.lock_mode>$10))";

      } else if (lockDesc->getInstanceRole() == LockDesc::WORKER) {
          if (lockDesc->getLockMode() == LockDesc::CRT) {
              isInvalidRequest = true;
          } else {
              // WORKER_WR
              lockInsertSql = "insert into array_version_lock"
              " (namespace_name, array_name, array_id, query_id, instance_id, array_version_id, array_version, "
              "   coordinator_id, lock_mode)"
              "  (select AVL.namespace_name, AVL.array_name, AVL.array_id, AVL.query_id, $4, "
              "   AVL.array_version_id, AVL.array_version, AVL.coordinator_id, AVL.lock_mode "
              " from array_version_lock as AVL where "
              "   AVL.namespace_name=$1::VARCHAR and "
              "   AVL.array_name=$2::VARCHAR and "
              "   AVL.query_id=$3 and "
              "   AVL.coordinator_id=$5 and "
              "   AVL.coordinator_id=AVL.instance_id and "
              "   (AVL.lock_mode=$6 or AVL.lock_mode=$7))";
          }
      }
   } else if (lockDesc->getLockMode() == LockDesc::RM ||
              lockDesc->getLockMode() == LockDesc::RNF ||
              lockDesc->getLockMode() == LockDesc::XCL) {

       if (lockDesc->getInstanceRole() == LockDesc::COORD) {
           // COORD_XCL
           lockInsertSql = "insert into array_version_lock"
           " (namespace_name, array_name, array_id, query_id, instance_id, array_version_id, "
           "   array_version, coordinator_id, lock_mode)"
           " (select $1::VARCHAR,$2::VARCHAR,$3,$4,$5,$6,$7,$8,$9 where "
           " not exists (select AVL.namespace_name, AVL.array_name from array_version_lock as AVL where"
           "   AVL.namespace_name=$1::VARCHAR and "
           "   AVL.array_name=$2::VARCHAR and "
           "   (AVL.query_id<>$4 or AVL.coordinator_id<>$8)))";
       } else if (lockDesc->getInstanceRole() == LockDesc::WORKER &&
                  lockDesc->getLockMode() == LockDesc::XCL) {
           // WORKER_XCL
           lockInsertSql = "insert into array_version_lock"
           " (namespace_name, array_name, array_id, query_id, instance_id, array_version_id, "
           "   array_version, coordinator_id, lock_mode)"
           " (select AVL.namespace_name, AVL.array_name, AVL.array_id, AVL.query_id, $4, "
           "   AVL.array_version_id, AVL.array_version, AVL.coordinator_id, AVL.lock_mode"
           " from array_version_lock as AVL where "
           "   AVL.namespace_name=$1::VARCHAR and "
           "   AVL.array_name=$2::VARCHAR and "
           "   AVL.query_id=$3 and "
           "   AVL.coordinator_id=$5 and "
           "   AVL.instance_id=AVL.coordinator_id and "
           "   AVL.lock_mode=$6 and "
           "  not exists (select 1 from array_version_lock as AVL2 where "
           "   AVL2.namespace_name=$1::VARCHAR and "
           "   AVL2.array_name=$2::VARCHAR and "
           "   AVL2.query_id=$3 and "
           "   AVL.coordinator_id=$5 and "
           "   AVL2.instance_id=$4))";
       } else {
          isInvalidRequest = true;
      }
   } else {
        isInvalidRequest = true;
    }

   if (isInvalidRequest) {
      assert(false);
      throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_FUNCTION_ARGUMENT)
          << string("Invalid lock requested: ")+lockDesc->toString();
   }

   return lockInsertSql;
}

bool SystemCatalog::lockArray(const std::shared_ptr<LockDesc>& lockDesc, ErrorChecker& errorChecker)
{
    boost::function<bool()> work = boost::bind(&SystemCatalog::_lockArray,
                                               this, boost::cref(lockDesc), boost::ref(errorChecker));
    return Query::runRestartableWork<bool, broken_connection>(work, _reconnectTries);
}

bool SystemCatalog::_lockArray(const std::shared_ptr<LockDesc>& lockDesc, ErrorChecker& errorChecker)
{
   assert(lockDesc);
   LOG4CXX_DEBUG(logger, "SystemCatalog::lockArray: "<<lockDesc->toString());
   try
   {
      assert(_connection);
      string lockInsertSql = getLockInsertSql(lockDesc);

      string lockTableSql = "LOCK TABLE array_version_lock";
      {
          ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
          ScopedWaitTimer timer(PTCW_PG);

          work tr(*_connection);
          size_t affectedRows=0;

          _connection->prepare(lockTableSql, lockTableSql);
          tr.prepared(lockTableSql).exec();

          if (lockDesc->getLockMode() == LockDesc::RD) {

              if (lockDesc->getInstanceRole() == LockDesc::COORD) {
                  string uniquePrefix("COORD_RD_");
                  _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                  PQXX_DECL("varchar", treat_string)    // namespace_name
                  PQXX_DECL("varchar", treat_string)    // array_name
                  PQXX_DECL("bigint", treat_direct)     // array_id
                  PQXX_DECL("bigint", treat_direct)     // query_id
                  PQXX_DECL("bigint", treat_direct)     // instance_id
                  PQXX_DECL("bigint", treat_direct)     // array_version_id
                  PQXX_DECL("bigint", treat_direct)     // array_version
                  PQXX_DECL("bigint", treat_direct)     // coordinator_id
                  PQXX_DECL("integer", treat_direct)    // lock_mode
                  PQXX_DECL("integer", treat_direct) ;  // lock_mode

                  result query_res = tr.prepared(uniquePrefix+lockInsertSql)
                  (lockDesc->getNamespaceName())
                  (lockDesc->getArrayName())
                  (lockDesc->getArrayId())
                  (lockDesc->getQueryId().getId())
                  (lockDesc->getInstanceId())
                  (lockDesc->getArrayVersionId())
                  (lockDesc->getArrayVersion())
                  (lockDesc->getQueryId().getCoordinatorId())
                  ((int)lockDesc->getLockMode())
                  ((int)LockDesc::CRT).exec();
                  affectedRows = query_res.affected_rows();

              } else { assert(false);}
          } else if (lockDesc->getLockMode() == LockDesc::WR
                     || lockDesc->getLockMode() == LockDesc::CRT) {

              if (lockDesc->getInstanceRole() == LockDesc::COORD) {
                  string uniquePrefix("COORD_WR_");
                  _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                  PQXX_DECL("varchar", treat_string)    // namespace_name
                  PQXX_DECL("varchar", treat_string)    // array_name
                  PQXX_DECL("bigint", treat_direct)     // array_id
                  PQXX_DECL("bigint", treat_direct)     // query_id
                  PQXX_DECL("bigint", treat_direct)     // instance_id
                  PQXX_DECL("bigint", treat_direct)     // array_version_id
                  PQXX_DECL("bigint", treat_direct)     // array_version
                  PQXX_DECL("bigint", treat_direct)     // coordinator_id
                  PQXX_DECL("integer", treat_direct)    // lock_mode
                  PQXX_DECL("integer", treat_direct);   // lock_mode

                  result query_res = tr.prepared(uniquePrefix+lockInsertSql)
                  (lockDesc->getNamespaceName())
                  (lockDesc->getArrayName())
                  (lockDesc->getArrayId())
                  (lockDesc->getQueryId().getId())
                  (lockDesc->getInstanceId())
                  (lockDesc->getArrayVersionId())
                  (lockDesc->getArrayVersion())
                  (lockDesc->getQueryId().getCoordinatorId())
                  ((int)lockDesc->getLockMode())
                  ((int)LockDesc::RD).exec();
                  affectedRows = query_res.affected_rows();

              } else if (lockDesc->getInstanceRole() == LockDesc::WORKER) {
                  assert(lockDesc->getLockMode() != LockDesc::CRT);
                  string uniquePrefix("WORKER_WR_");
                  _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                  PQXX_DECL("varchar", treat_string)  // namespace_name
                  PQXX_DECL("varchar", treat_string)  // array_name
                  PQXX_DECL("bigint", treat_direct)   // query_id
                  PQXX_DECL("bigint", treat_direct)   // instance_id
                  PQXX_DECL("bigint", treat_direct)   // coordinator_id
                  PQXX_DECL("integer", treat_direct)  // lock_mode
                  PQXX_DECL("integer", treat_direct); // lock_mode

                  result query_res = tr.prepared(uniquePrefix+lockInsertSql)
                  (lockDesc->getNamespaceName())
                  (lockDesc->getArrayName())
                  (lockDesc->getQueryId().getId())
                  (lockDesc->getInstanceId())
                  (lockDesc->getQueryId().getCoordinatorId())
                  ((int)LockDesc::WR)
                  ((int)LockDesc::CRT).exec();
                  affectedRows = query_res.affected_rows();

                  if (query_res.affected_rows() == 1) {
                      string lockReadSql =
                      "select array_id, array_version_id, array_version "
                      "from array_version_lock "
                      "where "
                      "  namespace_name=$1::VARCHAR and "
                      "  array_name=$2::VARCHAR and "
                      "  query_id=$3 and "
                      "  coordinator_id=$4 and "
                      "  instance_id=$5";

                      _connection->prepare(lockReadSql, lockReadSql)
                      PQXX_DECL("varchar", treat_string)  // namespace_name
                      PQXX_DECL("varchar", treat_string)  // array_name
                      PQXX_DECL("bigint", treat_direct)   // query_id
                      PQXX_DECL("bigint", treat_direct)   // coordinator_id
                      PQXX_DECL("bigint", treat_direct);  // instance_id

                      result query_res_read = tr.prepared(lockReadSql)
                      (lockDesc->getNamespaceName())
                      (lockDesc->getArrayName())
                      (lockDesc->getQueryId().getId())
                      (lockDesc->getQueryId().getCoordinatorId())
                      (lockDesc->getInstanceId()).exec();

                      assert(query_res_read.size() == 1);

                      lockDesc->setArrayVersion(query_res_read[0].at("array_version").as(VersionID()));
                      lockDesc->setArrayId(query_res_read[0].at("array_id").as(ArrayID()));
                      lockDesc->setArrayVersionId(query_res_read[0].at("array_version_id").as(ArrayID()));
                  }
              } else { assert(false); }
          } else if (lockDesc->getLockMode() == LockDesc::XCL) {

              if (lockDesc->getInstanceRole() == LockDesc::COORD) {
                  string uniquePrefix("COORD_XCL_");
                  _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                  PQXX_DECL("varchar", treat_string)    // namespace_name
                  PQXX_DECL("varchar", treat_string)    // array_name
                  PQXX_DECL("bigint", treat_direct)     // array_id
                  PQXX_DECL("bigint", treat_direct)     // query_id
                  PQXX_DECL("bigint", treat_direct)     // instance_id
                  PQXX_DECL("bigint", treat_direct)     // array_version_id
                  PQXX_DECL("bigint", treat_direct)     // array_version
                  PQXX_DECL("bigint", treat_direct)     // coordinator_id
                  PQXX_DECL("integer", treat_direct);   // lock_mode

                  result query_res = tr.prepared(uniquePrefix+lockInsertSql)
                  (lockDesc->getNamespaceName())
                  (lockDesc->getArrayName())
                  (lockDesc->getArrayId())
                  (lockDesc->getQueryId().getId())
                  (lockDesc->getInstanceId())
                  (lockDesc->getArrayVersionId())
                  (lockDesc->getArrayVersion())
                  (lockDesc->getQueryId().getCoordinatorId())
                  ((int)lockDesc->getLockMode()).exec();
                  affectedRows = query_res.affected_rows();

              } else if (lockDesc->getInstanceRole() == LockDesc::WORKER) {

                  string uniquePrefix("WORKER_XCL_");
                  _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                  PQXX_DECL("varchar", treat_string)    // namespace_name
                  PQXX_DECL("varchar", treat_string)    // array_name
                  PQXX_DECL("bigint", treat_direct)     // query_id
                  PQXX_DECL("bigint", treat_direct)     // instance_id
                  PQXX_DECL("bigint", treat_direct)     // coordinator_id
                  PQXX_DECL("integer", treat_direct);   // lock_mode

                  pqxx::prepare::invocation invc = tr.prepared(uniquePrefix+lockInsertSql)
                  (lockDesc->getNamespaceName())
                  (lockDesc->getArrayName())
                  (lockDesc->getQueryId().getId())
                  (lockDesc->getInstanceId())
                  (lockDesc->getQueryId().getCoordinatorId())
                  ((int)LockDesc::XCL);

                  result query_res = invc.exec();
                  affectedRows = query_res.affected_rows();

                  // Handle store(blah(scan(tempA)),tempA) or join(tempB,tempB)
                  // in which case both store & scan will try to lock (or two scans)
                  if (affectedRows == 1 || affectedRows == 0)
                  {
                      string lockReadSql =
                      "select array_id, array_version_id, array_version "
                      "from array_version_lock "
                      "where "
                      "  namespace_name=$1::VARCHAR and "
                      "  array_name=$2::VARCHAR and "
                      "  query_id=$3 and "
                      "  coordinator_id=$4 and "
                      "  instance_id=$5";

                      _connection->prepare(lockReadSql, lockReadSql)
                      PQXX_DECL("varchar", treat_string)
                      PQXX_DECL("varchar", treat_string)
                      PQXX_DECL("bigint", treat_direct)
                      PQXX_DECL("bigint", treat_direct)
                      PQXX_DECL("bigint", treat_direct);

                      result query_res_read = tr.prepared(lockReadSql)
                      (lockDesc->getNamespaceName())
                      (lockDesc->getArrayName())
                      (lockDesc->getQueryId().getId())
                      (lockDesc->getQueryId().getCoordinatorId())
                      (lockDesc->getInstanceId()).exec();

                      affectedRows = query_res_read.size();
                      if (affectedRows == 1 ) {
                          lockDesc->setArrayVersion(query_res_read[0].at("array_version").as(VersionID()));
                          lockDesc->setArrayId(query_res_read[0].at("array_id").as(ArrayID()));
                          lockDesc->setArrayVersionId(query_res_read[0].at("array_version_id").as(ArrayID()));
                      } else {
                          ASSERT_EXCEPTION(affectedRows == 0, "Array lock entry not unique on worker");
                      }
                  } else {
                      ASSERT_EXCEPTION_FALSE("Array lock entry not unique on worker");
                  }
              } else { assert(false); }
          } else if (lockDesc->getLockMode() == LockDesc::RM) {

              assert(lockDesc->getInstanceRole() == LockDesc::COORD);

              string uniquePrefix("RM_");
              _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
              PQXX_DECL("varchar", treat_string)
              PQXX_DECL("varchar", treat_string)
              PQXX_DECL("bigint", treat_direct)
              PQXX_DECL("bigint", treat_direct)
              PQXX_DECL("bigint", treat_direct)
              PQXX_DECL("bigint", treat_direct)
              PQXX_DECL("bigint", treat_direct)
              PQXX_DECL("bigint", treat_direct)
              PQXX_DECL("integer", treat_direct);

              result query_res = tr.prepared(uniquePrefix+lockInsertSql)
              (lockDesc->getNamespaceName())
              (lockDesc->getArrayName())
              (lockDesc->getArrayId())
              (lockDesc->getQueryId().getId())
              (lockDesc->getInstanceId())
              (lockDesc->getArrayVersionId())
              (lockDesc->getArrayVersion())
              (lockDesc->getQueryId().getCoordinatorId())
              ((int)lockDesc->getLockMode()).exec();
              affectedRows = query_res.affected_rows();
          }  else if (lockDesc->getLockMode() == LockDesc::RNF) {
              if (lockDesc->getInstanceRole() == LockDesc::COORD) {

                  string uniquePrefix("COORD_RNF_");
                  _connection->prepare(uniquePrefix+lockInsertSql, lockInsertSql)
                  PQXX_DECL("varchar", treat_string)
                  PQXX_DECL("varchar", treat_string)
                  PQXX_DECL("bigint", treat_direct)
                  PQXX_DECL("bigint", treat_direct)
                  PQXX_DECL("bigint", treat_direct)
                  PQXX_DECL("bigint", treat_direct)
                  PQXX_DECL("bigint", treat_direct)
                  PQXX_DECL("bigint", treat_direct)
                  PQXX_DECL("integer", treat_direct);

                  result query_res = tr.prepared(uniquePrefix+lockInsertSql)
                  (lockDesc->getNamespaceName())
                  (lockDesc->getArrayName())
                  (lockDesc->getArrayId())
                  (lockDesc->getQueryId().getId())
                  (lockDesc->getInstanceId())
                  (lockDesc->getArrayVersionId())
                  (lockDesc->getArrayVersion())
                  (lockDesc->getQueryId().getCoordinatorId())
                  ((int)lockDesc->getLockMode()).exec();
                  affectedRows = query_res.affected_rows();
              } else { assert(false); }
          } else {
              assert(false);
          }
          if (affectedRows == 1) {
              tr.commit();
              lockDesc->setLocked(true);
              LOG4CXX_DEBUG(logger, "SystemCatalog::lockArray: locked "<<lockDesc->toString());
              return true;
          }
          if (lockDesc->getInstanceRole() == LockDesc::WORKER &&
              affectedRows != 1) {
              // workers must error out immediately
              assert(affectedRows==0);
              tr.commit();
              return false;
          }
          tr.commit();
      }
      if (errorChecker && !errorChecker()) {
          return false;
      }
      throw LockBusyException(REL_FILE, __FUNCTION__, __LINE__);
   }
   catch (const pqxx::unique_violation &e)
   {
       if (!lockDesc->isLocked()) {
           throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
       }

       // On coordinator we may try to acquire the same lock
       // multiple times. If the lock is already acquired,
       // we should just return success.
       // XXX tigor 6/20/2013 TODO: Technically, just checking isLocked()
       // before running the query should be sufficient.
       // After debugging we should/can switch to doing just that.

       ASSERT_EXCEPTION((lockDesc->getInstanceRole() == LockDesc::COORD),
                        string("On a worker instance the array lock: ")+
                        lockDesc->toString()+
                        string(" cannot be acquired more than once"));
       return true;
   }
   catch (const pqxx::broken_connection &e)
   {
       throw;
   }
   catch (const pqxx::sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::lockArray: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::lockArray: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::lockArray: "
                    << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString()));
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return false;
}

bool SystemCatalog::unlockArray(const std::shared_ptr<LockDesc>& lockDesc)
{
    boost::function<bool()> work = boost::bind(&SystemCatalog::_unlockArray,
            this, boost::cref(lockDesc));
    return Query::runRestartableWork<bool, broken_connection>(work, _reconnectTries);
}

bool SystemCatalog::_unlockArray(const std::shared_ptr<LockDesc>& lockDesc)
{
   assert(lockDesc);
   LOG4CXX_DEBUG(logger, "SystemCatalog::unlockArray: "<<lockDesc->toString());
   bool rc = false;
   try
   {
      assert(_connection);
      string lockDeleteSql(
        "delete from array_version_lock "
        "where "
        "  namespace_name=$1::VARCHAR and "
        "  array_name=$2::VARCHAR and "
        "  query_id=$3 and "
        "  coordinator_id=$4 and "
        "  instance_id=$5");


      {
         ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
         ScopedWaitTimer timer(PTCW_PG);

         work tr(*_connection);

         _connection->prepare(lockDeleteSql, lockDeleteSql)
         PQXX_DECL("varchar", treat_string)
         PQXX_DECL("varchar", treat_string)
         PQXX_DECL("bigint", treat_direct)
         PQXX_DECL("bigint", treat_direct)
         PQXX_DECL("bigint", treat_direct);

         result query_res = tr.prepared(lockDeleteSql)
         (lockDesc->getNamespaceName())
         (lockDesc->getArrayName())
         (lockDesc->getQueryId().getId())
         (lockDesc->getQueryId().getCoordinatorId())
         (lockDesc->getInstanceId()).exec();

         rc = (query_res.affected_rows() == 1);
         tr.commit();
      }
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::unlockArray: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::unlockArray: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::unlockArray: "
                    << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString()));
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return rc;
}

bool SystemCatalog::updateArrayLock(const std::shared_ptr<LockDesc>& lockDesc)
{
    boost::function<bool()> work = boost::bind(&SystemCatalog::_updateArrayLock,
            this, boost::cref(lockDesc));
    return Query::runRestartableWork<bool, broken_connection>(work, _reconnectTries);
}

bool SystemCatalog::_updateArrayLock(const std::shared_ptr<LockDesc>& lockDesc)
{
   assert(lockDesc);

   LOG4CXX_TRACE(logger, "SystemCatalog::updateArrayLock: "<<lockDesc->toString());
   bool rc = false;
   try
   {
      assert(_connection);
      string lockUpdateSql =
        "update array_version_lock "
        "set array_id=$6, array_version_id=$7, array_version=$8, lock_mode=$9 "
        "where "
        "  namespace_name=$1::VARCHAR and "
        "  array_name=$2::VARCHAR and "
        "  query_id=$3 and "
        "  coordinator_id=$4 and "
        "  instance_id=$5";
      {
         ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
         ScopedWaitTimer timer(PTCW_PG);

         work tr(*_connection);

         _connection->prepare(lockUpdateSql, lockUpdateSql)
         PQXX_DECL("varchar", treat_string)
         PQXX_DECL("varchar", treat_string)
         PQXX_DECL("bigint", treat_direct)
         PQXX_DECL("bigint", treat_direct)
         PQXX_DECL("bigint", treat_direct)
         PQXX_DECL("bigint", treat_direct)
         PQXX_DECL("bigint", treat_direct)
         PQXX_DECL("bigint", treat_direct)
         PQXX_DECL("integer", treat_direct);

         result query_res = tr.prepared(lockUpdateSql)
         (lockDesc->getNamespaceName())
         (lockDesc->getArrayName())
         (lockDesc->getQueryId().getId())
         (lockDesc->getQueryId().getCoordinatorId())
         (lockDesc->getInstanceId())
         (lockDesc->getArrayId())
         (lockDesc->getArrayVersionId())
         (lockDesc->getArrayVersion())
         ((int)lockDesc->getLockMode()).exec();

         rc = (query_res.affected_rows() == 1);
         tr.commit();
      }
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::updateArrayLock: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::updateArrayLock: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::updateArrayLock: "
                    << ((!lockDesc) ? string("lock:NULL") : lockDesc->toString()));
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return rc;
}

void SystemCatalog::readArrayLocks(const InstanceID instanceId,
                                   std::list<std::shared_ptr<LockDesc> >& coordLocks,
                                   std::list<std::shared_ptr<LockDesc> >& workerLocks)
{
    boost::function<void()> work = boost::bind(&SystemCatalog::_readArrayLocks,
            this, instanceId, boost::ref(coordLocks), boost::ref(workerLocks));
    Query::runRestartableWork<void, broken_connection>(work, _reconnectTries);
}

void SystemCatalog::_readArrayLocks(const InstanceID instanceId,
                                   std::list<std::shared_ptr<LockDesc> >& coordLocks,
                                   std::list<std::shared_ptr<LockDesc> >& workerLocks)
{
   SCIDB_ASSERT(isValidPhysicalInstance(instanceId));

   ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
   ScopedWaitTimer timer(PTCW_PG);

   assert(_connection);
   try
   {
      work tr(*_connection);

      string sql =
        "select "
        "  namespace_name, array_name, array_id, query_id, array_version_id, "
        "  array_version, coordinator_id, lock_mode "
        "from array_version_lock "
        "where instance_id=$1";

      _connection->prepare(sql, sql) PQXX_DECL("bigint", treat_direct);

      result query_res = tr.prepared(sql)(instanceId).exec();
      size_t size = query_res.size();
      LOG4CXX_TRACE(logger, "SystemCatalog::getArrayLocks: found "<< size <<" locks");

      for (size_t i=0; i < size; ++i) {
          const QueryID qId(query_res[i].at("coordinator_id").as(InstanceID()),
                            query_res[i].at("query_id").as(uint64_t()));
          SCIDB_ASSERT(qId.isValid());

          const LockDesc::InstanceRole role =
             qId.getCoordinatorId() == instanceId ? LockDesc::COORD : LockDesc::WORKER ;

          std::shared_ptr<LockDesc> lock(
            new LockDesc(
                query_res[i].at("namespace_name").as(string()),
                query_res[i].at("array_name").as(string()),
                qId,
                instanceId,
                role,
                static_cast<LockDesc::LockMode>(query_res[i].at("lock_mode").as(int()))
                ));
         lock->setArrayVersion(query_res[i].at("array_version").as(VersionID()));
         lock->setArrayId(query_res[i].at("array_id").as(ArrayID()));
         lock->setArrayVersionId(query_res[i].at("array_version_id").as(ArrayID()));

         if (lock->getInstanceRole() == LockDesc::COORD) {
            coordLocks.push_back(lock);
         } else {
            workerLocks.push_back(lock);
         }
         LOG4CXX_TRACE(logger, lock->toString());
      }
      tr.commit();
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      LOG4CXX_ERROR(logger, "SystemCatalog::readArrayLocks: postgress exception:"<< e.what());
      LOG4CXX_ERROR(logger, "SystemCatalog::readArrayLocks: query:"<< e.query());
      LOG4CXX_ERROR(logger, "SystemCatalog::readArrayLocks: instance ID = " << instanceId);
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
}

uint32_t SystemCatalog::deleteCoordArrayLocks(InstanceID instanceId)
{
    return deleteArrayLocks(instanceId, INVALID_QUERY_ID, LockDesc::COORD);
}

uint32_t SystemCatalog::deleteWorkerArrayLocks(InstanceID instanceId)
{
    return deleteArrayLocks(instanceId, INVALID_QUERY_ID, LockDesc::WORKER);
}

uint32_t SystemCatalog::deleteArrayLocks(InstanceID instanceId, const QueryID& queryId, LockDesc::InstanceRole role)
{
    boost::function<uint32_t()> work = boost::bind(&SystemCatalog::_deleteArrayLocks,
                                                   this, instanceId, queryId, role);
    return Query::runRestartableWork<uint32_t, broken_connection>(work, _reconnectTries);
}

uint32_t SystemCatalog::_deleteArrayLocks(InstanceID instanceId, const QueryID& queryId, LockDesc::InstanceRole role)
{
    LOG4CXX_DEBUG(logger, "SystemCatalog::deleteArrayLocks instanceId = "
                  << instanceId
                  << " role = "<<role
                  << " queryId = "<<queryId);
   size_t numLocksDeleted = 0;
   ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
   ScopedWaitTimer timer(PTCW_PG);

   try
   {
      assert(_connection);

      uint16_t argNum = 1;
      string lockDeleteSql("delete from array_version_lock where instance_id=$1");
      bool isQuerySpecified = queryId.isValid();
      bool isRoleSpecified = (role != LockDesc::INVALID_ROLE);

      result query_res;
      stringstream ss;

      if (isQuerySpecified) {
          ss << " and query_id=$"<< ++argNum;
          ss << " and coordinator_id=$"<< ++argNum;
      }
      std::string uniquePrefix;
      if (isRoleSpecified) {
          if (role == LockDesc::COORD) {
              ss << " and instance_id=coordinator_id";
              uniquePrefix = std::string("COORD_");
          } else {
              SCIDB_ASSERT(role == LockDesc::WORKER);
              ss << " and instance_id!=coordinator_id";
              uniquePrefix = std::string("WORKER_");
          }
      }

      lockDeleteSql += ss.str();

      if (needPreparedParamDecls()) {
          PQXX_DECL_USE (
          pqxx::prepare::declaration decl = _connection->prepare(uniquePrefix+lockDeleteSql,
                                                                 lockDeleteSql);
          decl("bigint", treat_direct);

          if (isQuerySpecified) {
              decl("bigint", treat_direct);
              decl("bigint", treat_direct);
          } )
      } else {
          _connection->prepare(uniquePrefix+lockDeleteSql, lockDeleteSql);
      }

      work tr(*_connection);

      pqxx::prepare::invocation invc = tr.prepared(uniquePrefix+lockDeleteSql);
      invc(instanceId);

      if (isQuerySpecified) {
          invc(queryId.getId());
          invc(queryId.getCoordinatorId());
      }

      query_res = invc.exec();

      numLocksDeleted = query_res.affected_rows();

      LOG4CXX_TRACE(logger, "SystemCatalog::deleteArrayLocks: deleted "
                    << numLocksDeleted
                    <<"locks for instance " << instanceId);
      tr.commit();
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return safe_static_cast<uint32_t>(numLocksDeleted);
}

std::shared_ptr<SystemCatalog::LockDesc>
SystemCatalog::checkForCoordinatorLock(
    const string& namespaceName, const string& arrayName, const QueryID& queryId)
{
    boost::function<std::shared_ptr<SystemCatalog::LockDesc>()> work = boost::bind(
        &SystemCatalog::_checkForCoordinatorLock,
        this, boost::cref(namespaceName), boost::cref(arrayName), queryId);
    return Query::runRestartableWork<std::shared_ptr<SystemCatalog::LockDesc>, broken_connection>(
        work, _reconnectTries);
}

std::shared_ptr<SystemCatalog::LockDesc>
SystemCatalog::checkForCoordinatorLock(const string& arrayName, const QueryID& queryId)
{
    return checkForCoordinatorLock(std::string("public"), arrayName, queryId);
}

std::shared_ptr<SystemCatalog::LockDesc>
SystemCatalog::_checkForCoordinatorLock(
    const string& namespaceName,
    const string& arrayName,
    const QueryID& queryId)
{
   LOG4CXX_TRACE(logger, "SystemCatalog::checkForCoordinatorLock:"
                 << " namespaceName = " << namespaceName
                 << " arrayName = " << arrayName
                 << " queryID = " << queryId);

   std::shared_ptr<LockDesc> coordLock;

   ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
   ScopedWaitTimer timer(PTCW_PG);

   assert(_connection);
   try
   {
       // Serializable txn should not be necessary because when
       // this check is performed, the same lock is not supposed to be
       // re-inserted. So, the lock row is either in the table or not ...
       work tr(*_connection);

      string sql =
        "select array_id, instance_id, array_version_id, array_version, lock_mode "
        "from array_version_lock "
        "where "
        "  namespace_name=$1::VARCHAR and "
        "  array_name=$2::VARCHAR and "
        "  query_id=$3 and "
        "  coordinator_id=$4 and "
        "  coordinator_id=instance_id";
      _connection->prepare(sql, sql)
      PQXX_DECL("varchar", treat_string)
      PQXX_DECL("varchar", treat_string)
      PQXX_DECL("bigint", treat_direct)
      PQXX_DECL("bigint", treat_direct);

      result query_res = tr.prepared(sql)
        (namespaceName)(arrayName)(queryId.getId())(queryId.getCoordinatorId()).exec();
      size_t size = query_res.size();
      LOG4CXX_TRACE(logger, "SystemCatalog::checkForCoordinatorLock found "<< size <<" locks");

      assert(size < 2);
      if (size > 0) {
          const InstanceID instanceId = query_res[0].at("instance_id").as(InstanceID());
          SCIDB_ASSERT(instanceId == queryId.getCoordinatorId());
          coordLock = std::shared_ptr<LockDesc>(
                new LockDesc(
                    namespaceName,
                    arrayName,
                    queryId,
                    instanceId,
                    LockDesc::COORD,
                    static_cast<LockDesc::LockMode>(query_res[0].at("lock_mode").as(int()))
                    ));
         coordLock->setArrayVersion(query_res[0].at("array_version").as(VersionID()));
         coordLock->setArrayId(query_res[0].at("array_id").as(ArrayID()));
         coordLock->setArrayVersionId(query_res[0].at("array_version_id").as(ArrayID()));
         LOG4CXX_TRACE(logger, coordLock->toString());
      }
      tr.commit();
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const sql_error &e)
   {
      throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
   return coordLock;
}

void SystemCatalog::renameArray(const string &old_array_name, const string &new_array_name)
{
    boost::function<void()> work1 = boost::bind(&SystemCatalog::_renameArray,
                                                this, boost::cref(old_array_name),
                                                boost::cref(new_array_name));
    boost::function<void()> work2 = boost::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                                work1, _serializedTxnTries);
    Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
}

void SystemCatalog::_renameArray(const string &old_array_name, const string &new_array_name)
{
   LOG4CXX_TRACE(logger, "SystemCatalog::renameArray( old name = "
                 << old_array_name << ")"
                 << "new name = " << new_array_name << ")");
    // replace all AAA, AAA@y with BBB, BBB@y correspondingly
    string renameSql =
        "update \"array\" as ARR "
        "set name=regexp_replace(PA.name, '^'||$1::VARCHAR||'(@.+)?$', $2::VARCHAR||E'\\\\1') "
        "from public_arrays as PA "
        "where PA.id=ARR.id";

   ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
   ScopedWaitTimer timer(PTCW_PG);

   assert(_connection);
   try
   {
       pqxx::transaction<pqxx::serializable> tr(*_connection);

      _connection->prepare(renameSql, renameSql)
          PQXX_DECL("varchar", treat_string)
          PQXX_DECL("varchar", treat_string);
      result query_res = tr.prepared(renameSql)(old_array_name)(new_array_name).exec();

      bool rc = (query_res.affected_rows() > 0);
      if (!rc) {
          throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_DOESNT_EXIST) << old_array_name;
      }
      tr.commit();
   }
   catch (const broken_connection &e)
   {
       throw;
   }
   catch (const unique_violation& e)
   {
       LOG4CXX_ERROR(logger, "SystemCatalog::renameArray: unique constraint violation:"<< e.what());
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_ARRAY_ALREADY_EXIST) << new_array_name;
   }
   catch (const sql_error &e)
   {
       throwOnSerializationConflict(e);
       if (isDebug() ) {
           const string t = typeid(e).name();
           const string w = e.what();
           assert(false);
       }
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
   }
   catch (const pqxx::failure &e)
   {
       throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
   }
}

void SystemCatalog::getArrays(std::vector<ArrayDesc> &arrays,
                              bool ignoreOrphanAttributes,
                              bool ignoreVersions,
                              bool allNamespaces)
{
    boost::function<void()> work1 = boost::bind(&SystemCatalog::_getArrays,
                                                this, boost::ref(arrays),
                                                ignoreOrphanAttributes,
                                                ignoreVersions,
                                                allNamespaces);
    boost::function<void()> work2 = boost::bind(&Query::runRestartableWork<void, TxnIsolationConflict>,
                                                    work1, _serializedTxnTries);
    Query::runRestartableWork<void, broken_connection>(work2, _reconnectTries);
}

void SystemCatalog::_getArrays(std::vector<ArrayDesc> &arrays,
                               bool ignoreOrphanAttributes,
                               bool ignoreVersions,
                               bool allNamespaces)
{
    LOG4CXX_TRACE(logger, "SystemCatalog::getArrays(ArrayDescs)");

    stringstream sql;
    if (allNamespaces) {
        sql << "select name from \"array\" ";
        if (ignoreVersions ) { sql << " where name not like '%@%'" ; }
        sql << " order by id";
    } else {
        sql << "select name from public_arrays where name is not null";
        if (ignoreVersions) { sql << " and name not like '%@%'" ; }
        sql << " order by name";
    }

    LOG4CXX_TRACE(logger, "SystemCatalog::getArrays(ArrayDescs): sql=" << sql.str());

    ScopedMutexLock mutexLock(_pgLock, PTCW_PG);
    ScopedWaitTimer timer(PTCW_PG);

    assert(_connection);

    try
    {
        pqxx::transaction<pqxx::serializable> tr(*_connection);
        result query_res = tr.exec(sql.str());

        arrays.clear();
        arrays.resize(query_res.size());
        size_t i=0;
        for (result::const_iterator cursor = query_res.begin();
             cursor != query_res.end(); ++cursor, ++i) {

            const string arrName(cursor.at("name").c_str());
            ArrayDesc& arrDesc = arrays[i];
            _getArrayDesc(arrName, ANY_VERSION, ignoreOrphanAttributes, arrDesc, &tr);
        }

        tr.commit();
    }
    catch (const broken_connection &e)
    {
        throw;
    }
    catch (const sql_error &e)
    {
        throwOnSerializationConflict(e);
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_PG_QUERY_EXECUTION_FAILED) << e.query() << e.what();
    }
    catch (const pqxx::failure &e)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_UNKNOWN_ERROR) << e.what();
    }

    LOG4CXX_TRACE(logger, "Retrieved " << arrays.size() << " arrays from catalogs");
}

void SystemCatalog::throwOnSerializationConflict(const pqxx::sql_error& e)
{
  // libpqxx does not provide SQLSTATE via which the serialization problem can be identified
  // See http://pqxx.org/development/libpqxx/ticket/219, so do the text comparison ...
  static const string SERIALIZATION_CONFLICT
    ("ERROR:  could not serialize access");
  static const string::size_type scSize = SERIALIZATION_CONFLICT.size();

  if (SERIALIZATION_CONFLICT.compare(0, scSize, e.what(), scSize) == 0) {
    LOG4CXX_WARN(logger, "SystemCatalog::_invalidateTempArray: postgress exception:"<< e.what());
    throw TxnIsolationConflict(e.what(), e.query());
  }
}

} // namespace catalog
