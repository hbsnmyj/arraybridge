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
 * @file SciDBExecutor.cpp
 *
 * @author  roman.simakov@gmail.com
 *
 * @brief SciDB API internal implementation to coordinate query execution.
 *
 * This implementation is used by server side of remote protocol and
 * can be loaded directly to user process and transform it into scidb instance.
 * Maybe useful for debugging and embedding scidb into users applications.
 */

#include <stdlib.h>
#include <string>
#include <memory>
#include <boost/bind.hpp>
#include <log4cxx/logger.h>

#include <network/Connection.h>
#include <array/StreamArray.h>
#include <system/Exceptions.h>
#include <query/QueryProcessor.h>
#include <query/Serialize.h>
#include <network/NetworkManager.h>
#include <network/MessageUtils.h>
#include <system/Cluster.h>
#include <util/RWLock.h>

#include "SciDBAPI.h"

using namespace std;
using namespace boost;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.executor"));

/**
 * Engine implementation of the SciDBAPI interface
 */
class SciDBExecutor: public scidb::SciDB
{
    public:
    virtual ~SciDBExecutor() {}

    virtual void* connect(
        const std::string & userName,
        const std::string & userPassword,
        const std::string & connectionString,
        uint16_t port) const
    {
        ASSERT_EXCEPTION_FALSE(
            "connect - not needed, to implement in engine");

        // Shutting down warning
        return NULL;
    }

    virtual void* connect(
        const std::string& connectionString,
        uint16_t port) const
    {
        ASSERT_EXCEPTION_FALSE(
            "connect - not needed, to implement in engine");

        // Shutting down warning
        return NULL;
    }

    void disconnect(void* connection = NULL) const
    {
        ASSERT_EXCEPTION(
            false,
            "disconnect - not needed, to implement in engine");
    }

    void fillUsedPlugins(const ArrayDesc& desc, vector<string>& plugins) const
    {
        for (size_t i = 0; i < desc.getAttributes().size(); i++) {
            const string& libName = TypeLibrary::getTypeLibraries().getObjectLibrary(desc.getAttributes()[i].getType());
            if (libName != "scidb")
                plugins.push_back(libName);
        }
    }

    void prepareQuery(const std::string& queryString,
                      bool afl,
                      const std::string& programOptions,
                      QueryResult& queryResult,
                      void* connection) const
    {
        ASSERT_EXCEPTION(connection, "NULL connection");

        // Parsing query string
        if (Query::getQueryByID(queryResult.queryID, false)) {
            assert(false);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "SciDBExecutor::prepareQuery";
        }

        size_t querySize = queryString.size();
        size_t maxSize = Config::getInstance()->getOption<size_t>(CONFIG_QUERY_MAX_SIZE);
        if (querySize > maxSize) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_TOO_BIG) << querySize << maxSize;
        }

        std::shared_ptr<Connection> &scidb_connection =
            *reinterpret_cast<std::shared_ptr<Connection> *>(connection);
        ASSERT_EXCEPTION(scidb_connection.get()!=nullptr, "NULL scidb_connection");

        std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();
        std::shared_ptr<Query> query = queryProcessor->createQuery(
            queryString,
            queryResult.queryID,
            scidb_connection->getSession());
        ASSERT_EXCEPTION(
            queryResult.queryID == query->getQueryID(),
            "queryResult.queryID == query->getQueryID()");


        StatisticsScope sScope(&query->statistics);
        LOG4CXX_DEBUG(logger, "Parsing query(" << query->getQueryID() << "): " << queryString << "");


        try {
            prepareQueryBeforeLocking(query, queryProcessor, afl, programOptions);
            query->acquireLocks(); //can throw "try-again", i.e. SystemCatalog::LockBusyException
            prepareQueryAfterLocking(query, queryProcessor, afl, queryResult);
        } catch (const scidb::SystemCatalog::LockBusyException& e) {
            e.raise();

        } catch (const Exception& e) {
            query->done(e.copy());
            e.raise();
        }
        LOG4CXX_DEBUG(logger, "Prepared query(" << query->getQueryID() << "): " << queryString << "");
    }

    virtual void retryPrepareQuery(const std::string& queryString,
                                 bool afl,
                                 const std::string& programOptions,
                                 QueryResult& queryResult) const
    {
        std::shared_ptr<Query>  query = Query::getQueryByID(queryResult.queryID);

        assert(queryResult.queryID == query->getQueryID());
        StatisticsScope sScope(&query->statistics);
        try {

            query->retryAcquireLocks();  //can throw "try-again", i.e. SystemCatalog::LockBusyException

            std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

            prepareQueryAfterLocking(query, queryProcessor, afl, queryResult);

        } catch (const scidb::SystemCatalog::LockBusyException& e) {
            e.raise();

        } catch (const Exception& e) {
            query->done(e.copy());
            e.raise();
        }
        LOG4CXX_DEBUG(logger, "Prepared query(" << query->getQueryID() << "): " << queryString << "");
   }

    void prepareQueryBeforeLocking(std::shared_ptr<Query>& query,
                                   std::shared_ptr<QueryProcessor>& queryProcessor,
                                   bool afl,
                                   const std::string& programOptions) const
    {
       query->validate();
       query->programOptions = programOptions;
       query->start();

       // first pass to collect the array names in the query
       queryProcessor->parseLogical(query, afl);

       queryProcessor->inferArrayAccess(query);
   }

    void prepareQueryAfterLocking(std::shared_ptr<Query>& query,
                                  std::shared_ptr<QueryProcessor>& queryProcessor,
                                  bool afl,
                                  QueryResult& queryResult) const
    {
        query->validate();

        // second pass under the array locks
        queryProcessor->parseLogical(query, afl);
        LOG4CXX_TRACE(logger, "Query is parsed");

        const ArrayDesc& desc = queryProcessor->inferTypes(query);
        fillUsedPlugins(desc, queryResult.plugins);
        LOG4CXX_TRACE(logger, "Types of query are inferred");

        std::ostringstream planString;
        query->logicalPlan->toString(planString);
        queryResult.explainLogical = planString.str();

        queryResult.selective = !query->logicalPlan->getRoot()->isDdl();
        queryResult.requiresExclusiveArrayAccess = query->doesExclusiveArrayAccess();

        query->stop();
        LOG4CXX_DEBUG(logger, "The query is prepared");
   }

    void executeQuery(const std::string& queryString, bool afl, QueryResult& queryResult, void* connection) const
    {
        const clock_t startClock = clock();
        assert(queryResult.queryID>0);

        // Executing query string
        std::shared_ptr<Query> query = Query::getQueryByID(queryResult.queryID);
        std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();

        assert(query->getQueryID() == queryResult.queryID);
        StatisticsScope sScope(&query->statistics);

        if (!query->logicalPlan->getRoot()) {
            throw USER_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_WAS_EXECUTED);
        }
        std::ostringstream planString;
        query->logicalPlan->toString(planString);
        queryResult.explainLogical = planString.str();
        // Note: Optimization will be performed while execution
        std::shared_ptr<Optimizer> optimizer =  Optimizer::create();
        bool isDdl = true;
        try {
            query->start();

            while (queryProcessor->optimize(optimizer, query))
            {
                LOG4CXX_DEBUG(logger, "Query is optimized");

                isDdl = query->getCurrentPhysicalPlan()->isDdl();
                query->isDDL = isDdl;
                LOG4CXX_DEBUG(logger, "The physical plan is detected as " << (isDdl ? "DDL" : "DML") );
                if (logger->isDebugEnabled())
                {
                    std::ostringstream planString;
                    query->getCurrentPhysicalPlan()->toString(planString);
                    LOG4CXX_DEBUG(logger, "\n" + planString.str());
                }

                // Execution of single part of physical plan
                queryProcessor->preSingleExecute(query);
                NetworkManager* networkManager = NetworkManager::getInstance();
                const size_t instancesCount = query->getInstancesCount();

                {
                    std::ostringstream planString;
                    query->getCurrentPhysicalPlan()->toString(planString);
                    query->statistics.explainPhysical += planString.str() + ";";

                    // Serialize physical plan and sending it out
                    const string physicalPlan = serializePhysicalPlan(query->getCurrentPhysicalPlan());
                    LOG4CXX_DEBUG(logger, "The query plan is: " << planString.str());
                    LOG4CXX_DEBUG(logger, "The serialized form of the physical plan: queryID="
                                  << queryResult.queryID << ", physicalPlan='" << physicalPlan << "'");
                    std::shared_ptr<MessageDesc> preparePhysicalPlanMsg = std::make_shared<MessageDesc>(mtPreparePhysicalPlan);
                    std::shared_ptr<scidb_msg::PhysicalPlan> preparePhysicalPlanRecord =
                        preparePhysicalPlanMsg->getRecord<scidb_msg::PhysicalPlan>();
                    preparePhysicalPlanMsg->setQueryID(query->getQueryID());
                    preparePhysicalPlanRecord->set_physical_plan(physicalPlan);
                    std::shared_ptr<const InstanceLiveness> queryLiveness(query->getCoordinatorLiveness());
                    serializeQueryLiveness(queryLiveness, preparePhysicalPlanRecord);

                    size_t redundancy = Config::getInstance()->getOption<size_t>(CONFIG_REDUNDANCY);
                    Cluster* cluster = Cluster::getInstance();
                    assert(cluster);
                    std::shared_ptr<const InstanceMembership> membership(cluster->getInstanceMembership());
                    assert(membership);
                    if ((membership->getViewId() != queryLiveness->getViewId()) ||
                        ((instancesCount + redundancy) < membership->getInstances().size())) {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_QUORUM2);
                    }
                    preparePhysicalPlanRecord->set_cluster_uuid(cluster->getUuid());
                    networkManager->broadcastLogical(preparePhysicalPlanMsg);
                    LOG4CXX_DEBUG(logger, "Prepare physical plan was sent out");
                    LOG4CXX_DEBUG(logger, "Waiting confirmation about preparing physical plan in queryID from "
                                  << instancesCount - 1 << " instances")
                }
                try {
                    // Execution of local part of physical plan
                    queryProcessor->execute(query);
                }
                catch (const std::bad_alloc& e) {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_MEMORY_ALLOCATION_ERROR) << e.what();
                }
                LOG4CXX_DEBUG(logger, "Query is executed locally");

                // Wait for results from every instance except itself
                Semaphore::ErrorChecker ec = bind(&Query::validate, query);
                query->results.enter(instancesCount-1, ec);
                LOG4CXX_DEBUG(logger, "The responses are received");
                /**
                 * Check error state
                 */
                query->validate();

                queryProcessor->postSingleExecute(query);
            }
            query->done();
        } catch (const Exception& e) {
            query->done(e.copy());
            e.raise();
        }

        const clock_t stopClock = clock();
        query->statistics.executionTime = (stopClock - startClock) * 1000 / CLOCKS_PER_SEC;

        queryResult.queryID = query->getQueryID();
        queryResult.executionTime = query->statistics.executionTime;
        queryResult.explainPhysical = query->statistics.explainPhysical;
        queryResult.selective = query->getCurrentResultArray().get()!=nullptr;
        if (queryResult.selective) {
            queryResult.array = query->getCurrentResultArray();
        }
        LOG4CXX_DEBUG(logger, "The result of query is returned")
    }

    void cancelQuery(QueryID queryID, void* connection) const
    {
        LOG4CXX_TRACE(logger, "Cancelling query " << queryID)
        std::shared_ptr<Query> query = Query::getQueryByID(queryID);

        StatisticsScope sScope(&query->statistics);

        query->handleCancel();
    }

    void completeQuery(QueryID queryID, void* connection) const
    {
        LOG4CXX_TRACE(logger, "Completing query " << queryID)
        std::shared_ptr<Query> query = Query::getQueryByID(queryID);

        StatisticsScope sScope(&query->statistics);

        query->handleComplete();
    }

    virtual void newClientStart(
        void*                   connection,
        const std::string &     name,
        const std::string &     password) const
    {
        ASSERT_EXCEPTION_FALSE(
            "newClientStart - not needed, to implement in engine");
    }

} _sciDBExecutor;


SciDB& getSciDBExecutor()
{
    return _sciDBExecutor;
}


}
