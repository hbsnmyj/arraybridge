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
 * @file QueryProcessor.cpp
 *
 * @author pavel.velikhov@gmail.com, roman.simakov@gmail.com
 *
 * @brief The interface to the Query Processor in SciDB
 *
 * The QueryProcessor provides the interface to create and execute queries
 * in SciDB.
 * The class that handles all major query processing tasks is QueryProcessor, which
 * is a stateless, reentrant class. The client of the QueryProcessor however uses the
 * Query and QueryResult interfaces instead of the QueryProcessor interface.
 */

#include <query/QueryProcessor.h>

#include <query/QueryPlan.h>
#include <query/RemoteArray.h>
#include <query/optimizer/Optimizer.h>

#include <array/ParallelAccumulatorArray.h>
#include <network/MessageUtils.h>
#include <network/NetworkManager.h>
#include <query/Parser.h>

#include <log4cxx/logger.h>

using std::string;
using std::stringstream;
using std::vector;

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

// Basic QueryProcessor implementation
class QueryProcessorImpl: public QueryProcessor
{
private:
    // Recursive method for executing physical plan
    std::shared_ptr<Array> execute(std::shared_ptr<PhysicalQueryPlanNode> node, std::shared_ptr<Query> query, int depth);
    void preSingleExecute(std::shared_ptr<PhysicalQueryPlanNode> node, std::shared_ptr<Query> query);
    void postSingleExecute(std::shared_ptr<PhysicalQueryPlanNode> node, std::shared_ptr<Query> query);
    // Synchronization methods
    /**
     * Worker notifies coordinator about its state.
     * Coordinator waits for worker notifications.
     */
    void notify(std::shared_ptr<Query>& query, uint64_t timeoutNanoSec=0);

    /**
     * Worker waits for a notification from coordinator.
     * Coordinator sends out notifications to all workers.
     */
    void wait(std::shared_ptr<Query>& query);

public:
    std::shared_ptr<Query> createQuery(
        string                              queryString,
        QueryID                             queryId,
        const std::shared_ptr<Session> &    session);

    void parseLogical(std::shared_ptr<Query> query, bool afl);
    void parsePhysical(const string& plan, std::shared_ptr<Query> query);
    const ArrayDesc& inferTypes(std::shared_ptr<Query> query);
    void setParameters(std::shared_ptr<Query> query, QueryParamMap queryParams);
    bool optimize(std::shared_ptr<Optimizer> optimizer, std::shared_ptr<Query> query);
    void preSingleExecute(std::shared_ptr<Query> query);
    void execute(std::shared_ptr<Query> query);
    void postSingleExecute(std::shared_ptr<Query> query);
    void inferArrayAccess(std::shared_ptr<Query> query);
    std::string inferPermissions(std::shared_ptr<Query> query);
};


std::shared_ptr<Query> QueryProcessorImpl::createQuery(
    string                              queryString,
    QueryID                             queryID,
    const std::shared_ptr<Session> &    session)
{
    SCIDB_ASSERT(queryID.isValid());
    assert(session);

    std::shared_ptr<Query> query = Query::create(queryID);

    LOG4CXX_DEBUG(logger, "QueryProcessorImpl::createQuery("
    << "_qry=" << this << ","
    << "id=" << queryID << ","
    << "session=" << session.get()
    << ")" );

    query->attachSession(session);
    query->queryString = queryString;
    return query;
}


void QueryProcessorImpl::parseLogical(std::shared_ptr<Query> query, bool afl)
{
    query->logicalPlan = std::make_shared<LogicalPlan>(parseStatement(query,afl));
}


void QueryProcessorImpl::parsePhysical(const std::string& plan, std::shared_ptr<Query> query)
{
    assert(!plan.empty());

    stringstream ss;
    ss << plan;
    TextIArchiveQueryPlan ia(ss);
    registerLeafDerivedOperatorParams<TextIArchiveQueryPlan>(ia);

    PhysicalQueryPlanNode* n;
    ia & n;

    std::shared_ptr<PhysicalQueryPlanNode> node = ia._helper._nodes.getSharedPtr(n);
    query->addPhysicalPlan(std::make_shared<PhysicalPlan>(node));
}


const ArrayDesc& QueryProcessorImpl::inferTypes(std::shared_ptr<Query> query)
{
    return query->logicalPlan->inferTypes(query);
}

void QueryProcessorImpl::inferArrayAccess(std::shared_ptr<Query> query)
{
    return query->logicalPlan->inferArrayAccess(query);
}

std::string QueryProcessorImpl::inferPermissions(std::shared_ptr<Query> query)
{
    return query->logicalPlan->inferPermissions(query);
}


bool QueryProcessorImpl::optimize(std::shared_ptr<Optimizer> optimizer, std::shared_ptr<Query> query)
{
   query->addPhysicalPlan(optimizer->optimize(query, query->logicalPlan));

    return !query->getCurrentPhysicalPlan()->empty();
}


void QueryProcessorImpl::setParameters(std::shared_ptr<Query> query, QueryParamMap queryParams)
{
}


// Recursive method for single executing physical plan
void QueryProcessorImpl::preSingleExecute(std::shared_ptr<PhysicalQueryPlanNode> node, std::shared_ptr<Query> query)
{
    Query::validateQueryPtr(query);

    std::shared_ptr<PhysicalOperator> physicalOperator = node->getPhysicalOperator();

    vector<std::shared_ptr<PhysicalQueryPlanNode> >& childs = node->getChildren();
    for (size_t i = 0; i < childs.size(); i++) {
        preSingleExecute(childs[i], query);
    }

    StatisticsScope sScope(&physicalOperator->getStatistics());
    physicalOperator->preSingleExecute(query);
}


void QueryProcessorImpl::preSingleExecute(std::shared_ptr<Query> query)
{
    LOG4CXX_DEBUG(logger, "(Pre)Single executing queryID: " << query->getQueryID())

    preSingleExecute(query->getCurrentPhysicalPlan()->getRoot(), query);
}

void QueryProcessorImpl::postSingleExecute(std::shared_ptr<PhysicalQueryPlanNode> node, std::shared_ptr<Query> query)
{
   Query::validateQueryPtr(query);

    std::shared_ptr<PhysicalOperator> physicalOperator = node->getPhysicalOperator();

    vector<std::shared_ptr<PhysicalQueryPlanNode> >& childs = node->getChildren();
    for (size_t i = 0; i < childs.size(); i++) {
        postSingleExecute(childs[i], query);
    }

    StatisticsScope sScope(&physicalOperator->getStatistics());
    physicalOperator->postSingleExecute(query);
}


void QueryProcessorImpl::postSingleExecute(std::shared_ptr<Query> query)
{
    LOG4CXX_DEBUG(logger, "(Post)Single executing queryID: " << query->getQueryID())

    postSingleExecute(query->getCurrentPhysicalPlan()->getRoot(), query);
}

// Recursive method for executing physical plan
std::shared_ptr<Array> QueryProcessorImpl::execute(std::shared_ptr<PhysicalQueryPlanNode> node, std::shared_ptr<Query> query, int depth)
{
    Query::validateQueryPtr(query);

    std::shared_ptr<PhysicalOperator> physicalOperator = node->getPhysicalOperator();
    physicalOperator->setQuery(query);

    vector<std::shared_ptr<Array> > operatorArguments;
    vector<std::shared_ptr<PhysicalQueryPlanNode> >& childs = node->getChildren();

    StatisticsScope sScope(&physicalOperator->getStatistics());
    if (node->isDdl())
    {
        physicalOperator->executeWrapper(operatorArguments, query);
        return std::shared_ptr<Array>();
    }
    else
    {
        for (size_t i = 0; i < childs.size(); i++)
        {
            std::shared_ptr<Array> arg = execute(childs[i], query, depth+1);
            if (!arg)
                throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_OPERATOR_RESULT);
            SCIDB_ASSERT(!arg->getArrayDesc().isAutochunked()); // Should have been caught below at depth+1!
            operatorArguments.push_back(arg);
        }
        std::shared_ptr<Array> result(physicalOperator->executeWrapper(operatorArguments, query));
        if (result.get() && result->getArrayDesc().isAutochunked()) {
            // Possibly a user-defined operator has not been adapted for autochunking.  (If it's a
            // built-in operator, this is an internal error, *sigh*.)
            throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_AUTOCHUNKED_EXECUTE_RESULT)
                << physicalOperator->getLogicalName() << result->getArrayDesc().getDimensions();
        }
        return result;
    }
}

void QueryProcessorImpl::execute(std::shared_ptr<Query> query)
{
    LOG4CXX_INFO(logger, "Executing query(" << query->getQueryID() << "): " << query->queryString <<
                 "; from program: " << query->programOptions << ";")

    // Make sure ALL instance are ready to run. If the coordinator does not hear
    // from the workers within a timeout, the query is aborted. This is done to prevent a deadlock
    // caused by thread starvation. XXX TODO: In a long term we should solve the problem of thread starvation
    // using for example asynchronous execution techniques.
    int deadlockTimeoutSec = Config::getInstance()->getOption<int>(CONFIG_DEADLOCK_TIMEOUT);
    if (deadlockTimeoutSec <= 0) {
        deadlockTimeoutSec = 10;
    }
    static const uint64_t NANOSEC_PER_SEC = 1000 * 1000 * 1000;
    notify(query, static_cast<uint64_t>(deadlockTimeoutSec)*NANOSEC_PER_SEC);
    wait(query);

    Query::validateQueryPtr(query);

    std::shared_ptr<PhysicalQueryPlanNode> rootNode = query->getCurrentPhysicalPlan()->getRoot();
    std::shared_ptr<Array> currentResultArray = execute(rootNode, query, 0);

    Query::validateQueryPtr(query);

    if (currentResultArray)
    {
        if (Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE) > 1 && currentResultArray->getSupportedAccess() == Array::RANDOM) {
            if (typeid(*currentResultArray) != typeid(ParallelAccumulatorArray)) {
               std::shared_ptr<ParallelAccumulatorArray> paa = std::make_shared<ParallelAccumulatorArray>(currentResultArray);
               currentResultArray = paa;
               paa->start(query);
            }
        } else {
            if (typeid(*currentResultArray) != typeid(AccumulatorArray)) {
                currentResultArray = std::make_shared<AccumulatorArray>(currentResultArray,query);
            }
        }
        if (query->getInstancesCount() > 1 &&
            query->isCoordinator() &&
            !rootNode->isDdl())
        {
            // RemoteMergedArray uses the Query::_currentResultArray as its local (stream) array
            // so make sure to set it in advance
            query->setCurrentResultArray(currentResultArray);
            currentResultArray = RemoteMergedArray::create(currentResultArray->getArrayDesc(),
                    query->getQueryID(), query->statistics);
        }
    }
    query->setCurrentResultArray(currentResultArray);
}

namespace {
bool validateQueryWithTimeout(uint64_t startTime,
                              uint64_t timeout,
                              std::shared_ptr<Query>& query)
{
    bool rc = query->validate();
    assert(rc);
    if (hasExpired(startTime, timeout)) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_RESOURCE_BUSY)
            << "remote query processor not ready to execute, try again later";
    }
    return rc;
}
}

void QueryProcessorImpl::notify(std::shared_ptr<Query>& query, uint64_t timeoutNanoSec)
{
   if (! query->isCoordinator())
    {
        QueryID queryID = query->getQueryID();
        LOG4CXX_DEBUG(logger, "Sending notification in queryID: " << queryID << " to coord instance #" << query->getCoordinatorID())
        std::shared_ptr<MessageDesc> messageDesc = makeNotifyMessage(queryID);

        NetworkManager::getInstance()->send(query->getCoordinatorID(), messageDesc);
    }
    else
    {
        const size_t instancesCount = query->getInstancesCount() - 1;
        LOG4CXX_DEBUG(logger, "Waiting notification in queryID from " << instancesCount << " instances")
        Semaphore::ErrorChecker errorChecker;
        if (timeoutNanoSec > 0) {
            errorChecker = bind(&validateQueryWithTimeout, getTimeInNanoSecs(), timeoutNanoSec, query);
        } else {
            errorChecker = bind(&Query::validate, query);
        }
        query->results.enter(instancesCount, errorChecker);
    }
}

void QueryProcessorImpl::wait(std::shared_ptr<Query>& query)
{
   if (query->isCoordinator())
    {
        QueryID queryID = query->getQueryID();
        LOG4CXX_DEBUG(logger, "Send message from coordinator for waiting instances in queryID: " << query->getQueryID())
        std::shared_ptr<MessageDesc> messageDesc = makeWaitMessage(queryID);

        NetworkManager::getInstance()->broadcastLogical(messageDesc);
    }
    else
    {
        LOG4CXX_DEBUG(logger, "Waiting notification in queryID from coordinator")
        Semaphore::ErrorChecker errorChecker = bind(&Query::validate, query);
        query->results.enter(errorChecker);
    }
}

/**
 * QueryProcessor static method implementation
 */

std::shared_ptr<QueryProcessor> QueryProcessor::create()
{
    return std::shared_ptr<QueryProcessor>(new QueryProcessorImpl());
}


} // namespace
