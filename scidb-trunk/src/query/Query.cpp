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
 * @file Query.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * @brief Implementation of query context methods
 */

#include <time.h>
#include <iostream>
#include <iomanip>

#include <boost/archive/text_iarchive.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/string.hpp>

#include <log4cxx/logger.h>


#include <array/DBArray.h>
#include <query/Query.h>
#include <query/QueryPlan.h>
//#include <query/QueryProcessor.h>
#include <query/RemoteArray.h>
#include <malloc.h>
#include <memory>
#include <network/BaseConnection.h>
#include <network/MessageHandleJob.h>
#include <network/MessageUtils.h>
#include <network/NetworkManager.h>

#include <system/BlockCyclic.h>
#include <system/Cluster.h>
#include <system/Config.h>
#include <system/Exceptions.h>
#include <system/SciDBConfigOptions.h>
#include <system/System.h>
#include <system/SystemCatalog.h>
#include <system/Warnings.h>
#include <smgr/io/ReplicationManager.h>
#include <smgr/io/Storage.h>

#include <util/iqsort.h>
#include <util/LockManager.h>
#ifndef SCIDB_CLIENT
#include <util/MallocStats.h>
#endif
#include <util/session/Session.h>
#include <usr_namespace/NamespacesCommunicator.h>

namespace scidb
{

using namespace std;
using namespace arena;

// Query class implementation
Mutex Query::queriesMutex;
Query::Queries Query::_queries;
uint32_t Query::nextID = 0;
log4cxx::LoggerPtr Query::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr UpdateErrorHandler::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr RemoveErrorHandler::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr BroadcastAbortErrorHandler::_logger = log4cxx::Logger::getLogger("scidb.qproc.processor");
log4cxx::LoggerPtr perfLogger = log4cxx::Logger::getLogger("scidb.qproc.processor.perf");

boost::mt19937 Query::_rng;

thread_local std::shared_ptr<Query> Query::_queryPerThread;

/// Stream input operator for QueryID
/// it is used format the string representation of QueryID
std::istream& operator>>(std::istream& is, QueryID& qId)
{
    InstanceID instanceId(INVALID_INSTANCE);
    uint64_t id(0);
    char dot(0);

    is >> instanceId;
    if (is.fail()) { return is; }

    is >> dot;
    if (is.fail()) { return is; }
    if( dot != '.') {
        is.setstate(std::ios::failbit);
        return is;
    }

    is  >> id;
    if (is.fail()) { return is; }

    qId = QueryID(instanceId,id);
    return is;
}

size_t Query::PendingRequests::increment()
{
    ScopedMutexLock cs(_mutex, PTCW_MUT_OTHER);
    _nReqs += 1;
    return _nReqs;
}

bool Query::PendingRequests::decrement()
{
    ScopedMutexLock cs(_mutex, PTCW_MUT_OTHER);
    if (--_nReqs == 0 && _sync) {
        _sync = false;
        return true;
    }
    return false;
}

bool Query::PendingRequests::test()
{
    ScopedMutexLock cs(_mutex, PTCW_MUT_OTHER);
    if (_nReqs != 0) {
        _sync = true;
        return false;
    }
    return true;
}

std::shared_ptr<Query> Query::createDetached(QueryID queryID)
{
    std::shared_ptr<Query> query = std::make_shared<Query>(queryID);

    const size_t smType = Config::getInstance()->getOption<int>(CONFIG_STAT_MONITOR);
    if (smType) {
        const string& smParams =
           Config::getInstance()->getOption<string>(CONFIG_STAT_MONITOR_PARAMS);
        query->statisticsMonitor = StatisticsMonitor::create(smType, smParams);
    }

    return query;
}

std::shared_ptr<Query>
Query::createFakeQuery(InstanceID coordID,
                       InstanceID localInstanceID,
                       const InstLivenessPtr& liveness,
                       int32_t *longErrorCode)
{
    std::shared_ptr<Query> query = createDetached(QueryID::getFakeQueryId());
    try {
        query->init(coordID, localInstanceID, liveness);

    } catch (const scidb::Exception& e) {
        if (longErrorCode != NULL) {
            *longErrorCode = e.getLongErrorCode();
        } else {
            destroyFakeQuery(query.get());
            throw;
        }
    } catch (const std::exception& e) {
        destroyFakeQuery(query.get());
        throw;
    }
    return query;
}

void Query::destroyFakeQuery(Query* q)
 {
     if (q!=NULL && q->getQueryID().isFake()) {
         try {
             q->handleAbort();
         } catch (scidb::Exception&) { }
     }
 }


Query::Query(const QueryID& queryID):
    _queryID(queryID),
    _instanceID(INVALID_INSTANCE),
    _coordinatorID(INVALID_INSTANCE),
    _error(SYSTEM_EXCEPTION_SPTR(SCIDB_E_NO_ERROR, SCIDB_E_NO_ERROR)),
    _completionStatus(INIT),
    _commitState(UNKNOWN),
    _creationTime(time(NULL)),
    _useCounter(0),
    _doesExclusiveArrayAccess(false),
    _procGrid(NULL),
    _isAutoCommit(false),
    _usecElapsedStart(int64_t(perfTimeGetElapsed()*1.0e6))
{
    for(unsigned i=PTC_FIRST; i< PTC_NUM; ++i) {
        _tcUsecs[i] = 0;
    }
    assert(_tcUsecs[0].is_lock_free());

}

Query::~Query()
{
    LOG4CXX_TRACE(_logger, "Query::~Query() " << _queryID << " "<<(void*)this);
    if (_arena!=NULL) {
        LOG4CXX_DEBUG(_logger, "Query._arena:" << *_arena);
    }

    perfTimeLog();      // log at last possible moment.

    if (statisticsMonitor) {
        statisticsMonitor->pushStatistics(*this);
    }
    delete _procGrid ; _procGrid = NULL ;
}

void Query::init(InstanceID coordID,
                 InstanceID localInstanceID,
                 const InstLivenessPtr& liveness)
{
   assert(liveness);
   assert(localInstanceID != INVALID_INSTANCE);
   {
      ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);

      validate();

      assert( _queryID != INVALID_QUERY_ID);

   /* Install a special arena within the query that all local operator arenas
      should delagate to; we're now using a LeaArena here -  an adaptation of
      Doug Lea's design with a tunable set of bin sizes - because it not only
      supports recycling but also suballocates all of the blocks it hands out
      from large - currently 64 MiB - slabs that are given back to the system
      en masse no later than when the query completes;  the hope here is that
      this reduces the overall fragmentation of the system heap...*/
      {
          assert(_arena == 0);
          stringstream ss ;
          ss << "query "<<_queryID;
          _arena = newArena(Options(ss.str().c_str()).lea(arena::getArena(),64*MiB));
      }

      assert(!_coordinatorLiveness);
      _coordinatorLiveness = liveness;
      assert(_coordinatorLiveness);

      size_t nInstances = _coordinatorLiveness->getNumLive();
      if (nInstances <= 0) {
          throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_LIVENESS_EMPTY);
      }
      assert(_liveInstances.size() == 0);
      _liveInstances.clear();
      _liveInstances.reserve(nInstances);

      const InstanceLiveness::LiveInstances& liveInstances =
         _coordinatorLiveness->getLiveInstances();
      assert(liveInstances.size() == nInstances);
      for ( InstanceLiveness::LiveInstances::const_iterator iter = liveInstances.begin();
        iter != liveInstances.end(); ++iter) {
         _liveInstances.push_back((*iter).getInstanceId());
      }

      _physInstanceID = localInstanceID;
      _instanceID = mapPhysicalToLogical(localInstanceID);
      assert(_instanceID != INVALID_INSTANCE);
      assert(_instanceID < nInstances);

      _defaultArrResidency = //XXX TODO: use _liveInstances instead !
         std::make_shared<MapArrayResidency>(_liveInstances.begin(),
                                             _liveInstances.end());
        SCIDB_ASSERT(_defaultArrResidency->size() == _liveInstances.size());
        SCIDB_ASSERT(_defaultArrResidency->size() > 0);

      if (coordID == INVALID_INSTANCE) {
         _coordinatorID = INVALID_INSTANCE;
         std::shared_ptr<Query::ErrorHandler> ptr(new BroadcastAbortErrorHandler());
         pushErrorHandler(ptr);
      } else {
         _coordinatorID = mapPhysicalToLogical(coordID);
         assert(_coordinatorID < nInstances);
      }

      _receiveSemaphores.resize(nInstances);
      _receiveMessages.resize(nInstances);
      chunkReqs.resize(nInstances);

      Finalizer f = bind(&Query::destroyFinalizer, _1);
      pushFinalizer(f);
      if (coordID == INVALID_INSTANCE) {
          f = bind(&Query::broadcastCommitFinalizer, _1);
          pushFinalizer(f);
      }
      f.clear();

      _errorQueue = NetworkManager::getInstance()->createWorkQueue();
      assert(_errorQueue);
      _errorQueue->start();
      _bufferReceiveQueue = NetworkManager::getInstance()->createWorkQueue();
      assert(_bufferReceiveQueue);
      _bufferReceiveQueue->start();
      _operatorQueue = NetworkManager::getInstance()->createWorkQueue();
      _operatorQueue->stop();
      assert(_operatorQueue);
      _replicationCtx = std::make_shared<ReplicationContext>(shared_from_this(),
                                                             nInstances);
      assert(_replicationCtx);
   }

   // register for notifications
   InstanceLivenessNotification::PublishListener listener =
      boost::bind(&Query::handleLivenessNotification, shared_from_this(), _1);
   _livenessListenerID = InstanceLivenessNotification::addPublishListener(listener);

   LOG4CXX_DEBUG(_logger, "Initialized query (" << _queryID << ")");
}

void Query::broadcastCommitFinalizer(const std::shared_ptr<Query>& q)
{
    assert(q);
    if (q->wasCommitted()) {
        std::shared_ptr<MessageDesc>  msg(makeCommitMessage(q->getQueryID()));
        NetworkManager::getInstance()->broadcastPhysical(msg);
    }
}

std::shared_ptr<Query> Query::insert(const std::shared_ptr<Query>& query)
{
    assert(query);
    SCIDB_ASSERT(query->getQueryID().isValid());

    SCIDB_ASSERT(queriesMutex.isLockedByThisThread());

    pair<Queries::iterator,bool> res =
       _queries.insert( std::make_pair ( query->getQueryID(), query ) );

    if (res.second) {
        const uint32_t nRequests =
           std::max(Config::getInstance()->getOption<int>(CONFIG_REQUESTS),1);
        if (_queries.size() > nRequests) {
            _queries.erase(res.first);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_NO_MEMORY, SCIDB_LE_RESOURCE_BUSY)
                   << "too many queries");
        }
        assert(res.first->second == query);
        LOG4CXX_DEBUG(_logger, "Allocating query (" << query->getQueryID() << ")");
        LOG4CXX_DEBUG(_logger, "Number of allocated queries = " << _queries.size());

        SCIDB_ASSERT(Query::getQueryByID(query->getQueryID(), false) == query);
        return query;
    }
    return res.first->second;
}

QueryID Query::generateID()
{

   const QueryID queryID(Cluster::getInstance()->getLocalInstanceId(),
                         getTimeInNanoSecs());
   LOG4CXX_DEBUG(_logger, "Generated queryID: instanceID="
                 << queryID.getCoordinatorId()
                 << ", time=" <<  queryID.getId()
                 << ", queryID=" << queryID);
   return queryID;
}

std::shared_ptr<Query> Query::create(QueryID queryID, InstanceID instanceId)
{
    SCIDB_ASSERT(queryID.isValid());

    std::shared_ptr<Query> query = createDetached(queryID);
    assert(query);
    assert(query->_queryID == queryID);

    std::shared_ptr<const scidb::InstanceLiveness> myLiveness =
       Cluster::getInstance()->getInstanceLiveness();
    assert(myLiveness);

    query->init(instanceId,
                Cluster::getInstance()->getLocalInstanceId(),
                myLiveness);
    {
       ScopedMutexLock mutexLock(queriesMutex, PTCW_MUT_OTHER);

       if (insert(query) != query) {
           throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_DUPLICATE_QUERY_ID);
       }
    }
    return query;
}

void Query::start()
{
    ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);
    checkNoError();
    if (_completionStatus == INIT) {
        _completionStatus = START;
    }
}

void Query::stop()
{
    ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);
    checkNoError();
    if (_completionStatus == START) {
        _completionStatus = INIT;
    }
}

void Query::pushErrorHandler(const std::shared_ptr<ErrorHandler>& eh)
{
    assert(eh);
    ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);
    checkNoError();
    _errorHandlers.push_back(eh);
}

void Query::pushFinalizer(const Finalizer& f)
{
    assert(f);
    ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);
    checkNoError();
    _finalizers.push_back(f);
}

void Query::done()
{
    bool isCommit = false;
    {
        ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);

        if (!_isAutoCommit &&
            SCIDB_E_NO_ERROR != _error->getLongErrorCode())
        {
            _completionStatus = ERROR;
            _error->raise();
        }
        _completionStatus = OK;
        if (_isAutoCommit) {
            _commitState = COMMITTED; // we have to commit now
            isCommit = true;
        }
    }
    if (isCommit) {
        handleComplete();
    }
}

void Query::done(const std::shared_ptr<Exception>& unwindException)
{
    bool isAbort = false;
    std::shared_ptr<const scidb::Exception> msg;
    {
        ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);
        if (SCIDB_E_NO_ERROR == _error->getLongErrorCode())
        {
            _error = unwindException;
            _error->setQueryId(_queryID);
            msg = _error;
        }
        _completionStatus = ERROR;
        isAbort = (_commitState != UNKNOWN);

        LOG4CXX_DEBUG(_logger, "Query::done: queryID=" << _queryID
                      << ", _commitState=" << _commitState
                      << ", errorCode=" << _error->getLongErrorCode());
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
    if (isAbort) {
        handleAbort();
    }
}

bool Query::doesExclusiveArrayAccess()
{
    return _doesExclusiveArrayAccess;
}

std::shared_ptr<SystemCatalog::LockDesc>
Query::requestLock(std::shared_ptr<SystemCatalog::LockDesc>& requestedLock)
{
    assert(requestedLock);
    assert(!requestedLock->isLocked());
    ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);

    if (requestedLock->getLockMode() > SystemCatalog::LockDesc::RD) {
        _doesExclusiveArrayAccess = true;
    }

    pair<SystemCatalog::QueryLocks::const_iterator, bool> res =
       _requestedLocks.insert(requestedLock);
    if (res.second) {
        assert((*res.first).get() == requestedLock.get());
        LOG4CXX_DEBUG(_logger, "Requested lock: "
                      << (*res.first)->toString()
                      << " inserted");
        return requestedLock;
    }

    if ((*(res.first))->getLockMode() < requestedLock->getLockMode()) {
        _requestedLocks.erase(res.first);
        res = _requestedLocks.insert(requestedLock);
        assert(res.second);
        assert((*res.first).get() == requestedLock.get());
        LOG4CXX_DEBUG(_logger, "Promoted lock: " << (*res.first)->toString() << " inserted");
    }
    return (*(res.first));
}

void Query::handleError(const std::shared_ptr<Exception>& unwindException)
{
    assert(unwindException);
    assert(unwindException->getLongErrorCode() != SCIDB_E_NO_ERROR);
    std::shared_ptr<const scidb::Exception> msg;
    {
        ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = unwindException;
            _error->setQueryId(_queryID);
            msg = _error;
        }
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
}

bool Query::checkFinalState()
{
   ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);
   return ( _finalizers.empty() &&
            ((_completionStatus == INIT &&
              _error->getLongErrorCode() != SCIDB_E_NO_ERROR) ||
             _completionStatus == OK ||
             _completionStatus == ERROR) );
}

void Query::invokeFinalizers(deque<Finalizer>& finalizers)
{
   assert(finalizers.empty() || checkFinalState());
   for (deque<Finalizer>::reverse_iterator riter = finalizers.rbegin();
        riter != finalizers.rend(); riter++) {
      Finalizer& fin = *riter;
      if (!fin) {
         continue;
      }
      try {
         fin(shared_from_this());
      } catch (const std::exception& e) {
         LOG4CXX_FATAL(_logger, "Query (" << _queryID
                       << ") finalizer failed:"
                       << e.what()
                       << "Aborting!");
         abort();
      }
   }
}

void
Query::invokeErrorHandlers(std::deque<std::shared_ptr<ErrorHandler> >& errorHandlers)
{
    for (deque<std::shared_ptr<ErrorHandler> >::reverse_iterator riter = errorHandlers.rbegin();
         riter != errorHandlers.rend(); riter++) {
        std::shared_ptr<ErrorHandler>& eh = *riter;
        try {
            eh->handleError(shared_from_this());
        } catch (const std::exception& e) {
            LOG4CXX_FATAL(_logger, "Query (" << _queryID
                          << ") error handler failed:"
                          << e.what()
                          << "Aborting!");
            abort();
        }
    }
}

void Query::handleAbort()
{
    QueryID queryId = INVALID_QUERY_ID;
    deque<Finalizer> finalizersOnStack;
    deque<std::shared_ptr<ErrorHandler> > errorHandlersOnStack;
    std::shared_ptr<const scidb::Exception> msg;
    {
        ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);

        queryId = _queryID;
        LOG4CXX_DEBUG(_logger, "Query (" << queryId << ") is being aborted");

        if(_commitState == COMMITTED) {
            LOG4CXX_ERROR(_logger, "Query (" << queryId
                          << ") cannot be aborted after commit."
                          << " completion status=" << _completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE)
                   << _queryID << "abort");
        }

        if(_isAutoCommit && _completionStatus == START) {
            LOG4CXX_ERROR(_logger, "Query (" << queryId
                          << ") cannot be aborted when in autoCommit state."
                          << " completion status=" << _completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE)
                   << _queryID << "abort");
        }

        _commitState = ABORTED;

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = (SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_QUERY_CANCELLED)
                      << queryId);
            _error->setQueryId(queryId);
            msg = _error;
        }
        if (_completionStatus == START)
        {
            LOG4CXX_DEBUG(_logger, "Query (" << queryId << ") is still in progress");
            return;
        }
        errorHandlersOnStack.swap(_errorHandlers);
        finalizersOnStack.swap(_finalizers);
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
    if (!errorHandlersOnStack.empty()) {
        LOG4CXX_ERROR(_logger, "Query (" << queryId << ") error handlers ("
                     << errorHandlersOnStack.size() << ") are being executed");
        invokeErrorHandlers(errorHandlersOnStack);
        errorHandlersOnStack.clear();
    }
    invokeFinalizers(finalizersOnStack);
}

void Query::handleCommit()
{
    QueryID queryId = INVALID_QUERY_ID;
    deque<Finalizer> finalizersOnStack;
    std::shared_ptr<const scidb::Exception> msg;
    {
        ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);

        queryId = _queryID;

        LOG4CXX_DEBUG(_logger, "Query (" << _queryID << ") is being committed");

        if (_completionStatus != OK || _commitState == ABORTED) {
            LOG4CXX_ERROR(_logger, "Query (" << _queryID
                          << ") cannot be committed after abort."
                          << " completion status=" << _completionStatus
                          << " commit status=" << _commitState
                          << " error=" << _error->getLongErrorCode());
            assert(false);
            throw (SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_INVALID_COMMIT_STATE)
                   << _queryID << "commit");
        }

        _errorHandlers.clear();

        _commitState = COMMITTED;

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_QUERY_ALREADY_COMMITED);
            (*static_cast<scidb::SystemException*>(_error.get())) << queryId;
            _error->setQueryId(queryId);
            msg = _error;
        }
        finalizersOnStack.swap(_finalizers);
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
    assert(queryId != INVALID_QUERY_ID);
    invokeFinalizers(finalizersOnStack);
}

void Query::handleComplete()
{
    handleCommit();
}

void Query::handleCancel()
{
    handleAbort();
}

void Query::handleLivenessNotification(std::shared_ptr<const InstanceLiveness>& newLiveness)
{
    QueryID thisQueryId;
    InstanceID coordPhysId = INVALID_INSTANCE;
    std::shared_ptr<const scidb::Exception> msg;
    bool isAbort = false;
    {
        ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);

        assert(newLiveness->getVersion() >= _coordinatorLiveness->getVersion());

        if (newLiveness->getVersion() == _coordinatorLiveness->getVersion()) {
            assert(newLiveness->isEqual(*_coordinatorLiveness));
            return;
        }

        LOG4CXX_ERROR(_logger, "Query " << _queryID << " is aborted on changed liveness");

        if (_error->getLongErrorCode() == SCIDB_E_NO_ERROR)
        {
            _error = SYSTEM_EXCEPTION_SPTR(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM);
            _error->setQueryId(_queryID);
            msg = _error;
        }

        if (_coordinatorID != INVALID_INSTANCE) {
            coordPhysId = getPhysicalCoordinatorID();

            InstanceLiveness::InstancePtr newCoordState = newLiveness->find(coordPhysId);
            isAbort = newCoordState->isDead();
            if (!isAbort) {
                InstanceLiveness::InstancePtr oldCoordState = _coordinatorLiveness->find(coordPhysId);
                isAbort = (*newCoordState != *oldCoordState);
            }
        }
        // If the coordinator is dead, we abort the query.
        // There is still a posibility that the coordinator actually has committed.
        // For read queries it does not matter.
        // For write queries UpdateErrorHandler::handleErrorOnWorker() will wait
        // (while holding its own array lock)
        // until the coordinator array lock is released and decide whether to really abort
        // based on the state of the catalog (i.e. if the new version is recorded).

        if (!_errorQueue) {
            LOG4CXX_TRACE(_logger,
                          "Liveness change will not be handled for a deallocated query ("
                          << _queryID << ")");
            isAbort = false;
        }
        thisQueryId = _queryID;
    }
    if (msg) {
        Notification<scidb::Exception> event(msg);
        event.publish();
    }
    if (!isAbort) {
        return;
    }
    try {
        std::shared_ptr<MessageDesc> msg = makeAbortMessage(thisQueryId);

        // HACK (somewhat): set sourceid to coordinator, because only it can issue an abort
        assert(coordPhysId != INVALID_INSTANCE);
        msg->setSourceInstanceID(coordPhysId);

        std::shared_ptr<MessageHandleJob> job = make_shared<ServerMessageHandleJob>(msg);
        std::shared_ptr<WorkQueue> rq = NetworkManager::getInstance()->getRequestQueue();
        std::shared_ptr<WorkQueue> wq = NetworkManager::getInstance()->getWorkQueue();
        job->dispatch(rq, wq);

    } catch (const scidb::Exception& e) {
        LOG4CXX_WARN(_logger, "Failed to abort queryID=" << thisQueryId
                      << " on coordinator liveness change because: " << e.what());
        if (e.getLongErrorCode() != SCIDB_LE_QUERY_NOT_FOUND) {
            SCIDB_ASSERT(false);
            throw;
        } else {
            LOG4CXX_TRACE(_logger,
                          "Liveness change will not be handled for a deallocated query ("
                          << thisQueryId << ")");
        }
    }
}

InstanceID Query::getPhysicalCoordinatorID(bool resolveLocalInstanceId)
{
    InstanceID coord = _coordinatorID;
    if (_coordinatorID == INVALID_INSTANCE) {
        if (!resolveLocalInstanceId) {
            return INVALID_INSTANCE;
        }
        coord = _instanceID;
    }
    assert(_liveInstances.size() > 0);
    assert(_liveInstances.size() > coord);
    return _liveInstances[coord];
}

InstanceID Query::mapLogicalToPhysical(InstanceID instance)
{
   if (instance == INVALID_INSTANCE) {
      return instance;
   }
   ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);  //XXX TODO: remove lock ?
   assert(_liveInstances.size() > 0);
   if (instance >= _liveInstances.size()) {
       throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_INSTANCE_OFFLINE) << instance;
   }
   checkNoError();
   instance = _liveInstances[instance];
   return instance;
}

InstanceID Query::mapPhysicalToLogical(InstanceID instanceID)
{
    ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER); //XXX TODO: remove lock ?
    assert(_liveInstances.size() > 0);
    size_t index=0;
    bool found = bsearch(_liveInstances, instanceID, index);
    if (!found) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_INSTANCE_OFFLINE) << instanceID;
    }
    return index;
}

bool Query::isPhysicalInstanceDead(InstanceID instance)
{
   ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);  //XXX TODO: remove lock ?
   checkNoError();
   InstanceLiveness::InstancePtr instEntry = _coordinatorLiveness->find(instance);
   // non-existent == dead
   return (instEntry==NULL || instEntry->isDead());
}

bool Query::isDistributionDegraded(const ArrayDesc& desc, size_t redundancy)
{
    // Arrays are allowed to exist on different instances.
    ArrayResPtr res = desc.getResidency();
    ArrayDistPtr dist = desc.getDistribution();

    Cluster* cluster = Cluster::getInstance();
    SCIDB_ASSERT(cluster);
    InstMembershipPtr membership = /* Make sure the membership has not changed from under us */
            cluster->getMatchingInstanceMembership(getCoordinatorLiveness()->getMembershipId());

    SCIDB_ASSERT(membership);

    const InstanceLiveness::LiveInstances& liveInstances =
       getCoordinatorLiveness()->getLiveInstances();
    std::set<InstanceID> liveSet; //XXX TODO: use _liveInstances instead !
    for (InstanceLiveness::LiveInstances::const_iterator iter = liveInstances.begin();
         iter != liveInstances.end();
         ++iter) {
        bool inserted = liveSet.insert((*iter).getInstanceId()).second;
        SCIDB_ASSERT(inserted);
    }

    if (res->isEqual(liveSet.begin(),liveSet.end())) {
        // query liveness is the same as array residency
        return false;
    }
    const size_t residencySize = res->size();
    if ((liveSet.size() + redundancy) < residencySize) {
        // not enough redundancy, too many instances are down
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM);
    }

    size_t numLiveInRes = 0;
    for (size_t i=0; i < residencySize; ++i) { //XXX TODO: can we make it O(n) ?
        InstanceID id = res->getPhysicalInstanceAt(i);
        const size_t nFound = liveSet.count(id);
        SCIDB_ASSERT(nFound<2);
        numLiveInRes += nFound;
    }

    if (numLiveInRes == residencySize) {
        // all instances in the residency are alive
        return false;
    }

    SCIDB_ASSERT(numLiveInRes <= residencySize);

    if ((numLiveInRes + redundancy) < residencySize) {
        // not enough redundancy, too many instances containing the array are down
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM);
    }

    return true;
}

bool Query::isDistributionDegradedForRead(const ArrayDesc& desc)
{
    const size_t redundancy = desc.getDistribution()->getRedundancy();
    return isDistributionDegraded(desc, redundancy);
}

bool Query::isDistributionDegradedForWrite(const ArrayDesc& desc)
{
    return isDistributionDegraded(desc, 0);
}

void Query::checkDistributionForRemove(const ArrayDesc& desc)
{
    // We need to make sure that the array residency was determined
    // strictly before the query membership. That will allow us to determine
    // if we have enough instances on-line to remove the array.
    InstMembershipPtr membership =
            Cluster::getInstance()->getInstanceMembership(MAX_MEMBERSHIP_ID);
    if (getCoordinatorLiveness()->getMembershipId() != membership->getId()) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,
                               SCIDB_LE_LIVENESS_MISMATCH);
    }

    //XXX TODO: if the entire residency set is alive, it should be OK to remove as well
    if (getCoordinatorLiveness()->getNumDead()>0) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_EXECUTION,
                               SCIDB_LE_NO_QUORUM);
    }
}

namespace
{
    class ResidencyConstructor
    {
    public:
        ResidencyConstructor(std::vector<InstanceID>& instances)
        :  _instances(instances)
        {}
        void operator() (const InstanceDesc& i)
        {
            _instances.push_back(i.getInstanceId());
        }
    private:
        std::vector<InstanceID>& _instances;
    };
}

ArrayResPtr Query::getDefaultArrayResidencyForWrite()
{
    Cluster* cluster = Cluster::getInstance();
    SCIDB_ASSERT(cluster);

    InstMembershipPtr membership = /* Make sure the membership has not changed from under us */
            cluster->getMatchingInstanceMembership(getCoordinatorLiveness()->getMembershipId());

    SCIDB_ASSERT(membership);
    SCIDB_ASSERT(membership->getNumInstances() == getCoordinatorLiveness()->getNumInstances());

    ArrayResPtr res;
    if (membership->getNumInstances() == getCoordinatorLiveness()->getNumLive()) {
        SCIDB_ASSERT(getCoordinatorLiveness()->getNumDead()==0);
        return getDefaultArrayResidency();
    }

    std::vector<InstanceID> resInstances;
    resInstances.reserve(membership->getNumInstances());
    ResidencyConstructor rc(resInstances);
    membership->visitInstances(rc);
    SCIDB_ASSERT(resInstances.size() == membership->getNumInstances());
    res = std::make_shared<MapArrayResidency>(resInstances);

    return res;
}

ArrayResPtr Query::getDefaultArrayResidency()
{
    SCIDB_ASSERT(_defaultArrResidency);
    return _defaultArrResidency;
}

std::shared_ptr<Query> Query::getQueryByID(QueryID queryID, bool raise)
{
    std::shared_ptr<Query> query;
    ScopedMutexLock mutexLock(queriesMutex, PTCW_MUT_OTHER);

    Queries::const_iterator q = _queries.find(queryID);
    if (q != _queries.end()) {
        return q->second;
    }
    LOG4CXX_DEBUG(_logger, "Query " << queryID << " is not found");
    if (raise) {
        throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_QUERY_NOT_FOUND) << queryID;
    }
    assert(!query);
    return query;
}

void Query::freeQueries()
{
    Queries queries;
    {
        ScopedMutexLock mutexLock(queriesMutex, PTCW_MUT_OTHER);
        queries.swap(_queries);
    }
    for (Queries::iterator q = queries.begin();
         q != queries.end(); ++q) {
        LOG4CXX_DEBUG(_logger, "Deallocating queries: (" << q->second->getQueryID() << ")");
        try {
            q->second->handleAbort();
        } catch (Exception&) { }
    }
}

size_t Query::visitQueries(const Visitor& visit)
{
    ScopedMutexLock mutexLock(queriesMutex, PTCW_MUT_OTHER);

    if (visit)
    {
        for (auto & i : _queries)
        {
            visit(i.second);
        }
    }

    return _queries.size();
}

void dumpMemoryUsage(const QueryID queryId)
{
#ifndef SCIDB_CLIENT
    if (Config::getInstance()->getOption<bool>(CONFIG_OUTPUT_PROC_STATS)) {
        const size_t* mstats = getMallocStats();
        LOG4CXX_DEBUG(Query::_logger,
                      "Stats after query ID ("<<queryId<<"): "
                      <<"Allocated size for PersistentChunks: " << StorageManager::getInstance().getUsedMemSize()
                      <<", allocated size for network messages: " << NetworkManager::getInstance()->getUsedMemSize()
                      <<", MAX size for MemChunks: "<< SharedMemCache::getInstance().getMemThreshold()
                      <<", allocated size for MemChunks: " << SharedMemCache::getInstance().getUsedMemSize()
                      <<", MemChunks were swapped out: " << SharedMemCache::getInstance().getSwapNum()
                      <<", MemChunks were loaded: " << SharedMemCache::getInstance().getLoadsNum()
                      <<", MemChunks were dropped: " << SharedMemCache::getInstance().getDropsNum()
                      <<", number of mallocs: " << (mstats ? mstats[0] : 0)
                      <<", number of frees: "   << (mstats ? mstats[1] : 0));
    }
#endif
}

void Query::destroy()
{

    freeQuery(getQueryID());

    // Any members explicitly destroyed in this method
    // should have an atomic query validating getter method
    // (and setter ?)
    std::shared_ptr<Array> resultArray;
    std::shared_ptr<RemoteMergedArray> mergedArray;
    std::shared_ptr<WorkQueue> bufferQueue;
    std::shared_ptr<WorkQueue> errQueue;
    std::shared_ptr<WorkQueue> opQueue;
    // XXX TODO: remove the context as well to avoid potential memory leak
    std::shared_ptr<ReplicationContext> replicationCtx;
    {
        ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);

        LOG4CXX_TRACE(_logger, "Cleaning up query (" << getQueryID() << ")");

        // Drop all unprocessed messages and cut any circular references
        // (from MessageHandleJob to Query).
        // This should be OK because we broadcast either
        // the error or abort before dropping the messages

        _bufferReceiveQueue.swap(bufferQueue);
        _errorQueue.swap(errQueue);
        _operatorQueue.swap(opQueue);
        _replicationCtx.swap(replicationCtx);

        // Unregister this query from liveness notifications
        InstanceLivenessNotification::removePublishListener(_livenessListenerID);

        // The result array may also have references to this query
        _currentResultArray.swap(resultArray);

        _mergedArray.swap(mergedArray);
    }
    if (bufferQueue) { bufferQueue->stop(); }
    if (errQueue)    { errQueue->stop(); }
    if (opQueue)     { opQueue->stop(); }
    dumpMemoryUsage(getQueryID());
}

void
BroadcastAbortErrorHandler::handleError(const std::shared_ptr<Query>& query)
{
    if (query->getQueryID().isFake()) {
        return;
    }
    if (!query->getQueryID().isValid()) {
        assert(false);
        return;
    }
    if (! query->isCoordinator()) {
        assert(false);
        return;
    }
    LOG4CXX_DEBUG(_logger, "Broadcast ABORT message to all instances for query "
                  << query->getQueryID());
    std::shared_ptr<MessageDesc> abortMessage = makeAbortMessage(query->getQueryID());
    // query may not have the instance map, so broadcast to all
    NetworkManager::getInstance()->broadcastPhysical(abortMessage);
}

void Query::freeQuery(const QueryID& queryID)
{
    ScopedMutexLock mutexLock(queriesMutex, PTCW_MUT_OTHER);
    Queries::iterator i = _queries.find(queryID);
    if (i != _queries.end()) {
        std::shared_ptr<Query> q = i->second;
        LOG4CXX_DEBUG(_logger, "Deallocating query (" << q->getQueryID() << ")");
        _queries.erase(i);
    }
}

bool Query::validate()
{
   bool isShutdown = NetworkManager::isShutdown();
   if (isShutdown) {
       handleAbort();
   }

   ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);
   checkNoError();
   return true;
}

void Query::checkNoError() const
{
    SCIDB_ASSERT(const_cast<Mutex*>(&errorMutex)->isLockedByThisThread());

    if (_isAutoCommit && _commitState == COMMITTED &&
        _error->getLongErrorCode() == SCIDB_LE_QUERY_ALREADY_COMMITED) {
        SCIDB_ASSERT(!_currentResultArray);
        return;
    }

    if (SCIDB_E_NO_ERROR != _error->getLongErrorCode())
    {
        // note: error code can be SCIDB_LE_QUERY_ALREADY_COMMITED
        //       because ParallelAccumulatorArray is started
        //       regardless of whether the client pulls the data
        //       (even in case of mutating queries).
        //       So, the client can request a commit before PAA is done.
        //       An exception will force the backgound PAA threads to exit.
        _error->raise();
    }
}

std::shared_ptr<Array> Query::getCurrentResultArray()
{
    ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);
    validate();
    ASSERT_EXCEPTION ( (!_isAutoCommit || !_currentResultArray),
                       "Auto-commit query cannot return data");
    return _currentResultArray;
}

void Query::setCurrentResultArray(const std::shared_ptr<Array>& array)
{
    ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);
    validate();
    ASSERT_EXCEPTION ( (!_isAutoCommit || !array),
                       "Auto-commit query cannot return data");
    _currentResultArray = array;
}

Query::OperatorContext::~OperatorContext()
{
}

void Query::setOperatorContext(std::shared_ptr<OperatorContext> const& opContext,
                               std::shared_ptr<JobQueue> const& jobQueue)
{
    assert(opContext);
    ScopedMutexLock lock(errorMutex, PTCW_MUT_OTHER);
    assert(validate());
    _operatorContext = opContext;
    assert(_operatorQueue);
    _operatorQueue->start(jobQueue);
}

void Query::unsetOperatorContext()
{
    assert(_operatorContext);
    ScopedMutexLock lock(errorMutex, PTCW_MUT_OTHER);
    assert(validate());
    _operatorContext.reset();
    assert(_operatorQueue);
    _operatorQueue->stop();
}

ostream& writeStatistics(ostream& os, std::shared_ptr<PhysicalQueryPlanNode> node, size_t tab)
{
    string tabStr(tab*4, ' ');
    std::shared_ptr<PhysicalOperator> op = node->getPhysicalOperator();
    os << tabStr << "*" << op->getPhysicalName() << "*: " << endl;
    writeStatistics(os, op->getStatistics(), tab + 1);
    for (size_t i = 0; i < node->getChildren().size(); i++) {
        writeStatistics(os, node->getChildren()[i], tab + 1);
    }
    return os;
}


std::ostream& Query::writeStatistics(std::ostream& os) const
{
    os << endl << "=== Query statistics: ===" << endl;
    scidb::writeStatistics(os, statistics, 0);
    for (size_t i = 0; (i < _physicalPlans.size()) && _physicalPlans[i]->getRoot(); i++)
    {
        os << "=== Statistics of plan #" << i << ": ===" << endl;
        scidb::writeStatistics(os, _physicalPlans[i]->getRoot(), 0);
    }
    os << endl << "=== Current state of system statistics: ===" << endl;
    scidb::writeStatistics(os, StatisticsScope::systemStatistics, 0);
    return os;
}

void Query::postWarning(const Warning& warn)
{
    ScopedMutexLock lock(_warningsMutex, PTCW_MUT_OTHER);
    _warnings.push_back(warn);
}

std::vector<Warning> Query::getWarnings()
{
    ScopedMutexLock lock(_warningsMutex, PTCW_MUT_OTHER);
    return _warnings;
}

void Query::clearWarnings()
{
    ScopedMutexLock lock(_warningsMutex, PTCW_MUT_OTHER);
    _warnings.clear();
}

void RemoveErrorHandler::handleError(const std::shared_ptr<Query>& query)
{
    boost::function<bool()> work = boost::bind(&RemoveErrorHandler::handleRemoveLock, _lock, true);
    Query::runRestartableWork<bool, Exception>(work);
}

bool RemoveErrorHandler::handleRemoveLock(const std::shared_ptr<SystemCatalog::LockDesc>& lock,
                                          bool forceLockCheck)
{
   assert(lock);
   assert(lock->getLockMode() == SystemCatalog::LockDesc::RM);

   std::shared_ptr<SystemCatalog::LockDesc> coordLock;
   if (!forceLockCheck) {
      coordLock = lock;
   } else {
      coordLock = SystemCatalog::getInstance()->checkForCoordinatorLock(
            lock->getNamespaceName(), lock->getArrayName(), lock->getQueryId());
   }
   if (!coordLock) {
       LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
                     " lock does not exist. No action for query "
                     << lock->getQueryId());
       return false;
   }

   if (coordLock->getArrayId() == 0 ) {
       LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
                     " lock is not initialized. No action for query "
                     << lock->getQueryId());
       return false;
   }

   bool rc;
   if (coordLock->getArrayVersion() == 0)
   {
       LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
            << " lock queryID="       << coordLock->getQueryId()
            << " lock namespaceName=" << coordLock->getNamespaceName()
            << " lock arrayName="     << coordLock->getArrayName());
       rc = scidb::namespaces::Communicator::deleteArray(
            coordLock->getNamespaceName(),
            coordLock->getArrayName());
   }
   else
   {
       LOG4CXX_DEBUG(_logger, "RemoveErrorHandler::handleRemoveLock"
            << " lock queryID="       << coordLock->getQueryId()
            << " lock namespaceName=" << coordLock->getNamespaceName()
            << " lock arrayName="     << coordLock->getArrayName()
            << " lock arrayVersion="  << coordLock->getArrayVersion());

       rc = scidb::namespaces::Communicator::deleteArrayVersions(
            coordLock->getNamespaceName(),
            coordLock->getArrayName(),
            coordLock->getArrayVersion());
   }
   return rc;
}

void UpdateErrorHandler::handleError(const std::shared_ptr<Query>& query)
{
    boost::function<void()> work = boost::bind(&UpdateErrorHandler::_handleError, this, query);
    Query::runRestartableWork<void, Exception>(work);
}

void UpdateErrorHandler::_handleError(const std::shared_ptr<Query>& query)
{
   assert(query);
   if (!_lock) {
      assert(false);
      LOG4CXX_TRACE(_logger,
                    "Update error handler has nothing to do for query ("
                    << query->getQueryID() << ")");
      return;
   }
   assert(_lock->getInstanceId() == Cluster::getInstance()->getLocalInstanceId());
   assert( (_lock->getLockMode() == SystemCatalog::LockDesc::CRT)
           || (_lock->getLockMode() == SystemCatalog::LockDesc::WR) );
   assert(query->getQueryID() == _lock->getQueryId());

   LOG4CXX_DEBUG(_logger,
                 "Update error handler is invoked for query ("
                 << query->getQueryID() << ")");

   RollbackWork rw = bind(&UpdateErrorHandler::doRollback, _1, _2, _3);

   if (_lock->getInstanceRole() == SystemCatalog::LockDesc::COORD) {
      handleErrorOnCoordinator(_lock, rw);
   } else {
      assert(_lock->getInstanceRole() == SystemCatalog::LockDesc::WORKER);
      handleErrorOnWorker(_lock, query->isForceCancelled(), rw);
   }
   return;
}

void UpdateErrorHandler::releaseLock(const std::shared_ptr<SystemCatalog::LockDesc>& lock,
                                     const std::shared_ptr<Query>& query)
{
   assert(lock);
   assert(query);
   boost::function<bool()> work = boost::bind(&SystemCatalog::unlockArray,
                                             SystemCatalog::getInstance(),
                                             lock);
   bool rc = Query::runRestartableWork<bool, Exception>(work);
   if (!rc) {
      LOG4CXX_WARN(_logger, "Failed to release the lock for query ("
                   << query->getQueryID() << ")");
   }
}

static bool isTransientArray(const std::shared_ptr<SystemCatalog::LockDesc> & lock)
{
    return ( lock->getArrayId() > 0 &&
             lock->getArrayId() == lock->getArrayVersionId() &&
             lock->getArrayVersion() == 0 );
}


void UpdateErrorHandler::handleErrorOnCoordinator(const std::shared_ptr<SystemCatalog::LockDesc> & lock,
                                                  RollbackWork& rollback)
{
   assert(lock);
   assert(lock->getInstanceRole() == SystemCatalog::LockDesc::COORD);

   string const& namespaceName = lock->getNamespaceName();
   string const& arrayName = lock->getArrayName();

   std::shared_ptr<SystemCatalog::LockDesc> coordLock =
      SystemCatalog::getInstance()->checkForCoordinatorLock(
            namespaceName, arrayName, lock->getQueryId());
   if (!coordLock) {
      LOG4CXX_DEBUG(_logger, "UpdateErrorHandler::handleErrorOnCoordinator:"
                    " coordinator lock does not exist. No abort action for query "
                    << lock->getQueryId());
      return;
   }

   if (isTransientArray(coordLock)) {
       SCIDB_ASSERT(false);
       // no rollback for transient arrays
       return;
   }

   const ArrayID unversionedArrayId  = coordLock->getArrayId();
   const VersionID newVersion      = coordLock->getArrayVersion();
   const ArrayID newArrayVersionId = coordLock->getArrayVersionId();

   if (unversionedArrayId == 0) {
       SCIDB_ASSERT(newVersion == 0);
       SCIDB_ASSERT(newArrayVersionId == 0);
       // the query has not done much progress, nothing to rollback
       return;
   }

   ASSERT_EXCEPTION(newVersion > 0,
                    string("UpdateErrorHandler::handleErrorOnCoordinator:")+
                    string(" inconsistent newVersion<=0"));
   ASSERT_EXCEPTION(unversionedArrayId > 0,
                    string("UpdateErrorHandler::handleErrorOnCoordinator:")+
                    string(" inconsistent unversionedArrayId<=0"));
   ASSERT_EXCEPTION(newArrayVersionId > 0,
                    string("UpdateErrorHandler::handleErrorOnCoordinator:")+
                    string(" inconsistent newArrayVersionId<=0"));

   const VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(unversionedArrayId);

   if (lastVersion == newVersion) {
       // we are done, the version is committed
       return;
   }
   SCIDB_ASSERT(lastVersion < newVersion);
   SCIDB_ASSERT(lastVersion == (newVersion-1));

   if (rollback) {

       LOG4CXX_DEBUG(_logger, "UpdateErrorHandler::handleErrorOnCoordinator:"
                     " the new version "<< newVersion
                     <<" of array " << ArrayDesc::makeQualifiedArrayName(namespaceName, arrayName)
                     <<" (arrId="<< newArrayVersionId <<")"
                     <<" is being rolled back for query ("
                     << lock->getQueryId() << ")");

       rollback(lastVersion, unversionedArrayId, newArrayVersionId);
   }
}

void UpdateErrorHandler::handleErrorOnWorker(const std::shared_ptr<SystemCatalog::LockDesc>& lock,
                                             bool forceCoordLockCheck,
                                             RollbackWork& rollback)
{
   assert(lock);
   assert(lock->getInstanceRole() == SystemCatalog::LockDesc::WORKER);

   string const& namespaceName  = lock->getNamespaceName();
   string const& arrayName      = lock->getArrayName();
   VersionID newVersion         = lock->getArrayVersion();
   ArrayID newArrayVersionId    = lock->getArrayVersionId();

   LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnWorker:"
                 << " forceLockCheck = "<< forceCoordLockCheck
                 << " arrayName = "<< ArrayDesc::makeQualifiedArrayName(namespaceName, arrayName)
                 << " newVersion = "<< newVersion
                 << " newArrayVersionId = "<< newArrayVersionId);

   if (newVersion != 0) {

       if (forceCoordLockCheck) {
           std::shared_ptr<SystemCatalog::LockDesc> coordLock;
           do {  //XXX TODO: fix the wait, possibly with batching the checks
               coordLock = SystemCatalog::getInstance()->checkForCoordinatorLock(
                    namespaceName, arrayName, lock->getQueryId());
               Query::waitForSystemCatalogLock();
           } while (coordLock);
       }
       ArrayID arrayId = lock->getArrayId();
       if(arrayId == 0) {
           LOG4CXX_WARN(_logger, "Invalid update lock for query ("
                        << lock->getQueryId()
                        << ") Lock:" << lock->toString()
                        << " No rollback is possible.");
       }
       VersionID lastVersion = SystemCatalog::getInstance()->getLastVersion(arrayId);

       LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnWorker:"
                     << " lastVersion = "<< lastVersion);

       assert(lastVersion <= newVersion);

       // if we checked the coordinator lock, then lastVersion == newVersion implies
       // that the commit succeeded, and we should not rollback.
       // if we are not checking the coordinator lock, then something failed locally
       // and it should not be possible that the coordinator committed---we should
       // definitely rollback.
       assert(forceCoordLockCheck || lastVersion < newVersion);

       if (lastVersion < newVersion && newArrayVersionId > 0 && rollback) {
           rollback(lastVersion, arrayId, newArrayVersionId);
       }
   }
   LOG4CXX_TRACE(_logger, "UpdateErrorHandler::handleErrorOnWorker:"
                     << " exit");
}

void UpdateErrorHandler::doRollback(VersionID lastVersion,
                                    ArrayID baseArrayId,
                                    ArrayID newArrayId)
{

    LOG4CXX_TRACE(_logger, "UpdateErrorHandler::doRollback:"
                  << " baseArrayId = "<< baseArrayId
                  << " newArrayId = "<< newArrayId);
   // if a query stopped before the coordinator recorded the new array version id
   // there is no rollback to do

   assert(newArrayId>0);
   assert(baseArrayId>0);

   Storage::RollbackMap undoArray;
   undoArray[baseArrayId] = std::make_pair(newArrayId, lastVersion);
   try {
       StorageManager::getInstance().rollback(undoArray);
       StorageManager::getInstance().removeVersionFromMemory(baseArrayId, newArrayId);
   } catch (const scidb::Exception& e) {
       LOG4CXX_ERROR(_logger, "UpdateErrorHandler::doRollback:"
                     << " baseArrayId = "<< baseArrayId
                     << " newArrayId = "<< newArrayId
                     << ". Error: "<< e.what());
       throw; //XXX TODO: anything to do ???
   }
}

void Query::releaseLocks(const std::shared_ptr<Query>& q)
{
    assert(q);
    LOG4CXX_DEBUG(_logger, "Releasing locks for query " << q->getQueryID());

    boost::function<uint32_t()> work = boost::bind(&SystemCatalog::deleteArrayLocks,
                                                   SystemCatalog::getInstance(),
                                                   Cluster::getInstance()->getLocalInstanceId(),
                                                   q->getQueryID(),
                                                   SystemCatalog::LockDesc::INVALID_ROLE);
    runRestartableWork<uint32_t, Exception>(work);
}

void Query::acquireLocks()
{
    SystemCatalog::QueryLocks locks;
    {
        ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);
        validate();
        Query::Finalizer f = bind(&Query::releaseLocks, _1);
        pushFinalizer(f);
        assert(_finalizers.size() > 1);
        locks = _requestedLocks;
    }
    acquireLocksInternal(locks);
}

void Query::retryAcquireLocks()
{
    SystemCatalog::QueryLocks locks;
    {
        ScopedMutexLock cs(errorMutex, PTCW_MUT_OTHER);
        // try to assert that the lock release finalizer is in place
        assert(_finalizers.size() > 1);
        validate();
        locks = _requestedLocks;
    }
    if (locks.empty())
    {
        assert(false);
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "Query::retryAcquireLocks";
    }
    acquireLocksInternal(locks);
}

void Query::acquireLocksInternal(SystemCatalog::QueryLocks& locks)
{
    LOG4CXX_TRACE(_logger, "Acquiring "<< locks.size()
                  << " array locks for query " << _queryID);

    bool foundDeadInstances = (_coordinatorLiveness->getNumDead() > 0);
    try {
        SystemCatalog::ErrorChecker errorChecker = bind(&Query::validate, this);
        for (auto & lock : locks)
        {
            assert(lock);
            assert(lock->getQueryId() == _queryID);
            LOG4CXX_TRACE(_logger, "Acquiring lock: " << lock->toString());

            //XXX TODO: if the array residency does not include the dead instance(s), we will still fail ...
            if (foundDeadInstances && (lock->getLockMode() > SystemCatalog::LockDesc::RD)) {

                throw SYSTEM_EXCEPTION(SCIDB_SE_QPROC, SCIDB_LE_NO_QUORUM);
            }

            bool rc = SystemCatalog::getInstance()->lockArray(lock, errorChecker);
            if (!rc) {
                assert(false);
                throw std::runtime_error((string("Failed to acquire SystemCatalog lock")+lock->toString()).c_str());
            }
        }
        validate();

        // get the array metadata catalog version, i.e. 'timestamp' the arrays in use by this query
        if (!locks.empty()) {
            SystemCatalog::getInstance()->getCurrentVersion(locks);
        }
    } catch (const scidb::SystemCatalog::LockBusyException& e) {
        throw;
    } catch (std::exception&) {
        releaseLocks(shared_from_this());
        throw;
    }
    if (_logger->isDebugEnabled()) {
        LOG4CXX_DEBUG(_logger, "Acquired "<< locks.size() << " array locks for query " << _queryID);
        for (auto & lock : locks)
        {
            LOG4CXX_DEBUG(_logger, "Acquired lock: " << lock->toString());
        }
    }
}

ArrayID
Query::getCatalogVersion(
    const std::string& namespaceName,
    const std::string& arrayName,
    bool allowMissing  /* =false */) const
{
    assert(isCoordinator());
    // XXX TODO: currently synchronization is not used because
    // XXX TODO: it is called strictly before or after the query array lock acquisition
    if (_requestedLocks.empty() ) {
        // we have not acquired the locks yet
        return SystemCatalog::ANY_VERSION;
    }
    const std::string* unversionedNamePtr(&arrayName);
    std::string unversionedName;
    if (!ArrayDesc::isNameUnversioned(arrayName) ) {
        unversionedName = ArrayDesc::makeUnversionedName(arrayName);
        unversionedNamePtr = &unversionedName;
    }

    std::shared_ptr<SystemCatalog::LockDesc> key(
        make_shared<SystemCatalog::LockDesc>(
            namespaceName,
            (*unversionedNamePtr),
            getQueryID(),
            Cluster::getInstance()->getLocalInstanceId(),
            SystemCatalog::LockDesc::COORD,
            SystemCatalog::LockDesc::INVALID_MODE));

    SystemCatalog::QueryLocks::const_iterator iter = _requestedLocks.find(key);
    if (iter == _requestedLocks.end() && allowMissing) {
        return SystemCatalog::ANY_VERSION;
    }
    ASSERT_EXCEPTION(iter!=_requestedLocks.end(),
                     string("Query::getCatalogVersion: unlocked array: ")+arrayName);
    const std::shared_ptr<SystemCatalog::LockDesc>& lock = (*iter);
    assert(lock->isLocked());
    return lock->getArrayCatalogId();
}

ArrayID Query::getCatalogVersion(
    const std::string&      arrayName,
    bool                    allowMissing /* =false */) const
{
    return getCatalogVersion("public", arrayName, allowMissing);
}


uint64_t Query::getLockTimeoutNanoSec()
{
    static const uint64_t WAIT_LOCK_TIMEOUT_MSEC = 2000;
    const uint64_t msec = _rng()%WAIT_LOCK_TIMEOUT_MSEC + 1;
    const uint64_t nanosec = msec*1000000;
    return nanosec;
}

void Query::waitForSystemCatalogLock()
{
    Thread::nanoSleep(getLockTimeoutNanoSec());
}

void Query::setQueryPerThread(const std::shared_ptr<Query>& query)
{
    _queryPerThread = query;
}

std::shared_ptr<Query> Query::getQueryPerThread()
{
    return _queryPerThread;
}

void  Query::resetQueryPerThread()
{
    _queryPerThread.reset();
}

void Query::perfTimeAdd(const perfTimeCategory_t tc, const double sec)
{
    assert(tc >= 0);
    assert(tc < PTC_NUM);
    _tcUsecs[tc] += int64_t(sec*1.0e6);                   // NB: can record separate from execute
}

const int PERF_TIME_SUBTOTALS = 0;
const int PERF_TIME_PERCENTS = 0;

static void perfTimeLogPrintKey()
{
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: TOT = TOTal seconds query existed in SciDB");
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: ACT = ACTive seconds, subtotal of time the query was scheduled to run");
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key:       [the query is unscheduled  while, e.g., waiting for array locks]");

    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: CPU   = seconds query on main thread used the CPU");
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wPG   = seconds query on main thread waited for PostGres");
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wEXT  = seconds query on main thread waited for an EXTernal process");

    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wFSr  = seconds query on main thread waited for File System read");
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wFSw  = seconds query on main thread waited for File System write");
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wFSf  = seconds query on main thread waited for File System flushes");

    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wSGr  = seconds query on main thread waited for SG receives");
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wBAR  = seconds query on main thread waited for BARriers");
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wREP  = seconds query on main thread waited for REPlication");

    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wNETr = seconds query on main thread waited for Receive");
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wNETs = seconds query on main thread waited for Send");

    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wMUTo = seconds query on main thread waited for other MUTexes");
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wSEMo = seconds query on main thread waited for other SEMaphores");
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: wEVo  = seconds query on main thread waited for other EVENTs");
    if (PERF_TIME_SUBTOTALS) {
        LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: CAT  = seconds CATegorized");
        LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: unCAT  = Active - Cat");
    }
    LOG4CXX_DEBUG (perfLogger, "QueryTiming Key: OTH%  = percent of Active not categorized above");
}

uint64_t Query::_perfTimeLogCount = 0;
void Query::perfTimeLog() const
{
    const uint LINES_LOGGED_PER_KEY_PRINTED= 100;
    if (_perfTimeLogCount++ % LINES_LOGGED_PER_KEY_PRINTED == 0) { perfTimeLogPrintKey(); }

    int64_t usecNow = int64_t(perfTimeGetElapsed()*1.0e6);
    int64_t usecTotal = usecNow - _usecElapsedStart;

    // because CPUtime and elapsedTime clocks were not synchronized
    // it is possible for some values to be slightly negative
    // here we make a copy adjusted without the negative values and
    // note the total bias we introduced by doing so.
    int64_t posUsecs[PTC_NUM];
    int64_t usecBias = 0;
    for (unsigned i=PTC_FIRST; i< PTC_NUM; i++) {
        int64_t tmp = _tcUsecs[i];
        if (tmp >= 0) {
            posUsecs[i] = tmp;
        } else {
            if (tmp < -5000) { // more than 5msec off
                LOG4CXX_WARN(perfLogger, "QueryTiming: _tcUsecs["<<i<<"]="<<tmp<<" ignored");
            }
            usecBias -= tmp;
            posUsecs[i] = 0;
        }
    }
    if(usecBias > 100000) { // more than 100msec total bias
        LOG4CXX_WARN(perfLogger, "QueryTiming: total usecBias = " << usecBias);
    }

    int64_t usecCategorized = 0;
    for (unsigned i=PTC_FIRST; i< PTC_NUM; i++) {
        if (i != PTC_ACTIVE) {   // goal is for others to sum to active
            usecCategorized += posUsecs[i];
        }
    }

    // because of usecBias, beware that the difference below could be negative
    int64_t usecUncategorized = std::max(posUsecs[PTC_ACTIVE] - usecCategorized, int64_t(0));

    std::stringstream ssStats;
    ssStats << std::fixed << std::setprecision(3)
            <<  "TOT "  << double(usecTotal) * 1.0e-6
            << " ACT "  << double(posUsecs[PTC_ACTIVE]) * 1.0e-6;
    if(PERF_TIME_PERCENTS) {
        ssStats << " ACT% " << double(posUsecs[PTC_ACTIVE]) / double(usecTotal) * 100.0;   // percent
    }
    if(PERF_TIME_SUBTOTALS) {
        ssStats << " Cat "  << double(usecCategorized) * 1.0e-6;
        if(PERF_TIME_PERCENTS) {
            ssStats << " Cat% " << double(usecCategorized) / double(posUsecs[PTC_ACTIVE]) * 100.0;  // percent
            // note that due to usecBias, this could be slightly more than 100%
        }
    }
    ssStats << " CPU "   << double(posUsecs[PTC_CPU]) * 1.0e-6
            << " wPG "   << double(posUsecs[PTCW_PG]) * 1.0e-6
            << " wEXT "  << double(posUsecs[PTCW_EXT]) * 1.0e-6
            << " wFSr "  << double(posUsecs[PTCW_FS_RD]) * 1.0e-6
            << " wFSw "  << double(posUsecs[PTCW_FS_WR]) * 1.0e-6
            << " wFSws " << double(posUsecs[PTCW_FS_WR_SYNC]) * 1.0e-6
            << " wFSf "  << double(posUsecs[PTCW_FS_FL]) * 1.0e-6
            << " wSGr "  << double(posUsecs[PTCW_SG_RCV]) * 1.0e-6
            //<< " SGs " << double(posUsecs[PTCW_SG_SND]) * 1.0e-6         // no such category at present
            << " wBAR "  << double(posUsecs[PTCW_BAR])*1.0e-6
            << " wREP "  << double(posUsecs[PTCW_REP])*1.0e-6
            << " wNETr " << double(posUsecs[PTCW_NET_RCV]) * 1.0e-6
            << " wNETs " << double(posUsecs[PTCW_NET_SND])* 1.0e-6
            << " wMUTo " << double(posUsecs[PTCW_MUT_OTHER]) * 1.0e-6
            << " wSEMo " << double(posUsecs[PTCW_SEM_OTHER]) * 1.0e-6
            << " wEVo "  << double(posUsecs[PTCW_EVENT_OTHER]) * 1.0e-6;

    if(PERF_TIME_SUBTOTALS) {
        ssStats << " unCat "  << double(usecUncategorized) * 1.0e-6;
    }

    // this percentage is always printed, too easy to make mistakes without it on casual observation
    // its last so that those who don't want it can more easily exclude it
    ssStats << std::fixed << std::setprecision(1)
            << " OTH% " << double(usecUncategorized) / double(posUsecs[PTC_ACTIVE]) * 100.0;  // percent

    LOG4CXX_DEBUG (perfLogger, "QueryTiming " << getInstanceID() << " " << _queryID << " " << ssStats.str() << " " << queryString);
}

ScopedQueryPerThread::ScopedQueryPerThread(const std::shared_ptr<Query>& query) {
        Query::setQueryPerThread(query);
}

ScopedQueryPerThread::~ScopedQueryPerThread() {
        Query::resetQueryPerThread();
}

const ProcGrid* Query::getProcGrid() const
{
    // locking to ensure a single allocation
    // XXX TODO: consider always calling Query::getProcGrid() in MpiManager::checkAndSetCtx
    //           that should guarantee an atomic creation of _procGrid
    ScopedMutexLock lock(const_cast<Mutex&>(errorMutex), PTCW_MUT_OTHER);
    // logically const, but we made _procGrid mutable to allow the caching
    // NOTE: Tigor may wish to push this down into the MPI context when
    //       that code is further along.  But for now, Query is a fine object
    //       on which to cache the generated procGrid
    if (!_procGrid) {
        _procGrid = new ProcGrid(safe_static_cast<procNum_t>(getInstancesCount()));
    }
    return _procGrid;
}

void Query::listLiveInstances(InstanceVisitor& func)
{
    assert(func);
    ScopedMutexLock lock(errorMutex, PTCW_MUT_OTHER);  //XXX TODO: remove lock ?

    for (vector<InstanceID>::const_iterator iter = _liveInstances.begin();
         iter != _liveInstances.end(); ++iter) {
        std::shared_ptr<Query> thisQuery(shared_from_this());
        func(thisQuery, (*iter));
    }
}

std::string Query::getNamespaceName() const
{
    return _session ? _session->getNamespace().getName() : "public";
}

void Query::getNamespaceArrayNames(
    const std::string &         qualifiedName,
    std::string &               namespaceName,
    std::string &               arrayName) const
{
    namespaceName = getNamespaceName();
    scidb::ArrayDesc::splitQualifiedArrayName(qualifiedName, namespaceName, arrayName);
}



ReplicationContext::ReplicationContext(const std::shared_ptr<Query>& query, size_t nInstances)
: _query(query)
#ifndef NDEBUG // for debugging
,_chunkReplicasReqs(nInstances)
#endif
{
    // ReplicatonManager singleton is initialized at startup time
    if (_replicationMngr == NULL) {
        _replicationMngr = ReplicationManager::getInstance();
    }
}

ReplicationContext::QueueInfoPtr ReplicationContext::getQueueInfo(QueueID id)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());
    QueueInfoPtr& qInfo = _inboundQueues[id];
    if (!qInfo) {
        int size = Config::getInstance()->getOption<int>(CONFIG_REPLICATION_RECEIVE_QUEUE_SIZE);
        assert(size>0);
        size = (size<1) ? 4 : size+4; // allow some minimal extra space to tolerate mild overflows
        qInfo = std::make_shared<QueueInfo>(NetworkManager::getInstance()->createWorkQueue(1,static_cast<uint64_t>(size)));
        assert(!qInfo->getArray());
        assert(qInfo->getQueue());
        qInfo->getQueue()->stop();
    }
    assert(qInfo->getQueue());
    return qInfo;
}

void ReplicationContext::enableInboundQueue(ArrayID aId, const std::shared_ptr<Array>& array)
{
    assert(array);
    assert(aId > 0);
    ScopedMutexLock cs(_mutex, PTCW_MUT_OTHER);
    QueueID qId(aId);
    QueueInfoPtr qInfo = getQueueInfo(qId);
    assert(qInfo);
    std::shared_ptr<scidb::WorkQueue> wq = qInfo->getQueue();
    assert(wq);
    qInfo->setArray(array);
    wq->start();
}

std::shared_ptr<scidb::WorkQueue> ReplicationContext::getInboundQueue(ArrayID aId)
{
    assert(aId > 0);
    ScopedMutexLock cs(_mutex, PTCW_MUT_OTHER);
    QueueID qId(aId);
    QueueInfoPtr qInfo = getQueueInfo(qId);
    assert(qInfo);
    std::shared_ptr<scidb::WorkQueue> wq = qInfo->getQueue();
    assert(wq);
    return wq;
}

std::shared_ptr<scidb::Array> ReplicationContext::getPersistentArray(ArrayID aId)
{
    assert(aId > 0);
    ScopedMutexLock cs(_mutex, PTCW_MUT_OTHER);
    QueueID qId(aId);
    QueueInfoPtr qInfo = getQueueInfo(qId);
    assert(qInfo);
    std::shared_ptr<scidb::Array> array = qInfo->getArray();
    assert(array);
    assert(qInfo->getQueue());
    return array;
}

void ReplicationContext::removeInboundQueue(ArrayID aId)
{
    // tigor:
    // Currently, we dont remove the queue until the query is destroyed.
    // The reason for this was that we did not have a sync point, and
    // each instance was not waiting for the INCOMING replication to finish.
    // But we now have a sync point here, to coordinate the storage manager
    // fluhes.  So we may be able to implement queue removal in the future.

    std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
    syncBarrier(0, query);
    syncBarrier(1, query);

    return;
}

namespace {
void generateReplicationItems(std::shared_ptr<MessageDesc>& msg,
                              std::vector<std::shared_ptr<ReplicationManager::Item> >* replicaVec,
                              const std::shared_ptr<Query>& query,
                              InstanceID physInstanceId)
{
    if (physInstanceId == query->getPhysicalInstanceID()) {
        return;
    }
    std::shared_ptr<ReplicationManager::Item> item(new ReplicationManager::Item(physInstanceId, msg, query));
    replicaVec->push_back(item);
}
}
void ReplicationContext::replicationSync(ArrayID arrId)
{
    assert(arrId > 0);
    std::shared_ptr<MessageDesc> msg = std::make_shared<MessageDesc>(mtChunkReplica);
    std::shared_ptr<scidb_msg::Chunk> chunkRecord = msg->getRecord<scidb_msg::Chunk> ();
    chunkRecord->set_array_id(arrId);
    // tell remote instances that we are done replicating
    chunkRecord->set_eof(true);

    assert(_replicationMngr);
    std::shared_ptr<Query> query(Query::getValidQueryPtr(_query));
    msg->setQueryID(query->getQueryID());

    vector<std::shared_ptr<ReplicationManager::Item> > replicasVec;
    Query::InstanceVisitor f =
        boost::bind(&generateReplicationItems, msg, &replicasVec, _1, _2);
    query->listLiveInstances(f);

    assert(replicasVec.size() == (query->getInstancesCount()-1));
    for (size_t i=0; i<replicasVec.size(); ++i) {
        const std::shared_ptr<ReplicationManager::Item>& item = replicasVec[i];
        assert(_replicationMngr);
        _replicationMngr->send(item);
    }
    for (size_t i=0; i<replicasVec.size(); ++i) {
        const std::shared_ptr<ReplicationManager::Item>& item = replicasVec[i];
        assert(_replicationMngr);
        _replicationMngr->wait(item);
        assert(item->isDone());
        assert(item->validate(false));
    }

    QueueInfoPtr qInfo;
    {
        ScopedMutexLock cs(_mutex, PTCW_MUT_OTHER);
        QueueID qId(arrId);
        qInfo = getQueueInfo(qId);
        assert(qInfo);
        assert(qInfo->getArray());
        assert(qInfo->getQueue());
    }
    // wait for all to ack our eof
    Semaphore::ErrorChecker ec = bind(&Query::validate, query);
    qInfo->getSemaphore().enter(replicasVec.size(), ec, PTCW_REP);
}

void ReplicationContext::replicationAck(InstanceID sourceId, ArrayID arrId)
{
    assert(arrId > 0);
    QueueInfoPtr qInfo;
    {
        ScopedMutexLock cs(_mutex, PTCW_MUT_OTHER);
        QueueID qId(arrId);
        qInfo = getQueueInfo(qId);
        assert(qInfo);
        assert(qInfo->getArray());
        assert(qInfo->getQueue());
    }
    // sourceId acked our eof
    qInfo->getSemaphore().release();
}

/// cached pointer to the ReplicationManager singeton
ReplicationManager*  ReplicationContext::_replicationMngr;

void ReplicationContext::enqueueInbound(ArrayID arrId, std::shared_ptr<Job>& job)
{
    assert(job);
    assert(arrId>0);
    assert(job->getQuery());
    ScopedMutexLock cs(_mutex, PTCW_MUT_OTHER);

    std::shared_ptr<WorkQueue> queryQ(getInboundQueue(arrId));

    if (Query::_logger->isTraceEnabled()) {
        std::shared_ptr<Query> query(job->getQuery());
        LOG4CXX_TRACE(Query::_logger, "ReplicationContext::enqueueInbound"
                      <<" job="<<job.get()
                      <<", queue="<<queryQ.get()
                      <<", arrId="<<arrId
                      << ", queryID="<<query->getQueryID());
    }
    assert(_replicationMngr);
    try {
        WorkQueue::WorkItem item = _replicationMngr->getInboundReplicationItem(job);
        queryQ->enqueue(item);
    } catch (const WorkQueue::OverflowException& e) {
        LOG4CXX_ERROR(Query::_logger, "ReplicationContext::enqueueInbound"
                      << ": Overflow exception from the message queue (" << queryQ.get()
                      << "): "<<e.what());
        std::shared_ptr<Query> query(job->getQuery());
        assert(query);
        assert(false);
        query->handleError(e.copy());
        throw;
    }
}

} // namespace

