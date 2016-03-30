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
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Transparent interface for examining cluster physical resources
 */
#include <boost/filesystem/operations.hpp>
#include <boost/bind.hpp>

#include "system/Resources.h"
#include "network/NetworkManager.h"
#include "network/proto/scidb_msg.pb.h"
#include "query/Query.h"
#include "util/Semaphore.h"
#include "util/Mutex.h"
#include <network/BaseConnection.h>
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.system.resources"));

using namespace std;

namespace scidb
{

class BaseResourcesCollector
{
public:
    virtual ~BaseResourcesCollector(){};

protected:
    Semaphore _collectorSem;

    friend class Resources;
};

class FileExistsResourcesCollector: public BaseResourcesCollector
{
public:
    void collect(InstanceID instanceId, bool exists, bool release = true)
    {
        LOG4CXX_TRACE(logger, "FileExistsResourcesCollector::collect." << " instanceId=" << instanceId
            << " exists=" << exists);
        {
            ScopedMutexLock lock(_lock, PTCW_MUT_OTHER);
            _instancesMap[instanceId] = exists;
            if (release)
                _collectorSem.release();
        }
    }

    ~FileExistsResourcesCollector()  {
        ScopedMutexLock lock(_lock, PTCW_MUT_OTHER);
    }

    map<InstanceID, bool> _instancesMap;

private:
    Mutex _lock;
    friend class Resources;
};

void Resources::fileExists(const string &path, map<InstanceID, bool> &instancesMap, const std::shared_ptr<Query> &query)
{
    LOG4CXX_TRACE(logger, "Resources::fileExists. Checking file '" << path << "'");
    NetworkManager* networkManager = NetworkManager::getInstance();

    FileExistsResourcesCollector* collector = new FileExistsResourcesCollector();
    uint64_t id = 0;
    {
        ScopedMutexLock lock(_lock, PTCW_MUT_OTHER);
        id = ++_lastResourceCollectorId;
        _resourcesCollectors[id] = collector;

        collector->collect(query->getInstanceID(), checkFileExists(path), false);
    }

    std::shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtResourcesFileExistsRequest);
    std::shared_ptr<scidb_msg::ResourcesFileExistsRequest> request =
        msg->getRecord<scidb_msg::ResourcesFileExistsRequest>();
    msg->setQueryID(QueryID::getFakeQueryId());
    request->set_resource_request_id(id);
    request->set_file_path(path);
    networkManager->broadcastPhysical(msg);

    LOG4CXX_TRACE(logger, "Resources::fileExists. Waiting while instances return result for collector " << id);

    try
    {
       Semaphore::ErrorChecker errorChecker = bind(&Query::validateQueryPtr, query);
       collector->_collectorSem.enter(query->getInstancesCount() - 1, errorChecker);
    }
    catch (...)
    {
        LOG4CXX_TRACE(logger, "Resources::fileExists. Waiting for result of collector " << id <<
            " interrupter by error");
        {
            ScopedMutexLock lock(_lock, PTCW_MUT_OTHER);
            delete _resourcesCollectors[id];
            _resourcesCollectors.erase(id);
        }
        throw;
    }

    LOG4CXX_TRACE(logger, "Resources::fileExists. Returning result of collector " << id);

    {
        ScopedMutexLock lock(_lock, PTCW_MUT_OTHER);
        instancesMap = ((FileExistsResourcesCollector*) _resourcesCollectors[id])->_instancesMap;
        delete _resourcesCollectors[id];
        _resourcesCollectors.erase(id);
    }
}

bool Resources::fileExists(const string &path, InstanceID instanceId, const std::shared_ptr<Query>& query)
{
    LOG4CXX_TRACE(logger, "Resources::fileExists. Checking file '" << path << "'");
    NetworkManager* networkManager = NetworkManager::getInstance();

    if (instanceId == query->getInstanceID())
    {
        LOG4CXX_TRACE(logger, "Resources::fileExists. Instance id " << instanceId << " is local instance. Returning result.");
        return checkFileExists(path);
    }
    else
    {
        LOG4CXX_TRACE(logger, "Resources::fileExists. Instance id " << instanceId << " is remote instance. Requesting result.");
        FileExistsResourcesCollector* collector = new FileExistsResourcesCollector();
        uint64_t id = 0;
        {
            ScopedMutexLock lock(_lock, PTCW_MUT_OTHER);
            id = ++_lastResourceCollectorId;
            _resourcesCollectors[id] = collector;
        }

        std::shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtResourcesFileExistsRequest);
        std::shared_ptr<scidb_msg::ResourcesFileExistsRequest> request =
            msg->getRecord<scidb_msg::ResourcesFileExistsRequest>();
        msg->setQueryID(QueryID::getFakeQueryId());
        request->set_resource_request_id(id);
        request->set_file_path(path);
        networkManager->sendPhysical(instanceId, msg);

        LOG4CXX_TRACE(logger, "Resources::fileExists. Waiting while instance return result for collector " << id);

        try
        {
           Semaphore::ErrorChecker errorChecker = bind(&Query::validateQueryPtr, query);
           collector->_collectorSem.enter(1, errorChecker);
        }
        catch (...)
        {
            LOG4CXX_TRACE(logger, "Resources::fileExists. Waiting for result of collector " << id <<
                " interrupter by error");
            {
                ScopedMutexLock lock(_lock, PTCW_MUT_OTHER);
                delete _resourcesCollectors[id];
                _resourcesCollectors.erase(id);
            }
            throw;
        }

        LOG4CXX_TRACE(logger, "Resources::fileExists. Returning result of collector " << id);

        bool result;
        {
            ScopedMutexLock lock(_lock, PTCW_MUT_OTHER);
            result = ((FileExistsResourcesCollector*) _resourcesCollectors[id])->_instancesMap[instanceId];
            delete _resourcesCollectors[id];
            _resourcesCollectors.erase(id);
        }
        return result;
    }
}

void Resources::handleFileExists(const std::shared_ptr<MessageDesc>& messageDesc)
{
    NetworkManager* networkManager = NetworkManager::getInstance();

    if (mtResourcesFileExistsRequest == messageDesc->getMessageType())
    {
        LOG4CXX_TRACE(logger, "Message is mtResourcesFileExistsRequest");
        std::shared_ptr<scidb_msg::ResourcesFileExistsRequest> inMsgRecord =
            messageDesc->getRecord<scidb_msg::ResourcesFileExistsRequest>();
        const string& file = inMsgRecord->file_path();
        LOG4CXX_TRACE(logger, "Checking file " << file);

        std::shared_ptr<MessageDesc> msg = make_shared<MessageDesc>(mtResourcesFileExistsResponse);
        msg->setQueryID(QueryID::getFakeQueryId());

        std::shared_ptr<scidb_msg::ResourcesFileExistsResponse> outMsgRecord =
            msg->getRecord<scidb_msg::ResourcesFileExistsResponse>();
        outMsgRecord->set_resource_request_id(inMsgRecord->resource_request_id());
        outMsgRecord->set_exits_flag(Resources::getInstance()->checkFileExists(file));

        networkManager->sendPhysical(messageDesc->getSourceInstanceID(), msg);
    }
    // mtResourcesFileExistsResponse
    else
    {
        LOG4CXX_TRACE(logger, "Message is mtResourcesFileExistsResponse");
        std::shared_ptr<scidb_msg::ResourcesFileExistsResponse> inMsgRecord =
            messageDesc->getRecord<scidb_msg::ResourcesFileExistsResponse>();

        LOG4CXX_TRACE(logger, "Marking file");
        Resources::getInstance()->markFileExists(
            inMsgRecord->resource_request_id(),
            messageDesc->getSourceInstanceID(),
            inMsgRecord->exits_flag());
    }
}

bool Resources::checkFileExists(const std::string &path) const
{
    LOG4CXX_TRACE(logger, "Resources::checkFileExists. path=" << path);

    try
    {
        return boost::filesystem::exists(path);
    }
    catch (...)
    {
        return false;
    }
}

void Resources::markFileExists(uint64_t resourceCollectorId, InstanceID instanceId, bool exists)
{
    LOG4CXX_TRACE(logger, "Resources::markFileExists. resourceCollectorId=" << resourceCollectorId
        << " instanceId=" << instanceId << " exists=" << exists);

    if (_resourcesCollectors.find(resourceCollectorId) != _resourcesCollectors.end())
    {
        FileExistsResourcesCollector *rc = (FileExistsResourcesCollector*) _resourcesCollectors[resourceCollectorId];
        rc->collect(instanceId, exists);
    }
    else
    {
        LOG4CXX_WARN(logger, "Resources::markFileExists. FileExistsResourcesCollector with resourceCollectorId="
            << resourceCollectorId << " not found!");
    }
}

}
