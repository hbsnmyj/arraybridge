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
 * ConnectionClientCommunicator.cpp
 *
 *  Modified on: May 18, 2015
 *      Author: mcorbett@paradigm.com
 *      Purpose:  Basic Security enhancements
 */

#include <usr_namespace/ConnectionClientCommunicator.h>


#include "log4cxx/logger.h"
#include <memory>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <time.h>

#include <array/Metadata.h>
#include <network/Connection.h>
#include <network/NetworkManager.h>
#include <network/MessageUtils.h>
#include <query/QueryProcessor.h>
#include <query/Serialize.h>
#include <query/executor/SciDBExecutor.h>
#include <system/Exceptions.h>


using namespace std;
using namespace boost;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.securityPluginComm"));


ConnectionClientCommunicator::ConnectionClientCommunicator(
    std::shared_ptr<Connection> connection)
    : _connection(connection)
    , _receivedResponse(false)
    , _waiting(-2)
{

}

ConnectionClientCommunicator::~ConnectionClientCommunicator()
{

}


void ConnectionClientCommunicator::sendMessageToClient(
        const std::string &strMessage)
{

    LOG4CXX_DEBUG(logger,
       "ConnectionClientCommunicator::sendMessageToClient"
           << "(" << strMessage << ")");

    _receivedResponse = false;
    _strMessage  = strMessage;

    std::shared_ptr<Connection> connection = _connection.lock();
    if(!connection)
    {
        throw SYSTEM_EXCEPTION(
            SCIDB_SE_EXECUTION,
            SCIDB_LE_CONNECTION_ERROR2);
    }

    // -- send SecurityMessage to client --- //
    std::shared_ptr<MessageDesc> msg =
        std::make_shared<MessageDesc>(
            mtSecurityMessage);
    assert(msg);

    std::shared_ptr<scidb_msg::SecurityMessage> record =
        msg->getRecord<scidb_msg::SecurityMessage>();
    assert(record);

    record->set_msg_type(1);
    record->set_msg(_strMessage);

    try {
        stringstream ss;
        ss << "ConnectionClientCommunicator::sendMessageToClient" << std::endl;
        LOG4CXX_DEBUG(logger, msg->str(ss.str()));

        _receivedResponse=false;
        _strResponse = "Unknown";
        connection->sendMessage(msg);
    } catch (const scidb::Exception& e) {
        try { connection->disconnect(); } catch (...) {}
        throw;
    }
}

namespace {
/**
 * returns true - keep waiting, false otherwise
 */
bool _waitForClientResponseWithTimeout(
    uint64_t startTime,
    uint64_t timeout,
    ConnectionClientCommunicator *
        connectionClientCommunicator)
{
    ASSERT_EXCEPTION(
        connectionClientCommunicator,
        "NULL connectionClientCommunicator");

    if(connectionClientCommunicator->receivedResponse())
    {
        connectionClientCommunicator->setWaiting(0);
        return false;
    }

    if (hasExpired(startTime, timeout))
    {
        stringstream ss;
        ss << "_waitForClientResponseWithTimeout"
            << " startTime=" << startTime
            << " timeout=" << timeout;

        throw SYSTEM_EXCEPTION(
            SCIDB_SE_EXECUTION,
            SCIDB_LE_WAIT_FOR_CLIENT_TIMEOUT)
                << ss.str();
        connectionClientCommunicator->setWaiting(1);
    }
    connectionClientCommunicator->setWaiting(1);
    return true;
}
}

void ConnectionClientCommunicator::waitForClientResponse(
    std::string &strResponse,
    uint32_t timeoutMilliseconds
        /* = DEFAULT_WAIT_TIMEOUT_MILLISECONDS */)
{
    static const char *funcName =
        "ConnectionClientCommunicator::waitForClientResponse";

    #define MILLISECONDS_TO_NANOSECONDS(a)      \
        ((uint64_t) (a) * 1000 * 1000)

    uint64_t startTime = getTimeInNanoSecs();
    uint64_t timeoutNanosec =
        MILLISECONDS_TO_NANOSECONDS(timeoutMilliseconds);

    setWaiting(-2);
    Semaphore::ErrorChecker errorChecker = bind(
        &_waitForClientResponseWithTimeout,
        startTime,
        timeoutNanosec,
        this);
    ReleaseOnExit releaseOnExit(_waitForClientSemaphore);

    // Enter times out after 10 seconds.  So, try 20 times
    // to enter.  I.E.  try for 200 seconds.
    const size_t maxTries = 20;
    if(!_waitForClientSemaphore.enter(maxTries, errorChecker))
    {
        // The response could have arrived before we waited
        // for the semaphore above
        if(!receivedResponse())
        {
            LOG4CXX_DEBUG(logger, funcName
                << " _waitForClientSemaphore.enter(10, errorChecker) failed");
        }
    }


    // --- //

    strResponse = _strResponse;
    if(	(strResponse.length() == 0) ||
        (strResponse.compare("Unknown") == 0))
    {
        stringstream ss;
        ss  << funcName
            << " _receivedResponse="
            << (_receivedResponse ? "true" : "false")
            << " strResponse="    << strResponse
            << " startTime="      << startTime
            << " timeoutNanosec=" << timeoutNanosec
            << " waiting="        << getWaiting();

        throw SYSTEM_EXCEPTION(
            SCIDB_SE_INTERNAL,
            SCIDB_LE_NULL_CLIENT_RESPONSE)
                << ss.str();
    }
}


void ConnectionClientCommunicator::setResponse(
    const std::string &strResponse)
{
    static const char *funcName =
        "ConnectionClientCommunicator::setResponse";

    if(strResponse.empty())
    {
        throw SYSTEM_EXCEPTION(
            SCIDB_SE_INTERNAL,
            SCIDB_LE_NULL_CLIENT_RESPONSE)
                << funcName;
    }

    _strResponse = strResponse;
    _receivedResponse = true;
    bool result;

    const int maxTries = 10;
    _waitForClientSemaphore.release(result, maxTries);
    if(result == false)
    {
        LOG4CXX_DEBUG(logger, funcName
            << "_waitForClientSemaphore.release(10) failed");
    }
}
} // namespace scidb
