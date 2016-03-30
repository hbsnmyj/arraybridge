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
 * @file ConnectionClientCommunicator.h
 *
 * @author mcorbett@paradigm4.com
 *
 * @brief The job for handling communication to/from the client between
 *   the messages of newClientStart and newClientComplete.
 */

#ifndef CONNECTION_CLIENT_COMMUNICATOR_H_
#define CONNECTION_CLIENT_COMMUNICATOR_H_

#include <usr_namespace/ClientCommunicator.h>

#include <memory>
#include <util/Semaphore.h>


namespace scidb
{

class Connection;

class ConnectionClientCommunicator : public ClientCommunicator
{
private:
    std::weak_ptr<Connection> _connection;
    std::string _strMessage;
    std::string _strResponse;
    bool _receivedResponse;
    Semaphore _waitForClientSemaphore;
    int _waiting;  // Useful for debugging

public:
    /**
     * Constructor
     *
     * @param connection - the network connection that the messages
     * will be sent and received on.
     */
    ConnectionClientCommunicator(
        std::shared_ptr<Connection> connection);

    /**
     *  Destructor
     */
    virtual ~ConnectionClientCommunicator();

    /**
     * Send a string message to the client.  If the message requires
     * a response the waitForClientResponse below can be used to
     * asynchronously wait.
     *
     * @param msg - the message to send to the client
     */
    virtual void sendMessageToClient(const std::string &msg);

    /**
     * @return true - response received, false - otherwise
     */
    bool receivedResponse() const
    {
        return _receivedResponse;
    }

    /**
     * Allows waiting for a repsonse from the client to the a message
     * that was sent using sendMessageToClient.
     *
     * @param rsp - filled in with the client's response
     * @param timeoutMilliseconds - the max time to wait for the client
     */
    virtual void waitForClientResponse(
        std::string &rsp,
        uint32_t timeoutMilliseconds =
            DEFAULT_WAIT_TIMEOUT_MILLISECONDS);

    /**
     * When the message is received asynchronously from the client this
     * method should be called with the client's response.  Currently
     * this method is called by ClientMessageHandleJob.
     */
    virtual void setResponse(const std::string &rsp);

    /**
     * Get the value of the debug "_waiting" variable
     * @returns the value of the debug "_waiting" variable
     */
    int getWaiting() const { return _waiting; }

    /**
     * Set the value of the debug "_waiting" variable
     * @param[in] value - the new value for the debug "_waiting" variable
     */
    void setWaiting(int value) { _waiting = value; }
};


} // namespace scidb

#endif /* CONNECTION_CLIENT_COMMUNICATOR_H_ */
