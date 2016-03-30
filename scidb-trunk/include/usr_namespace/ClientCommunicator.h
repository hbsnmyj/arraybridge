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
 * ClientCommunicator.h
 *
 *  Created on: May 29, 2015
 *      Author: mcorbett@paradigm4.com
 */

#ifndef CLIENT_COMMUNICATOR_H_
#define CLIENT_COMMUNICATOR_H_

#include <memory>
#include <boost/assign.hpp>
#include <inttypes.h>
#include <string>

namespace scidb
{
    /**
     * Interface to allow the server to communicate messages to
     * the client.  These messages are allowed only between the
     * newClientStart and the newClientComplete messages from
     * the client.
     */
    class ClientCommunicator
    {
    public:
        static uint32_t const DEFAULT_WAIT_TIMEOUT_MILLISECONDS =
            30 * 1000;  // 30 seconds

    public:
        /**
         * Send a string message to the client
         * @param msg - String message to send
         */
        virtual void sendMessageToClient(
            const std::string &msg) = 0;

        /**
         * Wait for a response to the message sent via
         * sendMessageToClient.
         *
         * @param response - String response returned by the client.
         * @param timeoutMilliseconds - Max time to wait for the client
         */
        virtual void waitForClientResponse(
            std::string &response,
            uint32_t timeoutMilliseconds =
                DEFAULT_WAIT_TIMEOUT_MILLISECONDS) = 0;

        /**
         * When the message is received asynchronously from the client
         * this method should be called with the client's response.
         * Currently this method is called by ClientMessageHandleJob.
         */
        virtual void setResponse(const std::string &strResponse) = 0;

    };
} // namespace scidb

#endif /* CLIENT_COMMUNICATOR_H_ */
