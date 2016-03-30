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
 * @file BaseConnection.h
 *
 * @author: roman.simakov@gmail.com
 *
 * @brief The BaseConnection class
 *
 * The file includes the main data structures and interfaces used in message exchanging.
 * Also the file contains BaseConnection class for synchronous connection and message exchanging.
 * This class is used in client code. The scidb engine will use a class which is derived from BaseConnection.
 */

#ifndef BASECONNECTION_H_
#define BASECONNECTION_H_

#include <stdint.h>
#include <boost/asio.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/exception_ptr.hpp>
#include <log4cxx/logger.h>
#include <vector>

#include <query/QueryID.h>
#include <array/Metadata.h>
#include <array/Array.h>
#include <util/Semaphore.h>
#include <util/NetworkMessage.h>
#include <util/Network.h>

namespace scidb
{

/**
 * If you are changing the format of the protobuf messages in src/network/proto/scidb_msg.proto
 * (especially by adding required message fields), or any structures like MessageType and/or MessagesHeader
 * - you must increment this number. Notice that this will impact all the client tools (by breaking backwards compatibility).
 *
 * Revision history:
 *
 * NET_PROTOCOL_CURRENT_VER = 8:
 *    Author: tigor
 *    Date: 9/XXX/2015
 *    Ticket: XXX
 *
 * NET_PROTOCOL_CURRENT_VER = 7:
 *    Author: james
 *    Date: 7/9/2015
 *    Ticket: 4535
 *
 * NET_PROTOCOL_CURRENT_VER = 6:
 *    Author: marty
 *    Date: 5/18/2015
 *    Ticket: 4294
 *
 * NET_PROTOCOL_CURRENT_VER = 5:
 *    Author: tigor
 *    Date: 2/XXX/2015
 *    Ticket: 4422
 *
 * NET_PROTOCOL_CURRENT_VER = 4:
 *    Author: tigor
 *    Date: 7/17/2014
 *    Ticket: 4138, 3667, ...
 *
 *
 * NET_PROTOCOL_CURRENT_VER = 3:
 *    Author: ??
 *    Date: ??
 *    Ticket: ??
 *    Note: Initial implementation dating back some time
 */
const uint32_t NET_PROTOCOL_CURRENT_VER = 8;

/**
 * Messageg types
 */
enum MessageType
{
    mtNone                                   = SYSTEM_NONE_MSG_ID + 0,
    mtExecuteQuery                           = SYSTEM_NONE_MSG_ID + 1,
    mtPreparePhysicalPlan                    = SYSTEM_NONE_MSG_ID + 2,
    mtUnusedPlus3                            = SYSTEM_NONE_MSG_ID + 3,
    mtFetch                                  = SYSTEM_NONE_MSG_ID + 4,
    mtChunk                                  = SYSTEM_NONE_MSG_ID + 5,
    mtChunkReplica                           = SYSTEM_NONE_MSG_ID + 6,
    mtRecoverChunk                           = SYSTEM_NONE_MSG_ID + 7,
    mtReplicaSyncRequest                     = SYSTEM_NONE_MSG_ID + 8,
    mtReplicaSyncResponse                    = SYSTEM_NONE_MSG_ID + 9,
    mtUnusedPlus10                           = SYSTEM_NONE_MSG_ID + 10,
    mtQueryResult                            = SYSTEM_NONE_MSG_ID + 11,
    mtError                                  = SYSTEM_NONE_MSG_ID + 12,
    mtSyncRequest                            = SYSTEM_NONE_MSG_ID + 13,
    mtSyncResponse                           = SYSTEM_NONE_MSG_ID + 14,
    mtCancelQuery                            = SYSTEM_NONE_MSG_ID + 15,
    mtRemoteChunk                            = SYSTEM_NONE_MSG_ID + 16,
    mtNotify                                 = SYSTEM_NONE_MSG_ID + 17,
    mtWait                                   = SYSTEM_NONE_MSG_ID + 18,
    mtBarrier                                = SYSTEM_NONE_MSG_ID + 19,
    mtBufferSend                             = SYSTEM_NONE_MSG_ID + 20,
    mtAlive                                  = SYSTEM_NONE_MSG_ID + 21,
    mtPrepareQuery                           = SYSTEM_NONE_MSG_ID + 22,
    mtResourcesFileExistsRequest             = SYSTEM_NONE_MSG_ID + 23,
    mtResourcesFileExistsResponse            = SYSTEM_NONE_MSG_ID + 24,
    mtAbort                                  = SYSTEM_NONE_MSG_ID + 25,
    mtCommit                                 = SYSTEM_NONE_MSG_ID + 26,
    mtCompleteQuery                          = SYSTEM_NONE_MSG_ID + 27,
    mtControl                                = SYSTEM_NONE_MSG_ID + 28,
    mtUpdateQueryResult                      = SYSTEM_NONE_MSG_ID + 29,
    mtNewClientStart                         = SYSTEM_NONE_MSG_ID + 30,
    mtNewClientComplete                      = SYSTEM_NONE_MSG_ID + 31,
    mtSecurityMessage                        = SYSTEM_NONE_MSG_ID + 32,
    mtSecurityMessageResponse                = SYSTEM_NONE_MSG_ID + 33,
    mtSystemMax // must be last, make sure scidb::SYSTEM_MAX_MSG_ID is set to this value
};


class MessageHeader
{
private:
    uint16_t _netProtocolVersion;         /** < Version of network protocol */
    uint16_t _messageType;                /** < Type of message */
    uint64_t _recordSize;                 /** < The size of structured part of message to know what buffer size we must allocate */
    uint64_t _binarySize;                 /** < The size of unstructured part of message to know what buffer size we must allocate */
    InstanceID _sourceInstanceID;         /** < The source instance number */
    QueryID _queryID;                    /** < Query ID */

public:
    MessageHeader()
        : _netProtocolVersion(0),
          _messageType(0),
          _recordSize(0),
          _binarySize(0),
          _sourceInstanceID(0)
          {}

    uint16_t getNetProtocolVersion() const { return _netProtocolVersion; }
    uint16_t getMessageType() const { return _messageType; }
    uint64_t getRecordSize() const { return _recordSize; }
    uint64_t getBinarySize() const { return _binarySize; }
    InstanceID getSourceInstanceID() const { return _sourceInstanceID; }
    const QueryID& getQueryID() const { return _queryID; }

    void setNetProtocolVersion(uint16_t v) { _netProtocolVersion = v; }
    void setRecordSize(uint64_t v) { _recordSize = v; }
    void setBinarySize(uint64_t v) { _binarySize = v; }
    void setMessageType(uint16_t v){ _messageType = v; }
    void setSourceInstanceID(InstanceID v) { _sourceInstanceID = v; }
    void setQueryID(const QueryID& v) { _queryID = v; }

    /**
     * Convert the MessageHeader to a string optionally prefixing a
     * string prior to the converted MessageHeader
     *
     * @param strPrefix - the prefix string to use (optional)
     */
    std::string str(const std::string &strPrefix = "") const;
};

std::ostream& operator <<(
    std::ostream& out,
    const MessageHeader &messsageHeader);



/**
 * Message descriptor with all necessary parts
 */
class MessageDesc
{
public:
   MessageDesc();
   MessageDesc(MessageID messageType);
   MessageDesc(const std::shared_ptr< SharedBuffer >& binary);
   MessageDesc(MessageID messageType, const std::shared_ptr< SharedBuffer >& binary);
   virtual ~MessageDesc() {}
   void writeConstBuffers(std::vector<boost::asio::const_buffer>& constBuffers);
   bool parseRecord(size_t bufferSize);
   void prepareBinaryBuffer();

   InstanceID getSourceInstanceID() {
      return _messageHeader.getSourceInstanceID();
   }

   /**
    * This method is not part of the public API
    */
   void setSourceInstanceID(const InstanceID& instanceId) {
      _messageHeader.setSourceInstanceID( instanceId );
   }

   template <class Derived>
    std::shared_ptr<Derived> getRecord() {
        return std::static_pointer_cast<Derived>(_record);
    }

    MessageID getMessageType() {
       return static_cast<MessageID>(_messageHeader.getMessageType());
    }

    std::shared_ptr< SharedBuffer > getBinary()
    {
        return _binary;
    }

    virtual bool validate();

    size_t getMessageSize() const
    {
        return  _messageHeader.getRecordSize() +
                _messageHeader.getBinarySize() +
                sizeof(MessageHeader);
    }

    QueryID getQueryID() const
    {
        return _messageHeader.getQueryID();
    }

    void setQueryID(QueryID queryID)
    {
        _messageHeader.setQueryID(queryID);
    }

    void initRecord(MessageID messageType)
    {
       assert( _messageHeader.getMessageType() == mtNone);
       _record = createRecord(messageType);
       _messageHeader.setMessageType(
            static_cast<uint16_t>(messageType));
    }

    /**
     * Convert the MessageDesc to a string optionally prefixing a
     * string prior to the converted MessageDesc.
     *
     * @param strPrefix - the prefix string to use (optional)
     */
    std::string str(const std::string &strPrefix = "") const;


    /**
     * primarily for serialization purposes
     */
    MessageHeader & getMessageHeader() {return _messageHeader; }
    const MessageHeader & getMessageHeader() const { return _messageHeader; }

    /**
     * primarily for serialization purposes
     */
    const MessagePtr &getRecord() const { return _record; }

 protected:

    virtual MessagePtr createRecord(MessageID messageType)
    {
       return createRecordByType(static_cast<MessageID>(messageType));
    }

private:

    void init(MessageID messageType);
    MessageHeader _messageHeader;   /** < Message header */
    MessagePtr _record;             /** < Structured part of message */
    std::shared_ptr< SharedBuffer > _binary;     /** < Buffer for binary data to be transfered */
    boost::asio::streambuf _recordStream; /** < Buffer for serializing Google Protocol Buffers objects */

    static MessagePtr createRecordByType(MessageID messageType);

    friend class BaseConnection;
    friend class Connection;
};


std::ostream& operator <<(
    std::ostream& out,
    MessageDesc &messsageDesc);



/**
 * Base class for connection to a network manager and send message to it.
 * Class uses sync mode and knows nothing about NetworkManager.
 */
class BaseConnection
{
protected:

        boost::asio::ip::tcp::socket _socket;
        /**
         * Set socket options such as TCP_KEEP_ALIVE
         */
        void configConnectedSocket();

public:
        BaseConnection(boost::asio::io_service& ioService);
        virtual ~BaseConnection();

        /// Connect to remote site
        void connect(std::string address, uint32_t port);

        virtual void disconnect();

        boost::asio::ip::tcp::socket& getSocket() {
            return _socket;
        }

        /**
         * Send message to peer and read message from it.
         * @param inMessageDesc a message descriptor for sending message.
         * @param template MessageDesc_tt must implement MessageDesc APIs
         * @return message descriptor of received message.
         * @throw System::Exception
         */
        template <class MessageDesc_tt>
        std::shared_ptr<MessageDesc_tt> sendAndReadMessage(std::shared_ptr<MessageDesc>& inMessageDesc);

         /**
         * Send message to peer
         * @param inMessageDesc a message descriptor for sending message.
         * @throw System::Exception
         */
        void send(std::shared_ptr<MessageDesc>& messageDesc);

        /**
         * Receive message from peer
         * @param template MessageDesc_tt must implement MessageDesc APIs
         * @return message descriptor of received message
         * @throw System::Exception
         */
        template <class MessageDesc_tt>
        std::shared_ptr<MessageDesc_tt> receive();

        static log4cxx::LoggerPtr logger;
};

template <class MessageDesc_tt>
std::shared_ptr<MessageDesc_tt> BaseConnection::sendAndReadMessage(std::shared_ptr<MessageDesc>& messageDesc)
{
    LOG4CXX_TRACE(BaseConnection::logger, "The sendAndReadMessage: begin");
    send(messageDesc);
    std::shared_ptr<MessageDesc_tt> resultDesc = receive<MessageDesc_tt>();
    LOG4CXX_TRACE(BaseConnection::logger, "The sendAndReadMessage: end");
    return resultDesc;
}

template <class MessageDesc_tt>
std::shared_ptr<MessageDesc_tt> BaseConnection::receive()
{
    LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::receive: begin");
    std::shared_ptr<MessageDesc_tt> resultDesc(new MessageDesc_tt());
    try
    {
        // Reading message description
        size_t readBytes = read(_socket, boost::asio::buffer(&resultDesc->_messageHeader, sizeof(resultDesc->_messageHeader)));
        assert(readBytes == sizeof(resultDesc->_messageHeader));
        ASSERT_EXCEPTION((resultDesc->validate()), "BaseConnection::receive:");
        // TODO: This must not be an assert but exception of correct handled backward compatibility
        ASSERT_EXCEPTION(
            (   resultDesc->_messageHeader.getNetProtocolVersion() ==
                NET_PROTOCOL_CURRENT_VER),
            "BaseConnection::receive:");

        // Reading serialized structured part
        readBytes = read(_socket, resultDesc->_recordStream.prepare(
            resultDesc->_messageHeader.getRecordSize()));
        assert(readBytes == resultDesc->_messageHeader.getRecordSize());

        LOG4CXX_TRACE(BaseConnection::logger,
            "BaseConnection::receive: recordSize="
                << resultDesc->_messageHeader.getRecordSize());

        bool rc = resultDesc->parseRecord(readBytes);
        ASSERT_EXCEPTION(rc, "BaseConnection::receive:");

        resultDesc->prepareBinaryBuffer();

        if (resultDesc->_messageHeader.getBinarySize() > 0)
        {
            readBytes = read(_socket,
                boost::asio::buffer(
                    resultDesc->_binary->getData(),
                    resultDesc->_binary->getSize()));

            assert(readBytes == resultDesc->_binary->getSize());
        }

        LOG4CXX_TRACE(BaseConnection::logger,
            "read message: messageType="
                << resultDesc->_messageHeader.getMessageType()
                << " ; binarySize="
                << resultDesc->_messageHeader.getBinarySize());

        LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::receive: end");
    }
    catch (const boost::exception &e)
    {
        LOG4CXX_TRACE(BaseConnection::logger, "BaseConnection::receive: exception: "<< boost::diagnostic_information(e));
        throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_CANT_SEND_RECEIVE);
    }
    return resultDesc;
}

}

#endif /* SYNCCONNECTION_H_ */
