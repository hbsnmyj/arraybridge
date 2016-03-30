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
 * Connection.cpp
 *
 *  Created on: Jan 15, 2010
 *      Author: roman.simakov@gmail.com
 */

#include <network/Connection.h>

#include <network/NetworkManager.h>

#include <system/Config.h>
#include <system/Exceptions.h>
#include <util/Notification.h>
#include <util/session/Session.h>

#include <log4cxx/logger.h>
#include <boost/bind.hpp>
#include <memory>

using namespace std;
using namespace boost;

namespace scidb
{
// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.services.network"));

/***
 * C o n n e c t i o n
 */
Connection::Connection(
    NetworkManager& networkManager,
    InstanceID sourceInstanceID,
    InstanceID instanceID):
    BaseConnection(networkManager.getIOService()),
    _messageQueue(instanceID),
    _networkManager(networkManager),
    _instanceID(instanceID),
    _sourceInstanceID(sourceInstanceID),
    _connectionState(NOT_CONNECTED),
    _isSending(false),
    _logConnectErrors(true)
{
   assert(sourceInstanceID != INVALID_INSTANCE);
}

void Connection::start()
{
    assert(_connectionState == NOT_CONNECTED);
    assert(!_error);

    _session = std::make_shared<Session>();

    std::string securityMode = scidb::Config::getInstance()->
        getOption<string>(CONFIG_SECURITY);

    _session->setSecurityMode(securityMode);
    if(securityMode.compare("trust") == 0)
    {
        ASSERT_EXCEPTION(
            _session->getAuthenticatedState() ==
            scidb::Session::AUTHENTICATION_STATE_E_NOT_INITIALIZED,
            "Invalid authentication state");

        _session->setAuthenticatedState(
            scidb::Session::AUTHENTICATION_STATE_E_AUTHORIZING);

        _session->setAuthenticatedState(
            scidb::Session::AUTHENTICATION_STATE_E_AUTHORIZED);
    }

    _connectionState = CONNECTED;
    getRemoteIp();

    LOG4CXX_DEBUG(logger, "Connection started from " << getPeerId());

    // The first work we should do is reading initial message from client
    _networkManager.getIOService().post(bind(&Connection::readMessage,
                                            shared_from_this()));
}

/*
 * @brief Read from socket sizeof(MessageHeader) bytes of data to _messageDesc.
 */
void Connection::readMessage()
{
   LOG4CXX_TRACE(logger, "Reading next message");

   assert(!_messageDesc);
   _messageDesc = make_shared<ServerMessageDesc>();
   // XXX TODO: add a timeout after we get the first byte
   async_read(_socket, asio::buffer(
        &_messageDesc->getMessageHeader(),
        sizeof(_messageDesc->getMessageHeader())),

   bind(&Connection::handleReadMessage, shared_from_this(),
       asio::placeholders::error,
       asio::placeholders::bytes_transferred));
}

/**
 * @brief Validate _messageDesc & read from socket
 *    _messageDesc->getMessageHeader().getRecordSize() bytes of data
 */
void Connection::handleReadMessage(
    const boost::system::error_code& error,
    size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }

   if(!_messageDesc->validate() ||
      _messageDesc->getMessageHeader().getSourceInstanceID() == _sourceInstanceID) {
      LOG4CXX_ERROR(logger, "Connection::handleReadMessage: unknown/malformed message,"
                            " closing connection");
      using namespace boost::asio::error;
      handleReadError(make_error_code(eof));
      return;
   }

   assert(bytes_transferr == sizeof(_messageDesc->getMessageHeader()));
   assert(_messageDesc->getMessageHeader().getSourceInstanceID() != _sourceInstanceID);
   assert(_messageDesc->getMessageHeader().getNetProtocolVersion() == NET_PROTOCOL_CURRENT_VER);

   async_read(_socket, _messageDesc->_recordStream.prepare(
       _messageDesc->getMessageHeader().getRecordSize()),

   bind(&Connection::handleReadRecordPart,
       shared_from_this(),  asio::placeholders::error,
       asio::placeholders::bytes_transferred));

   LOG4CXX_TRACE(logger, "Connection::handleReadMessage: "
            << "messageType="
            << _messageDesc->getMessageHeader().getMessageType()
            << "; from instanceID="
            << _messageDesc->getMessageHeader().getSourceInstanceID()
            << " ; recordSize="
            << _messageDesc->getMessageHeader().getRecordSize()
            << " ; messageDesc.binarySize="
            << _messageDesc->getMessageHeader().getBinarySize());
}

/*
 * @brief  If header indicates data is available, read from
 *         socket _messageDesc->binarySize bytes of data.
 */
void Connection::handleReadRecordPart(
    const boost::system::error_code& error,
    size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }

   assert(_messageDesc->validate());

   assert(  _messageDesc->getMessageHeader().getRecordSize() ==
            bytes_transferr);

   assert(  _messageDesc->getMessageHeader().getSourceInstanceID() !=
            _sourceInstanceID);

   if (!_messageDesc->parseRecord(bytes_transferr)) {
       LOG4CXX_ERROR(logger,
                     "Network error in handleReadRecordPart: cannot parse record for "
                     << " msgID="
                     << _messageDesc->getMessageHeader().getMessageType()
                     << ", closing connection");

       using boost::asio::error::make_error_code;
       using boost::asio::error::eof;
       handleReadError(make_error_code(eof));
       return;
   }
   _messageDesc->prepareBinaryBuffer();

   LOG4CXX_TRACE(logger,
        "handleReadRecordPart: "
            << " messageType="
            << _messageDesc->getMessageHeader().getMessageType()
            << " ; messageDesc.binarySize="
            << _messageDesc->getMessageHeader().getBinarySize());

   if (_messageDesc->_messageHeader.getBinarySize())
   {
      async_read(_socket,
            asio::buffer(
                _messageDesc->_binary->getData(),
                _messageDesc->_binary->getSize()),
                bind(
                    &Connection::handleReadBinaryPart,
                    shared_from_this(),
                    asio::placeholders::error,
                    asio::placeholders::bytes_transferred));
      return;
   }

   handleReadBinaryPart(error, 0);
}

/*
 * @brief Invoke the appropriate dispatch routine
 */
void Connection::handleReadBinaryPart(
    const boost::system::error_code& error,
    size_t bytes_transferr)
{
   if (error)
   {
      handleReadError(error);
      return;
   }
   assert(_messageDesc);

   assert(_messageDesc->getMessageHeader().getBinarySize() ==
          bytes_transferr);

   std::shared_ptr<MessageDesc> msgPtr;
   _messageDesc.swap(msgPtr);

   std::shared_ptr<Connection> self(shared_from_this());
   _networkManager.handleMessage(self, msgPtr);

   // Preparing to read new message
   assert(_messageDesc.get() == NULL);
   readMessage();
}

void Connection::sendMessage(std::shared_ptr<MessageDesc>& messageDesc,
                             NetworkManager::MessageQueueType mqt)
{
    pushMessage(messageDesc, mqt);
   _networkManager.getIOService().post(bind(&Connection::pushNextMessage,
                                            shared_from_this()));
}

void Connection::pushMessage(std::shared_ptr<MessageDesc>& messageDesc,
                             NetworkManager::MessageQueueType mqt)
{
    std::shared_ptr<const NetworkManager::ConnectionStatus> connStatus;
    {
        ScopedMutexLock mutexLock(_mutex);

        LOG4CXX_TRACE(logger, "pushMessage: send message queue size = "
                      << _messageQueue.size()
                      << " for instanceID=" << _instanceID);

       connStatus = _messageQueue.pushBack(mqt, messageDesc);
       publishQueueSizeIfNeeded(connStatus);
    }
    if (connStatus) {
        _networkManager.getIOService().post(bind(&Connection::publishQueueSize,
                                                 shared_from_this()));
    }
}

std::shared_ptr<MessageDesc> Connection::popMessage()
{
    std::shared_ptr<const NetworkManager::ConnectionStatus> connStatus;
    std::shared_ptr<MessageDesc> msg;
    {
        ScopedMutexLock mutexLock(_mutex);

        connStatus = _messageQueue.popFront(msg);

        publishQueueSizeIfNeeded(connStatus);
    }
    if (connStatus) {
        _networkManager.getIOService().post(bind(&Connection::publishQueueSize,
                                                 shared_from_this()));
    }
    return msg;
}

void Connection::setRemoteQueueState(NetworkManager::MessageQueueType mqt,  uint64_t size,
                                     uint64_t localGenId, uint64_t remoteGenId,
                                     uint64_t localSn, uint64_t remoteSn)
{
    assert(mqt != NetworkManager::mqtNone);
    std::shared_ptr<const NetworkManager::ConnectionStatus> connStatus;
    {
        ScopedMutexLock mutexLock(_mutex);

        connStatus = _messageQueue.setRemoteState(mqt, size,
                                                  localGenId, remoteGenId,
                                                  localSn, remoteSn);

        LOG4CXX_TRACE(logger, "setRemoteQueueSize: remote queue size = "
                      << size <<" for instanceID="<<_instanceID << " for queue "<<mqt);

        publishQueueSizeIfNeeded(connStatus);
    }
    if (connStatus) {
        _networkManager.getIOService().post(bind(&Connection::publishQueueSize,
                                                 shared_from_this()));
    }
    _networkManager.getIOService().post(bind(&Connection::pushNextMessage,
                                             shared_from_this()));
}

bool
Connection::publishQueueSizeIfNeeded(const std::shared_ptr<const NetworkManager::ConnectionStatus>& connStatus)
{
    SCIDB_ASSERT(_mutex.isLockedByThisThread());

    if (!connStatus) {
        return false;
    }
    _statusesToPublish[connStatus->getQueueType()] = connStatus;
    return true;
}

void Connection::publishQueueSize()
{
    InstanceID instanceId = INVALID_INSTANCE;
    ConnectionStatusMap toPublish;
    {
        ScopedMutexLock mutexLock(_mutex);
        instanceId = _instanceID;
        toPublish.swap(_statusesToPublish);
    }
    for (ConnectionStatusMap::iterator iter = toPublish.begin();
         iter != toPublish.end(); ++iter) {

        std::shared_ptr<const NetworkManager::ConnectionStatus>& status = iter->second;
        NetworkManager::MessageQueueType mqt = iter->first;
        assert(mqt == status->getQueueType());
        assert(mqt != NetworkManager::mqtNone);
        assert(mqt < NetworkManager::mqtMax);
        LOG4CXX_TRACE(logger, "publishQueueSize: publishing queue size = "
                      << status->getAvailabeQueueSize()
                      <<" for instanceID="<< instanceId
                      <<" for queue type="<< mqt);

        Notification<NetworkManager::ConnectionStatus> event(status);
        event.publish();
    }
}

void Connection::handleSendMessage(const boost::system::error_code& error,
                                   size_t bytes_transferred,
                                   std::shared_ptr< std::list<std::shared_ptr<MessageDesc> > >& msgs,
                                   size_t bytes_sent)
{
   _isSending = false;
   if (!error) { // normal case
       assert(msgs);
       assert(bytes_transferred == bytes_sent);
       if (logger->isTraceEnabled()) {
           for (std::list<std::shared_ptr<MessageDesc> >::const_iterator i = msgs->begin();
                i != msgs->end(); ++i) {
               const std::shared_ptr<MessageDesc>& messageDesc = *i;
               LOG4CXX_TRACE(logger, "handleSendMessage: bytes_transferred="
                             << messageDesc->getMessageSize()
                             << ", "<< getPeerId()
                             <<", msgID =" << messageDesc->getMessageType());
           }
       }

       pushNextMessage();
       return;
   }

   // errror case

   assert(error != asio::error::interrupted);
   assert(error != asio::error::would_block);
   assert(error != asio::error::try_again);

   LOG4CXX_ERROR(logger, "Network error in handleSendMessage #"
                 << error.value() << "('" << error.message() << "')"
                 << ", "<< getPeerId());

   for (std::list<std::shared_ptr<MessageDesc> >::const_iterator i = msgs->begin();
        i != msgs->end(); ++i) {
       const std::shared_ptr<MessageDesc>& messageDesc = *i;
       _networkManager.handleConnectionError(messageDesc->getQueryID());
   }

   if (_connectionState == CONNECTED) {
       disconnectInternal();
   }
   if (_instanceID == INVALID_INSTANCE) {
       LOG4CXX_TRACE(logger, "Not recovering connection from "<<getPeerId());
       return;
   }

   LOG4CXX_DEBUG(logger, "Recovering connection to " << getPeerId());
   _networkManager.reconnect(_instanceID);
}

void Connection::pushNextMessage()
{
   if (_connectionState != CONNECTED) {
      assert(!_isSending);
      LOG4CXX_TRACE(logger, "Not yet connected to " << getPeerId());
      return;
   }
   if (_isSending) {
      LOG4CXX_TRACE(logger, "Already sending to " << getPeerId());
      return;
   }

   vector<asio::const_buffer> constBuffers;
   typedef std::list<std::shared_ptr<MessageDesc> > MessageDescList;
   std::shared_ptr<MessageDescList> msgs = make_shared<MessageDescList>();
   size_t size(0);
   const size_t maxSize(32*KiB); //XXX TODO: pop all the messages!!!

   while (true) {
       std::shared_ptr<MessageDesc> messageDesc = popMessage();
       if (!messageDesc) {
           break;
       }
       msgs->push_back(messageDesc);
       if (messageDesc->getMessageType() != mtAlive) {
           // mtAlive are useful only if there is no other traffic
           messageDesc->_messageHeader.setSourceInstanceID(
                _sourceInstanceID);

           messageDesc->writeConstBuffers(constBuffers);
           size += messageDesc->getMessageSize();
           if (size >= maxSize) {
               break;
           }
       }
   }
   if (msgs->empty()) {
       LOG4CXX_TRACE(logger, "Nothing to send to " << getPeerId());
       return;
   }
   if (_instanceID != CLIENT_INSTANCE) {
       std::shared_ptr<MessageDesc> controlMsg = getControlMessage();
       if (controlMsg) {
           msgs->push_back(controlMsg);
           controlMsg->writeConstBuffers(constBuffers);
           controlMsg->_messageHeader.setSourceInstanceID(
                _sourceInstanceID);
           size += controlMsg->getMessageSize();
       }
   }
   if (size == 0) {
       assert(!msgs->empty());
       assert(msgs->front()->getMessageType() == mtAlive);
       std::shared_ptr<MessageDesc>& aliveMsg = msgs->front();
       aliveMsg->writeConstBuffers(constBuffers);
       aliveMsg->_messageHeader.setSourceInstanceID(_sourceInstanceID);
       size += aliveMsg->getMessageSize();
   }

   asio::async_write(_socket, constBuffers,
                     bind(&Connection::handleSendMessage,
                          shared_from_this(),
                          asio::placeholders::error,
                          asio::placeholders::bytes_transferred,
                          msgs, size));
   _isSending = true;
}

MessagePtr
Connection::ServerMessageDesc::createRecord(MessageID messageType)
{
   if (messageType < mtSystemMax) {
      return MessageDesc::createRecord(messageType);
   }

   std::shared_ptr<NetworkMessageFactory> msgFactory;
   msgFactory = getNetworkMessageFactory();
   assert(msgFactory);
   MessagePtr recordPtr = msgFactory->createMessage(messageType);

   if (!recordPtr) {
      LOG4CXX_ERROR(logger, "Unknown message type " << messageType);
      throw SYSTEM_EXCEPTION(SCIDB_SE_NETWORK, SCIDB_LE_UNKNOWN_MESSAGE_TYPE) << messageType;
   }
   return recordPtr;
}

bool
Connection::ServerMessageDesc::validate()
{
   if (MessageDesc::validate()) {
      return true;
   }
   std::shared_ptr<NetworkMessageFactory> msgFactory;
   msgFactory = getNetworkMessageFactory();
   assert(msgFactory);
   return msgFactory->isRegistered(getMessageType());
}

void Connection::onResolve(std::shared_ptr<asio::ip::tcp::resolver>& resolver,
                           std::shared_ptr<asio::ip::tcp::resolver::query>& query,
                           const boost::system::error_code& err,
                           asio::ip::tcp::resolver::iterator endpoint_iterator)
 {
    assert(query);
    assert(resolver);

    if (_connectionState != CONNECT_IN_PROGRESS ||
        _query != query) {
       LOG4CXX_DEBUG(logger, "Dropping resolve query "
                     << query->host_name() << ":" << query->service_name());
       return;
    }

    asio::ip::tcp::resolver::iterator end;
    if (err || endpoint_iterator == end) {
       _error = err ? err : boost::asio::error::host_not_found;
       if (_logConnectErrors) {
          _logConnectErrors = false;
          LOG4CXX_ERROR(logger, "Network error #"
                        << _error.value() << "('" << _error.message() << "')"
                        << " while resolving name of "
                        << getPeerId() << ", "
                        << _query->host_name() << ":" << _query->service_name());

       }
       disconnectInternal();
       _networkManager.reconnect(_instanceID);
       return;
    }

    LOG4CXX_TRACE(logger, "Connecting to the first candidate for: "
                  << _query->host_name() << ":" << _query->service_name());
    asio::ip::tcp::endpoint ep = *endpoint_iterator;
    _socket.async_connect(ep,
                          boost::bind(&Connection::onConnect, shared_from_this(),
                                      resolver, query,
                                      ++endpoint_iterator,
                                      boost::asio::placeholders::error));
 }

void Connection::onConnect(std::shared_ptr<asio::ip::tcp::resolver>& resolver,
                           std::shared_ptr<asio::ip::tcp::resolver::query>& query,
                           asio::ip::tcp::resolver::iterator endpoint_iterator,
                           const boost::system::error_code& err)

{
   assert(query);
   assert(resolver);
   asio::ip::tcp::resolver::iterator end;

   if (_connectionState != CONNECT_IN_PROGRESS ||
       _query != query) {
      LOG4CXX_TRACE(logger, "Dropping resolve query "
                    << query->host_name() << ":" << query->service_name());
      return;
   }

   if (err && endpoint_iterator == end) {
      if (_logConnectErrors) {
         _logConnectErrors = false;
         LOG4CXX_ERROR(logger, "Network error #"
                       << err.value() << "('" << err.message() << "')"
                       << " while connecting to "
                       << getPeerId() << ", "
                       << _query->host_name() << ":" << _query->service_name());
      }
      disconnectInternal();
      _error = err;
      _networkManager.reconnect(_instanceID);
      return;
   }

   if (err) {
      LOG4CXX_TRACE(logger, "Connecting to the next candidate,"
                    << getPeerId() << ", "
                    << _query->host_name() << ":" << _query->service_name()
                    << "Last error #"
                    << err.value() << "('" << err.message() << "')");
      _error = err;
      _socket.close();
      asio::ip::tcp::endpoint ep = *endpoint_iterator;
      _socket.async_connect(ep,
                            boost::bind(&Connection::onConnect, shared_from_this(),
                                        resolver, query,
                                        ++endpoint_iterator,
                                        boost::asio::placeholders::error));
      return;
   }

   configConnectedSocket();
   getRemoteIp();

   LOG4CXX_DEBUG(logger, "Connected to "
                 << getPeerId() << ", "
                 << _query->host_name() << ":"
                 << _query->service_name());

   _connectionState = CONNECTED;
   _error.clear();
   _query.reset();
   _logConnectErrors = true;

   assert(!_isSending);
   pushNextMessage();
}

void Connection::connectAsync(const string& address, uint16_t port)
{
   _networkManager.getIOService().post(bind(&Connection::connectAsyncInternal,
                                            shared_from_this(), address, port));
}

void Connection::connectAsyncInternal(const string& address, uint16_t port)
{
   if (_connectionState == CONNECTED ||
       _connectionState == CONNECT_IN_PROGRESS) {
      LOG4CXX_WARN(logger, "Already connected/ing! Not Connecting to " << address << ":" << port);
      return;
   }

   disconnectInternal();
   LOG4CXX_TRACE(logger, "Connecting (async) to " << address << ":" << port);

   //XXX TODO: switch to using scidb::resolveAsync()
   std::shared_ptr<asio::ip::tcp::resolver>  resolver(new asio::ip::tcp::resolver(_socket.get_io_service()));
   stringstream serviceName;
   serviceName << port;
   _query.reset(new asio::ip::tcp::resolver::query(address, serviceName.str()));
   _error.clear();
   _connectionState = CONNECT_IN_PROGRESS;
   resolver->async_resolve(*_query,
                           boost::bind(&Connection::onResolve, shared_from_this(),
                                       resolver, _query,
                                       boost::asio::placeholders::error,
                                       boost::asio::placeholders::iterator));
}

void Connection::attachQuery(
    QueryID queryID,
    ClientContext::DisconnectHandler& dh)
{
    // Note:  at this point in time the query object itself has
    // not been instantiated.
    ScopedMutexLock mutexLock(_mutex);
    _activeClientQueries[queryID] = dh;
}

void Connection::attachQuery(QueryID queryID)
{
    // Note:  at this point in time the query object itself has
    // not been instantiated.
    ClientContext::DisconnectHandler dh;
    attachQuery(queryID, dh);
}

void Connection::detachQuery(QueryID queryID)
{
    ScopedMutexLock mutexLock(_mutex);
    _activeClientQueries.erase(queryID);
}

void Connection::disconnectInternal()
{
   LOG4CXX_DEBUG(logger, "Disconnecting from " << getPeerId());
   _socket.close();
   _connectionState = NOT_CONNECTED;
   _query.reset();
   _remoteIp = boost::asio::ip::address();
   ClientQueries clientQueries;
   {
       ScopedMutexLock mutexLock(_mutex);
       clientQueries.swap(_activeClientQueries);
   }

   LOG4CXX_TRACE(logger, str(format("Number of active client queries %lld") % clientQueries.size()));

   for (ClientQueries::const_iterator i = clientQueries.begin();
        i != clientQueries.end(); ++i)
   {
       assert(_instanceID == CLIENT_INSTANCE);
       QueryID queryID = i->first;
       const ClientContext::DisconnectHandler& dh = i->second;
       _networkManager.handleClientDisconnect(queryID, dh);
   }
}

void Connection::disconnect()
{
   _networkManager.getIOService().post(bind(&Connection::disconnectInternal,
                                            shared_from_this()));
}

void Connection::handleReadError(const boost::system::error_code& error)
{
   assert(error);

   if (error != boost::asio::error::eof) {
      LOG4CXX_ERROR(logger, "Network error while reading, #"
                    << error.value() << "('" << error.message() << "')");
   } else {
      LOG4CXX_TRACE(logger, "Sender disconnected");
   }
   if (_connectionState == CONNECTED) {
      disconnectInternal();
   }
}

Connection::~Connection()
{
   LOG4CXX_TRACE(logger, "Destroying connection to " << getPeerId());
   abortMessages();
   disconnectInternal();
}

void Connection::abortMessages()
{
    MultiChannelQueue connQ(_instanceID);
    {
        ScopedMutexLock mutexLock(_mutex);
        connQ.swap(_messageQueue);
    }
    LOG4CXX_TRACE(logger, "Aborting "<< connQ.size()
                  << " buffered connection messages to "
                  << getPeerId());
   connQ.abortMessages();
}

string Connection::getPeerId()
{
   string res((_instanceID == CLIENT_INSTANCE)
              ? std::string("CLIENT")
              : boost::str(boost::format("instance %lld") % _instanceID));
   if (boost::asio::ip::address() != _remoteIp) {
      boost::system::error_code ec;
      string ip(_remoteIp.to_string(ec));
      assert(!ec);
      return (res + boost::str(boost::format(" (%s)") % ip));
   }
   return res;
}

void Connection::getRemoteIp()
{
   boost::system::error_code ec;
   boost::asio::ip::tcp::endpoint endpoint = _socket.remote_endpoint(ec);
   if (!ec)
   {
      _remoteIp = endpoint.address();
   }
   else
   {
      LOG4CXX_ERROR(logger,
                    "Could not get the remote IP from connected socket to/from"
                    << getPeerId()
                    << ". Error:" << ec.value() << "('" << ec.message() << "')");
   }
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::MultiChannelQueue::pushBack(NetworkManager::MessageQueueType mqt,
                                       const std::shared_ptr<MessageDesc>& msg)
{
    assert(msg);
    assert(mqt<NetworkManager::mqtMax);

    std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        channel = std::make_shared<Channel>(_instanceId, mqt);
    }
    bool isActiveBefore = channel->isActive();

    std::shared_ptr<NetworkManager::ConnectionStatus> status = channel->pushBack(msg);
    ++_size;

    bool isActiveAfter = channel->isActive();
    if (isActiveBefore != isActiveAfter) {
        (isActiveAfter ? ++_activeChannelCount : --_activeChannelCount);
        assert(_activeChannelCount<=NetworkManager::mqtMax);
    }
    return status;
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::MultiChannelQueue::popFront(std::shared_ptr<MessageDesc>& msg)
{
    assert(!msg);

    Channel *channel(NULL);
    uint32_t start = _currChannel;
    while (true) {
        ++_currChannel;
        channel = _channels[_currChannel % NetworkManager::mqtMax].get();
        if ((_currChannel % NetworkManager::mqtMax) == (start % NetworkManager::mqtMax)) {
            break;
        }
        if (channel==NULL) {
            continue;
        }
        if (!channel->isActive()) {
            continue;
        }
        break;
    }
    std::shared_ptr<NetworkManager::ConnectionStatus> status;
    if (channel!=NULL && channel->isActive()) {

        status = channel->popFront(msg);
        assert(msg);
        --_size;

        _activeChannelCount -= (!channel->isActive());
        assert(_activeChannelCount<=NetworkManager::mqtMax);
    }
    return status;
}

std::shared_ptr<MessageDesc> Connection::getControlMessage()
{
    std::shared_ptr<MessageDesc> msgDesc = make_shared<MessageDesc>(mtControl);
    assert(msgDesc);

    namespace gpb = google::protobuf;

    std::shared_ptr<scidb_msg::Control> record = msgDesc->getRecord<scidb_msg::Control>();
    gpb::uint64 localGenId=0;
    gpb::uint64 remoteGenId=0;
    assert(record);
    {
        ScopedMutexLock mutexLock(_mutex);

        localGenId = _messageQueue.getLocalGenId();
        record->set_local_gen_id(localGenId);

        remoteGenId = _messageQueue.getRemoteGenId();
        record->set_remote_gen_id(remoteGenId);

        LOG4CXX_TRACE(logger, "Control message localGenId=" << localGenId
                      <<", remoteGenId=" << remoteGenId);

        for (uint32_t mqt = (NetworkManager::mqtNone+1); mqt < NetworkManager::mqtMax; ++mqt) {
            const gpb::uint64 localSn   = _messageQueue.getLocalSeqNum(NetworkManager::MessageQueueType(mqt));
            const gpb::uint64 remoteSn  = _messageQueue.getRemoteSeqNum(NetworkManager::MessageQueueType(mqt));
            const gpb::uint32 id        = mqt;
            scidb_msg::Control_Channel* entry = record->add_channels();
            assert(entry);
            entry->set_id(id);
            entry->set_local_sn(localSn);
            entry->set_remote_sn(remoteSn);
        }
    }
    gpb::RepeatedPtrField<scidb_msg::Control_Channel>* entries = record->mutable_channels();
    for(gpb::RepeatedPtrField<scidb_msg::Control_Channel>::iterator iter = entries->begin();
        iter != entries->end(); ++iter) {
        scidb_msg::Control_Channel& entry = (*iter);
        assert(entry.has_id());
        const NetworkManager::MessageQueueType mqt = static_cast<NetworkManager::MessageQueueType>(entry.id());
        const gpb::uint64 available = _networkManager.getAvailable(NetworkManager::MessageQueueType(mqt), _instanceID);
        entry.set_available(available);
    }

    if (logger->isTraceEnabled()) {
        const gpb::RepeatedPtrField<scidb_msg::Control_Channel>& channels = record->channels();
        for(gpb::RepeatedPtrField<scidb_msg::Control_Channel>::const_iterator iter = channels.begin();
            iter != channels.end(); ++iter) {
            const scidb_msg::Control_Channel& entry = (*iter);
            const NetworkManager::MessageQueueType mqt  = static_cast<NetworkManager::MessageQueueType>(entry.id());
            const uint64_t available    = entry.available();
            const uint64_t remoteSn     = entry.remote_sn();
            const uint64_t localSn      = entry.local_sn();

            LOG4CXX_TRACE(logger, "getControlMessage: Available queue size=" << available
                          << ", instanceID="<<_instanceID
                          << ", queue="<<mqt
                          << ", localGenId="<<localGenId
                          << ", remoteGenId="<<remoteGenId
                          <<", localSn="<<localSn
                          <<", remoteSn="<<remoteSn);
        }
    }
    return msgDesc;
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::MultiChannelQueue::setRemoteState(NetworkManager::MessageQueueType mqt,
                                              uint64_t rSize,
                                              uint64_t localGenId, uint64_t remoteGenId,
                                              uint64_t localSn, uint64_t remoteSn)
{
    // XXX TODO: consider turning asserts into exceptions
    std::shared_ptr<NetworkManager::ConnectionStatus> status;
    if (mqt>=NetworkManager::mqtMax) {
        assert(false);
        return status;
    }
    if (remoteGenId < _remoteGenId) {
        assert(false);
        return status;
    }
    if (localGenId > _localGenId) {
        assert(false);
        return status;
    }
    if (localGenId < _localGenId) {
        localSn = 0;
    }

    std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        channel = std::make_shared<Channel>(_instanceId, mqt);
    }
    if (!channel->validateRemoteState(rSize, localSn, remoteSn)) {
        assert(false);
        return status;
    }
    if (remoteGenId > _remoteGenId) {
        _remoteGenId = remoteGenId;
    }
    bool isActiveBefore = channel->isActive();

    status = channel->setRemoteState(rSize, localSn, remoteSn);

    bool isActiveAfter = channel->isActive();
    if (isActiveBefore != isActiveAfter) {
        (isActiveAfter ? ++_activeChannelCount : --_activeChannelCount);
        assert(_activeChannelCount<=NetworkManager::mqtMax);
    }
    return status;
}

uint64_t
Connection::MultiChannelQueue::getAvailable(NetworkManager::MessageQueueType mqt) const
{
    assert(mqt<NetworkManager::mqtMax);
    const std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        return NetworkManager::MAX_QUEUE_SIZE;
    }
    return channel->getAvailable();
}

uint64_t
Connection::MultiChannelQueue::getLocalSeqNum(NetworkManager::MessageQueueType mqt) const
{
    assert(mqt<NetworkManager::mqtMax);
    const std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        return 0;
    }
    return channel->getLocalSeqNum();
}

uint64_t
Connection::MultiChannelQueue::getRemoteSeqNum(NetworkManager::MessageQueueType mqt) const
{
    assert(mqt<NetworkManager::mqtMax);
    const std::shared_ptr<Channel>& channel = _channels[mqt];
    if (!channel) {
        return 0;
    }
    return channel->getRemoteSeqNum();
}

void
Connection::MultiChannelQueue::abortMessages()
{
    for (Channels::iterator iter = _channels.begin();
         iter != _channels.end(); ++iter) {
        std::shared_ptr<Channel>& channel = (*iter);
        if (!channel) {
            continue;
        }
        channel->abortMessages();
    }
    _activeChannelCount = 0;
    _size = 0;
}

void
Connection::MultiChannelQueue::swap(MultiChannelQueue& other)
{
    const InstanceID instanceId = _instanceId;
    _instanceId = other._instanceId;
    other._instanceId = instanceId;

    const uint32_t currMqt = _currChannel;
    _currChannel = other._currChannel;
    other._currChannel = currMqt;

    const size_t activeCount = _activeChannelCount;
    _activeChannelCount = other._activeChannelCount;
    other._activeChannelCount = activeCount;

    const uint64_t size = _size;
    _size = other._size;
    other._size = size;

    const uint64_t remoteGenId = _remoteGenId;
    _remoteGenId = other._remoteGenId;
    other._remoteGenId = remoteGenId;

    const uint64_t localGenId = _localGenId;
    _localGenId = other._localGenId;
    other._localGenId = localGenId;

    _channels.swap(other._channels);
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::pushBack(const std::shared_ptr<MessageDesc>& msg)
{
    if ( !_msgQ.empty() &&  msg->getMessageType() == mtAlive) {
        // mtAlive are useful only if there is no other traffic
        assert(_mqt == NetworkManager::mqtNone);
        return  std::shared_ptr<NetworkManager::ConnectionStatus>();
    }
    const uint64_t spaceBefore = getAvailable();
    if (spaceBefore <= 0) {
        throw NetworkManager::OverflowException(_mqt, REL_FILE, __FUNCTION__, __LINE__);
    }
    _msgQ.push_back(msg);
    const uint64_t spaceAfter = getAvailable();
    std::shared_ptr<NetworkManager::ConnectionStatus> status = getNewStatus(spaceBefore, spaceAfter);
    return status;
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::popFront(std::shared_ptr<MessageDesc>& msg)
{
    std::shared_ptr<NetworkManager::ConnectionStatus> status;
    if (!isActive()) {
        msg.reset();
        return status;
    }
    const uint64_t spaceBefore = getAvailable();
    msg = _msgQ.front();
    _msgQ.pop_front();
    ++_localSeqNum;
    const uint64_t spaceAfter = getAvailable();
    status = getNewStatus(spaceBefore, spaceAfter);

    LOG4CXX_TRACE(logger, "popFront: Channel "<< _mqt
                  << " to " << _instanceId << " "
                  << ((isActive()) ? "ACTIVE" : "BLOCKED"));

    return status;
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::setRemoteState(uint64_t rSize, uint64_t localSn, uint64_t remoteSn)
{
   const uint64_t spaceBefore = getAvailable();
    _remoteSize = rSize;
    _remoteSeqNum = remoteSn;
    _localSeqNumOnPeer = localSn;
    const uint64_t spaceAfter = getAvailable();
    std::shared_ptr<NetworkManager::ConnectionStatus> status = getNewStatus(spaceBefore, spaceAfter);

    LOG4CXX_TRACE(logger, "setRemoteState: Channel "<< _mqt
                  << " to " << _instanceId
                  << ", remoteSize="<<_remoteSize
                  << ", remoteSeqNum="<<_remoteSeqNum
                  << ", remoteSeqNumOnPeer="<<_localSeqNumOnPeer);
    return status;
}

void
Connection::Channel::abortMessages()
{
    MessageQueue mQ;
    mQ.swap(_msgQ);
    LOG4CXX_TRACE(logger, "abortMessages: Aborting "<< mQ.size()
                  << " buffered connection messages to "
                  << _instanceId);
    std::set<QueryID> queries;
    for (MessageQueue::iterator iter = mQ.begin();
         iter != mQ.end(); ++iter) {
        std::shared_ptr<MessageDesc>& messageDesc = (*iter);
        queries.insert(messageDesc->getQueryID());
    }
    mQ.clear();
    NetworkManager* networkManager = NetworkManager::getInstance();
    assert(networkManager);
    networkManager->handleConnectionError(_instanceId, queries);
}

std::shared_ptr<NetworkManager::ConnectionStatus>
Connection::Channel::getNewStatus(const uint64_t spaceBefore,
                                  const uint64_t spaceAfter)
{
    if ((spaceBefore != spaceAfter) && (spaceBefore == 0 || spaceAfter == 0)) {
        return std::make_shared<NetworkManager::ConnectionStatus>(_instanceId, _mqt, spaceAfter);
    }
    return std::shared_ptr<NetworkManager::ConnectionStatus>();
}

uint64_t
Connection::Channel::getAvailable() const
{
    const uint64_t localLimit = _sendQueueLimit;
    const uint64_t localSize  = _msgQ.size();

    if (localSize >= localLimit) {
        return 0;
    }
    return (localLimit-localSize);
}

} // namespace
