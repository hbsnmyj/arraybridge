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
 * @file MessageUtils.h
 *
 * @author roman.simakov@gmail.com
 */

#ifndef MESSAGEUTILS_H_
#define MESSAGEUTILS_H_

#include <memory>

#include <network/proto/scidb_msg.pb.h>
#include <network/BaseConnection.h>
#include <array/Array.h>
#include <system/Exceptions.h>
#include <query/Query.h>

namespace scidb
{

#ifndef SCIDB_CLIENT

/// @param forClient set to true if the message is to be sent to the client, false by default
std::shared_ptr<MessageDesc> makeErrorMessageFromExceptionForClient(const Exception& e, QueryID queryID);
std::shared_ptr<MessageDesc> makeErrorMessageFromException(const Exception& e, QueryID queryID, bool forClient=false);
std::shared_ptr<MessageDesc> makeErrorMessage(int code, const std::string& errorMessage, QueryID queryID);
std::shared_ptr<MessageDesc> makeOkMessage(QueryID queryID);
std::shared_ptr<MessageDesc> makeAbortMessage(QueryID queryID);
std::shared_ptr<MessageDesc> makeCommitMessage(QueryID queryID);
std::shared_ptr<MessageDesc> makeWaitMessage(QueryID queryID);
std::shared_ptr<MessageDesc> makeNotifyMessage(QueryID queryID);

bool parseQueryLiveness(std::shared_ptr<InstanceLiveness>& queryLiveness,
                        std::shared_ptr<scidb_msg::PhysicalPlan>& ppMsg);

bool serializeQueryLiveness(std::shared_ptr<const InstanceLiveness>& queryLiveness,
                            std::shared_ptr<scidb_msg::PhysicalPlan>& ppMsg);

#endif //SCIDB_CLIENT

/// @param forClient set to true if the message is parsed on the client, false by default
std::shared_ptr<Exception> makeExceptionFromErrorMessageOnClient(const std::shared_ptr<MessageDesc> &msg);
std::shared_ptr<Exception> makeExceptionFromErrorMessage(const std::shared_ptr<MessageDesc> &msg, bool forClient=false);

/// @param forClient set to true if the message is parsed on the client, true by default
void makeExceptionFromErrorMessageAndThrowOnClient(const std::shared_ptr<MessageDesc> &msg);
} // namespace

#endif /* MESSAGEUTILS_H_ */
