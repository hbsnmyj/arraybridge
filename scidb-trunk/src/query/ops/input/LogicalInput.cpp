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
 * @file LogicalInput.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Input operator for inputing data from external files into array
 */
#include <log4cxx/logger.h>

#include "InputArray.h"
#include <query/Operator.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/Cluster.h>
#include <system/Resources.h>
#include <system/Warnings.h>
#include "LogicalInput.h"
#include <usr_namespace/NamespacesCommunicator.h>
#include <usr_namespace/Permissions.h>

using namespace std;
using namespace boost;

static log4cxx::LoggerPtr oplogger(log4cxx::Logger::getLogger("scidb.ops.input"));

namespace scidb
{
const char* LogicalInput::OP_INPUT_NAME="input";
/**
 * Must be called as INPUT('existing_array_name', '/path/to/file/on/instance')
 */
LogicalInput::LogicalInput(const std::string& logicalName, const std::string& alias): LogicalOperator(logicalName, alias)
{
    ADD_PARAM_SCHEMA();   //0
    ADD_PARAM_CONSTANT("string");//1
    ADD_PARAM_VARIES();          //2
}

std::vector<std::shared_ptr<OperatorParamPlaceholder> > LogicalInput::nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
{
    std::vector<std::shared_ptr<OperatorParamPlaceholder> > res;
    res.reserve(2);
    res.push_back(END_OF_VARIES_PARAMS());
    switch (_parameters.size()) {
      case 0:
      case 1:
        assert(false);
        break;
      case 2:
        res.push_back(PARAM_CONSTANT("int64"));
        break;
      case 3:
        res.push_back(PARAM_CONSTANT("string"));
        break;
      case 4:
        res.push_back(PARAM_CONSTANT("int64"));
        break;
      case 5:
        res.push_back(PARAM_OUT_ARRAY_NAME());
        res.push_back(PARAM_CONSTANT("bool"));
        break;
      case 6:
        res.push_back(PARAM_CONSTANT("bool"));
        break;
    }
    return res;
}

std::string LogicalInput::inferPermissions(std::shared_ptr<Query>& query)
{
    // Ensure we have proper permissions
    std::string permissions;
    permissions.push_back(scidb::permissions::namespaces::ReadArray);
    return permissions;
}

ArrayDesc LogicalInput::inferSchema(
    std::vector< ArrayDesc> inputSchemas,
    std::shared_ptr< Query> query)
{
    assert(inputSchemas.size() == 0);

    InstanceID instanceID = COORDINATOR_INSTANCE_MASK;
    if (_parameters.size() >= 3)
    {
        instanceID = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[2])->getExpression(),
                                  query, TID_INT64).getInt64();
        if (instanceID != COORDINATOR_INSTANCE_MASK && instanceID != ALL_INSTANCE_MASK &&
            !isValidPhysicalInstance(instanceID))
        {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_INVALID_INSTANCE_ID,
                                       _parameters[2]->getParsingContext()) << instanceID;
        }
    }

    const string &path = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                                  query, TID_STRING).getString();

    string format;
    if (_parameters.size() >= 4) {
        format = evaluate(((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[3])->getExpression(),
                                            query, TID_STRING).getString();
        if (!InputArray::isSupportedFormat(format))
        {
            throw  USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_UNSUPPORTED_FORMAT,
                                        _parameters[3]->getParsingContext()) << format;
        }
    }

    bool isStrictSet = false;
    if (_parameters.size() >= 6) {
        if(_parameters[5]->getParamType() == PARAM_ARRAY_REF) {
            // nothing
        } else if (_parameters[5]->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            isStrictSet = true;
            if(isDebug()) {
                OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[5].get());
                SCIDB_ASSERT(lExp->isConstant());
                assert(lExp->getExpectedType()==TypeLibrary::getType(TID_BOOL));
            }
        } else {
            ASSERT_EXCEPTION(false, "LogicalInput::inferSchema: ");
        }
    }

    if (_parameters.size() >= 7) {
        if (isStrictSet) {
            throw  USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_WRONG_OPERATOR_ARGUMENTS_COUNT,
                                        _parameters[6]->getParsingContext()) << OP_INPUT_NAME << 6 << _parameters.size();
        }
        if (isDebug()) {
            assert(_parameters[6]->getParamType() == PARAM_LOGICAL_EXPRESSION);
            OperatorParamLogicalExpression* lExp = static_cast<OperatorParamLogicalExpression*>(_parameters[6].get());
            SCIDB_ASSERT(lExp->isConstant());
            assert(lExp->getExpectedType()==TypeLibrary::getType(TID_BOOL));
        }
    }

    if (instanceID == ALL_INSTANCE_MASK)
    {
        /* Let's support it: lets each instance assign unique coordiantes to its chunks based on
        * distribution function.  It is based on two assumptions:
        * - coordinates are not iportant (as in SQL)
        * - there can be holes in array
        *
        *           if (format[0] == '(') { // binary template loader
        *               throw  USER_QUERY_EXCEPTION(
        *                   SCIDB_SE_INFER_SCHEMA,
        *                   SCIDB_LE_INVALID_INSTANCE_ID,
        *                   _parameters[2]->getParsingContext())
        *                       << "-1 can not be used for binary template loader";
        *           }
        */
        //Distributed loading let's check file existence on all instances
        map<InstanceID, bool> instancesMap;
        Resources::getInstance()->fileExists(path, instancesMap, query);

        bool fileDetected = false;
        vector<InstanceID> instancesWithoutFile;
        for (map<InstanceID, bool>::const_iterator it = instancesMap.begin(); it != instancesMap.end(); ++it)
        {
            if (it->second)
            {
                if (!fileDetected)
                    fileDetected = true;
            }
            else
            {
                //Remembering file name on each missing file
                LOG4CXX_WARN(oplogger, "File '" << path << "' not found on instance #" << it->first);
                instancesWithoutFile.push_back(it->first);
            }
        }

        //Such file not found on any instance. Failing with exception
        if (!fileDetected)
        {
            throw USER_QUERY_EXCEPTION(
                SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_NOT_FOUND,
                _parameters[1]->getParsingContext()) << path;
        }

        //If some instances missing this file posting appropriate warning
        if (instancesWithoutFile.size())
        {
            stringstream instancesList;
            for (size_t i = 0, count = instancesWithoutFile.size();  i < count; ++i)
            {
                instancesList << instancesWithoutFile[i] << (i == count - 1 ? "" : ", ");
            }
            LOG4CXX_WARN(oplogger, "File " << path << " not found on instances " << instancesList.str());
            query->postWarning(SCIDB_WARNING(SCIDB_LE_FILE_NOT_FOUND_ON_INSTANCES) << path << instancesList.str());
        }
    }
    else if (instanceID == COORDINATOR_INSTANCE_MASK)
    {
        //This is loading from local instance. Throw error if file not found.
        if (path.find('@') == string::npos && !Resources::getInstance()->fileExists(path, query->getInstanceID(), query))
        {
            throw USER_QUERY_EXCEPTION(
                SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_NOT_FOUND,
                _parameters[1]->getParsingContext()) << filesystem::absolute(path);
        }
    }
    else
    {
        // convert from physical to logical
        instanceID = query->mapPhysicalToLogical(instanceID);

        //This is loading from single instance. Throw error if file not found.
        if (!Resources::getInstance()->fileExists(path, instanceID, query))
        {
            throw USER_QUERY_EXCEPTION(
                SCIDB_SE_INFER_SCHEMA, SCIDB_LE_FILE_NOT_FOUND,
                _parameters[1]->getParsingContext()) << filesystem::absolute(path);
        }
    }

    ArrayDesc arrayDesc = ((std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
    if (arrayDesc.isAutochunked()) {
        throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_AUTOCHUNKING_NOT_SUPPORTED) << getLogicalName();
    }
    Dimensions const& srcDims = arrayDesc.getDimensions();

    //Use array name from catalog if possible or generate temporary name
    string inputArrayName = arrayDesc.getName();
    PartitioningSchema partitioningSchema = psUninitialized;
    if (arrayDesc.getDistribution()) {
        partitioningSchema = arrayDesc.getDistribution()->getPartitioningSchema();
    }

    ArrayDistPtr dist;
    if (instanceID != ALL_INSTANCE_MASK) {
        // loading from a single file/instance as in
        // load(ARRAY, 'file.x', -2) or input(<XXX>[YYY], 'file.x', 0)
        partitioningSchema = psLocalInstance;
        dist = std::make_shared<LocalArrayDistribution>((instanceID == COORDINATOR_INSTANCE_MASK) ?
                                                        query->getInstanceID() :
                                                        instanceID);
    } else if (partitioningSchema == psUninitialized) {
        // in-line schema currently does not provide a distribution, i.e.
        // input(<XXX>[YYY], 'file.x')
        dist = createDistribution(psUndefined);
    } else {
        // the user-specified schema will be used for generating the implicit coordinates.
        // NOTICE that the optimizer will still be told psUndefined for any parallel ingest
        // (e.g.  load(ARRAY, 'file.x', -1, ...)
        // by PhysicalInput::getOutputDistribution() because some input formats (e.g. opaque, text)
        // may specify the data coordinates (in any distribution).
        dist = arrayDesc.getDistribution();
    }
    if (inputArrayName.empty())
    {
        inputArrayName = "tmp_input_array";
    }

    SCIDB_ASSERT(dist);

    return ArrayDesc(inputArrayName,
                     arrayDesc.getAttributes(),
                     srcDims,
                     dist,
                     query->getDefaultArrayResidency(),
                     arrayDesc.getFlags());
}

void LogicalInput::inferArrayAccess(std::shared_ptr<Query>& query)
{
    LogicalOperator::inferArrayAccess(query);

    string shadowArrayNameOrg;
    if (_parameters.size() >= 6 && _parameters[5]->getParamType() == PARAM_ARRAY_REF) {
        shadowArrayNameOrg = ((std::shared_ptr<OperatorParamArrayReference>&)_parameters[5])->getObjectName();
    }
    if (!shadowArrayNameOrg.empty()) {
        assert(shadowArrayNameOrg.find('@') == std::string::npos);

        std::string shadowArrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(shadowArrayNameOrg, namespaceName, shadowArrayName);

        std::shared_ptr<SystemCatalog::LockDesc>  lock(
            new SystemCatalog::LockDesc(
                namespaceName,
                shadowArrayName,
                query->getQueryID(),
                Cluster::getInstance()->getLocalInstanceId(),
                SystemCatalog::LockDesc::COORD,
                SystemCatalog::LockDesc::WR));
        std::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        assert(resLock);
        assert(resLock->getLockMode() >= SystemCatalog::LockDesc::WR);
    }
}

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalInput, LogicalInput::OP_INPUT_NAME)


} //namespace
