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
 * @file PhysicalInput.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Physical implementation of INPUT operator for inputing data from text file
 * which is located on coordinator
 */

#include <string.h>
#include <log4cxx/logger.h>

#include <query/Operator.h>
#include <query/QueryProcessor.h>
#include <query/QueryPlan.h>
#include <network/NetworkManager.h>
#include <system/Cluster.h>
#include "InputArray.h"

using namespace std;
using namespace boost;

namespace scidb
{

// Logger for network subsystem. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr oplogger(log4cxx::Logger::getLogger("scidb.ops.impl_input"));

class ParsingContext;
class PhysicalInput : public PhysicalOperator
{
public:
    PhysicalInput(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                  ArrayDesc const& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    int64_t getSourceInstanceID() const
    {
        if (_parameters.size() >= 3)
        {
            assert(_parameters[2]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            std::shared_ptr<OperatorParamPhysicalExpression> paramExpr =
                (std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[2];
            assert(paramExpr->isConstant());
            return paramExpr->getExpression()->evaluate().getInt64();
        }
        return COORDINATOR_INSTANCE_MASK;
    }

    virtual RedistributeContext getOutputDistribution(
            std::vector<RedistributeContext> const&,
            std::vector<ArrayDesc> const&) const
    {
        InstanceID sourceInstanceID = getSourceInstanceID();
        if (sourceInstanceID == ALL_INSTANCE_MASK) {
            //The file is loaded from multiple instances - the distribution could be possibly violated - assume the worst
            return RedistributeContext(psUndefined);
        }
        else {
            return RedistributeContext(psLocalInstance,
                                     std::shared_ptr<CoordinateTranslator>(),
                                     sourceInstanceID);
        }
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        string shadowArrayName;
        const size_t shadowArrayParamIndx = 5;
        if (_parameters.size() >= (shadowArrayParamIndx+1) &&
            _parameters[shadowArrayParamIndx]->getParamType() == PARAM_ARRAY_REF) {
            shadowArrayName =
                ((std::shared_ptr<OperatorParamArrayReference>&)_parameters[shadowArrayParamIndx])->getObjectName();
        } else {
            // no shadow array
            return;
        }

        //Let's store shadow arrays in defaultPartitioning()
        //TODO: revisit this when we allow users to store arrays with specified distributions
        ArrayDesc shadowArrayDesc = InputArray::generateShadowArraySchema(_schema, shadowArrayName);
        shadowArrayDesc.setPartitioningSchema(defaultPartitioning());
        SCIDB_ASSERT(shadowArrayName == shadowArrayDesc.getName());

        LOG4CXX_DEBUG(oplogger, "Preparing catalog for shadow array " << shadowArrayName);

        _shadowArrayUpdateOp = make_shared<PhysicalUpdate>("shadow_update_logical",
                                                           "shadow_update_physical",
                                                           Parameters(),
                                                           shadowArrayDesc,
                                                           shadowArrayName);
        _shadowArrayUpdateOp->preSingleExecute(query);

        SCIDB_ASSERT(shadowArrayName ==
                     ArrayDesc::makeUnversionedName(_shadowArrayUpdateOp->getSchema().getName()));

        shadowArrayDesc = _shadowArrayUpdateOp->getSchema();

        if (shadowArrayDesc.isTransient()) {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_CAN_NOT_STORE)
                << shadowArrayDesc;
        }

        LOG4CXX_DEBUG(oplogger, "Shadow array schema: " << shadowArrayDesc);

        // replace shadow array name with its schema, so that it is sent to the workers
        _parameters[shadowArrayParamIndx] = make_shared<OperatorParamSchema>(std::shared_ptr<ParsingContext>(),
                                                                             shadowArrayDesc);
    }

    void postSingleExecute(std::shared_ptr<Query> query)
    {
        if (_shadowArrayUpdateOp) {
            if (isDebug()) {
                SCIDB_ASSERT (_parameters.size() >= 6 &&
                    _parameters[5]->getParamType() == PARAM_SCHEMA);
            }
            _shadowArrayUpdateOp->postSingleExecute(query);
        }
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                     std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        assert(_parameters.size() >= 2);

        assert(_parameters[1]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        std::shared_ptr<OperatorParamPhysicalExpression> paramExpr =
            (std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1];
        assert(paramExpr->isConstant());
        const string fileName = paramExpr->getExpression()->evaluate().getString();

        InstanceID sourceInstanceID = getSourceInstanceID();

        if (sourceInstanceID != COORDINATOR_INSTANCE_MASK &&
            sourceInstanceID != ALL_INSTANCE_MASK &&
            (size_t)sourceInstanceID >= query->getInstancesCount())
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_INVALID_INSTANCE_ID) << sourceInstanceID;

        if (sourceInstanceID == COORDINATOR_INSTANCE_MASK) {
            sourceInstanceID = (query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID());
        }

        int64_t maxErrors = 0;
        const ArrayDesc* shadowArraySchema(NULL);

        InstanceID myInstanceID = query->getInstanceID();
        bool enforceDataIntegrity = true;
        string format;
        if (_parameters.size() >= 4)
        {
            assert(_parameters[3]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            paramExpr = (std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[3];
            assert(paramExpr->isConstant());
            format = paramExpr->getExpression()->evaluate().getString();
            if (_parameters.size() >= 5)
            {
                assert(_parameters[4]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
                paramExpr = (std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[4];
                assert(paramExpr->isConstant());
                maxErrors = paramExpr->getExpression()->evaluate().getInt64();
                if (_parameters.size() >= 6)
                {
                    if (_parameters[5]->getParamType() == PARAM_SCHEMA) {
                        shadowArraySchema = &((std::shared_ptr<OperatorParamSchema>&)_parameters[5])->getSchema();
                        assert(shadowArraySchema);
                        assert(!shadowArraySchema->getName().empty());
                        assert(shadowArraySchema->getId() > 0);
                        assert(shadowArraySchema->getUAId() > 0);
                        assert(shadowArraySchema->getId() > shadowArraySchema->getUAId());
                    } else {
                        ASSERT_EXCEPTION((_parameters[5]->getParamType() == PARAM_PHYSICAL_EXPRESSION),
                             "Invalid input() parameters 5");
                    }

                    enforceDataIntegrity = PhysicalQueryPlanNode::getInputIsStrict(_parameters);

                    SCIDB_ASSERT(_parameters.size() <= 7);
                }
            }
        }

        std::shared_ptr<Array> result;
        bool emptyArray = (sourceInstanceID != ALL_INSTANCE_MASK &&
                           sourceInstanceID != myInstanceID);
        InputArray* ary = new InputArray(_schema, format, query,
                                         emptyArray,
                                         enforceDataIntegrity,
                                         maxErrors,
                                         (shadowArraySchema != NULL ? *shadowArraySchema : ArrayDesc()),
                                         sourceInstanceID == ALL_INSTANCE_MASK);
        result.reset(ary);

        if (emptyArray) {
            // No need to actually open the file.  (In fact, if the file is a pipe and
            // double-buffering is enabled, opening it would wrongly steal data intended for
            // some other instance!  See ticket #4466.)
            SCIDB_ASSERT(ary->inEmptyMode());
        } else {
            try
            {
                ary->openFile(fileName);
            }
            catch(const Exception& e)
            {
                if (e.getLongErrorCode() != SCIDB_LE_CANT_OPEN_FILE)
                {
                    // Only expecting an open failure, but whatever---pass it up.
                    throw;
                }

                if (sourceInstanceID == myInstanceID)
                {
                    // If mine is the one-and-only load instance, let
                    // callers see the open failure.
                    throw;
                }

                // No *local* file to load... but we must return the
                // InputArray result, since even in its failed state it
                // knows how to cooperate with subsequent SG pulls of the
                // shadow array.  An empty MemArray won't do.
                //
                // The open failure itself has already been logged.

                assert(ary->inEmptyMode()); // ... regardless of emptyArray value above.
            }
        }

        return result;
    }

    private:
    std::shared_ptr<PhysicalUpdate> _shadowArrayUpdateOp;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalInput, "input", "impl_input")

} //namespace
