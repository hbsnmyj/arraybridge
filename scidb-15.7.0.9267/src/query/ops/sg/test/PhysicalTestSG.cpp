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
 * @file PhysicalTestSG.cpp
 *
 * @author
 *
 */

#include <string.h>

#include <query/Operator.h>
#include <query/QueryPlan.h>
#include <array/Array.h>
#include <array/MemArray.h>
#include <query/PullSGArray.h>
#include <query/PullSGArrayUtil.h>

using namespace std;
using namespace boost;
using namespace scidb;

namespace scidb
{
class PhysicalTestSG: public PhysicalOperator
{
  public:
    PhysicalTestSG(const string& logicalName,
                   const string& physicalName,
                   const Parameters& parameters,
                   const ArrayDesc& schema):
    PhysicalOperator(logicalName,
                     physicalName,
                     parameters,
                     schema)
    { }

  private:

    string getSGMode(const Parameters& parameters)
    {
        if (parameters.size() < 3) {
            return string();
        }
        SCIDB_ASSERT(parameters[2]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        return static_cast<OperatorParamPhysicalExpression*>(parameters[2].get())->getExpression()->evaluate().getString();
    }

  public:

    virtual bool changesDistribution(std::vector<ArrayDesc> const& inputSchemas) const
    {
        return true;
    }

    PartitioningSchema getPartitioningSchema() const
    {
        assert(_parameters[0]);
        OperatorParamPhysicalExpression* pExp = static_cast<OperatorParamPhysicalExpression*>(_parameters[0].get());
        PartitioningSchema ps = static_cast<PartitioningSchema>(pExp->getExpression()->evaluate().getInt32());
        return ps;
    }

    InstanceID getInstanceId() const
    {
        InstanceID instanceId = ALL_INSTANCE_MASK;
        if (_parameters.size() >=2 )
        {
            OperatorParamPhysicalExpression* pExp = static_cast<OperatorParamPhysicalExpression*>(_parameters[1].get());
            instanceId = static_cast<InstanceID>(pExp->getExpression()->evaluate().getInt64());
        }
        return instanceId;
    }

    bool outputFullChunks(std::vector< ArrayDesc> const&) const
    {
        return true;
    }

    RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                              const std::vector< ArrayDesc> & inputSchemas) const
    {
        PartitioningSchema ps = getPartitioningSchema();

        DimensionVector offset = getOffsetVector(inputSchemas);

        std::shared_ptr<CoordinateTranslator> distMapper;

        if ( !offset.isEmpty() )
        {
            distMapper = CoordinateTranslator::createOffsetMapper(offset);
        }

        return RedistributeContext(ps,distMapper);
    }

    DimensionVector getOffsetVector(const vector<ArrayDesc> & inputSchemas) const
    {
        if (_parameters.size() <= 4)
        {
            return DimensionVector();
        }
        else
        {
            DimensionVector result(_schema.getDimensions().size());
            assert (_parameters.size() == _schema.getDimensions().size() + 4);
            for (size_t i = 0; i < result.numDimensions(); i++)
            {
                result[i] = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[i+4])->getExpression()->evaluate().getInt64();
            }
            return result;
        }
    }

    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                           const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        const PartitioningSchema ps = getPartitioningSchema();
        const InstanceID instanceID = getInstanceId();
        DimensionVector offsetVector = getOffsetVector(vector<ArrayDesc>());
        std::shared_ptr<Array> srcArray = inputArrays[0];
        std::shared_ptr <CoordinateTranslator> distMapper;
        if (!offsetVector.isEmpty())
        {
            distMapper = CoordinateTranslator::createOffsetMapper(offsetVector);
        }
        std::string sgMode = getSGMode(_parameters);
        if (sgMode.empty()) {
            sgMode = "parallel";
        }

        bool enforceDataIntegrity=false;
        if (_parameters.size() >= 4)
        {
            assert(_parameters[3]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            OperatorParamPhysicalExpression* paramExpr = static_cast<OperatorParamPhysicalExpression*>(_parameters[3].get());
            enforceDataIntegrity = paramExpr->getExpression()->evaluate().getBool();
            assert(paramExpr->isConstant());
        }

        if (sgMode == "parallel") {
            return redistributeToRandomAccess(srcArray, query, ps,
                                              instanceID, distMapper, 0,
                                              std::shared_ptr<PartitioningSchemaData>(),
                                              enforceDataIntegrity);
        } else if (sgMode == "serial") {
            return redistributeSeriallyInReverse(srcArray,
                                                 query, ps,
                                                 instanceID,
                                                 distMapper,
                                                 0,
                                                 std::shared_ptr<PartitioningSchemaData>(),
                                                 enforceDataIntegrity);
        }

        ASSERT_EXCEPTION_FALSE("Bad SG mode");

        return std::shared_ptr<Array>();
    }

    std::shared_ptr<Array>
    redistributeSeriallyInReverse(std::shared_ptr<Array>& inputArray,
                                  const std::shared_ptr<Query>& query,
                                  PartitioningSchema ps,
                                  InstanceID destInstanceId,
                                  const std::shared_ptr<CoordinateTranslator > & distMapper,
                                  size_t shift,
                                  const std::shared_ptr<PartitioningSchemaData>& psData,
                                  bool enforceDataIntegrity)
    {

        inputArray = ensureRandomAccess(inputArray, query);

        const ArrayDesc& inputArrayDesc = inputArray->getArrayDesc();
        std::shared_ptr<MemArray> outputArray = make_shared<MemArray>(inputArray->getArrayDesc(), query);

        // set up redistribute without the empty bitmap
        std::set<AttributeID> attributeOrdering;
        const Attributes& attributes = inputArrayDesc.getAttributes();

        for  (Attributes::const_reverse_iterator a = attributes.rbegin();
              a != attributes.rend(); ++a ) {

            if (!a->isEmptyIndicator()) {
              SCIDB_ASSERT(inputArrayDesc.getEmptyBitmapAttribute()==NULL ||
                             // if emptyable, make sure the attribute is not the last
                             // the last must be the empty bitmap
                           (a->getId()+1) != attributes.size());
                attributeOrdering.insert(a->getId());
            }
        }
        redistributeAttributes(inputArray,
                               query,
                               ps,
                               destInstanceId,
                               std::shared_ptr<CoordinateTranslator>(),
                               0,
                               std::shared_ptr<PartitioningSchemaData>(),
                               enforceDataIntegrity,
                               attributeOrdering,
                               outputArray);

        // now redistribute the empty bitmap only
        attributeOrdering.clear();
        for  (Attributes::const_reverse_iterator a = attributes.rbegin();
              a != attributes.rend(); ++a ) {

            if (a->isEmptyIndicator()) {
                SCIDB_ASSERT( // if emptyable, make sure the attribute is not the last
                              // the last must be the empty bitmap
                             (a->getId()+1) == attributes.size());
                attributeOrdering.insert(a->getId());
                break;
            }
        }
        redistributeAttributes(inputArray,
                               query,
                               ps,
                               destInstanceId,
                               std::shared_ptr<CoordinateTranslator>(),
                               0,
                               std::shared_ptr<PartitioningSchemaData>(),
                               enforceDataIntegrity,
                               attributeOrdering,
                               outputArray);


        return std::shared_ptr<Array>(outputArray);
    }

    void redistributeAttributes(std::shared_ptr<Array>& inputArray,
                                const std::shared_ptr<Query>& query,
                                PartitioningSchema ps,
                                InstanceID destInstanceId,
                                const std::shared_ptr<CoordinateTranslator > & distMapper,
                                size_t shift,
                                const std::shared_ptr<PartitioningSchemaData>& psData,
                                bool enforceDataIntegrity,
                                std::set<AttributeID>& attributeOrdering,
                                const std::shared_ptr<MemArray>& outputArray)
    {
        std::shared_ptr<Array> tmpRedistedInput = pullRedistributeInAttributeOrder(inputArray,
                                                            attributeOrdering,
                                                            query,
                                                            ps,
                                                            destInstanceId,
                                                            std::shared_ptr<CoordinateTranslator>(),
                                                            0,
                                                            std::shared_ptr<PartitioningSchemaData>());

        // only when redistribute was actually done (sometimes optimized away)
        const bool wasConverted = (tmpRedistedInput != inputArray) ;

        // drain into outputArray
        const ArrayDesc& inputArrayDesc = inputArray->getArrayDesc();
        const Attributes& attributes = inputArrayDesc.getAttributes();

        sg::WriteChunkToArrayFunc chunkWriter(outputArray, NULL, enforceDataIntegrity);
        std::vector<std::shared_ptr<ConstArrayIterator> > inputArrIters(attributes.size());
        const AttributeID attId = (*attributeOrdering.begin());
        while (!inputArrIters[attId] || !inputArrIters[attId]->end()) {

            for  (std::set<AttributeID>::const_iterator attr = attributeOrdering.begin();
                  attr != attributeOrdering.end(); ++attr ) {
                const AttributeID a = *attr;

                if (!inputArrIters[a]) {
                    inputArrIters[a] = tmpRedistedInput->getConstIterator(a);
                    if (inputArrIters[a]->end()) {
                        continue;
                    }
                }

                ConstArrayIterator* arrIter = inputArrIters[a].get();
                SCIDB_ASSERT(arrIter != NULL);
                const ConstChunk& chunk = arrIter->getChunk();

                chunkWriter(a, chunk, query);
            }

            // need to bump them all at once
            for  (std::set<AttributeID>::const_iterator attr = attributeOrdering.begin();
                  attr != attributeOrdering.end(); ++attr ) {
                const AttributeID a = *attr;
                if (inputArrIters[a]) {
                    ++(*inputArrIters[a]);
                }
            }
        }

        if (wasConverted) {
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
            syncArray->sync();
        }
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalTestSG, "test_sg", "PhysicalTestSG");
} //namespace
