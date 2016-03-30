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

    bool outputFullChunks(std::vector< ArrayDesc> const&) const
    {
        return true;
    }

    RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                              const std::vector< ArrayDesc> & inputSchemas) const
    {
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                           const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        std::shared_ptr<Array> srcArray = inputArrays[0];

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

        ArrayDistPtr  arrDist = _schema.getDistribution();
        ArrayResPtr arrRes; //default query residency
        if (sgMode == "randomRes") {
            arrRes = _schema.getResidency();
            return redistributeToRandomAccess(srcArray,
                                              arrDist,
                                              arrRes,
                                              query,
                                              enforceDataIntegrity);
        } else if (sgMode == "parallel") {
            return redistributeToRandomAccess(srcArray,
                                              arrDist,
                                              arrRes,
                                              query,
                                              enforceDataIntegrity);
        } else if (sgMode == "serial") {
            return redistributeSerially(srcArray,
                                        arrDist,
                                        arrRes,
                                        query,
                                        enforceDataIntegrity);
        }

        ASSERT_EXCEPTION_FALSE("Bad SG mode");

        return std::shared_ptr<Array>();
    }

    std::shared_ptr<Array>
    redistributeSerially(std::shared_ptr<Array>& inputArray,
                         const ArrayDistPtr& arrDist,
                         const ArrayResPtr& arrRes,
                         const std::shared_ptr<Query>& query,
                         bool enforceDataIntegrity)
    {
        inputArray = ensureRandomAccess(inputArray, query);

        const ArrayDesc& inputArrayDesc = inputArray->getArrayDesc();

        ArrayDesc outputArrayDesc(inputArray->getArrayDesc());
        outputArrayDesc.setDistribution(arrDist);
        if (!arrRes) {
            outputArrayDesc.setResidency(query->getDefaultArrayResidency());
        } else {
            outputArrayDesc.setResidency(arrRes);
        }

        std::shared_ptr<MemArray> outputArray = std::make_shared<MemArray>(outputArrayDesc, query);

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
                               outputArrayDesc.getDistribution(),
                               outputArrayDesc.getResidency(),
                               query,
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
                               outputArrayDesc.getDistribution(),
                               outputArrayDesc.getResidency(),
                               query,
                               enforceDataIntegrity,
                               attributeOrdering,
                               outputArray);

        return std::shared_ptr<Array>(outputArray);
    }

    void redistributeAttributes(std::shared_ptr<Array>& inputArray,
                                const ArrayDistPtr& arrDist,
                                const ArrayResPtr& arrRes,
                                const std::shared_ptr<Query>& query,
                                bool enforceDataIntegrity,
                                std::set<AttributeID>& attributeOrdering,
                                const std::shared_ptr<MemArray>& outputArray)
    {
        std::shared_ptr<Array> tmpRedistedInput =
        pullRedistributeInAttributeOrder(inputArray,
                                         attributeOrdering,
                                         arrDist,
                                         arrRes,
                                         query);

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
