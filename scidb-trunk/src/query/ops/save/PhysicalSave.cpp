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
 * @file PhysicalSave.cpp
 *
 * @author roman.simakov@gmail.com
 *
 * Physical implementation of SAVE operator for saving data from text file
 * which is located on coordinator
 */


#include <array/Array.h>
#include <array/DBArray.h>
#include <log4cxx/logger.h>
#include <query/Operator.h>
#include <query/QueryProcessor.h>
#include <smgr/io/ArrayWriter.h>
#include <string.h>
#include <system/Config.h>
#include <system/SciDBConfigOptions.h>

using namespace std;
using namespace boost;
using namespace scidb;

// Useful local shorthand.
#define Parm(_n) \
    ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[(_n)])
#define ParmExpr(_n)    (Parm(_n)->getExpression())

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.physical_save"));

class PhysicalSave: public PhysicalOperator
{
public:
    PhysicalSave(const std::string& logicalName,
                 const std::string& physicalName,
                 const Parameters& parameters,
                 const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual PhysicalBoundaries getOutputBoundaries(
        const std::vector<PhysicalBoundaries> & inputBoundaries,
        const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    int64_t getSourceInstanceID() const
    {
        if (_parameters.size() >= 2)
        {
            assert(_parameters[1]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
            std::shared_ptr<OperatorParamPhysicalExpression> parm1 = Parm(1);
            assert(parm1->isConstant());
            return parm1->getExpression()->evaluate().getInt64();
        }
        // return ALL_INSTANCES_MASK; -- old behaviour
        return COORDINATOR_INSTANCE_MASK; // new behaviour compatible with LOAD/INPUT
    }

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::Any);
    }


    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays,
                                   std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        assert(_parameters.size() >= 1);
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        assert(_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION);
        const string fileName = ParmExpr(0)->evaluate().getString();
        string format = "store";
        if (_parameters.size() >= 3) {
            format = ParmExpr(2)->evaluate().getString();
        }
        bool parallel = false;
        const bool isOpaque = (format == "opaque");
        InstanceID sourceInstanceID = getSourceInstanceID();
        if (sourceInstanceID == COORDINATOR_INSTANCE_MASK) {
            sourceInstanceID = (query->isCoordinator() ? query->getInstanceID() : query->getCoordinatorID());
        } else if (sourceInstanceID == ALL_INSTANCE_MASK) {
            parallel = true;
        } else {
            // convert from physical to logical
            sourceInstanceID = query->mapPhysicalToLogical(sourceInstanceID);
        }
        const ArrayDesc& inputArrayDesc = inputArrays[0]->getArrayDesc();
        std::shared_ptr<Array> tmpRedistedInput;

        if (!parallel) {

            const Attributes& attribs = inputArrayDesc.getAttributes();
            std::set<AttributeID> attributeOrdering;
            for  ( Attributes::const_iterator a = attribs.begin(); a != attribs.end(); ++a ) {
                if (!a->isEmptyIndicator() || isOpaque) {
                    SCIDB_ASSERT(inputArrayDesc.getEmptyBitmapAttribute()==NULL ||
                                 isOpaque ||
                                 // if emptyable, make sure the attribute is not the last
                                 // the last must be the empty bitmap
                                 (a->getId()+1) != attribs.size());
                    attributeOrdering.insert(a->getId());
                }
            }

            stringstream ss;
            ss << sourceInstanceID;
            ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(psLocalInstance,
                                                                                        DEFAULT_REDUNDANCY,
                                                                                        ss.str());
            tmpRedistedInput = pullRedistributeInAttributeOrder(inputArrays[0],
                                                                attributeOrdering,
                                                                localDist,
                                                                ArrayResPtr(), //default query residency
                                                                query);
        } else {
            tmpRedistedInput = inputArrays[0];
        }

        // only when redistribute was actually done (sometimes optimized away)
        const bool wasConverted = (tmpRedistedInput != inputArrays[0]) ;

        const InstanceID myInstanceID = query->getInstanceID();

        if (parallel || sourceInstanceID == myInstanceID) {
            ArrayWriter::setPrecision(Config::getInstance()->getOption<int>(CONFIG_PRECISION));
            ArrayWriter::save(*tmpRedistedInput, fileName, query, format,
                              (parallel ? ArrayWriter::F_PARALLEL : 0));
        } // else dont need to pull

        if (wasConverted) {
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
            syncArray->sync();
        }

        return make_shared<MemArray>(inputArrayDesc, query); //empty array
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalSave, "save", "impl_save")

} //namespace
