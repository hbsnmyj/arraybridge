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
 * PhysicalRedimension.cpp
 *
 *  Created on: Apr 16, 2010
 *  @author Knizhnik
 *  @author poliocough@gmail.com
 */

#include <boost/foreach.hpp>
#include <log4cxx/logger.h>
#include <query/QueryPlan.h>
#include "RedimensionCommon.h"

namespace scidb
{

using namespace std;
using namespace boost;

/**
 * Redimension operator
 */
class PhysicalRedimension: public RedimensionCommon
{
public:
    /**
     * Vanilla.
     * @param logicalName the name of operator "redimension"
     * @param physicalName the name of the physical counterpart
     * @param parameters the operator parameters - the output schema and optional aggregates
     * @param schema the result of LogicalRedimension::inferSchema
     */
    PhysicalRedimension(const string& logicalName,
                        const string& physicalName,
                        const Parameters& parameters,
                        const ArrayDesc& schema)
        : RedimensionCommon(logicalName, physicalName, parameters, schema)
    {}

    /**
     * Check if we are dealing with aggregates or a synthetic dimension.
     * @return true iff this redimension either uses a synthetic dimension or has at least one aggregate
     */
    bool haveAggregatesOrSynthetic(ArrayDesc const& srcDesc) const
    {
        if (_parameters.size() > 2)
        {
            return true; //aggregate
        }

        if (_parameters.size() == 2 &&
            _parameters[1]->getParamType() == PARAM_AGGREGATE_CALL) {
            return true;
        }

        ArrayDesc dstDesc = ((std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
        BOOST_FOREACH(const DimensionDesc &dstDim, dstDesc.getDimensions())
        {
            BOOST_FOREACH(const AttributeDesc &srcAttr, srcDesc.getAttributes())
            {
                if (dstDim.hasNameAndAlias(srcAttr.getName()))
                {
                    goto NextDim;
                }
            }
            BOOST_FOREACH(const DimensionDesc &srcDim, srcDesc.getDimensions())
            {
                if (srcDim.hasNameAndAlias(dstDim.getBaseName()))
                {
                    goto NextDim;
                }
            }
            return true; //synthetic
            NextDim:;
        }
        return false;
    }

    /**
     * @return false if the isStrict parameter is specified and is equal to false; true otherwise
     */
    bool isStrict() const
    {
        return PhysicalQueryPlanNode::getRedimensionIsStrict(_parameters);
    }

    /**
     * @see PhysicalOperator::getOutputDistribution
     */
    virtual RedistributeContext getOutputDistribution(vector<RedistributeContext>const& inputDistributions,
                                                      vector<ArrayDesc>const& inputSchemas) const
    {
        assertConsistency(inputSchemas[0], inputDistributions[0]);

        ArrayDesc* mySchema = const_cast<ArrayDesc*>(&_schema);

        if (haveAggregatesOrSynthetic(inputSchemas[0])) {
            // With aggreagations or synthetic dimension,
            // redistribution happens within the execute() method.
            // The distribution & residency become the defaults.
            mySchema->setDistribution(defaultPartitioning());
            SCIDB_ASSERT(_schema.getResidency()->isEqual(Query::getValidQueryPtr(_query)->getDefaultArrayResidency()));
        } else {
            // the optimizer needs to deal with the fallout,
            // i.e. redistributing (from psUndefined) and merging partial chunks
            SCIDB_ASSERT(_schema.getDistribution()->getPartitioningSchema()==psUndefined);
            mySchema->setResidency(inputDistributions[0].getArrayResidency());
        }
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    /**
     * @see PhysicalOperator::outputFullChunks
     */
    virtual bool outputFullChunks(std::vector<ArrayDesc>const& inputSchemas) const
    {
        return haveAggregatesOrSynthetic(inputSchemas[0]);
    }

    /**
     * @see PhysicalOperator::execute
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        std::shared_ptr<Array>& srcArray = inputArrays[0];
        ArrayDesc const& srcArrayDesc = srcArray->getArrayDesc();

        Attributes const& destAttrs = _schema.getAttributes(true); // true = exclude empty tag.
        Dimensions const& destDims  = _schema.getDimensions();

        vector<AggregatePtr> aggregates (destAttrs.size());
        vector<size_t>       attrMapping(destAttrs.size());
        vector<size_t>       dimMapping (destDims.size());

        setupMappings(srcArrayDesc, aggregates, attrMapping, dimMapping, destAttrs, destDims);
        ElapsedMilliSeconds timing;

        RedistributeMode redistributeMode(AUTO);
        if (haveAggregatesOrSynthetic(srcArrayDesc)) {
            redistributeMode = AGGREGATED;
        } else if ( isStrict()) {
            redistributeMode = VALIDATED;
        }
        std::shared_ptr<Array> resultArray =
           redimensionArray(srcArray,
                            attrMapping,
                            dimMapping,
                            aggregates,
                            query,
                            timing,
                            redistributeMode);
        SCIDB_ASSERT(!inputArrays[0]); // should be dropped by now
        return resultArray;
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRedimension, "redimension", "PhysicalRedimension")
}  // namespace ops
