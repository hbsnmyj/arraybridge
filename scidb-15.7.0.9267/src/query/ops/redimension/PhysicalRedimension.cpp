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
    PhysicalRedimension(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
    RedimensionCommon(logicalName, physicalName, parameters, schema)
    {}

    /**
     * Check if we are dealing with aggregates or a synthetic dimension.
     * @return true if this redimension has at least one aggregate or if it uses a synthetic dimension; false otherwise
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
    virtual RedistributeContext getOutputDistribution(vector<RedistributeContext>const&  sourceDistribution,
                                                      vector<ArrayDesc>const& inputSchemas) const
    {
        if (haveAggregatesOrSynthetic(inputSchemas[0])) {
            return RedistributeContext(defaultPartitioning());
        }
        return RedistributeContext(psUndefined);
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
        return redimensionArray(srcArray,
                                attrMapping,
                                dimMapping,
                                aggregates,
                                query,
                                timing,
                                redistributeMode);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalRedimension, "redimension", "PhysicalRedimension")
}  // namespace ops
