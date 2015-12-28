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
 * PhysicalApply.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */

#include "query/Operator.h"
#include "MergeArray.h"
#include <log4cxx/logger.h>

using namespace std;
using namespace boost;

namespace scidb {
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

class PhysicalMerge: public PhysicalOperator
{
public:
    PhysicalMerge(const string& logicalName, const string& physicalName,
                  const Parameters& parameters, const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    { }

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::Collocated);
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0].unionWith(inputBoundaries[1]);
    }

    /**
     * Ensure startMin, endMax, chunkOverlap, and chunkInterval match
     * for each of the input arrays.  Note:  the only one this routine
     * is allowed to change is endMax.
     */
    virtual void requiresRedimensionOrRepartition(
        vector<ArrayDesc> const& inputSchemas,
        vector<ArrayDesc const*>& modifiedPtrs) const
    {
        const size_t N = inputSchemas.size();
        assert(N > 1);
        assert(N == modifiedPtrs.size());


        ArrayDesc const&     mergedSchema      = _schema;
        std::string const&   mergedName        = mergedSchema.getName();
        Dimensions const&    mergedDimensions  = mergedSchema.getDimensions();
        Attributes const &   mergedAttributes  = mergedSchema.getAttributes();

        _redimRepartSchemas.clear();
        for (size_t nSchema = 0; nSchema < inputSchemas.size(); nSchema++)
        {
            ArrayDesc const &currentSchema  = inputSchemas[nSchema];
            if (modifiedPtrs[nSchema])
            {
                // If an explicit redimension or repartition is
                // present, we are forbidden to change it
                // --- yet it *must* match!!! ---
                ArrayDesc const &modifiedSchema = *(modifiedPtrs[nSchema]);
                if(!mergedSchema.sameSchema(modifiedSchema))
                {
                    Dimensions const&  modifiedDimensions =
                        modifiedSchema.getDimensions();

                    throw USER_EXCEPTION(
                            SCIDB_SE_OPERATOR,
                            SCIDB_LE_BAD_EXPLICIT_REPART)
                        << getLogicalName()
                        << mergedDimensions
                        << modifiedDimensions;
                }

                // Indicate no modification is necessary.
                modifiedPtrs[nSchema] = 0;
            }
            else
            {
                if(mergedSchema.sameSchema(currentSchema))
                {
                    // Indicate no modification is necessary.
                    modifiedPtrs[nSchema] = 0;
                }
                else
                {
                    ArrayDesc newSchema  = currentSchema;

                    // Replace the current schema's dimension values with
                    // those from the merged schema.
                    newSchema.replaceDimensionValues(mergedSchema);

                    // Create a modifification indicator.
                    _redimRepartSchemas.push_back(make_shared<ArrayDesc>(
                        newSchema.getName(),
                        newSchema.getAttributes(),
                        newSchema.getDimensions(),
                        defaultPartitioning()));

                    // Indicate modification is necessary.
                    modifiedPtrs[nSchema] = _redimRepartSchemas.back().get();
                }
            }  // if (modifiedPtrs[nSchema]) { ... } else { ... }
        }  // for (size_t nSchema = 0; nSchema < ...) { ... }

        if (_redimRepartSchemas.empty())
        {
            // Assertions elsewhere hate an all-NULLs vector here.
            modifiedPtrs.clear();
        }
    }

    /***
     * Merge is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() >= 2);
        return std::shared_ptr<Array>(new MergeArray(_schema, inputArrays));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalMerge, "merge", "physicalMerge")

}  // namespace scidb
