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
 * @file PhysicalOrrderedTranspose.cpp
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 * @author poliocough@gmail.com
 */

#include "TransposeArray.h"

#include <query/Operator.h>
#include <query/AutochunkFixer.h>

using namespace std;
using namespace boost;

namespace scidb {

class PhysicalTranspose: public PhysicalOperator
{
  public:
    /**
     * Create the operator.
     * @param logicalName the user-visible name
     * @param physicalName the internal physical name
     * @param parameters the operator arguments; none expected
     * @param schema the shape of the output array
     */
    PhysicalTranspose(std::string const& logicalName,
                      std::string const& physicalName,
                      Parameters const& parameters,
                      ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    /**
     * @see PhysicalOperator::changesDistribution
     */
    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    /**
     * @see PhysicalOperator::getOutputDistribution
     */
    virtual RedistributeContext getOutputDistribution(
            std::vector<RedistributeContext> const& inputDistributions,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        assertConsistency(inputSchemas[0], inputDistributions[0]);

        const RedistributeContext& inputDistro = inputDistributions[0];

        if (inputDistro.getPartitioningSchema() == psByRow) {
            ASSERT_EXCEPTION(_schema.getDistribution()->getPartitioningSchema() == psByCol,
                             "Invalid array distribution in LogicalTranspose, psByCol expected");
        }
        else if (inputDistro.getPartitioningSchema() == psByCol) {
            ASSERT_EXCEPTION(_schema.getDistribution()->getPartitioningSchema() == psByRow,
                             "Invalid array distribution in LogicalTranspose, psByRow expected");
        } else {
            //TODO:OPTAPI mapper
            ASSERT_EXCEPTION(_schema.getDistribution()->getPartitioningSchema() == psUndefined,
                             "Invalid array distribution in LogicalTranspose, psUndefined expected");
        }

        ArrayDesc* mySchema = const_cast<ArrayDesc*>(&_schema);
        mySchema->setResidency(inputDistro.getArrayResidency());

        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    /**
     * @see PhysicalOperator::getOutputBoundaries
     */
    virtual PhysicalBoundaries getOutputBoundaries(std::vector<PhysicalBoundaries> const& inputBoundaries,
                                                   std::vector< ArrayDesc> const& inputSchemas) const
    {
        if (inputBoundaries[0].isEmpty()) {
            return PhysicalBoundaries::createEmpty(_schema.getDimensions().size());
        }

        Coordinates start = inputBoundaries[0].getStartCoords();
        size_t const nDims = start.size();
        Coordinates newStart(nDims);

        for (size_t i = 0; i < nDims; i++) {
            newStart[nDims-i-1] = start[i];
        }

        Coordinates end = inputBoundaries[0].getEndCoords();
        Coordinates newEnd(nDims);

        for (size_t i = 0; i < nDims; i++) {
            newEnd[nDims-i-1] = end[i];
        }

        return PhysicalBoundaries(newStart, newEnd, inputBoundaries[0].getDensity());
    }

    /// Get the stringified AutochunkFixer so we can fix up the intervals in execute().
    /// @see LogicalTranspose::getInspectable()
    void inspectLogicalOp(LogicalOperator const& lop) override
    {
        setControlCookie(lop.getInspectable());
    }

    /**
     * @see PhysicalOperator::execute
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);

        AutochunkFixer af(getControlCookie());
        af.fix(_schema, inputArrays);

        std::shared_ptr<Array> inputArray = ensureRandomAccess(inputArrays[0], query);
        std::shared_ptr<CoordinateSet> inputChunkPositions = inputArray->findChunkPositions();

        SCIDB_ASSERT(_schema.getResidency()->isEqual(inputArray->getArrayDesc().getResidency()));
        return make_shared<TransposeArray>(_schema, inputArray, inputChunkPositions, query);
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalTranspose, "transpose", "physicalTranspose")

}  // namespace scidb
