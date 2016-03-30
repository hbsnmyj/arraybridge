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
 * PhysicalUnfold.cpp
 *
 *  Created on: 13 May 2014
 *      Author: Dave Gosselin
 */

#include <query/Operator.h>
#include <query/AutochunkFixer.h>
#include "UnfoldArray.h"

using namespace std;

namespace scidb
{

  class PhysicalUnfold : public PhysicalOperator
  {
  public:
    PhysicalUnfold(string const& logicalName,
		   string const& physicalName,
		   Parameters const& parameters,
		   ArrayDesc const& schema)
      : PhysicalOperator(logicalName, physicalName, parameters, schema) {}

      virtual RedistributeContext
      getOutputDistribution(vector<RedistributeContext> const& inputDistributions,
                            vector<ArrayDesc> const& inputSchemas) const
      {
          // Distribution is undefined.
          assertConsistency(inputSchemas[0], inputDistributions[0]);

          ArrayDesc* mySchema = const_cast<ArrayDesc*>(&_schema);
          SCIDB_ASSERT(_schema.getDistribution()->getPartitioningSchema()==psUndefined);
          mySchema->setResidency(inputDistributions[0].getArrayResidency());

          return RedistributeContext(_schema.getDistribution(),
                                     _schema.getResidency());
      }

    virtual bool
    changesDistribution(std::vector<ArrayDesc> const& sourceSchemas) const {
      // This could change the distribution.
      return true;
    }

    /// Get the stringified AutochunkFixer so we can fix up the intervals in execute().
    /// @see LogicalUnfold::getInspectable()
    void inspectLogicalOp(LogicalOperator const& lop) override
    {
        setControlCookie(lop.getInspectable());
    }

    /**
     * Unfold transforms the input array into a 2-d matrix whose columns
     * correspond to the input array attributes. The output matrix row dimension
     * will have a chunk size equal to the input array, and column chunk size
     * equal to the number of columns.
     */
    std::shared_ptr<Array>
    execute(vector<std::shared_ptr<Array> >& inputArrays,
	    std::shared_ptr<Query> query) {
      // This operator never takes more than one input array.
      assert(inputArrays.size() == 1);

      AutochunkFixer af(getControlCookie());
      af.fix(_schema, inputArrays);

      // Return an UnfoldArray which defers the work to the "pull" phase.
      return std::make_shared<UnfoldArray> (_schema, inputArrays[0],
					      query);
    }
  };

  // In this registration, the second argument must match the AFL operator
  // name and the name provided in the Logical##name file. The third
  // argument is arbitrary and used for debugging purposes.
  DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalUnfold,
				    "unfold",
				    "PhysicalUnfold")

}  // namespace scidb
