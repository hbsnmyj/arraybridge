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
#include "array/Metadata.h"
#include "JoinArray.h"

using namespace std;
using namespace boost;

namespace scidb {

class JoinArrayIterator : public DelegateArrayIterator
{
  public:
        virtual bool end()
    {
        return !hasCurrent;
    }

    virtual void operator ++()
    {
        assert(hasCurrent);
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_POSITION);
        ++(*inputIterator);
        while (!inputIterator->end()) {
            if (joinIterator->setPosition(inputIterator->getPosition())) {
                return;
            }
            ++(*inputIterator);
        }
        hasCurrent = false;
    }

    virtual bool setPosition(Coordinates const& pos)
    {
        return hasCurrent = inputIterator->setPosition(pos) && joinIterator->setPosition(pos);
    }

    virtual void reset()
    {
        inputIterator->reset();
        while (!inputIterator->end()) {
            if (joinIterator->setPosition(inputIterator->getPosition())) {
                hasCurrent = true;
                return;
            }
            ++(*inputIterator);
        }
        hasCurrent = false;
    }

    JoinArrayIterator(DelegateArray const& array, AttributeID attrID, std::shared_ptr<ConstArrayIterator> inputIterator,
                      std::shared_ptr<ConstArrayIterator> pairIterator)
    : DelegateArrayIterator(array, attrID, inputIterator),
      joinIterator(pairIterator)
    {
        reset();
    }

  private:
    std::shared_ptr<ConstArrayIterator> joinIterator;
    bool hasCurrent;
};

class JoinArray : public DelegateArray
{
public:
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const
    {
        return new JoinArrayIterator(*this, id,
                                     id < nLeftAttributes
                                         ? leftArray->getConstIterator(id)
                                         : rightArray->getConstIterator(id - nLeftAttributes),
                                     id < nLeftAttributes
                                         ? rightArray->getConstIterator(0)
                                         : leftArray->getConstIterator(0));
    }

    JoinArray(ArrayDesc const& desc, std::shared_ptr<Array> left, std::shared_ptr<Array> right)
    : DelegateArray(desc, left),
      leftArray(left),
      rightArray(right),
      nLeftAttributes(left->getArrayDesc().getAttributes().size())
    {
    }

  private:
    std::shared_ptr<Array> leftArray;
    std::shared_ptr<Array> rightArray;
    size_t nLeftAttributes;
};

class PhysicalJoin: public PhysicalOperator
{
public:
    PhysicalJoin(const string& logicalName,
                 const string& physicalName,
                 const Parameters& parameters,
                 const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual DistributionRequirement getDistributionRequirement (const std::vector< ArrayDesc> & inputSchemas) const
    {
        return DistributionRequirement(DistributionRequirement::Collocated);
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        if (inputBoundaries[0].isEmpty() || inputBoundaries[1].isEmpty())
        {
            return PhysicalBoundaries::createEmpty(_schema.getDimensions().size());
        }
        return inputBoundaries[0].intersectWith(inputBoundaries[1]);
    }

    /**
     * Ensure input array chunk sizes and overlaps match.
     */
    virtual void requiresRedimensionOrRepartition(
        vector<ArrayDesc> const& inputSchemas,
        vector<ArrayDesc const*>& modifiedPtrs) const
    {
        repartByLeftmost(inputSchemas, modifiedPtrs);
    }

    /***
     * Join is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 2);
        std::shared_ptr<Array> left = inputArrays[0];
        std::shared_ptr<Array> right = inputArrays[1];
        left = ensureRandomAccess(left, query);
        right = ensureRandomAccess(right, query);

        return std::shared_ptr<Array>(_schema.getEmptyBitmapAttribute() == NULL
                                        ? (Array*)new JoinArray(_schema, left, right)
                                        : (Array*)new JoinEmptyableArray(_schema, left, right));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalJoin, "join", "physicalJoin")

}  // namespace scidb
