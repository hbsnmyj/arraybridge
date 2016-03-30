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
 * PhysicalProject.cpp
 *
 *  Created on: Apr 20, 2010
 *      Author: Knizhnik
 */
#include <log4cxx/logger.h>
#include <query/Operator.h>
#include <array/Metadata.h>
#include <array/DelegateArray.h>
#include <system/Cluster.h>

namespace scidb
{

using namespace boost;
using namespace std;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.reduce_distro"));

class ReduceDistroArrayIterator : public DelegateArrayIterator
{
public:
   ReduceDistroArrayIterator(const std::shared_ptr<Query>& query,
                             DelegateArray const& delegate,
                             AttributeID attrID,
                             std::shared_ptr<ConstArrayIterator> inputIterator,
                             PartitioningSchema ps):
   DelegateArrayIterator(delegate, attrID, inputIterator),
   _ps(ps), _myInstance(query->getInstanceID()), _nextChunk(0), _query(query)
    {
        findNext();
    }


    virtual ~ReduceDistroArrayIterator()
    {}

    virtual void findNext()
    {
        while (!inputIterator->end())
        {
            Coordinates const& pos = inputIterator->getPosition();
            std::shared_ptr<Query> query = Query::getValidQueryPtr(_query);
            if (getInstanceForChunk(pos,
                                    array.getArrayDesc().getDimensions(),
                                    array.getArrayDesc().getDistribution(),
                                    array.getArrayDesc().getResidency(),
                                    query) == _myInstance)
            {
                _nextChunk = &inputIterator->getChunk();
                _hasNext = true;
                return;
            }

            ++(*inputIterator);
        }
        _hasNext = false;
    }

    virtual void reset()
    {
        chunkInitialized = false;
        inputIterator->reset();
        findNext();
    }

    bool end()
    {
        return _hasNext == false;
    }

    void operator ++()
    {
        chunkInitialized = false;
        ++(*inputIterator);
        findNext();
    }

    bool setPosition(Coordinates const& pos)
    {
        chunkInitialized = false;
        std::shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        if (getInstanceForChunk(pos,
                                array.getArrayDesc().getDimensions(),
                                array.getArrayDesc().getDistribution(),
                                array.getArrayDesc().getResidency(),
                                query) == _myInstance &&
            inputIterator->setPosition(pos))
        {
            _nextChunk = &inputIterator->getChunk();
            return _hasNext = true;
        }

        return _hasNext = false;
    }

    ConstChunk const& getChunk()
    {
        if (!chunkInitialized)  {
            chunk->setInputChunk(*_nextChunk);
            chunkInitialized = true;
        }
        return *chunk;
    }


private:
    bool _hasNext;
    PartitioningSchema _ps;
    InstanceID _myInstance;
    ConstChunk const* _nextChunk;
    std::weak_ptr<Query> _query;
};



class ReduceDistroArray: public DelegateArray
{
public:
    ReduceDistroArray(const std::shared_ptr<Query>& query,
                      ArrayDesc const& desc,
                      std::shared_ptr<Array> const& array,
                      PartitioningSchema ps):
    DelegateArray(desc, array, true), _ps(ps)
    {
        assert(query);
        _query = query;
    }

    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const
    {
        return new ReduceDistroArrayIterator(Query::getValidQueryPtr(_query),
                                             *this, id, inputArray->getConstIterator(id), _ps);
    }

private:
   PartitioningSchema _ps;
};

class PhysicalReduceDistro: public  PhysicalOperator
{
public:
    PhysicalReduceDistro(const string& logicalName,
                         const string& physicalName,
                         const Parameters& parameters,
                         const ArrayDesc& schema)
        :  PhysicalOperator(logicalName, physicalName, parameters, schema)
	{
	}

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector< ArrayDesc> & inputSchemas) const
    {
        return inputBoundaries[0];
    }

    virtual bool changesDistribution(std::vector< ArrayDesc> const&) const
    {
        return true;
    }

    virtual RedistributeContext
    getOutputDistribution(std::vector<RedistributeContext> const& inputDistributions,
                          std::vector<ArrayDesc> const& inputSchemas) const
    {
        PartitioningSchema ps = (PartitioningSchema)((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt32();
        assertConsistency(inputSchemas[0], inputDistributions[0]);

        LOG4CXX_TRACE(logger, "reduce_distro::getOutputDist: ps=" << ps);
        LOG4CXX_TRACE(logger, "reduce_distro::getOutputDist: schema ps="
                      << _schema.getDistribution()->getPartitioningSchema());

        SCIDB_ASSERT(_schema.getDistribution()->getPartitioningSchema() == ps);
        ASSERT_EXCEPTION(_schema.getResidency()->isEqual(inputDistributions[0].getArrayResidency()),
                         "reduce_distro must not change residency");

        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    /**
     * Project is a pipelined operator, hence it executes by returning an iterator-based array to the consumer
     * that overrides the chunkiterator method.
     */
    virtual std::shared_ptr<Array>
    execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 1);
        checkOrUpdateIntervals(_schema, inputArrays[0]);

        PartitioningSchema ps = (PartitioningSchema)((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])->getExpression()->evaluate().getInt32();
        return std::shared_ptr<Array>(new ReduceDistroArray(query, _schema, inputArrays[0], ps));
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalReduceDistro, "_reduce_distro", "physicalReduceDistro")

}  // namespace scidb
