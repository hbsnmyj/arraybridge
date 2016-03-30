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

#include "ChunkEstimator.h"

#include <array/Metadata.h>
#include <boost/array.hpp>
#include <system/SystemCatalog.h>
#include <query/Operator.h>
#include <log4cxx/logger.h>
#include <array/TransientCache.h>
#include <util/session/Session.h>
#include <usr_namespace/NamespacesCommunicator.h>

using namespace std;

/****************************************************************************/
namespace scidb {
/****************************************************************************/
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.physcial_create_array"));

struct PhysicalCreateArray : PhysicalOperator
{
    PhysicalCreateArray(const string& logicalName,
                        const string& physicalName,
                        const Parameters& parameters,
                        const ArrayDesc& schema)
     : PhysicalOperator(logicalName,physicalName,parameters,schema)
    {}

    virtual void
    fixDimensions(PointerRange<std::shared_ptr<Array> >, Dimensions&)
    {}

    virtual std::shared_ptr<Array>
    execute(vector<std::shared_ptr<Array> >& in,std::shared_ptr<Query> query)
    {
        bool const temp(
            param<OperatorParamPhysicalExpression>(2)->
                getExpression()->evaluate().getBool());

        if (query->isCoordinator())
        {
            string arrayNameOrg(param<OperatorParamArrayReference>(0)->getObjectName());

            std::string arrayName;
            std::string namespaceName;
            query->getNamespaceArrayNames(arrayNameOrg, namespaceName, arrayName);

            ArrayDesc arrSchema(param<OperatorParamSchema>(1)->getSchema());
            assert(ArrayDesc::isNameUnversioned(arrayName));

            arrSchema.setName(arrayName);
            arrSchema.setNamespaceName(namespaceName);
            arrSchema.setTransient(temp);

         /* Give our subclass a chance to compute missing dimension details
            such as a wild-carded chunk interval, for example...*/

            this->fixDimensions(in,arrSchema.getDimensions());
            const size_t redundancy = Config::getInstance()->getOption<size_t> (CONFIG_REDUNDANCY);
            arrSchema.setDistribution(defaultPartitioning(redundancy));
            arrSchema.setResidency(query->getDefaultArrayResidencyForWrite());
            ArrayID uAId = SystemCatalog::getInstance()->getNextArrayId();
            arrSchema.setIds(uAId, uAId, VersionID(0));
            if (!temp) {
                query->setAutoCommit();
            }

            LOG4CXX_TRACE(logger, "PhysicalCreateArray::execute("
                << "namespaceName=" << namespaceName
                << ",arrayName=" << arrSchema.getName() << ")");

            SystemCatalog::getInstance()->addArray(arrSchema);
        }

        if (temp)                                        // 'temp' flag given?
        {
            syncBarrier(0,query);                        // Workers wait here

            string arrayNameOrg(param<OperatorParamArrayReference>(0)->getObjectName());

            std::string arrayName;
            std::string namespaceName;
            query->getNamespaceArrayNames(arrayNameOrg, namespaceName, arrayName);

            ArrayDesc arrSchema;
            // XXX TODO: this needs to change to eliminate worker catalog access
            scidb::namespaces::Communicator::getArrayDesc(
                namespaceName, arrayName,SystemCatalog::ANY_VERSION,arrSchema);

            transient::record(make_shared<MemArray>(arrSchema,query));
        }

        return std::shared_ptr<Array>();
    }

    template<class t>
    std::shared_ptr<t>& param(size_t i) const
    {
        assert(i < _parameters.size());

        return (std::shared_ptr<t>&)_parameters[i];
    }
};

/**
 *  Implements the create_array_as() operator (a variant of create_array that
 *  accepts additional statistics from which missing dimension sizes can then
 *  be computed and filled in) as a subclass of PhysicalCreateArray.
 *
 *  The goal here is to override the virtual fixDimensions() member() to fill
 *  in missing dimension details with sizes computed from the array of stats
 *  supplied as our initial input array, which has the following shape:
 *
 *      <loBound,hiBound,interval,overlap,minimum,maximum,distinct>[dimension]
 *
 *  where the first four components are boolean flags indicating whether the
 *  corresponding component of the target shcema was set by the user (true) or
 *  is to be computed here (false).
 *
 *  @see [wiki:Development/components/Client_Tools/ChunkLengthCalculator_NewPythonVersion]
 *  for the basic chunk selection algorithm we are implementing here.
 */
struct PhysicalCreateArrayUsing : PhysicalCreateArray
{
    typedef ChunkEstimator::Statistics statistics;

    PhysicalCreateArrayUsing(const string& logicalName,
                             const string& physicalName,
                             const Parameters& parameters,
                             const ArrayDesc& schema)
      : PhysicalCreateArray(logicalName,physicalName,parameters,schema)
    {}

    virtual void fixDimensions(PointerRange<std::shared_ptr<Array> > in,
                               Dimensions&                           dims)
    {
        assert(in.size()==2 && !dims.empty());           // Validate arguments

        size_t const   desiredValuesPerChunk(getDesiredValuesPerChunk(*in[1]));
        size_t const    overallDistinctCount(getOverallDistinctCount (*in[1]));
        vector<ChunkEstimator::Statistics> S(dims.size()); // Array of statistics

        getStatistics(S,*in[0]);                         // Read in statistics

        ChunkEstimator estimator(dims);
        estimator.setTargetCellCount(desiredValuesPerChunk)
            .setOverallDistinct(overallDistinctCount)
            .setAllStatistics(S)
            .go();
    }

    /**
     *  Input array A is a list of 7-tuples, each a (possibly null) integer,
     *  one record for each dimension in the target schema.
     *
     *  Read the rows of A into a vector of 'statistics' objects.
     */
    void getStatistics(vector<statistics>& v,Array& A)
    {
        size_t constexpr statisticsSize = std::tuple_size<statistics>::value;
        for (size_t a=0; a!=statisticsSize; ++a)     // For each attribute
        {
            std::shared_ptr<ConstChunkIterator> i(
                A.getConstIterator(safe_static_cast<AttributeID>(a))->getChunk().getConstIterator());

            for (size_t d=0; d!=v.size(); ++d)           // ...for each dimension
            {
                v[d][a] = i->getItem();                  // ....read the item

                i->operator++();                         // ....to next item
            }
        }
    }

    /**
     *  Return the first integer from the input array A, the overall distinct
     *  count of values found in the load array.
     */
    int64_t getOverallDistinctCount(Array& A) const
    {
        return A.getConstIterator(0)->getChunk().getConstIterator()->getItem().getInt64();
    }

    /**
     *  Return the second integer from the input array A, the desired number
     *  of values per chunk.
     */
    int64_t getDesiredValuesPerChunk(Array& A) const
    {
        return A.getConstIterator(1)->getChunk().getConstIterator()->getItem().getInt64();
    }
};

/****************************************************************************/

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCreateArray,     "create_array",      "impl_create_array")
DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalCreateArrayUsing,"create_array_using","impl_create_array_using")

/****************************************************************************/
}
/****************************************************************************/
