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
 * @file
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 *
 * @brief Shows object. E.g. schema of array.
 */

#include <array/MemArray.h>
#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <query/QueryProcessor.h>
#include <log4cxx/logger.h>
#include <system/SystemCatalog.h>
#include <util/Thread.h>

using namespace std;
using namespace boost;

namespace scidb
{
    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.physical_show"));

class PhysicalShow: public PhysicalOperator
{
public:
    PhysicalShow(const string& logicalName, const string& physicalName, const Parameters& parameters, const ArrayDesc& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual RedistributeContext getOutputDistribution(const std::vector<RedistributeContext>& inputDistributions,
                                                      const std::vector< ArrayDesc>& inputSchemas) const
    {
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        stringstream ss;

        ArrayDesc desc;

        if (_parameters[0]->getParamType() == PARAM_SCHEMA)
        {
        	desc = ((const std::shared_ptr<OperatorParamSchema>&)_parameters[0])->getSchema();
        }
        else if (_parameters[0]->getParamType() == PARAM_PHYSICAL_EXPRESSION)
        {
    		string queryString =
    				((const std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0])
    				->getExpression()->evaluate().getString();
    		bool afl = false;
        	if (_parameters.size() == 2)
        	{
                string lang = ((std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1])
                		->getExpression()->evaluate().getString();
    			std::transform(lang.begin(), lang.end(), lang.begin(), ::tolower);
                afl = lang == "afl";
        	}

            std::shared_ptr<QueryProcessor> queryProcessor = QueryProcessor::create();
            std::shared_ptr<Query> innerQuery = Query::createFakeQuery(
                             query->getPhysicalCoordinatorID(),
                             query->mapLogicalToPhysical(query->getInstanceID()),
                             query->getCoordinatorLiveness());
            innerQuery->attachSession(query->getSession());

            boost::function<void()> func = boost::bind(&Query::destroyFakeQuery, innerQuery.get());
            Destructor<boost::function<void()> > fqd(func);

            innerQuery->queryString = queryString;

            queryProcessor->parseLogical(innerQuery, afl);
            desc = queryProcessor->inferTypes(innerQuery);
        }

        printSchema(ss, desc);

        _result = std::shared_ptr<MemArray>(new MemArray(_schema,query));
        std::shared_ptr<ArrayIterator> arrIt = _result->getIterator(0);
        Coordinates coords;
        coords.push_back(0);
        Chunk& chunk = arrIt->newChunk(coords);
        std::shared_ptr<ChunkIterator> chunkIt = chunk.getIterator(query);
        Value v(TypeLibrary::getType(TID_STRING));
        v.setString(ss.str().c_str());
        chunkIt->writeItem(v);
        chunkIt->flush();
    }

    std::shared_ptr<Array> execute(
        std::vector<std::shared_ptr<Array> >& inputArrays,
        std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        if (!_result) {
            _result = std::shared_ptr<MemArray>(new MemArray(_schema,query));
        }
        return _result;
    }

private:
    std::shared_ptr<Array> _result;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalShow, "show", "impl_show")

} //namespace
