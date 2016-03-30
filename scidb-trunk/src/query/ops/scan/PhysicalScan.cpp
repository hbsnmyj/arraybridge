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
 * PhysicalScan.cpp
 *
 *  Created on: Oct 28, 2010
 *      Author: knizhnik@garret.ru
 */
#include <memory>

#include <array/DBArray.h>
#include <array/Metadata.h>
#include <array/TransientCache.h>
#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <usr_namespace/NamespacesCommunicator.h>

using namespace std;

namespace scidb
{
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.physical_scan"));

class PhysicalScan: public  PhysicalOperator
{
  public:
    PhysicalScan(const std::string& logicalName,
                 const std::string& physicalName,
                 const Parameters& parameters,
                 const ArrayDesc& schema):
    PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
        _arrayName = dynamic_pointer_cast<OperatorParamReference>(parameters[0])->getObjectName();
    }

    virtual RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                                      const std::vector< ArrayDesc> & inputSchemas) const
    {
        ArrayDistPtr arrDist = _schema.getDistribution();
        SCIDB_ASSERT(arrDist);
        SCIDB_ASSERT(arrDist->getPartitioningSchema()!=psUninitialized);
        std::shared_ptr<Query> query(_query);
        SCIDB_ASSERT(query);
        if (query->isDistributionDegradedForRead(_schema)) {
            // make sure PhysicalScan informs the optimizer that the distribution is unknown
            SCIDB_ASSERT(arrDist->getPartitioningSchema()!=psUndefined);
            //XXX TODO: psReplication declared as psUndefined would confuse SG because most of the data would collide.
            //XXX TODO: One option is to take the intersection between the array residency and the query live set
            //XXX TODO: (i.e. the default array residency) and advertize that as the new residency (with psReplicated)...
            ASSERT_EXCEPTION((arrDist->getPartitioningSchema()!=psReplication),
                             "Arrays with replicated distribution in degraded mode are not supported");

            // not  updating the schema, so that DBArray can succeed
            return RedistributeContext(createDistribution(psUndefined),
                                       _schema.getResidency());
        }
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    virtual PhysicalBoundaries getOutputBoundaries(const std::vector<PhysicalBoundaries> & inputBoundaries,
                                                   const std::vector<ArrayDesc> & inputSchemas) const
    {
        Coordinates lowBoundary = _schema.getLowBoundary();
        Coordinates highBoundary = _schema.getHighBoundary();

        return PhysicalBoundaries(lowBoundary, highBoundary);
    }

    virtual void preSingleExecute(std::shared_ptr<Query> query)
    {
        if (_schema.isTransient())
        {
            query->isDistributionDegradedForWrite(_schema);
        }
    }

    std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays,
                                      std::shared_ptr<Query> query)
    {
        SCIDB_ASSERT(!_arrayName.empty());
        if (_schema.isTransient())
        {
            if (!query->isCoordinator())
            {
                std::string arrayName;
                std::string namespaceName;
                query->getNamespaceArrayNames(_arrayName, namespaceName, arrayName);

                std::shared_ptr<SystemCatalog::LockDesc> lock(
                    make_shared<SystemCatalog::LockDesc>(
                        namespaceName,
                        arrayName,
                        query->getQueryID(),
                        Cluster::getInstance()->getLocalInstanceId(),
                        SystemCatalog::LockDesc::WORKER,
                        SystemCatalog::LockDesc::XCL));

                Query::Finalizer f = bind(&UpdateErrorHandler::releaseLock, lock,_1);
                query->pushFinalizer(f);
                SystemCatalog::ErrorChecker errorChecker(bind(&Query::validate, query));
                if (!SystemCatalog::getInstance()->lockArray(lock, errorChecker)) {
                    throw USER_EXCEPTION(SCIDB_SE_SYSCAT, SCIDB_LE_CANT_INCREMENT_LOCK)<< lock->toString();
                }
            }
            MemArrayPtr a = transient::lookup(_schema,query);
            ASSERT_EXCEPTION(a.get()!=nullptr, string("Temp array ")+_schema.toString()+string(" not found"));
            return a;                                   // ...temp array
        }
        else
        {
            assert(_schema.getId() != 0);
            assert(_schema.getUAId() != 0);
            return std::shared_ptr<Array>(DBArray::newDBArray(_schema, query));
        }
    }

  private:
    string _arrayName;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalScan, "scan", "physicalScan")

} //namespace scidb
