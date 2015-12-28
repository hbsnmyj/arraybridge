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
 * LogicalStore.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include <boost/foreach.hpp>
#include <map>

#include "query/Operator.h"
#include "system/SystemCatalog.h"
#include "system/Exceptions.h"
#include <smgr/io/Storage.h>

using namespace std;
using namespace boost;

namespace scidb {

/**
 * @brief The operator: store().
 *
 * @par Synopsis:
 *   store( srcArray, outputArray )
 *
 * @par Summary:
 *   Stores an array to the database. Each execution of store() causes a new version of the array to be created.
 *
 * @par Input:
 *   - srcArray: the source array with srcAttrs and srcDim.
 *   - outputArray: an existing array in the database, with the same schema as srcArray.
 *
 * @par Output array:
 *        <
 *   <br>   srcAttrs
 *   <br> >
 *   <br> [
 *   <br>   srcDims
 *   <br> ]
 *
 * @par Examples:
 *   n/a
 *
 * @par Errors:
 *   n/a
 *
 * @par Notes:
 *   n/a
 *
 */
class LogicalStore: public  LogicalOperator
{
public:
    LogicalStore(const string& logicalName, const std::string& alias)
        : LogicalOperator(logicalName, alias)
    {
        _properties.tile = true;
        ADD_PARAM_INPUT()
        ADD_PARAM_OUT_ARRAY_NAME()
    }

    void inferArrayAccess(std::shared_ptr<Query>& query)
    {
        LogicalOperator::inferArrayAccess(query);
        SCIDB_ASSERT(_parameters.size() > 0);
        SCIDB_ASSERT(_parameters[0]->getParamType() == PARAM_ARRAY_REF);
        const string& arrayName = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        SCIDB_ASSERT(arrayName.find('@') == std::string::npos);

        ArrayDesc srcDesc;
        SCIDB_ASSERT(!srcDesc.isTransient());
        const bool dontThrow(false);

        SystemCatalog::getInstance()->getArrayDesc(arrayName, SystemCatalog::ANY_VERSION, srcDesc, dontThrow);

        const SystemCatalog::LockDesc::LockMode lockMode =
            srcDesc.isTransient() ? SystemCatalog::LockDesc::XCL : SystemCatalog::LockDesc::WR;

        std::shared_ptr<SystemCatalog::LockDesc>  lock(make_shared<SystemCatalog::LockDesc>(arrayName,
                                                                                       query->getQueryID(),
                                                                                       Cluster::getInstance()->getLocalInstanceId(),
                                                                                       SystemCatalog::LockDesc::COORD,
                                                                                       lockMode));
        std::shared_ptr<SystemCatalog::LockDesc> resLock = query->requestLock(lock);
        SCIDB_ASSERT(resLock);
        SCIDB_ASSERT(resLock->getLockMode() >= SystemCatalog::LockDesc::WR);
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        SCIDB_ASSERT(schemas.size() == 1);
        SCIDB_ASSERT(_parameters.size() == 1);

        const string& arrayName = ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();
        SCIDB_ASSERT(ArrayDesc::isNameUnversioned(arrayName));

        ArrayDesc const& srcDesc = schemas[0];
        PartitioningSchema ps = srcDesc.getPartitioningSchema();
        //XXX TODO: at some point we will take the distribution of the input,
        //XXX TODO: but currently the ps value is not propagated through the pipeline correctly,
        //XXX TODO: so we are forcing it.
        //XXX TODO: Another complication is that SGs are inserted before the physical execution,
        //XXX TODO: Here, we dont know the true distribution coming into the store()
        ps = defaultPartitioning();

        //Ensure attributes names uniqueness.
        ArrayDesc dstDesc;
        if (!SystemCatalog::getInstance()->getArrayDesc(arrayName,
                                                        query->getCatalogVersion(arrayName),
                                                        dstDesc, false))
        {
            Attributes outAttrs;
            map<string, uint64_t> attrsMap;
            BOOST_FOREACH(const AttributeDesc &attr, srcDesc.getAttributes())
            {
                AttributeDesc newAttr;
                if (!attrsMap.count(attr.getName()))
                {
                    attrsMap[attr.getName()] = 1;
                    newAttr = attr;
                }
                else
                {
                    while (true) {
                        stringstream ss;
                        ss << attr.getName() << "_" << ++attrsMap[attr.getName()];
                        if (attrsMap.count(ss.str()) == 0) {
                            newAttr = AttributeDesc(attr.getId(), ss.str(),
                                                    attr.getType(), attr.getFlags(),
                                                    attr.getDefaultCompressionMethod(),
                                                    attr.getAliases(),
                                                    &attr.getDefaultValue(),
                                                    attr.getDefaultValueExpr());
                            attrsMap[ss.str()] = 1;
                            break;
                        }
                    }
                }

                outAttrs.push_back(newAttr);
            }

            Dimensions outDims;
            map<string, uint64_t> dimsMap;
            BOOST_FOREACH(const DimensionDesc &dim, srcDesc.getDimensions())
            {
                DimensionDesc newDim;
                if (!dimsMap.count(dim.getBaseName()))
                {
                    dimsMap[dim.getBaseName()] = 1;
                    newDim = DimensionDesc(dim.getBaseName(),
                                           dim.getStartMin(),
                                           dim.getCurrStart(),
                                           dim.getCurrEnd(),
                                           dim.getEndMax(),
                                           dim.getChunkInterval(),
                                           dim.getChunkOverlap());
                }
                else
                {
                    while (true) {
                        stringstream ss;
                        ss << dim.getBaseName() << "_" << ++dimsMap[dim.getBaseName()];
                        if (dimsMap.count(ss.str()) == 0) {
                            newDim = DimensionDesc(ss.str(),
                                                   dim.getStartMin(),
                                                   dim.getCurrStart(),
                                                   dim.getCurrEnd(),
                                                   dim.getEndMax(),
                                                   dim.getChunkInterval(),
                                                   dim.getChunkOverlap());
                            dimsMap[ss.str()] = 1;
                            break;
                        }
                    }
                }

                outDims.push_back(newDim);
            }

            /* Notice that when storing to a non-existant array, we do not propagate the
               transience of the source array to to the target ...*/
            ArrayDesc schema(arrayName, outAttrs, outDims, ps,
                             srcDesc.getFlags() & (~ArrayDesc::TRANSIENT));
            return schema;
        }

        // Check schemas to ensure that the source array can be stored in the destination.  We
        // can ignore overlaps and chunk intervals because our physical operator implements
        // requiresRedimensionOrRepartition() to get automatic repartitioning.
        //
        // XXX Why we allow short source dimensions if the source has an empty bitmap
        // (SHORT_OK_IF_EBM), while insert() does not, remains a mystery.
        //
        ArrayDesc::checkConformity(srcDesc, dstDesc,
                                   ArrayDesc::IGNORE_PSCHEME |
                                   ArrayDesc::IGNORE_OVERLAP |
                                   ArrayDesc::IGNORE_INTERVAL |
                                   ArrayDesc::SHORT_OK_IF_EBM);

        Dimensions const& dstDims = dstDesc.getDimensions();
        Dimensions newDims(dstDims.size()); //XXX need this ?
        for (size_t i = 0; i < dstDims.size(); i++) {
            DimensionDesc const& dim = dstDims[i];
            newDims[i] = DimensionDesc(dim.getBaseName(),
                                       dim.getNamesAndAliases(),
                                       dim.getStartMin(), dim.getCurrStart(),
                                       dim.getCurrEnd(), dim.getEndMax(),
                                       dim.getChunkInterval(), dim.getChunkOverlap());
        }

        dstDesc.setDimensions(newDims);
        SCIDB_ASSERT(dstDesc.getId() == dstDesc.getUAId() && dstDesc.getName() == arrayName);
        SCIDB_ASSERT(dstDesc.getUAId() > 0);
        SCIDB_ASSERT(ps==dstDesc.getPartitioningSchema());
        return dstDesc;
    }

};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalStore, "store")

}  // namespace scidb
