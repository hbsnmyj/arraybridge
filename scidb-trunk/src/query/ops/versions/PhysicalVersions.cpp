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
 * @file PhysicalVersions.cpp
 *
 * @author knizhnik@garret.ru
 *
 * Physical implementation of VERSIONS operator for versionsing data from text files
 */

#include <string.h>

#include <array/TupleArray.h>
#include <query/Operator.h>
#include <system/SystemCatalog.h>
#include <usr_namespace/NamespacesCommunicator.h>


using namespace std;
using namespace boost;

namespace scidb
{

class PhysicalVersions: public PhysicalOperator
{
public:
    PhysicalVersions(
        const string& logicalName, const string& physicalName,
        const Parameters& parameters, const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {
    }

    virtual RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                                      const std::vector< ArrayDesc> & inputSchemas) const
    {
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    void preSingleExecute(std::shared_ptr<Query> query)
    {
        assert(_parameters.size() == 1);

        const string &arrayNameOrg =
            ((std::shared_ptr<OperatorParamReference>&)_parameters[0])->getObjectName();

        std::string arrayName;
        std::string namespaceName;
        query->getNamespaceArrayNames(arrayNameOrg, namespaceName, arrayName);

        ArrayDesc arrayDesc;
        const ArrayID catalogVersion = query->getCatalogVersion(namespaceName, arrayName);
        scidb::namespaces::Communicator::getArrayDesc(
            namespaceName, arrayName, catalogVersion, arrayDesc);

        std::vector<VersionDesc> versions =
            SystemCatalog::getInstance()->getArrayVersions(arrayDesc.getId());

        std::shared_ptr<TupleArray> tuples(std::make_shared<TupleArray>(_schema, _arena));
        for (size_t i = 0; i < versions.size(); ++i) {
            const VersionDesc& verDesc = versions[i];
            if (verDesc.getArrayID() > catalogVersion) {
                //XXX tigor: this is a HACK to allow concurrent readers & writers
                // instead, we should either remove this op or make getArrayVersions()
                // respect the catalog version (or smthn like that)
                break;
            }
            Value tuple[2];
            tuple[0] = Value(TypeLibrary::getType(TID_INT64));
            tuple[0].setInt64(verDesc.getVersionID());
            tuple[1] = Value(TypeLibrary::getType(TID_DATETIME));
            tuple[1].setDateTime(verDesc.getTimeStamp());

            tuples->appendTuple(tuple);
        }
        _result = tuples;
    }

    std::shared_ptr<Array> execute(
        std::vector<std::shared_ptr<Array> >& inputArrays,
        std::shared_ptr<Query> query)
    {
        assert(inputArrays.size() == 0);
        if (!_result)
        {
            _result = std::make_shared<MemArray>(_schema, query);
        }
        return _result;
    }

private:
    std::shared_ptr<Array> _result;
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalVersions, "versions", "physicalVersions")

} //namespace
