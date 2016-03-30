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
#include <malloc.h>
#include <string.h>
#include <sstream>

#include <log4cxx/logger.h>

#include <query/Parser.h>
#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <array/TupleArray.h>
#include <array/DelegateArray.h>
#include <array/TransientCache.h>
#include <system/SystemCatalog.h>
#include <query/TypeSystem.h>
#include <util/PluginManager.h>
#include <smgr/io/Storage.h>
#include "ListArrayBuilders.h"
#include <usr_namespace/NamespacesCommunicator.h>
#include <usr_namespace/NamespaceDesc.h>
#include <usr_namespace/Permissions.h>
#include <usr_namespace/RoleDesc.h>
#include <usr_namespace/SecurityCommunicator.h>
#include <usr_namespace/UserDesc.h>
#include <util/session/Session.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.ops.PhysicalList"));

using namespace std;
using namespace boost;

struct PhysicalList : PhysicalOperator
{
    PhysicalList(const string& logicalName,
                 const string& physicalName,
                 const Parameters& parameters,
                 const ArrayDesc& schema)
        : PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    string getMainParameter() const
    {
        if (_parameters.empty())
        {
            return "arrays";
        }

        OperatorParamPhysicalExpression& exp =
            *(std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[0];
        return exp.getExpression()->evaluate().getString();
    }

    bool getShowSysParameter() const
    {
        if (_parameters.size() < 2)
        {
            return false;
        }

        OperatorParamPhysicalExpression& exp =
            *(std::shared_ptr<OperatorParamPhysicalExpression>&)_parameters[1];
        return exp.getExpression()->evaluate().getBool();
    }

    bool coordinatorOnly() const
    {
        // The operations NOT in this list run exclusively on the coordinator

        static const char* const s[] =
        {
            "chunk descriptors",
            "chunk map",
            "datastores",
            "libraries",
            "meminfo",
            "queries",
        };

        return !std::binary_search(s,s+SCIDB_SIZE(s),getMainParameter().c_str(),less_strcmp());
    }

    virtual RedistributeContext getOutputDistribution(const std::vector<RedistributeContext> & inputDistributions,
                                                      const std::vector< ArrayDesc> & inputSchemas) const
    {
        if (coordinatorOnly()) {
            stringstream ss;

            std::shared_ptr<Query> query(_query);
            SCIDB_ASSERT(query);

            ss << query->getInstanceID();
            ArrayDistPtr localDist = ArrayDistributionFactory::getInstance()->construct(psLocalInstance,
                                                                                        DEFAULT_REDUNDANCY,
                                                                                        ss.str());
            ArrayDesc* mySchema = const_cast<ArrayDesc*>(&_schema);
            mySchema->setDistribution(localDist);
        }
        return RedistributeContext(_schema.getDistribution(),
                                   _schema.getResidency());
    }

    std::shared_ptr<Array> execute(vector< std::shared_ptr<Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        if (coordinatorOnly() && !query->isCoordinator())
        {
            return make_shared<MemArray>(_schema,query);
        }

        vector<string> items;
        string  const  what = getMainParameter();
        bool           showSys = getShowSysParameter();

        if (what == "aggregates") {
            ListAggregatesArrayBuilder builder(
                AggregateLibrary::getInstance()->getNumAggregates(), showSys);
            builder.initialize(query);
            AggregateLibrary::getInstance()->visitPlugins(
                AggregateLibrary::Visitor(
                    boost::bind(
                        &ListAggregatesArrayBuilder::list,&builder,_1,_2,_3)));
            return builder.getArray();
        } else if (what == "arrays") {

            bool showAllArrays = false;
            if (_parameters.size() == 2)
            {
                showAllArrays = ((std::shared_ptr<OperatorParamPhysicalExpression>&)
                    _parameters[1])->getExpression()->evaluate().getBool();
            }
            return listArrays(showAllArrays, query);

        } else if (what == "operators") {
            OperatorLibrary::getInstance()->getLogicalNames(items, showSys);
            std::shared_ptr<TupleArray> tuples(std::make_shared<TupleArray>(_schema, _arena));
            for (size_t i=0, n=items.size(); i!=n; ++i) {
                Value tuple[3];
                size_t tupleSize = (showSys ? 3 : 2);
                tuple[0].setString(items[i]);
                tuple[1].setString(
                    OperatorLibrary::getInstance()->getOperatorLibraries().getObjectLibrary(items[i]));
                if (showSys) {
                    tuple[2].setBool(OperatorLibrary::isHiddenOp(items[i]));
                }
                tuples->appendTuple(PointerRange<Value>(tupleSize, tuple));
            }
            return tuples;
        } else if (what == "types") {
            items = TypeLibrary::typeIds();
            std::shared_ptr<TupleArray> tuples(std::make_shared<TupleArray>(_schema, _arena));
            for (size_t i=0, n=items.size(); i!=n; ++i) {
                Value tuple[2];
                tuple[0].setString(items[i]);
                tuple[1].setString(
                    TypeLibrary::getTypeLibraries().getObjectLibrary(items[i]));
                tuples->appendTuple(tuple);
            }
            return tuples;
        } else if (what == "functions") {
            std::shared_ptr<TupleArray> tuples(std::make_shared<TupleArray>(_schema, _arena));
            funcDescNamesMap& funcs = FunctionLibrary::getInstance()->getFunctions();
            for (funcDescNamesMap::const_iterator i = funcs.begin();
                 i != funcs.end(); ++i)
            {
                for (funcDescTypesMap::const_iterator j = i->second.begin();
                     j != i->second.end(); ++j)
                {
                    Value tuple[4];
                    FunctionDescription const& func = j->second;
                    tuple[0].setString(func.getName());
                    tuple[1].setString(func.getMangleName());
                    tuple[2].setBool(func.isDeterministic());
                    tuple[3].setString(FunctionLibrary::getInstance()->getFunctionLibraries().getObjectLibrary(func.getMangleName()));
                    tuples->appendTuple(tuple);
                }
            }
            Value tuple1[4];
            tuple1[0].setString("iif");
            tuple1[1].setString("<any> iif(bool, <any>, <any>)");
            tuple1[2].setBool(true);
            tuple1[3].setString("scidb");
            tuples->appendTuple(tuple1);

            Value tuple2[4];
            tuple2[0].setString("missing_reason");
            tuple2[1].setString("int32 missing_reason(<any>)");
            tuple2[2].setBool(true);
            tuple2[3].setString("scidb");
            tuples->appendTuple(tuple2);

            Value tuple3[4];
            tuple3[0].setString("sizeof");
            tuple3[1].setString("uint64 sizeof(<any>)");
            tuple3[2].setBool(true);
            tuple3[3].setString("scidb");
            tuples->appendTuple(tuple3);

            return tuples;
        } else if (what == "macros") {
            return physicalListMacros(_arena, query); // see Parser.h
        } else if (what == "queries") {
            ListQueriesArrayBuilder builder;
            builder.initialize(query);
            Query::visitQueries(
                Query::Visitor(
                    boost::bind(
                        &ListQueriesArrayBuilder::list, &builder, _1)));
            return builder.getArray();
        } else if (what == "instances") {
            return listInstances(query);
        } else if (what == "users") {
            return listUsers(query);
        } else if (what == "roles") {
            return listRoles(query);
        } else if (what == "namespaces") {
            return listNamespaces(query);
        } else if (what == "chunk descriptors") {
            ListChunkDescriptorsArrayBuilder builder;
            builder.initialize(query);
            StorageManager::getInstance().visitChunkDescriptors(
                Storage::ChunkDescriptorVisitor(
                    boost::bind(
                        &ListChunkDescriptorsArrayBuilder::list,&builder,_1,_2)));
            return builder.getArray();
        } else if (what == "chunk map") {
            ListChunkMapArrayBuilder builder;
            builder.initialize(query);
            StorageManager::getInstance().visitChunkMap(
                Storage::ChunkMapVisitor(
                    boost::bind(
                        &ListChunkMapArrayBuilder::list,&builder,_1,_2,_3,_4,_5)));
            return builder.getArray();
        } else if (what == "libraries") {
            ListLibrariesArrayBuilder builder;
            builder.initialize(query);
            PluginManager::getInstance()->visitPlugins(
                PluginManager::Visitor(
                    boost::bind(
                        &ListLibrariesArrayBuilder::list,&builder,_1)));
            return builder.getArray();
        } else if (what == "datastores") {
            ListDataStoresArrayBuilder builder;
            builder.initialize(query);
            StorageManager::getInstance().getDataStores().visitDataStores(
                DataStores::Visitor(
                    boost::bind(
                        &ListDataStoresArrayBuilder::list,&builder,_1)));
            return builder.getArray();
        } else if (what == "counters") {
            bool reset = false;
            if (_parameters.size() == 2)
            {
                reset = ((std::shared_ptr<OperatorParamPhysicalExpression>&)
                         _parameters[1])->getExpression()->evaluate().getBool();
            }
            ListCounterArrayBuilder builder;
            builder.initialize(query);
            CounterState::getInstance()->visitCounters(
                CounterState::Visitor(
                    boost::bind(
                        &ListCounterArrayBuilder::list,&builder,_1)));
            if (reset)
            {
                CounterState::getInstance()->reset();
            }
            return builder.getArray();
        }
        else
        {
            SCIDB_UNREACHABLE();
        }

        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_UNREACHABLE_CODE) << "PhysicalList::execute";
     }

    std::shared_ptr<Array> listInstances(
        const std::shared_ptr<Query>& query)
    {
        std::shared_ptr<const InstanceLiveness> queryLiveness(
            query->getCoordinatorLiveness());

        Instances instances;
        SystemCatalog::getInstance()->getInstances(instances);

        assert(queryLiveness->getNumInstances() == instances.size());

        std::shared_ptr<TupleArray> tuples(
            std::make_shared<TupleArray>(_schema, _arena));

        for (   Instances::const_iterator iter = instances.begin();
                iter != instances.end();
                ++iter)
        {
            Value tuple[5];

            const InstanceDesc& instanceDesc = *iter;
            InstanceID instanceId = instanceDesc.getInstanceId();
            time_t t = static_cast<time_t>(instanceDesc.getOnlineSince());
            tuple[0].setString(instanceDesc.getHost());
            tuple[1].setUint16(instanceDesc.getPort());
            tuple[2].setUint64(instanceId);
            if ((t == (time_t)0) || queryLiveness->isDead(instanceId)){
                tuple[3].setString("offline");
            } else {
                assert(queryLiveness->find(instanceId));
                struct tm date;

                if (!(&date == gmtime_r(&t, &date)))
                {
                  throw SYSTEM_EXCEPTION(
                      SCIDB_SE_EXECUTION,
                      SCIDB_LE_CANT_GENERATE_UTC_TIME);
                }

                string out(boost::str(
                    boost::format("%04d-%02d-%02d %02d:%02d:%02d")
                        % (date.tm_year+1900)
                        % (date.tm_mon+1)
                        % date.tm_mday
                        % date.tm_hour
                        % date.tm_min
                        % date.tm_sec));
                tuple[3].setString(out);
            }
            tuple[4].setString(instanceDesc.getPath());
            tuples->appendTuple(tuple);
        }
        return tuples;
    }

    std::shared_ptr<Array> listUsers(
        const std::shared_ptr<Query>& query)
    {
        std::shared_ptr<TupleArray> tuples(
            std::make_shared<TupleArray>(_schema, _arena));

        std::vector<scidb::UserDesc> users;
        scidb::security::Communicator::getUsers(users);
        for (size_t i=0, n=users.size(); i!=n; ++i) {
            scidb::UserDesc &user = users[i];
            Value tuple[2];
            tuple[0].setString(user.getName());
            tuple[1].setUint64(user.getId());
            tuples->appendTuple(tuple);
        }

        return tuples;
    }

    std::shared_ptr<Array> listRoles(
        const std::shared_ptr<Query>& query)
    {
        std::shared_ptr<TupleArray> tuples(
            std::make_shared<TupleArray>(_schema, _arena));

        std::vector<scidb::RoleDesc> roles;
        scidb::namespaces::Communicator::getRoles(roles);
        for (size_t i=0, n=roles.size(); i!=n; ++i) {
            scidb::RoleDesc &role = roles[i];
            Value tuple[1];
            tuple[0].setString(role.getName());
            tuples->appendTuple(tuple);
        }

        return tuples;
    }

     std::shared_ptr<Array> listNamespaces(
         const std::shared_ptr<Query>& query)
     {
        std::shared_ptr<TupleArray> tuples(
            std::make_shared<TupleArray>(_schema, _arena));

        const std::shared_ptr<scidb::Session> &session = query->getSession();
        std::string permissions;
        permissions.push_back(scidb::permissions::namespaces::ListArrays);

        // Add only the namespaces that we have permission to list
        std::vector<NamespaceDesc> namespaces;
        scidb::namespaces::Communicator::getNamespaces(namespaces);
        for (size_t i=0, n=namespaces.size(); i!=n; ++i) {
            NamespaceDesc &current_namespace = namespaces[i];

            try
            {
                scidb::namespaces::Communicator::checkNamespacePermissions(
                    session, current_namespace, permissions);
            }
            catch(const scidb::Exception& e)
            {
                continue;
            }

            Value tuple[1];
            tuple[0].setString(current_namespace.getName());
            tuples->appendTuple(tuple);
        }

        return tuples;
    }


    std::shared_ptr<Array> listArrays(
        bool showAllArrays,
        const std::shared_ptr<Query>& query)
    {
        ListArraysArrayBuilder builder;
        builder.initialize(query);

        vector<ArrayDesc> arrayDescs;
        const bool ignoreOrphanAttributes = true;

        std::string namespaceName = scidb::namespaces::Communicator::getNamespaceName(query);
        scidb::namespaces::Communicator::getArrays(
            namespaceName,
            arrayDescs,
            ignoreOrphanAttributes,
            !showAllArrays);

        std::string permissions;
        permissions.push_back(scidb::permissions::namespaces::ListArrays);
        scidb::namespaces::Communicator::checkNamespacePermissions(
            query->getSession(), namespaceName, permissions);

        for (size_t i=0, n=arrayDescs.size(); i!=n; ++i)
        {
            const ArrayDesc& arrayDesc = arrayDescs[i];
            // filter out metadata introduced after the catalog version of this query/txn
            // XXX TODO: this does not deal with the arrays not locked by this query
            // XXX TODO: (they can be added/updated/removed mid-flight, i.e. before list::execute() runs).
            // XXX TODO: Either make list() take an 'ALL' array lock or
            // XXX TODO: introduce a single serialized PG timestamp, or ...
            const ArrayID catVersion = query->getCatalogVersion(
                namespaceName, arrayDesc.getName(), true);

            if (arrayDesc.getId() <= catVersion && arrayDesc.getUAId() <= catVersion)
            {
                builder.list(arrayDesc);
            }
        }
        return builder.getArray();
    }
};

DECLARE_PHYSICAL_OPERATOR_FACTORY(PhysicalList, "list", "physicalList")

/****************************************************************************/
}
/****************************************************************************/
