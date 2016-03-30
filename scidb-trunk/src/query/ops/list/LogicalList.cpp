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

#include <query/Parser.h>
#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <system/Exceptions.h>
#include <array/Metadata.h>
#include <system/SystemCatalog.h>
#include <util/PluginManager.h>
#include <smgr/io/Storage.h>
#include <util/DataStore.h>
#include "ListArrayBuilders.h"
#include <usr_namespace/NamespacesCommunicator.h>
#include <usr_namespace/NamespaceDesc.h>
#include <usr_namespace/RoleDesc.h>
#include <usr_namespace/SecurityCommunicator.h>
#include <usr_namespace/UserDesc.h>

/****************************************************************************/
namespace scidb {
/****************************************************************************/

using namespace std;
using namespace boost;

/**
 * @brief The operator: list().
 *
 * @par Synopsis:
 *   list( what='arrays', showSystem=false )
 *
 * @par Summary:
 *   Produces a result array and loads data from a given file, and optionally stores to shadowArray.
 *   The available things to list include:
 *   - aggregates: show all the aggregate operators.
 *   - arrays: show all the arrays.
 *   - chunk descriptors: show all the chunk descriptors.
 *   - chunk map: show the chunk map.
 *   - functions: show all the functions.
 *   - instances: show all SciDB instances.
 *   - libraries: show all the libraries that are loaded in the current SciDB session.
 *   - operators: show all the operators and the libraries in which they reside.
 *   - types: show all the datatypes that SciDB supports.
 *   - queries: show all the active queries.
 *   - datastores: show information about each datastore
 *   - counters: (undocumented) dump info from performance counters
 *
 * @par Input:
 *   - what: what to list.
 *   - showSystem: whether to show systems information.
 *
 * @par Output array:
 *        <
 *   <br>   The list of attributes depends on the input.
 *   <br> >
 *   <br> [
 *   <br>   No: sequence number
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
struct LogicalList : LogicalOperator
{
    LogicalList(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_VARIES()
    }

    std::vector<std::shared_ptr<OperatorParamPlaceholder> >
    nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas)
    {
        std::vector<std::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() == 0)
            res.push_back(PARAM_CONSTANT(TID_STRING));
        if (_parameters.size() == 1)
            res.push_back(PARAM_CONSTANT(TID_BOOL));
        return res;
    }

    string getMainParameter(std::shared_ptr<Query> query) const
    {
        if (_parameters.empty())
        {
            return "arrays";
        }

        return evaluate(
                ((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[0])->getExpression(),
                query,
                TID_STRING).getString();
    }

    bool getShowSysParameter(std::shared_ptr<Query> query) const
    {
        if (_parameters.size() < 2)
        {
            return false;
        }

        return evaluate(
                ((std::shared_ptr<OperatorParamLogicalExpression>&)_parameters[1])->getExpression(),
                query,
                TID_BOOL).getBool();
    }

    ArrayDesc inferSchema(std::vector<ArrayDesc>, std::shared_ptr<Query> query)
    {
        vector<AttributeDesc> attributes(1,AttributeDesc(0,"name",TID_STRING,0,0));

        string const what = getMainParameter(query);
        size_t       size = 0;
        bool         showSys = getShowSysParameter(query);

        if (what == "aggregates") {
            ListAggregatesArrayBuilder builder(
                AggregateLibrary::getInstance()->getNumAggregates(), showSys);
            return builder.getSchema(query);
        } else if (what == "arrays") {
            ListArraysArrayBuilder builder;
            return builder.getSchema(query);
        } else if (what == "operators") {
            vector<string> names;
            OperatorLibrary::getInstance()->getLogicalNames(names, showSys);
            size = names.size();
            attributes.push_back(AttributeDesc(1,"library",TID_STRING,0,0));
            if (showSys) {
                attributes.push_back(AttributeDesc(2,"internal",TID_BOOL,0,0));
            }
        } else if (what == "types") {
            size =  TypeLibrary::typesCount();
            attributes.push_back(AttributeDesc(1,"library",TID_STRING,0,0));
        } else if (what == "functions") {
            funcDescNamesMap& funcs = FunctionLibrary::getInstance()->getFunctions();
            for (funcDescNamesMap::const_iterator i = funcs.begin(); i != funcs.end(); ++i)
            {
                size += i->second.size();
            }
            size += 3; // for hardcoded iif, missing_reason and sizeof
            attributes.push_back(AttributeDesc(1,"profile",      TID_STRING, 0,0));
            attributes.push_back(AttributeDesc(2,"deterministic",TID_BOOL,   0,0));
            attributes.push_back(AttributeDesc(3,"library",      TID_STRING, 0,0));
        } else if (what == "macros") {
            return logicalListMacros(query); // see Parser.h
        } else if (what == "queries") {
            ListQueriesArrayBuilder builder;
            return builder.getSchema(query);
        } else if (what == "instances") {
            std::shared_ptr<const InstanceLiveness> queryLiveness(query->getCoordinatorLiveness());
            size = queryLiveness->getNumInstances();
            attributes.reserve(5);
            attributes.push_back(AttributeDesc(1, "port",         TID_UINT16,0,0));
            attributes.push_back(AttributeDesc(2, "instance_id",  TID_UINT64,0,0));
            attributes.push_back(AttributeDesc(3, "online_since", TID_STRING,0,0));
            attributes.push_back(AttributeDesc(4, "instance_path",TID_STRING,0,0));
        } else if (what == "chunk descriptors") {
            return ListChunkDescriptorsArrayBuilder().getSchema(query);
        } else if (what == "chunk map") {
            return ListChunkMapArrayBuilder().getSchema(query);
        } else if (what == "libraries") {
            return ListLibrariesArrayBuilder().getSchema(query);
        } else if (what == "datastores") {
            return ListDataStoresArrayBuilder().getSchema(query);
        } else if (what == "counters") {
            return ListCounterArrayBuilder().getSchema(query);
        } else if (what == "users") {
            // There is already a name field.
            std::vector<UserDesc> users;
            scidb::security::Communicator::getUsers(users);
            size=users.size();
            attributes.push_back(AttributeDesc(1, "id",  TID_UINT64,0,0));
        } else if (what == "roles") {
            // There is already a name field.
            std::vector<RoleDesc> roles;
            scidb::namespaces::Communicator::getRoles(roles);
            size=roles.size();
        } else if (what == "namespaces") {
            // There is already a name field.
            std::vector<NamespaceDesc> namespaces;
            scidb::namespaces::Communicator::getNamespaces(namespaces);

            // Get the total # of namespaces.
            size = namespaces.size();

            // Now subtract one for each namespace which we do not have permission to list.
            std::string permissions;
            permissions.push_back(scidb::permissions::namespaces::ListArrays);
            for(std::vector<NamespaceDesc>::const_iterator it = namespaces.begin();
                it != namespaces.end();
                ++it)
            {
                try
                {
                    scidb::namespaces::Communicator::checkNamespacePermissions(
                        query->getSession(), (*it), permissions);
                }
                catch(const scidb::Exception& e)
                {
                    size--;
                }
            }

        } else {
            throw USER_QUERY_EXCEPTION(SCIDB_SE_INFER_SCHEMA,
                                       SCIDB_LE_LIST_ERROR1,
                                       _parameters[0]->getParsingContext());
        }

        size_t const chunkInterval = size>0 ? size : 1;

        return ArrayDesc(what, attributes,
                         vector<DimensionDesc>(1, DimensionDesc("No", 0, 0, chunkInterval-1,
                                                                chunkInterval-1, chunkInterval, 0)),
                         defaultPartitioning(),
                         query->getDefaultArrayResidency());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalList, "list")

/****************************************************************************/
}
/****************************************************************************/
