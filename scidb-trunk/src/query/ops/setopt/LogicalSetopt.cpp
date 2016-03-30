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
 * LogicalSetopt.cpp
 *
 *  Created on: Apr 17, 2010
 *      Author: Knizhnik
 */

#include <query/Operator.h>
#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <usr_namespace/Permissions.h>


using namespace std;

namespace scidb {

/**
 * @brief The operator: _setopt().
 *
 * @par Synopsis:
 *   _setopt( option [, newValue] )
 *
 * @par Summary:
 *   Gets/Sets a config option at runtime.
 *
 * @par Input:
 *   - option: the config option.
 *   - newValue: an optional new value for the config option. If provided, the option is set. Either way, the option value(s) is returned.
 *
 * @par Output array:
 *        <
 *   <br>   old: string
 *   <br>   new: string, if newValue is provided
 *   <br> >
 *   <br> [
 *   <br>   No: start=0, end=#instances-1, chunk interval=1
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
class LogicalSetopt: public LogicalOperator
{
public:
    LogicalSetopt(const string& logicalName, const std::string& alias)
    : LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_CONSTANT("string");
        ADD_PARAM_VARIES();
    }

    std::vector<std::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<std::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() == 1) {
            res.push_back(PARAM_CONSTANT("string"));
        }
        return res;
    }

    std::string inferPermissions(std::shared_ptr<Query>& query)
    {
        // Ensure we have the proper permissions
        assert(_parameters.size() >= 1 && _parameters.size() <= 2);

        std::string permissions;
        if (_parameters.size() == 2)
        {
            permissions.push_back(scidb::permissions::Administrate);
        }

        return permissions;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
    {
        assert(schemas.size() == 0);
        assert(_parameters.size() >= 1 && _parameters.size() <= 2);

        if (_parameters.size() == 2)
        {
            std::string whatToChange = evaluate(
                reinterpret_cast<std::shared_ptr<OperatorParamLogicalExpression> &>(
                    _parameters[0])->getExpression(), query, TID_STRING).getString();
            if( whatToChange.compare("security") == 0)
            {
                throw USER_EXCEPTION(SCIDB_SE_INFER_SCHEMA, SCIDB_LE_OPTION_NOT_ALLOWED)
                    << whatToChange;
            }
        }

        vector<AttributeDesc> attributes;
        attributes.push_back( AttributeDesc((AttributeID)0, "old",  TID_STRING, 0, 0));
        if (_parameters.size() == 2) {
            attributes.push_back(AttributeDesc((AttributeID)1, "new",  TID_STRING, 0, 0));
        }
        vector<DimensionDesc> dimensions(1);
        const size_t nInstances = query->getInstancesCount();
        const size_t end = nInstances>0 ? nInstances-1 : 0;
        dimensions[0] = DimensionDesc("No", 0, 0, end, end, 1, 0);
        return ArrayDesc("Option", attributes, dimensions,
                         defaultPartitioning(),
                         query->getDefaultArrayResidency());
    }
};

DECLARE_LOGICAL_OPERATOR_FACTORY(LogicalSetopt, "_setopt")


}  // namespace scidb
