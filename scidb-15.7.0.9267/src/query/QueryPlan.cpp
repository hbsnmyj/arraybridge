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
 * @file QueryTree.cpp
 *
 * @author roman.simakov@gmail.com
 */

#include <boost/foreach.hpp>
#include <memory>

#include <log4cxx/logger.h>
#include <query/QueryPlanUtilites.h>
#include <query/QueryPlan.h>
#include <query/LogicalExpression.h>

using namespace boost;
using namespace std;

namespace scidb
{

// Logger for query processor. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.processor"));

// LogicalQueryPlanNode
LogicalQueryPlanNode::LogicalQueryPlanNode(
	const std::shared_ptr<ParsingContext>& parsingContext,
	const std::shared_ptr<LogicalOperator>& logicalOperator):
	_logicalOperator(logicalOperator),
	_parsingContext(parsingContext)
{

}

LogicalQueryPlanNode::LogicalQueryPlanNode(
	const std::shared_ptr<ParsingContext>& parsingContext,
	const std::shared_ptr<LogicalOperator>& logicalOperator,
	const std::vector<std::shared_ptr<LogicalQueryPlanNode> > &childNodes):
	_logicalOperator(logicalOperator),
	_childNodes(childNodes),
	_parsingContext(parsingContext)
{
}

const ArrayDesc& LogicalQueryPlanNode::inferTypes(std::shared_ptr< Query> query)
{
    std::vector<ArrayDesc> inputSchemas;
    ArrayDesc outputSchema;
    for (size_t i=0, end=_childNodes.size(); i<end; i++)
    {
        inputSchemas.push_back(_childNodes[i]->inferTypes(query));
    }
    outputSchema = _logicalOperator->inferSchema(inputSchemas, query);
    //FIXME: May be cover inferSchema method with another one and assign alias there?
    if (!_logicalOperator->getAliasName().empty())
    {
        outputSchema.addAlias(_logicalOperator->getAliasName());
    }
    _logicalOperator->setSchema(outputSchema);
    LOG4CXX_DEBUG(logger, "Inferred schema for operator " <<
                  _logicalOperator->getLogicalName() << ": " << outputSchema);
    return _logicalOperator->getSchema();
}

void LogicalQueryPlanNode::inferArrayAccess(std::shared_ptr<Query>& query)
{
    //XXX TODO: consider non-recursive implementation
    for (size_t i=0, end=_childNodes.size(); i<end; i++)
    {
        _childNodes[i]->inferArrayAccess(query);
    }
    assert(_logicalOperator);
    _logicalOperator->inferArrayAccess(query);
}

void LogicalQueryPlanNode::toString(std::ostream &out, int indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[lInstance] children "<<_childNodes.size()<<"\n";
    _logicalOperator->toString(out,indent+1);

    if (children) {
        for (size_t i = 0; i< _childNodes.size(); i++)
        {
            _childNodes[i]->toString(out, indent+1);
        }
    }
}

PhysicalQueryPlanNode::PhysicalQueryPlanNode(const std::shared_ptr<PhysicalOperator>& physicalOperator,
                                             bool ddl, bool tile)
: _physicalOperator(physicalOperator),
  _parent(), _ddl(ddl), _tile(tile), _isSgMovable(true), _isSgOffsetable(true), _distribution()
{
}

PhysicalQueryPlanNode::PhysicalQueryPlanNode(const std::shared_ptr<PhysicalOperator>& physicalOperator,
		const std::vector<std::shared_ptr<PhysicalQueryPlanNode> > &childNodes,
                                             bool ddl, bool tile):
	_physicalOperator(physicalOperator),
	_childNodes(childNodes),
    _parent(), _ddl(ddl), _tile(tile), _isSgMovable(true), _isSgOffsetable(true), _distribution()
{
}

void PhysicalQueryPlanNode::toString(std::ostream &out, int indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);

    out<<"[pNode] "<<_physicalOperator->getPhysicalName()<<" ddl "<<isDdl()<<" tile "<<supportsTileMode()<<" children "<<_childNodes.size()<<"\n";
    _physicalOperator->toString(out,indent+1);

    if (children) {
        out << prefix(' ');
        out << "output full chunks: ";
        out << (outputFullChunks() ? "yes" : "no");
        out << "\n";
        out << prefix(' ');
        out << "changes dstribution: ";
        out << (changesDistribution() ? "yes" : "no");
        out << "\n";
    }

    out << prefix(' ');
    out<<"props sgm "<<_isSgMovable<<" sgo "<<_isSgOffsetable<<"\n";
    out << prefix(' ');
    out<<"diout "<<_distribution<<"\n";
    const ArrayDesc& schema = _physicalOperator->getSchema();
    out << prefix(' ');
    out<<"bound "<<_boundaries
      <<" cells "<<_boundaries.getNumCells();

    if (_boundaries.getStartCoords().size() == schema.getDimensions().size()) {
        out  <<" chunks "<<_boundaries.getNumChunks(schema.getDimensions())
            <<" est_bytes "<<_boundaries.getSizeEstimateBytes(schema)
           <<"\n";
    }
    else {
        out <<" [improperly initialized]\n";
    }

    if (children) {
        for (size_t i = 0; i< _childNodes.size(); i++) {
            _childNodes[i]->toString(out, indent+1);
        }
    }
}

bool PhysicalQueryPlanNode::isStoringSg() const
{
    if ( isSgNode() ) {
        return (!getSgArrayName(_physicalOperator->getParameters()).empty());
    }
    return false;
}

string PhysicalQueryPlanNode::getSgArrayName(const PhysicalOperator::Parameters& sgParameters)
{
    std::string arrayName;
    if (sgParameters.size() >= 3) {
        arrayName = static_cast<OperatorParamReference*>(sgParameters[2].get())->getObjectName();
    }
    return arrayName;
}

bool PhysicalQueryPlanNode::getRedimensionIsStrict(const PhysicalOperator::Parameters& redimParameters)
{
    bool isStrict = true;
    if (redimParameters.size() == 2 &&
        redimParameters[1]->getParamType() == scidb::PARAM_PHYSICAL_EXPRESSION) {
        OperatorParamPhysicalExpression* paramExpr = static_cast<OperatorParamPhysicalExpression*>(redimParameters[1].get());
        SCIDB_ASSERT(paramExpr->isConstant());
        isStrict = paramExpr->getExpression()->evaluate().getBool();
    }
    return isStrict;
}

bool PhysicalQueryPlanNode::getInputIsStrict(const PhysicalOperator::Parameters& inputParameters)
{
    bool isStrict = true;
    if (inputParameters.size() == 6 &&
        inputParameters[5]->getParamType() == scidb::PARAM_PHYSICAL_EXPRESSION)
    {
        OperatorParamPhysicalExpression* paramExpr =
        static_cast<OperatorParamPhysicalExpression*>(inputParameters[5].get());
        SCIDB_ASSERT(paramExpr->isConstant());
        isStrict = paramExpr->getExpression()->evaluate().getBool();
    }
    else if (inputParameters.size() == 7)
    {
        ASSERT_EXCEPTION((inputParameters[6]->getParamType() == scidb::PARAM_PHYSICAL_EXPRESSION),
                         "Invalid input() parameters 6");

        OperatorParamPhysicalExpression* paramExpr =
        static_cast<OperatorParamPhysicalExpression*>(inputParameters[6].get());
        SCIDB_ASSERT(paramExpr->isConstant());
        isStrict = paramExpr->getExpression()->evaluate().getBool();
    }
    return isStrict;
}

// LogicalPlan
LogicalPlan::LogicalPlan(const std::shared_ptr<LogicalQueryPlanNode>& root):
        _root(root)
{

}

void LogicalPlan::toString(std::ostream &out, int indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[lPlan]:\n";
    _root->toString(out, indent+1, children);
}

// PhysicalPlan
PhysicalPlan::PhysicalPlan(const std::shared_ptr<PhysicalQueryPlanNode>& root):
        _root(root)
{

}

void PhysicalPlan::toString(std::ostream &out, int const indent, bool children) const
{
    Indent prefix(indent);
    out << prefix('>', false);
    out << "[pPlan]:";
    if (_root.get() != NULL)
    {
        out << "\n";
        _root->toString(out, indent+1, children);
    }
    else
    {
        out << "[NULL]\n";
    }
}

} // namespace
