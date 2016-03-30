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

std::string LogicalQueryPlanNode::inferPermissions(std::shared_ptr<Query>& query)
{
    std::stringstream ss;

    // Consider non-recursive implementation
    for (size_t i=0, end=_childNodes.size(); i<end; i++)
    {
        ss << _childNodes[i]->inferPermissions(query);
    }
    assert(_logicalOperator);
    ss << _logicalOperator->inferPermissions(query);
    std::string permissions = ss.str();

    // Remove duplicates
    std::map<char, int> hashMap;

    std::string::const_iterator itStr;
    for (   itStr = permissions.begin();
            itStr != permissions.end();
            ++itStr)
    {
        // Add/overwrite the node in the hashMap
        hashMap[*itStr] = 1;
    }

    std::map<char, int>::const_iterator itHashMap;
    std::string response;
    for (   itHashMap = hashMap.begin();
            itHashMap != hashMap.end();
            ++itHashMap)
    {
        // Append each character from the hashMap
        response += itHashMap->first;
    }

    return response;
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
        out  << " chunks ";
        try {
            uint64_t n = _boundaries.getNumChunks(schema.getDimensions());
            out << n;
        } catch (PhysicalBoundaries::UnknownChunkIntervalException&) {
            out << '?';
        }
        out << " est_bytes " << _boundaries.getSizeEstimateBytes(schema)
            << '\n';
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

void
PhysicalQueryPlanNode::supplantChild(const PhysNodePtr& targetChild,
                                     const PhysNodePtr& newChild)
{
    assert(newChild);
    assert(targetChild);
    assert(newChild.get() != this);
    int removed = 0;
    std::vector<PhysNodePtr> newChildren;

    if (logger->isTraceEnabled()) {
        std::ostringstream os;
        os << "Supplanting targetChild Node:\n";
        targetChild->toString(os, 0 /*indent*/,false /*children*/);
        os << "\nwith\n";
        newChild->toString(os, 0 /*indent*/,false /*children*/);
        LOG4CXX_TRACE(logger, os.str());
    }

    for(auto &child : _childNodes) {
        if (child != targetChild) {
            newChildren.push_back(child);
        }
        else {
            // Set the parent of the newChild to this node.
            newChild->_parent = shared_from_this();

            // NOTE: Any existing children of the newChild are removed from the
            // Query Plan.
            if ((newChild->_childNodes).size() > 0) {
                LOG4CXX_INFO(logger,
                             "Child nodes of supplanting node are being removed from the tree.");
            }

            // Re-parent the children of the targetChild to the newChild
            newChild->_childNodes.swap(targetChild->_childNodes);
            for (auto grandchild : newChild -> _childNodes) {
                assert(grandchild != newChild);
                grandchild->_parent = newChild;
            }

            // Remove any references to the children from the targetChild
            targetChild->_childNodes.clear();
            targetChild->resetParent();

            // Add the newChild to this node
            newChildren.push_back(newChild);
            ++removed;
        }
    }
    _childNodes.swap(newChildren);

    if (logger->isTraceEnabled()) {
        std::ostringstream os;
        newChild->toString(os);
        LOG4CXX_TRACE(logger, "New Node subplan:\n"
                      << os.str());
    }

    SCIDB_ASSERT(removed==1);
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
