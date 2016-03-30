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
 * QueryPlan.h
 *
 *  Created on: Dec 24, 2009
 *      Author: Emad, roman.simakov@gmail.com
 */

#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_

#include "QueryPlanFwd.h"

#include <query/Operator.h>
#include <query/OperatorLibrary.h>
#include <util/SerializedPtrConverter.h>

#include <boost/archive/text_iarchive.hpp>

namespace scidb
{

/**
 * Node of logical plan of query. Logical node keeps logical operator to
 * perform inferring result type and validate types.
 */
class LogicalQueryPlanNode
{
public:
    LogicalQueryPlanNode(std::shared_ptr<ParsingContext>  const&,
                         std::shared_ptr<LogicalOperator> const&);

    LogicalQueryPlanNode(std::shared_ptr<ParsingContext>  const&,
                         std::shared_ptr<LogicalOperator> const&,
                         std::vector<std::shared_ptr<LogicalQueryPlanNode> > const &children);

    void addChild(const std::shared_ptr<LogicalQueryPlanNode>& child)
    {
        _childNodes.push_back(child);
    }

    std::shared_ptr<LogicalOperator> getLogicalOperator()
    {
        return _logicalOperator;
    }

    std::vector<std::shared_ptr<LogicalQueryPlanNode> >& getChildren()
    {
        return _childNodes;
    }

    bool isDdl() const
    {
        return _logicalOperator->getProperties().ddl;
    }

    bool supportsTileMode() const
    {
        return _logicalOperator->getProperties().tile;
    }

    std::shared_ptr<ParsingContext> getParsingContext() const
    {
        return _parsingContext;
    }

    const ArrayDesc& inferTypes      (std::shared_ptr<Query>);
    void             inferArrayAccess(std::shared_ptr<Query>&);
    std::string      inferPermissions(std::shared_ptr<Query>&);

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     * @param[in] children print or not children.
     */
    void toString(std::ostream &,int indent = 0,bool children = true) const;

  private:
    std::shared_ptr<LogicalOperator>                    _logicalOperator;
    std::vector<std::shared_ptr<LogicalQueryPlanNode> > _childNodes;
    std::shared_ptr<ParsingContext>                     _parsingContext;
};

typedef std::shared_ptr<PhysicalOperator>      PhysOpPtr;

/*
 *  Currently LogicalQueryPlanNode and PhysicalQueryPlanNode have similar structure.
 *  It may change in future as it needed
 */
class PhysicalQueryPlanNode
    : boost::noncopyable,
      public std::enable_shared_from_this<PhysicalQueryPlanNode>
{
  public:
    PhysicalQueryPlanNode()
    {}

    PhysicalQueryPlanNode(PhysOpPtr const& physicalOperator,
                          bool ddl, bool tile);

    PhysicalQueryPlanNode(PhysOpPtr const& PhysicalOperator,
                          std::vector<PhysNodePtr> const& childNodes,
                          bool ddl, bool tile);

    virtual ~PhysicalQueryPlanNode() {}

    void addChild(const PhysNodePtr & child)
    {
        child->_parent = shared_from_this();
        _childNodes.push_back(child);
    }

    /**
     * Removes node pointed to by targetChild from children.
     * @param targetChild node to remove. Must be in children.
     */
    void removeChild(const PhysNodePtr & targetChild)
    {
        std::vector<PhysNodePtr> newChildren;
        for(size_t i = 0; i < _childNodes.size(); i++)
        {
            if (_childNodes[i] != targetChild)
            {
                newChildren.push_back(_childNodes[i]);
            }
            else
            {
                targetChild->_parent.reset();
            }
        }
        assert(_childNodes.size() > newChildren.size());
        _childNodes = newChildren;
    }

    /**
     * Replace @c targetChild with @c newChild in QueryPlan Tree.
     *
     * The @c newChild (and its children) completely replaces the @c targetChild
     * (and any of its children) in the QueryPlan.
     *
     * NOTE: The @c targetChild and any of its children nodes are removed from
     * the query Plan tree, although the references between the @c targetChild
     * and its children remain.
     *
     * @verbatim
     *    a                                  a
     *   / \                                / \
     *  b  targetChild    newChild  ==>    b   newChild    targetChild
     *     /  \           /  \                 /  \        / \
     *    c    d         e    f               e    f      c   d
     * @endverbatim
     *
     * @param targetChild node to remove. Must be in children.
     * @param newChild node to insert. Must be in children.
     */
    void replaceChild(const PhysNodePtr & targetChild, const PhysNodePtr & newChild)
    {
        bool removed = false;
        std::vector<PhysNodePtr> newChildren;
        for(size_t i = 0; i < _childNodes.size(); i++)
        {
            if (_childNodes[i] != targetChild)
            {
                newChildren.push_back(_childNodes[i]);
            }
            else
            {
                newChild->_parent = shared_from_this();
                newChildren.push_back(newChild);
                removed = true;
            }
        }
        _childNodes = newChildren;
        SCIDB_ASSERT(removed==1);
    }

    /**
     * Supplant the @c targetChild with the @c newChild in the QueryPlan tree.
     *
     * The children of the original child are assigned to the newChild, and
     * the newChild is assigned as the child of original child's parent.
     *
     * @note Any children nodes of the @c newChild Node are removed from the
     * query Plan tree, and the references are removed between @c newChild and
     * any of its original children. Each node is a @c std::shared_ptr so any
     * object (@c targetChild, or original children of @c newChild) where the
     * refcount drops to 0 will be deleted.
     *
     * @verbatim
     *    a                                   a
     *   / \                                 / \
     *  b   targetChild    newChild  ==>    b   newChild       targetChild
     *        /  \          /   \                /  \
     *       c    d        e     f              c    d           e     f
     * @endverbatim
     *
     * @note Make certain to avoid creating cyclic graphs. @c newChild must @b
     * not be a child or descendant of @c targetChild. The main purpose of this
     * function is to insert a newChild (with no children) in the place of
     * targetChild.
     *
     *
     * @param targetChild node to remove. Must be in _childNodes.
     * @param newChild node to supplant the original.
     */
    void supplantChild(const PhysNodePtr& targetChild, const PhysNodePtr& newChild);

    PhysOpPtr getPhysicalOperator()
    {
        return _physicalOperator;
    }

    std::vector<PhysNodePtr>& getChildren()
    {
        return _childNodes;
    }

    bool hasParent() const
    {
        return _parent.lock().get() != NULL;
    }

    void resetParent()
    {
        _parent.reset();
    }

    const PhysNodePtr getParent()
    {
        return _parent.lock();
    }

    bool isDdl() const
    {
        return _ddl;
    }

    bool supportsTileMode() const
    {
        return _tile;
    }

    //TODO: there should be a list of arbitrary markers for optimizer to scratch with.
    //Something like a std::map<std::string, boost::any>.

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     * @param[in] children print or not children.
     */
    void toString(std::ostream &str, int indent = 0, bool children = true) const;

    /**
     * Retrieve an ordered list of the shapes of the arrays to be input to this
     * node.
     */
    std::vector<ArrayDesc> getChildSchemas() const
    {
        std::vector<ArrayDesc> result;
        for (size_t i = 0, count = _childNodes.size(); i < count; ++i)
        {
            PhysNodePtr const& child = _childNodes[i];
            result.push_back(child->getPhysicalOperator()->getSchema());
        }
        return result;
    }

    /**
     * Determine if this node is for the PhysicalRepart operator.
     * @return true if physicalOperator is PhysicalRepart. False otherwise.
     */
    bool isRepartNode() const
    {
        return _physicalOperator.get() != NULL &&
               _physicalOperator->getPhysicalName() == "physicalRepart";
    }

    /**
     * Determine if this node is for the PhysicalRedimension operator.
     * @return true if physicalOperator is PhysicalRedimension. False otherwise.
     */
    bool isRedimensionNode() const
    {
        return _physicalOperator.get() != NULL &&
               _physicalOperator->getPhysicalName() == "PhysicalRedimension";
    }

    /**
     * Determine if this node is for the PhysicalInput operator.
     * @return true if physicalOperator is PhysicalInput. False otherwise.
     */
    bool isInputNode() const
    {
        return _physicalOperator.get() != NULL &&
               _physicalOperator->getPhysicalName() == "impl_input";
    }

    /**
     * Determine if this node is for the PhysicalSG operator.
     * @return true if physicalOperator is PhysicalSG. False otherwise.
     */
    bool isSgNode() const
    {
        return _physicalOperator.get() != NULL &&
                _physicalOperator->getPhysicalName() == "impl_sg";
    }

    bool isStoringSg() const;

    /**
     * Extract the array name paramenter from SG operator parameters
     * @return array name, empty if the paramenter is not present or empty
     */
    static std::string getSgArrayName(const PhysicalOperator::Parameters& sgParameters);

    /**
     * Extract the isStrict paramenter from the redimension operator parameters
     * @return false if the isStrict parameter is specified and is equal to false; true otherwise
     */
    static bool getRedimensionIsStrict(const PhysicalOperator::Parameters& redimParameters);

    /**
     * Extract the isStrict paramenter from the input operator parameters
     * @return false if the isStrict parameter is specified and is equal to false; true otherwise
     */
    static bool getInputIsStrict(const PhysicalOperator::Parameters& inputParameters);

    /**
     * @return the sgMovable flag
     */
    bool isSgMovable() const
    {
        return _isSgMovable;
    }

    /**
     * Set the sgMovable flag
     * @param value value to set
     */
    void setSgMovable(bool value)
    {
        _isSgMovable = value;
    }

    /**
     * @return the sgOffsetable flag
     */
    bool isSgOffsetable() const
    {
        return _isSgOffsetable;
    }

    /**
     * Set the sgOffsetable flag
     * @param value value to set
     */
    void setSgOffsetable(bool value)
    {
        _isSgOffsetable = value;
    }

    /**
     * Delegator to physicalOperator.
     */
    bool changesDistribution() const
    {
        return _physicalOperator->changesDistribution(getChildSchemas());
    }

    /**
     * Delegator to physicalOperator.
     */
    bool outputFullChunks() const
    {
        return _physicalOperator->outputFullChunks(getChildSchemas());
    }

    /**
      * [Optimizer API] Determine if the output chunks
      * of this subtree will be completely filled.
      * Optimizer may insert SG operations for subtrees
      * that do not provide full chunks.
      * @return true if output chunking is guraranteed full, false otherwise.
      */
    bool subTreeOutputFullChunks() const
    {
        if (isSgNode())
        {
            return true;
        }
        for (size_t i = 0, count = _childNodes.size(); i< count; ++i)
        {
            if (!_childNodes[i]->subTreeOutputFullChunks())
            {
                return false;
            }
        }
        return _physicalOperator->outputFullChunks(getChildSchemas());
    }

    DistributionRequirement getDistributionRequirement() const
    {
        return _physicalOperator->getDistributionRequirement(getChildSchemas());
    }

    bool needsSpecificDistribution() const
    {
        return getDistributionRequirement().getReqType()== DistributionRequirement::SpecificAnyOrder;
    }

    /**
     * @return the number of attributes emitted by the node.
     */
    double getDataWidth()
    {
        return _boundaries.getSizeEstimateBytes(getPhysicalOperator()->getSchema());
    }

    /**
     * @return stats about distribution of node output
     */
    const RedistributeContext& getDistribution() const
    {
        return _distribution;
    }

    /**
     * Calculate information about distribution of node output, using
     * the distribution stats of the child nodes, plus
     * the data provided from the PhysicalOperator. Sets distribution
     * stats of node to the result.
     * @param prev distribution stats of previous node's output
     * @return new distribution stats for this node's output
     */
    const RedistributeContext& inferDistribution ()
    {
        std::vector<RedistributeContext> childDistros;
        for (size_t i =0; i<_childNodes.size(); i++)
        {
            childDistros.push_back(_childNodes[i]->getDistribution());
        }
        _distribution = _physicalOperator->getOutputDistribution(childDistros, getChildSchemas());
        return _distribution;
    }

    //I see an STL pattern coming soon...
    const PhysicalBoundaries& getBoundaries() const
    {
        return _boundaries;
    }

    const PhysicalBoundaries& inferBoundaries()
    {
        std::vector<PhysicalBoundaries> childBoundaries;
        for (size_t i =0; i<_childNodes.size(); i++)
        {
            childBoundaries.push_back(_childNodes[i]->getBoundaries());
        }
        _boundaries = _physicalOperator->getOutputBoundaries(childBoundaries, getChildSchemas());
        return _boundaries;
    }

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version);

private:
    PhysOpPtr _physicalOperator;

    std::vector< PhysNodePtr > _childNodes;
    std::weak_ptr <PhysicalQueryPlanNode> _parent;

    bool _ddl;
    bool _tile;

    bool _isSgMovable;
    bool _isSgOffsetable;

    RedistributeContext _distribution;
    PhysicalBoundaries _boundaries;
};

/**
 * A text_iarchive class that helps de-serializing shared_ptr objects,
 * when a worker instance calls QueryProcessorImpl::parsePhysical().
 * The idea is that multiple deserialized raw pointers are equal will produce the same shared_ptr.
*/
class TextIArchiveQueryPlan: public boost::archive::text_iarchive
{
public:
    TextIArchiveQueryPlan(std::istream & is_, unsigned int flags = 0)
    : boost::archive::text_iarchive(is_, flags)
    {}

    struct SerializationHelper
    {
        SerializedPtrConverter<PhysicalQueryPlanNode> _nodes;
        SerializedPtrConverter<OperatorParam> _params;

        void clear()
        {
            _nodes.clear();
            _params.clear();
        }
    };

    SerializationHelper _helper;
};

template<class Archive>
void PhysicalQueryPlanNode::serialize(Archive& ar, const unsigned int version)
{
    // ar & _childNodes;
    if (Archive::is_loading::value) {
        TextIArchiveQueryPlan::SerializationHelper& helper = dynamic_cast<TextIArchiveQueryPlan&>(ar)._helper;
        size_t size = 0;
        ar & size;
        _childNodes.resize(size);
        for (size_t i=0; i<size; ++i) {
            PhysicalQueryPlanNode* n;
            ar & n;
            _childNodes[i] = helper._nodes.getSharedPtr(n);
        }
    }
    else {
        size_t size = _childNodes.size();
        ar & size;
        for (size_t i=0; i<size; ++i) {
            PhysicalQueryPlanNode* n = _childNodes[i].get();
            ar & n;
        }
    }

    ar & _ddl;
    ar & _tile;
    ar & _isSgMovable;
    ar & _isSgOffsetable;
    //We don't need distribution or sizing info - they are used for optimization only.

    /*
     * We not serializing whole operator object, to simplify user's life and get rid work serialization
     * user classes and inherited SciDB classes. Instead this we serializing operator name and
     * its parameters, and later construct operator by hand
     */
    if (Archive::is_loading::value)
    {
        TextIArchiveQueryPlan::SerializationHelper& helper = dynamic_cast<TextIArchiveQueryPlan&>(ar)._helper;
        std::string logicalName;
        std::string physicalName;
        std::string controlCookie;
        PhysicalOperator::Parameters parameters;
        ArrayDesc schema;

        ar & logicalName;
        ar & physicalName;
        ar & controlCookie;

        // ar & parameters;
        size_t size = 0;
        ar & size;
        parameters.resize(size);
        for (size_t i=0; i<size; ++i) {
            OperatorParam* op;
            ar & op;
            parameters[i] = helper._params.getSharedPtr(op);
        }
        ar & schema;

        _physicalOperator = OperatorLibrary::getInstance()->createPhysicalOperator(
                    logicalName, physicalName, parameters, schema);
        _physicalOperator->setTileMode(_tile);
        _physicalOperator->setControlCookie(controlCookie);
    }
    else
    {
        std::string logicalName = _physicalOperator->getLogicalName();
        std::string physicalName = _physicalOperator->getPhysicalName();
        std::string controlCookie = _physicalOperator->getControlCookie();
        PhysicalOperator::Parameters parameters = _physicalOperator->getParameters();
        ArrayDesc schema = _physicalOperator->getSchema();

        ar & logicalName;
        ar & physicalName;
        ar & controlCookie;

        //ar & parameters;
        size_t size = parameters.size();
        ar & size;
        for (size_t i=0; i<size; ++i) {
            OperatorParam* op = parameters[i].get();
            ar & op;
        }

        ar & schema;
    }
}

/**
 * The LogicalPlan represents result of parsing query and is used for validation query.
 * It's input data for optimization and generation physical plan.
 */
class LogicalPlan
{
public:
    LogicalPlan(const std::shared_ptr<LogicalQueryPlanNode>& root);

    std::shared_ptr<LogicalQueryPlanNode> getRoot()
    {
        return _root;
    }

    void setRoot(const std::shared_ptr<LogicalQueryPlanNode>& root)
    {
        _root = root;
    }

    const ArrayDesc& inferTypes(std::shared_ptr< Query>& query)
    {
        return _root->inferTypes(query);
    }

    void inferArrayAccess(std::shared_ptr<Query>& query)
    {
        return _root->inferArrayAccess(query);
    }

    std::string inferPermissions(std::shared_ptr<Query>& query)
    {
        return _root->inferPermissions(query);
    }

	/**
	 * Retrieve a human-readable description.
	 * Append a human-readable description of this onto str. Description takes up
	 * one or more lines. Append indent spacer characters to the beginning of
	 * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     * @param[in] children print or not children.
     */
    void toString(std::ostream &str, int indent = 0, bool children = true) const;

private:
    std::shared_ptr<LogicalQueryPlanNode> _root;
};

/**
 * The PhysicalPlan is produced by Optimizer or in simple cases directly by query processor (DDL).
 * It has ready to execution operator nodes and will be passed to an executor.
 */
class PhysicalPlan
{
public:
    PhysicalPlan(const std::shared_ptr<PhysicalQueryPlanNode>& root);

    std::shared_ptr<PhysicalQueryPlanNode> getRoot()
    {
        return _root;
    }

    bool empty() const
    {
        return _root == std::shared_ptr<PhysicalQueryPlanNode>();    // _root is NULL
    }

    bool isDdl() const
    {
    	assert(!empty());
    	return _root->isDdl();
    }

    bool supportsTileMode() const
    {
    	assert(!empty());
    	return _root->supportsTileMode();
    }

	void setRoot(const std::shared_ptr<PhysicalQueryPlanNode>& root)
	{
		_root = root;
	}

    /**
     * Retrieve a human-readable description.
     * Append a human-readable description of this onto str. Description takes up
     * one or more lines. Append indent spacer characters to the beginning of
     * each line. Call toString on interesting children. Terminate with newline.
     * @param[out] stream to write to
     * @param[in] indent number of spacer characters to start every line with.
     * @param[in] children print or not children.
     */
    void toString(std::ostream &out, int indent = 0, bool children = true) const;

private:
    std::shared_ptr<PhysicalQueryPlanNode> _root;
};

} // namespace


#endif /* QUERYPLAN_H_ */
