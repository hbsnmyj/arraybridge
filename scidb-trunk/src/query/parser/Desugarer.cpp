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

/****************************************************************************/

#include <query/Query.h>                                 // For Query
#include <system/SystemCatalog.h>                        // For SystemCatalog
#include <usr_namespace/NamespacesCommunicator.h>
#include <util/arena/ScopedArena.h>                      // For ScopedArena
#include <util/arena/Vector.h>                           // For mgd::vector
#include "AST.h"                                         // For Node etc.

/****************************************************************************/
namespace scidb { namespace parser { namespace {
/****************************************************************************/

/**
 *  @brief      Eliminates syntacic sugar by rewriting derived constructs into
 *              the kernel language.
 *
 *  @details    Currently handles:
 *
 *              - create array() => create_array_as()
 *              - load()         => store(input())
 *
 *  @author     jbell@paradigm4.com.
 */
class Desugarer : public Visitor
{
 public:
                           // Construction
                              Desugarer(Factory&,Log&,const QueryPtr&);

 private:                  // From class Visitor
    virtual void              onApplication(Node*&);

 private:                  // CreateArrayUsing:
            void              onCreateArrayUsing(Node*&);
            name              caGetMi           (Node*,const ArrayDesc&)const;
            Node*             caGetEi           (Node*,size_t)          const;
            Node*             caGetDi           (Node*)                 const;
            Node*             caGetX            (Node*)                 const;
            Node*             caGetXi           (Node*)                 const;
            Node*             caMerge           (cnodes)                const;

 private:                  // Load:
            void              onLoad            (Node*&);

 private:                  // Implementation
            bool              isApplicationOf   (Node*,name)            const;

 private:                  // Representation
            Factory&          _fac;                      // The node factory
            Log&              _log;                      // The error log
            SystemCatalog&    _cat;                      // The system catalog
            ScopedArena       _mem;                      // The local heap
            QueryPtr  const   _qry;                      // The query context
};

/**
 *
 */
Desugarer::Desugarer(Factory& f,Log& l,const QueryPtr& q)
             : _fac(f),
               _log(l),
               _cat(*SystemCatalog::getInstance()),
               _mem("parser::Desugarer"),
               _qry(q)
{}

/**
 *
 */
void Desugarer::onApplication(Node*& pn)
{
    assert(pn!=0 && pn->is(application));                // Validate arguments

 /* Is this a top level application of the 'create_array()' operator to five
    operands? If so, rewrite as a call to 'create_array_as()'...*/

    if (isApplicationOf(pn,"Create_Array") && pn->get(applicationArgOperands)->getSize()==5)
    {
        onCreateArrayUsing(pn);                          // ...rewrite the app
    }

 /* Is this a top level application of the 'load()' operator?...*/

    if (isApplicationOf(pn,"Load"))
    {
        onLoad(pn);                                      // ...rewrite the app
    }

    Visitor::onApplication(pn);                          // Process as before
}

/****************************************************************************/

/**
 *  Translate:
 *
 *      CREATE_ARRAY      (A,<..>[D1=L1:H1,C1,O1, .. , Dn=Ln:Hn,Cn,On],T,L,C)
 *
 *  into:
 *
 *      CREATE_ARRAY_USING(A,<..>[D1=L1:H1,C1,O1, .. , Dn=Ln:Hn,Cn,On],T,
 *          merge(
 *              M1(L,E1,D1),
 *                   ..
 *              Mn(L,En,Dn)),
 *          sys_create_array_aux(L,X,C))
 *
 *  where:
 *
 *      A   = is the name of the new array to be created
 *
 *      T   = is true for a temp array and false otherwise
 *
 *      L   = is an existing 'load array' whose data is to be analysed
 *
 *      Di  = names either an attribute or dimension of L
 *
 *      Mi  = "sys_create_array_att" if Di is an attribute of L
 *            "sys_create_array_dim" if Di is a  dimension of L
 *
 *      Ei  = is a build string of the form:
 *
 *              "[(Ni,Li,Hi,Ci,Oi)]"
 *
 *            where each component is a boolean literal, according to whether
 *            the corresponding component of the target schema is meaningful
 *            (true) or is to be inferred (false).
 *
 *      X   = "string(D1) + '|' .. '|' + string(Dn)"
 *
 *      C   = is the desired logical cell count (default = 1M)
 */
void Desugarer::onCreateArrayUsing(Node*& pn)
{
    assert(pn->is(application));                         // Validate arguments

    location    const w(pn->getWhere());                 // The source location
    Node*       const a(pn->get(applicationArgOperands));// The operand list
    Node*       const D(a->get(listArg1,schemaArgDimensions));
    Node*       const L(a->get(listArg3));               // The load array L
    Node*       const C(a->get(listArg4));               // The cell count C
    ArrayDesc         lArrDesc;                          // The schema for L
    mgd::vector<Node*>v(&_mem);                          // The args to merge

 /* Fetch the array descriptor 'lArrDesc' for the load array 'L', which we will need
    in order to distinguish  whether each target  dimension 'd' is a dimension
    or attribute of the load array 'L'...*/
    assert(_qry);
    std::string arrayName = L->get(referenceArgName,variableArgName)->getString();

    std::string namespaceName = scidb::namespaces::Communicator::getNamespaceName(_qry);
    ArrayID arrayId = _qry->getCatalogVersion(namespaceName, arrayName);
    scidb::namespaces::Communicator::getArrayDesc(namespaceName, arrayName, arrayId, lArrDesc);

 /* For each dimension 'd' of the proposed target schema, construct (abstract
    syntax for) the initial (synthesized) arguments to the 'create_array_as()'
    operator into which we are rewriting this application...*/
    size_t Ni = 0;
    for_each (Node* d,D->getList())
    {
        name  Mi(caGetMi(d,lArrDesc));                   // ...macro name
        Node* Li(_fac.newCopy(L));                       // ...load array
        Node* Ei(caGetEi(d,Ni++));                       // ...build string
        Node* Di(caGetDi(d));                            // ...dimension ref

        v.push_back(_fac.newApp(w,Mi,Li,Ei,Di));         // ...apply macro
    }

 /* Rewrite the original application of 'create_array' as seen by the parser
    into a call to the the 'create_array_as' operator as described above...*/

    pn = _fac.newApp(w,"Create_Array_Using",
            caMerge(v),                                  // Merge the rows
            _fac.newApp(w,"sys_create_array_aux",
                        _fac.newCopy(L),                 // The load array
                        caGetX(D),                       // The distinct string
                        C),                              // The desired cells
            a->get(listArg0),                            // The target name
            a->get(listArg1),                            // The target schema
            a->get(listArg2));                           // The temporary flag
}

/**
 *  Return the name of the system macro we should use to compute statistics of
 *  the load array 'l' for the proposed target dimension'pn'.
 */
name Desugarer::caGetMi(Node* pn,const ArrayDesc& l) const
{
    assert(pn->is(dimension));                           // Validate arguments

    name n(pn->get(dimensionArgName)->getString());      // Get dimesnion name

    for_each (const DimensionDesc& d,l.getDimensions())  // For each dim of l
    {
        if (d.hasNameAndAlias(n))                        // ...do names match?
        {
            return "sys_create_array_dim";               // ....so pn is a dim
        }
    }

    return "sys_create_array_att";                       // Is attribute of l
}

/**
 *  Return a build string of the form:
 *
 *      Ei := "[([Ni,Li,Hi,Ci,Oi)]"
 *
 *  that is suitable as an argument for the 'build' operator, in which each of
 *  the components encodes whether it was specified by the user (true), or is
 *  to be computed from the load array statistics (false).
 */
Node* Desugarer::caGetEi(Node* pn,size_t Ni) const
{
    assert(pn->is(dimension));                           // Validate arguments

    std::ostringstream os;

    os << "[(";
    os << Ni                                      << ',';// The row number
    os << (pn->get(dimensionArgLoBound)      !=0) << ',';// Was lo specified?
    os << (pn->get(dimensionArgHiBound)      !=0) << ',';// Was hi specified?

    const Node* n = pn->get(dimensionArgChunkInterval);
    bool hasInterval = n && !n->is(questionMark) && !n->is(asterisk);
    n = pn->get(dimensionArgChunkInterval);
    bool hasOverlap = n && !n->is(questionMark);

    os << hasInterval << ',';                            // Was ci specified?
    os << hasOverlap;                                    // Was co specified?
    os << ")]";

    return _fac.newString(pn->getWhere(),
           _fac.getArena().strdup(os.str().c_str()));
}

/**
 *  Return (the abstract syntax for) a reference to the attribute or dimension
 *  'pn' of the load array.
 */
Node* Desugarer::caGetDi(Node* pn) const
{
    assert(pn->is(dimension));                           // Validate arguments

    return _fac.newRef(pn->getWhere(),pn->get(dimensionArgName));
}

/**
 *  Return (the abstract syntax for) a scalar expression of the form:
 *
 *      X := "string(D1) + '|' .. '|' + string(Dn)"
 *
 *  where the Di name the dimensions of the load array.
 */
Node* Desugarer::caGetX(Node* pn) const
{
    assert(pn->is(list));                                // Validate arguments

    location   const w(pn->getWhere());
    cnodes     const d(pn->getList());
    cnodes::iterator i(d.begin());
    Node*            p(caGetXi(*i));

    while (++i != d.end())
    {
        p = _fac.newApp   (w,"+",p,
            _fac.newApp   (w,"+",
            _fac.newString(w,"|"),
            caGetXi       (*i)));
    }

    return p;
}

/**
 *  Return (the abstract syntax for) an operator expression of the form:
 *
 *      Merge(n1, n2 ... )
 */
Node* Desugarer::caMerge(cnodes rows) const
{
    assert(!rows.empty());                               // Validate arguments

    Node* n = rows.front();                              // The first row

    if (rows.size() == 1)                                // Is the only one?
    {
        return n;                                        // ...no ned to merge
    }

    return _fac.newApp(n->getWhere(),"Merge",rows);      // Merge all the rows
}

/**
 *
 */
Node* Desugarer::caGetXi(Node* pn) const
{
    assert(pn->is(dimension));                           // Validate arguments

    location const w(pn->getWhere());                    // The source location

    return _fac.newApp(w,"string",_fac.newRef(w,pn->get(dimensionArgName)));
}

/****************************************************************************/

/**
 *  Translate:
 *
 *      LOAD    (array,<a>)
 *
 *  into:
 *
 *      STORE   (INPUT(array,<a>)),array)
 *
 *  where:
 *
 *      array = names the target array to be stored to
 *
 *      a     = whatever remaining arguments the 'input' operator may happen
 *              to accept.
 */
void Desugarer::onLoad(Node*& pn)
{
    assert(pn->is(application));                         // Validate arguments

    location w(pn->getWhere());                          // The source location
    cnodes   a(pn->getList(applicationArgOperands));     // Operands for input

 /* Is the mandatory target array name missing? If so,  the application will
    certainly fail to compile, but we'll leave it to the 'input' operator to
    report the error...*/

    if (a.empty())                                       // No target array?
    {
        pn = _fac.newApp(w,"Input");                     // ...yes, will fail
    }
    else
    {
        pn = _fac.newApp(w,"Store",                      // store(
             _fac.newApp(w,"Input",a),                   //   input(<a>),
             a.front());                                 //   a[0])
    }
}

/****************************************************************************/

/**
 *  Return true if the application node 'pn' represents an application of the
 *  operator-macro-function named 'nm'.
 */
bool Desugarer::isApplicationOf(Node* pn,name nm) const
{
    assert(pn->is(application) && nm!=0);                // Validate arguments

    return strcasecmp(nm,pn->get(applicationArgOperator)->get(variableArgName)->getString())==0;
}

/****************************************************************************/
}
/****************************************************************************/

/**
 *  Traverse the abstract syntax tree in search of derived constructs that are
 *  to be rewritten into the kernel syntax.
 */
Node*& desugar(Factory& f,Log& l,Node*& n,const QueryPtr& q)
{
    return Desugarer(f,l,q)(n);                          // Run the desugarer
}

/****************************************************************************/
}}
/****************************************************************************/
