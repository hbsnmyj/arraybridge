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
 * @file NamespaceObject.h
 *
 * @author mcorbett@paradigm.com
 *
 * @brief A class for describing a namespace.
 *
 */

#ifndef NAMESPACE_OBJECT_H_
#define NAMESPACE_OBJECT_H_

#include <string>

namespace scidb
{
/**
 * Contains user specific information
 */
    class NamespaceObject
    {
    private:
        std::string    _name;

    public:
        /**
         * Default constructor
         */
        NamespaceObject();

        /**
         * Constructor given a reference name
         * param[in] rName - the name representing this namespace
         */
        NamespaceObject(
            const std::string & rName);


		/**
         * Retrieve the name of the namespace object
         * @returns - the name of the namespace object
         */
        inline const std::string &getName() const
        {
            return _name;
        }


        /**
         * Sets the name of the namespace object
         * param[in] rName - the name representing this namespace object
         */
        inline void setName(const std::string &name)
        {
            _name = name;
        }
    };

	/**
     * @brief Comparison operator - "less than"
     *
     * @param[in] lhs - left hand side of the operator
     * @param[in] rhs - right hand side of the operator
     * @returns if(lhs < rhs) return true; else return false;
     */
    inline bool operator < (
        const NamespaceObject& lhs,
        const NamespaceObject& rhs)
    {
        return (lhs.getName() < rhs.getName());
    }

	/**
     * @brief Comparison operator - "greater than"
     *
     * @param[in] lhs - left hand side of the operator
     * @param[in] rhs - right hand side of the operator
     * @returns if(lhs > rhs) return true; else return false;
     */
    inline bool operator > (
        const NamespaceObject& lhs,
        const NamespaceObject& rhs)
    {
        return lhs > rhs;
    }

	/**
     * @brief Comparison operator - "less than or equal to"
     *
     * @param[in] lhs - left hand side of the operator
     * @param[in] rhs - right hand side of the operator
     * @returns if(lhs <= rhs) return true; else return false;
     */
    inline bool operator <= (
        const NamespaceObject& lhs,
        const NamespaceObject& rhs)
    {
        return !(lhs > rhs);
    }

	/**
     * @brief Comparison operator - "greater than or equal to"
     *
     * @param[in] lhs - left hand side of the operator
     * @param[in] rhs - right hand side of the operator
     * @returns if(lhs >= rhs) return true; else return false;
     */
    inline bool operator >= (
        const NamespaceObject& lhs,
        const NamespaceObject& rhs)
    {
        return !(lhs < rhs);
    }

	/**
     * @brief Comparison operator - "equality"
     *
     * @param[in] lhs - left hand side of the operator
     * @param[in] rhs - right hand side of the operator
     * @returns if(lhs == rhs) return true; else return false;
     */
    inline bool operator == (
        const NamespaceObject& lhs,
        const NamespaceObject& rhs)
    {
        return (lhs.getName() == rhs.getName());
    }

	/**
     * @brief Comparison operator - "inequality"
     *
     * @param[in] lhs - left hand side of the operator
     * @param[in] rhs - right hand side of the operator
     * @returns if(lhs != rhs) return true; else return false;
     */
    inline bool operator != (
        const NamespaceObject& lhs,
        const NamespaceObject& rhs)
    {
        return !(lhs == rhs);
    }
} // namespace scidb

#endif /* NAMESPACE_OBJECT_H_ */
