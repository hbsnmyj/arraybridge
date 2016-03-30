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
 * @file NamespaceDesc.h
 *
 * @author mcorbett@paradigm.com
 *
 * @brief A class for describing a namespace.
 *
 */

#ifndef NAMESPACE_DESC_H_
#define NAMESPACE_DESC_H_

#include <string>

namespace scidb
{
/**
 * Contains user specific information
 */
    class NamespaceDesc
    {
    public:
        typedef uint64_t ID;  // Note:  0 is an uninitialized ID
        static const ID UNINITIALIZED_NS_ID = 0;

    private:
        static const ID PUBLIC_NS_ID        = 1;
        std::string     _name;
        ID              _id;

    public:
        /**
         * @brief Default constructor
         */
        NamespaceDesc();

        /**
         * @brief Constructor given a reference name
         * param[in] rName - the name representing this namespace
         */
        NamespaceDesc(
            const std::string & rName);

        /**
         * @brief Constructor given a reference name and id
         * param[in] rName - the name representing this namespace
         * param[in] id - the id representing this namespace
         */
        NamespaceDesc(
            const std::string &     rName,
            ID                      id);

        /**
         * Determine if the namespace Id is initialized
         */
        inline bool isIdInitialized() const
        {
            return (_id!=UNINITIALIZED_NS_ID);
        }

        /**
         * @brief Retrieve the name of the namespace
         * @returns - the name of the namespace
         */
        inline const std::string &getName() const
        {
            return _name;
        }

        /**
         * @brief Sets the name of the namespace
         * param[in] rName - the name representing this namespace
         */
        inline void setName(const std::string &name)
        {
            _name = name;
        }

        /**
         * @brief Get the id for the namespace
         * @returns - Positive value=namespaceId, -1=Not initialized
         */
        inline ID getId() const
        {
            return _id;
        }

        /**
         * @brief Set the id for the namespace
         * @param[in] - The id for the namespace
         */
        inline void setId(ID id)
        {
            _id = id;
        }

		/**
		 * Retrieve the id for the public namespace
		 * @return the value of the id for the public namespace
		 */
		static ID getPublicNamespaceId()
		{
			return PUBLIC_NS_ID;
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
        const NamespaceDesc& lhs,
        const NamespaceDesc& rhs)
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
        const NamespaceDesc& lhs,
        const NamespaceDesc& rhs)
    {
        return (lhs.getName() > rhs.getName());
    }

    /**
     * @brief Comparison operator - "less than or equal to"
     *
     * @param[in] lhs - left hand side of the operator
     * @param[in] rhs - right hand side of the operator
     * @returns if(lhs <= rhs) return true; else return false;
     */
    inline bool operator <= (
        const NamespaceDesc& lhs,
        const NamespaceDesc& rhs)
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
        const NamespaceDesc& lhs,
        const NamespaceDesc& rhs)
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
        const NamespaceDesc& lhs,
        const NamespaceDesc& rhs)
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
        const NamespaceDesc& lhs,
        const NamespaceDesc& rhs)
    {
        return !(lhs == rhs);
    }
} // namespace scidb

#endif /* NAMESPACE_DESC_H_ */
