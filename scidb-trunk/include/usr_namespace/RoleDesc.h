/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2010-2015 SciDB, Inc.
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
 * @file RoleDesc.h
 *
 * @author mcorbett@paradigm.com
 *
 * @brief A class for describing a role.
 *
 */

#ifndef _ROLE_DESC_H_
#define _ROLE_DESC_H_

#include <string>
#include <vector>

namespace scidb
{
    /**
     * Contains role specific information
     */
    class RoleDesc
    {
    public:
        typedef uint64_t ID;  // Note:  0 is an invalid ID


    private:
        std::string     _name;


    public:
        /**
         * @brief Default constructor
         */
        RoleDesc();

        /**
         * @brief Constructor given a reference name
         * param[in] rName - the name representing this role
         */
        RoleDesc(const std::string &rName);

        /**
         * @brief Retrieve the name of the role
         * @returns - the name of the role
         */
        inline const std::string &getName() const
        {
            return _name;
        }

        /**
         * @brief Sets the name of the role
         * param[in] rName - the name representing this role
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
        const RoleDesc& lhs,
        const RoleDesc& rhs)
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
        const RoleDesc& lhs,
        const RoleDesc& rhs)
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
        const RoleDesc& lhs,
        const RoleDesc& rhs)
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
        const RoleDesc& lhs,
        const RoleDesc& rhs)
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
        const RoleDesc& lhs,
        const RoleDesc& rhs)
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
        const RoleDesc& lhs,
        const RoleDesc& rhs)
    {
        return !(lhs == rhs);
    }
} // namespace scidb

#endif /* _ROLE_DESC_H_ */
