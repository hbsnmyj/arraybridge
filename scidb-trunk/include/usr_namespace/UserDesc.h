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
 * @file UserDesc.h
 *
 * @author mcorbett@paradigm.com
 *
 * @brief A class for describing a user.
 *
 */

#ifndef USER_DESC_H_
#define USER_DESC_H_

#include <string>

namespace scidb
{

    /**
     * Contains user specific information
     */
    class UserDesc
    {
    public:
        typedef uint64_t ID;  //Note:  0 is an invalid ID
        static inline ID rootId() { return 1; }
        static inline const char *rootName() { return "root"; }

        static bool isRoot(UserDesc::ID id) { return id == rootId(); }
        static bool isRoot(const std::string &rName) { return (0 == rName.compare(rootName())); }

    private:
        std::string _name;
        std::string _password;
        std::string _passwordMethod;
        ID          _id;

    public:
        /**
         * @brief Default constructor
         */
        UserDesc();

        /**
         * @brief Constructor given a reference name, password, & method
         * @param[in] rName - the name representing this role
         * @param[in] rPassword - the user's password
         * @param[in] rPasswordMethod - the crypto algorithm performed on the passwrod
         * @param[in] id - the identification number of the user
         */
         UserDesc(
            const std::string &rName,
            const std::string &rPassword = "",
            const std::string &rPasswordMethod = "raw",
            const ID &id = 0 );


        /**
         * @brief Retrieves the user's name
         * @returns - the user's name
         */
        inline const std::string &getName() const
        {
            return _name;
        }

		/**
         * @brief Retrieve the user's password
         * @returns - the user's password
         */
        inline const std::string &getPassword() const
        {
            return _password;
        }

		/**
         * @brief Retrieve the user's password crypto method
         * @returns - the user's password crypto method
         */
        inline const std::string &getPasswordMethod() const
        {
            return _passwordMethod;
        }

        /**
         * @brief Retrieves the user's identification number
         * @returns - the user's id
         */
        inline const ID &getId() const
        {
            return _id;
        }

		/**
         * @brief Sets the user's name
         * @param[in] name - the user's name
         */
        inline void setName(const std::string &name)
        {
            _name = name;
        }

		/**
         * @brief Sets the user's password
         * @param[in] password - the user's password
         */
        inline void setPassword(const std::string &password)
        {
            _password = password;
        }

        /**
         * @brief Sets the user's password crypto method
         * @param[in] passwordMethod - the user's password crypto method
         */
        inline void setPasswordMethod(const std::string &passwordMethod)
        {
            _passwordMethod = passwordMethod;
        }

        /**
         * @brief Sets the user's identification number
         */
        inline void setId(const ID &id)
        {
            _id = id;
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
        const UserDesc& lhs,
        const UserDesc& rhs)
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
        const UserDesc& lhs,
        const UserDesc& rhs)
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
        const UserDesc& lhs,
        const UserDesc& rhs)
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
        const UserDesc& lhs,
        const UserDesc& rhs)
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
        const UserDesc& lhs,
        const UserDesc& rhs)
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
        const UserDesc& lhs,
        const UserDesc& rhs)
    {
        return !(lhs == rhs);
    }
} // namespace scidb

#endif /* USER_DESC_H_ */
