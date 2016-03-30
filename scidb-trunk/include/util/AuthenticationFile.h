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
 * @file AuthenticationFile.h
 *
 * @brief Utility for retrieving authentication information from an authentication file.
 *
 * @author Marty Corbett <mcorbett@paradigm4.com>
 */

#ifndef AUTHENTICATION_FILE_H_
#define AUTHENTICATION_FILE_H_

#include <stdlib.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/regex.hpp>
#include <string>

namespace scidb
{
    /**
     * A class for accessing an authentication file similiar in structure to the config.ini file.
     * Currently this file will contain the username and password.  But may grow to include other
     * authentication specific settings.
     */
    class AuthenticationFile
    {
    private:
        boost::property_tree::ptree     _pt;

        bool checkAgainstRegularExpression(const std::string &str) const
        {
            static boost::regex  re("[A-Za-z0-9,-_./]+");
            boost::cmatch what;

            if(boost::regex_match(str.c_str(), what, re))
            {
                return true;
            }

            return false;
        }

    public:
        AuthenticationFile(const std::string &fileName)
        {
            boost::property_tree::ini_parser::read_ini(fileName.c_str(), _pt);
        }

        const std::string getUserName() const
        {
            std::string name = _pt.get<std::string>("security_password.user-name");
            if(false == checkAgainstRegularExpression(name))
            {
                name = "";
            }

            return name;
        }

        const std::string getUserPassword() const
        {
            std::string password = _pt.get<std::string>("security_password.user-password");
            if(false == checkAgainstRegularExpression(password))
            {
                password = "";
            }

            return password;
        }
    };
} // namespace scidb
#endif /* AUTHENTICATION_FILE_H_ */
