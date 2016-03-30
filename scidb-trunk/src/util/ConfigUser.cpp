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
 * @file ConfigUser.cpp
 *
 * @author Marty Corbett <mcorbett@paradigm4.com>
 */

#include <util/ConfigUser.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


namespace scidb
{
    void ConfigUser::verifySafeFile(
        const std::string &filename)
    {
        struct stat stats;

        if(filename.empty())
        {
            throw USER_EXCEPTION(SCIDB_SE_INTERNAL,SCIDB_LE_CONFIGURATION_FILE_BLANK_NAME);
        }

        if(lstat(filename.c_str(), &stats))
        {
            throw USER_EXCEPTION(
                SCIDB_SE_INTERNAL,
                SCIDB_LE_CONFIGURATION_FILE_MISSING)
                    << filename.c_str();
        }

        switch(stats.st_mode)
        {
            default:
            {
                std::stringstream ss;
                ss  << " file=" << filename << std::endl
                    << "stats.st_mode="
                        << std::oct
                        << stats.st_mode
                        << std::dec
                        << " (octal)";

                throw USER_EXCEPTION(
                    SCIDB_SE_INTERNAL,
                    SCIDB_LE_CONFIGURATION_FILE_MODE)
                        << ss.str();
            } break;

            case S_IFREG | S_IRUSR | S_IWUSR:
            case S_IFREG | S_IRUSR:
                break;
        }

        if( !S_ISREG(stats.st_mode) ||  // regular file
            S_ISDIR(stats.st_mode)  ||  // directory
            S_ISFIFO(stats.st_mode) ||  // named pipe
            S_ISLNK(stats.st_mode)  ||  // symbolic link
            S_ISSOCK(stats.st_mode))    // socket
        {
            std::stringstream ss;
            ss << "fileMode file=" << filename;

            throw USER_EXCEPTION(
                SCIDB_SE_INTERNAL,
                SCIDB_LE_CONFIGURATION_FILE_MODE)
                    << ss.str();
        }

        if(geteuid() != stats.st_uid)
        {
            std::stringstream ss;
            ss << "owner file=" << filename;

            throw USER_EXCEPTION(
                SCIDB_SE_INTERNAL,
                SCIDB_LE_CONFIGURATION_FILE_OWNER)
                    << ss.str();
        }
    }

    void ConfigUser::addOptions()
    {
        addOption
            (
                CONFIG_STRING_SECURITY_USER_NAME,    // int32_t option
                'u',                    // char shortCmdLineArg
                "user-name",            // const std::string &longCmdLineArg
                "user-name",            // const std::string &configOption
                "",                     // const std::string &envVariable
                STRING,  // ConfigOptionType type
                "UserName.",            // const std::string &description = ""
                std::string("root"),    // const boost::any &value = boost::any()
                false)                  // bool required = true

            (
                CONFIG_STRING_SECURITY_USER_PASSWORD,  // int32_t option
                'p',                    // char shortCmdLineArg
                "user-password",        // const std::string &longCmdLineArg
                "user-password",        // const std::string &configOption
                "",                     // const std::string &envVariable
                STRING,  // ConfigOptionType type
                "UserPassword.",        // const std::string &description = ""
                std::string(""),        // const boost::any &value = boost::any()
                false)                  // bool required = true
            ;
    }

    // -----------------------------------------------------------------
    std::string ConfigUser::getUserName()
    {
        return getOption<std::string>(
            CONFIG_STRING_SECURITY_USER_NAME);
    }

    // -----------------------------------------------------------------
    std::string ConfigUser::getUserPassword()
    {
        return getOption<std::string>(
            CONFIG_STRING_SECURITY_USER_PASSWORD);
    }

} // namespace scidb
