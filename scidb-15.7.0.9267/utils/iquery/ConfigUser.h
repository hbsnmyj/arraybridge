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
 * @file ConfigUser.h
 *
 * @brief Wrapper around boost::programm_options and config parser which
 * consolidate command-line arguments, enviroment variables and config options.
 *
 * @author Marty Corbett <mcorbett@paradigm4.com>
 */

#ifndef CONFIG_USER_H_
#define CONFIG_USER_H_

#include <string>
#include <log4cxx/logger.h>
#include <system/Config.h>
#include <util/Singleton.h>

namespace scidb
{
    static log4cxx::LoggerPtr logger = log4cxx::Logger::getRootLogger();

    /**
     * A class for accessing a user configuration file similiar
     * in structure to the iquery.conf file.  Currently this file
     * will contain the username and password.  But may grow to
     * include other user specific settings.
     */
    class ConfigUser
        : public Singleton<ConfigUser>
        , public ConfigBase
    {
    private:
        enum
        {
            // Plug-in builds depend on this enum, so please keep the order
            // stable to avoid a "flag day".  Add new values at the bottom, or
            // take one of the CONFIG_UNUSED_* slots.  Don't remove values,
            // instead rename to CONFIG_UNUSED_<integer_value_of_the_constant>.

            CONFIG_STRING_SECURITY_USER_NAME,
            CONFIG_STRING_SECURITY_USER_PASSWORD
        };

    public:
        /**
         * Add all of the options that are in the SCIDB_CONFIG_USER file
         */
        void addOptions();

        /**
         * Get the user's name from the user configuration file
         * In the file the the parameter "user-name"
         */
        std::string getUserName();

        /**
         * Get the user's password from the user configuration file
         * In the file the the parameter "user-password"
         */
        std::string getUserPassword();

        /**
         * Verify that the file permissions are set to one of the following:
         *     (a regular file and owner has read and write permission)
         * or:
         *     (a regular file and owner has read permission)
         *
         * In addition, verify that the currently logged in user is the
         * owner of the file.
         */
        static void verifySafeFile(
            const std::string &filename);


    private:
        friend class Singleton<ConfigUser>;
    };  // class ConfigUser
} // namespace scidb
#endif /* CONFIG_USER_H_ */
