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
 * @file
 *
 * @brief Config arguments for iquery executable
 *
 * @author Marty Corbett <mcorbett@paradigm4.com>
 */


#ifndef IQUERYCONFIG_H_
#define IQUERYCONFIG_H_

namespace iquery
{

enum
{
    CONFIG_PRECISION,
    CONFIG_HOST,
    CONFIG_PORT,
    CONFIG_QUERY_STRING,
    CONFIG_QUERY_FILE,
    CONFIG_RESULT_FILE,
    CONFIG_RESULT_FORMAT,
    CONFIG_VERBOSE,
    CONFIG_TIMER,
    CONFIG_NO_FETCH,
    CONFIG_AFL,
    CONFIG_READ_STDIN,
    CONFIG_PLUGINSDIR,
    CONFIG_HELP,
    CONFIG_VERSION,
    CONFIG_IGNORE_ERRORS,
    CONFIG_USER_NAME,
    CONFIG_USER_PASSWORD
};

}

#endif /* IQUERYCONFIG_H_ */
