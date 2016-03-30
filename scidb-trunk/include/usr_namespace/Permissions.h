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

/*
 * Permissions.h
 *
 *  Created on: May 27, 2015
 *      Author: mcorbett@paradigm4.com
 */

#ifndef __SCIDB_PERMISSIONS_H__
#define __SCIDB_PERMISSIONS_H__

namespace scidb
{
    namespace permissions
    {
        // General permissions
        static const char Root           = 'R';
        static const char Administrate   = 'A';

        // Permissions at the namespace level
        namespace namespaces
        {
            static const char Administrate   = 'a';
            static const char CreateArray    = 'c';
            static const char ReadArray      = 'r';
            static const char UpdateArray    = 'u';
            static const char DeleteArray    = 'd';
            static const char ListArrays     = 'l';
        } // namespace Namespaces
    } // namespace Permissions
} // namespace scidb

#endif // __SCIDB_PERMISSIONS_H__

