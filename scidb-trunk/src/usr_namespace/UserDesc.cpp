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
 * @file UserDesc.cpp
 *
 * @author mcorbett@paradigm.com
 *
 * @brief A class for describing a user.
 *
 */

#include <usr_namespace/UserDesc.h>

namespace scidb
{
    UserDesc::UserDesc()
        : _name("")
        , _password("")
        , _passwordMethod("raw")
        , _id(0)
    {

    }

    UserDesc::UserDesc(
        const std::string &rName,
        const std::string &rPassword /* = "" */,
        const std::string &rPasswordMethod /* = "raw" */,
        const ID &id /* = 0 */)
        : _name(rName)
        , _password(rPassword)
        , _passwordMethod(rPasswordMethod)
        , _id(id)
    {

    }
} // namespace scidb

