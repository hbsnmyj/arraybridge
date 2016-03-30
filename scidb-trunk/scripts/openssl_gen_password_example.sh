#!/bin/bash
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2014-2015 SciDB, Inc.
# All Rights Reserved.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#

password=$1

echo
echo
echo -------------------------------------------------------------------
echo Password:
echo -------------------------------------------------------------------
echo -n $password | openssl dgst -binary -sha512 | openssl base64
echo
echo "Note:  Carriage returns must be replaced with \n prior to using "
echo "       in iquery.  In addition, there are no carriage returns at"
echo "       the end of the last line unless the line is 64 characters"
echo "       long."
echo
echo
