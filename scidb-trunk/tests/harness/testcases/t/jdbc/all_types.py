#!/usr/bin/python
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2015-2015 SciDB, Inc.
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
"""This script tests the building and printing of all built-in types.
In particular, it builds a 1-cell, 1-attribute array using multiple kinds of interesting values.
If the script is called with no parameter, the Java version of iquery will be used;
otherwise the C++ version of iquery will be used.

@author Donghui Zhang

Assumptions:
  - It assumes SciDB is running.
  - It assumes $SCIDB_INSTALL_PATH is set correctly.
"""

import subprocess
import sys
import os
import re
import random
import find_java8

# This is set in main().
using_java_iquery=True

def my_call(q, expected_error=''):
    # Print the query string.
    print q

    env_scidb_config_user = os.environ["SCIDB_CONFIG_USER"]

    # Choose between Java or C++ version of iquery.
    cmd_list = [
               os.environ['SCIDB_INSTALL_PATH']+"/bin/iquery"
               ]

    if using_java_iquery:
        cmd_list = [
              find_java8.find(),
              "-ea",
              "-cp",
              os.environ['SCIDB_INSTALL_PATH']+"/jdbc/scidb4j.jar",
              "org.scidb.iquery.Iquery"
              ]

    if len(env_scidb_config_user) > 0:
        cmd_list.extend(["--auth-file", env_scidb_config_user])

    cmd_list.extend(["-aq", q])

    # Execute the query.
    p = subprocess.Popen(cmd_list, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    out, err = p.communicate() # collect stdout,stderr, wait

    # Print the result.
    if expected_error=='' and err!='':
        raise Exception(err)
    elif expected_error!='' and err=='':
        raise Exception("Expected error " + expected_error + ", but the query succeeded.")
    elif expected_error!='' and err!='':
        if expected_error in err:
            print 'Expecting error ' + expected_error + ', and the query failed with that error. Good!'
            print
        else:
            raise Exception('The expected error, ' + expected_error + ', did not match the actual error\n' + err)
    else: # expected_error=='' and err==''
        print out

def testing_missing(missing_code):
    print "========== Testing missing(" + str(missing_code) + ") for all types =========="
    my_call("build(<v:bool null>      [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:char null>      [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:datetime null>  [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:datetimetz null>[i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:double null>    [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:float null>     [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:int8 null>      [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:int16 null>     [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:int32 null>     [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:int64 null>     [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:string null>    [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:uint8 null>     [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:uint16 null>    [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:uint32 null>    [i=0:0,1,0], missing(" + str(missing_code) + "))")
    my_call("build(<v:uint64 null>    [i=0:0,1,0], missing(" + str(missing_code) + "))")

def testing_bool_type():
    print "========== Testing 'bool' type =========="
    my_call( "build(<v:bool null>[i=0:0,1,0], true)" )
    my_call( "build(<v:bool null>[i=0:0,1,0], false)" )
    my_call( "build(<v:bool null>[i=0:0,1,0], '1847')", expected_error='SCIDB_SE_TYPE::SCIDB_LE_CANT_FIND_CONVERTER' )

def testing_char_type():
    print "========== Testing 'char' type =========="
    my_call( "build(<v:char null>[i=0:0,1,0], 'a')" )
    my_call( "build(<v:char null>[i=0:0,1,0], true)", expected_error='SCIDB_SE_TYPE::SCIDB_LE_CANT_FIND_CONVERTER' )

def testing_datetime_type():
    print "========== Testing 'datetime' type =========="
    my_call( "build(<v:datetime null>[i=0:0,1,0], '2015-04-27 14:50:13')" )
    my_call( "build(<v:datetime null>[i=0:0,1,0], 'abc')", expected_error='SCIDB_SE_TYPE_CONVERSION::SCIDB_LE_FAILED_PARSE_STRING' )

def testing_datetimetz_type():
    print "========== Testing 'datetimetz' type =========="
    my_call( "build(<v:datetimetz null>[i=0:0,1,0], '2015-04-27 10:51:19 -04:00')" )
    my_call( "build(<v:datetimetz null>[i=0:0,1,0], now())", expected_error='SCIDB_SE_TYPE::SCIDB_LE_CANT_FIND_CONVERTER' )

def testing_float_or_double_type(t):
    assert(t=='float' or t=='double')
    print "========== Testing '" + t + "' type =========="
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], -1.0/0)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 1.0/0)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 1.0/-0.0)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], -1.0/-0.0)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 0.0/0)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 32.0)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 12.345)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 12.34567)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 12.00000085)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 0.000)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], -0)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], -10)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 0/0)", expected_error='SCIDB_SE_EXECUTION::SCIDB_LE_DIVISION_BY_ZERO' )

def testing_string_type():
    print "========== Testing 'string' type =========="
    my_call( "build(<v:string null>[i=0:0,1,0], -1.0/0)" )
    my_call( "build(<v:string null>[i=0:0,1,0], 0.0/0)" )
    my_call( "build(<v:string null>[i=0:0,1,0], -10)" )
    my_call( "build(<v:string null>[i=0:0,1,0], 'a')" )
    my_call( "build(<v:string null>[i=0:0,1,0], 'abcd')" ) # This is counter intuitive. The special characters should be recognized.
    my_call( "build(<v:string null>[i=0:0,1,0], '')" )
    my_call( "build(<v:string null>[i=0:0,1,0], abcd)", expected_error='SCIDB_SE_SYNTAX::SCIDB_LE_UNKNOWN_ATTRIBUTE_OR_DIMENSION' )

def testing_integer_type(t):
    assert(t=='int8' or t=='int16' or t=='int32' or t=='int64' or
           t=='uint8' or t=='uint16' or t=='uint32' or t=='uint64')
    print "========== Testing '" + t + "' type =========="
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], -10)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 0)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 10)" )
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 2.5/0)" )  # It is counter intuitive to treat inf as 0
    my_call( "build(<v:" + t + " null>[i=0:0,1,0], 1/0)", expected_error='SCIDB_SE_EXECUTION::SCIDB_LE_DIVISION_BY_ZERO' )

def main():
    global using_java_iquery

    using_java_iquery = (len(sys.argv)==1)

    testing_missing(2)
    testing_missing(0)
    testing_bool_type()
    testing_char_type()
    testing_datetime_type()
    testing_datetimetz_type()
    testing_float_or_double_type('float')
    testing_float_or_double_type('double')
    testing_string_type()
    testing_integer_type('int8')
    testing_integer_type('int16')
    testing_integer_type('int32')
    testing_integer_type('int64')
    testing_integer_type('uint8')
    testing_integer_type('uint16')
    testing_integer_type('uint32')
    testing_integer_type('uint64')
    sys.exit(0)

### MAIN
if __name__ == "__main__":
   main()
### end MAIN
