#!/usr/bin/python
#
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
"""Lists all arrays optionally selecting temporaries or checking for an individual array.

@author Marty Corbett

Assumptions:
  - SciDB is running.
  - The namespaces library has been properly setup
  - 'iquery' is in your path.
"""

#
# Whether to print traceback, upon an exception
#
# If print_traceback_upon_exception (defined at the top of the script) is True,
# stack trace will be printed. This is helpful during debugging.
_print_traceback_upon_exception = True

import argparse
import sys
import re
import traceback
import scidblib
from operator import itemgetter
from scidblib import scidb_afl
from scidblib import scidb_psf

def scidb_make_qualified_array_name(namespace_name, array_name):
    """Given namespace_name and array_name return namespace_name.array_name as a string"""
    return "{0}.{1}".format(namespace_name, array_name)

def main():
    """The main function lists all arrays
    """
    parser = argparse.ArgumentParser(
                                     description='List all scidb arrays.',
                                     epilog=
                                     'Assumptions:\n' +
                                     '  - SciDB is running.\n'
                                     '  - The environment is setup to support namespaces.\n'
                                     '  - The iquery application is in your path.',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-c', '--host',
                        help='Host name to be passed to iquery.')
    parser.add_argument('-p', '--port',
                        help='Port number to be passed to iquery.')
    parser.add_argument('-t', '--temp-only', action='store_true',
                        help='Limiting the candidates to temp arrays.')
    parser.add_argument('-v', '--versions',
                        help='Include all versions in the list.')
    parser.add_argument('-A', '--auth-file',
                        help='Authentication file to be passed to iquery.')
    parser.add_argument('-s', '--sort-by', default='array', choices=['array', 'namespace'],
                        help='Either array or namespace.')
    parser.add_argument('-f', '--find-array',
                        help='find a particular array name.')

    args = parser.parse_args()

    try:
        arrays = []
        iquery_cmd = scidb_afl.get_iquery_cmd(args)
        namespaces = scidb_afl.get_namespace_names(iquery_cmd)
        for namespace in namespaces:
            new_arrays = scidb_afl.get_array_names(
                iquery_cmd=iquery_cmd,
                temp_only=args.temp_only,
                versions=args.versions,
                namespace=namespace)
            for array in new_arrays:
                t=(array, namespace)
                arrays.append(t)

        if arrays:
            if args.find_array:
                result=[tup for tup in arrays if tup[0] == args.find_array]
                if not result:
                    raise ValueError, 'array {0} not found'.format(args.find_array)
                array, namespace = result[0]
                print scidb_make_qualified_array_name('namespace', 'array')
                print scidb_make_qualified_array_name(namespace, array)
            else:
                print scidb_make_qualified_array_name('namespace', 'array')
                item=0
                if args.sort_by == 'namespace':
                    item=1

                for (array, namespace) in sorted(arrays, key=itemgetter(item)):
                    print scidb_make_qualified_array_name(namespace, array)
        else:
            print >> sys.stderr, 'No arrays found'

    except Exception, e:
        print >> sys.stderr, '------ Exception -----------------------------'
        print >> sys.stderr, e

        if _print_traceback_upon_exception:
            print >> sys.stderr, '------ Traceback (for debug purpose) ---------'
            traceback.print_exc()

        print >> sys.stderr, '----------------------------------------------'
        sys.exit(-1)  # upon an exception, throw -1

    # normal exit path
    sys.exit(0)

### MAIN
if __name__ == "__main__":
   main()
### end MAIN
