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

"""
Ticket 2016 test: allow insertion of unbounded array into a bounded
one, so long as no cells actually live outside the bounded array's
dimension limits.

The other.insert_08 test also tests this, but I wanted to do some
additional tests with more complex queries (placing arbitrary "grids"
into the sparse unbounded input), and writing them on one line would
be yucky.
"""

import argparse
import sys

import t_other_utils
from t_other_utils import iquery, ok, fail, make_grid

_args = None

def main(argv=None):
    if argv is None:
        argv = sys.argv

    global _args
    parser = argparse.ArgumentParser(
        description='The insert_09 test script.')
    parser.add_argument('-c', '--host', default='localhost',
                        help="The SciDB host address.")
    parser.add_argument('-p', '--port', type=int, default=1239,
                        help="The TCP port for connecting to SciDB.")
    parser.add_argument('-r', '--run-id', default="", help="""
        Uniquifier (such as $HPID) to use in naming files etc.""")
    _args = parser.parse_args(argv[1:])

    t_other_utils.IQUERY_HOST = _args.host
    t_other_utils.IQUERY_PORT = _args.port

    BOUNDED_SCHEMA = "<value:int64>[row=0:499,100,0,col=0:99,100,0]"
    BOUNDED_ARRAY = "bounded_%s" % _args.run_id

    print 'Create bounded array.'
    print iquery(['-aq', 'create array %s %s' % (BOUNDED_ARRAY,BOUNDED_SCHEMA)])
    INSERT_QUERY = 'insert(%s, {0})'.format(BOUNDED_ARRAY)

    fails = 0

    print '\nEasy insert...'
    fails += ok(iquery(['-naq', INSERT_QUERY % make_grid(10, 10, 60, 30)]))

    print '\nRight up against the row limit...'
    fails += ok(iquery(['-naq', INSERT_QUERY % make_grid(450, 10, 499, 20)]))

    print '\nOne step over the line...'
    fails += fail(iquery(['-naq', INSERT_QUERY % make_grid(450, 10, 500, 20)]),
                  "SCIDB_SE_OPERATOR::SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES")

    print '\nWay over the line...'
    fails += fail(iquery(['-naq', INSERT_QUERY % make_grid(480, 10, 520, 20)]),
                  "SCIDB_SE_OPERATOR::SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES")

    print '\nRight up against the column limit...'
    fails += ok(iquery(['-naq', INSERT_QUERY % make_grid(10, 80, 50, 99)]))

    print '\nOne step over the column limit...'
    fails += fail(iquery(['-naq', INSERT_QUERY % make_grid(10, 80, 50, 100)]),
                  "SCIDB_SE_OPERATOR::SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES")

    print '\nPartially over both limits...'
    fails += fail(iquery(['-naq', INSERT_QUERY % make_grid(480, 95, 500, 100)]),
                  "SCIDB_SE_OPERATOR::SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES")

    print '\nWay over both limits...'
    fails += fail(iquery(['-naq', INSERT_QUERY % make_grid(510, 120, 530, 140)]),
                  "SCIDB_SE_OPERATOR::SCIDB_LE_CHUNK_OUT_OF_BOUNDARIES")

    print '\nCleanup.'
    iquery(['-naq', 'remove(%s)' % BOUNDED_ARRAY])

    if fails:
        print fails, "test case failures"
    else:
        print "All test cases passed."

    # By returning 0 even for failure, we prefer a FILES_DIFFER error
    # to an EXECUTOR_FAILED error.  Seems slightly more accurate.
    return 0

if __name__ == '__main__':
    sys.exit(main())
