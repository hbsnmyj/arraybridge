#!/usr/bin/python
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

"""
Create big chunks, and fail cleanly if chunk is "too big".

With ticket #4603 fixed, we should now be able to create phyiscal
chunks of up to 4GiB in size.  Anything over that should get a
SCIDB_LE_CHUNK_TOO_HUGE error (which in this case is wrapped in a
SCIDB_LE_FILE_IMPORT_FAILED error, but we check the error text to make
sure we did get "chunk too huge").
"""

import argparse
import errno
import os
import struct
import sys

from multiprocessing import Process
from t_other_utils import (iquery, IQUERY_HOST, IQUERY_PORT, ElapsedTimer)

_args = None
_pgm = None
_fifo = None

CHUNK_INTERVAL = 100000
BIG_BLOB_SIZE  = 42000          # 42000 * 100000 < ((2^32)-1)
HUGE_BLOB_SIZE = 43000          # 43000 * 100000 > ((2^32)-1), YIKES!

SCHEMA = "<a:binary>[i=1:{0},{0},0]".format(CHUNK_INTERVAL)

# Portions of the corresponding SCIDB_LE_... message text.  (We have
# to examine error output for the message text rather than the error
# name because the input() operator wraps all root-cause errors inside
# SCIDB_LE_IMPORT_FAILED, so the original error name is gone.  It sure
# would be nice if SciDB exceptions kept the complete error chain, as
# Java exceptions do... ticket 4841.)
#
LE_CHUNK_TOO_HUGE = "exceeds the limitations of the current storage format"
LE_CANT_REALLOCATE_MEMORY = "Failed to reallocate memory"

def log(*args):
    """Log to stderr (goes to big_chunk.out, doesn't affect test result)."""
    print >>sys.stderr, _pgm, ' '.join(map(str, args))

def log_mem_info():
    """Log the memory configuration etc. to big_chunk.out .

    This is to get a better understanding of what memory
    configurations allow the test to pass cleanly (as opposed to
    having to silently tolerate SCIDB_LE_CANT_REALLOCATE_MEMORY
    failures).
    """
    for opt in ('mem-array-threshold',
                'max-memory-limit',
                'chunk-size-limit-mb',
                'redim-chunk-overhead-limit-mb',
                'small-memalloc-size',
                'large-memalloc-limit',
                'network-buffer',
                'async-io-buffer'):
        val = iquery(['-otsv:l', '-aq', "_setopt('%s')" % opt]).splitlines()[1]
        log(opt, '=', val)
    log("Contents of /proc/meminfo:")
    try:
        with open('/proc/meminfo') as F:
            while True:
                x = F.read(4096)
                if not x:
                    break
                sys.stderr.write(x)
    except IOError as e:
        log("Cannot read /proc/meminfo:", e)

def make_blob(length, idnum):
    """Create a binary datum with the given length and unique id.

    By giving each blob a unique number, they can't be combined into a
    single RLE run (with our current encoding scheme anyway).  Hence
    we know the chunk payload gets very big.
    """
    blob = "\nBlob {0}: {1}".format(idnum, "*" * length)[:length]
    blob = struct.pack("<I{0}s".format(length), length, blob)
    return blob

def blob_writer(size, count):
    """Write 'count' blobs of size 'size' to the _fifo."""
    with open(_fifo, 'wb') as F:
        for i in xrange(count):
            blob = make_blob(size, i)
            try:
                F.write(blob)
            except IOError as e:
                if e.errno != errno.EPIPE:
                    print >>sys.stderr, _pgm, "Cannot write fifo", _fifo, "-", e

def ok(query_result):
    """Check if the query_result is acceptable, and log to stderr.

    @param query_result   combined stdout/stderr of iquery output
    @return 0 on success (got no error), 1 otherwise

    @note We cannot use t_other_utils.ok() here because some CDash
    memory configurations are too restricted to run the tests.  So we
    have to allow SCIDB_LE_CANT_REALLOCATE_MEMORY errors and just
    pretend everything is fine in that case.  However, we do log what
    actually happened to stderr, which goes to the big_chunk.out file
    without causing a test failure.
    """
    if not query_result.startswith('Error id: '):
        log("Test passed cleanly.")
        return 0
    if LE_CANT_REALLOCATE_MEMORY in query_result:
        log("Test ran out of memory on this platform, ignoring the failure.")
        return 0
    print "---- Test failed! ----"
    print query_result
    print "----------------------"
    return 1

def fail(query_result, expected_error, quiet=True):
    """Check for an expected error condition in the query_result.

    @param query_result   combined stdout/stderr of iquery output
    @param expected_error string we expect to find on error
    @param quiet          ignored, backward compat w/ t_other_utils
    @return 0 on success (got expected error), 1 otherwise

    @note See ok() above for why we can't use t_other_utils.fail() in
    this script.
    """
    if expected_error in query_result:
        log("Query failed as expected, test passes cleanly.")
        return 0
    if LE_CANT_REALLOCATE_MEMORY in query_result:
        log("Test ran out of memory on this platform, ignoring the failure.")
        return 0
    if not query_result.startswith('Error id: '):
        log("Query succeeded but we expected a failure:", expected_error)
        return 1
    print "---- Query failed as expected, but with an unexpected error! ----"
    print query_result
    print "----------------------"
    return 1

def run_tests():
    """Run the test cases.  Only two of them."""
    query = "consume(input({0}, '{1}', -2, '(binary)'))".format(SCHEMA, _fifo)

    # Test 1: A big chunk, but not too big.
    log("Starting big chunk test ({0} blobs of {1} bytes)".format(
        CHUNK_INTERVAL, BIG_BLOB_SIZE))
    p = Process(target=blob_writer, args=(BIG_BLOB_SIZE, CHUNK_INTERVAL))
    p.start()
    with ElapsedTimer():
        ret = ok(iquery(['-aq', query]))
    p.join()
    if ret:
        return ret

    # Test 2: A ***HUGE*** chunk!
    log("Starting HUGE chunk test ({0} blobs of {1} bytes)".format(
        CHUNK_INTERVAL, HUGE_BLOB_SIZE))
    p = Process(target=blob_writer, args=(HUGE_BLOB_SIZE, CHUNK_INTERVAL))
    p.start()
    with ElapsedTimer():
        ret = fail(iquery(['-aq', query]), LE_CHUNK_TOO_HUGE, not _args.verbose)
    p.join()
    return ret

def main(argv=None):
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0]) # colon for easy use by print

    parser = argparse.ArgumentParser(
        description='The other.big_chunk test script.')
    parser.add_argument('-c', '--host', default='localhost',
                        help="The SciDB host address.")
    parser.add_argument('-p', '--port', type=int, default=1239,
                        help="The TCP port for connecting to SciDB.")
    parser.add_argument('-r', '--run-id', type=str, default="", help="""
        Uniquifier (such as $HPID) to use in naming files etc.""")
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help="""Print timings and full error descriptions.""")
    global _args
    _args = parser.parse_args(argv[1:])

    IQUERY_HOST = _args.host
    IQUERY_PORT = _args.port
    ElapsedTimer.enabled = _args.verbose

    global _fifo
    _fifo = '/tmp/big_chunk_fifo_%s' % _args.run_id

    try:
        os.mkfifo(_fifo)
    except OSError as e:
        log("Cannot create fifo", fifo, "-", e)
        return 1
    try:
        return run_tests()
    finally:
        os.unlink(_fifo)
        log_mem_info()

if __name__ == '__main__':
    sys.exit(main())
