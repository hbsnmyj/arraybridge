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
Broken pipe test.

Hang a reader process on a fifo, but have it quit after reading only a
few records, resulting in a broken pipe.  The SciDB save() operator
should fail gracefully.

See ticket 4642.
"""

import argparse
import os
import sys
import subprocess

_args = None

def main(argv=None):
    if argv is None:
        argv = sys.argv

    global _args
    parser = argparse.ArgumentParser(
        description='The save_SIGPIPE_1 test script.')
    parser.add_argument('-c', '--host', default='localhost',
                        help="The SciDB host address.")
    parser.add_argument('-p', '--port', type=int, default=1239,
                        help="The TCP port for connecting to SciDB.")
    parser.add_argument('-r', '--run-id', default="", help="""
        Uniquifier (such as $HPID) to use in naming files etc.""")
    _args = parser.parse_args(argv[1:])

    cmd = ['iquery']
    cmd.extend(['-c', _args.host, '-p', str(_args.port)])

    fifo = "/tmp/sigpipe_fifo_%s" % _args.run_id
    try:
        os.unlink(fifo)
    except:
        pass
    os.mkfifo(fifo)
    try:
        # Start a soon-to-quit fifo reader in the background.
        rdr = subprocess.Popen(['head', '-n', '10', fifo ])

        # High dimension bound of array to build.  Increasing this
        # value will exhibit the SG problem described in
        # https://trac.scidb.net/ticket/4642#comment:5 .  Using eight
        # 9's takes about 46 seconds.  The time-to-failure is
        # linearly proportional to this value.
        hi = '999999'

        # AFL save() some data into the fifo.
        query = """save(build(<x:double>[i=0:{0},10000,0], sin(i)),
                        '{1}', -2, 'tsv:l')""".format(hi, fifo)
        cmd.extend(['-naq', ' '.join(query.split())])
        iq = subprocess.Popen(cmd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)
        result = iq.communicate()[0]

        # We'd like the result in the output, but without the first
        # line since it contains a file and line number, and those may
        # change.
        print '-' * 4, "Query result:", '-' * 4
        print '\n'.join(result.splitlines()[1:])
        print '-' * 22

        # Collect reader proc's pid.
        rdr.wait()

        # Verify results.
        ok = "Broken pipe" in result
        if ok:
            print os.path.basename(argv[0]), "test: PASS"
            return 0
        else:
            print os.path.basename(argv[0]), "test: FAIL"
            return 1
    finally:
        os.unlink(fifo)

if __name__ == '__main__':
    sys.exit(main())
