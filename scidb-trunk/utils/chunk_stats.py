#!/usr/bin/python

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

"""
Print chunk statistics for given arrays.
"""

import argparse
import os
import pprint as pp
import subprocess
import sys
import traceback

from collections import namedtuple
from scidblib import statistics as StatLib
from scidblib.util import iquery, make_table

try:
    from collections import Counter # Latest, presumably with bug fixes
except ImportError:
    from scidblib.counter import Counter # Python 2.6, so use local copy

_args = None                    # Globally visible parsed arguments
_pgm = None                     # Globally visible program name

_PPRINT_FRIENDLY = True         # Else use ChunkStats (program friendly)

# Named tuple types to hold our results.
Stat = namedtuple('Stat', ('mean', 'stdev', 'min', 'median', 'max'))
CHUNK_STAT_SCALARS = ('array', 'attr', 'attid', 'nchunks')
CHUNK_STAT_TUPLES = ('nelem', 'csize', 'usize', 'asize')
ChunkStats = namedtuple('ChunkStats', CHUNK_STAT_SCALARS + CHUNK_STAT_TUPLES)

class AppError(Exception):
    """Base class for all exceptions that halt script execution."""
    pass

def dbg(*args):
    if _args.verbosity:
        print >>sys.stderr, _pgm, ' '.join(map(str, args))

def warn(*args):
    print >>sys.stderr, _pgm, "Warning:", ' '.join(map(str, args))

def get_chunk_statistics(array_name, _arrays=[], _chdescs=[]):
    """Derive chunk statistics for the given array."""

    # Read and cache results of these list() queries.
    if not _arrays:
        _arrays = make_table('array', "list('arrays', true)")
    if not _chdescs:
        want = "arrid,attid,nelem,csize,usize,asize"
        _chdescs = make_table('chdesc',
                              "project(list('chunk descriptors'),%s)" % want)

    # Find the versioned array id (vaid) of the given array name.
    if '@' in array_name:
        aname, ver = array_name.split('@')
        ver = int(ver)
        pairs = [(ver, int(x.aid)) for x in _arrays if x.name == array_name]
    else:
        aname = array_name
        pairs = [(int(x.name[x.name.index('@')+1:]), int(x.aid))
                 for x in _arrays if x.name.startswith("%s@" % array_name)]
    if not pairs:
        raise AppError("Array '{0}' not found".format(array_name))
    vaid = sorted(pairs)[-1][1]
    dbg("Versioned array id for", array_name, "is", vaid)

    # Use vaid to collect chunk data from the chunkdesc table.
    attr_table = make_table('attrs', "attributes(%s)" % aname)
    attr2name = dict([(int(x.No), x.name) for x in attr_table])
    attr2chunks = dict([(int(x.No), []) for x in attr_table])
    ebm = len(attr2name)
    attr2name[ebm] = "emptyBitmap"
    attr2chunks[ebm] = []
    nchunks = 0
    chunks_per_attr = Counter()
    for cdesc in _chdescs:
        if int(cdesc.arrid) == vaid:
            aid = int(cdesc.attid)
            nchunks += 1
            chunks_per_attr[aid] += 1
            # Appended list *MUST* be in same order as CHUNK_STAT_TUPLES.
            attr2chunks[aid].append(
                map(int, (cdesc.nelem, cdesc.csize, cdesc.usize, cdesc.asize)))

    # Paranoid check that all attributes have same number of chunks.
    expected_cpa = nchunks // len(attr2name)
    complaints = []
    for x in chunks_per_attr:
        if chunks_per_attr[x] != expected_cpa:
            complaints.append(
                "Attribute {0} has unexpected chunk count {1}, should be {2}".format(
                    x, chunks_per_attr[x], expected_cpa))
    if complaints:
        raise AppError('\n'.join(complaints))

    # Build ChunkStats objects, one per attribute.
    all_chunk_stats = []
    for aid in attr2name:
        if _PPRINT_FRIENDLY:
            stats = [array_name,
                     (attr2name[aid], aid),
                     ('chunks_per_attr', chunks_per_attr[aid])]
        else:
            stats = [array_name, attr2name[aid], aid, chunks_per_attr[aid]]
        for i, datum in enumerate(CHUNK_STAT_TUPLES):
            data = sorted([x[i] for x in attr2chunks[aid]])
            stat = Stat(StatLib.mean(data),
                        StatLib.stdev(data),
                        data[0],
                        StatLib.median(data),
                        data[-1])
            if _PPRINT_FRIENDLY:
                stats.append((datum, stat))
            else:
                stats.append(stat)
        if _PPRINT_FRIENDLY:
            all_chunk_stats.append(stats)
        else:
            all_chunk_stats.append(ChunkStats(*stats))

    # Done!
    return all_chunk_stats

def process():
    """Process remaining arguments as array names."""
    ret = 0
    for array in _args.arglist:
        try:
            stats = get_chunk_statistics(array)
        except AppError as e:
            print >>sys.stderr, _pgm, e
            ret += 1
        else:
            pp.pprint(stats)
    return ret

def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0]) # colon for easy use by print

    parser = argparse.ArgumentParser(
        description="Compute chunk statistics for arrays using scidblib.statistics.",
        epilog='Type "pydoc %s" for more information.' % _pgm[:-1])
    parser.add_argument('-v', '--verbosity', default=0, action='count',
                    help='Increase debug logging. 1=info, 2=debug, 3=debug+')
    parser.add_argument('arglist', nargs=argparse.REMAINDER,
                        help='Arrays whose statistics are wanted')
    
    global _args
    _args = parser.parse_args(argv[1:])

    try:
        return process()
    except AppError as e:
        print >>sys.stderr, _pgm, e
        return 1
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always want this for unexpected exceptions
        return 2

if __name__ == '__main__':
    sys.exit(main())
