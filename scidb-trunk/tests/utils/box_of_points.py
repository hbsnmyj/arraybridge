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
A tool for generating random points inside a bounded n-dimensional box.

You specify the "upper left/lower right" points of an n-dimensional box,
along with the number of cells to generate.  The output is a flat file
of n+1 attributes: a row number, and the n coordinates of a cell in the
box.

Optionally you can specify the percentage (or absolute number) of
collisions you want, and the number of repeat collisions (for testing
the synthetic dimension interval).  Use -h to see the full list of
options.  The default output format is binary.

Only uniform distribution is provided; in future other distributions
may be supported (Pareto, Zipf).

Examples:

    Generate 700 points inside a 10x10x10 3-d box, with a 25%
    probability of any one point being a collision.  Use TSV output
    format.

        $ box_of_points.py -l 0,0,0 -u 9,9,9 -C 0.25 -c 700 -f tsv

    Generate 9000 points inside a 10x1000 2-d box, with 10% chance of
    collision, and each collision occurring 3 times.  Use binary output.

        $ box_of_points.py -l 0,0 -u 9,999 -C 0.1 -c 9000 -k 3

    An AFL query you might use to load the resulting file:

        store(
          redimension(
             input(<v:int64,x:int64,y:int64>[dummy], '/tmp/f.bin',
                   -2, '(int64,int64,int64)'),
             <v:int64>[x,y,synth]),  -- Autochunking is cool!
          A);
"""

import argparse
import os
import random
import struct
import sys
import traceback

_args = None                    # Globally visible parsed arguments
_pgm = None                     # Globally visible program name

def dbg(*args, **kwargs):
    if _args.verbosity:
        if (_args.verbosity > 1
            and _args.format != 'binary'
            and 'stdout' in kwargs
            and kwargs['stdout']):
            print "# DBG:", ' '.join(map(str, args))
        else:
            print >>sys.stderr, "DBG:", ' '.join(map(str, args))

class AppError(Exception):
    """Base class for all exceptions that halt script execution."""
    pass

def make_bounding_box(low, high):
    """Make two int tuples corresponding to the given coordinate strings."""
    try:
        lows = map(int, low.split(','))
        highs = map(int, high.split(','))
    except Exception as e:
        raise AppError("Bad coordinate specification: {0} / {1}".format(
                low, high))
    if len(lows) != len(highs) or not lows:
        raise AppError("Bad dimension counts: {0} / {1}".format(
                lows, highs))
    # Finally, make sure the lows are low and highs high.  We just want
    # a good bounding box, we don't care if the luser gave us coordinate
    # specs in a whacky order.
    high_highs = []
    low_lows = []
    for l, h in zip(lows, highs):
        if l < h:
            high_highs.append(h)
            low_lows.append(l)
        else:
            high_highs.append(l)
            low_lows.append(h)
    dbg("box(", low_lows, ",", high_highs, ")")
    return (low_lows, high_highs)

def random_point(box):
    """Return a random point from within the bounding box."""
    pt = []
    for lo, hi in zip(*box):
        pt.append(random.randint(lo, hi))
    return tuple(pt)

class CoordinateGenerator(object):

    """Generate random coordinates within a bounding box."""

    def __init__(self, bounding_box, p_collision=None):
        self._box = bounding_box
        self._seen = set()
        self._seen_list = []
        self._coll = set()
        self._repeat = None
        self._rep_count = 0
        self._pcoll = p_collision

    # Python 3 compatibility because why not.
    def __next__(self):
        return self.next()

    def _get_unseen_point(self):
        while True:
            point = random_point(self._box)
            if point not in self._seen:
                self._seen.add(point)
                self._seen_list.append(point)
                return point

    def _get_collision_point(self):
        while True:
            idx = random.randint(0, len(self._seen_list)-1)
            point = self._seen_list[idx]
            if point not in self._coll:
                self._coll.add(point)
                return point

    def next(self):
        # Are we in the midst of creating k collisions (-k/--collision-depth)?
        if self._rep_count:
            self._rep_count -= 1
            return self._repeat
        # Should we generate a collision?
        if (self._pcoll is not None and self._seen
                and random.random() < self._pcoll):
            # Find a collision and set up for k more repeats.
            self._repeat = self._get_collision_point()
            assert _args.collision_depth >= 2
            self._rep_count = _args.collision_depth - 1
            return self._repeat
        else:
            return self._get_unseen_point()

def emit(coords):
    """Print coordinates to stdout in specified format."""
    if _args.format.lower() == "tsv":
        print >>_args.output, '\t'.join(map(str, coords))
    elif _args.format.lower() == "csv":
        print >>_args.output, ','.join(map(str, coords))
    elif _args.format.lower() == "binary":
        fmt = "<{0}q".format(len(coords))
        datum = struct.pack(fmt, *coords)
        assert len(datum) == 8*len(coords)
        _args.output.write(datum)
    else:
        raise AppError("Unrecognized output format %s" % _args.format)

def process():
    """Generate the cell locations."""
    box = make_bounding_box(_args.lower_corner, _args.upper_corner)

    # Collisions can be specified as absolute number or probability.
    # Construct a coordinates generator that will yield the proper
    # ration of collisions to non-collsions.
    if not _args.collisions:
        dbg("No collisions.")
        cg = CoordinateGenerator(box)
    else:
        if _args.collisions.is_integer():
            if _args.collision > _args.cells:
                raise AppError("Cannot have more collisions than cells.")
            p = float(_args.collisions) / _args.cells
            dbg("Computed collision probability:", p)
            cg = CoordinateGenerator(box, p)
        else:
            if _args.collisions < 0.0 or _args.collisions > 1.0:
                raise AppError(
                  "Probability -C/--collisions value not in range [0.0 .. 1.0]")
            dbg("Given collision probability:", _args.collisions)
            cg = CoordinateGenerator(box, _args.collisions)

    for row in xrange(_args.cells):
        coords = list(cg.next())
        coords.insert(0, row)   # Row number makes useful attribute data.
        emit(coords)
    return 0

def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    # Type-check the -k/--collision-depth parameter.
    def ge2(k):
        if k < 2:
            raise AppError("Bad -k/--collision-depth {0}, must be >= 2".format(k))
        return k

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0]) # colon for easy use by print

    parser = argparse.ArgumentParser(
        description="Generate random points inside an n-dimensional box.",
        epilog='Type "pydoc %s" for more information.' % _pgm[:-1])
    parser.add_argument('-l', '--lower-corner', default=None, help="""
        Lower corner of bounding box, e.g. "0,0,0.""")
    parser.add_argument('-u', '--upper-corner', default=None, help="""
        Upper corner of bounding box, e.g. "100,100,100".""")
    parser.add_argument('-c', '--cells', type=int, default=100, help="""
        Number of cells to generate within the box.""")
    parser.add_argument('-C', '--collisions', type=float, default=0.0, help="""
        Count (int) or probability (float) of duplicate coordinates to
        generate.""")
    parser.add_argument('-k', '--collision-depth', type=ge2, default=2,
        help="""When a collision occurs, generate this many colliding cells.
        Must be >= 2.  Used to tickle the synthetic interval limit.""")
    parser.add_argument('-f', '--format', default="binary", help="""
        Output format one of "tsv", "csv", or "binary".""")
    parser.add_argument('-o', '--output', type=argparse.FileType('w'),
        default=None, help="""Output file (default stdout).""")
    parser.add_argument('-s', '--seed', default=None, help="""
        Seed for random number generator.  IF YOU ARE CREATING AN
        AUTOMATED TEST, use the same seed for each run or your results
        will vary.""")
    parser.add_argument('-v', '--verbosity', default=0, action='count',
                    help='Increase debug logging. 1=info, 2=debug, 3=debug+')
    global _args
    _args = parser.parse_args(argv[1:])

    if _args.seed is not None:
        random.seed(_args.seed)
    if _args.verbosity:
        # Unbuffer so dbg() output appears in the right place.
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    if _args.output is None:
        _args.output = sys.stdout

    try:
        return process()
    except AppError as e:
        print >>sys.stderr, _pgm, e
        return 1
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always want this for unexpected exceptions
        return 2
    finally:
        if _args.output != sys.stdout:
            _args.output.close()

if __name__ == '__main__':
    sys.exit(main())
