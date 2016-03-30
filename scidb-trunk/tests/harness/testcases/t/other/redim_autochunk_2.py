#!/usr/bin/python

"""Some basic functional tests for autochunking redimension()."""

import argparse
import os
import sys
import traceback

from scidblib.util import (iquery, make_table)
from box_of_points import main as boxofpoints
from collections import defaultdict

_args = None                    # Globally visible parsed arguments
_pgm = None                     # Globally visible program name
_tmpdir = None                  # Dir for generated data files

class AppError(Exception):
    """Base class for all exceptions that halt script execution."""
    pass

def dbg(*args):
    #print >>sys.stderr, "DBG:", ' '.join(map(str, args))
    print "DBG:", ' '.join(map(str, args))

def noerr(s):
    """Predicate for easy testing for SciDB errors."""
    return "Error id:" not in s

class TheTest(object):

    """
    A dirt-simple test runner.

    I wanted to use unittest for this, but the version in Python 2.6
    is so old and crufty that it doesn't support setUpClass() and
    setUpModule() methods.  We should install "nose" everywhere, IMHO
    it's much better than unittest. -mjl
    """

    def setUpClass(self):
        """Create some test data files used by all test methods."""
        print "Setup ...",
        sys.stdout.flush()
        self._array_cleanups = []
        self._files = {}        # map array name to input file
        # Put all our data files in one temp directory so we can
        # easily remove them all during tearDown.
        if os.system("rm -rf {0} ; mkdir -p {0}".format(_tmpdir)):
            raise AppError("Trouble (re)creating %s" % _tmpdir)

        # Create slightly sparse 3-D input data with no collisions.
        self._files['nocoll_3d'] = os.path.join(_tmpdir, "nocoll_3d.bin")
        if boxofpoints(['boxofpoints',
                        '--lower-corner', '0,0,0',
                        '--upper-corner', '9,99,99',
                        '--cells', '80000', # sparse: 80,000 < 10x100x100
                        '--format', 'binary',
                        '--output', self._files['nocoll_3d'],
                        '--seed', '42']):
            raise AppError("box_of_points could not create %s" %
                           self._files['nocoll_3d'])

        # Create dense 2-D input data with 10% collisions.
        self._files['coll_2d'] = os.path.join(_tmpdir, "coll_2d.bin")
        if boxofpoints(['boxofpoints',
                        '--lower-corner', '0,0',
                        '--upper-corner', '99,999',
                        '--cells', '100000', # dense: 100,000 == 100x1000
                        '--collisions', '0.1', # 10% collision rate
                        '--format', 'binary',
                        '--output', self._files['coll_2d'],
                        '--seed', '42']):
            raise AppError("box_of_points could not create %s" %
                           self._files['coll_2d'])
        print "done"

    def tearDownClass(self):
        print "Teardown ...",
        sys.stdout.flush()
        if not _args.keep_arrays:
            if os.system("rm -rf {0}".format(_tmpdir)):
                raise AppError("Trouble cleaning up %s" % _tmpdir)
            for a in self._array_cleanups:
                iquery('-naq', "remove(%s)" % a)
        print "done"

    def test_00_load_3d_ac(self):
        """Load 3-D no-collision data using autochunking"""
        dims = "x,y,z"          # Autochunked!
        query = """
            store(
              redimension(
                input(<v:int64,x:int64,y:int64,z:int64>[dummy], '{0}',
                      -2, '(int64,int64,int64,int64)'),
                <v:int64>[{1}]),
              {2}) """.format(self._files['nocoll_3d'], dims, "nocoll_3d_ac")
        out = iquery('-naq', query)
        assert noerr(out), out
        self._array_cleanups.append("nocoll_3d_ac")

    def test_01_load_3d_concrete(self):
        """Load 3-D no-collisions data with specified chunks"""
        dims = "x=0:*,10,0,y=0:*,100,0,z=0:*,100,0"
        query = """
            store(
              redimension(
                input(<v:int64,x:int64,y:int64,z:int64>[dummy], '{0}',
                      -2, '(int64,int64,int64,int64)'),
                <v:int64>[{1}]),
              {2}) """.format(self._files['nocoll_3d'], dims, "nocoll_3d")
        out = iquery('-naq', query)
        assert noerr(out), out
        self._array_cleanups.append("nocoll_3d")

    def test_02_nocoll_3d_counts_and_sums(self):
        """Compare 3-D array counts and sums"""
        out1 = iquery('-otsv', '-aq', 'aggregate(nocoll_3d_ac,count(*),sum(v))')
        assert noerr(out1), out1
        out2 = iquery('-otsv', '-aq', 'aggregate(nocoll_3d,count(*),sum(v))')
        assert noerr(out2), out2
        c1, s1 = map(int, out1.split())
        c2, s2 = map(int, out2.split())
        assert c1 == c2, "counts differ"
        assert s1 == s2, "sums differ"

    def test_03_nocoll_3d_check_values(self):
        """Cell-by-cell value comparison for 3-D arrays"""
        out = iquery('-otsv+', '-aq', """
            filter(join(nocoll_3d,nocoll_3d_ac), nocoll_3d.v <> nocoll_3d_ac.v)
            """)
        assert noerr(out), out
        assert out == '', "Cell values differ:\n\t{0}".format(out)

    def test_04_load_2d_ac_w_collisions(self):
        """Load 2-D data containing collisions using autochunking"""
        dims = "x=0:*,*,0,y=0:*,*,0,synth=0:*,*,0"
        query = """
            store(
              redimension(
                input(<v:int64,x:int64,y:int64>[dummy], '{0}',
                      -2, '(int64,int64,int64)'),
                <v:int64>[{1}]),
              {2}) """.format(self._files['coll_2d'], dims, "coll_2d_ac")
        out = iquery('-naq', query)
        assert noerr(out), out
        self._array_cleanups.append("coll_2d_ac")

    def test_05_load_2d_concrete_w_collisions(self):
        """Load 2-D data containing collisions with specified chunks"""
        dims = "x=0:*,100,0,y=0:*,100,0,synth=0:9,10,0"
        query = """
            store(
              redimension(
                input(<v:int64,x:int64,y:int64>[dummy], '{0}',
                      -2, '(int64,int64,int64)'),
                <v:int64>[{1}]),
              {2}) """.format(self._files['coll_2d'], dims, "coll_2d")
        out = iquery('-naq', query)
        assert noerr(out), out
        self._array_cleanups.append("coll_2d")

    def test_06_coll_2d_counts_and_sums(self):
        """Compare 2-D array counts and sums"""
        out1 = iquery('-otsv', '-aq', 'aggregate(coll_2d_ac,count(*),sum(v))')
        assert noerr(out1), out1
        out2 = iquery('-otsv', '-aq', 'aggregate(coll_2d,count(*),sum(v))')
        assert noerr(out2), out2
        c1, s1 = map(int, out1.split())
        c2, s2 = map(int, out2.split())
        assert c1 == c2, "counts differ"
        assert s1 == s2, "sums differ"

    def test_07_coll_2d_check_values(self):
        """Cell-by-cell value comparison for 2-D arrays

        This test is complicated by the fact that with different chunk
        intervals, redimension() does not produce synthetic dimension
        siblings in any particular order.  So we must process the
        filtered list of differing cells, and only complain if the
        *set* of values along the synthetic dimension at [x,y,*]
        differs for the two arrays.  For example, if the two arrays
        held

            {x,y,synth} v           {x,y,synth} v
            {2,7,0} 20              {2,7,0} 20
            {2,7,1} 73              {2,7,1} 99
            {2,7,2} 99              {2,7,2} 73

        that is perfectly fine.
        """
        tbl = make_table('CellDiffs', """
            filter(join(coll_2d,coll_2d_ac), coll_2d.v <> coll_2d_ac.v)
            """)
        v_xy_sets = defaultdict(set)
        v2_xy_sets = defaultdict(set)
        for celldiff in tbl:
            key = (int(celldiff.x), int(celldiff.y))
            v_xy_sets[key].add(int(celldiff.v))
            v2_xy_sets[key].add(int(celldiff.v_2))
        assert len(v_xy_sets) == len(v2_xy_sets)
        for xy in v_xy_sets:
            assert v_xy_sets[xy] == v2_xy_sets[xy], \
                "Synthetic dimension trouble at {0}".format(xy)

    def test_08_load_3d_ac_w_overlap(self):
        """Load 3-D no-collision data using autochunking and overlaps."""
        dims = "x=0:*,*,2;y=0:*,*,3;z=0:*,*,0" # Autochunked!
        query = """
            store(
              redimension(
                input(<v:int64,x:int64,y:int64,z:int64>[dummy], '{0}',
                      -2, '(int64,int64,int64,int64)'),
                <v:int64>[{1}]),
              {2}) """.format(self._files['nocoll_3d'], dims, "nocoll_3d_ac_ol")
        out = iquery('-naq', query)
        assert noerr(out), out
        self._array_cleanups.append("nocoll_3d_ac_ol")

    def test_09_nocoll_3d_overlap_counts_and_sums(self):
        """Compare 3-D array counts and sums (overlap)"""
        out1 = iquery('-otsv', '-aq',
                      'aggregate(nocoll_3d_ac_ol,count(*),sum(v))')
        assert noerr(out1), out1
        out2 = iquery('-otsv', '-aq', 'aggregate(nocoll_3d,count(*),sum(v))')
        assert noerr(out2), out2
        c1, s1 = map(int, out1.split())
        c2, s2 = map(int, out2.split())
        assert c1 == c2, "counts differ"
        assert s1 == s2, "sums differ"

    def test_10_nocoll_3d_overlap_check_values(self):
        """Cell-by-cell value comparison for 3-D arrays (overlap)"""
        out = iquery('-otsv+', '-aq', """
            filter(join(nocoll_3d, nocoll_3d_ac_ol),
                        nocoll_3d.v <> nocoll_3d_ac_ol.v)
            """)
        assert noerr(out), out
        assert out == '', "Cell values differ:\n\t{0}".format(out)

    def test_11_empty_input(self):
        """Autochunked redimension of empty array should not fail (SDB-5109)"""
        out = iquery('-aq', 'create temp array empty<val:double>[k=0:39,20,4]')
        assert noerr(out), out
        self._array_cleanups.append("empty")
        out = iquery('-otsv+', '-aq',
                     'redimension(empty, <val:double>[k=0:39,*,3])')
        assert noerr(out), out
        assert not out, "Redim of empty array is not empty: '%s'" % out

    def test_12_one_input_cell(self):
        """Autochunked redimension of 1-cell array should not fail"""
        out = iquery('-aq', 'create temp array ONE<val:double>[k=0:39,20,4]')
        assert noerr(out), out
        self._array_cleanups.append("ONE")
        # Insert one cell at k == 25.
        out = iquery('-otsv+:l', '-naq', """
            insert(
              redimension(
                apply(build(<val:double>[i=0:0,1,0], 3.14), k, 25),
                ONE),
            ONE)""")
        assert noerr(out), out
        out = iquery ('-otsv+', '-aq',
                      'redimension(ONE, <val:double>[k=0:39,*,3])')
        assert noerr(out), out
        try:
            numbers = map(float, out.split())
        except ValueError:
            assert False, "Unexpected non-number in '%s'" % out
        assert len(numbers) == 2
        assert numbers[0] == 25
        assert numbers[1] == 3.14

    def run(self):
        """Dirt-simple test runner."""
        self.setUpClass()
        tests = [x for x in dir(self) if x.startswith("test_")]
        tests.sort()
        errors = 0
        for t in tests:
            print t, "...",
            sys.stdout.flush()
            try:
                getattr(self,t)()
            except AssertionError as e:
                print "FAIL (%s)" % e
                errors += 1
            else:
                print "ok"
        self.tearDownClass()
        if errors:
            print "Errors in", errors, "of", len(tests), "tests."
        else:
            print "All", len(tests), "tests passed."
        return errors


def set_iquery_envariables():
    """Make sure scidblib.util.iquery() knows how to contact SciDB.

    Command line options override environment.  Environment overrides
    hard-coded defaults.
    """
    if _args.host:
        os.environ['IQUERY_HOST'] = _args.host
    elif 'IQUERY_HOST' not in os.environ:
        os.environ['IQUERY_HOST'] = 'localhost'
    if _args.port:
        os.environ['IQUERY_PORT'] = _args.port
    elif 'IQUERY_PORT' not in os.environ:
        os.environ['IQUERY_PORT'] = '1239'


def main(argv=None):
    """Argument parsing and last-ditch exception handling.

    See http://www.artima.com/weblogs/viewpost.jsp?thread=4829
    """
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = "%s:" % os.path.basename(argv[0]) # colon for easy use by print

    parser = argparse.ArgumentParser(
        description="This skeleton program does very little.",
        epilog='Type "pydoc %s" for more information.' % _pgm[:-1])
    parser.add_argument('-c', '--host', default=None,
                        help='Target host for iquery commands.')
    parser.add_argument('-p', '--port', default=None,
                        help='SciDB port on target host for iquery commands.')
    parser.add_argument('-r', '--run-id', type=int, default=0,
                        help='Unique run identifier.')
    parser.add_argument('-k', '--keep-arrays', action='store_true',
                        help='Do not remove test arrays during cleanup.')
    parser.add_argument('-v', '--verbosity', default=0, action='count',
                    help='Increase debug logging. 1=info, 2=debug, 3=debug+')

    global _args
    _args = parser.parse_args(argv[1:])

    global _tmpdir
    _tmpdir = "/tmp/redim_autochunk_2.{0}".format(_args.run_id)

    set_iquery_envariables()

    try:
        tt = TheTest()
        return tt.run()
    except AppError as e:
        print >>sys.stderr, _pgm, e
        return 1
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always want this for unexpected exceptions
        return 2

if __name__ == '__main__':
    sys.exit(main())
