#!/usr/bin/python

# BEGIN_COPYRIGHT
#
# Copyright (C) 2008-2015 SciDB, Inc.
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

# References:
#    https://docs.python.org/2/tutorial/datastructures.html
#    https://docs.python.org/2/library/unittest.html#unittest.TestCase

"""Tiny unit test, to test individual python module files."""

import random
import re
import sys
import tempfile
import unittest


import scidblib
from scidblib import scidb_afl
from scidblib import scidb_progress


# ======================================================================
class TemporaryFile(object):
    def __init__(self):
        self._tempfile = tempfile.NamedTemporaryFile()
        self._tempfile.seek(0)

    def getName(self):
        return self._tempfile.name

    def importListTuple(self, listTuple):
        for element in listTuple:
            string_val="".join(str(element))    \
                .replace("(", "")               \
                .replace(")","")
            self._tempfile.write(string_val + '\n')
        self._tempfile.flush()

    def seek(self,pos):
        self._tempfile.seek(pos)

    def close(self):
        self._tempfile.close()


# ======================================================================
class Scidb_Afl:
    # -----------------------------------------------------------
    def __init__(self):
        class TmpArgs:
            def __init__(self):
                self.host = ''
                self.port = ''

        args = TmpArgs()
        self._iquery_cmd = scidb_afl.get_iquery_cmd(args, 'iquery')


    # -----------------------------------------------------------
    def query(self, cmd,            \
        want_output=True,           \
        tolerate_error=False,       \
        verbose=True):

        return scidb_afl.afl(       \
            self._iquery_cmd,       \
            cmd,                    \
            want_output,            \
            tolerate_error,         \
            verbose)


    # -----------------------------------------------------------
    def remove_arrays_by_prefix(self, prefix):
        names=scidb_afl.get_array_names(self._iquery_cmd)
        names_to_remove = []
        for name in names:
            if name.startswith(prefix):
                names_to_remove.append(name)

        for name in names_to_remove:
            self.query('remove('+name+')')


    # -----------------------------------------------------------
    # Stores a single dimension
    # -----------------------------------------------------------
    class Dimension:
        def __init__(self, name,    \
            startMin, endMax, chunkSize, chunkOverlap):
            self._name          = name
            self._startMin      = startMin
            self._endMax        = endMax
            self._chunkSize     = chunkSize
            self._chunkOverlap  = chunkOverlap

        def __repr__(self):
            return self._name               + "="  \
                + str(self._startMin)       + ":"  \
                + str(self._endMax)         + ","  \
                + str(self._chunkSize)      + ","  \
                + str(self._chunkOverlap)

    # -----------------------------------------------------------
    # Stores a list of dimensions
    # -----------------------------------------------------------
    class Dimensions:
        # - - - - - - - - - - - - - - - - - - - - - - -
        def __init__(self):
            self._list=[]

        # - - - - - - - - - - - - - - - - - - - - - - -
        def append(self, attribute):
            self._list.append(attribute)

        # - - - - - - - - - - - - - - - - - - - - - - -
        def __repr__(self):
            return str(self._list)


    # -----------------------------------------------------------
    # Stores a single attribute
    # for now it only stores the attribute string, extend later
    # -----------------------------------------------------------
    class Attribute:
        def __init__(self, str_attr):
            self._str_attr = str_attr

        def __repr__(self):
            return self._str_attr

    # -----------------------------------------------------------
    # Stores a list of attributes
    # -----------------------------------------------------------
    class Attributes:
        # - - - - - - - - - - - - - - - - - - - - - - -
        def __init__(self):
            self._list=[]

        # - - - - - - - - - - - - - - - - - - - - - - -
        def append(self, attribute):
            self._list.append(attribute)

        # - - - - - - - - - - - - - - - - - - - - - - -
        def __repr__(self):
            str_attr='<'
            for attribute in self._list:
                str_attr += str(attribute)
                if attribute != self._list[-1]:
                    str_attr += ', '
                else:
                    str_attr += '> '
            return str_attr


    # -----------------------------------------------------------
    # provides the array functionality needed by the tests
    # -----------------------------------------------------------
    class Array:
        # - - - - - - - - - - - - - - - - - - - - - - -
        def __init__(self, afl, name, attribute_list, dimension_list):
            self._afl            = afl
            self._name           = name
            self._attribute_list = attribute_list
            self._dimension_list = dimension_list

            cmd  = 'create array ' + self._name
            cmd += str(self._attribute_list)
            cmd += str(self._dimension_list)

            result=self._afl.query(cmd)
            print result

        # - - - - - - - - - - - - - - - - - - - - - - -
        def getName(self):
            return self._name

        # - - - - - - - - - - - - - - - - - - - - - - -
        def getAttributeList(self):
            return self._attribute_list

        # - - - - - - - - - - - - - - - - - - - - - - -
        def getDimensionList(self):
            return self._dimension_list

        # - - - - - - - - - - - - - - - - - - - - - - -
        def remove(self):
            return self._afl.query(     \
                'remove(%s)'            \
                % self.getName())

        # - - - - - - - - - - - - - - - - - - - - - - -
        def merge(self, other):
            return self._afl.query(     \
                'merge(%s, %s)' %       \
                (self.getName(),        \
                other.getName()))


# ======================================================================
class TestMerge(unittest.TestCase):
    # -----------------------------------------------------------
    def setUp(self):
        #random.seed(0x2458ACED)
        random.seed(0x1F2E3D4C)

        self._afl = Scidb_Afl()

        # --- first base set of data for merge
        self._merge_src_a=[
            (-2,0,-2),
            (-2,1,8),
            (-1,0,-1),
            (-1,1,9),
            (0,0,0),
            (0,1,10),
            (1,0,1),
            (1,1,11),
            (-2,2,18),
            (-1,2,19),
            (0,2,20),
            (1,2,21),
            (3,0,3),
            (3,1,13),
            (4,0,4),
            (4,1,14),
            (6,0,6),
            (6,1,16),
            (7,0,7),
            (7,1,17),
            (3,2,23),
            (4,2,24),
            (6,2,26),
            (7,2,27),
            (8,0,8),
            (8,1,18),
            (8,2,28)
        ]

        # --- second base set of data for merge
        self._merge_src_b=[
            (9,0,-2),
            (9,1,8),
            (9,2,18),
            (10,0,-1),
            (10,1,9),
            (10,2,19),
            (11,0,0),
            (11,1,10),
            (11,2,20),
            (12,0,1),
            (12,1,11),
            (12,2,21),
            (14,0,3),
            (14,1,13),
            (14,2,23),
            (15,0,4),
            (15,1,14),
            (15,2,24),
            (17,0,6),
            (17,1,16),
            (17,2,26),
            (18,0,7),
            (18,1,17),
            (18,2,27),
            (19,0,8),
            (19,1,18),
            (19,2,28)
        ]


    # -----------------------------------------------------------
    # provide easier access to the query functionality within the
    # test framework
    # -----------------------------------------------------------
    def query(self, cmd,            \
        want_output=True,           \
        tolerate_error=False,       \
        verbose=True):

        return self._afl.query(     \
            cmd,                    \
            want_output,            \
            tolerate_error,         \
            verbose)


    # -----------------------------------------------------------
    def compare_merge(self, a_list, b_list, result):
        expected_list = []

        # Create expected_list
        expected_list.append('{x,y} a')

        # Add the a_list to the expected list
        for a_element in a_list:
            (a_x, a_y, a_attr) = a_element
            expected_list.append(   \
                "{" + str(a_x) + "," + str(a_y) + "} " + str(a_attr))

        # Add the b_list to the expected list
        for b_element in b_list:
            (b_x, b_y, b_attr) = b_element

            for a_element in a_list:
                (a_x, a_y, a_attr) = a_element

                # If the b_element coordinates match the a_element
                # coordinates then the a_element is chosen (which
                # was already added.  If they differ then add the
                # b_element as these are new coordinates.
                if  not ((a_x == b_x) and (a_y == b_y)):
                    expected_list.append(   \
                        "{"                 \
                        + str(b_x) + ","    \
                        + str(b_y) + "} "   \
                        + str(b_attr))

        # delta should be an empty list if the merge worked
        delta = [x for x in result if x not in expected_list]
        self.assertTrue(delta == [''], 'merge failures' + str(delta))


    # -----------------------------------------------------------
    def generate_collisions(self, a_list, b_list):
        # determine how many collisions to generate
        _num = random.randint(1, 10)

        # convenience variable
        _len = len(a_list)

        _appended=0
        while (_appended != _num):
            # choose a random location in the a_list
            # which essentially chooses a random coordinate
            _loc = random.randint(0, _len)
            (_a_x, _a_y, _a_attr) = a_list[_loc]


            for _b_element in b_list:
                (_b_x, _b_y, _b_attr) = _b_element

                # verify that the _a_element coordinate is different
                # from the _b_element coordinate
                if  not ((_a_x == _b_x) and (_a_y == _b_y)):
                    # coordinates are different, add a random
                    # value at the coordinates specified by the
                    # +a_element
                    _value = random.randint(-10000, 10000)

                    #print 'add collision(%s, %s, %s)' \
                    #    % (str(_a_x), str(_a_y), str(_value))
                    b_list.append((_a_x, _a_y, _value))
                    _appended+=1
                    break


    # -----------------------------------------------------------
    # core test method
    # -----------------------------------------------------------
    def _test_scidb_merge_boundary(self,    \
        a_merge_data, b_merge_data,         \
        a_dims, a_attrs, b_dims, b_attrs,   \
        test_name):

        """Testing test_scidb_merge_boundary_aligned_aligned"""
        print '\n\n\n*** testing test_scidb_merge_boundary collisions=%s ***' \
            % test_name

        ## --- ##

        ## --- import data into temporary files --- ##
        tempfile_a=TemporaryFile()
        tempfile_a.importListTuple(a_merge_data)

        tempfile_b=TemporaryFile()
        tempfile_b.importListTuple(b_merge_data)

        try:

            ## --- Create array test_A --- ##
            arrayA=Scidb_Afl.Array(     \
                self._afl, 'test_A',    \
                a_attrs, a_dims)

            ## --- Init array test_A with tempfile_a --- ##
            result=self._afl.query(                             \
                'store(                                         \
                    redimension(                                \
                        input(                                  \
                            <x:int64,y:int64,a:int32 null>      \
                            [i=0:*,1000,0],                     \
                            \'%s\', 0, \'csv\'),                \
                        %s %s), %s)'                            \
                        % (                                     \
                            tempfile_a.getName(),               \
                            str(arrayA.getAttributeList()),     \
                            str(arrayA.getDimensionList()),     \
                            arrayA.getName() ))

            ## --- Create array test_B --- ##
            arrayB=Scidb_Afl.Array(     \
                self._afl, 'test_B',    \
                b_attrs, b_dims)

            ## --- Init array test_B with tempfile_b --- ##
            result=self._afl.query(                             \
                'store(                                         \
                    redimension(                                \
                        input(                                  \
                            <x:int64,y:int64,a:int32 null>      \
                            [i=0:*,1000,0],                     \
                            \'%s\', 0, \'csv\'),                \
                        %s %s), %s)'                            \
                        % (                                     \
                            tempfile_b.getName(),               \
                            str(arrayB.getAttributeList()),     \
                            str(arrayB.getDimensionList()),     \
                            arrayB.getName() ))

            ## --- merge arrays test_A and test_B together --- ##
            stdout, stderr=arrayA.merge(arrayB)

            ## --- verify the results of the merge --- ##
            self.compare_merge(       \
                a_merge_data,         \
                b_merge_data,         \
                stdout.split('\n'))

            ## --- merge arrays test_B and test_A together --- ##
            stdout, stderr=arrayB.merge(arrayA)

            ## --- verify the results of the merge --- ##
            self.compare_merge(       \
                a_merge_data,         \
                b_merge_data,         \
                stdout.split('\n'))


            ## --- remove the arrays test_A and test_B --- ##
            result=arrayA.remove()
            result=arrayB.remove()


        except Exception, error:
            print "Exception caught: %s" % str(error)
            ## --- the removal is done this way in case the   --- ##
            ## --- exception was thrown prior to both arrays  --- ##
            ## --- being created                              --- ##
            self._afl.remove_arrays_by_prefix("test_")

            ## --- Close the temporary files --- ##
            tempfile_a.close()
            tempfile_b.close()

            ## --- Assert here so that the primary --- ##
            ## --- unittest infrastructure knows   --- ##
            ## --- about the error.                --- ##
            self.assertTrue(0, str(error))

        ## --- Close the temporary files --- ##
        tempfile_a.close()
        tempfile_b.close()

        return


# In the following methods (for the Y dimension):
#    unaligned:   length of dimension % chunkSize != 0
#      aligned:   length of dimension % chunkSize = 0


    # -----------------------------------------------------------
    # Intermediate unit test for aligned/aligned
    # -----------------------------------------------------------
    def _test_scidb_merge_boundary_aligned_aligned(     \
        self, collisions, overlap):

        ## --- generate test name --- ##
        _test_name = '_test_scidb_merge_boundary'
        if collisions == 0:
            _test_name += '_nocollisions'
        else:
            _test_name += '_collisions'

        if overlap == 0:
            _test_name += '_nooverlap'
        else:
            _test_name += '_overlap'

        _test_name += '_aligned_aligned'

        ## --- retrieve original data --- ##
        _merge_src_a = self._merge_src_a
        _merge_src_b = self._merge_src_b

        ## --- generate collisions if necessary --- ##
        if collisions != 0:
            self.generate_collisions(_merge_src_a, _merge_src_b)

        ## --- add at least one piece of data into y=3 row for dim_a
        _merge_src_a.append((-1,3,-103))

        ## --- add at least one piece of data into y=3 row for dim_b
        _merge_src_b.append((11,3,-103))

        ## --- create arrayA dimension and attribute lists --- ##
        _a_dims=[]
        _a_dims.append(Scidb_Afl.Dimension('x', -2,  8, 5, overlap))
        _a_dims.append(Scidb_Afl.Dimension('y',  0,  3, 2, overlap))
        _a_attrs=Scidb_Afl.Attributes()
        _a_attrs.append(Scidb_Afl.Attribute('a:int32 null'))

        ## --- create arrayB dimension and attribute lists --- ##
        _b_dims=[]
        _b_dims.append(Scidb_Afl.Dimension('x', -2, 20, 5, overlap))
        _b_dims.append(Scidb_Afl.Dimension('y',  0,  3, 2, overlap))
        _b_attrs=Scidb_Afl.Attributes()
        _b_attrs.append(Scidb_Afl.Attribute('a:int32 null'))

        ## --- perform the actual test --- ##
        self._test_scidb_merge_boundary(    \
            _merge_src_a,                   \
            _merge_src_b,                   \
            _a_dims, _a_attrs,              \
            _b_dims, _b_attrs,              \
            _test_name)
        return

    # -----------------------------------------------------------
    # Intermediate unit test for unaligned/aligned
    # -----------------------------------------------------------
    def _test_scidb_merge_boundary_unaligned_aligned(   \
        self, collisions, overlap):

        ## --- generate test name --- ##
        _test_name = '_test_scidb_merge_boundary_'
        if collisions == 0:
            _test_name += '_nocollisions'
        else:
            _test_name += '_collisions'

        if overlap == 0:
            _test_name += '_nooverlap'
        else:
            _test_name += '_overlap'

        _test_name += '_unaligned_aligned'

        ## --- retrieve original data --- ##
        _merge_src_a = self._merge_src_a
        _merge_src_b = self._merge_src_b

        ## -- generate collisions if necessary --- ##
        if collisions != 0:
            self.generate_collisions(_merge_src_a, _merge_src_b)

        ## --- add at least one piece of data into y=3 row for dim_b
        _merge_src_b.append((11,3,-102))

        ## --- create arrayA dimension and attribute lists --- ##
        _a_dims=[]
        _a_dims.append(Scidb_Afl.Dimension('x', -2,  8, 5, overlap))
        _a_dims.append(Scidb_Afl.Dimension('y',  0,  2, 2, overlap))
        _a_attrs=Scidb_Afl.Attributes()
        _a_attrs.append(Scidb_Afl.Attribute('a:int32 null'))

        ## --- create arrayB dimension and attribute lists --- ##
        _b_dims=[]
        _b_dims.append(Scidb_Afl.Dimension('x', -2, 20, 5, overlap))
        _b_dims.append(Scidb_Afl.Dimension('y',  0,  3, 2, overlap))
        _b_attrs=Scidb_Afl.Attributes()
        _b_attrs.append(Scidb_Afl.Attribute('a:int32 null'))

        ## --- perform the actual test --- ##
        self._test_scidb_merge_boundary(    \
            _merge_src_a,                   \
            _merge_src_b,                   \
            _a_dims, _a_attrs,              \
            _b_dims, _b_attrs,              \
            _test_name)

        return

    # -----------------------------------------------------------
    # Intermediate unit test for aligned/unaligned
    # -----------------------------------------------------------
    def _test_scidb_merge_boundary_aligned_unaligned(   \
        self, collisions, overlap):

        ## --- generate test name --- ##
        _test_name = '_test_scidb_merge_boundary_'
        if collisions == 0:
            _test_name += '_nocollisions'
        else:
            _test_name += '_collisions'

        if overlap == 0:
            _test_name += '_nooverlap'
        else:
            _test_name += '_overlap'

        _test_name += '_aligned_unaligned'

        ## --- retrieve original data --- ##
        _merge_src_a = self._merge_src_a
        _merge_src_b = self._merge_src_b

        ## -- generate collisions if necessary --- ##
        if collisions != 0:
            self.generate_collisions(_merge_src_a, _merge_src_b)

        ## --- add at least one piece of data into y=3 row for dim_a
        _merge_src_a.append((-1,3,-103))


        ## --- create arrayA dimension and attribute lists --- ##
        _a_dims=[]
        _a_dims.append(Scidb_Afl.Dimension('x', -2,  8, 5, overlap))
        _a_dims.append(Scidb_Afl.Dimension('y',  0,  3, 2, overlap))
        _a_attrs=Scidb_Afl.Attributes()
        _a_attrs.append(Scidb_Afl.Attribute('a:int32 null'))

        ## --- create arrayB dimension and attribute lists --- ##
        _b_dims=[]
        _b_dims.append(Scidb_Afl.Dimension('x', -2, 20, 5, overlap))
        _b_dims.append(Scidb_Afl.Dimension('y',  0,  2, 2, overlap))
        _b_attrs=Scidb_Afl.Attributes()
        _b_attrs.append(Scidb_Afl.Attribute('a:int32 null'))

        ## --- perform the actual test --- ##
        self._test_scidb_merge_boundary(    \
            _merge_src_a,                   \
            _merge_src_b,                   \
            _a_dims, _a_attrs,              \
            _b_dims, _b_attrs,              \
            _test_name)

        return

    # -----------------------------------------------------------
    # Intermediate unit test for aligned/unaligned
    # -----------------------------------------------------------
    def _test_scidb_merge_boundary_unaligned_unaligned( \
        self, collisions, overlap):

        ## --- generate test name --- ##
        _test_name = '_test_scidb_merge_boundary_'
        if collisions == 0:
            _test_name += '_nocollisions'
        else:
            _test_name += '_collisions'

        if overlap == 0:
            _test_name += '_nooverlap'
        else:
            _test_name += '_overlap'

        _test_name += '_unaligned_unaligned'

        ## --- retrieve original data --- ##
        _merge_src_a = self._merge_src_a
        _merge_src_b = self._merge_src_b

        ## -- generate collisions if necessary --- ##
        if collisions != 0:
            self.generate_collisions(_merge_src_a, _merge_src_b)

        ## --- create arrayA dimension and attribute lists --- ##
        _a_dims=[]
        _a_dims.append(Scidb_Afl.Dimension('x', -2,  8, 5, overlap))
        _a_dims.append(Scidb_Afl.Dimension('y',  0,  2, 2, overlap))
        _a_attrs=Scidb_Afl.Attributes()
        _a_attrs.append(Scidb_Afl.Attribute('a:int32 null'))

        ## --- create arrayB dimension and attribute lists --- ##
        _b_dims=[]
        _b_dims.append(Scidb_Afl.Dimension('x', -2, 20, 5, overlap))
        _b_dims.append(Scidb_Afl.Dimension('y',  0,  2, 2, overlap))
        _b_attrs=Scidb_Afl.Attributes()
        _b_attrs.append(Scidb_Afl.Attribute('a:int32 null'))

        ## --- perform the actual test --- ##
        self._test_scidb_merge_boundary(    \
            _merge_src_a,                   \
            _merge_src_b,                   \
            _a_dims, _a_attrs,              \
            _b_dims, _b_attrs,              \
            _test_name)

        return


    # The following unit-tests are instantiated
    #_aligned_aligned_nooverlap
    #_aligned_aligned_overlap

    #_unaligned_aligned_nooverlap
    #_unaligned_aligned_overlap

    #_aligned_unaligned_nooverlap
    #_aligned_unaligned_overlap

    #_unaligned_unaligned_nooverlap
    #_unaligned_unaligned_overlap


    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_nocollisions_nooverlap_aligned_aligned(self):
        self._test_scidb_merge_boundary_aligned_aligned(        \
            collisions=0, overlap=0)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_nocollisions_nooverlap_unaligned_aligned(self):
        self._test_scidb_merge_boundary_unaligned_aligned(      \
            collisions=0, overlap=0)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_nocollisions_nooverlap_aligned_unaligned(self):
        self._test_scidb_merge_boundary_aligned_unaligned(      \
            collisions=0, overlap=0)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_nocollisions_nooverlap_unaligned_unaligned(self):
        self._test_scidb_merge_boundary_unaligned_unaligned(    \
            collisions=0, overlap=0)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_nocollisions_overlap_aligned_aligned(self):
        self._test_scidb_merge_boundary_aligned_aligned(        \
            collisions=0, overlap=1)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_nocollisions_overlap_unaligned_aligned(self):
        self._test_scidb_merge_boundary_unaligned_aligned(      \
            collisions=0, overlap=1)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_nocollisions_overlap_aligned_unaligned(self):
        self._test_scidb_merge_boundary_aligned_unaligned(      \
            collisions=0, overlap=1)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_nocollisions_overlap_unaligned_unaligned(self):
        self._test_scidb_merge_boundary_unaligned_unaligned(    \
            collisions=0, overlap=1)
        return


    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_collisions_nooverlap_aligned_aligned(self):
        self._test_scidb_merge_boundary_aligned_aligned(        \
            collisions=1, overlap=0)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_collisions_nooverlap_unaligned_aligned(self):
        self._test_scidb_merge_boundary_unaligned_aligned(      \
            collisions=1, overlap=0)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_collisions_nooverlap_aligned_unaligned(self):
        self._test_scidb_merge_boundary_aligned_unaligned(      \
            collisions=1, overlap=0)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_collisions_nooverlap_unaligned_unaligned(self):
        self._test_scidb_merge_boundary_unaligned_unaligned(    \
            collisions=1, overlap=0)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_collisions_overlap_aligned_aligned(self):
        self._test_scidb_merge_boundary_aligned_aligned(        \
            collisions=1, overlap=1)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_collisions_overlap_unaligned_aligned(self):
        self._test_scidb_merge_boundary_unaligned_aligned(      \
            collisions=1, overlap=1)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_collisions_overlap_aligned_unaligned(self):
        self._test_scidb_merge_boundary_aligned_unaligned(      \
            collisions=1, overlap=1)
        return

    # -----------------------------------------------------------
    # unit test
    # -----------------------------------------------------------
    def test_scidb_merge_boundary_collisions_overlap_unaligned_unaligned(self):
        self._test_scidb_merge_boundary_unaligned_unaligned(    \
            collisions=1, overlap=1)
        return


# -----------------------------------------------------------
# main method, load and execute the unit tests
# -----------------------------------------------------------
def main():
    suite=unittest.TestLoader().loadTestsFromTestCase(TestMerge)
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    assert result.wasSuccessful()
    sys.exit(0)


# -----------------------------------------------------------
# pythonic way to invoke main
# -----------------------------------------------------------

### MAIN
if __name__ == "__main__":
    main()
### end MAIN


