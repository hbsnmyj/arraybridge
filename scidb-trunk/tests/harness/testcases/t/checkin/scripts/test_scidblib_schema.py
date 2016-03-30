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


"""Tiny unit test, to test individual python module files."""

import sys
import re

from scidblib import scidb_schema as SS

def test_scidblib_parse_schema():
    """Unit test for the Python schema parser."""
    print '*** testing scidblib.scidb_schema.parse...'

    schema1 = """<
        a:double NULL DEFAULT -0.5,
        b:char DEFAULT 'a',
        c:int8 NULL,
        d:uint64,
        e:string DEFAULT 'aa
        Aa',
        f:datetime DEFAULT datetime('25Nov2009:16:11:19'),
        g:datetimetz DEFAULT datetimetz('11/25/2009 16:11:19 +10:00'),
        h:uint16 NOT NULL
        >
        [
        d_0=1:*,?,1,
        d_1=-101:-9,5,0
        ]
    """
    attrs,dims = SS.parse(schema1)
    # Check that the correct number of attributes and dimensions was parsed.
    print 'checking length of attributes list...'
    assert len(attrs) == 8
    print 'checking length of dimensions list...'
    assert len(dims) == 2
    # Check the names of all attributes.
    print 'checking attribute names...'
    attr_names = [
        'a',
        'b',
        'c',
        'd',
        'e',
        'f',
        'g',
        'h'
        ]
    assert [a.name for a in attrs] == attr_names
    # Check the types of all attributes.
    print 'checking attribute types...'
    attr_types = [
        'double',    # a
        'char',      # b
        'int8',      # c
        'uint64',    # d
        'string',    # e
        'datetime',  # f
        'datetimetz',# g
        'uint16'     # h
        ]
    assert [a.type for a in attrs] == attr_types
    # Check which attributes are nullable.
    print 'checking if attributes are nullable...'
    attr_nullables = [
        True,  # a
        True,  # b
        True,  # c
        True,  # d
        True,  # e
        True,  # f
        True,  # g
        False  # h
        ]
    assert [a.nullable for a in attrs] == attr_nullables

    # Check which attributes have default modifiers.
    print 'checking attribute default modifiers...'
    attr_defaults = [
        '-0.5',  # a
        '\'a\'', # b
        None,    # c
        None,    # d
        """\'aa
        Aa\'""", # e
        'datetime(\'25Nov2009:16:11:19\')', # f
        'datetimetz(\'11/25/2009 16:11:19 +10:00\')', # g
        None     # h
        ]
    assert [a.default for a in attrs] == attr_defaults
    # Check dimension names.
    print 'checking dimension names...'

    dim_names = ['d_0','d_1']
    assert [d.name for d in dims] == dim_names

    # Check dimension lower bounds.
    print 'checking lower bounds of dimensions...'

    dim_los = [1,-101]
    assert [d.lo for d in dims] == dim_los

    # Check dimension upper bounds.
    print 'checking upper bounds of dimensions...'

    dim_his = [SS.MAX_COORDINATE,-9]
    assert [d.hi for d in dims] == dim_his

    # Check dimension chunks.
    print 'checking dimension chunks...'

    dim_chunks = ['?',5]
    assert [d.chunk for d in dims] == dim_chunks

    # Check dimension overlaps.
    print 'checking dimension overlaps...'

    dim_overlaps = [1,0]
    assert [d.overlap for d in dims] == dim_overlaps

    # Reparse with old nullability rules and check nullable.
    attrs,_ = SS.parse(schema1, default_nullable=False)
    attr_nullables = [
        True,  # a
        False, # b
        True,  # c
        False, # d
        False, # e
        False, # f
        False, # g
        False  # h
        ]
    assert [a.nullable for a in attrs] == attr_nullables

def test_scidblib_unparse_schema():
    """Unit test for the Python schema un-parser."""
    print '*** testing scidblib.scidb_schema.unparse...'
    schema1 = "<z1:string DEFAULT 'aa aa',z2:int64 NULL DEFAULT -2," + \
        "z3:int32 NULL,z4:float DEFAULT -0.5,z5:char char('x')," + \
        "z4:datetime DEFAULT datetime(\'25Nov2009:16:11:19\')," + \
        "z5:datetimetz DEFAULT datetimetz(\'10/13/2008 15:10:20 +9:00\')>" + \
        "[dim1=-77:*,23,0,dim2=0:99,?,1,dim3=-100:7,?,1]"

    # TODO: Fix for default_nullable=True, see SDB-5138.
    attrs1,dims1 = SS.parse(schema1, default_nullable=False)
    schema2 = SS.unparse(attrs1, dims1, default_nullable=False)
    attrs2,dims2 = SS.parse(schema2, default_nullable=False)

    # Check attributes:
    print 'checking attributes...'
    for i, (attr1,attr2) in enumerate(zip(attrs1,attrs2)):
        assert attr1.name == attr2.name, "%d: %s != %s" % (i, attr1.name, attr2.name)
        assert attr1.type == attr2.type, "%d: %s != %s" % (i, attr1.type, attr2.type)
        assert attr1.nullable == attr2.nullable,  "%d: %s != %s" % (i, attr1.nullable, attr2.nullable)
        assert attr1.default == attr2.default, "%d: %s != %s" % (i, attr1.default, attr2.default)

    # Check dimensions:
    print 'checking dimensions...'
    for dim1,dim2 in zip(dims1,dims2):
        assert dim1.name == dim2.name
        assert dim1.lo == dim2.lo
        assert dim1.hi == dim2.hi
        assert dim1.chunk == dim2.chunk
        assert dim1.overlap == dim2.overlap

def main():
    test_scidblib_parse_schema()
    test_scidblib_unparse_schema()
    sys.exit(0)

### MAIN
if __name__ == "__main__":
   main()
### end MAIN

