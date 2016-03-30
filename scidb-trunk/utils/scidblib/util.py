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

# BEGIN_COPYRIGHT_EXCEPTIONS
#
# All code in this file is covered by the copyright detailed above,
# with the following explicit exceptions.  See doc strings for
# attribution and/or copyright information.
#
#   - superTuple
#
# END_COPYRIGHT_EXCEPTIONS

import getpass
import os
import subprocess
from collections import namedtuple
from operator import itemgetter

def superTuple(typename, *attribute_names):
    """Create and return a subclass of 'tuple', with named attributes.

    THIS FUNCTION IS OBSOLETE!  USE collections.namedtuple INSTEAD!!!

    Sample Usage:
        Employee = superTuple('Employee', 'name', 'phone', 'email')
        joe = Employee('Joe Smith', '617-555-1212', 'jsmith@example.com')
        if joe.phone.startswith('617-'):
            boston_employees.append(joe)

    @see "Python Cookbook", Martelli et. al., O'Reilly, 2nd ed.,
    recipe 6.7 "Implementing Tuples with Named Items".
    """
    # Make the subclass with appropriate __new__ and __repr__ methods.
    nargs = len(attribute_names)
    class supertup(tuple):
        __slots__ = () # save memory, we don't need per-instance dict
        def __new__(cls, *args):
            if len(args) != nargs:
                raise TypeError("%s takes exactly %d arguments (%d given)" % (
                    typename, nargs, len(args)))
            return tuple.__new__(cls, args)
        def __repr__(self):
            return '%s(%s)' % (typename, ', '.join(map(repr, self)))
    # Set up attribute names for gettable indices.
    for i, attr_name in enumerate(attribute_names):
        setattr(supertup, attr_name, property(itemgetter(i)))
    supertup.__name__ = typename
    return supertup


def getVerifiedPassword(prompt=None, verify=None):
    """Read and verify a password from the tty.

    @param prompt the prompt string for initial password entry
    @param verify the prompt string for verification
    """
    if prompt is None:
        prompt = "Password: "
    if verify is None:
        verify = "Re-enter password: "
    while True:
        p1 = getpass.getpass(prompt)
        p2 = getpass.getpass(verify)
        if p1 == p2:
            break
        try:
            with open("/dev/tty", "w") as F:
                print >>F, "Passwords do not match"
        except OSError:
            print >>sys.stderr, "Passwords do not match"
    return p1


def iquery(*args, **kwargs):
    """Run an iquery command.  First argument should be '-aq' or similar.

    Unfortunately the code in scidb_afl.py is very unpythonic, hence
    this minimalist iquery command wrapper.  SciDB host and port are
    taken from keyword args or from the environment in that order.

    Keyword Args:
    host - SciDB cluster host (e.g. $IQUERY_HOST, default 'localhost').
    port - SciDB query port (e.g. $IQUERY_PORT, default 1239).
    """
    cmd = ['iquery']
    if 'host' in kwargs:
        cmd.extend(['-c', kwargs['host']])
    elif 'IQUERY_HOST' in os.environ:
        cmd.extend(['-c', os.environ['IQUERY_HOST']])
    if 'port' in kwargs:
        cmd.extend(['-p', str(kwargs['port'])])
    elif 'IQUERY_PORT' in os.environ:
        cmd.extend(['-p', os.environ['IQUERY_PORT']])
    cmd.extend(map(str, args))
    p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    return p.communicate()[0]


def make_table(entry_name, query, **kwargs):
    """Build a list of named tuples based on result of the given AFL query.

    @param entry_name name of type to be created by collections.namedtuple
    @param query      AFL query from whose output we will make a table
    @param kwargs     keyword args for iquery() call

    Because the entire query result is read into memory, best to use
    this only with queries returning smallish results.

    All returned namedtuple fields are strings; we do not (yet)
    automatically convert AFL data types to their Python equivalents.

    An example:
    >>> t = make_table('ArrayTable', "list('arrays',true)")
    >>> all_versioned_array_ids = [x.aid for x in t if x.aid != x.uaid]
    """
    # Format tsv+:l gives dimension/attribute names for use as tuple attributes.
    table_data = iquery('-otsv+:l', '-aq', query, kwargs).splitlines()
    if "Error id:" in table_data:
        raise RuntimeError(table_data)
    # Sometimes SciDB can give the same label to >1 attribute; make them unique.
    attrs = []
    seen = dict()
    for label in table_data[0].split():
        if label in seen:
            seen[label] += 1
            label = '_'.join((label, str(seen[label])))
        else:
            seen[label] = 1
        attrs.append(label)
    # Create our data type and fill in the table.
    tuple_type = namedtuple(entry_name, attrs)
    table = []
    for line in table_data[1:]:
        table.append(tuple_type._make(line.split('\t')))
    return table
