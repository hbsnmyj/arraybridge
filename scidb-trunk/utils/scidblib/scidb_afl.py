#!/usr/bin/env python

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

import sys
import os
import subprocess
import math
import argparse
import datetime
import re
import traceback
import copy
import csv
from StringIO import StringIO
import scidblib
import scidblib.util

# ----------------------------------------------------------------------
# get_iquery_cmd
# ----------------------------------------------------------------------
def get_iquery_cmd(args = None, base_iquery_cmd = 'iquery -o dcsv'):
    """Change iquery_cmd to be base_iquery_cmd followed by optional parameters host and/or port from args.

    @param args      argparse arguments that may include host and port.
    @param base_iquery_cmd the iquery command without host or port.
    @return the iquery command which starts and ends with a whitespace.
    """
    iquery_cmd = ' ' + base_iquery_cmd + ' '
    if args and args.host:
        iquery_cmd += '-c ' + args.host + ' '
    if args and args.port:
        iquery_cmd += '-p ' + args.port + ' '
    if args and hasattr(args, 'auth_file') and args.auth_file:
        iquery_cmd += "-A " + args.auth_file + ' '
    return iquery_cmd

# ----------------------------------------------------------------------
# execute_it_return_out_err
# ----------------------------------------------------------------------
def execute_it_return_out_err(cmd):
    """Execute one command, and return the data of STDOUT and STDERR.

    @param cmd   the system command to execute.
    @return a tuple (stdoutdata, stderrdata)
    @note It is up to the caller to decide whether to throw.
    """
    p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    return p.communicate()

# ----------------------------------------------------------------------
# verbose_afl_result_line_start
# ----------------------------------------------------------------------
def verbose_afl_result_line_start(want_re=False):
    """The beginning of a result line, if afl() or time_afl() is called with verbose=True.

    @param want_re  whether a regular expression is needed.
    @return the line start string (if want_re=False), or pattern (if want_re=True)
    """
    if want_re:
        return r'^\s\s---\sExecuted\s'
    else:
        return '  --- Executed '

# ----------------------------------------------------------------------
# afl
# ----------------------------------------------------------------------
def afl(iquery_cmd, query, want_output=False, tolerate_error=False, verbose=False):
    """Execute an AFL query.

    @param iquery_cmd     the iquery command.
    @param query          the AFL query.
    @param want_output    requesting iquery to output query result.
    @param tolerate_error whether to keep silent when STDERR is not empty.
                          A use case is when trying to delete an array which may or may not exist.
    @return (stdout_data, stderr_data)
    @exception AppError if STDERR is not empty and the caller says tolerate_error=False.
    """
    full_command = iquery_cmd + ' -'
    if not want_output:
        full_command += 'n'
    full_command += "aq \"" + query + "\""
    out_data, err_data = execute_it_return_out_err(full_command)
    if not tolerate_error and len(err_data)>0:
        raise scidblib.AppError('The AFL query, [' + query + '], failed with the following error:\n' +
                        err_data)
    if verbose:
        print verbose_afl_result_line_start() + '%s.' % query
    return (out_data, err_data)

# ----------------------------------------------------------------------
# time_afl
# ----------------------------------------------------------------------
def time_afl(iquery_cmd, query, verbose=False):
    """Execute an AFL query, and return the execution time.

    @param iquery_cmd the iquery command.
    @param query  the AFL query.
    @return the execution time.
    @exception AppError if the error did not execute successfully.
    """
    full_command = '/usr/bin/time -f \"%e\" ' + iquery_cmd + ' -naq \"' + query + "\" 1>/dev/null"
    out_data, err_data = execute_it_return_out_err(full_command)
    try:
        t = float(err_data)
        if verbose:
            print verbose_afl_result_line_start() + '%s in %f seconds.' % (query, t)
        return t
    except ValueError:
        raise scidblib.AppError('Timing the AFL query ' + query + ', failed with the following error:\n' +
                        err_data)

# ----------------------------------------------------------------------
# single_cell_afl
# ----------------------------------------------------------------------
def single_cell_afl(iquery_cmd, query, num_attrs):
    """Execute an AFL query that is supposed to return a single cell, and return the attribute values.

    The return type is either a scalar (if num_attrs=1), or a list (if num_attrs>1).
    @example
      - scaler_result1 = single_cell_afl(iquery_cmd, cmd, 1)
      - scaler_result1, scaler_result2 = single_cell_afl(iquery_cmd, cmd, 2)

    @param iquery_cmd the iquery command
    @param query the query.
    @param num_attrs the expected number of attributes in the return array.
    @return the attribute value (if num_attrs=1), or a list of attribute values (if num_attrs>1)
    @exception AssertionError if num_attrs is not a positive integer.
    @exception AppError if either the query fails, or the query result is not single cell,
                     or the actual number of attributes is not num_attrs.
    """
    assert isinstance(num_attrs, (int, long)) and num_attrs>0, \
        'AssertionError: single_cell_afl must be called with a positive num_attrs.'
    out_data, err_data = afl(iquery_cmd, query, want_output=True)
    lines = out_data.strip().split('\n')
    if len(lines) != 2:
        raise scidblib.AppError('The afl query, ' + query + ', is supposed to return two lines including header; but it returned ' +
                        str(len(lines)) + ' lines.')

    class DcsvDialect(csv.excel):
        """Dialect slightly tweaked from csv.excel, as a parameter to csv.reader."""
        def __init__(self):
            csv.excel.__init__(self)
            self.quotechar = "'"
            self.lineterminator = '\n'

    re_result = r'^\{0\}\s([^\n]+)$'  # A single-cell afl query returns result at row 0.
    match_result = re.match(re_result, lines[1], re.M|re.I)
    if not match_result:
        raise scidblib.AppError('The afl query, ' + query + ', did not generate ' + str(num_attrs) + ' attributes as expected.')

    string_io = StringIO(match_result.group(1))
    csv_reader = csv.reader(string_io, DcsvDialect())
    row = csv_reader.next()
    if len(row) != num_attrs:
        raise scidblib.AppError('The afl query, ' + query + ', did not generate ' + str(num_attrs) + ' attributes as expected.')
    if num_attrs==1:
        return row[0]
    return row

# ----------------------------------------------------------------------
# get_num_instances
# ----------------------------------------------------------------------
def get_num_instances(iquery_cmd = None):
    """Get the number of SciDB instances.

    @param iquery_cmd  the iquery command to use.
    @return the number of SciDB instances acquired by AFL query list('instances')
    @exception AppError if SciDB is not running or if #instances <= 0 (for whatever reason)
    """
    if not iquery_cmd:
        iquery_cmd = get_iquery_cmd()
    query = 'list(\'instances\')'
    out_data, err_data = afl(iquery_cmd, query, want_output=True)
    num_lines = len(out_data.strip().split('\n'))
    if num_lines < 2:
        raise scidblib.AppError(query + ' is expected to return at least two lines.')
    return num_lines - 1  # skip the header line

# ----------------------------------------------------------------------
# get_instances_info
# ----------------------------------------------------------------------
def get_instances_info(iquery_cmd = None):
    """Get the info returned by the list('instances') query as a list of lists.

    @param iquery_cmd  the iquery command to use.
    @return info returned by AFL query list('instances') as list of lists
    @exception AppError if SciDB is not running or if #instances <= 0 (for whatever reason)
    """
    iquery_args = scidblib.util.superTuple('args','host','port')
    iquery_args.host = os.environ.get('IQUERY_HOST',None)
    iquery_args.port = os.environ.get('IQUERY_PORT',None)

    if not iquery_cmd:
        iquery_cmd = get_iquery_cmd(args=iquery_args,base_iquery_cmd='iquery -o csv:l')

    query = 'list(\'instances\')'
    out_data, err_data = afl(iquery_cmd, query, want_output=True)

    lines = [line.strip() for line in out_data.strip().split('\n')]
    if (len(lines) < 2):
        raise scidblib.AppError(query + ' is expected to return at least two lines.')

    tokenized_lines = [[t.strip().replace('\'','') for t in line.split(',')] for line in lines[1:]]

    return tokenized_lines

# ----------------------------------------------------------------------
# csv_splitter
#
# Splits a string based on commas, but ignores any commas that are
# embedded between the string_delimiter (which in the default case is a
# double quote).  So, "abc, def, \"ghi, jkl\", mno, pqr" will be split
# into the following:
#    abc
#    def
#    "ghi, jkl"
#    mno
#    pqr
#
#    tests:
#
#    params = scidb_afl.csv_splitter("abc, def, \"ghi, jkl\", mno, pqr", string_delimiter='\"')
#    print "params=" + str(params)
#    operators = scidb_afl.get_operators()
#    print "operators=" + str(operators)
# ----------------------------------------------------------------------
def csv_splitter(line, string_delimiter='\"'):
    output = []
    try:
        lines=[line]
        output = csv.reader(lines,
            quotechar=string_delimiter,
            delimiter=',',
            quoting=csv.QUOTE_ALL,
            skipinitialspace=True)
        output=next(output)
    except Exception, error:
        raise scidblib.AppError(
            'csv_splitter exception '
            + str(error)
            + " on line="
            + line)

    return output

# ----------------------------------------------------------------------
# get_operators
# ----------------------------------------------------------------------
def get_operators(iquery_cmd = None):
    """Get a list of array names.
    @return a list of operators and the associated libraries that are in SciDB, returned by AFL query list('operators').
    Example usage:
        operators = scidb_afl.get_operators()
        for (operator, library) in operators:
            print "operator=" + operator + " library=" + library
    """

    if not iquery_cmd:
        iquery_cmd = get_iquery_cmd()

    query = 'list(\'operators\');'
    out_data, err_data = afl(iquery_cmd, query, want_output=True)
    lines = out_data.strip().splitlines()
    if not lines:
        raise scidblib.AppError(query + ' is expected to return at least one line.')

    list=[]
    for line in lines:
        try:
            parse_line=line[line.find(" ") + 1:]
            operator, library=csv_splitter(parse_line, "\'")
            list.append((operator, library))
        except Exception, error:
            pass

    return list

# ----------------------------------------------------------------------
# find_operator
# ----------------------------------------------------------------------
def find_operator(operators, name, library):
    return filter(lambda x: x[0] == name and x[1] == library, operators)


# ----------------------------------------------------------------------
# set_namespace_cmd
# ----------------------------------------------------------------------
def make_set_namespace_cmd(namespace, iquery_cmd = None):
    set_namespace_cmd=''

    op_set_namespace = find_operator(
        get_operators(iquery_cmd),
        name='set_namespace',
        library='namespaces')

    if op_set_namespace and namespace:
        set_namespace_cmd="set_namespace('" + namespace + "'); "

    return set_namespace_cmd

# ----------------------------------------------------------------------
# remove_array
# ----------------------------------------------------------------------
def remove_array(arrayName, namespace=None, iquery_cmd = None):
    """Remove an array from scidb
    @param arrayName the name of the array to remove
    @param namespace the namespace in which the array resides, None=public
    @param iquery_cmd  the iquery command to use.
    @exception AppError if SciDB is not running or if the AFL query failed.
    """
    if not iquery_cmd:
        iquery_cmd = get_iquery_cmd()


    if namespace and (namespace != 'public'):
        query = ';'.join((make_set_namespace_cmd(namespace, iquery_cmd),
                          'remove(%s)' % arrayName))
        expected='Query was executed successfully\nQuery was executed successfully\n'
    else:
        query = 'remove(%s)' % arrayName
        expected='Query was executed successfully\n'

    out_data, err_data = afl(iquery_cmd, query, want_output=True)

    if out_data != expected:
        failureMsg='Cannot remove array ', arrayName
        if namespace:
            ' from namespace ', namespace
        failureMsg += "\nout_data=", out_data
        failureMsg += "\nexpected=", expected
        raise scidblib.AppError(failureMsg)


# ----------------------------------------------------------------------
# get_array_names
# ----------------------------------------------------------------------
def get_array_names(iquery_cmd = None, temp_only=False, versions=False, namespace=None):
    """Get a list of array names.
    @param iquery_cmd  the iquery command to use.
    @param temp_only   only get the names of temporary arrays.
    @param versions    set to true if interested in getting all array names and their versions
    @param namespace used to specify a namespace prior to getting the arrays
    @return a list of array names that are in SciDB, returned by AFL query project(list(), name).
    @exception AppError if SciDB is not running or if the AFL query failed.
    """
    if not iquery_cmd:
        iquery_cmd = get_iquery_cmd()


    set_namespace_cmd=''
    if namespace and (namespace != 'public'):
        set_namespace_cmd=make_set_namespace_cmd(namespace, iquery_cmd)

    query_arrays="list('arrays')"
    if versions:
        query_arrays="list('arrays', true)"

    if temp_only:
        query_arrays="filter({0}, temporary=true)".format(query_arrays)

    query = set_namespace_cmd + 'project({0}, name);'.format(query_arrays)

    out_data, err_data = afl(iquery_cmd, query, want_output=True)
    lines = out_data.strip().splitlines()
    if not lines:
        raise scidblib.AppError(query + ' is expected to return at least one line.')
    ret = []

    if set_namespace_cmd == '':
        start = 1
    else:
        start = 2
        if not lines[0].startswith('Query was executed successfully'):
            raise scidblib.AppError(
                iquery_cmd, ' ',  query, ' failed - result=', lines[0])

    for line in lines[start:]:  # Skip the header line.
        re_name = r'^\{\d+\}\s\'(.+)\'$'  # e.g.: {4} 'MyArray'
        match_name = re.match(re_name, line)
        if not match_name:
            raise scidblib.AppError(
                'get_array_names() failed to parse: ['
                + line
                + "] the expected format is {No} 'name'\nquery=",iquery_cmd, " ", query, " -- start=",str(start))

        ret.append(match_name.group(1))
    return ret

# ----------------------------------------------------------------------
# get_namespace_names
# ----------------------------------------------------------------------
def get_namespace_names(iquery_cmd = None):
    """Get a list of namespace names.

    @param iquery_cmd  the iquery command to use.
    @return a list of namespace names that are in SciDB, returned by AFL query project(list('namespaces'), name).
    @exception AppError if SciDB is not running or if the AFL query failed.
    """
    if not iquery_cmd:
        iquery_cmd = get_iquery_cmd()
    query = 'project(list(\'namespaces\'), name)'
    out_data, err_data = afl(iquery_cmd, query, want_output=True)
    lines = out_data.strip().splitlines()
    if not lines:
        raise scidblib.AppError(query + ' is expected to return at least one line.')
    ret = []
    for line in lines[1:]:  # Skip the header line.
        re_name = r'^\{\d+\}\s\'(.+)\'$'  # e.g.: {4} 'MyNamespace'
        match_name = re.match(re_name, line)
        if not match_name:
            raise scidblib.AppError('I don\'t understand the result line ' + str(i+1) + ': ' + line)
        ret.append(match_name.group(1))
    return ret

# ----------------------------------------------------------------------
# get_role_names
# ----------------------------------------------------------------------
def get_role_names(iquery_cmd = None):
    """Get a list of role names.

    @param iquery_cmd  the iquery command to use.
    @return a list of namespace names that are in SciDB, returned by AFL query project(list('namespaces'), name).
    @exception AppError if SciDB is not running or if the AFL query failed.
    """
    if not iquery_cmd:
        iquery_cmd = get_iquery_cmd()
    query = 'project(list(\'roles\'), name)'
    out_data, err_data = afl(iquery_cmd, query, want_output=True)
    lines = out_data.strip().splitlines()
    if not lines:
        raise scidblib.AppError(query + ' is expected to return at least one line.')
    ret = []
    for line in lines[1:]:  # Skip the header line.
        re_name = r'^\{\d+\}\s\'(.+)\'$'  # e.g.: {4} 'MyNamespace'
        match_name = re.match(re_name, line)
        if not match_name:
            raise scidblib.AppError('I don\'t understand the result line ' + str(i+1) + ': ' + line)
        ret.append(match_name.group(1))
    return ret

# ----------------------------------------------------------------------
# get_user_names
# ----------------------------------------------------------------------
def get_user_names(iquery_cmd = None):
    """Get a list of user names.

    @param iquery_cmd  the iquery command to use.
    @return a list of namespace names that are in SciDB, returned by AFL query project(list('namespaces'), name).
    @exception AppError if SciDB is not running or if the AFL query failed.
    """
    if not iquery_cmd:
        iquery_cmd = get_iquery_cmd()
    query = 'project(list(\'users\'), name)'
    out_data, err_data = afl(iquery_cmd, query, want_output=True)
    lines = out_data.strip().splitlines()
    if not lines:
        raise scidblib.AppError(query + ' is expected to return at least one line.')
    ret = []
    i=1
    for line in lines[1:]:  # Skip the header line.
        re_name = r'^\{\d+\}\s\'(.+)\'$'  # e.g.: {4} 'username'
        match_name = re.match(re_name, line)
        if not match_name:
            raise scidblib.AppError('I don\'t understand the result line ' + str(i+1) + ': ' + line)
        ret.append(match_name.group(1))
        i += 1
    return ret

class LibraryInfo(object):
    """A class for storing the library information for an individual SciDb library"""
    def __init__(self):
        self._is_valid = False

    def __init__(self, inst, n, name, major, minor, patch, build, build_type="null"):
        self._inst          = inst
        self._n             = n
        self._name          = name
        self._major         = major
        self._minor         = minor
        self._patch         = patch
        self._build         = build
        self._build_type    = build_type
        self._is_valid      = True

    def get_inst(self):
        return self._inst

    def get_n(self):
        return self._n

    def get_name(self):
        return self._name

    def get_major(self):
        return self._major

    def get_minor(self):
        return self._minor

    def get_patch(self):
        return self._patch

    def get_build(self):
        return self._build

    def get_build_type(self):
        return self._build_type

    def is_valid(self):
        return self._is_valid

    def __str__(self):
        if True == self.is_valid():
            result= "inst={0} n={1} name={2} major={3} minor={4} patch={5} build={6} build_type={7}".format(
                self._inst, self._n, self._name, self._major, self._minor,
                self._patch, self._build, self._build_type)
        else:
            result= "Uninitialized library"

        return result


# ----------------------------------------------------------------------
# get_libraries
# ----------------------------------------------------------------------
def get_libraries(iquery_cmd = None):
    """Get a list of user names.

    @param iquery_cmd  the iquery command to use.
    @return a tuple containing a list of library info objects (found in SciDb) and err_data
        ([LibraryInfo(...)], err_data)
    @exception AppError if SciDB is not running or if the AFL query failed.
    """
    if not iquery_cmd:
        iquery_cmd = get_iquery_cmd()
    query = "list('libraries')"
    out_data, err_data = afl(iquery_cmd, query, want_output=True)
    if len(err_data) > 0:
        raise scidblib.AppError("Failed to list('libraries')\nerr={0}".format(err_data))

    # Successful query execution - out_data format
    # {inst,n} name,major,minor,patch,build,build_type
    # {0,0} 'SciDB',15,12,0,1,'Debug'
    # {1,0} 'SciDB',15,12,0,1,'Debug'
    # {2,0} 'SciDB',15,12,0,1,'Debug'
    # {3,0} 'SciDB',15,12,0,1,'Debug'

    lines = out_data.strip().splitlines()
    if not lines:
        raise scidblib.AppError(query + ' is expected to return at least one line.')

    ret = []
    i=1
    regex1 = re.compile(r'^\{(\d+)\,(\d+)\}\s\'(.+)\',(\d+),(\d+),(\d+),(\d+),\s*\'(.+)\'$')
    regex2 = re.compile(r'^\{(\d+)\,(\d+)\}\s\'(.+)\',(\d+),(\d+),(\d+),(\d+),\s*null$')
    for line in lines[1:]:  # Skip the header line.
        match1 = regex1.match(line)
        if match1:
            inst, n, name, major, minor, patch, build, build_type = match1.groups()
            libInfo = LibraryInfo(inst, n, name, major, minor, patch, build, build_type)
            ret.append(libInfo)
        else:
            match2 = regex2.match(line)
            if match2:
                inst, n, name, major, minor, patch, build = match2.groups()
                libInfo = LibraryInfo(inst, n, name, major, minor, patch, build)
                ret.append(libInfo)
            else:
                raise scidblib.AppError(
                    "I don\'t understand the result line {0} :\n{1}".format(i+1, line))

        i += 1
    return ret

# ----------------------------------------------------------------------
# is_library_loaded
# ----------------------------------------------------------------------
def is_library_loaded(name, iquery_cmd = None):
    """Determine whether a SciDb library is loaded

    @param name The name of the library
    @param iquery_cmd The iquery command to use to submit the query
    """
    if not iquery_cmd:
        iquery_cmd = get_iquery_cmd()

    libraries = get_libraries(iquery_cmd)
    print "libraries:"
    for lib in libraries:
        print str(lib)

    lib_name = "lib{0}.so".format(name)
    for lib in libraries:
        if (name == lib.get_name()) or (lib_name == lib.get_name()):
            return True

    return False
