#!/usr/bin/python

#
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
#

import base64
import getpass
import hashlib
import re
import sys

import site
site.addsitedir(sys.path[0]+'/../utils')
import scidblib
from scidblib import scidb_afl


# Example syntax:
# ./scripts/setup_scidb_example.py create_user <authentication_file> "Jon Doe"
# ./scripts/setup_scidb_example.py change_user_password <authentication_file> "Jon Doe"
# iquery -auth-file <authentication_file> -aq "show_user()"

class Scidb_Afl:
    def __init__(self, authentication_file = None):
        class TmpArgs:
            def __init__(self):
                self.host = ''
                self.port = ''

        args = TmpArgs()

        iquery = 'iquery'
        if authentication_file:
            iquery += ' --auth-file ' + authentication_file

        self._iquery_cmd = scidb_afl.get_iquery_cmd(args, iquery)


    def query(self, cmd,            \
        want_output=True,           \
        tolerate_error=False,       \
        verbose=False):
        """ Execute a SciDb AFL query

        @param cmd The SciDB command
        @param want_output If true we want the output
        @param tolerate_error If true, tolerate errors
        @param verbose If true, provide a higher level of verbosity
        """
        return scidb_afl.afl(       \
            self._iquery_cmd,       \
            cmd,                    \
            want_output,            \
            tolerate_error,         \
            verbose)

    def create_user(self, name, password):
        """ Create a user in SciDB

        @param name The name of the new user
        @param password The password for the new user
        """
        return self.query(                                          \
            'create_user(\'%s\', \'%s\')' %                         \
            (name, password))

    def show_user(self):
        """ Show the current SciDb user"""
        return self.query('show_user()')

    def is_library_loaded(self, name):
        """ Determine if the specified SciDB library is loaded

        @param name The name of the library to check for
        """
        libraries=self.query('list(\'libraries\')')
        for lib in libraries:
            if name in lib:
                return True

        return False

    def change_user(self, what_to_change, username, setting):
        """ Change a specific attribute of a SciDb user

        @param what_to_change The attribute to be changed
        @param username The name of the user the attribute is to be changed on
        @param setting The new setting of the attribute
        """
        return self.query(                                          \
            'change_user(\'%s\', \'%s\', \'%s\')' %                 \
            (what_to_change, username, setting))


def hashPassword(password):
    """ Given a password return Base64(SHA512(password))

    @param password The password to be used in the operation
    """
    hash_object = hashlib.sha512(password)
    hash_digest_b64 = base64.b64encode(hash_object.digest())
    hash_digest_b64 = re.sub(r'(.{64})(?!$)', r'\1\n', hash_digest_b64)
    return hash_digest_b64

def show_syntax(info):
    """ Show the syntax for the program"""
    if info == 'create_user':
        print 'Syntax:  ./setup_scidb_example.py create_user <authentication_file> new_username'
    elif info == 'change_user_password':
        print 'Syntax:  ./setup_scidb_example.py change_user_password <authentication_file> user_name'
    else:
        print 'Syntax:  ./setup_scidb_example.py create_user <authentication_file> new_username'
        print 'Syntax:  ./setup_scidb_example.py change_user_password <authentication_file> user_name'
        print 'Extra Info:  ' + info


def check_setup(afl):
    """ Verify the setup has the namespaces library loaded"""
    try:
        result = afl.show_user()
    except Exception, error:
        if not afl.is_library_loaded('namespaces'):
            print "ERROR:  The \'namespaces\' library is not loaded"
            print "        Please load the \'namespaces\' library in \'trust\' mode"
            print "        and then switch to \'password\' mode."
        else:
            print "ERROR:  The security setting in config.ini is not \'password\'"
        return

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


def change_user_password(argv):
    """ Change the users password.  Equivalent to:

    ./setup_scidb.py    change_user_password
                        <authentication_file>
                        change_user_name

    @param argv Contains the parameters necessary to change the user's password
    """
    if len(argv) != 4:
        show_syntax('change_user_password')

    authentication_file   = argv[2]
    change_user_name      = argv[3]
    change_user_password = getVerifiedPassword()

    _afl = Scidb_Afl(authentication_file)
    check_setup(_afl)

    try:
        result = _afl.change_user(              \
            'password',                         \
            change_user_name,                   \
            hashPassword(change_user_password));
        if 'Query was executed successfully' in result[0]:
            print 'Success'
        else:
            print 'ERROR:  Unable to change ' + change_user_name + '\'s password'
            print '        result=' + result
    except Exception, error:
        print 'ERROR:  Unable to change ' + change_user_name + '\'s password'
        print "Exception caught: %s" % str(error)

def create_user(argv):
    """ Create a new SciDb user.  Equivalent to:

        ./setup_scidb.py create_user <authentication_file> new_username
        @param argv Contains the parameters necessary to change the user's password
    """


    if len(argv) != 4:
        show_syntax('create_user')

    authentication_file = argv[2]
    new_username        = argv[3]
    new_user_password = getVerifiedPassword()

    _afl = Scidb_Afl(authentication_file)
    check_setup(_afl)

    try:
        result = _afl.create_user(              \
            new_username,                       \
            hashPassword(new_user_password));
        if 'Query was executed successfully' in result[0]:
            print 'Success'
        else:
            print 'ERROR:  Unable to create ' + new_username
            print '        result=' + result
    except Exception, error:
        print 'ERROR:  Unable to create ' + new_username


def main(argv):
    """ Primary program entry point"""
    if len(argv) >= 2:
        try:

            if argv[1] == "create_user":
                create_user(argv)

            elif argv[1] == "change_user_password":
                change_user_password(argv)

            else:
                show_syntax('main:  unknown ' + argv[1])

        except Exception, error:
            print "Exception caught: %s" % str(error)

    else:
        show_syntax('main:  arg list too small')

    sys.exit(0)


# -----------------------------------------------------------
# pythonic way to invoke main
# -----------------------------------------------------------
### MAIN
if __name__ == "__main__":
    main(sys.argv)

### end MAIN
