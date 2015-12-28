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
import hashlib
import re
import sys

import site
site.addsitedir(sys.path[0]+'/../utils')
import scidblib
from scidblib import scidb_afl


# Example syntax:
# ./scripts/setup_scidb_example.py create_user Paradigm4 "Jon Doe" 'hello world'
# ./scripts/setup_scidb_example.py change_user_password "'Jon Doe'" "'hello world'" "Jon Doe" 'try this'
# iquery -U 'Jon Doe' -P 'try this' -aq "who_am_i()"

class Scidb_Afl:
    # ------------------------------------------------------------------
    # Scidb_Afl::__init__
    # ------------------------------------------------------------------
    def __init__(self, login_username = None, login_userpassword = None):
        class TmpArgs:
            def __init__(self):
                self.host = ''
                self.port = ''

        args = TmpArgs()

        iquery = 'iquery'
        if login_username:
            iquery += ' -U ' + login_username

        if login_userpassword:
            iquery += ' -P ' + login_userpassword

        self._iquery_cmd = scidb_afl.get_iquery_cmd(args, iquery)


    # ------------------------------------------------------------------
    # Scidb_Afl::query
    # ------------------------------------------------------------------
    def query(self, cmd,            \
        want_output=True,           \
        tolerate_error=False,       \
        verbose=False):

        return scidb_afl.afl(       \
            self._iquery_cmd,       \
            cmd,                    \
            want_output,            \
            tolerate_error,         \
            verbose)


    # ------------------------------------------------------------------
    # Scidb_Afl::create_user
    # ------------------------------------------------------------------
    def create_user(self, name, password):
        return self.query(                                          \
            'create_user(\'%s\', \'%s\')' %                         \
            (name, password))

    # ------------------------------------------------------------------
    # Scidb_Afl::who_am_i
    # ------------------------------------------------------------------
    def who_am_i(self):
        return self.query('who_am_i()')

    # ------------------------------------------------------------------
    # Scidb_Afl::is_library_loaded
    # ------------------------------------------------------------------
    def is_library_loaded(self, name):
        libraries=self.query('list(\'libraries\')')
        for lib in libraries:
            if name in lib:
                return True

        return False

    # ------------------------------------------------------------------
    # Scidb_Afl::change_user
    # ------------------------------------------------------------------
    def change_user(self, what_to_change, username, setting):
        return self.query(                                          \
            'change_user(\'%s\', \'%s\', \'%s\')' %                 \
            (what_to_change, username, setting))


# -----------------------------------------------------------
def hashPassword(password):
    hash_object = hashlib.sha512(password)
    hash_digest_b64 = base64.b64encode(hash_object.digest())
    hash_digest_b64 = re.sub(r'(.{64})(?!$)', r'\1\n', hash_digest_b64)
    return hash_digest_b64

# ----------------------------------------------------------------------
# ::show_syntax
# ----------------------------------------------------------------------
def show_syntax(info):
    print "setup_scidb_example.py is provided as an example only,"
    print "                       use at your own risk"

    if info == 'create_user':
        print 'Syntax:  ./setup_scidb_example.py create_user root_password new_username new_user_password'
    elif info == 'change_user_password':
        print 'Syntax:  ./setup_scidb_example.py change_user_password login_user_name login_user_password user_name new_password'
    else:
        print 'Syntax:  ./setup_scidb_example.py change_user_password login_user_name login_user_password user_name new_password'
        print 'Syntax:  ./setup_scidb_example.py create_user root_password new_username new_user_password'
        print 'Extra Info:  ' + info


# -----------------------------------------------------------
# -----------------------------------------------------------
def check_setup(afl):
    try:
        result = afl.who_am_i()
    except Exception, error:
        if not afl.is_library_loaded('authpw'):
            print "ERROR:  The \'authpw\' library is not loaded"
            print "        Please load the \'authpw\' library in \'trust\' mode"
            print "        and then switch to \'password\' mode."
        else:
            print "ERROR:  The security setting in config.ini is not \'password\'"
        return

# -----------------------------------------------------------
# ./setup_scidb.py change_user_password login_user_name login_user_password change_user_name change_user_password'
# -----------------------------------------------------------
def change_user_password(argv):
    if len(argv) != 6:
        show_syntax('change_user_password')

    login_user_name       = argv[2]
    login_user_password   = argv[3]
    change_user_name      = argv[4]
    change_user_password  = argv[5]

    _afl = Scidb_Afl(login_user_name, login_user_password)
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

# -----------------------------------------------------------
#  ./setup_scidb.py create_user root_password new_username new_user_password'
# -----------------------------------------------------------
def create_user(argv):
    if len(argv) != 5:
        show_syntax('create_user')

    root_password     = argv[2]
    new_username      = argv[3]
    new_user_password = argv[4]

    _afl = Scidb_Afl('root', root_password)
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

# -----------------------------------------------------------
def main(argv):
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
