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

"""
Update a Postgres $HOME/.pgpass file.

See http://www.postgresql.org/docs/9.3/interactive/libpq-pgpass.html .

Author: Mike Leibensperger <mjl@paradigm4.com>


MODULE VARIABLES

These module constants are indices into the tuples returned by
PgpassUpdater.find():

    I_HOST  Database host
    I_PORT  Database port
    I_DB    Database name
    I_USER  Database user
    I_PASS  Database user's password
"""

import argparse
import getpass
import os
import pwd
import re
import stat
import subprocess as subp
import sys
import traceback

class PgpassError(Exception):
    pass

_args = None
_pgm = None
_DBG = False

def _dbg(self, *args):
    if _DBG:
        print >>sys.stderr, ' '.join(map(str, args))

# Indices of the pgpass fields within an entry.
I_HOST = 0
I_PORT = 1
I_DB = 2
I_USER = 3
I_PASS = 4
_N_FIELDS = 5

_PG_PORT = 5432                 # Default Postgres TCP port


class PgpassUpdater(object):

    """
    Class for automating updates to a Postgres $HOME/.pgpass file.

    Sample usage:
        from pgpass_updater import PgpassUpdater
        os_user = "scidb_linux_user"
        pup = PgpassUpdater(os_user)
        for cluster in clusters:
            pup.update("scidb_pg_user", "secret42", pg_db=cluster)
        print "Look at the nice pgpass file I'm writing:", pup
        pup.write_file()
    """

    _use_ipv4 = False

    def __init__(self, filename=None, os_user=None, mode_check=True):
        """Create an updater for an OS user's pgpass file.

        @param filename pgpass file, default ~os_user/.pgpass
        @param os_user  OS username, default is current user
        @param mode_check ensure file permissions are correct
        @throw PgpassError if something is wrong

        @note The only non-regular filename allowed is /dev/null.
        That's useful for testing when you don't want to default to
        your actual ~/.pgpass file.
        """
        if os_user is None:
            self._os_user = getpass.getuser()
        else:
            self._os_user = os_user
        if filename is None:
            pwent = pwd.getpwnam(self._os_user)
            if not pwent:
                raise PgpassError("User '%s' not in password database" % (
                        self._os_user))
            self._filename = os.sep.join((pwent.pw_dir, ".pgpass"))
        else:
            self._filename = os.path.expanduser(filename)
        self._lines = []        # Unparsed lines of the file
        self._entries = []      # Parsed entries for non-comment lines
        self._dirty = False     # Unless dirty, write_file() is a no-op
        if os.path.isfile(self._filename):
            if mode_check:
                self._do_mode_check()
            self._load()
        elif os.path.exists(self._filename) and self._filename != '/dev/null':
            raise PgpassError("%s is not a regular file" % self._filename)
        # ...else no such file (yet).

    @classmethod
    def use_ip4(cls, use=None):
        """Set/get whether to use IP addr or hostname when pg_host is omitted.

        @param use True: use IPv4 address, False: use hostname, None: no change
        @returns current setting
        """
        if use is None:
            return cls._use_ipv4
        prev = cls._use_ipv4
        cls._use_ipv4 = use
        return prev

    @staticmethod
    def _local_ipv4(_cache=[]):
        if not _cache:
            p = subp.Popen(["ip", "addr", "show", "eth0"], stdout=subp.PIPE)
            for line in p.communicate()[0].splitlines():
                fields = line.strip().split()
                if fields and fields[0] == "inet":
                    host = fields[1].split('/')[0] # cidr
                    break
            if not host:
                raise PgpassError("Cannot find IPv4 address for eth0")
            _cache.append(host)
        return _cache[0]

    @staticmethod
    def _local_short_hostname(_cache=[]):
        if not _cache:
            p = subp.Popen(["hostname"], stdout=subp.PIPE)
            host = p.communicate()[0].strip().split('.')[0]
            if not host:
                raise PgpassError("Cannot find local hostname")
            _cache.append(host)
        return _cache[0]

    @classmethod
    def default_host(cls, _cache=[None, None]):
        """Return the local host's IPv4 address or hostname.

        @description Depending on the current setting of
        PgpassUpdater.use_ip4(), return either the eth0 IPv4 address
        or the first component of the DNS hostname.
        """
        return _local_ipv4() if cls._use_ipv4 else _local_short_nostname()

    def filename(self):
        """Return the name of the file we read in."""
        return self._filename

    def os_user(self):
        """Return the user name of the purported owner of the .pgpass file."""
        return self._os_user

    def _do_mode_check(self):
        """Only secure .pgpass files are worth reading."""
        assert os.path.isfile(self._filename), "Should have already checked isfile"
        MUST_BE_OFF = stat.S_IRWXG | stat.S_IRWXO
        try:
            st = os.stat(self._filename)
        except OSError as e:
            if e.errno != errno.ENOENT:
                # Also rather redundant if os.path.isfile() was checked.
                raise PgpassError("Cannot stat(%s): %s" % (self._filename, e))
        else:
            if st.st_nlink != 1 or not stat.S_ISREG(st.st_mode) or (st.st_mode & MUST_BE_OFF):
                raise PgpassError("Permission check failed on %s" % self._filename)

    def _load(self):
        """Load and parse a .pgpass file."""
        with open(self._filename) as F:
            for line in F:
                self._lines.append(line.rstrip('\n'))
                line = line.strip()
                if line.startswith("#") or not line:
                    # Keep the indices identical for blank lines and comments.
                    self._entries.append(None)
                else:
                    self._entries.append(self._parse(line))

    def __len__(self):
        """Return the number of entries in this updater."""
        # Tricky because of comment lines, see _load() above.
        return reduce(lambda x, y: x if y is None else x+1, self._entries, 0)

    def __nonzero__(self):
        """Make sure that an empty updater is True in Boolean contexts.
        @returns True
        """
        return True

    @staticmethod
    def _encode(s):
        """Internal encoding: replace \: with ^A, \\ with ^B."""
        #return re.sub(r'\\\\', chr(2), re.sub(r'\\:', chr(1), s))
        s1 = re.sub(r'\\:', chr(1), s)
        s2 = re.sub(r'\\\\', chr(2), s1)
        return s2
        
    @staticmethod
    def _decode(s):
        """Internal decoding: replace ^A with \:, ^B with \\."""
        return re.sub(chr(2), r'\\\\', re.sub(chr(1), r'\\:', s))

    def _parse(self, line):
        """Parse an entry from a .pgpass line."""
        # The tricky part is the \-escaped \'s and :'s.  Replace them
        # with Ctrl-A and Ctrl-B temporarily, then re-replace them.
        return PgpassUpdater._encode(line).split(':')

    def _unparse(self, entry):
        """Create a .pgpass line from an entry."""
        return ':'.join(map(PgpassUpdater._decode, entry))

    def __str__(self):
        """Emit the updated file contents (without a trailing newline)."""
        assert len(self._entries) == len(self._lines)
        result = []
        for line, entry in zip(self._lines, self._entries):
            result.append(line if entry is None else self._unparse(entry))
        return '\n'.join(result) # No final crlf (so you can use print).

    def find(self, pg_user, pg_db, pg_host=None, pg_port=None):
        """Return a matching tuple from the loaded .pgpass file.

        @param pg_user user name
        @param pg_db   database name
        @param pg_host Postgres instance host, default is local hostname
        @param pg_port Postgres instance port, default _PG_PORT
        @return matching tuple or None if not found

        @note Any \-escapes will be removed from the returned tuple's fields.
        """
        if pg_port is None:
            pg_port = _PG_PORT
        for entry in (x for x in self._entries if x is not None):
            ent = map(PgpassUpdater._decode, entry)
            if ent[I_USER] != pg_user or ent[I_DB] != pg_db:
                continue
            if ent[I_PORT] != str(pg_port):
                continue
            if pg_host is None:
                if (pg_host != _local_ipv4() and
                    pg_host != _local_short_hostname()):
                    continue
            elif ent[I_HOST] != pg_host:
                continue
            # Decode, so list items won't be \-escaped.
            return tuple(map(PgpassUpdater._decode, ent))
        return None

    def update(self,
               pg_user, pg_pass, pg_db=None, pg_host=None, pg_port=None):
        """Set or update Postgres connection parameters for a user.

        @description
        Set the pg_user's password to pg_pass, in the specified
        database or (if pg_db is None) in all databases.  If the user
        is not named in any entry, and pg_db is specified, then a
        entry will be created for her based on pg_host/pg_port/db.

        @param pg_user Postgres user name
        @param pg_pass password
        @param pg_db   database name, aka SciDB cluster name, default all
        @param pg_host Postgres instance host, default is local hostname
        @param pg_port Postgres instance port, default 5432
        @returns count of updated entries
        @throws PgpassError
        """
        # Parameter validation.
        user = PgpassUpdater._encode(pg_user)
        passw = PgpassUpdater._encode(pg_pass)
        host = None if pg_host is None else PgpassUpdater._encode(pg_host)
        if pg_port is None:
            pg_port = _PG_PORT  # Unlike other args, port of None != wildcard
        try:
            int(pg_port)        # Must be a good int...
        except:
            raise PgpassError("Non-integer pg_port '{0}'".format(pg_port))
        else:
            port = str(pg_port) # ... but we want the str anyway.
        db = None if pg_db is None else PgpassUpdater._encode(pg_db)
        for x in (user, passw, host, db):
            if x is not None and (':' in x or r'\\' in x):
                raise PgpassError(
                    "Unescaped colon or backslash in update parameter '%s'" % x)
        if host is None:
            host = PgpassUpdater.default_host()
        assert host

        def matching_entry(ent):
            """Match host, port, db (None ==> wildcard), and user."""
            return (ent is not None and 
                    (host == ent[I_HOST]) and
                    (port == ent[I_PORT]) and
                    (db is None or db == ent[I_DB]))

        # Find and update all relevant entries.
        count = 0
        for i, e in enumerate(self._entries):
            if matching_entry(e):
                _dbg("Match:", e)
                self._entries[i][I_PASS] = passw
                self._lines[i] = self._unparse(self._entries[i])
                count += 1
            else:
                _dbg("No match:", e)
        if db and not count:
            # Hmmm, must need a new entry.
            self._entries.append([host, port, db, user, passw])
            self._lines.append(self._unparse(self._entries[-1]))
            _dbg("Add:", self._entries[-1])
            count = 1
        if count:
            self._dirty = True
        return count
        
    def write_file(self, filename=None):
        """Write modified contents to filename.

        @description If this updater has been modified since it was
        created, write its contents to the named file, creating a
        backup file as 'filename~' .  If unmodified, this method is a
        no-op.

        @param filename file to write, default is file we loaded from

        @note Unless this updater has actually been modified,
        """
        if self._dirty:
            if filename is None:
                filename = self._filename
                if filename == '/dev/null':
                    return      # Nothing to do.
            os.system("cp --backup /dev/null {0} ; chmod 0600 {0}".format(filename))
            with open(filename, 'w') as F:
                print >>F, self
            self._dirty = False

def _process():
    """Perform an entry update on the specified .pgpass file.

    Only the --update verb is supported at present, so let's get to it.
    """
    assert _args.update, "How did you sneak by without a verb?"
    fname = _args.pgpassfile if _args.pgpassfile else os.path.expanduser("~/.pgpass")
    if not _args.user:
        raise PgpassError("Required -u/--user option is missing")
    if not _args.password:
        raise PgpassError("Required -p/--password option is missing")
    elif _args.password == '-':
        _args.password = sys.stdin.readline().rstrip('\n')
    pup = PgpassUpdater(filename=fname)
    pup.update(_args.user, _args.password, _args.database, _args.host, _args.port)
    pup.write_file()
    return 0

def main(argv=None):
    """Update the current user's ~/.pgpass file."""
    if argv is None:
        argv = sys.argv

    _pgm = "%s:" % os.path.basename(argv[0]) # colon for easy use by print

    parser = argparse.ArgumentParser(
        description="Update the current user's ~/.pgpass file.",
        epilog='Type "pydoc %s" for more information.' % _pgm[:-1])
    verb = parser.add_mutually_exclusive_group(required=True)
    verb.add_argument('-U', '--update', action='store_true',
                      help="Update a possibly non-existent entry in ~/.pgpass")
    parser.add_argument('-u', '--user', help='A database username')
    parser.add_argument('-H', '--host', help='A database hostname')
    parser.add_argument('-P', '--port', help='Database TCP port')
    parser.add_argument('-d', '--database', help='A database name')
    parser.add_argument('-p', '--password', help='A database password.  Use - to read it from stdin.')
    parser.add_argument('pgpassfile', nargs='?', help='The pgpass file to work with, default ~/.pgpass')
    
    global _args
    _args = parser.parse_args(argv[1:])

    try:
        return _process()
    except PgpassError as e:
        print >>sys.stderr, _pgm, e
        return 1
    except Exception as e:
        print >>sys.stderr, _pgm, "Unhandled exception:", e
        traceback.print_exc()   # always want this for unexpected exceptions
        return 2

if __name__ == '__main__':
    sys.exit(main())
