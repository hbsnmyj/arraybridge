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

import argparse
import sys
import os
import pwd
import re
import types
import subprocess
import time
import itertools
import functools
import signal
import textwrap

import scidblib.scidb_schema as SCHEMA

from collections import namedtuple
from distutils.util import strtobool

_args = None # Global reference to program args.

def print_and_flush(msg,stream=sys.stdout):
    """ Write the string message to the specified output stream and flush that
    stream.
    @param msg string message to write
    @param stream output stream (sys.stdout, sys.stderr, etc.)
    """
    stream.write('{0}\n'.format(msg))
    stream.flush()

class AppError(Exception):
    """ Exception class that wraps backup utility errors.
    """
    pass

#-------------------------------------------------------------------------------
class ProgramArgs:
    # Can anyone tell me why a bare argparse.Namespace object is not sufficient?
    def __init__(self,args):
        descMsg = textwrap.dedent("""
            Backup and restore utility for SciDB array data.

            The utility saves/restores scidb arrays into folder(s) specified
            by the user (positional dir argument).  It must be run on the
            coordinator host in either save or restore mode (--save or
            --restore options respectively).  Each array will be saved into
            its own file under the specified folder(s). The utility treats
            positional dir argument as a "base" folder name: if --parallel
            option is not used, then all arrays are saved into the base
            folder on the coordinator.  If --parallel option is specified,
            then the utility saves all arrays into multiple folders (one for
            each scidb instance).  These parallel folders are saved on their
            respective cluster hosts: host0 with instances 0 and 1 will have
            folders base.0 and base.1 while host1 with instances 2 and 3
            will have folders base.2 and base.3. Conversely, when user
            restores the previously saved arrays, the restore operation must
            be run with the same options as the original save (that is, if
            --parallel option was used to save, then it has to be specified
            during the restore).  By default, the utility saves all arrays.
            However, if user decides to save only a few arrays, that can be
            accomplished by filtering the array names via --filter option.

            IMPORTANT: iquery executable must be in the PATH.
        """)

        self._parser = argparse.ArgumentParser(
            description=descMsg,
            epilog='To run the program, either save or restore option is required.'
            )
        self._parser.add_argument(
            'dir',
            type=types.StringType,
            help='backup/restore directory (absolute path)'
            )
        self._parser.add_argument(
            '-s',
            '--save',
            metavar='SAVE',
            type=types.StringType,
            nargs=1,
            choices=['binary','text','store','opaque'],
            help="""Format for saving array data in files (allowed values are binary, text, store, or opaque).
                 Using text format to backup/restore array data is not recommended because overlap
                 regions are not saved; use store format instead."""
            )
        self._parser.add_argument(
            '-r',
            '--restore',
            metavar='RESTORE',
            type=types.StringType,
            nargs=1,
            choices=['binary','text','store','opaque'],
            help="""Format for saving array data in files (allowed values are binary, text, store, or opaque).
                 Using text format to backup/restore array data is not recommended because overlap
                 regions are not saved; use store format instead."""
            )
        self._parser.add_argument(
            '-d',
            '--delete',
            default=False,
            action='store_true',
            help='delete all saved array data files and folder(s) on all cluster hosts'
            )
        self._parser.add_argument(
            '--port',
            default='1239',
            type=types.StringType,
            help='network port used by iquery'
            )
        self._parser.add_argument(
            '--ssh-port',
            default=22,
            type=int,
            help='network port to use for ssh communication'
            )
        self._parser.add_argument(
            '--host',
            default='localhost',
            type=types.StringType,
            help='network host used by iquery'
            )
        self._parser.add_argument(
            '-a',
            '--allVersions',
            default=False,
            action='store_true',
            help='saves/restores all versions of arrays (potentially a lot of data)'
            )
        self._parser.add_argument(
            '-f',
            '--filter',
            metavar='PATTERN',
            type=types.StringType,
            nargs=1,
            help="""Python regex pattern to match against array names (escape and/or quote as
                 necessary to avoid shell expansion)"""
            )
        self._parser.add_argument(
            '-p',
            '--parallel',
            default=False,
            action='store_true',
            help='backup/restore array data in parallel mode (to all scidb instances simultaneously)'
            )
        self._parser.add_argument(
            '-z',
            '--zip',
            default=False,
            action='store_true',
            help='compress data files with gzip when saving/restoring array data'
            )
        self._parser.add_argument(
            '--force',
            default=False,
            action='store_true',
            help='force silent removal of arrays before restoring them'
            )
        self._parser.add_argument(
            '--auth-file',
            type=types.StringType,
            default=None,
            help='Authentication file for SciDB user.'
            )

        # Put all of the parser arguments and values into a dictionary for
        # easier retrieval.
        self._args = self._parser.parse_args(args)
        self._argsDict = {}
        self._argsDict['dir'] = os.path.abspath(self._args.dir)
        self._argsDict['allVersions'] = self._args.allVersions
        self._argsDict['parallel'] = self._args.parallel
        self._argsDict['save'] = self._args.save
        self._argsDict['restore'] = self._args.restore
        self._argsDict['delete'] = self._args.delete
        self._argsDict['filter'] = self._args.filter
        self._argsDict['zip'] = self._args.zip
        self._argsDict['force'] = self._args.force
        self._argsDict['host'] = self._args.host
        self._argsDict['port'] = self._args.port
        self._argsDict['ssh_port'] = self._args.ssh_port
        self._argsDict['auth-file'] = self._args.auth_file

        if (types.ListType == type(self._argsDict['save'])):
            self._argsDict['save'] = self._argsDict['save'][0]

        if (types.ListType == type(self._argsDict['restore'])):
            self._argsDict['restore'] = self._argsDict['restore'][0]

        if (types.ListType == type(self._argsDict['filter'])):
            self._argsDict['filter'] = self._argsDict['filter'][0]

        if self._argsDict['filter'] is not None:
            self._argsDict['filter'] = self._argsDict['filter'].replace('\"','')
            self._argsDict['filter'] = self._argsDict['filter'].replace('\'','')

    #-------------------------------------------------------------------------
    # get: small function for argument value retrieval
    def get(self,arg):
        if (arg in self._argsDict.keys()):
            return self._argsDict[arg]
        return None
    #-------------------------------------------------------------------------
    # getAllArgs: small function for getting the entire dictionary of
    #             command line arguments.
    def getAllArgs(self):
        D = {}
        D.update(self._argsDict.iteritems())
        return D
    def print_help(self):
        self._parser.print_help()


def yesNoQuestion(q):
    while True:
        try:
            return strtobool(raw_input(q).lower())
        except ValueError:
            sys.stdout.write('Please type \'y\' or \'n\'.\n')

def no_ver_name(name):
    """Strip version suffix from array name."""
    return name.split('@')[0]

# This is a normalized Manifest entry.
ArrayMetadata = namedtuple('ArrayMetadata',
                           ("name",    # array name
                            "ns",      # namespace, default 'public'
                            "format",  # format, may be per-array for binary backups
                            "uaid",    # unversioned array id (v0 compatibility)
                            "file_name", # where array data is stored
                            "schema",  # schema
                            "bin_schema")) # flat schema of unpacked binary data, if any

class Manifest(object):

    """
    An object for reading and writing backup directory .manifest files.
    """

    _BEGIN_METADATA = "%{"
    _END_METADATA = "%}"

    def __init__(self, filename=None):
        self.clear()
        if filename is not None:
            self.load(filename)

    def clear(self):
        """Reset internal state to freshly minted object."""
        self.metadata = dict()
        self.filename = None
        self.items = []
        self._lineno = 0

    @property
    def version(self):
        return self.metadata.get('version')

    @version.setter
    def version(self, value):
        assert isinstance(value, int)
        self.metadata['version'] = value

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def update(self, *args, **kwargs):
        """Update metadata section's key/value pairs."""
        self.metadata.update(*args, **kwargs)

    def append(self, item):
        if isinstance(item, ArrayMetadata):
            self.items.append(item)
        else:
            raise ValueError("Cannot append {0} to Manifest".format(item))

    def extend(self, item_list):
        def bad_item(x):
            return not isinstance(x, ArrayMetadata)
        if any(map(bad_item, item_list)):
            raise ValueError("Cannot extend {0} into Manifest".format(item_list))
        self.items.extend(item_list)

    def load(self, filename):
        """Read and parse a .manifest file.

        If the first line of the file is _BEGIN_METADATA, parse the metadata
        and continue.  Otherwise this is a version 0 manifest file.
        """
        self.clear()
        self.filename = filename
        with open(filename) as F:
            line1 = F.readline().strip()
            self._lineno = 1
            if line1 == Manifest._BEGIN_METADATA:
                self._parse_metadata(F)
                assert 'version' in self.metadata, "%s: No version found in metadata" % filename
                method = "_load_v%d" % self.metadata['version']
                try:
                    Manifest.__dict__[method](self, F)
                except KeyError:
                    raise RuntimeError("Manifest file version {0} not supported".format(self.version))
            elif '|' in line1:
                # A version 1 file, but written before we implemented
                # the metadata section.  No problem.
                self.metadata['version'] = 1
                self._load_v1(F, line1)
            else:
                self.metadata['version'] = 0
                self._load_v0(F, line1)

    def _parse_metadata(self, FH):
        """Parse metadata section of .manifest file.

        The format of the metadata section is as similar to config.ini
        as we can make it without actually pulling in a config.ini
        parser.  Right now it's pretty simple: we expect a decimal
        version number and nothing more:

            %{
            # Comments are OK.
            version = 1
            %}
        """
        line = None
        while True:
            line = FH.readline()
            if not line:
                raise RuntimeError("Manifest file metadata section is malformed")
            line = line.strip()
            if not line:
                continue        # Blank line.
            if line == Manifest._END_METADATA:
                break
            key, _, value = line.partition('=')
            key = key.strip()
            if key.startswith('#'):
                continue        # Comments allowed.
            if key.lower() == 'version':
                self.metadata['version'] = int(value)
            else:
                self.metadata[key] = value

    def _load_v0(self, FH, line1):
        """Read and parse a version 0 .manifest file (15.7 and (maybe) earlier).

        @param FH    file handle open for read
        @param line1 first line of file (was already read in)

        @note The v0 file format did not explicitly record binary schemas, so
              these must be derived later.  See fix_v0_schemas().
        """
        NS = "public"
        fmt = _args.get('restore')
        BIN_SCHEMA = ''
        line = line1
        while True:
            # Use partition and not split because schema contains commas.
            array, _, rest = line.partition(',')
            uaid, _, schema = rest.partition(',')
            self.items.append(
                ArrayMetadata(array, NS, fmt, int(uaid), array, schema, BIN_SCHEMA))
            line = FH.readline()
            if not line:
                break
            self._lineno += 1

    def _load_v1(self, FH, line1=None):
        """Read and parse a version 1 .manifest file (15.12 and maybe later).

        @param FH    file handle open for read
        @param line1 first line for older v1 manifests that had no metadata
        """
        def _load_v1_line(line):
            fields = line.strip().split('|')
            fields.insert(3, None) # No uaid in v1 manifests, use None.
            if len(fields) + 1 == len(ArrayMetadata._fields):
                # bin_schema was optional in early v1 manifests.
                fields.append(None)
            elif len(fields) != len(ArrayMetadata._fields):
                raise RuntimeError("{0}:{1} - {2} fields but expecting {3}: {4}".format(
                        self.filename, self._lineno, len(fields),
                        len(ArrayMetadata._fields), line))
            self.items.append(ArrayMetadata._make(fields))
        if line1:
            _load_v1_line(line1)
        for line in FH:
            self._lineno += 1
            _load_v1_line(line)

    def __str__(self):
        """Print in v1 format."""
        # Build metadata section.  It would be nice if entries were displayed in some deterministic
        # order, but for now the dict() hash order is fine.  See collections.OrderedDict.
        if 'version' not in self.metadata:
            # Probably because this Manifest was built with
            # extend()/append() rather than load().
            self.metadata['version'] = 1
        lines = [ Manifest._BEGIN_METADATA ]
        lines.extend([" = ".join((k.lower(), str(self.metadata[k])))
                      for k in self.metadata])
        lines.append(Manifest._END_METADATA)

        # Build per-array data lines.
        for item in self.items:
            lines.append('|'.join((item.name,
                                   item.ns,
                                   item.format,
                                   item.file_name,
                                   item.schema,
                                   item.bin_schema)))
        return '\n'.join(lines)


##############################################################################
# abnormalExitWatchdog:
#    This function "wraps" or decorates the waitForProcesses method in
#    CommandRunner class.  The function traps SystemExit and KeyboardInterrupt
#    signals in order to attempt some cleanup.  The function terminates
#    processes that were being waited on.  This is a partial cleanup since
#    there could be processes that were started on remote hosts which this
#    program has little control over.
def abnormalExitWatchdog(procWaitFunc):
    @functools.wraps(procWaitFunc)
    def wrapper(obj,proc,raiseOnError=False):
        abnormalExit = False
        exceptMsg = ''
        try:
            exitCode,output = procWaitFunc(obj,proc,raiseOnError)
        except (KeyboardInterrupt, SystemExit):
            exceptMsg = 'Abnormal termination - exiting...'
            abnormalExit = True
        except Exception, e:
            exceptMsg = 'Bad exception - exiting...\n'
            exceptMsg += e.message
            abnormalExit = True
        finally:
            pass

        thisCommand = obj.pop_pid_from_table(proc.pid)

        if (abnormalExit):
            obj.kill_all_started_pids()

            cmd = thisCommand[0]
            msg_list = [exceptMsg]
            msg_list.append('Exception(s) encountered while running command:')
            raise AppError('\n'.join(msg_list))
        if (exitCode != 0):
            msg_list = ['Abnormal return encountered while running command:']
            msg_list.append(thisCommand[0])
            errs = ''
            if (len(output) > 0):
                errs = output[1]
            if (raiseOnError):
                obj.kill_all_started_pids()
                raise AppError('\n'.join(msg_list))
        return (exitCode,output)
    return wrapper

def namespace_wrap_query(ns,query):
    if (ns == 'public'):
        return query
    return 'set_namespace(\'{0}\');{1}'.format(ns,query)

#-------------------------------------------------------------------------
# Get the iquery command as a list
#-------------------------------------------------------------------------
def getIqueryCmd(
    host,
    port,
    auth_file=None,
    flag_list=['-aq'],
    query_cmd=None):

    cmd_list = ['iquery']

    if auth_file:
        cmd_list.extend(['--auth-file', auth_file])

    cmd_list.extend(['-c', host,'-p', port])
    cmd_list.extend(flag_list)

    if query_cmd:
        cmd_list.extend([query_cmd])

    return cmd_list


def make_array_metadata_from_array_list_line(raw_info, ns='public', _cmdRunner=None):
    """Create an ArrayMetadata object from one line of list('arrays') output."""

    # I hesitate to make this a Manifest method because there's just too
    # much unnecessary coupling here.  So I leave it as an external
    # method.  All this code was formerly bundled into class
    # array_metadata, in its constructor no less.  Ugh.

    if _cmdRunner is None:
        # The old code got a new one for every call, but that seems wasteful.
        _cmdRunner = CommandRunner(ssh_port=_args.get('ssh_port'))

    m = re.compile('([^,]+)[^\<]*(\<[^\]]+\])').match(raw_info)
    name =  m.group(1).replace('\'','')
    schema = m.group(2)
    file_name = name
    bin_schema = ''

    fmt = ''
    if _args.get('save'):
        fmt = _args.get('save')
    if _args.get('restore'):
        fmt = _args.get('restore')
    assert fmt, "Missing --save or --restore option"

    if ns != 'public':
        # This array belongs to a namespace: file name will have to include both
        # array and namespace names.
        file_name = '.'.join((name, ns))

    if (schema[0] == '\''):
        schema = schema[1:]

    if (schema[-1] == '\''):
        schema = schema[:-1]

    # Need to remove \' characters from DEFAULT 'c' modifiers for char-type attributes.
    schema = schema.replace('\\\'','\'')

    # Binary fun...
    if (_args.get('save') == 'binary' or _args.get('restore') == 'binary'):
        # In case of binary format, we add extra fields to the listings.

        # Need to temporarily put back \' characters into DEFAULT 'c' modifiers
        # for char-type operators; show operator will enclose everything in single
        # quotes: so inside single quotes need to be \-escaped.
        temp_schema = schema.replace('\'','\\\'')

        input_arg = name
        if (_args.get('restore') == 'binary'):
            input_arg = temp_schema

        # Prepare and run the command to get 1D flat schema of the array.
        show_query = namespace_wrap_query(
            ns,
            'show(\'unpack(input({0},\\\'/dev/null\\\'),__row)\',\'afl\')'.format(input_arg)
            )

        cmd = getIqueryCmd(
            _args.get('host'),
            _args.get('port'),
            auth_file=_args.get('auth-file'),
            flag_list=['-ocsv:l', '-aq'],
            query_cmd=show_query)

        exits,outs = _cmdRunner.waitForProcesses(
            _cmdRunner.runSubProcesses([cmd]),
            True
            )
        cutoff_line = 1
        if ('set_namespace' in show_query):
            cutoff_line = 2
        # Brush up the raw query output.
        text = outs[0][0].split('\n')[cutoff_line]
        m = re.compile('<[^\]]+?\]').search(text)
        if (m is None):
            msg = ['Unable to extract schema string from show output:']
            msg.append(text)
            raise AppError('\n'.join(msg))
        bin_schema = m.group().strip()
        m = re.compile('<[^>]+?>').search(bin_schema)
        if (m is None):
            msg = ['Unable to extract schema attributes string from show output:']
            msg.append(text)
            raise AppError('\n'.join(msg))
        attr_list = m.group().replace('>','')
        attr_list = attr_list.replace('<','').split(',')
        array_bin_fmt = []
        for attr in attr_list:
            m = re.compile('([^:]+:)([^\s]+)(.*)').match(attr)
            if (m is None):
                msg = ['Unable to extract attribute type:']
                msg.append(attr)
                raise AppError('\n'.join(msg))
            fmt_string = m.group(2).strip()
            modifiers = m.group(3).strip()
            if (len(modifiers) > 0):
                if (modifiers[:4].upper() == 'NULL'):
                    fmt_string += ' NULL'
            array_bin_fmt.append(fmt_string)
        # Save the binary format.
        fmt = '({0})'.format(','.join(array_bin_fmt))
        # Save 1D schema
        bin_schema = bin_schema.replace('\\\'','\'')

    # Finally, construct and return some ArrayMetadata.
    return ArrayMetadata._make((name, ns, fmt, None, # uaid
                                file_name, schema, bin_schema))


def fix_v0_schemas(manifest, _cmdRunner=None):
    """Fix a v0 manifest: resolve binary schemas, adjust attribute nullability.

    @description The v0 .manifest file format does not contain explicit
    binary schemas, so we may need to derive them at runtime using the same
    queries used by the 15.7 version of this script.

    @p In addition, in 15.12 (v1), attributes became nullable by
    default.  All schemas from the v0 manifest have to be adjusted
    accordingly to preserve the semantics of the original array.

    @param manifest a v0 Manifest object
    @return v0 manifest with correct 'format', 'schema', and 'bin_schema' values
    """
    if _cmdRunner is None:
        _cmdRunner = CommandRunner(ssh_port=_args.get('ssh_port'))
    assert manifest.version == 0, "Implicit binary schema for v%d manifest??" % manifest.version

    SHOW_QUERY = r"show('unpack(input({0}, \'/dev/null\'),__row)', 'afl')"

    result = Manifest()
    for md in manifest:
        assert md.ns == 'public'

        # The v0 md.schema was recorded by 15.7, with attributes NOT NULL by default.  Now
        # attributes default to nullable, so we must reconstruct the schema to get correct
        # nullability.
        attrs, dims = SCHEMA.parse(md.schema, default_nullable=False)
        schema = SCHEMA.unparse(attrs, dims, default_nullable=True)

        bin_schema = md.bin_schema
        format = md.format
        if md.format == 'binary':

            # Run 'show' query to infer binary backup file's schema.
            assert not md.bin_schema
            cmd = getIqueryCmd(
                _args.get('host'),
                _args.get('port'),
                auth_file=_args.get('auth-file'),
                flag_list=['-otsv', '-aq'],
                query_cmd=SHOW_QUERY.format(schema))
            exits,outs = _cmdRunner.waitForProcesses(
                _cmdRunner.runSubProcesses([cmd]),
                True
                )
            raw_output = outs[0][0].strip() # Ugh.

            # Derive the binary format string from the binary schema.
            m = re.search(r'<[^\]]+]', raw_output)
            bin_schema = m.group()
            attrs, dims = SCHEMA.parse(bin_schema)
            types = [x.type for x in attrs]
            nulls = [" null" if x.nullable else "" for x in attrs ]
            format = "({0})".format(
                ','.join((''.join(x) for x in zip(types, nulls))))

        result.append(ArrayMetadata._make((md.name, md.ns, format, md.uaid,
                                           md.file_name, schema, bin_schema)))
    result.version = 0
    return result

##############################################################################
class CommandRunner:
    def __init__(self,ssh_port=22):
        self._ssh_port = ssh_port
        self.pids = {}

    def get_command_from_pid(self,pid):
        cmd = ''
        if (str(pid) in self.pids.keys()):
            cmd = self.pids[str(pid)][0]
        return cmd

    def pop_pid_from_table(self,pid):
        cmdInfo = ['',None]
        if (str(pid) in self.pids.keys()):
            cmdInfo = self.pids.pop(str(pid))

        return cmdInfo

    def kill_all_started_pids(self):
        for pid in self.pids:
            msg_list = ['Terminating process:']
            msg_list.append('pid: ' + pid + ', command line: ' + self.pids[pid][0])
            sys.stderr.write('\n'.join(msg_list) + '\n')
            proc = self.pids[pid][1]
            try:
                proc.kill()
            except:
                sys.stderr.write('Could not kill process!')
    #-------------------------------------------------------------------------
    # runSubProcess: execute one command and return the subprocess object; the
    #                command is a list of strings representing executables and
    #                their options.  By default, the function attaches pipes
    #                to standard out and standard error streams and disables
    #                standard in.
    def _wrapForBash(self,cmd):
        localCmd = ' '.join(cmd)
        localCmd = localCmd.replace('\'','\\\'')
        localCmd = '/bin/bash -c $\'' + localCmd + '\''
        return localCmd

    def runSubProcess(
        self,
        cmd, # Command to run (list of string options).
        si=None, # Standard in.
        so=subprocess.PIPE, # Standard out.
        se=subprocess.PIPE, # Standard error.
        useShell=False # Flag to use shell option when starting process.
        ):

        localCmd = list(cmd) # Copy list to make sure it is not referenced
                             # elsewhere.

        stringLocalCmd = ' '.join(localCmd)
        exe = None
        if (useShell): # If command is for shell, flatten it into a single
            exe = '/bin/bash'
            localCmd = [stringLocalCmd]

        try:
            proc = subprocess.Popen( # Run the command.
                localCmd,
                stdin=si,
                stdout=so,
                stderr=se,
                shell=useShell,
                executable=exe
                )
        except Exception, e:
            msg_list = ['Error encountered while running command:']
            msg_list.append(stringLocalCmd)

            self.kill_all_started_pids()

            raise AppError('\n'.join(msg_list))
        self.pids[str(proc.pid)] = [stringLocalCmd,proc]
        return proc


    #-------------------------------------------------------------------------
    # runSShCommand: execute one command via ssh; the function relies on the
    #                fact that the specified user has the ability to run ssh
    #                commands without entering password.  The function starts
    #                the ssh subprocess with all three standard streams
    #                attached (stdin, stdout, and stderr).  Once ssh starts,
    #                the function writes the entire user-specified command
    #                to the stdin of the ssh subprocess and closes it.  The
    #                return value is the subprocess object of the ssh.
    def runSShCommand(
        self,
        user, # Username to use with ssh.
        host, # Machine name/ip to ssh into.
        cmd, # Command to run over ssh.
        si=subprocess.PIPE, # Standard in.
        so=subprocess.PIPE, # Standard error.
        se=subprocess.PIPE # Standard error.
        ):
        sshCmd = ['ssh', '-p', str(self._ssh_port), user + '@' + host] # Actual ssh command to run.

        try:
            # Start the ssh command with out any actual useful things to
            # execute.
            proc = self.runSubProcess(sshCmd,si,so,se)

            localCmd = self._wrapForBash(cmd)

            # Write the actual command to the ssh standard in.
            proc.stdin.write(localCmd + '\n')
        except:
            sys.stderr.write(localCmd + '\n')
            if (proc is not None):
                proc.kill()
            raise AppError('Error running ssh command!')
        finally:
            if (proc is not None):
                proc.stdin.close() # Closing ssh standard in possibly
            proc.stdin = None      # prevents it from hanging.
        return proc
    @abnormalExitWatchdog # Decorator to watch for abnormal exit conditions.
    def waitForProcess(self,proc,raiseOnError=False):
        outs = proc.communicate()
        exitCode = proc.returncode
        return (exitCode,outs)
    #-------------------------------------------------------------------------
    # waitForProcesses: waits until all of the specified processes have
    #     have exited; optionally, the function returns the exit codes and
    #     (stdout,stderr) text tuples from the processes.  The function is
    #     decorated with the watchdog function.
    #
    def waitForProcesses(self,procs,raiseOnError=False):
        exits_and_outs = map(lambda proc: self.waitForProcess(proc,raiseOnError),procs)
        exits = [ret_tuple[0] for ret_tuple in exits_and_outs]
        outs = [ret_tuple[1] for ret_tuple in exits_and_outs]
        return exits,outs
    #-------------------------------------------------------------------------
    # runSubProcesses: calls runSubProcess in quick succession and returns a
    #                  list of subprocess objects.
    def runSubProcesses(
        self,
        cmds, # Command to run (list of string options).
        si=None, # Standard in.
        so=subprocess.PIPE, # Standard out.
        se=subprocess.PIPE, # Standard error.
        useShell=False # Flag for subprocess to use shell or not.
        ):
        return map(
            lambda cmd: self.runSubProcess(cmd,si,so,se,useShell),cmds
            )
    #-------------------------------------------------------------------------
    # runSshCommands: calls runSShCommand in quick succession on the specified
    #                 commands and returns the list of subprocess objects.
    def runSshCommands(
        self,
        user, # Username to use with ssh.
        hosts, # Machine names/ips to ssh into.
        cmds, # Commands to run over ssh (one element per each machine).
        si=subprocess.PIPE, # Standard in.
        so=subprocess.PIPE, # Standard out.
        se=subprocess.PIPE # Standard error.
        ):
        return map(
            lambda i: self.runSShCommand(user,hosts[i],cmds[i],si,so,se),range(len(hosts))
            )
##############################################################################
# ScidbCommander: central class for dealing with scidb and related operations.
#
class ScidbCommander:
    #-------------------------------------------------------------------------
    # __init__: collects some initial info; the assumption here is that the
    #           currently logged in user will do all of the ssh and scidb
    #           queries.
    def __init__(self, progArgs):
        self._iqueryHost = progArgs.get('host')
        self._iqueryPort = progArgs.get('port')
        self._iqueryAuthFile = progArgs.get('auth-file')
        self._user = pwd.getpwuid(os.getuid())[0] # Collect some info: get username.
        self._cmdRunner = CommandRunner(ssh_port=progArgs.get('ssh_port'))
        # Run iquery and stash the cluster instance data for later use.
        self._hosts,self._instanceData = self._getHostInstanceData()
        self._coordinator = self._hosts[0] # Coordinator is always first.

        self._iquery_header = getIqueryCmd(
            self._iqueryHost,
            self._iqueryPort,
            auth_file=self._iqueryAuthFile,
            flag_list=['-ocsv:l', '-aq'])

        self._list_arrays_query_command = 'filter(list(\'arrays\'),temporary=false)'

        self._list_all_versions_arrays_query_command = 'sort(filter(list(\'arrays\',true),temporary=false),aid)'

        self._list_namespaces_query_command = 'list(\'namespaces\')'

    #-------------------------------------------------------------------------
    # _getHostInstanceData: gets the information about all scidb instances
    #     currently running.  Basically, the function internalizes the scidb
    #     query list('instances') and keeps the information for later use by
    #     others.
    #     Instance data list is grouped by hosts and has the following format:
    #     [
    #       [ # Host 0 instances
    #         [port0,id0,start time,data folder0],
    #         [port1,id1,start time,data folder1],
    #         ...
    #         [portX,idX,start time,data folderX],
    #       ],
    #       [ #Host 1 instances
    #         [portX+1,idX,start time,data folderX+1],
    #         ...
    #       ],
    #       [ # Host N instances
    #         ...
    #         [portN,idN,start time,data folderN]
    #       ]
    #     ]
    #     The fields are more or less self-explanatory: scidb instance network
    #     port, instance id, time when scidb started, and data folder for the
    #     instance.
    #     Hosts are returned in a separate list.
    def _getHostInstanceData(self):

        cmd = getIqueryCmd(
            self._iqueryHost,
            self._iqueryPort,
            auth_file=self._iqueryAuthFile,
            flag_list=['-ocsv:l', '-aq'],
            query_cmd="list('instances')"
            )

        proc = self._cmdRunner.runSubProcess(cmd)
        exits,outs = self._cmdRunner.waitForProcesses([proc],True)

        # Once the process has finished, get the outputs and split them into
        # internals lists.
        text = outs[0][0].strip().replace('\'','')
        err_text = outs[0][1].strip().replace('\'','')
        lines = text.split('\n')
        try:
            hostList = sorted(
                [line.split(',') for line in lines[1:]],
                key=lambda x: int(x[2])
                )
        except:
            raise AppError('Unable to parse instance data from output!')

        hosts = reduce( # Make a list of host names and preserve the order.
            lambda hList,hostData: hList + hostData if hostData[0] not in hList else hList,
            [[h[0]] for h in hostList]
            )
        instanceData = [[] for i in range(len(hosts))]
        map(
            lambda x: instanceData[hosts.index(x[0])].append(x[1:]),
            hostList
            )
        return hosts,instanceData

    #-------------------------------------------------------------------------
    # _removeArrays: delete specified arrays from scidb database.
    def _removeArrays(self, manifest):

        cmd = getIqueryCmd(
            self._iqueryHost,
            self._iqueryPort,
            auth_file=self._iqueryAuthFile,
            flag_list=['-ocsv:l', '-naq']
            )

        removeCmds = [cmd + [namespace_wrap_query(md.ns,
                        'remove({0})'.format(no_ver_name(md.name)))] for md in manifest]

        exits = []
        outs = []
        for cmd in removeCmds:
            proc = self._cmdRunner.runSubProcess(cmd)
            pExit,pOut = self._cmdRunner.waitForProcesses([proc],raiseOnError=False)
            stdErrText = pOut[0][1]
            stdOutText = pOut[0][0]
            procExitCode = pExit[0]
            if (procExitCode != 0):
                if ('SCIDB_LE_ARRAY_DOESNT_EXIST' not in stdErrText):
                    msgList = []
                    msgList.append('Command \"{0}\" failed!\n'.format(' '.join(cmd)))
                    msgList.extend([stdOutText,stdErrText])
                    raise AppError(''.join(msgList))

    #-------------------------------------------------------------------------
    # _prepInstanceCommands: replicates the command over all instances
    #     of scidb and groups the results by host (returned command list is
    #     grouped by hosts).  For instance, if user has a scidb cluster with
    #     2 hosts and 2 scidb instances on each host, then this function will
    #     return a list of commands like this:
    #     [
    #       [cmd0,cmd1],
    #       [cmd2,cmd3]
    #     ]
    #     User supplies cmd in this case which can contain replaceable tokens
    #     like "<i>".  The function substitutes the replacment values based
    #     on what the rep_gen parameter function generates: the replacements
    #     generator function receives the instance list as the parameter and
    #     produces a list of replacement pairs - for instance,
    #     [('<i>',x[1])].  In this case, the function will replace any
    #     occurrence of string '<i>' with instance id.  Replacement
    #     generator can return as many pairs as needed.
    #
    #     Trim option removes the shell-separator strings (i.e. &, &&, ||,
    #     etc.) from the last command in the server's list.
    #
    #     Combine option collapses all individual command lists into commands-
    #     per-host lists:
    #     [
    #       [cmd0-options,cmd1-options],
    #       [cmd2-options,cmd3-options]
    #     ]
    #     The consumers can run the entire list of collapsed command options on
    #     each host.
    def _prepInstanceCommands(
        self,
        template,
        rep_gen,
        trim=True,
        combine=True,
        end=None
        ):
        # The following lines replicate the command per each instance in the
        # cluster: N commands for N instances grouped into lists for each host.
        # The lines also perform token replacements on each individual option
        # string within the command (see above).  So, for each host's instances,
        # for each instance, for each option in template - replace tokens and
        # place the option into a new command list.  The result is the list of
        # command lists grouped by host (into lists, of course).
        cmds = [
            [[ reduce(lambda a,kv: a.replace(*kv),rep_gen(inst),c) for c in template] for inst in dataL] for dataL in self._instanceData
            ]

        # Optionally remove last shell separator from each host command.
        if (trim):
            edge = -1
            if (cmds[0][-1][-1][-2:] in ['&&','||']):
                edge = -2
            cmds = [
                hCmds[:-1] + [hCmds[-1][:-1] + [hCmds[-1][-1][:edge]]] for hCmds in cmds
                ]
        # Optionally, append special en-suffix to last command list in each
        # host's list.
        if (end is not None):
            cmds = [
                hCmds[:-1] + [hCmds[-1] + [end]] for hCmds in cmds
                ]

        # Otionally combine commands for each host into a single list of
        # options.
        if (combine):
            cmds = [
                [c for c in itertools.chain(*hCmds)]for hCmds in cmds
                ]
        return cmds

    #-------------------------------------------------------------------------
    # _replaceTokensInCommand: replace placeholder string tokens found in
    #    specified command's options.  The function replaces every occurrence
    #    of tokens in every option of the command list with the corresponding
    #    replacement values.
    def _replaceTokensInCommand(
        self,
        tokens, # String tokens possibly found in command options.
        replacements, # Replacements for the tokens.
        cmd # Command option list.
        ):

        # Reducer is a short function that takes a string and a token/value
        # pair.  It then returns the string with the token replaced by the
        # value.  The * operator converts the token/value pair argument into
        # two arguments to the string replace method.
        reducer = lambda option,tokenAndVal: option.replace(*tokenAndVal)

        # Apply the reducer onto every option in the command and return the
        # result with the appropriate replacements made back to the caller.
        return [
            reduce(reducer,zip(tokens,replacements),option) \
            for option in cmd
            ]

    #-------------------------------------------------------------------------
    # cleanUpLinks: removes the links from instance data folders to backup
    #               folders.
    def cleanUpLinks(self,args):
        baseFolder = args.get('dir')
        _,baseFolderName = os.path.split(baseFolder)
        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]

        rmCmd = ['rm', '-f', '<dd>' + os.sep + baseFolderName + ';']

        rmCmds = self._prepInstanceCommands(
            rmCmd,
            lambda x: [('<i>',x[1]),('<dd>',x[-1])]
            )
        self._cmdRunner.waitForProcesses(
            self._cmdRunner.runSshCommands(
                self._user,
                self._hosts,
                rmCmds
                ),
            False # Outputs not checked.
            )
    #-------------------------------------------------------------------------
    # removeBackup: delete backup folders and all their contents on all scidb
    #     hosts.
    def removeBackup(self,args):
        baseFolder = args.get('dir')

        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]

        rmCmd = ['rm', '-rf', baseFolder + '*']
        rmCmds = [list(rmCmd) for i in range(len(self._hosts))]
        question = 'Delete backup folder(s) ' + baseFolder + '*' + ' on all hosts? [y/n]:'
        userAnswer = yesNoQuestion(question)
        if (userAnswer):
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(
                    self._user,
                    self._hosts,
                    rmCmds
                    ),
                False # Outputs not checked.
                )
            print_and_flush('Backup folder(s) deleted.')
    #-------------------------------------------------------------------------
    def verifyBackup(self,args, manifest):
        baseFolder = args.get('dir')
        _,baseFolderName = os.path.split(baseFolder)
        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]

        if (args.get('parallel')):
            cmd = ['ls', baseFolder + '.<i>' + ' &&']
            cmds = self._prepInstanceCommands(list(cmd),lambda x: [('<i>',x[1])])
            hosts = self._hosts
            instData = self._instanceData
            countCheck = lambda z: z[0].count(a) == z[1]
        else:
            cmds = [['ls', baseFolder]]
            hosts = [self._coordinator]
            instData = self._instanceData[0]
            countCheck = lambda z: z[0].count(a) == 1

        R = self._cmdRunner.waitForProcesses(
            self._cmdRunner.runSshCommands(
                self._user,
                hosts,
                cmds
                ),
            True
            )
        text = [t[0].strip().split('\n') for t in R[1]]
        arrayNames = [md.file_name for md in manifest]
        # Now, we can test if all arrays are found in correct quantity.
        nInst = [ len(instD) for instD in instData]
        arrayChecks = [[ countCheck(z) for a in arrayNames] for z in zip(text,nInst)]
        totalChecks = [all(ac) for ac in arrayChecks]
        if (not all(totalChecks)):
            # Backup is corrupted: pinpoint the host and exit.
            badBkpHosts = [z[0] for z in zip(hosts,totalChecks) if not z[1]]
            for badHost in badBkpHosts:
                print_and_flush('Backup folders are missing files on host {0}.'.format(badHost),stream=sys.stderr)
            raise AppError('Backup folders are corrupted!  Exiting...')
        print_and_flush('Ok.')
    #-------------------------------------------------------------------------
    # setupDataFolders: prepares backup data folders for the save operations.
    #     In case of parallel mode, multiple data folders have to be set up
    #     (one for each instance) and linked (from instance data folder to its
    #     corresponding backup folder).
    #
    def setupDataFolders(self,args):
        baseFolder = args.get('dir')
        _,baseFolderName = os.path.split(baseFolder)
        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]
        folderList = [baseFolder]
        dataPaths = []

        if (args.get('parallel')):
            # For save operation, both the folders and the links must be
            # created.
            setupDirsTemplate = [
                'mkdir',
                '-p',
                baseFolder + '.<i>;',
                'ln',
                '-snf',
                baseFolder + '.<i>',
                '<dd>' + os.sep + baseFolderName + ';'
                ]
            if (not (args.get('restore') is None)):
                # In case of restore operation, only the links must be created.
                setupDirsTemplate = [
                    'ln',
                    '-snf',
                    baseFolder + '.<i>',
                    '<dd>' + os.sep + baseFolderName + ';'
                    ]

                # Check to make sure backup folder(s) are present.
                checkCmd = ['ls',baseFolder + '.<i>' + ';']
                checkCmds = self._prepInstanceCommands(
                    checkCmd,
                    lambda x: [('<i>',x[1])]
                    )
                exits,outs = self._cmdRunner.waitForProcesses(
                    self._cmdRunner.runSshCommands(self._user,self._hosts,checkCmds),
                    True
                    )
                if (any([x != 0 for x in exits])):
                    raise AppError('Not all backup folders found (bad exit codes); verify that backup folders exist on on cluster hosts!')
                lsErrMsg = 'ls: cannot access'
                errorOuts = [e[1] for e in outs]
                if (any([ lsErrMsg in x for x in errorOuts ])):
                    raise AppError('Not all backup folders found (error messages); verify that backup folders exist on on cluster hosts!')
            # Set up the folder and/or link making commands.
            cmds = self._prepInstanceCommands(
                setupDirsTemplate,
                lambda x: [('<i>',x[1]),('<dd>',x[-1])]
                )
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(
                    self._user,
                    self._hosts,
                    cmds
                    ),
                False
                )
        else: # This is a non-parallel operation: only one folder needed.
            if (not (os.path.isdir(baseFolder))):
                os.makedirs(baseFolder)

    def _load_libraries(self,libs):
        """ Method for loading a list of libraries into SciDB.
        @param libs list of library names to load
        """
        # Prepare SciDB for user and namespace queries.
        query_header = list(self._iquery_header)
        load_libraries_query = ';'.join(['load_library(\'{0}\')'.format(lib) for lib in libs])
        load_libraries_cmd = query_header + [load_libraries_query]
        exits,outs = self._cmdRunner.waitForProcesses(
            [self._cmdRunner.runSubProcess(load_libraries_cmd)],
            True
            )
        text = outs[0][0]
        err = outs[0][1]
        if (exits[0] != 0):
            msg_list = ['Unable to load libraries:']
            msg_list.extend(libs)
            msg_list.append(text)
            msg_list.append(err)
            raise AppError('\n'.join(msg_list))

    def _show_namespaces(self):
        namespace_query = list(self._iquery_header)
        namespace_query.extend(['list(\'namespaces\')'])
        exits,outs = self._cmdRunner.waitForProcesses(
            [self._cmdRunner.runSubProcess(namespace_query)],
            True
            )
        text = outs[0][0]
        existing_namespaces = [ns.replace('\'','') for ns in map(str.strip,text.split('\n')[1:]) if ns]
        print 'Existing_namespaces=' + str(existing_namespaces)


    def _create_namespace(self, namespace):
        if (namespace == 'public'):
            return True # Nothing to do

        create_namespace_template = 'create_namespace(\'{0}\')'
        create_namespace_cmd = self._iquery_header + [create_namespace_template.format(namespace)]

        exits, outs = self._cmdRunner.waitForProcesses(
            [self._cmdRunner.runSubProcess(create_namespace_cmd)],
            False)  # Do not want an exception thrown on error

        text = outs[0][0]
        err  = outs[0][1]
        if (exits[0] != 0):
            if 'Error id: scidb::SCIDB_SE_SYSCAT::SCIDB_LE_NOT_UNIQUE' in err:
                # everything is okay, the namespace already exists
                return True
            elif 'Error id: scidb::SCIDB_SE_QPROC::SCIDB_LE_LOGICAL_OP_DOESNT_EXIST' in err:
                # we are in trust mode in which the namespaces library is
                # not loaded and therefore the create_namespace operator
                # is expected to not exist and the array should not be
                # created.
                return False
            else:
                msg_list = ['Cannot create namespace: {0}.'.format(namespace)]
                msg_list.append(text)
                msg_list.append(err)

                raise AppError('\n'.join(msg_list))

        return True

    def _gather_manifest_from_scidb(self,list_query_cmd):
        """ Collect the metadata info on all arrays in SciDB
        @param list_query_cmd query to get the array listing from SciDB: it could
               be either a plain query to list arrays or a query to list all versions
               of arrays.
        @return a Manifest object reflecting the selected arrays
        """

        # First, figure out what namespaces are in SciDB.
        namespace_query = list(self._iquery_header)
        namespace_query.extend(['list(\'namespaces\')'])
        exits,outs = self._cmdRunner.waitForProcesses(
            [self._cmdRunner.runSubProcess(namespace_query)],
            True
            )
        text = outs[0][0]
        namespaces = [ns.replace('\'','') for ns in map(str.strip,text.split('\n')[1:]) if ns]

        # Extract array name listings from each namespace.
        manifest = Manifest()
        for ns in namespaces:
            list_query = self._iquery_header + [namespace_wrap_query(ns,list_query_cmd)]
            exits,outs = self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(list_query)],
                True
                )
            text = outs[0][0]

            if (ns == 'public'):
                # Query will look like "list('arrays');"
                # The response will look similiar to:
                #
                # {No} name,uaid,aid,schema,availability,temporary
                # {0} 'A1',13,13,'A1<a:int32> [i=0:9,2,0]',true,false
                #
                # We ignore the line starting with "{No}"
                cutoff_line = 1
            else:
                # Query will look like "set_namespace('ns1');  list('arrays');"
                # and we want to ignore the result of the set_namespace;
                # The response will look similiar to:
                #
                # Query was executed successfully
                # {No} name,uaid,aid,schema,availability,temporary
                # {0} 'A2',14,14,'A2<a:int32> [i=0:9,2,0]',true,false
                #
                # We ignore the line beginning with "Query" and the
                # line beginning with "{No}".  The line beginning with
                # "Query" was generated by the set_namespace call.
                cutoff_line = 2

            # Since there are 2 queries in the same call, skip first 2 lines of the output.
            array_lines = [a for a in map(str.strip,text.split('\n')[cutoff_line:]) if a]

            if (_args.get('allVersions')): # Remove un-versioned array names from the list.
                array_lines = [al for al in array_lines if '@' in re.compile('^[^,]+').search(al).group()]

            fn = lambda x : make_array_metadata_from_array_list_line(x, ns)
            ns_metadata = map(fn, array_lines)
            # Pare down the list according to the user-specified array name filter.
            if (_args.get('filter')):
                ns_metadata = [md for md in ns_metadata if re.compile(_args.get('filter')).match(md.name)]

            manifest.extend(ns_metadata)

        return manifest

    #-------------------------------------------------------------------------
    # _buildSaveOpts: record options used during the save operation to prevent
    #     acidental option mismatches during the restore operations.
    @staticmethod
    def _buildSaveOpts(args):
        argsDict = args.getAllArgs()
        text = []
        if (not (args.get('save') is None)):
            text.append('--save')
            text.append(args.get('save'))
        if (args.get('parallel')):
            text.append('--parallel')
        if (args.get('allVersions')):
            text.append('--allVersions')
        if (not (args.get('filter') is None)):
            text.append('--filter')
            text.append(args.get('filter'))
        if (args.get('zip')):
            text.append('-z')
        text.append(args.get('dir'))
        return ' '.join(text)
    #-------------------------------------------------------------------------
    # _runSave: saves all arrays (one-by-one) in either parallel or
    #     non-parallel mode without zip option.
    def _runSave(
        self,
        manifest,   # Manifest object.
        baseFolder, # Backup folder path.
        parallel=False # Parallel mode flag.
        ):

        idNum = '0' # Zero means "save to coordinator", while "-1" means
                    # "save to all instances" (parallel mode).
        saveFolder = baseFolder # By default, absolute save folder is assumed.

        if (parallel):
            idNum = '-1' # Parallel mode: save to all instances
            # Similarly, in parallel mode save folder value for the save
            # operator is a path relative to scidb data folder (the data paths
            # for all instances have been outfitted with the appropriate sym.
            # links ).
            _,baseFolderName = os.path.split(baseFolder)
            saveFolder = baseFolderName

        cmd_header = getIqueryCmd(
            self._iqueryHost,
            self._iqueryPort,
            auth_file=self._iqueryAuthFile,
            flag_list=['-naq'])

        saved_arrays = 0
        for md in manifest:
            print_and_flush('Archiving array {0}'.format(md.name))
            save_cmd = list(cmd_header)
            input_expr = md.name
            if _args.get('save') == 'binary':
                input_expr = 'unpack({0},__row)'.format(md.name)
            save_query = 'save({0},\'{1}\',{2},\'{3}\')'.format(
                input_expr,
                os.path.join(saveFolder,md.file_name),
                idNum,
                md.format
                )
            save_query = namespace_wrap_query(md.ns, save_query)
            save_cmd.extend([save_query])
            # Run save command, wait for its completion, and get its exit
            # codes and output.
            exits,outs = self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(save_cmd)],
                True
                )

            text = outs[0][0].strip() # Print iquery output for the user.
            print_and_flush(text)
            saved_arrays += 1
        print_and_flush('Saved arrays: {0}'.format(saved_arrays))

    #-------------------------------------------------------------------------
    # _runZipSave: save arrays with gzip in non-parallel mode.
    #
    def _runZipSave(
        self,
        manifest, # Manifest object.
        baseFolder # Backup folder path.
        ):

        cmd_header = getIqueryCmd(
            self._iqueryHost,
            self._iqueryPort,
            auth_file=self._iqueryAuthFile,
            flag_list=['-naq'])

        saved_arrays = 0

        for md in manifest:
            print_and_flush('Archiving array {0}'.format(md.name))
            input_expr = md.name
            if (_args.get('save') == 'binary'):
                input_expr = 'unpack({0},__row)'.format(md.name)
            pipeName = os.path.join(baseFolder,md.file_name)
            makePipeCmd = ['rm', '-f',pipeName, ';','mkfifo','--mode=666',pipeName]
            gzipCmd = [
                'gzip',
                '-c',
                '<',
                pipeName,
                '>',
                '{0}.gz'.format(pipeName),
                '&&',
                'mv',
                '{0}.gz'.format(pipeName),
                pipeName
                ]
            # Make the named pipe and wait for the command to complete (no
            # need to read the output).
            exits,outs = self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(makePipeCmd,useShell=True)],
                False
                )

            save_query = 'save({0},\'{1}\',0,\'{2}\')'.format(input_expr,pipeName,md.format)
            save_query = namespace_wrap_query(md.ns, save_query)
            save_query_cmd = cmd_header + [save_query]

            # Start the iquery save command into the named pipe and do not
            # wait for completion.
            mainProc = self._cmdRunner.runSubProcess(save_query_cmd)

            # Start gzip/move shell command and wait for its completion: when
            # this command is done, save operator is done, gzip is done, and
            # the file has been renamed with the proper name (array name).
            self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(gzipCmd,useShell=True)],
                False
                )
            # Get the output from the iquery save command.
            exits,outs = self._cmdRunner.waitForProcesses([mainProc],True)

            text = outs[0][0].strip() # Print iquery output for user.
            print_and_flush(text)
            saved_arrays += 1

        print_and_flush('Saved arrays: {0}'.format(saved_arrays))
    #-------------------------------------------------------------------------
    # _runParallelZipSave: save all arrays in parallel mode and with gzip.
    #
    def _runParallelZipSave(
        self,
        manifest,  # Manifest object.
        baseFolder # Backup folder path.
        ):

        _,baseFolderName = os.path.split(baseFolder);

        input_expr = ''
        saved_arrays = 0

        cmd_header = getIqueryCmd(
            self._iqueryHost,
            self._iqueryPort,
            auth_file=self._iqueryAuthFile,
            flag_list=['-naq'])

        for md in manifest:
            print_and_flush('Archiving array {0}'.format(md.name))

            # Set up pipe making shell commands here for each array.
            pipeName = os.path.join(baseFolder + '.<i>',md.file_name)

            makePipeCmd = ['rm', '-f',pipeName, ';','mkfifo','--mode=666',pipeName + ';']

            # Set up the gzip commands here for each array.  The command lists
            # have several shell commands in them: gzip the pipe contents and
            # move/rename the named pipe to the array name (final file name).
            gzipCmd = [
                'gzip',
                '-c',
                '<',
                pipeName,
                '>',
                pipeName + '.gz',
                '&',
                'export',
                'GZ_EXIT_CODES=\"$GZ_EXIT_CODES $!\";'
                ]

            # Final command template: move/rename .gz file to its final
            # (array) name.
            moveCmd = [
                'mv',
                pipeName + '.gz',
                pipeName + ';'
                ]

            input_expr = md.name
            if (_args.get('save') == 'binary'):
                input_expr = 'unpack({0},__row)'.format(md.name)

            save_query = 'save({0},\'{1}\',-1,\'{2}\')'.format(
                input_expr,
                os.path.join(baseFolderName,md.file_name),
                md.format
                )
            namespace_wrap_query(md.ns, save_query)
            save_query_cmd = cmd_header + [save_query]

            reducer = lambda x: [('<i>',x[1])]
            make_pipe_cmds = self._prepInstanceCommands(
                list(makePipeCmd),
                reducer
                )
            gzip_cmds = self._prepInstanceCommands(
                list(gzipCmd),
                reducer,
                trim=False,
                end='wait $GZ_EXIT_CODES'
                )
            move_cmds = self._prepInstanceCommands(
                list(moveCmd),
                reducer
                )

            # Make the named pipe and wait for the command to complete (no
            # need to read the output).
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(
                    self._user,
                    self._hosts,
                    make_pipe_cmds
                    ),
                False
                )
            # Start the iquery save command into the named pipes and do not
            # wait for completion.
            mainProc = self._cmdRunner.runSubProcess(save_query_cmd)

            # Start gzip/move shell command and wait for its completion: when
            # this command is done, save operator is done, gzip is done, and
            # the file has been renamed with the proper name (array name).
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(
                    self._user,
                    self._hosts,
                    gzip_cmds
                    ),
                False
                )
            # Get the output from the iquery save command.
            exits,outs = self._cmdRunner.waitForProcesses([mainProc],True)

            # Finally, move all of the .gz files into the array files.
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(
                    self._user,
                    self._hosts,
                    move_cmds
                    ),
                False
                )

            text = outs[0][0].strip() # Print iquery output for user.
            print_and_flush(text)

            saved_arrays += 1
        print_and_flush('Saved arrays: {0}'.format(saved_arrays))
    #-------------------------------------------------------------------------
    # _runRestore: restore all arrays in parallel or in non-parallel modes.
    #
    def _runRestore(
        self,
        manifest,   # The Manifest object.
        baseFolder, # Base backup folder path.
        parallel=False # Parallel mode flag.
        ):

        idNum = '0' # Zero means "save to coordinator", while "-1" means
                    # "save to all instances" (parallel mode).
        restoreFolder = baseFolder # By default, absolute restore folder is
                                   # assumed.
        if (parallel):
            idNum = '-1' # Parallel mode: save to all instances
            # Similarly, in parallel mode save folder value for the save
            # operator is a path relative to scidb data folder (the data paths
            # for all instances have been outfitted with the appropriate sym.
            # links ).
            _,baseFolderName = os.path.split(baseFolder) # Use relative path.
            restoreFolder = baseFolderName

        restoreQuery = 'store(input({0},\'{1}\',{2},\'{3}\'),{4})'
        metadata_func = lambda md: [
            md.schema,
            os.path.join(restoreFolder,md.file_name),
            idNum,
            md.format,
            no_ver_name(md.name)
            ]

        if (_args.get('restore') == 'binary'):
            restoreQuery = 'store(redimension(input({0},\'{1}\',{2},\'{3}\'),{4}),{5})'
            metadata_func = lambda md: [
                md.bin_schema,
                os.path.join(restoreFolder,md.file_name),
                idNum,
                md.format,
                md.schema,
                no_ver_name(md.name)
                ]

        restored_arrays = 0

        cmd_header = getIqueryCmd(
            self._iqueryHost,
            self._iqueryPort,
            auth_file=self._iqueryAuthFile,
            flag_list=['-naq'])

        for md in manifest:
            print_and_flush('Restoring array {0}'.format(md.name))

            if False == self._create_namespace(md.ns):
                print_and_flush('WARNING: namespace \'' + md.ns + '\' can not be created')
                print_and_flush('         array \'' + md.name + '\' cannot be restored.')
                print_and_flush('         Please load the plugin for namespace management (\'namespaces\'),')
                print_and_flush('         or change the array namespace(s) to \'public\' in the')
                print_and_flush('         <restore directory>/.manifest file(s)')
                continue

            array_restore_query = restoreQuery.format(*metadata_func(md))
            array_restore_query = namespace_wrap_query(md.ns, array_restore_query)
            cmd = cmd_header + [array_restore_query]

            exits,outs = self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(cmd)],
                True
                )

            text = outs[0][0].strip() # Print iquery output for the user.
            print_and_flush(text)
            restored_arrays += 1
        print_and_flush('Restored arrays: {0}'.format(restored_arrays))

    #-------------------------------------------------------------------------
    # _runZipRestore: restore all arrays with gzip option.
    #
    def _runZipRestore(
        self,
        manifest,  # The Manifest object.
        baseFolder # Backup folder path.
        ):

        restore_query_template = 'store(input({0},\'{1}\',0,\'{2}\'),{3})'
        metadata_func = lambda md,p_name: [
            md.schema,
            p_name,
            md.format,
            no_ver_name(md.name)
            ]
        if (_args.get('restore') == 'binary'):
            restore_query_template = 'store(redimension(input({0},\'{1}\',0,\'{2}\'),{3}),{4})'
            metadata_func = lambda md,p_name: [
                md.bin_schema,
                p_name,
                md.format,
                md.schema,
                no_ver_name(md.name)
                ]

        restored_arrays = 0

        cmd_header = getIqueryCmd(
            self._iqueryHost,
            self._iqueryPort,
            auth_file=self._iqueryAuthFile,
            flag_list=['-naq'])

        for md in manifest:
            print_and_flush('Restoring array {0}'.format(md.name))
            pipeName = os.path.join(baseFolder,'{0}.p'.format(md.file_name))
            pipeCmd = ['rm','-f',pipeName,'>','/dev/null','2>&1',';','mkfifo','--mode=666',pipeName]
            gzipCmd = ['gzip','-d','-c','<',os.path.join(baseFolder,md.file_name),'>',pipeName]
            cleanupCmd = ['rm','-f',pipeName]

            if False == self._create_namespace(md.ns):
                print_and_flush('WARNING: namespace \'' + md.ns + '\' can not be created')
                print_and_flush('         array \'' + md.name + '\' cannot be restored.')
                print_and_flush('         Please load the plugin for namespace management (\'namespaces\'),')
                print_and_flush('         or change the array namespace(s) to \'public\' in the')
                print_and_flush('         <restore directory>/.manifest file(s)')
                continue

            restore_query = restore_query_template.format(*metadata_func(md,pipeName))
            restore_query = namespace_wrap_query(md.ns, restore_query)

            restore_cmd = cmd_header + [restore_query]

            self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(pipeCmd,useShell=True)],
                False
                )
            self._cmdRunner.runSubProcess(gzipCmd,useShell=True)
            exits,outs = self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(restore_cmd)],
                True
                )
            self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(cleanupCmd,useShell=True)],
                False
                )
            text = outs[0][0].strip() # Print iquery output for user.
            print_and_flush(text)
            restored_arrays += 1
        print_and_flush('Restored arrays: {0}'.format(restored_arrays))

    #-----------------------------------------------------------------------------
    # _runParallelZipRestore: restore all arrays in parallel mode and with gzip
    #     option.
    def _runParallelZipRestore(
        self,
        manifest,  # The Manifest object.
        baseFolder # Backup folder path.
        ):

        _,baseFolderName = os.path.split(baseFolder)

        restore_query_template = 'store(input({0},\'{1}\',-1,\'{2}\'),{3})'
        metadata_func = lambda md,p_fname: [
            md.schema,
            os.path.join(baseFolderName,p_fname),
            md.format,
            no_ver_name(md.name)
            ]
        if (_args.get('restore') == 'binary'):
            restore_query_template = 'store(redimension(input({0},\'{1}\',-1,\'{2}\'),{3}),{4})'
            metadata_func = lambda md,p_fname: [
                md.bin_schema,
                os.path.join(baseFolderName,p_fname),
                md.format,
                md.schema,
                no_ver_name(md.name)
                ]

        arrays_restored = 0

        cmd_header = getIqueryCmd(
            self._iqueryHost,
            self._iqueryPort,
            auth_file=self._iqueryAuthFile,
            flag_list=['-naq'])

        for md in manifest:
            print_and_flush('Restoring array {0}'.format(md.name))
            rel_pipeName = '{0}.p'.format(md.file_name)
            pipeName = os.path.join(baseFolder + '.<i>',rel_pipeName)

            pipeCmd = ['rm','-f',pipeName,'>','/dev/null','2>&1',';','mkfifo','--mode=666',pipeName + '&&']

            if False == self._create_namespace(md.ns):
                print_and_flush(textwrap.dedent("""
                    WARNING: namespace '{0}' cannot be created, array '{1}' cannot be restored.
                             Please load the plugin for namespace management ('namespaces'),
                             or change the array namespace(s) to 'public' in the
                             <restore directory>/.manifest file(s)""".format(md.ns, md.name)))
                continue

            restore_query = restore_query_template.format(*metadata_func(md,rel_pipeName))
            restore_query = namespace_wrap_query(md.ns, restore_query)

            restore_query_cmd = cmd_header + [restore_query]

            gzipCmd = [
                'gzip',
                '-d',
                '-c',
                '<',
                os.path.join(baseFolder + '.<i>',md.file_name),
                '>',
                pipeName + '&',
                'export GZ_EXIT_CODES=\"$GZ_EXIT_CODES $!\";'
                ]
            cleanupCmd = ['rm','-f',pipeName + ';']

            reducer = lambda x: [('<i>',x[1])]

            # Put together pipe-creation commands generator.
            pipe_cmds = self._prepInstanceCommands(pipeCmd,reducer)

            # Put together gzip commands generator.
            gzip_cmds = self._prepInstanceCommands(
                gzipCmd,
                reducer,
                trim=False,
                end='wait $GZ_EXIT_CODES'
                )

            # Put together cleanup commands generator.
            cleanup_cmds = self._prepInstanceCommands(cleanupCmd,reducer)

            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(self._user,self._hosts,pipe_cmds),
                False
                )
            self._cmdRunner.runSshCommands(self._user,self._hosts,gzip_cmds)
            exits,outs = self._cmdRunner.waitForProcesses(
                [self._cmdRunner.runSubProcess(restore_query_cmd)],
                True
                )
            self._cmdRunner.waitForProcesses(
                self._cmdRunner.runSshCommands(self._user,self._hosts,cleanup_cmds),
                False
                )
            text = outs[0][0].strip() # Print iquery output for user.
            print_and_flush(text)
            arrays_restored += 1
        print_and_flush('Restored arrays: {0}'.format(arrays_restored))
    #-----------------------------------------------------------------------------
    # restoreArrays: determines what kind of restore function to call based on
    #     specified user arguments.
    def restoreArrays(self,args,manifestPath):
        # Grab the initial specified backup folder.
        baseFolder = args.get('dir')
        _,baseFolderName = os.path.split(baseFolder)
        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]

        manifest = Manifest(manifestPath)
        if manifest.version == 0:
            manifest = fix_v0_schemas(manifest)

        print_and_flush('Verifying backup...')
        self.verifyBackup(args, manifest)

        if (not args.get('force')):
            # Query for listing arrays in scidb database.
            listArraysCmd = self._list_arrays_query_command

            if (args.get('allVersions')):
                # If --allVersions option is set, then we need to grab all
                # versions of arrays.  Scidb sorts the array list based on id so
                # that when we reload arrays back, the versions of each array are
                # loaded in correct order.
                listArraysCmd = self._list_all_versions_arrays_query_command

            # Check that the database does not already have the arrays we are
            # about to remove.
            existing_manifest = self._gather_manifest_from_scidb(listArraysCmd)

            # Just in case array names are version-ed, remove the versions for
            # set comparison.
            current_arrays = ['{0}.{1}'.format(no_ver_name(md.name), md.ns) for md in existing_manifest]
            backup_arrays = ['{0}.{1}'.format(no_ver_name(md.name),md.ns) for md in manifest]
            intersection = set(current_arrays) & set(backup_arrays)

            if (len(intersection) > 0):
                msgList = ['The following arrays still exist (array.namespace):']
                msgList.extend([a for a in intersection])
                msgList.append('Please remove them manually and re-run the restore operation!')
                raise AppError('\n'.join(msgList))
        else:
            self._removeArrays(manifest) # Remove all

        if (args.get('parallel')):
            if (args.get('zip')):
                self._runParallelZipRestore(manifest, baseFolder)
            else:
                # This is a local parallel restore.
                self._runRestore(manifest, baseFolder, parallel=True)
        else:
            if (args.get('zip')):
                self._runZipRestore(manifest, baseFolder)
            else:
                self._runRestore(manifest, baseFolder)
    #-------------------------------------------------------------------------
    # saveArrays: determine which save method to call based on user-specified
    #     options
    def saveArrays(self,args,manifestPath,optsFilePath):
        # Get the user-specified value for the backup folder.
        baseFolder = args.get('dir')
        _,baseFolderName = os.path.split(baseFolder)
        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]

        # Query for listing arrays in scidb database.
        listArraysCmd = self._list_arrays_query_command

        if (args.get('allVersions')):
            # If --allVersions option is set, then we need to grab all
            # versions of arrays.  Scidb sorts the array list based on id so
            # that when we reload arrays back, the versions of each array are
            # loaded in correct order.
            listArraysCmd = self._list_all_versions_arrays_query_command

        savedOpts = self._buildSaveOpts(args)
        manifest = self._gather_manifest_from_scidb(listArraysCmd)
        manifest.update({"save-options": savedOpts})
        with open(manifestPath, 'w') as F:
            print >>F, manifest
        # For now we still write the optsFilePath, but in the future we
        # hope to rely on examining the 'save-options' entry in the
        # manifest (made above).
        with open(optsFilePath, 'w') as F:
            print >>F, savedOpts

        if (args.get('parallel')):
            if (args.get('zip')):
                self._runParallelZipSave(
                    manifest,
                    baseFolder
                    )
            else:
                # This is a plain parallel save.
                self._runSave(manifest, baseFolder,parallel=True)
        else:
            if (args.get('zip')):
                self._runZipSave(manifest, baseFolder)
            else:
                self._runSave(manifest, baseFolder)

        print_and_flush('Verifying backup...')
        self.verifyBackup(args, manifest)


#-----------------------------------------------------------------------------
# checkProgramArgs: perform checks on specified arguments beyond what the
#     options parser has already done.
def checkProgramArgs(args):
    actions = [args.get('save'),args.get('restore')]
    if (args.get('delete') and actions.count(None) <= 1):
        raise AppError('Delete action cannot be combined with other actions!')

    if (args.get('delete')): # Done checking: this is a simple delete job.
        return

    # TODO: Proper use of argparse could enforce this for you.
    if (actions.count(None) > 1):
        raise AppError('No action specified (--save FMT, --restore FMT)!')
    if (actions.count(None) <= 0):
        raise AppError('Both save and restore actions specified; please choose only one!')

    if (args.get('save') is None): # This is a restore.
        # Check manifest and saved options files.
        bkpDir = args.get('dir')
        if (bkpDir[-1] == os.sep):
            bkpDir = bkpDir[:-1]
        if (args.get('parallel')):
            inst0bkpDir = bkpDir + '.0'
            if (not os.path.isdir(inst0bkpDir )):
                raise AppError('Coordinator backup directory {0} is missing!'.format(inst0bkpDir))

        manifestPath = os.path.join(bkpDir,'.manifest')
        optsFilePath = os.path.join(bkpDir,'.save_opts')
        if (args.get('parallel')):
            manifestPath = os.path.join(inst0bkpDir,'.manifest')
            optsFilePath = os.path.join(inst0bkpDir,'.save_opts')
        if (not (os.path.isfile(manifestPath))):
            raise AppError('Backup is corrupted; manifest file {0} is missing!'.format(manifestPath))

        if (not (os.path.isfile(optsFilePath))):
            raise AppError('Backup is corrupted; saved options file {0} is missing!'.format(optsFilePath))

def main(argv=None):
    global _args

    if (argv is None):
        argv = sys.argv[1:]
    # For proper subprocess management:
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    # Collect program arguments first:
    program_args = ProgramArgs(argv)
    _args = program_args

    try:
        checkProgramArgs(program_args)

        sh = ScidbCommander(program_args)

        sh.setupDataFolders(program_args)

        baseFolder = program_args.get('dir')
        if (baseFolder[-1] == os.sep):
            baseFolder = baseFolder[:-1]
        manifestPath = os.path.join(baseFolder,'.manifest')
        optsFilePath = os.path.join(baseFolder,'.save_opts')
        if (program_args.get('parallel')):
            manifestPath = os.path.join(baseFolder + '.0','.manifest')
            optsFilePath = os.path.join(baseFolder + '.0','.save_opts')

        if (program_args.get('delete')):
            sh.removeBackup(program_args)
            sys.exit(0)

        if (not (program_args.get('save') is None)):
            sh.saveArrays(
                program_args,
                manifestPath,
                optsFilePath
                )
        else:
            sh.restoreArrays(
                program_args,
                manifestPath
                )
        # Remove links to backup folders from instance data folders.
        sh.cleanUpLinks(program_args)
    except AppError as e:
        print >>sys.stderr, "Error:", str(e)
        return 2

    return 0

#-----------------------------------------------------------------------------
# Script main entry point.
#
if __name__ == '__main__':
    sys.exit(main())
