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
Incrementally load a SciDB array from stdin.

This script is intended to work with nc(1) (-aka- netcat) to allow
lines of comma- or tab-separated values read from a socket to be
batched up and periodically inserted into a SciDB array.  For example:

    $ nc -d -k -l 8080 | loadpipe.py <options>

For information about command line options, run "loadpipe.py --help".

Array Insertion
===============

Loadpipe reads data line by line from stdin and batches it up until
one of the following criteria are met:

 - The number of lines specified by --batch-lines have been read
   from stdin,

 - The number of bytes specified by --batch-size is exceeded,

 - The batch is non-empty and N seconds have elapsed since the last
   'insert'.  (This timeout value is settable with the --timeout
   option.)

When any of those conditions have been met, the batch of data is
inserted into the target array using an "insert(redimension(input(...)))"
query (or an "insert(input(...), ...)" query if no redimension is needed).

Signal Handling
===============

The 'loadpipe' script responds to the following signals:

  SIGUSR1 - immediately insert any batched data into the array.
  SIGUSR2 - print a statistics summary to the log.
  SIGTERM - immediately insert batched data into the array and exit.

Open Issues
===========

 - No precaution is taken to prevent an 'insert' from clobbering
   pre-existing array values.  To prevent overwrites, the application
   designer must ensure that incoming 1-D data contains strictly
   increasing fields that will map onto a dimension during the
   redimension() operation.

 - We assume one input record per line.

 - Input format errors (e.g. records with too few or too many fields,
   field X varying in type from record to record, etc.) are not
   detected or handled.
"""

import argparse
import copy
import errno
import fcntl
import os
import Queue
import re
import select
import signal
import subprocess
import sys
import syslog
import tempfile
import threading
import time
import traceback

from scidblib import scidb_schema
from cStringIO import StringIO

_args = None                    # soon to be an argparse.Namespace
_parser = None                  # soon to be an ArgumentParser object
_pgm = ''                       # soon to be the program name
_loader = None                  # soon to be a worker thread object
_iquery = ['iquery']            # iquery command template

_start_gmtime = time.gmtime()
_start_iso8601 = time.strftime('%FT%TZ', _start_gmtime)

class AppError(Exception):
    """For runtime errors."""
    pass

class Usage(Exception):
    """For flagging script usage errors."""
    pass

def set_nonblock(file_):
    """Set the os.O_NONBLOCK flag on an fd or file object.

    @param file_  file object or file descriptor to modify.
    @return previous value of fcntl(2) flags.
    """
    try:
        fd = file_.fileno()
    except AttributeError:
        fd = file_
    oflags = fcntl.fcntl(fd, fcntl.F_GETFL)
    flags = oflags | os.O_NONBLOCK
    rc = fcntl.fcntl(fd, fcntl.F_SETFL, flags)
    if rc < 0:
        raise Usage('Cannot set non-blocking mode on fd %d' % fd)
    return oflags

_saw_sigusr1 = False
_saw_sigusr2 = False
_saw_sigterm = False

def _sigusr1_handler(signo, frame):
    """Arrange to post the current batch immediately."""
    global _saw_sigusr1
    _saw_sigusr1 = True

def _sigusr2_handler(signo, frame):
    """Arrange to log statistics."""
    global _saw_sigusr2
    _saw_sigusr2 = True

def _sigterm_handler(signo, frame):
    """Prepare to shutdown!"""
    global _saw_sigterm
    _saw_sigterm = True

def check_for_signals():
    """Respond to signals in the main thread's non-signal context.

    @return True iff we detected SIGUSR1, informing the caller that
            the current batch should be posted.
    """
    global _saw_sigusr1, _saw_sigusr2, _saw_sigterm
    ret = False
    if _saw_sigusr1:
        # By returning True here the current batch will be posted.
        debuglog("saw sigusr1")
        ret = True
        _saw_sigusr1 = False
    if _saw_sigusr2:
        log_statistics()
        _saw_sigusr2 = False
    if _saw_sigterm:
        critlog('Caught SIGTERM, shutting down')
        raise KeyboardInterrupt('Caught SIGTERM')
    return ret

def number(s):
    """Convert a string to a number, allowing suffixes like KiB, M, etc.

    Currently only useful for integers (we had high aspirations).

    @param s    string to be converted.
    @return integer value of string.
    """
    # Doesn't handle negative values very consistently, oh well.
    assert isinstance(s, basestring)
    sufat = None
    for i, ch in enumerate(s):
        if not ch.isdigit():
            sufat = i
            break
    if not sufat:
        return int(s)
    num = int(s[0:sufat])
    suffix = s[sufat:].strip().lower()
    if suffix in ('k', 'kb', 'kib'):
        return num * 1024
    if suffix in ('m', 'mb', 'mib'):
        return num * 1024 * 1024
    if suffix in ('g', 'gb', 'gib'):
        return num * 1024 * 1024 * 1024
    raise Usage('Unrecognized numeric suffix "{0}"'.format(suffix))

# Using syslog(3) is likely overkill for this example script, but I
# wanted to illustrate how a real service might do logging.

# For argparse below.
_facility_choices = ['kern', 'user', 'mail', 'daemon', 'auth', 'lpr', 'news',
                     'uucp', 'cron', 'local0', 'local1', 'local2', 'local3',
                     'local4', 'local5', 'local6', 'local7']

# Short strings for logging levels.
_strlevel = {
    syslog.LOG_DEBUG: "DEBG",
    syslog.LOG_INFO: "INFO",
    syslog.LOG_NOTICE: "NOTE",
    syslog.LOG_WARNING: "WARN",
    syslog.LOG_ERR: "ERR ",
    syslog.LOG_CRIT: "CRIT"
}

# Wrappers for syslog(3) logging.
def debuglog(*args):
    return _log(syslog.LOG_DEBUG, args)
def infolog(*args):
    return _log(syslog.LOG_INFO, args)
def notelog(*args):
    return _log(syslog.LOG_NOTICE, args)
def warnlog(*args):
    return _log(syslog.LOG_WARNING, args)
def errlog(*args):
    return _log(syslog.LOG_ERR, args)
def critlog(*args):
    return _log(syslog.LOG_CRIT, args)
def _log(level, *args):
    tname = threading.current_thread().name[:4]
    prefix = ' '.join((tname, _strlevel[level]))
    utc = None
    for line in ' '.join(map(str, *args)).split('\n'):
        msg = ' '.join((prefix, line))
        syslog.syslog(level, msg)
        if _args.verbose > 2:
            if utc is None:
                utc = time.strftime('%FT%TZ', time.gmtime())
            print utc, _pgm[:8], msg
    return None

def setup_logging(_initialized=[False]):
    """Initialize logging infrastructure."""
    # Takes no arguments; see http://effbot.org/zone/default-values.htm
    if _initialized[0]:
        syslog.closelog()
    opts = syslog.LOG_PID | syslog.LOG_NDELAY
    facility = syslog.LOG_LOCAL0
    if _args.facility:
        fac_name = ''.join(('LOG_', _args.facility.upper()))
        # We can assert this because we limit choices to _facilities_choices.
        assert fac_name in dir(syslog)
        facility = syslog.__dict__[fac_name]
    syslog.openlog(_pgm[:8], opts, facility)
    if _args.verbose == 0:
        syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_NOTICE))
    if _args.verbose == 1:
        syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_INFO))
    elif _args.verbose > 1:
        syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_DEBUG))
    _initialized[0] = True
    return None

def log_statistics():
    """Write a statistics summary to the log."""
    global _loader
    stats = [Batch.get_statistics()]
    if _loader is not None:
        stats.extend(_loader.get_statistics())
    notelog('=== Statistics since %s' % _start_iso8601)
    notelog('Created %d batches, %d bytes, %d total lines' % stats[0])
    if _loader is not None:
        notelog('Loaded %d batches, %d bytes, %d total lines' % stats[1])
        notelog('Failed %d batches, %d bytes, %d total lines' % stats[2])
    notelog('=== End statistics summary')
    return None

def dev_null(_state=[]):
    """Return an open file object pointing at /dev/null."""
    if not _state:
        _state.append(open('/dev/null', 'wb+'))
    return _state[0]

def start_cmd(cmd, **kwargs):
    """Start a command without waiting for it."""
    outfile = subprocess.PIPE
    try:
        if kwargs['devnull']:
            outfile = dev_null()
    except KeyError:
        pass
    p = subprocess.Popen(cmd,
                         stdout=outfile,
                         stderr=subprocess.STDOUT,
                         **kwargs)
    p.command = cmd
    return p

def wait_cmd(popen, quiet=False):
    """Wait for a started command to complete, and return its stdout."""
    out = popen.communicate()[0]
    while popen.returncode is None:
        os.sleep(1)
    if popen.returncode:
        if not quiet:
            print >>sys.stderr, '-' * 72
            print >>sys.stderr, out
            print >>sys.stderr, '-' * 72
            if popen.returncode < 0:
                raise AppError("Command terminated by signal {0}: {1}".format(
                        -popen.returncode, ' '.join(popen.command)))
            else:
                raise AppError("Exit status {0} for command: {1}".format(
                        popen.returncode, ' '.join(popen.command)))
    return out

def run_cmd(cmd, quiet=False, **kwargs):
    """Run command, passing along Popen keyword args. Return stdout+stderr."""
    return wait_cmd(start_cmd(cmd, **kwargs), quiet)

def echo_cmd(cmd, **kwargs):
    """For debugging."""
    print "Cmd:", ' '.join(cmd), "({0})".format(kwargs)
    return ""

def get_array_schema(array_name):
    """Return the schema string for the named array, or None."""
    cmd = copy.deepcopy(_iquery)
    cmd.extend(['-otsv','-aq', 'show({0})'.format(array_name)])
    try:
        # -otsv means no nasty quotes to strip
        out = run_cmd(cmd)
    except AppError as e:
        debuglog("Cannot show array", array_name, ":", e)
        return None
    if not re.match(array_name + r'\s*<', out):
        raise AppError("Unexpected show({0}) output: {1}".format(
                array_name, out))
    return out[len(array_name):]

class Batch(object):

    """Hoard lines of input until they are ready to be posted."""

    # Class parameters.
    max_lines = 0
    max_bytes = 0

    # Class statistics.
    total_batches = 0
    total_bytes = 0
    total_lines = 0

    def __init__(self):
        """Create an empty Batch object."""
        self.nbytes = 0        # bytes in batch so far
        self.nlines = 0        # lines in batch so far
        self.data = StringIO()
        Batch.total_batches += 1     # sequentially number these
        self.batchno = Batch.total_batches

    @classmethod
    def set_max_lines(cls, n):
        """Set the maximum number of lines a Batch may contain."""
        if n < 0:
            raise Usage('--batch-lines value must be >= 0')
        cls.max_lines = n

    @classmethod
    def set_max_bytes(cls, n):
        """Set the (approximate) maximum size in bytes of a Batch."""
        if n < 0:
            raise Usage('--max-bytes value must be >= 0')
        cls.max_bytes = n

    @classmethod
    def get_statistics(cls):
        """Get batch-related statistics.

        @return a tuple of (total_batches, total_bytes, total_lines).
        """
        return cls.total_batches, cls.total_bytes, cls.total_lines

    def full(self):
        """Return True iff batch exceeds the size or line count threshold."""
        if Batch.max_lines and self.nlines >= Batch.max_lines:
            return True
        if Batch.max_bytes and self.nbytes > Batch.max_bytes:
            return True
        return False

    def empty(self):
        """Return True iff the batch is empty."""
        return self.nbytes == 0

    def size(self):
        """Return number of bytes in this batch."""
        return self.nbytes

    def lines(self):
        """Return number of lines in this batch."""
        return self.nlines

    def add(self, data_line):
        """Add a datum to the batch."""
        self.nbytes += len(data_line)
        Batch.total_bytes += len(data_line)
        self.nlines += 1
        Batch.total_lines += 1
        self.data.write(data_line)

    def get_file(self):
        """Return a StringIO object containing the batch data."""
        self.data.seek(0)       # Get ready for reads.
        return self.data

    def __repr__(self):
        """Return a string representation for logging purposes."""
        # See http://stackoverflow.com/questions/1436703/difference-between-str-and-repr-in-python
        return '%s(id=%d, bytes=%d, lines=%d)' % (
            self.__class__.__name__, self.batchno, self.nbytes, self.nlines)

class DataMover(threading.Thread):

    """Helper thread writes batch data to a file (typically a fifo)."""

    def __init__(self, file_name):
        threading.Thread.__init__(self, name='DmTh') # short name for logging
        self.queue = Queue.Queue()
        self.fname = file_name
    def post(self, src):
        """Request copy from src file object to our file."""
        self.queue.put(src)
    def run(self):
        SZ = 4096
        while True:
            src = self.queue.get()
            if src is None:
                notelog("DataMover thread exiting")
                break
            src = src.get_file() # get StringIO from batch
            with open(self.fname, 'w') as DST:
                while True:
                    buf = src.read(SZ)
                    if buf:
                        DST.write(buf)
                    else:
                        break;

class LoaderThread(threading.Thread):

    """Worker thread, periodically loads a Batch of input."""

    max_queue = 0

    @classmethod
    def set_max_queue(cls, n):
        """Set upper limit on batch queue size.

        Must be called before the worker thread instance is created.
        """
        if n < 0:
            raise Usage('--max-queue must be >= 0')
        cls.max_queue = n
        return None

    def __init__(self):
        """Initialize a worker thread."""
        threading.Thread.__init__(self, name='LoTh') # short name for logging
        infolog("Creating loader thread")
        self.queue = Queue.Queue(maxsize=LoaderThread.max_queue)
        self.failure = [0, 0, 0] # batches, bytes, lines
        self.success = [0, 0, 0] # ditto (alpha ordered!)
        self.tmpdir = tempfile.mkdtemp(prefix='ldpipe.')
        self.fifo_name = os.sep.join((self.tmpdir, 'fifo'))
        os.mkfifo(self.fifo_name)
        self._setup_load_parameters()

    def __del__(self):
        """Destructor."""
        infolog("Loader thread cleaning up")
        os.unlink(self.fifo_name)
        os.rmdir(self.tmpdir)

    def _setup_load_parameters(self):
        """Interpret options that describe how to do the loading."""
        # Were we given a load_schema or a load_array in _args.array_or_schema?
        attrs, dims = None, None
        try:
            # Is it a schema?
            attrs, dims = scidb_schema.parse(_args.array_or_schema)
        except ValueError:
            # Hmmm, perhaps it is an array name.
            schema = get_array_schema(_args.array_or_schema)
            if schema is None:
                raise Usage('Array {0} does not exist'.format(
                        _args.array_or_schema))
            try:
                attrs, dims = scidb_schema.parse(schema)
            except ValueError:
                raise AppError("Internal error, show({0}) output: {1}".format(
                        _args.array_or_schema, schema))
            else:
                self.load_array = _args.array_or_schema
                self.load_schema = schema
        else:
            # We were given a schema, so we need a target array.
            self.load_array = None
            self.load_schema = _args.array_or_schema
            if not _args.target_array:
                raise Usage(' '.join(("Must specify at least one array with",
                                      "-s/--load-schema or -A/--target-array")))
        # Some final checks...
        if len(dims) != 1:
            raise Usage("Load schema '{0}' is not one-dimensional".format(
                    _args.array_or_schema))
        if _args.target_array and not get_array_schema(_args.target_array):
            raise Usage("Target array %s does not exist" % _args.target_array)
        return None

    def post(self, batch):
        """Post a batch of data to the work queue.

        Intended to be called from the main thread only.
        @param batch object to be sent to worker, @i usually a Batch
        """
        # Simple if unlimited queue or no debug logging!
        if LoaderThread.max_queue == 0 or _args.verbose < 2:
            self.queue.put(batch)
        else:
            # Tricky if we have a queue limit and debug logging is enabled.
            try:
                self.queue.put(batch, block=False)
            except Queue.Full:
                debuglog("Blocked on full worker queue (len=%d)" %
                         LoaderThread.max_queue)
                self.queue.put(batch)
                debuglog("Unblocked, worker queue size %d" % self.queue.qsize())

    def run(self):
        """Load each queued Batch.

        This is the worker thread's service loop.
        """
        notelog(self.name, 'worker thread running')
        self.datamover = DataMover(self.fifo_name)
        self.datamover.start()
        while True:
            batch = self.queue.get()
            if batch is None:
                break           # Done.
            size, lines = batch.size(), batch.lines()
            if self._load_batch(batch):
                self.success[0] += 1
                self.success[1] += size
                self.success[2] += lines
            else:
                self.failure[0] += 1
                self.failure[1] += size
                self.failure[2] += lines
            del batch
        notelog(self.name, 'worker thread exiting')
        self.datamover.post(None)
        self.datamover.join()
        return None

    def get_statistics(self):
        """Return statistics for posted batches.

        Does not perform any locking, oh well.

        @return a 2-tuple of (total_batches, total_bytes, total_lines)
                3-tuples: the first 3-tuple is for successfully loaded
                batches; the second is for failed batches.  (The same
                kind of 3-tuple is returned by Batch.get_statistics().)
        """
        return (tuple(self.success), tuple(self.failure))

    def _load_batch(self, batch):
        """Run AFL query to insert this batch of data.

        Returns True on success, False on failure.
        """
        # Build and run SciDB insert() query.
        if _args.target_array:
            afl = "insert(redimension(input({0},'{1}',-2,'{2}'),{3}), {3})".format(
                self.load_schema, self.fifo_name, _args.format, _args.target_array)
        else:
            assert self.load_array
            afl = "insert(input({0}, '{1}', -2, '{2}'), {0})".format(
                self.load_array, self.fifo_name, _args.format)
        if _args.no_load:
            infolog("+", afl)
            return True
        debuglog("Load", batch, 'with', afl)

        # Here we go.
        self.datamover.post(batch)
        cmd = copy.deepcopy(_iquery)
        cmd.extend(['-naq', afl])
        p = start_cmd(cmd)
        out = wait_cmd(p, quiet=True)
        if p.returncode == 0:
            infolog("Done loading %s, %d failures so far" % (
                    batch, self.failure[0]))
            return True

        errlog('Query failed with status', p.returncode, 'for batch', batch)
        if out and not out.isspace():
            errlog('-'*72, '\n', out, '\n', '-'*72)
        if 0:
            if _args.verbose > 3: # batch could be large...
                debuglog('--- begin bad %s ---' % str(batch))
                for i, line in enumerate(batch.get_file()):
                    debuglog('[%d] %s' % (i, line.rstrip()))
                debuglog('--- end bad batch ---')

        return False

def get_next_line(file_, tmo_seconds):
    """A generator that produces lines, timeouts, or EOF.

    @param file_        a readable file object.
    @param tmo_seconds  timeout value in seconds.

    @retval '' (an empty string) indicates a timeout.
    @retval None indicates EOF.
    @retval <data> a full line of data.
    """
    poll = select.poll()
    set_nonblock(file_)
    poll.register(file_.fileno())
    partial_line = ''
    while True:
        try:
            ready = poll.poll(tmo_seconds *  1000) # millis
        except select.error as e:
            if e[0] == errno.EINTR:
                yield ''        # pretend timeout allows for signal checking
                continue
            raise
        if not ready:
            yield ''            # timeout
        else:
            while True:
                try:
                    buf = file_.read(4096)
                except IOError as e:
                    if e.errno == errno.EWOULDBLOCK:
                        break
                    raise
                if not buf:
                    debuglog("eof on input")
                    yield None
                else:
                    lines = buf.split('\n')
                    start = 0
                    if partial_line:
                        yield ''.join((partial_line, lines[0], '\n'))
                        partial_line = ''
                        start = 1
                    for line in lines[start:-1]:
                        yield ''.join((line, '\n'))
                    partial_line = lines[-1]
                    if _args.verbose > 3 and partial_line:
                        debuglog('Partial line: %s' % partial_line)

def loadpipe_loop():
    """Read lines from stdin, batch them up, and load each batch.

    Lines are batched until either (a) the --batch-lines threshold is
    exceeded, (b) the --batch-size threshold is exceeded, or (c) the
    timeout has expired on a non-empty batch.  Then post the batch to
    the LoaderThread.

    @retval 0 EOF was reached on input.
    @retval 1 Terminated by signal.
    """
    global _loader
    _loader = LoaderThread()
    _loader.start()
    ret = 0
    batch = Batch()
    try:
        for line in get_next_line(sys.stdin, _args.timeout):
            if check_for_signals() and not batch.empty():
                debuglog("post on signal:", batch)
                _loader.post(batch)
                batch = Batch()
            if line is None:
                # eof
                if batch.empty():
                    del batch
                else:
                    # Last one!
                    debuglog("post final", batch)
                    _loader.post(batch)
                    batch = None
                break
            if not line:
                # timeout
                if not batch.empty():
                    # Timeout expired.
                    debuglog("timeout posts", batch)
                    _loader.post(batch)
                    batch = Batch()
            else:
                batch.add(line)
                if batch.full():
                    debuglog("post full", batch)
                    _loader.post(batch)
                    batch = Batch()
    except KeyboardInterrupt:
        ret = 1
    # Wait for worker thread to finish.
    _loader.post(None)
    _loader.join()
    del _loader
    notelog("Goodbye")
    return ret

def main(argv=None):
    if argv is None:
        argv = sys.argv

    global _pgm
    _pgm = os.path.basename(argv[0])

    global _parser
    _parser = argparse.ArgumentParser(
        description='Incrementally load a SciDB array from stdin.',
        epilog="Type 'pydoc {0}' for more information.".format(_pgm))
    group = _parser.add_mutually_exclusive_group()
    group.add_argument('-b', '--batch-size', default='16KiB', help='''
        Insert into array when buffered data reaches this size.  Multiplier
        suffixes are understood, e.g. 64K, 10MiB, 2G.  (Default = 16KiB)
        ''')
    group.add_argument('-B', '--batch-lines', default=0, type=int, help='''
        Insert into array whenever this many input lines have been batched.''')
    # Choosing -c/--host (and -p/--port) to be identical to iquery options.
    _parser.add_argument('-c', '--host', type=str, help='''
        Host of one of the cluster instances.  (Default = 'localhost')''')
    _parser.add_argument('-l', '--log', dest='facility', default='local0',
                        choices=_facility_choices, help='''
        Specify syslog(3) logging facility. (Default = local0)''')
    _parser.add_argument('-n', '--no-load', default=False, action='store_true',help='''
        Do not actually perform the load query.  Used for debugging.''')
    _parser.add_argument('-p', '--port', type=int, help='''
        Port for connection to SciDB.  (Default = 1239)''')
    _parser.add_argument('-Q', '--max-queue', type=int, default=0, help='''
        Never let array loading fall behind by more than this many batches.
        Reading from stdin ceases until array insertion catches up.
        (Default = 0 (unlimited queue size))''')
    _parser.add_argument('-t', '--timeout', type=int, default=10, help='''
        Maximum time to wait (in seconds) before performing an 'insert'
        of batched data.''')
    _parser.add_argument('-s', '--load-schema', required=True,
                        dest='array_or_schema', help='''
        Flat (1-D) schema describing the input, or name of array with the
        desired 1-D schema.''')
    _parser.add_argument('-A', '--target-array', help='''
        Target array name into which we'll insert the redimensioned
        data.  Must exist if specified.  If unspecified,
        -s/--load-schema must have named an existing array, which will
        be used as the target without any further redimensioning.''')
    _parser.add_argument('-i', '--input-file', help='''
        Input file (Default = stdin).''')
    _parser.add_argument('-f', '--format', default='tsv', help='''
        Format string to supply to input() operator (Default = tsv)''')
    _parser.add_argument('-v', '--verbose', default=0, action='count', help='''
        Increase logging verbosity.''')

    global _args
    _args = _parser.parse_args(argv[1:])

    # Above we don't use default=... for these options, so that the
    # iquery defaults will prevail.
    global _iquery
    if _args.host:
        _iquery.extend(['-c', _args.host])
    if _args.port:
        _iquery.extend(['-p', str(_args.port)])

    # See http://www.chiark.greenend.org.uk/ucgi/~cjwatson/blosxom/2009-07-02
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    signal.signal(signal.SIGUSR1, _sigusr1_handler)
    signal.signal(signal.SIGUSR2, _sigusr2_handler)
    signal.signal(signal.SIGTERM, _sigterm_handler)
    setup_logging()

    Batch.set_max_lines(_args.batch_lines)
    Batch.set_max_bytes(number(_args.batch_size))
    LoaderThread.set_max_queue(_args.max_queue)

    notelog(' '.join(('Starting loadpipe, max batch %s, max batch lines %d,',
                      'timeout %ds, max queue %d')) % (
           _args.batch_size, _args.batch_lines, _args.timeout, _args.max_queue))
    infolog('Info logging is enabled (verbose=%d)' % _args.verbose)
    debuglog('Debug logging is enabled')

    return loadpipe_loop()

if __name__ == '__main__':
    try:
        sys.exit(main())
    except Usage as e:
        print >>sys.stderr, e
        _parser.print_help()
        sys.exit(2)
    except AppError as e:
        print >>sys.stderr, e
        sys.exit(2)
