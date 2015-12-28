#!/usr/bin/python
#
# BEGIN_COPYRIGHT
#
# Copyright (C) 2015 SciDB, Inc.
# All Rights Reserved.
#
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# END_COPYRIGHT
#
"""
This script produces a package, either debian or rpm,
that will install the latest ANT into /opt/scidb/VERSION/3rdparty
"""
import argparse
import atexit
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile

# GLOBALS
_args = None    # Parsed arguments.
_MYDIR = None   # Directory this script is in.
_OS = None      # The OS we are running on.
_PGM = None     # This script's name (basename).
_TMPDIR = None  # Temporary directory used as packaging directory.

# CONSTANTS
_OS_CENTOS = "CentOS"
_OS_UBUNTU = "Ubuntu"

_ANT_TARBALL = "https://downloads.paradigm4.com/centos6.3/3rdparty_binaries/apache-ant-1.9.4-bin.tar.bz2"
_ANT_TARFILE = "apache-ant-1.9.4-bin.tar.bz2"

# UTILITY ROUTINES
def create_tempdir():
    global _TMPDIR

    _TMPDIR = tempfile.mkdtemp()
    atexit.register(remove_tempdir)


def remove_tempdir():
    if not _args.verbose:
        shutil.rmtree(_TMPDIR)


class AppError(Exception):
    pass


def printDebug(string, force=False):
    if _args.verbose or force:
        print >> sys.stderr, "DEBUG(%s): %s" % (_PGM, string)

def replaceVERSION(inFile, outFile):
    # Rewrite the inFile file to the outFile file substituting the scidb version for the string VERSION
    with open(inFile, "r") as source:
        contents = source.read()
    with open(outFile, "w") as output:
        output.write(contents.replace('VERSION',_args.version))
    printDebug("Rewrote %s" % outFile)


def prepare_package(OS, OSversion):
    """Prepare the packaging directory."""
    if OS == _OS_CENTOS:
        SOURCES_dir=os.path.join(_TMPDIR, "SOURCES")
        SPECS_dir=os.path.join(_TMPDIR, "SPECS")
        # Create SOURCES directory
        if not os.path.exists(SOURCES_dir):
            try:
                os.makedirs(SOURCES_dir)
            except:
                raise AppError("Unable to create SOURCES directory '%s'." % SOURCES_dir)
        printDebug("Created %s" % SOURCES_dir)
        # Create SPECS directory
        if not os.path.exists(SPECS_dir):
            try:
                os.makedirs(SPECS_dir)
            except:
                raise AppError("Unable to create SPECS directory '%s'." % SPECS_dir)
        printDebug("Created %s" % SPECS_dir)
        # Rewrite specs file, substituting scidb version for string VERSION
        replaceVERSION(os.path.join(_MYDIR, "scidb-ant.spec"), os.path.join(SPECS_dir, "scidb-ant.spec"))
        # Copy into SOURCES _ANT_TARBALL
        cmdline = ["wget",
                   "--directory-prefix=" + SOURCES_dir,
                   _ANT_TARBALL]
        printDebug(cmdline)
        subprocess.call(cmdline)
    elif OS == _OS_UBUNTU:
        package_dir=os.path.join(_TMPDIR,  "scidb-" + _args.version + "-ant")
        DEBIAN_dir=os.path.join(package_dir, "DEBIAN")
        OPT_THIRD_dir=os.path.join(package_dir, "opt", "scidb", _args.version, "3rdparty")
        # Create DEBIAN directory
        if not os.path.exists(DEBIAN_dir):
            try:
                os.makedirs(DEBIAN_dir)
            except:
                raise AppError("Unable to create DEBIAN directory '%s'." % DEBIAN_dir)
        printDebug("Created %s" % DEBIAN_dir)
        # Create OPT THIRD directory
        if not os.path.exists(OPT_THIRD_dir):
            try:
                os.makedirs(OPT_THIRD_dir)
            except:
                raise AppError("Unable to create OPT_THIRD directory '%s'." % OPT_THIRD_dir)
        printDebug("Created %s" % OPT_THIRD_dir)
        # Rewrite control file, substituting scidb _args.version
        replaceVERSION(os.path.join(_MYDIR, "control"), os.path.join(DEBIAN_dir, "control"))
        # Copy into THIRD _ANT_TARBALL
        cmdline = ["wget",
                   "--directory-prefix=" + _TMPDIR,
                   _ANT_TARBALL]
        printDebug(cmdline)
        subprocess.call(cmdline)
        cmdline = ["tar", "-xpf",
                   os.path.join(_TMPDIR, _ANT_TARFILE),
                   "-C", OPT_THIRD_dir]
        printDebug(cmdline)
        subprocess.call(cmdline)

def create_package(OS):
    """Call the package creation method particular to the OS"""
    if OS == _OS_CENTOS:
        cmdline = ["rpmbuild",
                   "--quiet",
                   "--define", "_topdir " + _TMPDIR,
                   "-bb", os.path.join(_TMPDIR, "SPECS", "scidb-ant.spec")]
        if _args.verbose:
            cmdline.remove("--quiet")
        printDebug(cmdline)
        subprocess.call(cmdline)
    elif OS == _OS_UBUNTU:
        cmdline = ["dpkg-deb",
                   "--build",
                   os.path.join(_TMPDIR, "scidb-" + _args.version + "-ant")]
        printDebug(cmdline)
        with open("/dev/null", "w") as null:
            subprocess.call(cmdline,stdout=null,stderr=null)


def copyout_results(OS):
    """Copy out the resulting package to the results directory."""
    if OS == _OS_CENTOS:
        shutil.copy(
            os.path.join(_TMPDIR, "RPMS", "noarch", "scidb-" + _args.version + "-ant-0-1.noarch.rpm"),
            _args.results_dir)
    elif OS == _OS_UBUNTU:
        shutil.copy(
            os.path.join(_TMPDIR, "scidb-" + _args.version + "-ant.deb"),
            os.path.join(_args.results_dir, "scidb-" + _args.version + "-ant.deb"))


def main(argv=None):
    global _args
    global _MYDIR
    global _OS
    global _PGM

    if argv is None:
        argv = sys.argv

    _PGM = os.path.basename(argv[0])
    _MYDIR = os.path.dirname(os.path.realpath(argv[0]))

    # Where are we (what OS).
    _OS, OSversion, OScodename = platform.linux_distribution()

    # Argument processing
    if _OS == _OS_CENTOS:
        parser = argparse.ArgumentParser(
            description='Produce an rpm package for Apache Ant that is used for building JDBC',
            epilog='Type "pydoc %s" for more information.' % _PGM)
    elif _OS == _OS_UBUNTU:
        parser = argparse.ArgumentParser(
            description='Produce a debian package for Apache Ant that is used for building JDBC',
            epilog='Type "pydoc %s" for more information.' % _PGM)
    else:
        raise AppError("Unsupported Platform.")
    parser.add_argument('version', help='The SciDB Version you are building.')
    parser.add_argument('results_dir',  help='Directory to put the package in.')
    parser.add_argument('-v','--verbose', action='store_true', help="display verbose output")
    _args = parser.parse_args(argv[1:])

    printDebug("OS=%s, OSversion=%s, OScodename=%s" % (_OS,OSversion,OScodename))
    printDebug("version=%s, results_dir=%s" % (_args.version,_args.results_dir))

    # Argument Checks
    if re.match("^\d{2}?\.\d{1,2}?$", _args.version) is None:
        raise AppError("Version '%s' is not valid." % _args.version)
    if not os.path.exists(_args.results_dir):
        try:
            os.makedirs(_args.results_dir)
        except:
            raise AppError("Unable to create results directory '%s'." % _args.results_dir)
    if not os.access(_args.results_dir, os.W_OK):
        raise AppError("Unable to write to results directory '%s'." % _args.results_dir)
    # User ID Check
    if os.geteuid() != 0:
        raise AppError("Script must be run with root privileges.")

    create_tempdir()
    printDebug("TMPDIR=%s" % _TMPDIR)

    prepare_package(_OS, OSversion)

    create_package(_OS)

    copyout_results(_OS)

    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except AppError as e:
        print >>sys.stderr, "ERROR(%s): %s" % (_PGM, e)
        sys.exit(2)
