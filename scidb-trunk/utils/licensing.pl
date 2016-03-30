#!/usr/bin/perl
#
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
################################################################
# This script takes as input a directory name and a license file.
# and modifies the license block in all files
# recursively within the specified containing directory
# to have the license specified.
#
# The license file contains the license terms and conditions
# and the copyright string "Copyright (C) 2008-2015 ..."
#
# The license text is delimited
# starting with a line that includes BEGIN_COPYRIGHT
# and ends with a line that includes END_COPYRIGHT.
#
# The use of COPYRIGHT as a delimiter is for historical reasons
# and in no way limits the license itself.
#
# @note:
#   - A file's beginning copyright year will be preserved.
#   - The license block will be replaced in full even if it is the same
#     as the specified license file.
#
# @note The following directories and files are omitted:
#   - all directories in @SKIP_DIRS
#   - all files in @SKIP_FILES
#   - all files ending with ~
#
# @author dzhang
# @original script: scidb/utils/check_or_replace_copyright.pl
# @adapted by mresnick
################################################################
use strict;
use File::Basename;

# Some constants.
my $BEGIN_LICENSE = "BEGIN_COPYRIGHT";
my $END_LICENSE = "END_COPYRIGHT";

my $STATE_BEFORE = 0;  # The state before seeing the license block (when scanning a file)
my $STATE_DURING = 1;  # The state during processing the license block.
my $STATE_AFTER  = 2;  # The state after processing the license block.

# The following variables will be set by getNewLicense(), which is called by main().
my @newLicense;

# The following dir/file names will be skipped.
my @SKIP_DIRS = (
    ".",
    "..",
    "stage",
    ".svn",
    "3rdparty",
    "lib_json"
);

my @SKIP_FILES = (
    "bsdiff.c",
    "bspatch.c",
    "FindProtobuf.cmake", # See ticket:3215
    "MurmurHash3.h",
    "MurmurHash3.cpp",
    "statistics.py",      # The statistics.py package was introduced in Python 3. But we have Python 2.
    "counter.py",         # The Counter class was introduced in Python 2.7. But we have 2.6.6.
    "PSF_license.txt",    # The file that contains the PSF license agreement
    "scidb_psf.py"        # Code we borrowed, that have PSF license
);

# As the name says.
#
sub printUsageAndExit
{
    print "Usage: ", __FILE__, " rootDir licenseFile\n";
    print "  - rootDir is the directory to start in\n";
    print "\n";
    print "Effect: Make all license blocks be of the chosen license.\n";
    print "  Traverse down from rootDir scanning every file and\n";
    print "  replacing license blocks with the specified licenseType.\n";
    exit;
}

# Replace the license block in a file with a new license block
# @param  file
# @action replace the whole license block with the new license block.
#
sub replace
{
    our @newLicense;

    my($file) = @_;
    my $state = $STATE_BEFORE;
    my $commentString;
    my $copyrightStartYear;
    my $mode;

    my $line;
    # Divide the file into three pieces: beforeLicense, oldLicense, afterLicense.
    local(*HAND);
    my @beforeLicense = ();
    my @oldLicense = ();
    my @afterLicense = ();
    open(HAND, $file) or die "Can't open $file for read.\n";
    $state = $STATE_BEFORE;
    while ($line = <HAND>) {
        if ($state == $STATE_BEFORE) {
            if ( $line =~ /^(\s*.)\s+$BEGIN_LICENSE/ ) {
		$commentString = $1;
                push @oldLicense, $line;
                $state = $STATE_DURING;
            }
            else {
                push @beforeLicense, $line;
            }
        }
        elsif ($state == $STATE_DURING) {
            push @oldLicense, $line;
            if ( $line =~ /^\s*(.)\s+$END_LICENSE/ ) {
                $state = $STATE_AFTER;
            }
            if    ( $line =~ /^(.*Copyright.*)(\d\d\d\d)(.*\d\d\d\d)(.*)$/ ) {
		$copyrightStartYear = $2;
	    }
	    elsif ( $line =~ /^(.*Copyright.*)(\d\d\d\d)(.*)$/ ) {
		$copyrightStartYear = $2;
	    }
        }
        else {
            push @afterLicense, $line;
        }
    }
    close HAND;

    if ($state != $STATE_AFTER) {
	return;
    }
    print "Licensing " . $file . "\n";

    local(*HAND2);
    open(HAND2, ">$file") or die "Can't write to $file.";

    foreach $line (@beforeLicense) {
        print HAND2 $line;
    }
    foreach $line (@newLicense) {
	my $line2 = $line;
	$line2 =~ s/^\s*#/$commentString/;
	if ( $line2 =~ /^(.*Copyright.*)(\d\d\d\d)(.*\d\d\d\d)(.*)$/ ) {
	    print HAND2 $1 . $copyrightStartYear . $3 . $4 . "\n";
	} else {
	    print HAND2 $line2;
	}
    }
    foreach $line (@afterLicense) {
        print HAND2 $line;
    }
    close HAND2;
}

# For each file in the directory
# Replace the license block appropriately
# @param dir
#
sub loopDir
{
    my($dir) = @_;
    chdir($dir) || die "Cannot chdir to $dir.\n";
    local(*DIR);
    opendir(DIR, ".");
    while (my $f = readdir(DIR)) {
        next if ( -l $f || $f =~ /\~$/ );  # skip symbolic links and names ending with tilde.
        next if ( $f =~ /\.lic$/ );  # skip names ending with '.lic'.
        if (-d $f) {
            next if (grep {$_ eq $f} @SKIP_DIRS);
            &loopDir($f);
        }
        elsif (-f $f) {
            next if (grep {$_ eq $f} @SKIP_FILES);
	    &replace($f);
        }
    }
    closedir(DIR);
    chdir("..");
}

# Get the new license.
# @param licenseType
# @note assign values to
#    - @newLicense
#
sub getNewLicense
{
    my($licenseFile) = @_;
    our @newLicense;

    local(*HAND);
    my $state = $STATE_BEFORE;

    open(HAND, $licenseFile) or die "Can't open $licenseFile for read.\n";
    while (my $line = <HAND>) {
        if ($state == $STATE_BEFORE) {
            if ( $line =~ /^\s*(.)\s+$BEGIN_LICENSE/ ) {
                push @newLicense, $line;
                $state = $STATE_DURING;
            }
        }
        elsif ($state == $STATE_DURING) {
            push @newLicense, $line;
            if ( $line =~ /^\s*(.)\s+$END_LICENSE/ ) {
                $state = $STATE_AFTER;
            }
        }
    }
    close HAND;
}

# The main function.
#
sub main
{
    if ($#ARGV ne 1) {
	print "Warning: licensing.pl() Wrong number of arguments.\n\n";
        printUsageAndExit;
    }
    my $root = $ARGV[0];
    my $licenseFile = $ARGV[1];

    if (not -d $root) {
	print "Warning: licensing.pl() rootDir $root is not a directory.\n\n";
	printUsageAndExit;
    }
    if (not -f $licenseFile) {
	print "Warning: licensing.pl() licenseFile $licenseFile not found.\n\n";
	printUsageAndExit;
    }

    &getNewLicense($licenseFile);

    &loopDir($root);
}

&main;
