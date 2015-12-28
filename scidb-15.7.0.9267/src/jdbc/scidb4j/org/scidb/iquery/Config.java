/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2015-2015 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/
package org.scidb.iquery;

import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

/**
 * Config is a class managing the config parameters for Iquery.
 * It could be placed inside Iquery.
 * I keep it separate for now, in case it is needed by a GUI client.
 * @author Donghui Zhang (original)
 * @author Marty Corbett (add setters, getters, and show method)
 */
public class Config
{
    private int _precision         = 6;
    private String _host           = new String("localhost");
    private int _port              = 1239;
    private String _query          = new String();
    private String _queryFile      = new String();
    private String _result         = new String("console");
    private String _format         = new String("dcsv");
    private String _userName       = new String();
    private String _userPassword   = new String();
    private boolean _verbose       = false;
    private boolean _timer         = false;
    private boolean _noFetch       = false;
    private boolean _afl           = false;
    private boolean _version       = false;
    private boolean _ignoreErrors  = false;
    private boolean _bypassUsrCfgPermsChk = false;

    public void setPrecision(int newValue) { _precision = newValue; }
    public void setHost(String newValue) { _host = newValue; }
    public void setPort(int newValue) { _port = newValue; }
    public void setQuery(String newValue) { _query = newValue; }
    public void setQueryFile(String newValue) { _queryFile = newValue; }
    public void setResult(String newValue) { _result = newValue; }
    public void setFormat(String newValue) { _format = newValue; }
    public void setUserName(String newValue) { _userName = newValue; }
    public void setUserPassword(String newValue) { _userPassword = newValue; }
    public void setVerbose(boolean newValue) { _verbose = newValue; }
    public void setTimer(boolean newValue) { _timer = newValue; }
    public void setNoFetch(boolean newValue) { _noFetch = newValue; }
    public void setAfl(boolean newValue) { _afl = newValue; }
    public void setVersion(boolean newValue) { _version = newValue; }
    public void setIgnoreErrors(boolean newValue) { _ignoreErrors = newValue; }
    public void setBypassUsrCfgPermsChk(
        boolean newValue)
    {
        _bypassUsrCfgPermsChk = newValue;
    }

    public int getPrecision() { return _precision; }
    public String getHost() { return _host; }
    public int getPort() { return _port; }
    public String getQuery() { return _query; }
    public String getQueryFile() { return _queryFile; }
    public String getResult() { return _result; }
    public String getFormat() { return _format; }
    public String getUserName() { return _userName; }
    public String getUserPassword() { return _userPassword; }
    public boolean getVerbose() { return _verbose; }
    public boolean getTimer() { return _timer; }
    public boolean getNoFetch() { return _noFetch; }
    public boolean getAfl() { return _afl; }
    public boolean getVersion() { return _version; }
    public boolean getIgnoreErrors() { return _ignoreErrors; }
    public boolean getBypassUsrCfgPermsChk()
    {
        return _bypassUsrCfgPermsChk;
    }

    public void show()
    {
        System.out.println("precision    = " + getPrecision());
        System.out.println("host         = " + getHost());
        System.out.println("port         = " + getPort());
        System.out.println("query        = " + getQuery());
        System.out.println("queryFile    = " + getQueryFile());
        System.out.println("result       = " + getResult());
        System.out.println("format       = " + getFormat());
        System.out.println("userName     = " + getUserName());
        System.out.println("userPassword = " + getUserPassword());
        System.out.println("verbose      = " + getVerbose());
        System.out.println("timer        = " + getTimer());
        System.out.println("noFetch      = " + getNoFetch());
        System.out.println("afl          = " + getAfl());
        System.out.println("version      = " + getVersion());
        System.out.println("ignoreErrors = " + getIgnoreErrors());
        System.out.println("bypassUsrCfgPermsChk = " +
            getBypassUsrCfgPermsChk());
    }

    /**
     * Parse command-line parameters into the Config object.
     */
    public void parse(String[] args)
    {
        // In shortOpts:
        //  - an option not followed by any colon does not have an argument.
        //  - an option followed by a single colon has a required argument.
        //  - an option followed by two colons has an optional argument.
        //  - @see the comment at the beginnnig of Getopt.java.
    String shortOpts = "w:c:p:q:f:r:o:vtnahViU:P:b";

    LongOpt[] longOpts = {
            new LongOpt("precision",     LongOpt.REQUIRED_ARGUMENT, null, 'w'),
            new LongOpt("host",          LongOpt.REQUIRED_ARGUMENT, null, 'c'),
            new LongOpt("port",          LongOpt.REQUIRED_ARGUMENT, null, 'p'),
            new LongOpt("query",         LongOpt.REQUIRED_ARGUMENT, null, 'q'),
            new LongOpt("query_file",    LongOpt.REQUIRED_ARGUMENT, null, 'f'),
            new LongOpt("result",        LongOpt.REQUIRED_ARGUMENT, null, 'r'),
            new LongOpt("format",        LongOpt.REQUIRED_ARGUMENT, null, 'o'),
            new LongOpt("user-name",     LongOpt.REQUIRED_ARGUMENT, null, 'U'),
            new LongOpt("user-password", LongOpt.REQUIRED_ARGUMENT, null, 'P'),
            new LongOpt("verbose",       LongOpt.NO_ARGUMENT,       null, 'v'),
            new LongOpt("timer",         LongOpt.NO_ARGUMENT,       null, 't'),
            new LongOpt("no_fetch",      LongOpt.NO_ARGUMENT,       null, 'n'),
            new LongOpt("afl",           LongOpt.NO_ARGUMENT,       null, 'a'),
            new LongOpt("help",          LongOpt.NO_ARGUMENT,       null, 'h'),
            new LongOpt("version",       LongOpt.NO_ARGUMENT,       null, 'V'),
            new LongOpt("ignore_errors", LongOpt.NO_ARGUMENT,       null, 'i'),
            new LongOpt("bypass_usr_cfg_perms_chk", LongOpt.NO_ARGUMENT,  null, 'b'),
        };

        Getopt getopt = new Getopt("Iquery", args, shortOpts, longOpts);
        int c;
        while ((c = getopt.getopt()) != -1)
        {
            switch (c)
            {
            case 'w':
                setPrecision(Integer.parseInt(getopt.getOptarg()));
                break;
            case 'c':
                setHost(getopt.getOptarg());
                break;
            case 'p':
                setPort(Integer.parseInt(getopt.getOptarg()));
                break;
            case 'q':
                setQuery(getopt.getOptarg());
                break;
            case 'f':
                setQueryFile(getopt.getOptarg());
                break;
            case 'r':
                setResult(getopt.getOptarg());
                break;
            case 'o':
                setFormat(getopt.getOptarg());
                break;
            case 'U':
                setUserName(getopt.getOptarg());
                break;
            case 'P':
                setUserPassword(getopt.getOptarg());
                break;
            case 'v':
                setVerbose(true);
                break;
            case 't':
                setTimer(true);
                break;
            case 'n':
                setNoFetch(true);
                break;
            case 'a':
                setAfl(true);
                break;
            case 'i':
                setIgnoreErrors(true);
                break;
            case 'b':
                setBypassUsrCfgPermsChk(true);
                break;
            case 'h':
                printHelp();
                System.exit(0);
            case 'V':
                printVersion();
                System.exit(0);
            case '?':
                System.exit(1);
            }
        }
    }

    private void printVersion()
    {
        // Not implemented yet.
        assert false;
    }

    private void printHelp()
    {
        System.out.println("Iquery is a command-line tool that interacts with a SciDB server.\n");
        System.out.println("usage: CLASSPATH=.../protobuf.jar:.../scidb4j.jar  java  org.scidb.iquery.Iquery  [options] \"QUERY_STRING\"\n");
        System.out.println("Available options:");
        System.out.println("     -w [ --precision ]      arg Precision for printing floating point numbers. ");
        System.out.println("                                 Default is 6");
        System.out.println("     -c [ --host ]           arg Host of one of the cluster instances. Default is ");
        System.out.println("                                 'localhost'");
        System.out.println("     -p [ --port ]           arg Port for connection. Default is 1239");
        System.out.println("     -q [ --query ]          arg Query to be executed");
        System.out.println("     -f [ --query-file ]     arg File with query to be executed");
        System.out.println("     -r [ --result ]         arg Filename with result array data.");
        System.out.println("     -o [ --format ]         arg Output format: auto, csv, dense, csv+, lcsv+, text, ");
        System.out.println("                                 sparse, lsparse, store, text, opaque, tsv, tsv+, ");
        System.out.println("                                 ltsv+, dcsv. Default is 'dcsv'.");
        System.out.println("     -U [ --user-name ]      arg User name");
        System.out.println("     -P [ --user-password ]  arg User password");
        System.out.println("     -v [ --verbose ]        Print debug info. Disabled by default");
        System.out.println("     -t [ --timer ]          Print query execution time (in milliseconds)");
        System.out.println("     -n [ --no-fetch ]       Skip data fetching. Disabled by default");
        System.out.println("     -a [ --afl ]            Switch to AFL query language mode. AQL by default");
        System.out.println("     -h [ --help ]           Show help");
        System.out.println("     -V [ --version ]        Show version info");
        System.out.println("     -i [--ignore-errors]    Ignore execution errors in batch mode");
    }
};
