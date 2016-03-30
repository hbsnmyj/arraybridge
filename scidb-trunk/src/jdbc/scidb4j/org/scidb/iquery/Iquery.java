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

import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import static java.lang.System.exit;
import static java.lang.System.out;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import java.sql.SQLException;

import java.util.Iterator;
import java.util.Vector;

import org.scidb.client.*;
import org.scidb.jdbc.ResultSet;
import org.scidb.client.SciDBException;
import org.scidb.client.QueryID;

/**
 * Iquery simulates iquery.
 * It does NOT support interactive mode.
 * It does NOT support OPAQUE/BINARY format.
 * @author Donghui Zhang
 */
public class Iquery
{
    /**
     * Iquery.State stores state information for the iquery session.
     * @note It simulates IqueryState used in the C++ client 'iquery'.
     * @author Donghui Zhang (original)
     * @author Marty Corbett (added setters & getters)
     */
    public class State
    {
        private long     _col;
        private long     _line;
        private long     _queryStart;
        private boolean  _insideComment;
        private boolean  _insideString;
        private boolean  _aql;
        private boolean  _interactive;
        private org.scidb.client.Connection _connection;
        private QueryID  _currentQueryID;
        private boolean  _firstSaving; //For clearing result file for the first time and appending next times
        private boolean  _nofetch;
        private boolean  _timer;
        private boolean  _verbose;
        private boolean  _ignoreErrors;
        private String   _format;
        private String   _userName;
        private String   _userPassword;
        private boolean  _bypassUsrCfgPermsChk;

        public long     getCol() { return _col; }
        public long     getLine() { return _line; }
        public long     getQueryStart() { return _queryStart; }
        public boolean  getInsideComment() { return _insideComment; }
        public boolean  getInsideString() { return _insideString; }
        public boolean  getAql() { return _aql; }
        public boolean  getInteractive() { return _interactive; }
        public org.scidb.client.Connection getConnection() { return _connection; }
        public QueryID  getCurrentQueryID() { return _currentQueryID; }
        public boolean  getFirstSaving() { return _firstSaving; }
        public boolean  getNoFetch() { return _nofetch; }
        public boolean  getTimer() { return _timer; }
        public boolean  getVerbose() { return _verbose; }
        public boolean  getIgnoreErrors() { return _ignoreErrors; }
        public String   getFormat() { return _format; }
        private String  getUserName() { return _userName; }
        private String  getUserPassword() { return _userPassword; }
        public boolean  getBypassUsrCfgPermsChk() {
            return _bypassUsrCfgPermsChk;
        }

        public void     setCol(long newValue) { _col = newValue; }
        public void     setLine(long newValue) { _line = newValue; }
        public void     setQueryStart(long newValue) { _queryStart = newValue; }
        public void     setInsideComment(boolean newValue) { _insideComment = newValue; }
        public void     setInsideString(boolean newValue) { _insideString = newValue; }
        public void     setAql(boolean newValue) { _aql = newValue; }
        public void     setInteractive(boolean newValue) { _interactive = newValue; }
        public void     setConnection(org.scidb.client.Connection newValue) { _connection = newValue; }
        public void     setCurrentQueryID(QueryID newValue) { _currentQueryID = newValue; }
        public void     setFirstSaving(boolean newValue) { _firstSaving = newValue; }
        public void     setNoFetch(boolean newValue) { _nofetch = newValue; }
        public void     setTimer(boolean newValue) { _timer = newValue; }
        public void     setVerbose(boolean newValue) { _verbose = newValue; }
        public void     setIgnoreErrors(boolean newValue) { _ignoreErrors = newValue; }
        public void     setFormat(String newValue) { _format = newValue; }
        public void     setUserName(String newValue) { _userName = newValue; }
        public void     setUserPassword(String newValue) { _userPassword = newValue; }
        public void     setBypassUsrCfgPermsChk(boolean newValue) {
            _bypassUsrCfgPermsChk= newValue;
        }

        public void update(Config config)
        {
            setUserName(config.getUserName());
            setUserPassword(config.getUserPassword());
            setBypassUsrCfgPermsChk(
                config.getBypassUsrCfgPermsChk());
        }
    }

    private State _state;
    public State getState() { return _state; }
    public void setState(State newValue) { _state = newValue; }


    /// Constructor.
    public Iquery()
    {
        this.setState(new State());
    }


    /**
     * Connect to specified SciDB instance
     * @param host Host name
     * @param port Port number
     * @throws SciDBException
     * @throws SciDBException, IOException
     */
    private void connect(String host, int port)
        throws SciDBException, IOException
    {
        getState().getConnection().getNetwork().connect(host, port);
    }

    /**
     * Tells SciDB that a new client is starting.
     * If SciDB is in authentication mode it will request the user
     * name and password.
     *
     * @param userName - passed to SciDB when "Login:" is requested
     * @param userPassword  - passed to SciDB when "Password:" is requested
     */
    public void startNewClient(
        String userName,
        String userPassword)
        throws IOException, SciDBException
    {
        getState().getConnection().startNewClient(
            getState().getUserName(),
            getState().getUserPassword());
    }


    /// The main function.
    public static void main(String[] args)
        throws ConfigUserException, IOException
    {
        //
        // Create an iquery object.
        //
        Iquery iquery = new Iquery();

        //
        // Parse command-line parameters.
        //
        Config config = new Config();
        try {
            config.parse(args);
            // config.show();
            iquery.getState().update(config);
        } catch (Exception e) {
            SciDBException.errorOut(e);
        }


        String scidbConfigUserFilename = config.getAuthFile();
        if(scidbConfigUserFilename.length() == 0)
        {
            scidbConfigUserFilename = System.getenv("SCIDB_CONFIG_USER");
        }

        if( (scidbConfigUserFilename!= null) &&
            (scidbConfigUserFilename.length() != 0))
        {
            try
            {
                ConfigUser configUser = ConfigUser.getInstance();
                configUser.verifySafeFile(
                    scidbConfigUserFilename,
                    iquery.getState().getBypassUsrCfgPermsChk());

                AuthenticationFile authFile;
                try
                {
                    authFile = new AuthenticationFile(scidbConfigUserFilename);
                    //System.out.println( authFile.toString() );
                } catch(FileNotFoundException ex) {
                    // Rethrow as ConfigUserException
                    throw new ConfigUserException(ex);
                } catch(IOException ex) {
                    // Rethrow as ConfigUserException
                    throw new ConfigUserException(ex);
                }

                String name     = authFile.getUserName();
                String password = authFile.getUserPassword();

                if((name.length() != 0) && (password.length() != 0))
                {
                    iquery.getState().setUserName(name);
                    iquery.getState().setUserPassword(password);
                }
            } catch(ConfigUserException ex) {
                // Print error and terminate application
                ex.printStackTrace();
                System.exit(1);
            } catch(IOException ex) {
                // Rethrow as ConfigUserException
                throw new ConfigUserException(ex);
            }
        }


        //
        // Set iquery.state.
        //
        iquery.getState().setInsideComment(false);
        iquery.getState().setInsideString( false);
        iquery.getState().setAql(!config.getAfl());
        iquery.getState().setCol(1);
        iquery.getState().setLine(1);
        iquery.getState().setInteractive(false);
        iquery.getState().setCurrentQueryID(new QueryID());
        iquery.getState().setFirstSaving(true);

        //
        // Connect to the server.
        //
        iquery.getState().setConnection(new Connection());
        try {
            iquery.connect(
                config.getHost(),
                config.getPort());

            iquery.startNewClient(
                iquery.getState().getUserName(),
                iquery.getState().getUserPassword());
        }
        catch (Exception e) {
            SciDBException.errorOut(e);
        }

        //
        // Figure out the query string(s) and whether this is interactive.
        //
        String queries = new String(); //all set of queries (can be divided with semicolon)

        if (!config.getQuery().isEmpty())
        {
            queries = config.getQuery();
        }
        else if (   config.getQuery().isEmpty() &&
                    !config.getQueryFile().isEmpty())
        {
            Path path = Paths.get(config.getQueryFile());
            queries = new String(Files.readAllBytes(path));
        }
        else if (   config.getQuery().isEmpty() &&
                    config.getQueryFile().isEmpty())
        {
            // Note from Donghui Zhang.
            // Here I omit the case of not providing a query, yet providing a '-' so as to read the query from stdin.
            // I feel the case is wierd.
            //
            //iquery.state.setInteractive(true);
            //
            // The above discussion and code deal with interactive mode, which is not supported.
            System.out.println("Iquery does not support interactive mode.");
            System.out.println("Please rerun, by providing '-aq' following by a query string.");
            System.exit(-1);
        }

        // Note from Donghui Zhang.
        // Here I omit the step of loading history.

        //
        // Below is the main loop.
        //

        // Whenever '{' is encountered, the count is increased by 1.
        // Whenever '}' is encountered, the count is reduced by 1.
        // The usage: do NOT terminate a query at ';' if the count is greater than 0.
        // TO-DO: negative count should be reported as an exception but omit this error checking for now.
        int nLevelsInsideCurlyBrackets = 0;

        String query = new String(); // separated query from overall set of queries

        do
        {
            // We analyzing begin of queries or next query, so reset position
            if (query.isEmpty())
            {
                iquery.getState().setCol(1);
                iquery.getState().setQueryStart(
                    iquery.getState().getLine());
            }

            // If we in interactive mode, requesting next line of query
            if (iquery.getState().getInteractive())
            {
                iquery.getState().setInsideComment(false);
                String prompt = (query.isEmpty()
                    ? (iquery.getState().getAql() ? "AQL% " : "AFL% ")
                    : "CON> ");

                System.out.print(prompt); System.out.flush();
                BufferedReader buffer=new BufferedReader(new InputStreamReader(System.in));
                String line = buffer.readLine();
                if (line!=null && !line.isEmpty()) {
                    queries = line;
                }
                else {
                    break;
                }

                // Ignore whitelines in begin of queries
                String trimmedQueries = queries.trim();
                if (trimmedQueries.isEmpty() && query.isEmpty()) {
                    continue;
                }
            }

            // Parsing next line of query
            char currC = 0;     // Character in current position
            char prevC = 0; // Character in previous position
            boolean eoq = false;

            for (int pos = 0; pos < queries.length(); ++pos)
            {
                prevC = currC;
                currC = queries.charAt(pos);

                // Checking string literal begin and end, but ignore if current part of query is comment
                if (    currC == '\'' &&
                        prevC != '\\' &&
                        !iquery.getState().getInsideComment())
                {
                    iquery.getState().setInsideString(
                        !iquery.getState().getInsideString());
                }

                // Checking comment, but ignore if current part of query is string
                if (    currC == '-' &&
                        prevC == '-' &&
                        !iquery.getState().getInsideString())
                {
                    iquery.getState().setInsideComment(true);
                }

                // Checking newline. Resetting comment if present
                if (currC == '\n')
                {
                    iquery.getState().setInsideComment(false);
                    iquery.getState().setLine(
                        iquery.getState().getLine() + 1);
                    iquery.getState().setCol(1);

                    if (query.isEmpty()) {
                        iquery.getState().setQueryStart(
                            iquery.getState().getLine());
                    }
                    else {
                        query += currC;
                    }
                }

                // Checking query separator, if not in string and comment, execute this query
                else if (   currC == ';' &&
                            !iquery.getState().getInsideComment() &&
                            !iquery.getState().getInsideString() &&
                            nLevelsInsideCurlyBrackets==0)
                {
                    iquery.executeCommandOrQuery(query, config);
                    query = new String();
                    eoq = true;
                    iquery.getState().setCol(
                        iquery.getState().getCol() + 1);
                }

                // Maintain nLevelsInsideCurlyBrackets
                else if ((  currC == '{' ||
                            currC == '}') &&
                            !iquery.getState().getInsideComment() &&
                            !iquery.getState().getInsideString())
                {
                    nLevelsInsideCurlyBrackets += (currC == '{' ? 1 : -1);
                    query += currC;
                }

                // All other just added to query
                else
                {
                    query += currC;
                    iquery.getState().setCol(
                        iquery.getState().getCol() + 1);
                }
            }

            if (eoq) {
                query = query.trim(); // Note: in iquery this is boost::trim_left.
            }

            // Adding last part of query to history
            //
            //if (iquery.getState().getInteractive()) {
            //    add_history(queries.c_str());
            //}

            // If interactive add newline after any part of query to maintain original query view
            if (iquery.getState().getInteractive() && !query.isEmpty())
            {
                query += '\n';
            }

            // Execute last part of query even without leading semicolon in non-interactive mode
            else if ( !iquery.getState().getInteractive() &&
                      !query.isEmpty())
            {
                iquery.executeCommandOrQuery(query, config);
            }
        }
        while (iquery.getState().getInteractive());

        // Note from Donghui Zhang.
        // Here I omit the step of saving history.
    } // end of main().

    /**
     * A private function supporting executeCommandOrQuery().
     * @return true if the executed query requires an explicit commit
     */
    private boolean executePreparedSciDBQuery(String queryString, Config config)
    throws IOException, FileNotFoundException, SciDBException
    {
        if (getState().getVerbose())
        {
            System.out.println("Query ID: " +
                getState().getCurrentQueryID());
        }

        Result result = getState().getConnection().execute();

        Array array = result.getArray();
        if (array!=null && !getState().getNoFetch())
        {
            /**
             * Printing result schema
             */
            if (getState().getVerbose())
            {
                Schema schema = array.getSchema();
                Schema.Attribute[] attributes = schema.getAttributes();
                Schema.Dimension[] dimensions = schema.getDimensions();
                String arrayName = schema.getName();
                assert(arrayName!=null);
                System.out.print("Result schema: " + (arrayName.isEmpty() ? "<unnamed>" : arrayName) + " <");

                assert(attributes.length >= 1);
                int numAttrsWithoutBitmap = attributes.length - 1; // normal case, when there is a bitmap.
                if (schema.getEmptyIndicator()==null) {
                    numAttrsWithoutBitmap = attributes.length;
                }
                for (int i = 0; i < numAttrsWithoutBitmap; i++)
                {
                    System.out.print(attributes[i].getName());
                    if (i+1 < numAttrsWithoutBitmap) {
                        System.out.print(", ");
                    }
                }
                System.out.print(">[");

                for (int i = 0; i < dimensions.length; i++)
                {
                    System.out.print(dimensions[i]);
                    if (i+1 < dimensions.length) {
                        System.out.print(", ");
                    }
                }
                System.out.println("]");

                // Note from Donghui:
                // Here I omit plugins.
            }

            long numCells = 0;
            long numChunks = 0;

            //
            // Fetching result array
            //
            if ( "/dev/null".equals(config.getResult()) )
            {
                // Note from Donghui Zhang:
                // This case is not supported.
            }
            else
            {
                save(array, config);
                getState().setFirstSaving(false);
            }

            if (getState().getTimer()) {
                System.out.println("Query execution time: " +
                    result.getElapsedTimeMillis() + "ms");
            }

            if (getState().getVerbose())
            {
                System.out.println("Query execution time: " +
                    result.getElapsedTimeMillis() + "ms");
                System.out.println( "Logical plan: ");
                System.out.println(result.getExplainLogical());
                System.out.println("Physical plans: ");
                System.out.println(result.getExplainPhysical());
            }
        }
        else
        {
            // The query is not selective. Maybe DDL.
            System.out.println("Query was executed successfully");
        }

        // Note from Donghui Zhang:
        // Warnings handling from iquery is omitted here.

        return (!result.getAutoCommit());
    }

    /**
     * A private function supporting executeCommandOrQuery().
     */
    private void executeSciDBQuery(String queryString, Config config)
            throws SQLException, IOException, SciDBException

    {
        getState().getConnection().setAfl(! getState().getAql());
        PrepareResult queryResult = null;

        try {
             queryResult = getState().getConnection().prepare(
                 queryString);
        } catch (Exception e) {
            SciDBException.errorOut(e);
        }

        getState().setCurrentQueryID(queryResult.getQueryId());

        boolean needCommit = true;
        try {
            needCommit = executePreparedSciDBQuery(queryString, config);
        } catch (Exception e) {
            SciDBException.errorOut(e);
        }

        QueryID queryID = getState().getCurrentQueryID();
        getState().setCurrentQueryID(new QueryID());

        if (queryID.isValid() && needCommit &&
            getState().getConnection()!=null)
        {
            getState().getConnection().commit();
        }
    }

    /**
     * Given a query string, execute it.
     */
    public void executeCommandOrQuery(String query, Config config)
    {
        String trimmedQuery = query.trim();
        if (trimmedQuery.isEmpty()
            || (trimmedQuery.length() >= 2
                && trimmedQuery.charAt(0) == '-'
                && trimmedQuery.charAt(1) == '-'
                &&  trimmedQuery.indexOf('\n') == -1))
        {
            return;
        }

        // Note from Donghui Zhang:
        // The parsing capability in iquery is omitted here.
        // I just send the whole query string over to the server.
        try {
            executeSciDBQuery(trimmedQuery, config);
        }
        catch (Exception e)
        {
            SciDBException.errorOut(e);
        } // end catch().
    } // end executeCommandOrQuery().

    /**
     * Save to file all data from an array.
     * @note We only support text format.
     * @param array  the array from which data are to be saved.
     * @param file   the file where data should be saved into.
     * @param format output format: csv, csv+, tsv, tsv+, sparse, auto, etc.
     * @return number of saved tuples.
     */
    long save(
            Array array,
            Config config)
    throws FileNotFoundException, SciDBException
    {
        PrintStream f;
        if (    config.getResult().equals("console") ||
                config.getResult().equals("stdout")) {
            f = System.out;
        } else if (config.getResult().equals("stderr")) {
            f = System.err;
        } else {
            f = new PrintStream(new FileOutputStream(
                config.getResult(), true)); // true means to append.
        }

        // Switch out to "foo-separated values" if we can.
        XsvFormatter.Parameters parameters =
            new XsvFormatter().new Parameters(
                config.getFormat(),
                config.getPrecision());

        // Write to the file.
        long n = 0;
        try {
            n = XsvFormatter.printArray(array, f, parameters);
        } catch (Exception e) {
            SciDBException.errorOut(e);
        } finally {
            if (f==System.out || f==System.err) {
                f.flush();
            }
            else {
                f.close();
            }
        }

        // Return the number of tuples written.
        return n;
    }

} // end class Iquery.
