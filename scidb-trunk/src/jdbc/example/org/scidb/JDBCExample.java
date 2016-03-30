/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2015 SciDB, Inc.
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
package org.scidb;

import org.scidb.jdbc.IResultSetWrapper;
import org.scidb.client.AuthenticationFile;
import org.scidb.client.ConfigUser;
import org.scidb.client.ConfigUserException;
import org.scidb.jdbc.Connection;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

class JDBCExample
{
    public static void main(String [] args) throws IOException
    {
        try
        {
            Class.forName("org.scidb.jdbc.Driver");
        }
        catch (ClassNotFoundException e)
        {
            System.out.println("Driver is not in the CLASSPATH -> " + e);
        }

        String iqueryHost = "localhost";
        String iqueryPort = "1239";
        String authFileName = "";

        if (args.length >= 1 && args.length <= 3)
        {
            if (args.length >= 1)
            {
                iqueryHost = args[0];
            }
            if (args.length >= 2)
            {
                iqueryPort = args[1];
            }
            if (args.length >= 3)
            {
                authFileName = args[2];
            }
        }

        try
        {
            Connection conn = new Connection(iqueryHost, Integer.parseInt(iqueryPort));

            ConfigUser configUser =  ConfigUser.getInstance();
            configUser.verifySafeFile(authFileName, false);
            AuthenticationFile authFile = new AuthenticationFile(authFileName);
            conn.getSciDBConnection().startNewClient(authFile.getUserName(), authFile.getUserPassword());

            Statement st = conn.createStatement();
            //create array A<a:string>[x=0:2,3,0, y=0:2,3,0];
            //select * into A from array(A, '[["a","b","c"]["d","e","f"]["123","456","789"]]');
            ResultSet res = st.executeQuery("select * from array(<a:string>[x=0:2,3,0, y=0:2,3,0], '[[\"a\",\"b\",\"c\"][\"d\",\"e\",\"f\"][\"123\",\"456\",\"789\"]]')");
            ResultSetMetaData meta = res.getMetaData();

            System.out.println("Source array name: " + meta.getTableName(0));
            System.out.println(meta.getColumnCount() + " columns:");

            IResultSetWrapper resWrapper = res.unwrap(IResultSetWrapper.class);
            for (int i = 1; i <= meta.getColumnCount(); i++)
            {
                System.out.println(meta.getColumnName(i) + " - " + meta.getColumnTypeName(i) + " - is attribute:" + resWrapper.isColumnAttribute(i));
            }
            System.out.println("=====");

            System.out.println("x y a");
            System.out.println("-----");
            while(!res.isAfterLast())
            {
                System.out.println(res.getLong("x") + " " + res.getLong("y") + " " + res.getString("a"));
                res.next();
            }
        }
        catch (Exception e)
        {
            System.out.println(e);
        }

    	System.exit(0);
    }
}
