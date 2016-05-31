/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.das.jdbc;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.das.jdbcdriver.jdbc.DASJConnection;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.fail;

public class DASJDriverTest {

    private static String url;
    static boolean bSetupCompleted = false;
    static boolean bStatusChecked = false;

    @BeforeClass
    public static void setUp() {
        url = "https://localhost:9443/analytics/";
        try {
            Class.forName("org.wso2.das.jdbcdriver.jdbc.DASJDriver");
        } catch (ClassNotFoundException e) {
            fail("Driver is not in the CLASSPATH -> " + e);
        }
    }

    @Before
    public void isSampleSetup() {
        if (!bStatusChecked) {
            bSetupCompleted = checkTableName();
            bStatusChecked = true;
        }
        Assume.assumeTrue(bSetupCompleted);
    }

    /**
     * Check whether the DAS is up and Smart home sample is running. All the test cases runs only if this is true.
     */
    private boolean checkTableName() {
        boolean bRet = false;
        try {
            String sTableName = "OVERUSED_DEVICES";
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            DatabaseMetaData dbmd = conn.getMetaData();
            String[] types = { "TABLE" };
            ResultSet rs2 = dbmd.getTables(null, null, "%", types);
            while (rs2.next()) {
                String table = rs2.getString("TABLE_NAME");
                if (table.equalsIgnoreCase(sTableName)) {
                    bRet = true;
                    break;
                }
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("Exception Thrown:checkTableName");
        }
        return bRet;
    }

    @Test
    public void testTableMetaData() {
        try {
            System.out.println("================TABLES=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            DatabaseMetaData dbmd = conn.getMetaData();
            String[] types = { "TABLE" };
            ResultSet rs2 = dbmd.getTables(null, null, "%", types);
            while (rs2.next()) {
                String table = rs2.getString("TABLE_NAME");
                System.out.println("TABLE:" + table);
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testTableMetaData: " + e.getMessage());
        }
    }

    @Test
    public void testColumnMetaData() {
        try {
            System.out.println("================COLUMNS OF OVERUSED_DEVICES=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet results1 = dbmd.getColumns(null, null, "CITY_USAGE", "%");
            while (results1.next()) {
                System.out.print("TABLE:" + results1.getString("TABLE_NAME"));
                System.out.print("|COL:" + results1.getString("COLUMN_NAME"));
                System.out.print("|DATA_TYPE:" + results1.getString("DATA_TYPE"));
                System.out.print("|TYPE_NAME:" + results1.getString("TYPE_NAME"));
                System.out.println("\n");
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testColumnMetaData: " + e.getMessage());
        }
    }

    @Test
    public void testPreparedStatementWithoutParameters() {
        try {
            System.out.println("================PREPARED STATEMENT WITHOUT PARAMETERS TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            String queryString = "select DAS.CITY_USAGE.MAX_USAGE from DAS.CITY_USAGE";
            PreparedStatement stmtpre = conn.prepareStatement(queryString);
            ResultSet rset = stmtpre.executeQuery();
            while (rset.next()) {
                System.out.println("MAX_USAGE:" + rset.getDouble("MAX_USAGE"));
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testPreparedStatementWithoutParameters: " + e.getMessage());
        }
    }

    @Test
    public void testPreparedStatementWithParameters() throws SQLException {
        try {
            System.out.println("================PREPARED STATEMENT WITH PARAMETERS TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");

            String queryString = "SELECT * FROM OVERUSED_DEVICES WHERE HOUSE_ID = ? AND METRO_AREA = ?";

            PreparedStatement prepstmt = conn.prepareStatement(queryString);
            prepstmt.setInt(1, 15);
            prepstmt.setString(2, "San Francisco");
            ResultSet rs = prepstmt.executeQuery();
            while (rs.next()) {
                System.out.println("ROW:" + rs.getString("HOUSE_ID"));
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testPreparedStatementWithParameters: " + e.getMessage());
        }
    }

    @Test
    public void testPrimaryKeys() {
        try {
            System.out.println("================PRIMARY KEY TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            List<String> stets = ((DASJConnection) conn).getPrimaryKeys("OVERUSED_DEVICES");
            System.out.println("PK Count:" + stets.size());
            for (String s : stets) {
                System.out.println("PK:" + s);
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testPrimaryKeys: " + e.getMessage());
        }
    }

    @Test
    public void testIndexKeys() {
        try {
            System.out.println("================INDEX TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            List<String> stets = ((DASJConnection) conn).getIndexes("OVERUSED_DEVICES");

            System.out.println("INDEX Count:" + stets.size());
            for (String s : stets) {
                System.out.println("INDEX:" + s);
            }

            conn.close();
        } catch (Exception e) {
            System.out.println("testIndexKeys: " + e.getMessage());
        }
    }

    @Test
    public void testSelectQuery() {
        try {
            System.out.println("================SELECT QUERY TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            Statement stmt = conn.createStatement();
            //ResultSet rs = stmt.executeQuery("SELECT house_id,state,metro_area,device_id,timestamp FROM
            // ORG_WSO2_DAS_SAMPLE_SMART_HOME_DATA where timestamp > 1455777700195;");
            //ResultSet rs = stmt.executeQuery("SELECT house_id,state,metro_area,device_id,timestamp
            // FROM ORG_WSO2_DAS_SAMPLE_SMART_HOME_DATA where state = 'Florida';");
            //ResultSet rs = stmt.executeQuery("SELECT house_id,state,metro_area,device_id,timestamp
            // FROM ORG_WSO2_DAS_SAMPLE_SMART_HOME_DATA where state = 'Florida';");
            //ResultSet rs = stmt.executeQuery("select tbl.HOUSE_ID,tbl.POWER_READING,tbl.DEVICE_ID,tbl.METRO_AREA,
            // tbl.STATE,tbl.VERSION,tbl.IS_PEAK,tbl.TIMESTAMP from \"ORG_WSO2_DAS_SAMPLE_SMART_HOME_DATA\" tbl;");
            //ResultSet rs = stmt.executeQuery("select * FROM OVERUSED_DEVICES WHERE HOUSE_ID = 15 OR HOUSE_ID=15");
            ResultSet rs = stmt.executeQuery("select * FROM OVERUSED_DEVICES");
            int i = 0;
            while (rs.next()) {
                i   ++;
                System.out.println(
                        "ROW:" + i + ":timestamp:" + rs.getString("timestamp") + ":house_id:" + rs.getString("house_id")
                                + "|state:" + rs.getString("state") + "|metro_area:" + rs.getString("metro_area")
                                + "|device_id:" + rs.getInt("device_id"));
            }
            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            System.out.println("testSelectQuery: " + e.getMessage());
        }
    }

    @Test public void testSelectWithColumnNamesQuery() {
        try {
            System.out.println("================SELECT QUERY TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "select house_id,state,metro_area,device_id,timestamp FROM OVERUSED_DEVICES");
            int i = 0;
            while (rs.next()) {
                i++;
                System.out.println(
                        "ROW:" + i + ":timestamp:" + rs.getString("timestamp") + ":house_id:" + rs.getString("house_id")
                                + "|state:" + rs.getString("state") + "|metro_area:" + rs.getString("metro_area")
                                + "|device_id:" + rs.getInt("device_id"));
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testSelectQuery: " + e.getMessage());
        }
    }

    @Test public void testSelectQueryWithWhere() {
        try {
            System.out.println("================SELECT QUERY WITH AND TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * FROM OVERUSED_DEVICES WHERE METRO_AREA='San Francisco'");
            int i = 0;
            while (rs.next()) {
                i++;
                System.out.println("ROW:"+i+":timestamp:"+rs.getString("timestamp")+":house_id:"+
                        rs.getString("house_id")+"|state:"+rs.getString("state")+"|metro_area:"+
                        rs.getString("metro_area")+"|device_id:"+rs.getInt("device_id"));
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testSelectQueryWithAnd: " + e.getMessage());
        }
    }

    @Test public void testSelectQueryWithAnd() {
        try {
            System.out.println("================SELECT QUERY WITH AND TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "select * FROM OVERUSED_DEVICES WHERE (HOUSE_ID = 15 AND METRO_AREA='San Francisco')");
            int i = 0;
            while (rs.next()) {
                i++;
                System.out.println(
                        "ROW:" + i + ":timestamp:" + rs.getString("timestamp") + ":house_id:" + rs.getString("house_id")
                                + "|state:" + rs.getString("state") + "|metro_area:" + rs.getString("metro_area")
                                + "|device_id:" + rs.getInt("device_id"));
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testSelectQueryWithAnd: " + e.getMessage());
        }
    }

    @Test public void testSelectQueryWithOR() {
        try {
            System.out.println("================SELECT QUERY WITH OR TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt
                    .executeQuery("select * FROM OVERUSED_DEVICES WHERE HOUSE_ID = 15 OR METRO_AREA='San Francisco'");
            int i = 0;
            while (rs.next()) {
                i++;
                System.out.println(
                        "ROW:" + i + ":timestamp:" + rs.getString("timestamp") + ":house_id:" + rs.getString("house_id")
                                + "|state:" + rs.getString("state") + "|metro_area:" + rs.getString("metro_area")
                                + "|device_id:" + rs.getInt("device_id"));
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testSelectQueryWithOR: " + e.getMessage());
        }
    }

    @Test public void testSelectQueryWithNotOperation() {
        try {
            System.out.println("================SELECT QUERY WITH NOTTEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * FROM OVERUSED_DEVICES WHERE HOUSE_ID != 15");
            int i = 0;
            while (rs.next()) {
                i++;
                System.out.println(
                        "ROW:" + i + ":timestamp:" + rs.getString("timestamp") + ":house_id:" + rs.getString("house_id")
                                + "|state:" + rs.getString("state") + "|metro_area:" + rs.getString("metro_area")
                                + "|device_id:" + rs.getInt("device_id"));
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testSelectQueryWithNOT: " + e.getMessage());
        }
    }

    @Test public void testCountAggregateFunction() throws SQLException {
        try {
            System.out.println("================COUNT FUNCTION TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery("select count(*) FROM OVERUSED_DEVICES");
            int i = 0;
            while (rs.next()) {
                i++;
                System.out.println("COUINT:" + rs.getInt(1));
            }

            conn.close();
        } catch (Exception e) {
            System.out.println("testCountAggregateFunction: " + e.getMessage());
        }

    }

    @Test public void testSumAggregateFunction() throws SQLException {
        try {
            System.out.println("================SUM FUNCTION TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery("select SUM(DEVICE_ID) FROM OVERUSED_DEVICES");
            while (rs.next()) {
                System.out.println("SUM:" + rs.getInt(1));
            }

            ResultSet rs2 = stmt.executeQuery("select SUM(HOUSE_ID) FROM OVERUSED_DEVICES WHERE POWER_READING > 995");
            int j = 0;
            while (rs2.next()) {
                j++;
                System.out.println("SUM:" + rs2.getDouble(1));
            }

            conn.close();
        } catch (Exception e) {
            System.out.println("testSumAggregateFunction: " + e.getMessage());
        }
    }

    @Test public void testMinAggregateFunction() {
        try {
            System.out.println("================MIN FUNCTION TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery("select MIN(POWER_READING) FROM OVERUSED_DEVICES");
            while (rs.next()) {
                System.out.println("MIN:" + rs.getDouble(1));
            }

            ResultSet rs2 = stmt
                    .executeQuery("select MIN(POWER_READING) FROM OVERUSED_DEVICES WHERE POWER_READING > 995");
            while (rs2.next()) {
                System.out.println("MIN:" + rs2.getDouble(1));
            }

            ResultSet rs3 = stmt.executeQuery("select MIN(DEVICE_ID) FROM OVERUSED_DEVICES");
            while (rs3.next()) {
                System.out.println("MIN:" + rs3.getInt(1));
            }

            ResultSet rs4 = stmt.executeQuery("select MIN(METRO_AREA) FROM OVERUSED_DEVICES");
            while (rs4.next()) {
                System.out.println("MIN:" + rs4.getString(1));
            }

            conn.close();
        } catch (Exception e) {
            System.out.println("testMinAggregateFunction: " + e.getMessage());
        }
    }

    @Test public void testMaxAggregateFunction() {
        try {
            System.out.println("================MAX FUNCTION TEST=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery("select MAX(POWER_READING) FROM OVERUSED_DEVICES");
            while (rs.next()) {
                System.out.println("MAX:" + rs.getDouble(1));
            }

            ResultSet rs2 = stmt
                    .executeQuery("select MAX(POWER_READING) FROM OVERUSED_DEVICES WHERE POWER_READING < 995");
            while (rs2.next()) {
                System.out.println("MAX:" + rs2.getDouble(1));
            }

            ResultSet rs3 = stmt.executeQuery("select MAX(DEVICE_ID) FROM OVERUSED_DEVICES");
            while (rs3.next()) {
                System.out.println("MAX:" + rs3.getInt(1));
            }

            ResultSet rs4 = stmt.executeQuery("select MAX(METRO_AREA) FROM OVERUSED_DEVICES");
            while (rs4.next()) {
                System.out.println("MAX:" + rs4.getString(1));
            }

            conn.close();
        } catch (Exception e) {
            System.out.println("testMaxAggregateFunction: " + e.getMessage());
        }
    }

    @Test public void testDasApi() {

        //Checking if a table exists
        //https://localhost:9443/analytics/table_exists?table=OVERUSED_DEVICES
        try {
            System.out.println("================Check Table Exists=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "OVERUSED_DEVICES", null);
            if (rs.next()) {
                System.out.println("Table Exists:OVERUSED_DEVICES");
            }
            else {
                System.out.println("Table Not Exists:OVERUSED_DEVICES");
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testDasApi:Table Exists: " + e.getMessage());
        }

        //Getting the record count of a table
        //https://localhost:9443/analytics/tables/OVERUSED_DEVICES/recordcount
        try {
            System.out.println("================Get Record Count=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery("select count(*) FROM OVERUSED_DEVICES");
            int i = 0;
            if (rs.next()) {
                System.out.println("Count:" + rs.getInt(1));
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testDasApi:Record Count: " + e.getMessage());
        }

        //Getting the schema of a table
        //https://localhost:9443/analytics/tables/OVERUSED_DEVICES/schema
        try {
            System.out.println("================Get Column Metadata=============");
            Connection conn = DriverManager.getConnection("jdbc:dasjdriver:" + url, "admin", "admin");
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet results1 = dbmd.getColumns(null, null, "OVERUSED_DEVICES", "%");
            while (results1.next()) {
                System.out.print("TABLE:" + results1.getString("TABLE_NAME"));
                System.out.print("|COL:" + results1.getString("COLUMN_NAME"));
                System.out.print("|DATA_TYPE:" + results1.getString("DATA_TYPE"));
                System.out.print("|TYPE_NAME:" + results1.getString("TYPE_NAME"));
                System.out.println("\n");
            }
            conn.close();
        } catch (Exception e) {
            System.out.println("testColumnMetaData: " + e.getMessage());
        }

    }
}