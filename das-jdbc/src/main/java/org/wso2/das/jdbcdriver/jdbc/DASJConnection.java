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
package org.wso2.das.jdbcdriver.jdbc;

import org.wso2.das.jdbcdriver.common.ServiceUtil;
import org.wso2.das.jdbcdriver.dasInterface.DASServiceConnector;
import org.wso2.das.jdbcdriver.common.JSONUtil;
import org.wso2.das.jdbcdriver.common.ServiceConstants;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class implements the java.sql.Connection JDBC interface for the DASJDriver driver.
 * This represents a connection (session) with a DAS Data Service.
 * SQL statements are executed and results are returned within the context of a connection.
 */
public class DASJConnection implements Connection {

    private String connURL;

    private String connURLForDASTables;

    private String userName;

    private String userPassword;

    private boolean connectionClosed;

    private boolean autoCommit;

    private Vector<Statement> dasStatements = new Vector<Statement>();

    private static Logger logger = Logger.getLogger(DASJConnection.class.getName());

    /**
     * Creates a new DAS Service Connection that takes the supplied path and properties.
     */
    protected DASJConnection(String url, Properties info) throws SQLException {
        if (url == null || url.length() == 0) {
            throw new IllegalArgumentException("[No Path]: DASJConnection.DASJConnection()");
        }
        this.connURL = url;
        this.connURLForDASTables = this.connURL + ServiceConstants.DAS_SERVICE_QUERIES.DAS_TABLE_NAMES_QUERY;
        System.out.println("con method");
        if (info != null) {
            setProperties(info);
        }

    }

    /**
     * Checks the Connection Status throws SQL Exception if connection is closed.
     *
     * @throws SQLException
     */
    protected void checkStatus() throws SQLException {
        if (this.connectionClosed) {
            throw new SQLException("[Closed Statement]: Driver.getParentLogger()");
        }
        System.out.println("checkstatus method");
    }

    /**
     *  Creates a Statement object for sending SQL statements to the database.
     *  Default type is TYPE_FORWARD_ONLY and default concurrency level is CONCUR_READ_ONLY.
     *
     *  @return a new default Statement object
     *  @throws SQLException
     */
    @Override
    public Statement createStatement() throws SQLException {
        checkStatus();
        DASJStatement statement = new DASJStatement(this, ResultSet.TYPE_FORWARD_ONLY);
        this.dasStatements.add(statement);
        System.out.println("createStatement method");
        return statement;
    }

    /**
     * Creates a PreparedStatement object for sending parameterized SQL statements to the database.
     * Default type is TYPE_FORWARD_ONLY and default concurrency level isCONCUR_READ_ONLY.
     *
     * @param sql  an SQL statement that may contain one or more '?' IN parameter placeholders
     * @return a new default PreparedStatement object
     * @throws SQLException
     */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkStatus();
        PreparedStatement preparedStatement = new DASJPreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY);
        this.dasStatements.add(preparedStatement);
        System.out.println("preparedSt method");
        return preparedStatement;
    }

    /*
     * Creates a Statement object for sending SQL statements to the database.
     * In the current implementation only CONCUR_READ_ONLY is supported for resultSetConcurrency.
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkStatus();
        DASJStatement stmt = new DASJStatement(this, resultSetType);
        this.dasStatements.add(stmt);
        return stmt;
    }

    /*
     * Creates a PreparedStatement object for sending SQL statements to the database.
     * In the current implementation only CONCUR_READ_ONLY is supported for resultSetConcurrency.
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        checkStatus();
        PreparedStatement preparedStatement = new DASJPreparedStatement(this, sql, resultSetType);
        this.dasStatements.add(preparedStatement);
        return preparedStatement;
    }

    /*
     * Creates a Statement object for sending SQL statements to the database.
     * In the current implementation only CONCUR_READ_ONLY is supported for resultSetConcurrency and
     * HOLD_CURSORS_OVER_COMMIT is allowd for resultSetHoldability.
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkStatus();
        DASJStatement statement = new DASJStatement(this, resultSetType);
        this.dasStatements.add(statement);
        return statement;
    }

    /*
     * Creates a Statement object for sending SQL statements to the database.
     * In the current implementation only CONCUR_READ_ONLY is supported for resultSetConcurrency and
     * HOLD_CURSORS_OVER_COMMIT is allowd for resultSetHoldability.
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        checkStatus();
        PreparedStatement preparedStatement = new DASJPreparedStatement(this, sql, resultSetType);
        this.dasStatements.add(preparedStatement);
        return preparedStatement;
    }

    /**
     * Sets this connection's auto-commit mode to the given state.
     *
     * @param autoCommit true to enable auto-commit mode; false to disable it
     * @throws SQLException
     */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkStatus();
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkStatus();
        return this.autoCommit;
    }

    @Override
    public void close() throws SQLException {
        closeDASStatements();
        this.connectionClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.connectionClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkStatus();
        return new DASJDatabaseMetaData(this);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkStatus();
        return true;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkStatus();
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException(
                    "Connection.SetHoldability-HoldabilityNotSupported: " + holdability);
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkStatus();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return !this.connectionClosed;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkStatus();
        return Connection.TRANSACTION_NONE;
    }

    /**
     * Close all statements of this connection.
     * @throws SQLException
     */
    private synchronized void closeDASStatements() throws SQLException {
        while (this.dasStatements.size() > 0) {
            this.dasStatements.firstElement().close();
        }
        this.dasStatements.clear();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException("UnsupportedOperation:Connection.prepareCall(String)");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException("UnsupportedOperation:Connection.nativeSQL(String)");
    }

    @Override
    public void commit() throws SQLException {
    }

    @Override
    public void rollback() throws SQLException {
    }


    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException("UnsupportedOperation:Connection.setTransactionIsolation(int)");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw new UnsupportedOperationException("UnsupportedOperation:Connection.prepareCall()");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("UnsupportedOperation:Connection.getTypeMap()");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("UnsupportedOperation:Connection.setTypeMap()");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("MethodNotSupported: Connection.setSavepoint()");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("MethodNotSupported: Connection.setSavepoint(String)");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "MethodNotSupported: Connection.prepareCall(String,int,int,int)");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("MethodNotSupported: Connection.prepareStatement(String,int)");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("MethodNotSupported: Connection.prepareStatement(String,int[])");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException(
                "MethodNotSupported: Connection.prepareStatement(String,String[])");
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
    }

    @Override
    public String getSchema() throws SQLException {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public String getUserName() {
        return this.userName;
    }

    /*
     * Set the Connection properties.
     */
    private void setProperties(Properties info) throws SQLException {
        // set the file extension to be used
        if (info.getProperty(ServiceConstants.DAS_DRIVER_SETTINGS.DAS_USER) != null) {
            this.userName = info.getProperty(ServiceConstants.DAS_DRIVER_SETTINGS.DAS_USER);
        }
        // set the file extension to be used
        if (info.getProperty(ServiceConstants.DAS_DRIVER_SETTINGS.DAS_PASS) != null) {
            this.userPassword = info.getProperty(ServiceConstants.DAS_DRIVER_SETTINGS.DAS_PASS);
        }
    }

    /**
     * Get list of table names from the DAS backend.
     *
     * @return list of table names.
     * @throws SQLException if getting list of table names fails.
     */
    public List<String> getTableNames() throws SQLException {
        List<String> tableNames = null;
        try {
            String sResponse = DASServiceConnector.sendGet(this.connURLForDASTables, this.userName, this.userPassword);
            tableNames = JSONUtil.parseSimpleArray(sResponse);
        } catch (Exception e) {
            throw new SQLException("Error in Get Table Names:",e);
        }
        return tableNames;
    }

    /**
     * Get Data types of the given table from the DAS backend.
     *
     * @param tableName Table name in which data types are requested
     * @return Map of column name -data type
     * @throws SQLException
     */
    public HashMap<String, String> getColumnDataTypes(String tableName) throws SQLException {
        HashMap<String, String> mapColumnDataTypes = null;
        tableName = ServiceUtil.extractTableName(tableName);
        String sRequestURL = getConnURLForTableSchema(tableName);
        try {
            String sResponse = DASServiceConnector.sendGet(sRequestURL, this.userName, this.userPassword);
            mapColumnDataTypes = JSONUtil.parseSubArray(sResponse, ServiceConstants.DAS_RESPONSE_KEYS.COLUMNS,
                    ServiceConstants.DAS_RESPONSE_KEYS.TYPE);
        } catch (Exception e) {
            throw new SQLException("Error in Get Column Data Types :",e);
        }
        return mapColumnDataTypes;
    }

    /**
     * Get a list of primary keys of the given table.
     *
     * @param tableName Name of the table where primary keys are required
     */
    public List<String> getPrimaryKeys(String tableName) {
        List<String> result = new LinkedList<String>();
        tableName = ServiceUtil.extractTableName(tableName);
        String sRequestURL = getConnURLForTableSchema(tableName);
        try {
            String sResponse = DASServiceConnector.sendGet(sRequestURL, this.userName, this.userPassword);
            result = JSONUtil.parseSubArray(sResponse, ServiceConstants.DAS_RESPONSE_KEYS.PRIMARYKEYS);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Get Primary Keys:", e);
        }
        return result;
    }

    /**
     * Get List of Indexes of a given table.
     *
     * @param tableName Table in which index details are required
     */
    public List<String> getIndexes(String tableName) {

        List<String> result = new LinkedList<String>();
        tableName = ServiceUtil.extractTableName(tableName);
        String sRequestURL = getConnURLForTableSchema(tableName);
        try {
            String sResponse = DASServiceConnector.sendGet(sRequestURL, this.userName, this.userPassword);
            result = JSONUtil.parseSubArrayWithCheckValue(sResponse, ServiceConstants.DAS_RESPONSE_KEYS.COLUMNS,
                    ServiceConstants.DAS_RESPONSE_KEYS.ISINDEX);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Get Indexes:", e);
        }
        return result;
    }

    /**
     * Get the data of the given table.
     *
     * @param tableName Table name in which data is required.
     * @return String JSON Response from DAS backend which contains the table data
     */
    public String getTableData(String tableName) {
        tableName = ServiceUtil.extractTableName(tableName);
        String sRequestURL = getConnURLForTable(tableName);
        String sResponse = null;
        try {
            sResponse = DASServiceConnector.sendGet(sRequestURL, this.userName, this.userPassword);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Get Table Data:", e);
        }
        return sResponse;
    }

    /**
     * Returns the conneciton url for this Connection.
     */
    public String getURL() {
        String url = "";
        if (this.connURL != null) {
            url = ServiceConstants.DAS_DRIVER_SETTINGS.URL_PREFIX + this.connURL;
        }
        return url;
    }

    /**
     * Removes the Statements of the current connection , when statements are closing.
     *
     * @param statement Statment to remvoe
     */
    public void removeStatement(Statement statement) {
        this.dasStatements.remove(statement);
    }

    /*
     * Create the DAS Backend url for given table.
     */
    private String getConnURLForTable(String tableName){
        return this.connURLForDASTables + ServiceConstants.DAS_SERVICE_QUERIES.URL_PATH_SEPERATOR + tableName;
    }

    /*
     * Create the DAS Backend url for given table for schema data.
     */
    private String getConnURLForTableSchema(String tableName){
        return this.connURLForDASTables + ServiceConstants.DAS_SERVICE_QUERIES.URL_PATH_SEPERATOR + tableName
                + ServiceConstants.DAS_SERVICE_QUERIES.DAS_SCHEMA_QUERY;
    }
}
