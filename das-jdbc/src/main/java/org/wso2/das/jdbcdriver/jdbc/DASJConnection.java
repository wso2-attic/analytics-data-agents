/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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

import org.wso2.das.jdbcdriver.dasInterface.DASServiceConnector;
import org.wso2.das.jdbcdriver.common.JSONUtil;
import org.wso2.das.jdbcdriver.common.ServiceConstants;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * This class implements the java.sql.Connection JDBC interface for the DASJDriver driver.
 */
public class DASJConnection implements Connection {

    private String connURL;

    private String userName;

    private String userPassword;

    private boolean connectionClosed;

    private boolean autoCommit;

    private Vector<Statement> dasStatements = new Vector<Statement>();

    protected DASJConnection(String url, Properties info)
            throws SQLException
    {
        if (url == null || url.length() == 0)
        {
            throw new IllegalArgumentException("[No Path]: DASJConnection.DASJConnection()");
        }
        this.connURL = url;

        if (info != null)
        {
            setProperties(info);
        }
    }

    protected void checkStatus() throws SQLException
    {
        if (connectionClosed)
            throw new SQLException("[Closed Statement]: Driver.getParentLogger()");
    }

    @Override
    public Statement createStatement() throws SQLException {

        checkStatus();

        DASJStatement statement = new DASJStatement(this,ResultSet.TYPE_FORWARD_ONLY);
        dasStatements.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkStatus();
        PreparedStatement preparedStatement = new DASJPreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY);
        dasStatements.add(preparedStatement);
        return preparedStatement;
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
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {
        closeDASStatements();
        connectionClosed = true;
    }

    /**
     * Close all statements of this connection.Snchronized to make sure closing runs only one time from one thread
     * @throws SQLException
     */
    private synchronized void closeDASStatements() throws SQLException
    {
        while (dasStatements.size() > 0)
        {
            dasStatements.firstElement().close();
        }
        dasStatements.clear();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connectionClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkStatus();
        return new DASJDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkStatus();
        return true;
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
    public int getTransactionIsolation() throws SQLException {
        checkStatus();
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkStatus();

        DASJStatement stmt = new DASJStatement(this,resultSetType);
        dasStatements.add(stmt);
        return stmt;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
       checkStatus();

        PreparedStatement preparedStatement  = new DASJPreparedStatement(this,sql,resultSetType);
        dasStatements.add(preparedStatement);
        return  preparedStatement;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
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
    public void setHoldability(int holdability) throws SQLException {
        checkStatus();

        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
            throw new SQLFeatureNotSupportedException("Connection.SetHoldability-HoldabilityNotSupported: " + holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        checkStatus();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("HoldabilityNotSupported: Connection.setSavepoint()");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("HoldabilityNotSupported: Connection.setSavepoint(String)");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkStatus();

        DASJStatement statement = new DASJStatement(this, resultSetType);
        dasStatements.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkStatus();

        PreparedStatement preparedStatement = new DASJPreparedStatement(this, sql, resultSetType);
        dasStatements.add(preparedStatement);
        return preparedStatement;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("HoldabilityNotSupported: Connection.prepareCall(String,int,int,int)");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("HoldabilityNotSupported: Connection.prepareStatement(String,int)");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("HoldabilityNotSupported: Connection.prepareStatement(String,int[])");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("HoldabilityNotSupported: Connection.prepareStatement(String,String[])");
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
    public boolean isValid(int timeout) throws SQLException {
        return !connectionClosed;
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

    private void setProperties(Properties info) throws SQLException
    {
        // set the file extension to be used
        if (info.getProperty(ServiceConstants.DAS_DRIVER_SETTINGS.DAS_USER) != null)
        {
            userName = info.getProperty(ServiceConstants.DAS_DRIVER_SETTINGS.DAS_USER);
        }

        // set the file extension to be used
        if (info.getProperty(ServiceConstants.DAS_DRIVER_SETTINGS.DAS_PASS) != null)
        {
            userPassword = info.getProperty(ServiceConstants.DAS_DRIVER_SETTINGS.DAS_PASS);
        }
    }


    /**
     * Get list of table names from the DAS backend.
     *
     * @return list of table names.
     * @throws SQLException if getting list of table names fails.
     */
    public List<String> getTableNames() throws SQLException
    {
        List<String> tableNames = null;
        String sRequestURL = connURL+ServiceConstants.DAS_SERVICE_QUERIES.DAS_TABLE_NAMES_QUERY;
        try {
            String sResponse = DASServiceConnector.sendGet(sRequestURL, userName, userPassword);
            tableNames = JSONUtil.parseSimpleArray(sResponse);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return tableNames;
    }

    /**
     * Get Data types of the given table from the DAS backend
     * @param tableName Table name in which data types are requested
     * @return Map of column name -data type
     * @throws SQLException
     */
    public HashMap<String ,String> getColumnDataTypes(String tableName) throws SQLException{
        HashMap<String,String> mapColumnDataTypes = null;

        if(tableName.startsWith("'") || tableName.startsWith("\"")) //Remove the inverted commas of the table name if exists
            tableName = tableName.substring(1);
        if(tableName.endsWith("'") || tableName.endsWith("\""))
            tableName = tableName.substring(0,tableName.length()-1);


        String sRequestURL = connURL+ServiceConstants.DAS_SERVICE_QUERIES.DAS_TABLE_NAMES_QUERY+
                ServiceConstants.DAS_SERVICE_QUERIES.URL_PATH_SEPERATOR+tableName+ServiceConstants.DAS_SERVICE_QUERIES.DAS_SCHEMA_QUERY;

        try {
            String sResponse = DASServiceConnector.sendGet(sRequestURL, userName, userPassword);
            mapColumnDataTypes = JSONUtil.parseSubArray(sResponse,ServiceConstants.DAS_RESPONSE_KEYS.COLUMNS, ServiceConstants.DAS_RESPONSE_KEYS.TYPE);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return mapColumnDataTypes;
    }

    /**
     * Get a list of primary keys of the given table
     * @param tableName Name of the table where primary keys are required
     */
    public List<String> getPrimaryKeys(String tableName){
        List<String> result = new LinkedList<String>();
        if(tableName.startsWith("'") || tableName.startsWith("\"")) //Remove the inverted commas of the table name if exists
            tableName = tableName.substring(1);
        if(tableName.endsWith("'") || tableName.endsWith("\""))
            tableName = tableName.substring(0,tableName.length()-1);

        String sRequestURL = connURL+ServiceConstants.DAS_SERVICE_QUERIES.DAS_TABLE_NAMES_QUERY+
                ServiceConstants.DAS_SERVICE_QUERIES.URL_PATH_SEPERATOR+tableName+ServiceConstants.DAS_SERVICE_QUERIES.DAS_SCHEMA_QUERY;

        try {
            String sResponse = DASServiceConnector.sendGet(sRequestURL, userName, userPassword);
            result = JSONUtil.parseSubArray(sResponse, ServiceConstants.DAS_RESPONSE_KEYS.PRIMARYKEYS);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;

    }

    /**
     * Get List of Indexes of a given table
     * @param tableName Table in which index details are required
     * @return
     */
    public List<String> getIndexes(String tableName){

        List<String> result = new LinkedList<String>();
        if(tableName.startsWith("'") || tableName.startsWith("\""))
            tableName = tableName.substring(1);
        if(tableName.endsWith("'") || tableName.endsWith("\""))
            tableName = tableName.substring(0,tableName.length()-1);

        String sRequestURL = connURL+ServiceConstants.DAS_SERVICE_QUERIES.DAS_TABLE_NAMES_QUERY+
                ServiceConstants.DAS_SERVICE_QUERIES.URL_PATH_SEPERATOR+tableName+ServiceConstants.DAS_SERVICE_QUERIES.DAS_SCHEMA_QUERY;

        try {
            String sResponse = DASServiceConnector.sendGet(sRequestURL, userName, userPassword);
            result = JSONUtil.parseSubArrayWithCheckValue(sResponse,ServiceConstants.DAS_RESPONSE_KEYS.COLUMNS, ServiceConstants.DAS_RESPONSE_KEYS.ISINDEX);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;

    }

    /**Get the data of the table
     * @param tableName Table name in which data is required.
     * @return JSON Response from DAS backend which contains the table data
     */
    public String getTableData(String tableName){

        if(tableName.startsWith("'") || tableName.startsWith("\""))
            tableName = tableName.substring(1);
        if(tableName.endsWith("'") || tableName.endsWith("\""))
            tableName = tableName.substring(0,tableName.length()-1);


        String sRequestURL = connURL+ServiceConstants.DAS_SERVICE_QUERIES.DAS_TABLE_NAMES_QUERY+
                ServiceConstants.DAS_SERVICE_QUERIES.URL_PATH_SEPERATOR+tableName;
        String sResponse = null;

        try {
            sResponse = DASServiceConnector.sendGet(sRequestURL, userName, userPassword);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sResponse;
    }

    public String getURL()
    {
        String url = "";
        if (connURL != null)
        {
            url = ServiceConstants.DAS_DRIVER_SETTINGS.URL_PREFIX + connURL;
        }

        return url;
    }

    public void removeStatement(Statement statement)
    {
        dasStatements.remove(statement);
    }
}
