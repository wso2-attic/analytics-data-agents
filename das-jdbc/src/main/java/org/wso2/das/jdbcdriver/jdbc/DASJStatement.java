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

import org.wso2.das.jdbcdriver.dasInterface.DataReader;
import org.wso2.das.jdbcdriver.common.JSONUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;

import org.wso2.das.jdbcdriver.common.SQLParser;

/**
 * This class implements the java.sql.Statement JDBC interface for the DASJDriver driver.
 * This is used for executing a SQL statement and returning the results it produces.
 */
public class DASJStatement implements Statement {

    private DASJConnection connection;
    protected int resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
    private int maxRows = 0;
    private boolean connectionClosed;
    private int queryTimeout = Integer.MAX_VALUE;
    protected ResultSet prevResultSet = null;
    private int fetchSize = 1;
    private int fetchDirection = ResultSet.FETCH_FORWARD;

    protected DASJStatement(DASJConnection connection, int resultSetType) {
        this.connection = connection;
        this.resultSetType = resultSetType;
    }

    protected void checkStatus() throws SQLException {
        if (connectionClosed) {
            throw new SQLException("[Closed Statement]: Driver.getParentLogger()");
        }
    }

    /**
     * Executes the given SQL statement, which returns a single ResultSet object.
     * @param sql SQL Statement for execution
     * @return  Resultset object which contains the result for the query
     * @throws SQLException
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkStatus();
        if (prevResultSet != null) {
            prevResultSet.close();
        }
        prevResultSet = null;

        //Parse the SQL
        SQLParser sqlParser = new SQLParser();
        try {
            sqlParser.parse(sql);
        } catch (Exception e) {
            throw new SQLException("SyntaxError:executeQuery" + sql + "|" + e.getMessage());
        }

        ResultSet rs = executeDASQuery(sqlParser);
        prevResultSet = rs;
        return rs;
    }

    @Override
    public void close() throws SQLException {
        if (prevResultSet != null) {
            prevResultSet.close();
        }
        prevResultSet = null;
        connectionClosed = true;
        connection.removeStatement(this);
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkStatus();
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkStatus();
        maxRows = max;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkStatus();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkStatus();
        queryTimeout = seconds;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return prevResultSet;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkStatus();
        if (direction == ResultSet.FETCH_FORWARD ||
                direction == ResultSet.FETCH_REVERSE ||
                direction == ResultSet.FETCH_UNKNOWN) {
            this.fetchDirection = direction;
        } else {
            throw new SQLException("Statement-setFetchDirection:unsupportedDirection:" + direction);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkStatus();
        return connection;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connectionClosed;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkStatus();
        return this.fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkStatus();
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkStatus();
        return this.fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkStatus();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkStatus();
        return this.resultSetType;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return 0;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    /**
     * Executes the given SQL statement, which may return multiple results.
     * @param sql SQL Statement for execution
     * @return true if the first result is a ResultSet object
     * @throws SQLException
     */
    @Override
    public boolean execute(String sql) throws SQLException {
        checkStatus();
        if (prevResultSet != null) {
            prevResultSet.close();
        }
        prevResultSet = null;

        //Parse the SQL
        ResultSet rs;
        SQLParser sqlParser = new SQLParser();
        try {
            sqlParser.parse(sql);
            rs = executeDASQuery(sqlParser);
            prevResultSet = rs;
        } catch (Exception e) {
            throw new SQLException("SQLExecution Error:executeQuery" + sql + "|" + e.getMessage());
        }
        return (rs != null);
    }


    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLException("UnsupportedOperation:Statement.setMaxFieldSize(int)");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLException("UnsupportedOperation:Statement.setEscapeProcessing(boolean)");
    }

    @Override
    public void cancel() throws SQLException {
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLException("UnsupportedOperation:Statement.setCursorName(String)");
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLException("MethodNotSupported: Statement.addBatch(String)");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLException("MethodNotSupported: Statement.clearBatch(String)");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLException("MethodNotSupported: Statement.executeBatch(String)");
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLException("MethodNotSupported: Statement.getMoreResults(int)");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLException("MethodNotSupported: Statement.getGeneratedKeys()");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("MethodNotSupported: Statement.executeUpdate(String, int)");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("MethodNotSupported: Statement.executeUpdate(String, int [])");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("MethodNotSupported: Statement.executeUpdate(String, String[])");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("MethodNotSupported: Statement.execute(String,int)");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("MethodNotSupported: Statement.execute(String,int[])");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("MethodNotSupported: Statement.execute(String,String[])");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("MethodNotSupported: Statement.getResultSetHoldability()");
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    /**
     * Send the query to the DAS Backend and generate the result set
     */
    protected ResultSet executeDASQuery(SQLParser sqlParser) throws SQLException {
        HashMap<String, String> mapColumnDataTypes = this.connection.getColumnDataTypes(sqlParser.getTableName());
        String sDataResponse = this.connection.getTableData(sqlParser.getTableName());
        DataReader dataReader = JSONUtil.parseDataArray(sDataResponse, mapColumnDataTypes);
        return createResultSet(dataReader, sqlParser);
    }

    /**
     * Create the result set by using the retrieved data from the DAS backend
     */
    private ResultSet createResultSet(DataReader reader, SQLParser sqlParser) throws SQLException {
        ResultSet rs;
        try {
            rs = new DASJResultSet(this, reader,sqlParser.getTableName(), sqlParser.getQueryEnvironment(), ResultSet.TYPE_FORWARD_ONLY,
                    -1, sqlParser.getWhereExpression());
        } catch (ClassNotFoundException e) {
            throw new SQLException(e.getMessage());
        }
        return rs;
    }
}