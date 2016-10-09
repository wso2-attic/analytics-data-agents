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

import org.wso2.das.jdbcdriver.common.SQLParser;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * The class that represents the Pre-compiled SQL statement.
 */
public class DASJPreparedStatement extends DASJStatement implements PreparedStatement {

    private Object[] queryParameters;
    private String sqlQuery;

    public DASJPreparedStatement(DASJConnection connection, String sql, int resultSetType) throws SQLException {
        super(connection, resultSetType);
        int iPlaceHolderCount = sql.length() - sql.replace("?", "").length();
        this.queryParameters = new Object[iPlaceHolderCount + 1];
        this.sqlQuery = sql;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkStatus();
        if (prevResultSet != null) {
            prevResultSet.close();
        }
        prevResultSet = null;
        for (int i = 1; i < this.queryParameters.length; i++) {
            Object parameter = this.queryParameters[i];
            if (parameter instanceof String) {
                this.sqlQuery = this.sqlQuery.replaceFirst("\\?", "\'" + parameter + "\'");
            } else {
                this.sqlQuery = this.sqlQuery.replaceFirst("\\?", parameter.toString());
            }
        }
        SQLParser parser = new SQLParser(this.sqlQuery);
        try {
            parser.parse();
        } catch (Exception e) {
            throw new SQLException("Syntax Error: " + e.getMessage());
        }
        ResultSet rs = executeDASQuery(parser);
        prevResultSet = rs;
        return rs;
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLException("MethodNotSupported: executeUpdate()");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLException("MethodNotSupported: setNull(int,int)");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void clearParameters() throws SQLException {
        for (int i = 1; i < this.queryParameters.length; i++) {
            this.queryParameters[i] = null;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public boolean execute() throws SQLException {
        return false;
    }

    @Override
    public void addBatch() throws SQLException {
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = value;
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = xmlObject;
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = x;
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = reader;
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = value;
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = reader;
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = inputStream;
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        checkStatus();
        checkParameterIndex(parameterIndex);
        this.queryParameters[parameterIndex] = reader;
    }

    private void checkParameterIndex(int parameterIndex) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > this.queryParameters.length) {
            throw new SQLException("Invalid ParameterIndex " + parameterIndex);
        }
    }
}
