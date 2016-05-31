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

import org.wso2.das.jdbcdriver.common.ServiceConstants;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Class which implements the java.sql.ResultSetMetaData class.
 * This is used to get information about the types and properties of the columns in a ResultSet object
 */
public class DASJResultSetMetaData implements ResultSetMetaData {

    private String[] columnNames;
    private String[] columnLabels;
    private String[] columnTypes;
    private int[] columnDisplaySizes;
    private String tableName;
    private static Map<String, Integer> typeNameCodes = new HashMap<String, Integer>();

    static {
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_STRING,Types.VARCHAR);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_BOOLEAN, Types.BOOLEAN);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_BYTE, Types.TINYINT);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_SHORT, Types.SMALLINT);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_INT, Types.INTEGER);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_INTEGER, Types.INTEGER);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_LONG, Types.BIGINT);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_FLOAT, Types.FLOAT);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_DOUBLE, Types.DOUBLE);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_BIGDECIMAL, Types.DECIMAL);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_DATE, Types.DATE);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_TIME, Types.TIME);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_TIMESTAMP, Types.TIMESTAMP);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_BLOB, Types.BLOB);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_CLOB, Types.CLOB);
        typeNameCodes.put(ServiceConstants.DATATYPES.DATATYPE_EXPRESSION, Types.BLOB);
    }

    DASJResultSetMetaData(String tableName, String[] columnNames, String[] columnLabels, String[] columnTypes,
            int[] columnDisplaySizes) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnLabels = columnLabels;
        this.columnTypes = columnTypes;
        this.columnDisplaySizes = columnDisplaySizes;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnTypes.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return ResultSetMetaData.columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return columnDisplaySizes[column - 1];
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return columnLabels[column - 1];
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columnNames[column - 1];
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return tableName;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        String sColTypeName = getColumnTypeName(column);
        return typeNameCodes.get(sColTypeName);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return columnTypes[column - 1];
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return columnTypes[column - 1];
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
