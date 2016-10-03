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
package org.wso2.das.jdbcdriver.dasInterface;

import org.wso2.das.jdbcdriver.common.ServiceConstants;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class which supports the data reading operations.
 */
public class DataReader {

    private String[] columnNames;
    private String[] columnTypes;
    private List<Object[]> columnValues;
    private int rowIndex;

    public DataReader() {
        this.rowIndex = -1;
    }

    public DataReader(String[] columnNames, String[] columnTypes, List<Object[]> columnValues) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnValues = columnValues;
        this.rowIndex = -1;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public void setColumnTypes(String[] columnTypes) {
        this.columnTypes = columnTypes;
    }

    public void setColumnValues(List<Object[]> columnValues) {
        this.columnValues = columnValues;
    }

    public boolean next() throws SQLException {
        this.rowIndex++;
        return (this.rowIndex < this.columnValues.size());
    }

    public String[] getColumnNames() throws SQLException {
        return this.columnNames;
    }

    public void close() throws SQLException {
    }

    /**
     * Get the data of the current record.
     *
     * @throws SQLException
     */
    public Map<String, Object> getEnvironment() throws SQLException {
        HashMap<String, Object> retval = new HashMap<String, Object>();
        Object[] o = this.columnValues.get(this.rowIndex);
        for (int i = 0; i < this.columnNames.length; i++) {
            retval.put(this.columnNames[i], o[i]);
        }
        return retval;
    }

    public String[] getColumnTypes() throws SQLException {
        return this.columnTypes;
    }

    public int[] getColumnSizes() throws SQLException {
        int[] columnSizes = new int[this.columnTypes.length];
        Arrays.fill(columnSizes, ServiceConstants.DAS_DRIVER_SETTINGS.DEFAULT_COLUMN_SIZE);
        return columnSizes;
    }

    public String getTableAlias() {
        return null;
    }
}
