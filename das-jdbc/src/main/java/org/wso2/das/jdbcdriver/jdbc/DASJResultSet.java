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

import org.wso2.das.jdbcdriver.dasInterface.DataReader;
import org.wso2.das.jdbcdriver.expressions.*;
import org.wso2.das.jdbcdriver.aggregateFunctions.AggregateFunction;
import org.wso2.das.jdbcdriver.common.ServiceUtil;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * This class implements the java.sql.ResultSet JDBC interface for the DASJDriver driver.
 */

public class DASJResultSet implements ResultSet {

    private DASJStatement statement;

    private DataReader reader;

    private String tableName;

    private List<Object []> queryEnvironment;

    private Map<String, Object> recordEnvironment;

    private ResultSetMetaData resultSetMetaData;

    private int maxRows;

    private int currentRow;

    private int limit;

    private boolean hitTail = false;

    private boolean nextResult = true;

    private int lastIndexRead = -1;

    private boolean connectionClosed;

    private int fetchSize;

    private int fetchDirection;

    private int resultSetType = ResultSet.TYPE_SCROLL_SENSITIVE;

    private Expression whereClause;

    private List<String> filterColumns;

    private List<AggregateFunction> aggregateFunctions;

    private List<Expression> groupByColumns;

    private ArrayList<Map<String, Object>> bufferedRecordEnvironments = null;


    /**
     * Constructor for the CsvResultSet object
     *
     * @param statement Statement that produced this ResultSet
     * @param reader Helper class that performs the actual file reads
     * @param tableName Table referenced by the Statement
     * @param queryEnvironment each query expression in the Statement.
     * @param isDistinct true if Statement contains DISTINCT keyword.
     * @throws ClassNotFoundException in case the typed columns fail.
     * @throws SQLException if executing the SQL statement fails.
     */
    protected DASJResultSet(DASJStatement statement,
                           DataReader reader,
                           String tableName,
                           List<Object []> queryEnvironment,
                           boolean isDistinct,
                           int resultSetType,
                           int sqlLimit,
                           Expression whereClause) throws ClassNotFoundException, SQLException {
        this.statement = statement;
        this.reader = reader;
        this.tableName = tableName;
        this.queryEnvironment = new ArrayList<Object[]>(queryEnvironment);
        this.aggregateFunctions = new ArrayList<AggregateFunction>();
        this.limit = sqlLimit;
        this.resultSetType = resultSetType;
        this.whereClause = whereClause;

        String[] columnNames = reader.getColumnNames();
        HashSet<String> allColumns = new HashSet<String>(Arrays.asList(columnNames));
        maxRows = statement.getMaxRows();
        fetchSize = statement.getFetchSize();
        fetchDirection = statement.getFetchDirection();

        if (whereClause!= null)
            this.filterColumns = new LinkedList<String>(whereClause.getFilteredColumns(allColumns));
        else
            this.filterColumns = new LinkedList<String>();

        //Replace any "select *" with the list of column names in that table.
        for (int i = 0; i < this.queryEnvironment.size(); i++)
        {
            Object[] o = this.queryEnvironment.get(i);
            Expression expr = (Expression)o[1];
            if (expr instanceof AsteriskExpression)
            {
                AsteriskExpression asteriskExpression = (AsteriskExpression)o[1];

				String asterisk = asteriskExpression.toString();
                if (!(asterisk.equals("*")))
                    throw new SQLException("[Invalid Column Name]: "+asterisk);
                this.queryEnvironment.remove(i);
                for (int j = 0; j < columnNames.length; j++)
                {
                    this.queryEnvironment.add(i + j, new Object[]{columnNames[j], new ColumnName(columnNames[j])});
                }
            }
            else{
                if(expr instanceof AggregateFunction){
                    this.aggregateFunctions.add((AggregateFunction)expr);
                }
            }
        }

        //Validate query - Check whether there is column names with the aggregate functions
        if (aggregateFunctions.size() > 0 && this.groupByColumns == null)
        {
			List<String> allColumnnsinQuery = new LinkedList<String>();
            for (int i = 0; i < this.queryEnvironment.size(); i++)
            {
                Object[] o = this.queryEnvironment.get(i);
                if (o[1] != null)
                {
                    allColumnnsinQuery.addAll(((Expression)o[1]).getFilteredColumns(allColumns));
                }
            }
            if (allColumnnsinQuery.size() > 0 && aggregateFunctions.size() > 0)
                throw new SQLException("INVALID QUERY: Columns with Aggregate functions");
        }

        if (this.aggregateFunctions.size() > 0){

            bufferedRecordEnvironments = new ArrayList<Map<String, Object>>();
            currentRow = 0;

            while (next())
            {
                for (Object o : this.aggregateFunctions)
                {
                    AggregateFunction func = (AggregateFunction)o;
                    func.processRow(recordEnvironment);
                }
            }


            bufferedRecordEnvironments.add(new HashMap<String, Object>());
            currentRow = 0;
        }
    }
        @Override
    public boolean next() throws SQLException {
            boolean hasNext;

            if(this.aggregateFunctions.size() > 0  && currentRow < bufferedRecordEnvironments.size()){
                currentRow++;
                recordEnvironment = bufferedRecordEnvironments.get(currentRow - 1);
                updateRecordEnvironment(true);
                hasNext = true;
            }
            else
            {
                if (maxRows != 0 && currentRow >= maxRows) {//reached the row limit.
                    hasNext = false;
                } else if (limit >= 0 && currentRow >= limit) {
                    hasNext = false;
                } else if (hitTail) {
                    hasNext = false;
                } else {
                    hasNext = reader.next();
                }

                if (hasNext) {
                    recordEnvironment = reader.getEnvironment();
                } else {
                    recordEnvironment = null;
                }


                //Give priority to where clause
                if (whereClause != null) {
                    Map<String, Object> objectEnvironment = updateRecordEnvironment(hasNext);
                    while (hasNext) {
                        if (whereClause == null || Boolean.TRUE.equals(whereClause.isTrue(objectEnvironment))) {
                            break;
                        }
                        hasNext = reader.next();
                        if (hasNext) {
                            recordEnvironment = reader.getEnvironment();
                        } else {
                            recordEnvironment = null;
                        }
                        objectEnvironment = updateRecordEnvironment(hasNext);
                    }
                }

                if (hasNext)
                    currentRow++;
                else
                    hitTail = true;
            }


            nextResult = hasNext;
            return hasNext;
    }

    private Map<String, Object> updateRecordEnvironment(boolean hasNext) throws SQLException
    {
        Map<String, Object> objectEnvironment = new HashMap<String, Object>();
        if(!hasNext)
        {
            recordEnvironment = null;
            return objectEnvironment;
        }

		for (int i = 0; i < queryEnvironment.size(); i++)
        {
            Object[] o = queryEnvironment.get(i);
            String key = (String) o[0];
            Object value = ((Expression) o[1]).eval(recordEnvironment);
            objectEnvironment.put(key.toUpperCase(), value);
        }
        for (int i=0; i<filterColumns.size(); i++)
        {
            String key = filterColumns.get(i);
            key = key.toUpperCase();
            if (recordEnvironment.containsKey(key))
                objectEnvironment.put(key, recordEnvironment.get(key));
        }

        return objectEnvironment;
    }

    @Override
    public void close() throws SQLException {
        reader.close();
        connectionClosed = true;
        recordEnvironment = null;
        bufferedRecordEnvironments = null;
    }

    @Override
    public boolean wasNull() throws SQLException {
        if(lastIndexRead >= 0)
        {
            return getString(lastIndexRead) == null;
        }
        else
        {
            throw new SQLException("[Execption in was null]");
        }
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object o = getObject(columnIndex);
        if (o != null) {
            return o.toString();
        }
        return null;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        boolean ret = false;
        if (s != null)
        {
            if (s.equals("1"))
                ret = true;
            else if (s.equals("0"))
                ret = false;
            else
                ret = Boolean.valueOf(s);
        }
        return ret;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s != null && s.length() != 0)
        {
            try {
                Byte b = Byte.parseByte(s);
                if (b != null)
                    return b.byteValue();
            }
            catch(Exception e){
                return 0;
            }
        }
        return 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s != null && s.length() != 0)
        {
            try {
                Short sh = Short.parseShort(s);
                if (sh != null)
                    return sh.shortValue();
            }
            catch(Exception e){
                return 0;
            }
        }
        return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s != null && s.length() != 0)
        {
            try {
                Integer i = Integer.parseInt(s);
                if (i != null)
                    return i.intValue();
            }
            catch(Exception e){
                return 0;
            }
        }
        return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s != null && s.length() != 0)
        {
            try {
                Long l = Long.parseLong(s);
                if (l != null)
                    return l.longValue();
            }
            catch(Exception e){
                return 0;
            }
        }
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s != null && s.length() != 0)
        {
            try {
                Float f = Float.parseFloat(s);
                if (f != null)
                    return f.floatValue();
            }
            catch(Exception e){
                return 0;
            }
        }
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s != null && s.length() != 0)
        {
            try {
                Double d = Double.parseDouble(s);
                if (d != null)
                    return d.doubleValue();
            }
            catch(Exception e){
                return 0;
            }
        }
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s != null)
            return ServiceUtil.parseBytes(s);
        return null;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return (Date) getObject(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return (Time) getObject(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object o = getObject(columnIndex);
        if (o instanceof Date)
            o = new Timestamp(((Date)o).getTime());
        return (Timestamp) o;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s != null)
            return ServiceUtil.getAsciiStream(s);
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getAsciiStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getAsciiStream(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLException("MethodNotSupported: ResultSet.getWarnings");
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLException("MethodNotSupported: ResultSet.getCursorName");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if(resultSetMetaData == null){

            String[] readerTypeNames = reader.getColumnTypes();
            String[] readerColumnNames = reader.getColumnNames();
            int[] readerColumnSizes = reader.getColumnSizes();
            String tableAlias = reader.getTableAlias();
            int columnCount = queryEnvironment.size();
            String []columnNames = new String[columnCount];
            String []columnLabels = new String[columnCount];
            int []columnSizes = new int[columnCount];
            String []typeNames = new String[columnCount];

            /*
			 * Create a record containing dummy values.
			 */
            HashSet<String> allReaderColumns = new HashSet<String>();
            HashMap<String, Object> env = new HashMap<String, Object>();
            for(int i=0; i<readerTypeNames.length; i++)
            {
                Object literal = ServiceUtil.getLiteral(readerTypeNames[i]);
                String columnName = readerColumnNames[i].toUpperCase();
                env.put(columnName, literal);
                allReaderColumns.add(columnName);
                if (tableName != null)
                {
                    env.put(tableName.toUpperCase() + "." + columnName, literal);
                    allReaderColumns.add(tableName.toUpperCase() + "." + columnName);
                }
                if (tableAlias != null)
                {
                    env.put(tableAlias + "." + columnName, literal);
                    allReaderColumns.add(tableAlias + "." + columnName);
                }
            }


            for(int i=0; i<columnCount; i++)
            {
                Object[] o = queryEnvironment.get(i);
                columnNames[i] = (String)o[0];
                columnLabels[i] = columnNames[i];

				/*
				 * Evaluate each expression to check the data type
				 */
                Object result = null;
                try
                {
                    Expression expr = ((Expression)o[1]);

                    int columnSize = DataReader.DEFAULT_COLUMN_SIZE;
                    if (expr instanceof ColumnName)
                    {
                        String usedColumn = expr.getFilteredColumns(allReaderColumns).get(0);
                        for (int k = 0; k < readerColumnNames.length; k++)
                        {
                            if (usedColumn.equalsIgnoreCase(readerColumnNames[k]))
                            {
                                columnSize = readerColumnSizes[k];
                                break;
                            }
                        }
                    }
                    columnSizes[i] = columnSize;
                    result = expr.eval(env);
                }
                catch (NullPointerException e)
                {
                    e.printStackTrace();
                }
                catch(Exception e){
                    e.printStackTrace();
                }
                if (result != null)
                    typeNames[i] = ServiceUtil.getSQLType(result);
                else
                    typeNames[i] = "expression";
            }

            resultSetMetaData = new DASJResultSetMetaData(tableName, columnNames, columnLabels, typeNames,columnSizes);
        }
        return resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {

        lastIndexRead = columnIndex;

        checkStatus();

        if (columnIndex < 1 || columnIndex > this.queryEnvironment.size())
        {
            throw new SQLException("[InvalidColumnIndex] DASJResultSet:getObject:"+columnIndex);
        }
        if (this.currentRow == 0)
        {
            throw new SQLException("[CurserNotSet] DASJResultSet:getObject");
        }


        Object[] o = queryEnvironment.get(columnIndex-1);
        if (recordEnvironment != null)
            return ((Expression) o[1]).eval(recordEnvironment);
        return null;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkStatus();

        if (columnLabel.equals(""))
            throw new SQLException("[InvalidColumnName:findColumn() " + columnLabel);
        for (int i = 0; i < this.queryEnvironment.size(); i++)
        {
            Object[] queryEnvEntry = this.queryEnvironment.get(i);
            if(((String)queryEnvEntry[0]).equalsIgnoreCase(columnLabel))
                return i+1;
        }
        throw new SQLException("[InvalidColumnName:findColumn()" + columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        if (str != null)
            return new StringReader(str);
        else
            return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        String str = getString(columnLabel);
        if (str != null)
            return new StringReader(str);
        else
            return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        BigDecimal val = null;
        String str = getString(columnIndex);
        if(str != null)
        {
            try
            {
                val = new BigDecimal(str);
            }
            catch (NumberFormatException e)
            {
                throw new SQLException("[BigDecimalConversionError]: " + str);
            }
        }
        return val;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkStatus();
        if(isScrollable()){
            return  currentRow ==0;
        }
        else{
            throw new SQLException("wrongResultSetType: ResultSet.isBeforeFirst()");
        }

    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkStatus();
        if (isScrollable())
        {
            return currentRow == bufferedRecordEnvironments.size() + 1;
        }
        else
        {
			return (this.nextResult == false && this.currentRow > 0);
        }
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkStatus();

        if (isScrollable())
        {
            return currentRow == 1;
        }
        else
        {
            throw new SQLException("wrongResultSetType: ResultSet.isFirst()");
        }
    }

    @Override
    public boolean isLast() throws SQLException {
        return false; //TODO:implement
    }

    @Override
    public void beforeFirst() throws SQLException {
        //TODO:implement
    }

    @Override
    public void afterLast() throws SQLException {
        checkStatus();

        if (isScrollable())
        {
            while(next());
        }
        else
        {
            throw new SQLException("wrongResultSetType: ResultSet.afterLast()");
        }
    }

    @Override
    public boolean first() throws SQLException {
        return false; //TODO:implement
    }

    @Override
    public boolean last() throws SQLException {
        return false; //TODO:implement
    }

    @Override
    public int getRow() throws SQLException {
        checkStatus();

        if (isScrollable() == false && nextResult == false)
            return 0;
        else
            return currentRow;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return false; //TODO:implement
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return false; //TODO:implement
    }

    @Override
    public boolean previous() throws SQLException {
        return false; //TODO:implement
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction == ResultSet.FETCH_FORWARD ||
                direction == ResultSet.FETCH_REVERSE ||
                direction == ResultSet.FETCH_UNKNOWN)
        {
            this.fetchDirection = direction;
        }
        else
        {
            throw new SQLException("ResultSet-setFetchDirection:unsupportedDirection:" + direction);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getType() throws SQLException {
        return resultSetType;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {

    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {

    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {

    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {

    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {

    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {

    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {

    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {

    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {

    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {

    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {

    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {

    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {

    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {

    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {

    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {

    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {

    }

    @Override
    public void insertRow() throws SQLException {

    }

    @Override
    public void updateRow() throws SQLException {

    }

    @Override
    public void deleteRow() throws SQLException {

    }

    @Override
    public void refreshRow() throws SQLException {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {

    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connectionClosed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    protected void checkStatus() throws SQLException
    {
        if (connectionClosed)
            throw new SQLException("[Closed Statement]: Driver.getParentLogger()");
    }

    private boolean isScrollable()
    {
        return (this.resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE ||
                this.resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE);
    }
}
