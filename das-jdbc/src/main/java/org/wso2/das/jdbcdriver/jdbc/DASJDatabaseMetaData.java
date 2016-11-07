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
import org.wso2.das.jdbcdriver.expressions.AsteriskExpression;
import org.wso2.das.jdbcdriver.common.ServiceConstants;
import org.wso2.das.jdbcdriver.common.ServiceUtil;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements the java.sql.DatabaseMetaData interface for the DASJDriver driver.
 * This provides Comprehensive information about the database as a whole.
 */
public class DASJDatabaseMetaData implements DatabaseMetaData {

    private Connection connection;
    private DASJStatement statement;

    public DASJDatabaseMetaData(Connection conn) {
        this.connection = conn;
    }

    @Override
    public String getURL() throws SQLException {
        return ((DASJConnection) this.connection).getURL();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return ServiceConstants.DAS_VERSIONS.DAS_DB_VERSION;
    }

    @Override
    public String getDriverName() throws SQLException {
        return ServiceConstants.DAS_DRIVER_SETTINGS.DAS_DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return ServiceConstants.DAS_VERSIONS.DAS_DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return ServiceConstants.DAS_VERSIONS.DAS_DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return ServiceConstants.DAS_VERSIONS.DAS_DRIVER_MINOR_VERSION;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return ServiceConstants.DAS_DRIVER_SETTINGS.DAS_DRIVER_NAME;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        Object[] data = new Object[] { ServiceConstants.DAS_SERVICE_QUERIES.DAS_SCHEMA_NAME, null };
        List<Object[]> columnValues = new ArrayList<Object[]>();
        columnValues.add(data);
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.SCHEMAS,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.SCHEMAS,  columnValues);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        List<String> listTables = ((DASJConnection) this.connection).getTableNames();
        List<Object[]> columnValues = new ArrayList<Object[]>(listTables.size());
        boolean bMatchType = false;
        if (types == null) {
            bMatchType = true;
        } else {
            for (String type : types) {
                if (type.equals("TABLE")) {
                    bMatchType = true;
                }
            }
        }
        for(String tableName : listTables) {
            if (bMatchType && (tableNamePattern == null || ServiceUtil
                    .isPatternMatched(tableNamePattern, ServiceConstants.DAS_SERVICE_QUERIES.DEFAULT_ESCAPE_STRING,
                            tableName))) {
                Object[] data = new Object[] { ServiceConstants.DAS_SERVICE_QUERIES.DAS_SCHEMA_NAME,
                        ServiceConstants.DAS_SERVICE_QUERIES.DAS_SCHEMA_NAME, tableName, "TABLE", "", null, null, null,
                        null, null };
                columnValues.add(data);
            }
        }
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.TABLES,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.TABLES, columnValues);
    }

    /**
     * Retrieves a description of table columns available in the specified catalog.
     */
    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        if (this.statement == null) {
            this.statement = (DASJStatement) this.connection.createStatement();
        }
        ResultSet resultSetTableList = getTables(catalog, schemaPattern, tableNamePattern, null);
        ResultSet resultSetTableData;
        Integer columnSize = Integer.valueOf(Short.MAX_VALUE);
        Integer decimalDigits = Integer.valueOf(Short.MAX_VALUE);
        Integer radix = 10;
        Integer nullable = columnNullable;
        String tableCat = null;
        Integer buffLength = 0;
        String remarks = null;
        String columnDef = null;
        Integer sqlDataType = 0;
        Integer sqlDateTimeSub = 0;
        String isNullable = "YES";
        String isAutoIncrement = "NO";
        while (resultSetTableList.next()) {
            String tableName = resultSetTableList.getString(3);
            resultSetTableData = this.statement.executeQuery("SELECT * FROM " + tableName + ";");
            ResultSetMetaData metadata = resultSetTableData.getMetaData();
            int columnCount = metadata.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                String columnName = metadata.getColumnName(i + 1);
                if (columnNamePattern == null || ServiceUtil
                        .isPatternMatched(columnNamePattern, ServiceConstants.DAS_SERVICE_QUERIES.DEFAULT_ESCAPE_STRING,
                                columnName)) {
                    int columnType = metadata.getColumnType(i + 1);
                    String columnTypeName = metadata.getColumnTypeName(i + 1);
                    Object data[] = { tableCat, ServiceConstants.DAS_SERVICE_QUERIES.DAS_SCHEMA_NAME, tableName,
                            columnName, columnType, columnTypeName, columnSize, buffLength,
                            decimalDigits, radix, nullable, remarks, columnDef, sqlDataType, sqlDateTimeSub, columnSize,
                            i + 1, isNullable, null, null, null, null, isAutoIncrement };
                    columnValues.add(data);
                }
            }
        }
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.COLUMNS,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.COLUMNS, columnValues);
    }

    /**
     *Retrieves a description of the given table's primary key columns.
     */
    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        List<String> primaryKeys = ((DASJConnection) this.connection).getPrimaryKeys(table);
        int iSeq = 0;
        for (String key : primaryKeys) {
            ++iSeq;
            Object data[] = { catalog, ServiceConstants.DAS_SERVICE_QUERIES.DAS_SCHEMA_NAME, table, key, iSeq, key };
            columnValues.add(data);
        }
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.PRIMARYKEYS,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.PRIMARYKEYS,  columnValues);
    }

    /**
     * Retrieves a description of the given table's indices and statistics.
     */
    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        List<String> indexColumns = ((DASJConnection) this.connection).getIndexes(table);
        boolean bNonUnique = false;
        String indexQualifier = null;
        String sAscOrDesc = null;
        int cardinality = 0;
        int pages = 0;
        String filterCondition = null;
        int iSeq = 0;
        for (String key : indexColumns) {
            ++iSeq;
            Object data[] = { catalog, ServiceConstants.DAS_SERVICE_QUERIES.DAS_SCHEMA_NAME, table, bNonUnique,
                    indexQualifier, key, tableIndexOther, iSeq, key, sAscOrDesc, cardinality, pages,
                    filterCondition };
            columnValues.add(data);
        }
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.INDEXINFO,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.INDEXINFO, columnValues);
    }

    /**
     * Retrieves the table types available in this database.
     */
    @Override
    public ResultSet getTableTypes() throws SQLException {
        Object[] data = new Object[] { "TABLE" };
        List<Object[]> columnValues = new ArrayList<Object[]>();
        columnValues.add(data);
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.TABLETYPES,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.TABLETYPES, columnValues);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.IMPORTEDKEYS,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.IMPORTEDKEYS, columnValues);
    }

    /**
     * Retrieves a description of the foreign key columns that reference the given table's primary key columns.
     */
    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.EXPORTEDKEYS,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.EXPORTEDKEYS, columnValues);
    }

    /**
     * Retrieves a description of the stored procedures available in the given catalog.
     */
    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.PROCEDURES,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.PROCEDURES, columnValues);
    }

    /**
     * Retrieves the catalog names available in this database.
     */
    @Override
    public ResultSet getCatalogs() throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.CATALOGS,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.CATALOGS,columnValues);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
            String columnNamePattern) throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.COLUMNPRIVILEGES,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.COLUMNPRIVILEGES, columnValues);
    }

    /**
     * Retrieves a description of the access rights for each table available in a catalog.
     */
    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.TABLEPRIVILEGES,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.TABLEPRIVILEGES, columnValues);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
            boolean nullable) throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.BESTROWID,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.BESTROWID, columnValues);
    }

    /**
     *Retrieves a description of a table's columns that are automatically updated when any value in a row is updated.
     */
    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.VERSIONCOLUMNS,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.VERSIONCOLUMNS, columnValues);
    }

    /**
     * Retrieves a description of all the data types supported by this database.
     */
    @Override
    public ResultSet getTypeInfo() throws SQLException {
        List<Object[]> columnValues = getColumnTypeInfo();
       return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.TYPEINFO,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.TYPEINFO, columnValues);
    }

    /**
     * Retrieves a description of the user-defined types (UDTs) defined in a particular schema.
     */
    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.UDT,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.UDT, columnValues);
    }

    /**
     * Retrieves a description of the pseudo or hidden columns available in a given table.
     */
    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        List<Object[]> columnValues = new ArrayList<Object[]>();
        return createResultSet(ServiceConstants.DAS_METADATA_DEF_COLUMN_NAMES.PSEUDOCOLUMNS,
                ServiceConstants.DAS_METADATA_DEF_COLUMN_TYPES.PSEUDOCOLUMNS, columnValues);
    }

    /**
     * Retrieves whether this database supports the given result set type.
     */
    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY || type == ResultSet.TYPE_SCROLL_SENSITIVE;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getUserName() throws SQLException {
        return ((DASJConnection) this.connection).getUserName();
    }

    /**
     *Retrieves whether this database supports the given result set holdability.
     */
    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return null;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return null;
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return null;
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return null;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return null;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return null;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        return null;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return  sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
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

    private ResultSet createResultSet(String columnNames, String columnTypes, List<Object[]> columnValues)
            throws SQLException {
        DataReader reader = new DataReader(columnNames.split(","), columnTypes.split(","), columnValues);
        List<Object[]> queryEnvironment = new ArrayList<Object[]>();
        queryEnvironment.add(new Object[] { ServiceConstants.DAS_CONSTANTS.ASTERISK,
                new AsteriskExpression(ServiceConstants.DAS_CONSTANTS.ASTERISK) });
        ResultSet rs;
        try {
            if (this.statement == null) {
                this.statement = (DASJStatement) this.connection.createStatement();
            }
            rs = new DASJResultSet(this.statement, reader, "", queryEnvironment, ResultSet.TYPE_FORWARD_ONLY, -1, null);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e.getMessage());
        }
        return rs;
    }

    private List<Object[]> getColumnTypeInfo() {
        Integer intZero = 0;
        Short shortZero = 0;
        Short shortMax = Short.MAX_VALUE;
        Short searchable = DatabaseMetaData.typeSearchable;
        Short nullable = DatabaseMetaData.typeNullable;
        List<Object[]> retval = new ArrayList<Object[]>();

        retval.add(new Object[] { "String", Types.VARCHAR, shortMax, "'", "'", null, nullable,
                Boolean.TRUE, searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
                intZero, intZero, intZero });
        retval.add(new Object[] { "Boolean", Types.BOOLEAN, shortMax, null, null, null, nullable,
                Boolean.TRUE, searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
                intZero, intZero, intZero });
        retval.add(new Object[] { "Byte",Types.TINYINT, shortMax, null, null, null, nullable,
                Boolean.TRUE, searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
                intZero, intZero, intZero });
        retval.add(new Object[] { "Short", Types.SMALLINT, shortMax, null, null, null, nullable,
                Boolean.TRUE, searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
                intZero, intZero, intZero });
        retval.add(new Object[] { "Integer", Types.INTEGER, shortMax, null, null, null, nullable,
                Boolean.TRUE, searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
                intZero, intZero, intZero });
        retval.add(new Object[] { "Long", Types.BIGINT, shortMax, null, null, null, nullable,
                Boolean.TRUE, searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
                intZero, intZero, intZero });
        retval.add(new Object[] { "Float", Types.FLOAT, shortMax, null, null, null, nullable,
                Boolean.TRUE, searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
                intZero, intZero, intZero });
        retval.add(new Object[] { "Double", Types.DOUBLE, shortMax, null, null, null, nullable,
                Boolean.TRUE, searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
                intZero, intZero, intZero });
        retval.add(new Object[] { "BigDecimal", Types.DECIMAL, shortMax, null, null, null, nullable,
                Boolean.TRUE, searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
                intZero, intZero, intZero });
        retval.add(new Object[] { "Date", Types.DATE, shortMax, "'", "'", null, nullable, Boolean.TRUE,
                searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax, intZero, intZero,
                intZero });
        retval.add(new Object[] { "Time", Types.TIME, shortMax, "'", "'", null, nullable, Boolean.TRUE,
                searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax, intZero, intZero,
                intZero });
        retval.add(new Object[] { "Timestamp", Types.TIMESTAMP, shortMax, null, null, null, nullable,
                Boolean.TRUE, searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
                intZero, intZero, intZero });
        retval.add(new Object[] { "Asciistream", Types.CLOB, shortMax, null, null, null, nullable,
                Boolean.TRUE, searchable, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, null, shortZero, shortMax,
                intZero, intZero, intZero });
        return retval;
    }
}
