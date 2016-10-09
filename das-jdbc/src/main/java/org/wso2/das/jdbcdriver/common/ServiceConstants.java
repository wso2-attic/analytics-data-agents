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
package org.wso2.das.jdbcdriver.common;

/**
 * Represents the global constants defined by the service.
 */
public class ServiceConstants {

    public static final class DAS_DRIVER_SETTINGS {
        public final static String  URL_PREFIX              = "jdbc:dasjdriver:";
        public final static String  DAS_USER                = "user";
        public final static String  DAS_PASS                = "password";
        public static final String  DAS_DRIVER_NAME         = "DASJDBC";
        public static final int     DEFAULT_COLUMN_SIZE     = 100;
    }

    public static final class DAS_VERSIONS {
        public static final String  DAS_DRIVER_VERSION          = "1";
        public static final int     DAS_DRIVER_MAJOR_VERSION    = 1;
        public static final int     DAS_DRIVER_MINOR_VERSION    = 0;
        public static final String  DAS_DB_VERSION              = "1";
    }

    public static final class DAS_CONSTANTS {
        public static final String  UNDERSCORE                  = "_";
        public static final String  ASTERISK                    = "*";
        public static final String  SINGLE_QUOTE                = "'";
        public static final String  DOUBLE_QUOTE                = "\"";
        public static final String  PERCENT_SIGN                = "%";
        public static final String  DOT                         = ".";
        public static final String  EMPTY_STRING                = "";
        public static final String  REGEX_PERCENT               = "%_";
        public static final String  REGEX_ALL                   = ".*";
        public static final String  DEFAULT_DATE                = "1970-01-01";
        public static final String  DEFAULT_TIME                = "00:00:00";
        public static final String  DEFAULT_DATETIME            = "1970-01-01 00:00:00";
        public static final Character  CHAR_PERCENT_SIGN        = '%';
        public static final Character  CHAR_UNDERSCORE_SIGN     = '_';

    }

    public static final class DAS_SERVICE_QUERIES {
        public static final String  DAS_SCHEMA_NAME             = "analytics";
        public static final String  DAS_TABLE_NAMES_QUERY       = "/tables";
        public static final String  DAS_SCHEMA_QUERY            = "/schema";
        public static final char    URL_PATH_SEPERATOR          = '/';
        public static final String  DEFAULT_ESCAPE_STRING       = "\\";
    }

    public static final class DAS_RESPONSE_KEYS{
        public static final String  COLUMNS     = "columns";
        public static final String  TYPE        = "type";
        public static final String  PRIMARYKEYS = "primaryKeys";
        public static final String  ISINDEX     = "isIndex";

    }
    public static final class DAS_RESPONSE_DATA {
        public static final String ROW_VERSION          = "version";
        public static final String ROW_TIMESTAMP        = "timestamp";
        public static final String RESPONSE_TAG_VALUES  = "values";
    }

    public static final class DATATYPES {
        public static final String DATATYPE_STRING      = "STRING";
        public static final String DATATYPE_BOOLEAN     = "BOOLEAN";
        public static final String DATATYPE_BYTE        = "BYTE";
        public static final String DATATYPE_SHORT       = "SHORT";
        public static final String DATATYPE_INT         = "INT";
        public static final String DATATYPE_INTEGER     = "INTEGER";
        public static final String DATATYPE_LONG        = "LONG";
        public static final String DATATYPE_FLOAT       = "FLOAT";
        public static final String DATATYPE_DOUBLE      = "DOUBLE";
        public static final String DATATYPE_BIGDECIMAL  = "BIGDECIMAL";
        public static final String DATATYPE_DATE        = "DATE";
        public static final String DATATYPE_TIME        = "TIME";
        public static final String DATATYPE_TIMESTAMP   = "TIMESTAMP";
        public static final String DATATYPE_ASCIISTREAM = "ASCIISTREAM";
        public static final String DATATYPE_BLOB        = "BLOB";
        public static final String DATATYPE_CLOB        ="CLOB";
        public static final String DATATYPE_EXPRESSION  ="EXPRESSION";
    }

    public static final class AGGREGATE_FUNCTIONS {
        public static final String AGGR_FUNC_COUNT  = "COUNT";
        public static final String AGGR_FUNC_SUM    = "SUM";
        public static final String AGGR_FUNC_MIN    = "MIN";
        public static final String AGGR_FUNC_MAX    = "MAX";
    }

    public static final class OPERATORS {
        public static final String OPERATOR_AND = "AND";
        public static final String OPERATOR_OR  = "OR";
    }

    public static final class PROPERTY_DESCRIPTIONS {
        public static final String USERNAME = "UserName";
        public static final String PASSWORD  = "Password";
    }

    public static final class DAS_METADATA_DEF_COLUMN_NAMES {
        public static final String PROCEDURES = "PROCEDURE_CAT,PROCEDURE_SCHEM,PROCEDURE_NAME,reserved4,reserved5,"
                + "reserved6,REMARKS,PROCEDURE_TYPE,SPECIFIC_NAME";
        public static final String TABLES = "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,TABLE_TYPE,REMARKS,TYPE_CAT,TYPE_SCHEM,"
                + "TYPE_NAME,SELF_REFERENCING_COL_NAME,REF_GENERATION";
        public static final String COLUMNS =
                "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,DATA_TYPE,TYPE_NAME,COLUMN_SIZE,BUFFER_LENGTH,"
                        + "DECIMAL_DIGITS,NUM_PREC_RADIX,NULLABLE,REMARKS,COLUMN_DEF,SQL_DATA_TYPE,SQL_DATETIME_SUB,"
                        + "CHAR_OCTET_LENGTH,ORDINAL_POSITION,IS_NULLABLE,SCOPE_CATLOG,SCOPE_SCHEMA,SCOPE_TABLE,"
                        + "SOURCE_DATA_TYPE,IS_AUTOINCREMENT";
        public static final String SCHEMAS = "TABLE_SCHEM,TABLE_CATALOG";
        public static final String CATALOGS = "TABLE_CAT";
        public static final String TABLETYPES = "TABLE_TYPE";
        public static final String COLUMNPRIVILEGES = "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,GRANTOR,GRANTEE,"
                + "PRIVILEGE,IS_GRANTABLE";
        public static final String TABLEPRIVILEGES = "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,GRANTOR,GRANTEE,PRIVILEGE,"
                + "IS_GRANTABLE";
        public static final String BESTROWID = "SCOPE,COLUMN_NAME,DATA_TYPE,TYPE_NAME,COLUMN_SIZE,BUFFER_LENGTH,"
                + "DECIMAL_DIGITS,PSEUDO_COLUMN";
        public static final String VERSIONCOLUMNS = "SCOPE,COLUMN_NAME,DATA_TYPE,TYPE_NAME,COLUMN_SIZE,BUFFER_LENGTH,"
                + "DECIMAL_DIGITS,PSEUDO_COLUMN";
        public static final String PRIMARYKEYS = "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,KEY_SEQ,PK_NAME";
        public static final String IMPORTEDKEYS =
                "PKTABLE_CAT,PKTABLE_SCHEM,PKTABLE_NAME,PKCOLUMN_NAME,FKTABLE_CAT,FKTABLE_SCHEM,"
                        + "FKTABLE_NAME,FKCOLUMN_NAME,KEY_SEQ,UPDATE_RULE,DELETE_RULE,FK_NAME,PK_NAME,DEFERRABILITY";
        public static final String EXPORTEDKEYS =
                "PKTABLE_CAT,PKTABLE_SCHEM,PKTABLE_NAME,PKCOLUMN_NAME,FKTABLE_CAT,FKTABLE_SCHEM,"
                        + "FKTABLE_NAME,FKCOLUMN_NAME,KEY_SEQ,UPDATE_RULE,DELETE_RULE,FK_NAME,PK_NAME,DEFERRABILITY";
        public static final String TYPEINFO =
                "TYPE_NAME,DATA_TYPE,PRECISION,LITERAL_PREFIX,LITERAL_SUFFIX,CREATE_PARAMS,NULLABLE,CASE_SENSITIVE,"
                        + "SEARCHABLE,UNSIGNED_ATTRIBUTE,FIXED_PREC_SCALE,AUTO_INCREMENT,LOCAL_TYPE_NAME,MINIMUM_SCALE,"
                        + "MAXIMUM_SCALE,SQL_DATA_TYPE,SQL_DATETIME_SUB,NUM_PREC_RADIX";
        public static final String INDEXINFO =
                "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,NON_UNIQUE,INDEX_QUALIFIER,INDEX_NAME,TYPE,ORDINAL_POSITION,"
                        + "COLUMN_NAME,ASC_OR_DESC,CARDINALITY,PAGES,FILTER_CONDITION";
        public static final String UDT = "TYPE_CAT,TYPE_SCHEM,TYPE_NAME,CLASS_NAME,DATA_TYPE,REMARKS,BASE_TYPE";
        public static final String PSEUDOCOLUMNS = "TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,DATA_TYPE,COLUMN_SIZE,"
                + "DECIMAL_DIGITS,NUM_PREC_RADIX,COLUMN_USAGE,REMARKS,CHAR_OCTET_LENGTH,IS_NULLABLE";

    }

    public static final class DAS_METADATA_DEF_COLUMN_TYPES {
        public static final String PROCEDURES = "String,String,String,String,String,String,String,Short,String";
        public static final String TABLES = "String,String,String,String,String,String,String,String,String,String";
        public static final String COLUMNS ="String,String,String,String,Integer,String,Integer,Integer,Integer,"
                + "Integer,Integer,String,String,Integer,Integer,Integer,Integer,String,String,String,String,Short,"
                + "String";
        public static final String SCHEMAS = "String,String";
        public static final String CATALOGS = "String";
        public static final String TABLETYPES = "String";
        public static final String COLUMNPRIVILEGES = "String,String,String,String,String,String,String,String";
        public static final String TABLEPRIVILEGES = "String,String,String,String,String,String,String";
        public static final String BESTROWID = "Short,String,Integer,String,Integer,Integer,Short,Short";
        public static final String VERSIONCOLUMNS = "Short,String,Integer,String,Integer,Integer,Short,Short";
        public static final String PRIMARYKEYS = "String,String,String,String,Short,String";
        public static final String IMPORTEDKEYS = "String,String,String,String,String,String,String,String,Short,Short,"
                + "Short,String,String,Short";
        public static final String EXPORTEDKEYS = "String,String,String,String,String,String,String,String,Short,Short,"
                + "Short,String,String,Short";
        public static final String TYPEINFO = "String,Integer,Integer,String,String,String,Short,Boolean,Short,"
                + "Boolean,Boolean,Boolean,String,Short,Short,Integer,Integer,Integer";
        public static final String INDEXINFO = "String,String,String,Boolean,String,String,Short,Short,String,String,"
                + "Integer,Integer,String";
        public static final String UDT = "String,String,String,String,Integer,String,Short";
        public static final String PSEUDOCOLUMNS = "String,String,String,String,Integer,Integer,Integer,Integer,String,"
                + "String,Integer,String";
    }
}
