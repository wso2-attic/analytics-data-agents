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

    public static final class DAS_SERVICE_QUERIES {
        public static final String  DAS_SCHEMA_NAME             = "analytics";
        public static final String  DAS_TABLE_NAMES_QUERY       = "/tables";
        public static final String  DAS_SCHEMA_QUERY            = "/schema";
        public static final char    URL_PATH_SEPERATOR          = '/';
        public static final String DEFAULT_ESCAPE_STRING        = "\\";
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

}
