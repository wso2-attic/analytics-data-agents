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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * This class have a collection of Utility functions required by the Driver classes.
 */
public class ServiceUtil {

    /**
     * Get a object of the given sql data type.
     *
     * @param sqlTypeName Sql Type input
     */
    public static Object getLiteral(String sqlTypeName) {
        Object retVal = null;
        sqlTypeName = sqlTypeName.toUpperCase();
        switch (sqlTypeName) {
            case ServiceConstants.DATATYPES.DATATYPE_STRING:
                retVal = ServiceConstants.DAS_CONSTANTS.EMPTY_STRING;
                break;
            case ServiceConstants.DATATYPES.DATATYPE_BOOLEAN:
                retVal = Boolean.FALSE;
                break;
            case ServiceConstants.DATATYPES.DATATYPE_BYTE:
                retVal = Byte.valueOf((byte) 1);
                break;
            case ServiceConstants.DATATYPES.DATATYPE_SHORT:
                retVal = Short.valueOf((short) 1);
                break;
            case ServiceConstants.DATATYPES.DATATYPE_INT:
            case ServiceConstants.DATATYPES.DATATYPE_INTEGER:
                retVal = Integer.valueOf(1);
                break;
            case ServiceConstants.DATATYPES.DATATYPE_LONG:
                retVal = Long.valueOf(1);
                break;
            case ServiceConstants.DATATYPES.DATATYPE_FLOAT:
                retVal = Float.valueOf(1);
                break;
            case ServiceConstants.DATATYPES.DATATYPE_DOUBLE:
                retVal = Double.valueOf(1);
                break;
            case ServiceConstants.DATATYPES.DATATYPE_BIGDECIMAL:
                retVal = BigDecimal.valueOf(1);
                break;
            case ServiceConstants.DATATYPES.DATATYPE_DATE:
                retVal = Date.valueOf(ServiceConstants.DAS_CONSTANTS.DEFAULT_DATE);
                break;
            case ServiceConstants.DATATYPES.DATATYPE_TIME:
                retVal = Time.valueOf(ServiceConstants.DAS_CONSTANTS.DEFAULT_TIME);
                break;
            case ServiceConstants.DATATYPES.DATATYPE_TIMESTAMP:
                retVal = Timestamp.valueOf(ServiceConstants.DAS_CONSTANTS.DEFAULT_DATETIME);
                break;
            case ServiceConstants.DATATYPES.DATATYPE_ASCIISTREAM:
                retVal = new ByteArrayInputStream(new byte[] { });
                break;
            default:
                retVal = null;
        }
        return retVal;
    }

    /**
     * Get SQL data type of a given object.
     *
     * @param literal Object which need to get the sql type
     */
    public static String getSQLType(Object literal) {
        if (literal instanceof String) {
            return ServiceConstants.DATATYPES.DATATYPE_STRING;
        } else if (literal instanceof Boolean) {
            return ServiceConstants.DATATYPES.DATATYPE_BOOLEAN;
        } else if (literal instanceof Byte) {
            return ServiceConstants.DATATYPES.DATATYPE_BYTE;
        } else if (literal instanceof Short) {
            return ServiceConstants.DATATYPES.DATATYPE_SHORT;
        } else if (literal instanceof Integer) {
            return ServiceConstants.DATATYPES.DATATYPE_INT;
        } else if (literal instanceof Long) {
            return ServiceConstants.DATATYPES.DATATYPE_LONG;
        } else if (literal instanceof Float) {
            return ServiceConstants.DATATYPES.DATATYPE_FLOAT;
        } else if (literal instanceof Double) {
            return ServiceConstants.DATATYPES.DATATYPE_DOUBLE;
        } else if (literal instanceof BigDecimal) {
            return ServiceConstants.DATATYPES.DATATYPE_BIGDECIMAL;
        } else if (literal instanceof Date) {
            return ServiceConstants.DATATYPES.DATATYPE_DATE;
        } else if (literal instanceof Time) {
            return ServiceConstants.DATATYPES.DATATYPE_TIME;
        } else if (literal instanceof Timestamp) {
            return ServiceConstants.DATATYPES.DATATYPE_TIMESTAMP;
        } else if (literal instanceof InputStream) {
            return ServiceConstants.DATATYPES.DATATYPE_ASCIISTREAM;
        } else {
            return null;
        }
    }

    /**
     * Match Regular expression with the given input string.
     *
     * @param likePattern Pattern to be matched
     * @param escape      Escape sequence
     * @param input       Input string which needs to match with the given pattern
     */
    public static boolean isPatternMatched(String likePattern, String escape, CharSequence input) {
        boolean retVal;
        int percentIndex = likePattern.indexOf(ServiceConstants.DAS_CONSTANTS.CHAR_PERCENT_SIGN);
        int underscoreIndex = likePattern.indexOf(ServiceConstants.DAS_CONSTANTS.CHAR_UNDERSCORE_SIGN);
        if (percentIndex < 0 && underscoreIndex < 0) {
            retVal = likePattern.equals(input); //No wildcards. Compare the string.
        } else {
            boolean isEscaped = false;
            StringBuilder regex = new StringBuilder();
            StringTokenizer tokenizer = new StringTokenizer(likePattern,
                    ServiceConstants.DAS_CONSTANTS.REGEX_PERCENT + escape, true);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if (token.equals(escape)) {
                    if (isEscaped) {
                        regex.append(Pattern.quote(token));
                    } else {
                        isEscaped = true;
                    }
                } else {
                    if (isEscaped) {
                        regex.append(Pattern.quote(token));
                    } else if (token.equals(ServiceConstants.DAS_CONSTANTS.PERCENT_SIGN)) {
                        regex.append(ServiceConstants.DAS_CONSTANTS.REGEX_ALL);
                    } else if (token.equals(ServiceConstants.DAS_CONSTANTS.UNDERSCORE)) {
                        regex.append(ServiceConstants.DAS_CONSTANTS.DOT);
                    } else {
                        regex.append(Pattern.quote(token));
                    }
                    isEscaped = false;
                }
            }
            Pattern pattern = Pattern.compile(regex.toString());
            retVal = pattern.matcher(input).matches();
        }
        return retVal;
    }

    /**
     * Get the byte array of a given string.
     *
     * @param str Input string which needs to convert to byte[]
     */
    public static byte[] parseBytes(String str) {
        try {
            if (str != null) {
                return str.getBytes();
            }
            return null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Get Ascii Stream of a given string.
     *
     * @param str Input string which needs to convert to Ascii Stream
     */
    public static InputStream getAsciiStream(String str) {
        if (str != null) {
            return new ByteArrayInputStream(str.getBytes());
        }
        return null;
    }

    /**
     * Remove the inverted commas of the table name if exists.
     *
     * @param tableName - Name of the table before removing inverted comma characters
     */
    public static String extractTableName(String tableName) {
        if (tableName != null) {
            if (tableName.startsWith(ServiceConstants.DAS_CONSTANTS.SINGLE_QUOTE) || tableName
                    .startsWith(ServiceConstants.DAS_CONSTANTS.DOUBLE_QUOTE)) {
                tableName = tableName.substring(1);
            }
            if (tableName.endsWith(ServiceConstants.DAS_CONSTANTS.SINGLE_QUOTE) || tableName
                    .endsWith(ServiceConstants.DAS_CONSTANTS.DOUBLE_QUOTE)) {
                tableName = tableName.substring(0, tableName.length() - 1);
            }
        }
        return tableName;
    }
}
