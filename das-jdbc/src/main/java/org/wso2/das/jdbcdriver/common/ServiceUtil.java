/*
 *  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
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
     * Get a object of the given sql data type
     * @param sqlTypeName Sql Type input
     */
    public static Object getLiteral(String sqlTypeName)
    {
        Object retval = null;
        sqlTypeName = sqlTypeName.toUpperCase();
        if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_STRING))
            retval = "";
        else if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_BOOLEAN))
            retval = Boolean.FALSE;
        else if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_BYTE))
            retval = Byte.valueOf((byte) 1);
        else if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_SHORT))
            retval = Short.valueOf((short) 1);
        else if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_INT) || sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_INTEGER))
            retval = Integer.valueOf(1);
        else if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_LONG))
            retval = Long.valueOf(1);
        else if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_FLOAT))
            retval = Float.valueOf(1);
        else if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_DOUBLE))
            retval = Double.valueOf(1);
        else if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_BIGDECIMAL))
            retval = BigDecimal.valueOf(1);
        else if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_DATE))
            retval = Date.valueOf("1970-01-01");
        else if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_TIME))
            retval = Time.valueOf("00:00:00");
        else if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_TIMESTAMP))
            retval = Timestamp.valueOf("1970-01-01 00:00:00");
        else if (sqlTypeName.equals(ServiceConstants.DATATYPES.DATATYPE_ASCIISTREAM))
            retval = new ByteArrayInputStream(new byte[]{});
        return retval;
    }

    /**
     * Get SQL data type of a given object.
     * @param literal Object which need to get the sql type
     * @return
     */
    public static String getSQLType(Object literal)
    {
        String retval = null;
        if (literal instanceof String)
            retval = ServiceConstants.DATATYPES.DATATYPE_STRING;
        else if (literal instanceof Boolean)
            retval = ServiceConstants.DATATYPES.DATATYPE_BOOLEAN;
        else if (literal instanceof Byte)
            retval = ServiceConstants.DATATYPES.DATATYPE_BYTE;
        else if (literal instanceof Short)
            retval = ServiceConstants.DATATYPES.DATATYPE_SHORT;
        else if (literal instanceof Integer)
            retval = ServiceConstants.DATATYPES.DATATYPE_INT;
        else if (literal instanceof Long)
            retval = ServiceConstants.DATATYPES.DATATYPE_LONG;
        else if (literal instanceof Float)
            retval = ServiceConstants.DATATYPES.DATATYPE_FLOAT;
        else if (literal instanceof Double)
            retval = ServiceConstants.DATATYPES.DATATYPE_DOUBLE;
        else if (literal instanceof BigDecimal)
            retval = ServiceConstants.DATATYPES.DATATYPE_BIGDECIMAL;
        else if (literal instanceof Date)
            retval = ServiceConstants.DATATYPES.DATATYPE_DATE;
        else if (literal instanceof Time)
            retval = ServiceConstants.DATATYPES.DATATYPE_TIME;
        else if (literal instanceof Timestamp)
            retval = ServiceConstants.DATATYPES.DATATYPE_TIMESTAMP;
        else if (literal instanceof InputStream)
            retval = ServiceConstants.DATATYPES.DATATYPE_ASCIISTREAM;
        return retval;
    }


    /**
     * Match Regular expression with the given input string.
     * @param likePattern Pattern to be matched
     * @param escape Escape sequence
     * @param input Input string which needs to match with the given pattern
     * @return
     */
    public static boolean isPatternMatched(String likePattern, String escape, CharSequence input)
    {
        boolean retval;
        int percentIndex = likePattern.indexOf('%');
        int underscoreIndex = likePattern.indexOf('_');
        if (percentIndex < 0 && underscoreIndex < 0)
        {
			retval = likePattern.equals(input); //No wildcards. Compare the string.
        }
        else
        {
            Pattern p =null;
            boolean isEscaped = false;
            StringBuilder regex = new StringBuilder();
            StringTokenizer tokenizer = new StringTokenizer(likePattern, "%_" + escape, true);
            while (tokenizer.hasMoreTokens())
            {
                String token = tokenizer.nextToken();
                if (token.equals(escape))
                {
                    if (isEscaped)
                    {
                       regex.append(Pattern.quote(token));
                    }
                    else
                    {
                        isEscaped = true;
                    }
                }
                else
                {
                    if (isEscaped)
                        regex.append(Pattern.quote(token));
                    else if (token.equals("%"))
                        regex.append(".*");
                    else if (token.equals("_"))
                        regex.append(".");
                    else
                        regex.append(Pattern.quote(token));
                    isEscaped = false;
                }
            }

			p = Pattern.compile(regex.toString());
            retval = p.matcher(input).matches();
        }
        return retval;


    }

    /**
     * Get the byte array of a given string
     * @param str Input string which needs to convert to byte[]
     */
    public static byte[] parseBytes(String str)
    {
        try
        {
            byte[] b;
            if (str == null)
                b = null;
            else
                b = str.getBytes();
            return b;
        }
        catch (RuntimeException e)
        {
            return null;
        }
    }

    /**
     * Get Ascii Stream of a given string
     * @param str Input string wich needs to convert to Ascii Stream
     */
    public static InputStream getAsciiStream(String str)
    {
        if(str == null)
            return null;
        else
            return new ByteArrayInputStream(str.getBytes());
    }

}
