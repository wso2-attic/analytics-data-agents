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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.wso2.das.jdbcdriver.dasInterface.DataReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JSON helper functions to decode the DAS Service responses.
 */
public class JSONUtil {

    private static Logger logger = Logger.getLogger(JSONUtil.class.getName());

    /**
     * Decode the given json array and returns a list of Strings.
     *
     * @param sInput JSon String with array
     * @return Array of string decoded from the json array.
     */
    public static List<String> parseSimpleArray(String sInput) {
        List<String> listValues = new ArrayList<String>();
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(sInput);
            JSONArray array = (JSONArray) obj;
            for (Object arrObj : array) {
                String str = (String) arrObj;
                listValues.add(str);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Parse simple array:", e);
        }
        return listValues;
    }

    /**
     * Decode a JSON array which contains sub arrays.
     *
     * @param sInput     JSON input with sub arrays
     * @param lOneHeader Tag name of the external array
     * @param lTwoHeader Tag name of the internal array
     */
    public static HashMap<String, String> parseSubArray(String sInput, String lOneHeader, String lTwoHeader) {
        HashMap<String, String> mapReturn = new HashMap<String, String>();
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(sInput);
            JSONObject internalObj = (JSONObject) ((JSONObject) obj).get(lOneHeader);
            for (Map.Entry entry : (Set<Map.Entry>) internalObj.entrySet()) {
                String sKey = (String) entry.getKey();
                JSONObject valObj = (JSONObject) entry.getValue();
                String sValue = (String) valObj.get(lTwoHeader);
                mapReturn.put(sKey, sValue);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Parse sub array with L2 Header:", e);
        }
        return mapReturn;
    }

    /**
     * Decode a json array to find elements with required key value.
     */
    public static List<String> parseSubArrayWithCheckValue(String sInput, String lOneHeader, String lTwoHeader) {
        List<String> listReturn = new LinkedList<String>();
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(sInput);
            JSONObject internalObj = (JSONObject) ((JSONObject) obj).get(lOneHeader);
            for (Map.Entry entry : (Set<Map.Entry>) internalObj.entrySet()) {
                String sKey = (String) entry.getKey();
                JSONObject valObj = (JSONObject) entry.getValue();
                boolean bValue = (Boolean) valObj.get(lTwoHeader);
                if (bValue) {
                    listReturn.add(sKey);
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Parse sub array with check Value:", e);
        }
        return listReturn;
    }

    /**
     * Find a json sub array with a given key.
     *
     * @param sInput     JSON String with sub arrays
     * @param lOneHeader Sub array with the key
     */
    public static List<String> parseSubArray(String sInput, String lOneHeader) {
        List<String> listReturn = new LinkedList<String>();
        JSONParser parser = new JSONParser();
        try {
            Object jsonObj = parser.parse(sInput);
            Object obj = ((JSONObject) jsonObj).get(lOneHeader);
            JSONArray array = (JSONArray) obj;
            for (Object arrObj : array) {
                String subValue = (String) arrObj;
                listReturn.add(subValue);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Parse sub array with L1 Header:", e);
        }
        return listReturn;
    }

    /**
     * Decode the JSON Data Array retrieved from the DAS backend.
     *
     * @param sInput       JSON String which contains the DAS response.
     * @param colDataTypes Data types of the each column
     */
    public static DataReader parseDataArray(String sInput, HashMap<String, String> colDataTypes) {
        JSONParser parser = new JSONParser();
        DataReader dataReader = new DataReader();
        try {
            Object obj = parser.parse(sInput);
            JSONArray arr = (JSONArray) obj;
            List<Object[]> columnValues = new ArrayList<Object[]>(arr.size());
            List<String> listColumnNames = new ArrayList<String>();
            List<String> listColumnDataTypes = new ArrayList<String>();
            //The data type of the Column "_version" is not present in the schema query.
            colDataTypes
                    .put(ServiceConstants.DAS_RESPONSE_DATA.ROW_VERSION, ServiceConstants.DATATYPES.DATATYPE_STRING);
            for (int i = 0; i < arr.size(); i++) {
                JSONObject dataObj = (JSONObject) arr.get(i);
                long lTime = (Long) dataObj.get(ServiceConstants.DAS_RESPONSE_DATA.ROW_TIMESTAMP);
                JSONObject rowValObj = (JSONObject) dataObj.get(ServiceConstants.DAS_RESPONSE_DATA.RESPONSE_TAG_VALUES);
                Object[] dataRow = new Object[rowValObj.size() + 1];//ONE is added to store the timestamp
                int j = 0;
                for (Map.Entry entry : (Set<Map.Entry>) rowValObj.entrySet()) {
                    String sKey = (String) entry.getKey();
                    //Get the column names in the order appear in the "values" object.
                    // Extract this info only for the first row.
                    if (i == 0) {
                        if (sKey.startsWith(ServiceConstants.DAS_CONSTANTS.UNDERSCORE)) {
                            sKey = sKey.substring(1);
                        }
                        listColumnNames.add(sKey.toUpperCase());
                        listColumnDataTypes.add(colDataTypes.get(sKey));
                    }
                    Object columnVal = entry.getValue();
                    dataRow[j] = columnVal;
                    j++;
                }
                //Add the timestamp data
                dataRow[j] = lTime;
                columnValues.add(dataRow);
            }
            if (!listColumnNames.isEmpty()) {
                listColumnNames.add(ServiceConstants.DAS_RESPONSE_DATA.ROW_TIMESTAMP.toUpperCase());
                listColumnDataTypes.add(ServiceConstants.DATATYPES.DATATYPE_LONG);
            }
            dataReader.setColumnTypes(listColumnDataTypes.toArray(new String[listColumnDataTypes.size()]));
            dataReader.setColumnNames(listColumnNames.toArray(new String[listColumnDataTypes.size()]));
            dataReader.setColumnValues(columnValues);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in Parse Data array:", e);
        }
        return dataReader;
    }
}
