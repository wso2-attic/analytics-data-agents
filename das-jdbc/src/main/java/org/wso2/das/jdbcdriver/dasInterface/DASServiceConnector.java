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

package org.wso2.das.jdbcdriver.dasInterface;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Class which creates the Http Url Connection with the DAS instance
 */
public class DASServiceConnector {

    /**
     * Sends HTTP GET request to DAS Backend rest API
     * @param url Connectio URL of the DAS API
     * @param user User name of the
     * @param pass
     * @return
     * @throws Exception
     */
    public static String sendGet(String url, String user, String pass) throws Exception {
        System.out.println("REQUEST:"+url);
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        //add authorization header
        String userPassword = user + ":" + pass;
        String encoding = new sun.misc.BASE64Encoder().encode(userPassword.getBytes());
        con.setRequestProperty("Authorization", "Basic " + encoding);

        int responseCode = con.getResponseCode();
        System.out.println("DAS Response Code:"+responseCode);

        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        return(response.toString());

    }
}
