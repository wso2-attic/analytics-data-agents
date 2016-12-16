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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * This class implements the java.sql.Driver JDBC interface for the DASJDriver driver.
 */

public class DASJDriver implements Driver {

    //This static code block runs during the JDBC driver class's loading,
    // which registers the driver with the DriverManager.
    static {
        try {
            DASJDriver driverInst = new DASJDriver();
            DriverManager.registerDriver(driverInst);
        } catch (SQLException e) {
            throw new RuntimeException("Driver Initialization Failed" + e.getMessage());
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!url.startsWith(ServiceConstants.DAS_DRIVER_SETTINGS.URL_PREFIX)) {
            return null;
        }
        String connectionURL = url.substring(ServiceConstants.DAS_DRIVER_SETTINGS.URL_PREFIX.length());
        return new DASJConnection(connectionURL, info);

    }

    /**
     * Drivers will return true if they understand the subprotocol specified in the URL and false if they don't.
     * This driver's protocols start with jdbc:dasjdriver.
     *
     * @param url url the URL of the driver
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(ServiceConstants.DAS_DRIVER_SETTINGS.URL_PREFIX);
    }

    /**
     * Allow a generic GUI tool to discover what properties it should prompt a human for in order to get
     * enough information to connect to a database.
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (info == null) {
            info = new Properties();
        }
        DriverPropertyInfo userProp = new DriverPropertyInfo(ServiceConstants.DAS_DRIVER_SETTINGS.DAS_USER,
                info.getProperty(ServiceConstants.DAS_DRIVER_SETTINGS.DAS_USER));
        userProp.required = true;
        userProp.description = ServiceConstants.PROPERTY_DESCRIPTIONS.USERNAME;
        DriverPropertyInfo passwordProp = new DriverPropertyInfo(ServiceConstants.DAS_DRIVER_SETTINGS.DAS_PASS,
                info.getProperty(ServiceConstants.DAS_DRIVER_SETTINGS.DAS_PASS));
        passwordProp.required = true;
        passwordProp.description = ServiceConstants.PROPERTY_DESCRIPTIONS.PASSWORD;
        DriverPropertyInfo[] propertyInfo = new DriverPropertyInfo[2];
        propertyInfo[0] = userProp;
        propertyInfo[1] = passwordProp;
        System.out.println("property info");
        return propertyInfo;

    }

    @Override
    public int getMajorVersion() {
        return ServiceConstants.DAS_VERSIONS.DAS_DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return ServiceConstants.DAS_VERSIONS.DAS_DRIVER_MINOR_VERSION;
    }

    /**
     * Indicates whether have full support for the JDBC API & SQL 92 Entry Level.
     */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("[MethodNotSupported]: Driver.getParentLogger()");
    }
}
