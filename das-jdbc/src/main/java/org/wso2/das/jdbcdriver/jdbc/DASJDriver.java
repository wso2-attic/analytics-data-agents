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

import org.wso2.das.jdbcdriver.common.ServiceConstants;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * This class implements the java.sql.Driver JDBC interface for the DASJDriver driver.
 */

public class DASJDriver implements Driver {

    // This static block inits the driver when the class is loaded by the JVM.
    static
    {
        try
        {
            //Register the DASJDriver with DriverManager
            DASJDriver driverInst = new DASJDriver();
            DriverManager.registerDriver(driverInst);
        }
        catch(SQLException e)
        {
            throw new RuntimeException("Driver Initialization Failed" + e.getMessage());
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!url.startsWith(ServiceConstants.DAS_DRIVER_SETTINGS.URL_PREFIX))
        {
            return null;
        }

        String connectionURL = url.substring(ServiceConstants.DAS_DRIVER_SETTINGS.URL_PREFIX.length());
        DASJConnection connection = new DASJConnection(connectionURL,info);
        return connection;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(ServiceConstants.DAS_DRIVER_SETTINGS.URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return ServiceConstants.DAS_VERSIONS.DAS_DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return ServiceConstants.DAS_VERSIONS.DAS_DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("[MethodNotSupported]: Driver.getParentLogger()");
    }
}
