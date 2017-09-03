/*
 * Copyright 2017 Google Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import io.vitess.util.Constants;

/**
 * VitessDriver is the official JDBC driver for Vitess.
 *
 * It was initially contributed by Flipkart.
 *
 * Before April 2017, the code was located in the package "com.flipkart.vitess.jdbc".
 *
 * Created by harshit.gangal on 23/01/16.
 */
public class VitessDriver implements Driver {

    /* Get actual class name to be printed on */
    private static Logger logger = Logger.getLogger(VitessDriver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new VitessDriver());
        } catch (SQLException e) {
            throw new RuntimeException(
                Constants.SQLExceptionMessages.INIT_FAILED + " : " + e.getErrorCode() + " - " + e
                    .getMessage());
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        final VitessConnection connection = new VitessConnection(url, info);
        connection.connect();
        return connection;
    }

    /**
     * Checks whether a given url is in a valid format.
     * <p/>
     * The current uri format is: jdbc:vitess://[host]:[port]
     *
     * @param url the URL of the database
     * @return true, if this driver understands the given URL;
     * false, otherwise
     * <p/>
     * TODO: Write a better regex
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return null != url && url.startsWith(Constants.URL_PREFIX);
    }

    /**
     * Given a String URL and a Properties object, computes the expected and
     * required parameters for the driver. Parameters can be set in either the URL
     * or Properties.
     *
     * @param url  - Connection Url
     *             A vitess-compatible URL, per {@link VitessJDBCUrl}
     *             i.e. jdbc:vitess://locahost:9000/vt_keyspace?TABLET_TYPE=master
     * @param info - Property for the connection
     *             May contain additional properties to configure this driver with
     * @return DriverPropertyInfo Array Object
     * @throws SQLException
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (null == info) {
            info = new Properties();
        }

        DriverPropertyInfo[] dpi = ConnectionProperties.exposeAsDriverPropertyInfo(info, 2);
        if (acceptsURL(url)) {
            VitessJDBCUrl vitessJDBCUrl = new VitessJDBCUrl(url, info);

            dpi[0] = new DriverPropertyInfo(Constants.Property.HOST,
                vitessJDBCUrl.getHostInfos().get(0).getHostname());
            dpi[0].required = true;
            dpi[0].description = Constants.VITESS_HOST;

            dpi[1] = new DriverPropertyInfo(Constants.Property.PORT,
                (new Integer(vitessJDBCUrl.getHostInfos().get(0).getPort())).toString());
            dpi[1].required = false;
            dpi[1].description = Constants.VITESS_PORT;

        } else {
            throw new SQLException(Constants.SQLExceptionMessages.INVALID_CONN_URL + " : " + url);
        }

        return dpi;
    }

    @Override
    public int getMajorVersion() {
        return Constants.DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return Constants.DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return Constants.JDBC_COMPLIANT;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

}
