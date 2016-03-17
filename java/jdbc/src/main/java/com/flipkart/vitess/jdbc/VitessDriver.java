package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;
import com.flipkart.vitess.util.Utils;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
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

    public Connection connect(String url, Properties info) throws SQLException {
        return acceptsURL(url) ? new VitessConnection(url, info) : null;
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
    public boolean acceptsURL(String url) throws SQLException {
        return null != url && url.startsWith(Constants.URL_PREFIX);
    }

    /**
     * @param url  - Connection Url
     * @param info - Property for the connection
     * @return DriverPropertyInfo Array Object
     * @throws SQLException
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (null == info) {
            info = new Properties();
        }

        DriverPropertyInfo[] dpi = new DriverPropertyInfo[4];
        if (acceptsURL(url)) {
            Utils.parseURLForPropertyInfo(url, info);

            dpi[0] = new DriverPropertyInfo(Constants.Property.HOST,
                info.getProperty(Constants.Property.HOST));
            dpi[0].required = true;
            dpi[0].description = Constants.VITESS_HOST;

            dpi[1] = new DriverPropertyInfo(Constants.Property.PORT,
                info.getProperty(Constants.Property.PORT));
            dpi[1].required = false;
            dpi[1].description = Constants.VITESS_PORT;

            dpi[2] = new DriverPropertyInfo(Constants.Property.KEYSPACE,
                info.getProperty(Constants.Property.KEYSPACE));
            dpi[2].required = true;
            dpi[2].description = Constants.VITESS_KEYSPACE;

            dpi[3] = new DriverPropertyInfo(Constants.Property.TABLET_TYPE,
                info.getProperty(Constants.Property.TABLET_TYPE));
            dpi[3].required = false;
            dpi[3].description = Constants.VITESS_TABLET_TYPE;

        } else {
            throw new SQLException(Constants.SQLExceptionMessages.INVALID_CONN_URL + " : " + url);
        }

        return dpi;
    }

    public int getMajorVersion() {
        return Constants.DRIVER_MAJOR_VERSION;
    }

    public int getMinorVersion() {
        return Constants.DRIVER_MINOR_VERSION;
    }

    public boolean jdbcCompliant() {
        return Constants.JDBC_COMPLIANT;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

}
