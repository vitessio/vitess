package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;

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

        DriverPropertyInfo[] dpi = new DriverPropertyInfo[7];
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

            dpi[2] =
                new DriverPropertyInfo(Constants.Property.KEYSPACE, vitessJDBCUrl.getKeyspace());
            dpi[2].required = true;
            dpi[2].description = Constants.VITESS_KEYSPACE;

            dpi[3] = new DriverPropertyInfo(Constants.Property.TABLET_TYPE,
                vitessJDBCUrl.getTabletType().toString());
            dpi[3].required = false;
            dpi[3].description = Constants.VITESS_TABLET_TYPE;

            dpi[4] = new DriverPropertyInfo(Constants.Property.EXECUTE_TYPE,
                vitessJDBCUrl.getExecuteType().toString());
            dpi[4].required = false;
            dpi[4].description = Constants.EXECUTE_TYPE_DESC;

            dpi[5] = new DriverPropertyInfo(Constants.Property.DBNAME, vitessJDBCUrl.getCatalog());
            dpi[5].required = true;
            dpi[5].description = Constants.VITESS_DB_NAME;

            dpi[6] =
                new DriverPropertyInfo(Constants.Property.USERNAME, vitessJDBCUrl.getUsername());
            dpi[6].required = false;
            dpi[6].description = Constants.USERNAME_DESC;


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
