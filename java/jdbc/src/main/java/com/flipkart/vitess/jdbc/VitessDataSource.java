package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * Created by harshit.gangal on 25/01/16.
 */
public class VitessDataSource implements DataSource {

    /* Get actual class name to be printed on */
    private static Logger logger = Logger.getLogger(VitessDataSource.class.getName());

    public Connection getConnection() throws SQLException {
        return null;
    }

    public Connection getConnection(String username, String password) throws SQLException {
        return null;
    }

    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    public void setLoginTimeout(int seconds) throws SQLException {

    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
