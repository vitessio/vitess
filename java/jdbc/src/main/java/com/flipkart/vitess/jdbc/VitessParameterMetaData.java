package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * Created by harshit.gangal on 25/01/16.
 */
public class VitessParameterMetaData implements ParameterMetaData {

    /* Get actual class name to be printed on */
    private static Logger logger = Logger.getLogger(VitessParameterMetaData.class.getName());

    public int getParameterCount() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public int isNullable(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public boolean isSigned(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public int getPrecision(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public int getScale(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public int getParameterType(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public String getParameterTypeName(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public String getParameterClassName(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public int getParameterMode(int param) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }
}
