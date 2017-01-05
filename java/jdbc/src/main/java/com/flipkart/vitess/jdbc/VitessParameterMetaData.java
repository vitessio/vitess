package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

public class VitessParameterMetaData implements ParameterMetaData {

  private final int parameterCount;

  /**
   * This implementation (and defaults below) is equivalent to
   * mysql-connector-java's "simple" (non-server)
   * statement metadata
   */
  VitessParameterMetaData(int count) {
    this.parameterCount = count;
  }

  @Override
  public int getParameterCount() throws SQLException {
    return parameterCount;
  }

  @Override
  public int isNullable(int param) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  @Override
  public boolean isSigned(int param) throws SQLException {
    return false;
  }

  @Override
  public int getPrecision(int param) throws SQLException {
    return 0;
  }

  @Override
  public int getScale(int param) throws SQLException {
    return 0;
  }

  @Override
  public int getParameterType(int param) throws SQLException {
    return Types.VARCHAR;
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {
    return "VARCHAR";
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {
    return "java.lang.String";
  }

  @Override
  public int getParameterMode(int param) throws SQLException {
    return param;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
