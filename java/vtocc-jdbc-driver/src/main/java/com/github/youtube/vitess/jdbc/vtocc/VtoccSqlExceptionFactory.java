package com.github.youtube.vitess.jdbc.vtocc;

import com.google.protobuf.ServiceException;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Factory to convert exceptions generated on RPC layer as a Vtocc call result to normal {@link
 * java.sql.SQLException}.
 */
public class VtoccSqlExceptionFactory {

  /**
   * Converts {@link com.google.protobuf.ServiceException} into {@link java.sql.SQLException}.
   */
  public static SQLException getSqlException(ServiceException e) {
    String detail = e.getMessage();
    if (detail == null) {
      return new SQLNonTransientException("Unknown Vtocc exception", e);
    } else if (detail.startsWith("retry")) {
      return new SQLTransientException("Retriable Vtocc exception: " + detail, e);
    } else if (detail.startsWith("fatal")) {
      return new SQLNonTransientException("Fatal Vtocc exception: " + detail, e);
    } else if (detail.startsWith("tx_pool_full")) {
      return new SQLTransientConnectionException(
          "Pool exhausted Vtocc exception: " + detail, e);
    }
    Matcher errNoMatcher = Pattern.compile("\\(errno (\\d+)\\)").matcher(detail);
    if (errNoMatcher.find()) {
      return new SQLException(
          "MySQL error: " + detail, detail, Integer.parseInt(errNoMatcher.group(1)), e);
    }
    return new SQLException("Vtocc exception: " + detail, detail, e);
  }
}
