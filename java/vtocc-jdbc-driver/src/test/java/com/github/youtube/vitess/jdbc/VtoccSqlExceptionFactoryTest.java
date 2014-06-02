package com.github.youtube.vitess.jdbc;

import com.google.protobuf.ServiceException;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientException;

/**
 * Tests {@link VtoccSqlExceptionFactory}.
 */
@RunWith(JUnit4.class)
public class VtoccSqlExceptionFactoryTest {

  @Test(expected = SQLNonTransientException.class)
  public void testUnknownApplicationError() throws Exception {
    throw VtoccSqlExceptionFactory.getSqlException(new ServiceException(""));
  }

  @Test(expected = SQLTransientException.class)
  public void testRetriableError() throws Exception {
    throw VtoccSqlExceptionFactory.getSqlException(new ServiceException("retry: foo"));
  }

  @Test(expected = SQLNonTransientException.class)
  public void testFatalError() throws Exception {
    throw VtoccSqlExceptionFactory.getSqlException(new ServiceException("fatal: foo"));
  }

  @Test(expected = SQLTransientException.class)
  public void testPoolError() throws Exception {
    throw VtoccSqlExceptionFactory.getSqlException(new ServiceException("tx_pool_full: foo"));
  }

  @Test(expected = SQLException.class)
  public void testGenericError() throws Exception {
    throw VtoccSqlExceptionFactory.getSqlException(new ServiceException("error: foo"));
  }

  @Test(expected = SQLException.class)
  public void testMySqlError() throws Exception {
    try {
      throw VtoccSqlExceptionFactory.getSqlException(new ServiceException("error: foo (errno 42)"));
    } catch (SQLException e) {
      Assert.assertEquals(42, e.getErrorCode());
      throw e;
    }
  }
}
