package com.github.youtube.vitess.jdbc;

import com.google.protobuf.ServiceException;

import com.github.youtube.vitess.jdbc.QueryService.Session;
import com.github.youtube.vitess.jdbc.QueryService.SessionInfo;
import com.github.youtube.vitess.jdbc.QueryService.SessionParams;
import com.github.youtube.vitess.jdbc.QueryService.SessionParams.Builder;
import com.github.youtube.vitess.jdbc.QueryService.SqlQuery;
import com.github.youtube.vitess.jdbc.QueryService.SqlQuery.BlockingInterface;
import com.github.youtube.vitess.jdbc.QueryService.TransactionInfo;
import com.github.youtube.vitess.jdbc.VtoccModule.VtoccKeyspace;

import java.sql.SQLException;

import javax.inject.Inject;

/**
 * {@link com.github.youtube.vitess.jdbc.QueryService.SqlQuery} methods for transaction management.
 *
 * Represents a connection with at most one transaction at a time.
 */
public class VtoccTransactionHandler {

  private final BlockingInterface sqlQueryBlockingInterface;
  private final String vtoccKeyspace;
  private SessionInfo sessionInfo = null;
  private TransactionInfo transactionInfo = TransactionInfo.getDefaultInstance();
  private boolean inTransaction = false;

  @Inject
  VtoccTransactionHandler(SqlQuery.BlockingInterface sqlQueryBlockingInterface, @VtoccKeyspace String vtoccKeyspace) {
    this.sqlQueryBlockingInterface = sqlQueryBlockingInterface;
    this.vtoccKeyspace = vtoccKeyspace;
  }

  /**
   * Ensures that transaction handler is initialized.
   */
  void init() throws SQLException {
    try {
      // lazy session creation to allow creation of VtoccTransactionHandler via Provider
      if (sessionInfo == null) {
        Builder sessionParams = SessionParams.newBuilder();
        sessionParams.setKeyspace(vtoccKeyspace);
        // TODO(timofeyb): provide rpc controller
        sessionInfo = sqlQueryBlockingInterface.getSessionId(null, sessionParams.build());
      }
    } catch (ServiceException e) {
      throw VtoccSqlExceptionFactory.getSqlException(e);
    }
  }

  /**
   * Returns current session with Vtocc. Establish one if necessary. Enters transaction
   * if necessary.
   */
  public Session getSession() throws SQLException {
    init();
    if (!inTransaction) {
      // end clients are not obligated to call begin()
      begin();
    }
    return Session.newBuilder()
        .setSessionId(sessionInfo.getSessionId())
        .setTransactionId(transactionInfo.getTransactionId())
        .build();
  }

  /**
   * Forces start of a transaction. Would fail on opened transaction.
   */
  public void begin() throws SQLException {
    if (inTransaction) {
      throw new IllegalStateException("Nested transactions are not supported");
    }
    init();
    try {
      inTransaction = true;
      // send previous transaction id to make it easier to cache these rpc queries
      // TODO(timofeyb): provide rpc controller
      transactionInfo = sqlQueryBlockingInterface.begin(null, getSession());
    } catch (ServiceException e) {
      throw VtoccSqlExceptionFactory.getSqlException(e);
    }
  }

  /**
   * Commits a transaction. Would fail on closed/committed/rolledback transaction.
   */
  public void commit() throws SQLException {
    if (!inTransaction) {
      throw new IllegalStateException("Transaction should begin() first");
    }
    init();
    try {
      // TODO(timofeyb): provide rpc controller
      sqlQueryBlockingInterface.commit(null, getSession());
      inTransaction = false;
    } catch (ServiceException e) {
      throw VtoccSqlExceptionFactory.getSqlException(e);
    }
  }

  /**
   * Rollbacks a transaction. Would fail on closed/committed/rolledback transaction.
   */
  public void rollback() throws SQLException {
    if (!inTransaction) {
      throw new IllegalStateException("Transaction should begin() first");
    }
    init();
    try {
      // TODO(timofeyb): provide rpc controller
      sqlQueryBlockingInterface.rollback(null, getSession());
      inTransaction = false;
    } catch (ServiceException e) {
      throw VtoccSqlExceptionFactory.getSqlException(e);
    }
  }


  /**
   * Frees underlying resources, if any.
   */
  public void close() {
    // TODO(timofeyb): free resources by channel
  }
}
