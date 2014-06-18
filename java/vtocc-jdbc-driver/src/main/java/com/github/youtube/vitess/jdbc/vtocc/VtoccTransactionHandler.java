package com.github.youtube.vitess.jdbc.vtocc;

import com.google.inject.Provider;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import com.github.youtube.vitess.jdbc.vtocc.QueryService.Session;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.SessionInfo;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.SessionParams;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.SessionParams.Builder;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.SqlQuery.BlockingInterface;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.TransactionInfo;
import com.github.youtube.vitess.jdbc.vtocc.VtoccModule.VtoccKeyspaceShard;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

/**
 * {@link QueryService.SqlQuery} methods for transaction management.
 *
 * Represents a connection with at most one transaction at a time.
 */
public class VtoccTransactionHandler {

  private final BlockingInterface sqlQueryBlockingInterface;
  private final String vtoccKeyspaceShard;
  private final Provider<RpcController> rpcControllerProvider;
  private SessionInfo sessionInfo = null;
  private TransactionInfo transactionInfo = TransactionInfo.getDefaultInstance();
  private boolean inTransaction = false;

  @Inject
  VtoccTransactionHandler(BlockingInterface sqlQueryBlockingInterface,
      @VtoccKeyspaceShard String vtoccKeyspaceShard,
      Provider<RpcController> rpcControllerProvider) {
    this.sqlQueryBlockingInterface = sqlQueryBlockingInterface;
    this.vtoccKeyspaceShard = vtoccKeyspaceShard;
    this.rpcControllerProvider = rpcControllerProvider;
  }

  /**
   * Ensures that transaction handler is initialized.
   */
  void init() throws SQLException {
    try {
      // lazy session creation to allow creation of VtoccTransactionHandler via Provider
      if (sessionInfo == null) {
        Builder sessionParams = SessionParams.newBuilder();
        Matcher matcher = Pattern.compile("([^/]*)(?:/(\\d+))?").matcher(vtoccKeyspaceShard);
        if (!matcher.matches()) {
          throw new IllegalArgumentException("Invalid keyspace/shard " + vtoccKeyspaceShard);
        }
        sessionParams.setKeyspace(matcher.group(1));
        if (matcher.group(2) != null) {
          sessionParams.setShard(matcher.group(2));
        }

        sessionInfo = sqlQueryBlockingInterface
            .getSessionId(rpcControllerProvider.get(), sessionParams.build());
      }
    } catch (ServiceException e) {
      throw VtoccSqlExceptionFactory.getSqlException(e);
    }
  }

  /**
   * Returns current session with Vtocc. Establish one if necessary. Enters transaction if
   * necessary.
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
      transactionInfo = sqlQueryBlockingInterface.begin(rpcControllerProvider.get(), getSession());
    } catch (ServiceException e) {
      throw VtoccSqlExceptionFactory.getSqlException(e);
    }
  }

  /**
   * Commits a transaction. Would fail on closed/committed/rolledback transaction.
   */
  public void commit() throws SQLException {
    if (!inTransaction) {
      begin();
    }
    init();
    try {
      sqlQueryBlockingInterface.commit(rpcControllerProvider.get(), getSession());
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
      begin();
    }
    init();
    try {
      sqlQueryBlockingInterface.rollback(rpcControllerProvider.get(), getSession());
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
