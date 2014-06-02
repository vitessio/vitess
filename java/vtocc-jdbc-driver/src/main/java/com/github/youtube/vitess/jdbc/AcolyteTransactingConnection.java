package com.github.youtube.vitess.jdbc;

import java.sql.SQLException;

import javax.inject.Inject;

/**
 * Implements connection which handles non-transaction SQL logic in addition to
 * {@link acolyte.Connection} internal logic. Transaction logic itself
 * is handled in a separate class {@link VtoccTransactionHandler}.
 */
public class AcolyteTransactingConnection extends acolyte.Connection {

  private final VtoccTransactionHandler vtoccTransactionHandler;

  @Inject
  AcolyteTransactingConnection(acolyte.Connection connection,
      VtoccTransactionHandler vtoccTransactionHandler) {
    super(connection);
    this.vtoccTransactionHandler = vtoccTransactionHandler;
  }

  /**
   * Initializes new connection by starting a transaction.
   * Optional to call.
   */
  void init() throws SQLException {
    vtoccTransactionHandler.init();
  }

  @Override
  public void commit() throws SQLException {
    super.commit();
    vtoccTransactionHandler.commit();
  }

  @Override
  public void rollback() throws SQLException {
    super.rollback();
    vtoccTransactionHandler.rollback();
  }

  @Override
  public void close() throws SQLException {
    super.close();
    vtoccTransactionHandler.close();
  }
}
