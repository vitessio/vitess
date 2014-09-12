package com.github.youtube.vitess.jdbc.vtocc;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.protobuf.BlockingRpcChannel;

import com.github.youtube.vitess.jdbc.vtocc.VtoccModule.VtoccConnectionCreationScope;
import com.github.youtube.vitess.jdbc.vtocc.VtoccModule.VtoccKeyspaceShard;

import java.sql.Connection;
import java.sql.SQLException;

import javax.inject.Provider;

/**
 * Provides instances of JDBC {@link Connection}s which use {@link acolyte.Driver} and Vtocc Stubby
 * RPC as a transport-level protocol.
 *
 * All the connections are returned fully initialized and ready to be used with transaction already
 * opened.
 */
public class VtoccJdbcConnectionFactory {

  private static VtoccJdbcConnectionFactory vtoccJdbcConnectionFactoryInstance = null;
  private final Provider<AcolyteTransactingConnection> acolyteTransactingConnectionProvider;
  private final VtoccConnectionCreationScope vtoccConnectionCreationScope;

  @Inject
  @VisibleForTesting
  VtoccJdbcConnectionFactory(
      Provider<AcolyteTransactingConnection> acolyteTransactingConnectionProvider,
      VtoccConnectionCreationScope vtoccConnectionCreationScope) {
    this.acolyteTransactingConnectionProvider = acolyteTransactingConnectionProvider;
    this.vtoccConnectionCreationScope = vtoccConnectionCreationScope;
  }

  /**
   * Helper method to create connections to Vtocc. Should be used by external parties that do not
   * use GKS.
   *
   * Uses singleton instance of {@link VtoccJdbcConnectionFactory} and a Guice injector internally.
   */
  public static synchronized VtoccJdbcConnectionFactory getInstance() {
    if (vtoccJdbcConnectionFactoryInstance == null) {
      Injector injector = Guice.createInjector(new VtoccModule());
      vtoccJdbcConnectionFactoryInstance = injector.getInstance(VtoccJdbcConnectionFactory.class);
    }
    return vtoccJdbcConnectionFactoryInstance;
  }

  /**
   * Creates and initializes JDBC connection to Vtocc.
   */
  public Connection create(BlockingRpcChannel blockingRpcChannel, String vtoccKeyspace)
      throws SQLException {
    vtoccConnectionCreationScope.enter();
    try {
      vtoccConnectionCreationScope.seed(BlockingRpcChannel.class,
          blockingRpcChannel);
      vtoccConnectionCreationScope.seed(
          Key.get(String.class, VtoccKeyspaceShard.class),
          vtoccKeyspace);
      AcolyteTransactingConnection acolyteTransactingConnection =
          acolyteTransactingConnectionProvider.get();
      acolyteTransactingConnection.init();
      return acolyteTransactingConnection;
    } finally {
      vtoccConnectionCreationScope.exit();
    }
  }
}
