package com.github.youtube.vitess.jdbc.bson;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;

import com.github.youtube.vitess.jdbc.bson.VtoccBsonBlockingRpcChannel.Factory;
import com.github.youtube.vitess.jdbc.vtocc.VtoccJdbcConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Provides instances of JDBC {@link java.sql.Connection}s which use {@link acolyte.jdbc.Driver} and
 * Vtocc Stubby RPC as a transport-level protocol.
 *
 * All the connections are returned fully initialized and ready to be used with transaction already
 * opened.
 */
public class VtoccBsonConnectionFactory {

  private static VtoccBsonConnectionFactory VtoccBsonConnectionFactoryInstance = null;
  private final VtoccBsonBlockingRpcChannel.Factory vtoccBsonRpcChannelFactory;

  @Inject
  public VtoccBsonConnectionFactory(Factory vtoccBsonRpcChannelFactory) {
    this.vtoccBsonRpcChannelFactory = vtoccBsonRpcChannelFactory;
  }

  /**
   * Helper method to create connections to Vtocc. Should be used by external parties that do not
   * use GKS.
   *
   * Uses singleton instance of {@link com.github.youtube.vitess.jdbc.bson.VtoccBsonConnectionFactory}
   * and a Guice injector internally.
   */
  public static synchronized VtoccBsonConnectionFactory getInstance() {
    if (VtoccBsonConnectionFactoryInstance == null) {
      Injector injector = Guice.createInjector();
      VtoccBsonConnectionFactoryInstance = injector.getInstance(VtoccBsonConnectionFactory.class);
    }
    return VtoccBsonConnectionFactoryInstance;
  }

  /**
   * Creates and initializes JDBC connection to Vtocc using {@link VtoccBsonBlockingRpcChannel}.
   */
  public Connection create(String vtoccKeyspaceShard, String vtoccServerSpec)
      throws SQLException {
    return VtoccJdbcConnectionFactory.getInstance().create(
        vtoccBsonRpcChannelFactory.create(vtoccServerSpec),
        vtoccKeyspaceShard);
  }
}
