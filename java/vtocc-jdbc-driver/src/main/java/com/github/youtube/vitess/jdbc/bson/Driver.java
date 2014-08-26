package com.github.youtube.vitess.jdbc.bson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.util.Properties;

/**
 * Jdbc driver for Vtocc server.
 *
 * Expects jdbc url in {@code jdbc:vtocc://host:port/keyspace} form. Can be used via {@link
 * java.sql.DriverManager} or direct instantiation.
 *
 * Driver is very lightweight, so you can create as many instances as you need.
 */
public class Driver extends acolyte.jdbc.Driver {

  private static final Logger logger = LoggerFactory.getLogger(Driver.class);

  private static final String DEFAULT_JDBC_PREFIX = "jdbc:vtocc:";

  static {
    // Register vtocc driver in DriverManager
    try {
      java.sql.DriverManager.registerDriver(new Driver());
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  private final String jdbcPrefix;

  public Driver() {
    this(DEFAULT_JDBC_PREFIX);
  }

  public Driver(String jdbcPrefix) {
    this.jdbcPrefix = jdbcPrefix;
  }

  /**
   * Establishes a new connection to Vtocc server.
   *
   * @param url jdbc:vtocc://host:port/keyspace
   * @param info TODO(timofeyb): stop ignoring
   */
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    logger.info("Opening jdbc Vtocc connection to {}", url);
    final URI jdbcUri;
    try {
      if (!url.startsWith(jdbcPrefix)) {
        throw new IllegalArgumentException(
            "jdbc url specification differs in connect() and accptURL() methods");
      }
      jdbcUri = new URI(url.substring(jdbcPrefix.length()));
      // TODO: select different implementations based on scheme
      if (jdbcUri.getHost().isEmpty()) {
        throw new IllegalArgumentException("Host should not be empty in jdbc url");
      }
      if (jdbcUri.getPort() < 0) {
        throw new IllegalArgumentException("Port should not be empty in jdbc url");
      }
      if (jdbcUri.getPath().isEmpty()) {
        throw new IllegalArgumentException("Keyspace should not be empty in jdbc url");
      }

    } catch (URISyntaxException | RuntimeException e) {
      logger.error("Invalid format for jdbc url, jdbc:vtocc://host:port/keyspace[/shard] expected",
          e);
      throw new SQLInvalidAuthorizationSpecException("jdbc url can not be parsed", e);
    }

    return VtoccBsonConnectionFactory.getInstance().create(jdbcUri.getPath().substring(1),
        jdbcUri.getHost() + ":" + Integer.toString(jdbcUri.getPort()));
  }

  /**
   * Accepts all urls starts with {@code jdbc:vtocc:}.
   */
  @Override
  public boolean acceptsURL(String url) {
    return url != null && url.startsWith(jdbcPrefix);
  }
}
