/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.jdbc;

import static java.lang.System.getProperty;

import io.vitess.client.Context;
import io.vitess.client.RefreshableVTGateConnection;
import io.vitess.client.RpcClient;
import io.vitess.client.VTGateConnection;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.client.grpc.RetryingInterceptorConfig;
import io.vitess.client.grpc.tls.TlsOptions;
import io.vitess.util.Constants.Property;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by naveen.nahata on 24/02/16.
 */
public class VitessVTGateManager {

  private static Logger logger = Logger.getLogger(VitessVTGateManager.class.getName());
  /*
  Current implementation have one VTGateConn for ip-port-username combination
  */
  private static ConcurrentHashMap<String, VTGateConnection> vtGateConnHashMap =
      new ConcurrentHashMap<>();
  private static Timer vtgateConnRefreshTimer = null;
  private static Timer vtgateClosureTimer = null;
  private static long vtgateClosureDelaySeconds = 0L;

  /**
   * VTGateConnections object consist of vtGateIdentifire list and return vtGate object in round
   * robin.
   */
  public static class VTGateConnections {

    private List<String> vtGateIdentifiers = new ArrayList<>();
    int counter;

    /**
     * Constructor
     */
    public VTGateConnections(final VitessConnection connection) {
      maybeStartClosureTimer(connection);
      for (final VitessJDBCUrl.HostInfo hostInfo : connection.getUrl().getHostInfos()) {
        String identifier = getIdentifer(hostInfo.getHostname(), hostInfo.getPort(),
            connection.getUsername(), connection.getTarget());
        synchronized (VitessVTGateManager.class) {
          if (!vtGateConnHashMap.containsKey(identifier)) {
            updateVtGateConnHashMap(identifier, hostInfo, connection);
          }
          if (connection.getUseSSL() && connection.getRefreshConnection()
              && vtgateConnRefreshTimer == null) {
            logger.info(
                "ssl vtgate connection detected -- installing connection refresh based on ssl "
                    + "keystore modification");
            vtgateConnRefreshTimer = new Timer("ssl-refresh-vtgate-conn", true);
            vtgateConnRefreshTimer.scheduleAtFixedRate(new TimerTask() {
                  @Override
                  public void run() {
                    refreshUpdatedSSLConnections(hostInfo, connection);
                  }
                }, TimeUnit.SECONDS.toMillis(connection.getRefreshSeconds()),
                TimeUnit.SECONDS.toMillis(connection.getRefreshSeconds()));
          }
        }
        vtGateIdentifiers.add(identifier);
      }
      Random random = new Random();
      counter = random.nextInt(vtGateIdentifiers.size());
    }

    /**
     * Return VTGate Instance object.
     */
    public VTGateConnection getVtGateConnInstance() {
      counter++;
      counter = counter % vtGateIdentifiers.size();
      return vtGateConnHashMap.get(vtGateIdentifiers.get(counter));
    }

  }

  private static void maybeStartClosureTimer(VitessConnection connection) {
    if (connection.getRefreshClosureDelayed() && vtgateClosureTimer == null) {
      synchronized (VitessVTGateManager.class) {
        if (vtgateClosureTimer == null) {
          vtgateClosureTimer = new Timer("vtgate-conn-closure", true);
          vtgateClosureDelaySeconds = connection.getRefreshClosureDelaySeconds();
        }
      }
    }
  }

  private static String getIdentifer(String hostname, int port, String userIdentifer,
      String keyspace) {
    return (hostname + port + userIdentifer + keyspace);
  }

  /**
   * Create VTGateConn and update vtGateConnHashMap.
   */
  private static void updateVtGateConnHashMap(String identifier, VitessJDBCUrl.HostInfo hostInfo,
      VitessConnection connection) {
    vtGateConnHashMap.put(identifier, getVtGateConn(hostInfo, connection));
  }

  private static void refreshUpdatedSSLConnections(VitessJDBCUrl.HostInfo hostInfo,
      VitessConnection connection) {
    synchronized (VitessVTGateManager.class) {
      int updatedCount = 0;
      for (Map.Entry<String, VTGateConnection> entry : vtGateConnHashMap.entrySet()) {
        if (entry.getValue() instanceof RefreshableVTGateConnection) {
          RefreshableVTGateConnection existing = (RefreshableVTGateConnection) entry.getValue();
          if (existing.checkKeystoreUpdates()) {
            updatedCount++;
            VTGateConnection old = vtGateConnHashMap
                .replace(entry.getKey(), getVtGateConn(hostInfo, connection));
            closeRefreshedConnection(old);
          }
        }
      }
      if (updatedCount > 0) {
        logger.info("refreshed " + updatedCount + " vtgate connections due to keystore update");
      }
    }
  }

  private static void closeRefreshedConnection(final VTGateConnection old) {
    if (vtgateClosureTimer != null) {
      logger.info(String
          .format("%s Closing connection with a %s second delay", old, vtgateClosureDelaySeconds));
      vtgateClosureTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          actuallyCloseRefreshedConnection(old);
        }
      }, TimeUnit.SECONDS.toMillis(vtgateClosureDelaySeconds));
    } else {
      actuallyCloseRefreshedConnection(old);
    }
  }

  private static void actuallyCloseRefreshedConnection(final VTGateConnection old) {
    try {
      logger.info(old + " Closing connection because it had been refreshed");
      old.close();
    } catch (IOException ioe) {
      logger.log(Level.WARNING, String.format("Error closing VTGateConnection %s", old), ioe);
    }
  }

  private static String nullIf(String ifNull, String returnThis) {
    if (ifNull == null) {
      return returnThis;
    } else {
      return ifNull;
    }
  }

  /**
   * Create vtGateConn object with given identifier.
   */
  private static VTGateConnection getVtGateConn(VitessJDBCUrl.HostInfo hostInfo,
                                                VitessConnection connection) {
    final Context context = connection.createContext(connection.getTimeout());
    RetryingInterceptorConfig retryingConfig = getRetryingInterceptorConfig(connection);
    GrpcClientFactory grpcClientFactory =
        new GrpcClientFactory(retryingConfig, connection.getUseTracing());
    if (connection.getUseSSL()) {
      TlsOptions tlsOptions = getTlsOptions(connection);
      RpcClient rpcClient = grpcClientFactory
          .createTls(context, hostInfo.toString(), tlsOptions);
      return new RefreshableVTGateConnection(rpcClient,
          tlsOptions.getKeyStore().getPath(),
          tlsOptions.getTrustStore().getPath());
    } else {
      RpcClient client = grpcClientFactory.create(context, hostInfo.toString());
      return new VTGateConnection(client);
    }
  }

  private static TlsOptions getTlsOptions(VitessConnection con) {
    String keyStorePath = nullIf(con.getKeyStore(), getProperty(Property.KEYSTORE_FULL));
    String keyStorePassword = nullIf(con.getKeyStorePassword(),
        getProperty(Property.KEYSTORE_PASSWORD_FULL));
    String keyAlias = nullIf(con.getKeyAlias(), getProperty(Property.KEY_ALIAS_FULL));
    String keyPassword = nullIf(con.getKeyPassword(), getProperty(Property.KEY_PASSWORD_FULL));
    String trustStorePath = nullIf(con.getTrustStore(), getProperty(Property.TRUSTSTORE_FULL));
    String trustStorePassword = nullIf(
        con.getTrustStorePassword(),
        getProperty(Property.TRUSTSTORE_PASSWORD_FULL));
    String trustAlias = nullIf(con.getTrustAlias(), getProperty(Property.TRUST_ALIAS_FULL));

    return new TlsOptions()
        .keyStorePath(keyStorePath)
        .keyStorePassword(keyStorePassword)
        .keyAlias(keyAlias)
        .keyPassword(keyPassword)
        .trustStorePath(trustStorePath)
        .trustStorePassword(trustStorePassword)
        .trustAlias(trustAlias);
  }

  private static RetryingInterceptorConfig getRetryingInterceptorConfig(VitessConnection conn) {
    if (!conn.getGrpcRetriesEnabled()) {
      return RetryingInterceptorConfig.noOpConfig();
    }

    return RetryingInterceptorConfig.exponentialConfig(conn.getGrpcRetryInitialBackoffMillis(),
        conn.getGrpcRetryMaxBackoffMillis(), conn.getGrpcRetryBackoffMultiplier());
  }

  public static void close() throws SQLException {
    SQLException exception = null;

    for (VTGateConnection vtGateConn : vtGateConnHashMap.values()) {
      try {
        vtGateConn.close();
      } catch (IOException ioe) {
        exception = new SQLException(ioe.getMessage(), ioe);
      }
    }
    vtGateConnHashMap.clear();
    if (null != exception) {
      throw exception;
    }
  }
}
