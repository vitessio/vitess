/*
 * Copyright 2017 Google Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.jdbc;

import java.io.File;
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
import java.util.logging.Logger;

import com.google.common.io.Closeables;

import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.VTGateConnection;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.client.grpc.RetryingInterceptorConfig;
import io.vitess.client.grpc.tls.TlsOptions;
import io.vitess.util.Constants;

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

    /**
     * VTGateConnections object consist of vtGateIdentifire list and return vtGate object in round robin.
     */
    public static class VTGateConnections {
        private List<String> vtGateIdentifiers = new ArrayList<>();
        int counter;

        /**
         * Constructor
         *
         * @param connection
         */
        public VTGateConnections(VitessConnection connection) {
            for (VitessJDBCUrl.HostInfo hostInfo : connection.getUrl().getHostInfos()) {
                String identifier = getIdentifer(hostInfo.getHostname(), hostInfo.getPort(), connection.getUsername(), connection.getTarget());
                synchronized (VitessVTGateManager.class) {
                    if (!vtGateConnHashMap.containsKey(identifier)) {
                        updateVtGateConnHashMap(identifier, hostInfo, connection);
                    }
                    if (connection.getUseSSL() && connection.getRefreshSSLConnectionsOnCertFileModification() && vtgateConnRefreshTimer == null) {
                        logger.info("ssl vtgate connection detected -- installing connection refresh based on ssl keystore modification");
                        vtgateConnRefreshTimer = new Timer("ssl-refresh-vtgate-conn", true);
                        vtgateConnRefreshTimer.scheduleAtFixedRate(
                            new TimerTask() {
                                @Override
                                public void run() {
                                    refreshUpdatedSSLConnections();
                                }
                            },
                            TimeUnit.SECONDS.toMillis(60),
                            TimeUnit.SECONDS.toMillis(60));
                    }
                }
                vtGateIdentifiers.add(identifier);
            }
            Random random = new Random();
            counter = random.nextInt(vtGateIdentifiers.size());
        }

        /**
         * Return VTGate Instance object.
         *
         * @return
         */
        public VTGateConnection getVtGateConnInstance() {
            counter++;
            counter = counter % vtGateIdentifiers.size();
            return vtGateConnHashMap.get(vtGateIdentifiers.get(counter));
        }

    }

    private static String getIdentifer(String hostname, int port, String userIdentifer, String keyspace) {
        return (hostname + port + userIdentifer + keyspace);
    }

    public static class RefreshableVTGateConn extends VTGateConnection {
        private final VitessJDBCUrl.HostInfo hostInfo;
        private final VitessConnection conn;
        private final File keystoreFile;
        private final File truststoreFile;
        private volatile long keystoreMtime;
        private volatile long truststoreMtime;

        RefreshableVTGateConn(RpcClient client,
                              VitessJDBCUrl.HostInfo hostInfo,
                              VitessConnection conn,
                              String keystorePath,
                              String truststorePath) {
            super(client);
            this.hostInfo = hostInfo;
            this.conn = conn;
            this.keystoreFile = new File(keystorePath);
            this.truststoreFile = new File(truststorePath);
            // initial check ensures the mtimes are up-to-date
            checkKeystoreUpdates();
        }

        VitessJDBCUrl.HostInfo getHostInfo() {
            return hostInfo;
        }

        VitessConnection getConn() {
            return conn;
        }

        boolean checkKeystoreUpdates() {
            long keystoreMtime = keystoreFile.exists() ? keystoreFile.lastModified() : 0;
            long truststoreMtime = truststoreFile.exists() ? truststoreFile.lastModified() : 0;

            boolean modified = false;
            if (keystoreMtime > this.keystoreMtime) {
                modified = true;
                this.keystoreMtime = keystoreMtime;
            }
            if (truststoreMtime > this.truststoreMtime) {
                modified = true;
                this.truststoreMtime = truststoreMtime;
            }

            return modified;
        }
    }

    /**
     * Create VTGateConn and update vtGateConnHashMap.
     *
     * @param identifier
     * @param hostInfo
     * @param connection
     */
    private static void updateVtGateConnHashMap(String identifier, VitessJDBCUrl.HostInfo hostInfo,
                                                VitessConnection connection) {
        vtGateConnHashMap.put(identifier, getVtGateConn(hostInfo, connection));
    }

    private static void refreshUpdatedSSLConnections() {
        synchronized (VitessVTGateManager.class) {
            int updatedCount = 0;
            for (Map.Entry<String, VTGateConnection> entry : vtGateConnHashMap.entrySet()) {
                if (entry.getValue() instanceof RefreshableVTGateConn) {
                    RefreshableVTGateConn existing = (RefreshableVTGateConn) entry.getValue();
                    if (existing.checkKeystoreUpdates()) {
                        updatedCount++;
                        VTGateConnection old = vtGateConnHashMap.replace(entry.getKey(), getVtGateConn(existing.getHostInfo(), existing.getConn()));
                        try {
                            Closeables.close(old, true);
                        } catch (IOException e) {
                            // exception will be logged by Closeables.close
                        }
                    }
                }
            }
            if (updatedCount > 0) {
                logger.info("refreshed " + updatedCount + " vtgate connections due to keystore update");
            }
        }
    }

    /**
     * Create vtGateConn object with given identifier.
     *
     * @param hostInfo
     * @param connection
     * @return
     */
    private static VTGateConnection getVtGateConn(VitessJDBCUrl.HostInfo hostInfo, VitessConnection connection) {
        final Context context = connection.createContext(connection.getTimeout());
        RetryingInterceptorConfig retryingConfig = getRetryingInterceptorConfig(connection);
        if (connection.getUseSSL()) {
            final String keyStorePath = connection.getKeyStore() != null
                    ? connection.getKeyStore() : System.getProperty(Constants.Property.KEYSTORE_FULL);
            final String keyStorePassword = connection.getKeyStorePassword() != null
                    ? connection.getKeyStorePassword() : System.getProperty(Constants.Property.KEYSTORE_PASSWORD_FULL);
            final String keyAlias = connection.getKeyAlias() != null
                    ? connection.getKeyAlias() : System.getProperty(Constants.Property.KEY_ALIAS_FULL);
            final String keyPassword = connection.getKeyPassword() != null
                    ? connection.getKeyPassword() : System.getProperty(Constants.Property.KEY_PASSWORD_FULL);
            final String trustStorePath = connection.getTrustStore() != null
                    ? connection.getTrustStore() : System.getProperty(Constants.Property.TRUSTSTORE_FULL);
            final String trustStorePassword = connection.getTrustStorePassword() != null
                    ? connection.getTrustStorePassword() : System.getProperty(Constants.Property.TRUSTSTORE_PASSWORD_FULL);
            final String trustAlias = connection.getTrustAlias() != null
                    ? connection.getTrustAlias() : System.getProperty(Constants.Property.TRUST_ALIAS_FULL);

            final TlsOptions tlsOptions = new TlsOptions()
                    .keyStorePath(keyStorePath)
                    .keyStorePassword(keyStorePassword)
                    .keyAlias(keyAlias)
                    .keyPassword(keyPassword)
                    .trustStorePath(trustStorePath)
                    .trustStorePassword(trustStorePassword)
                    .trustAlias(trustAlias);

            return new RefreshableVTGateConn(
                new GrpcClientFactory(retryingConfig).createTls(context, hostInfo.toString(), tlsOptions),
                hostInfo,
                connection,
                keyStorePath,
                trustStorePath);
        } else {
            return new VTGateConnection(new GrpcClientFactory(retryingConfig).create(context, hostInfo.toString()));
        }
    }

    private static RetryingInterceptorConfig getRetryingInterceptorConfig(VitessConnection conn) {
        if (!conn.getGrpcRetriesEnabled()) {
            return RetryingInterceptorConfig.noOpConfig();
        }

        return RetryingInterceptorConfig.exponentialConfig(conn.getGrpcRetryInitialBackoffMillis(), conn.getGrpcRetryMaxBackoffMillis(), conn.getGrpcRetryBackoffMultiplier());
    }

    public static void close() throws SQLException {
        SQLException exception = null;

        for (VTGateConnection vtGateConn : vtGateConnHashMap.values()) {
            try {
                vtGateConn.close();
            } catch (IOException e) {
                exception = new SQLException(e.getMessage(), e);
            }
        }
        vtGateConnHashMap.clear();
        if (null != exception) {
            throw exception;
        }
    }
}
