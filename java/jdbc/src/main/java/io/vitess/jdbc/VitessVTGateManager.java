package io.vitess.jdbc;

import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.VTGateConn;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.client.grpc.RetryingInterceptorConfig;
import io.vitess.client.grpc.tls.TlsOptions;
import io.vitess.util.CommonUtils;
import io.vitess.util.Constants;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by naveen.nahata on 24/02/16.
 */
public class VitessVTGateManager {
    /*
    Current implementation have one VTGateConn for ip-port-username combination
    */
    private static ConcurrentHashMap<String, VTGateConn> vtGateConnHashMap =
        new ConcurrentHashMap<>();


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
                String identifier = getIdentifer(hostInfo.getHostname(), hostInfo.getPort(), connection.getUsername());
                synchronized (VitessVTGateManager.class) {
                    if (!vtGateConnHashMap.containsKey(identifier)) {
                        updateVtGateConnHashMap(identifier, hostInfo, connection);
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
        public VTGateConn getVtGateConnInstance() {
            counter++;
            counter = counter % vtGateIdentifiers.size();
            return vtGateConnHashMap.get(vtGateIdentifiers.get(counter));
        }

    }

    private static String getIdentifer(String hostname, int port, String userIdentifer) {
        return (hostname + port + userIdentifer);
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

    /**
     * Create vtGateConn object with given identifier.
     *
     * @param hostInfo
     * @param connection
     * @return
     */
    private static VTGateConn getVtGateConn(VitessJDBCUrl.HostInfo hostInfo, VitessConnection connection) {
        final String username = connection.getUsername();
        final String keyspace = connection.getKeyspace();
        final Context context = CommonUtils.createContext(username, Constants.CONNECTION_TIMEOUT);
        RetryingInterceptorConfig retryingConfig = getRetryingInterceptorConfig(connection);
        RpcClient client;
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

            client = new GrpcClientFactory(retryingConfig).createTls(context, hostInfo.toString(), tlsOptions);
        } else {
            client = new GrpcClientFactory(retryingConfig).create(context, hostInfo.toString());
        }
        if (null == keyspace) {
            return (new VTGateConn(client));
        }
        return (new VTGateConn(client, keyspace));
    }

    private static RetryingInterceptorConfig getRetryingInterceptorConfig(VitessConnection conn) {
        if (!conn.getGrpcRetriesEnabled()) {
            return RetryingInterceptorConfig.noopConfig();
        }

        return RetryingInterceptorConfig.exponentialConfig(conn.getGrpcRetryInitialBackoffMillis(), conn.getGrpcRetryMaxBackoffMillis(), conn.getGrpcRetryBackoffMultiplier());
    }

    public static void close() throws SQLException {
        SQLException exception = null;

        for (VTGateConn vtGateConn : vtGateConnHashMap.values()) {
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
