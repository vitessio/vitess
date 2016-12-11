package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.CommonUtils;
import com.flipkart.vitess.util.Constants;
import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.RpcClient;
import com.youtube.vitess.client.VTGateConn;
import com.youtube.vitess.client.grpc.GrpcClientFactory;

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
         * @param vitessJDBCUrl
         */
        public VTGateConnections(VitessJDBCUrl vitessJDBCUrl) {
            for (VitessJDBCUrl.HostInfo hostInfo : vitessJDBCUrl.getHostInfos()) {
                String identifier = getIdentifier(hostInfo.getHostname(), hostInfo.getPort(),
                    vitessJDBCUrl.getUsername());
                synchronized (VitessVTGateManager.class) {
                    if (!vtGateConnHashMap.containsKey(identifier)) {
                        updateVtGateConnHashMap(identifier, hostInfo.getHostname(),
                            hostInfo.getPort(), vitessJDBCUrl);
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

    private static String getIdentifier(String hostname, int port, String userIdentifer) {
        return (hostname + port + userIdentifer);
    }

    /**
     * Create VTGateConne and update vtGateConnHashMap.
     *
     * @param identifier
     * @param hostname
     * @param port
     * @param jdbcUrl
     */
    private static void updateVtGateConnHashMap(String identifier, String hostname, int port,
                                                VitessJDBCUrl jdbcUrl) {
        vtGateConnHashMap.put(identifier, getVtGateConn(hostname, port, jdbcUrl));
    }

    /**
     * Create vtGateConn object with given identifier.
     *
     * @param hostname
     * @param port
     * @param jdbcUrl
     * @return
     */
    private static VTGateConn getVtGateConn(String hostname, int port, VitessJDBCUrl jdbcUrl) {
        final String username = jdbcUrl.getUsername();
        final String keyspace = jdbcUrl.getKeyspace();
        final Context context = CommonUtils.createContext(username, Constants.CONNECTION_TIMEOUT);
        final InetSocketAddress inetSocketAddress = new InetSocketAddress(hostname, port);
        RpcClient client;
        if (jdbcUrl.isUseSSL()) {
            final String keyStorePath = jdbcUrl.getKeyStore() != null
                    ? jdbcUrl.getKeyStore() : System.getProperty(Constants.Property.KEYSTORE_FULL);
            final String keyStorePassword = jdbcUrl.getKeyStorePassword() != null
                    ? jdbcUrl.getKeyStorePassword() : System.getProperty(Constants.Property.KEY_PASSWORD_FULL);
            final String keyAlias = jdbcUrl.getKeyAlias() != null
                    ? jdbcUrl.getKeyAlias() : System.getProperty(Constants.Property.KEY_ALIAS_FULL);
            final String keyPassword = jdbcUrl.getKeyPassword() != null
                    ? jdbcUrl.getKeyPassword() : System.getProperty(Constants.Property.KEY_PASSWORD_FULL);
            final String trustStorePath = jdbcUrl.getTrustStore() != null
                    ? jdbcUrl.getTrustStore() : System.getProperty(Constants.Property.TRUSTSTORE_FULL);
            final String trustStorePassword = jdbcUrl.getTrustStorePassword() != null
                    ? jdbcUrl.getTrustStorePassword() : System.getProperty(Constants.Property.TRUSTSTORE_PASSWORD_FULL);
            final String trustAlias = jdbcUrl.getTrustAlias() != null
                    ? jdbcUrl.getTrustAlias() : System.getProperty(Constants.Property.TRUSTSTORE_ALIAS_FULL);

            final GrpcClientFactory.TlsOptions tlsOptions = new GrpcClientFactory.TlsOptions()
                    .keyStorePath(keyStorePath)
                    .keyStorePassword(keyStorePassword)
                    .keyAlias(keyAlias)
                    .keyPassword(keyPassword)
                    .trustStorePath(trustStorePath)
                    .trustStorePassword(trustStorePassword)
                    .trustAlias(trustAlias);

            client = new GrpcClientFactory().createTls(context, inetSocketAddress, tlsOptions);
        } else {
            client = new GrpcClientFactory().create(context, inetSocketAddress);
        }
        if (null == keyspace) {
            return (new VTGateConn(client));
        }
        return (new VTGateConn(client, keyspace));
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
