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
        if (jdbcUrl.isUseSSL()) {
            // TODO: Check SSL-related optional parameters, and fall back to JVM startup properties or environment variables if any are missing
            // TODO: Call newly-created "GrpcClientFactory.createSecure(...)" method instead of the plain ".create(...)"
        }

        Context context = CommonUtils.createContext(username, Constants.CONNECTION_TIMEOUT);
        InetSocketAddress inetSocketAddress = new InetSocketAddress(hostname, port);
        RpcClient client = new GrpcClientFactory().create(context, inetSocketAddress);
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
