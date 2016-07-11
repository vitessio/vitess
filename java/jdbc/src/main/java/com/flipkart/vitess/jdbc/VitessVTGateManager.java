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
                String identifier = getIdentifer(hostInfo.getHostname(), hostInfo.getPort(),
                    vitessJDBCUrl.getUsername());
                synchronized (VitessVTGateManager.class) {
                    if (!vtGateConnHashMap.containsKey(identifier)) {
                        updateVtGateConnHashMap(identifier, hostInfo.getHostname(),
                            hostInfo.getPort(), vitessJDBCUrl.getUsername(),
                            vitessJDBCUrl.getKeyspace());
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
     * Create vtGateConn object with given identifier.
     *
     * @param hostname
     * @param port
     * @param username
     * @return
     */
    private static VTGateConn getVtGateConn(String hostname, int port, String username) {
        Context context = CommonUtils.createContext(username, Constants.CONNECTION_TIMEOUT);
        InetSocketAddress inetSocketAddress = new InetSocketAddress(hostname, port);
        RpcClient client = new GrpcClientFactory().create(context, inetSocketAddress);
        return (new VTGateConn(client));
    }

    /**
     * Create vtGateConn object with given identifier.
     *
     * @param hostname
     * @param port
     * @param username
     * @param keyspace
     * @return
     */
    private static VTGateConn getVtGateConn(String hostname, int port, String username,
        String keyspace) {
        Context context = CommonUtils.createContext(username, Constants.CONNECTION_TIMEOUT);
        InetSocketAddress inetSocketAddress = new InetSocketAddress(hostname, port);
        RpcClient client = new GrpcClientFactory().create(context, inetSocketAddress);
        if (null == keyspace) {
            return (new VTGateConn(client));
        }
        return (new VTGateConn(client, keyspace));
    }


    /**
     * Create VTGateConne and update vtGateConnHashMap.
     *
     * @param identifier
     * @param hostname
     * @param port
     * @param username
     * @param keyspace
     */
    private static void updateVtGateConnHashMap(String identifier, String hostname, int port,
        String username, String keyspace) {
        vtGateConnHashMap.put(identifier, getVtGateConn(hostname, port, username, keyspace));
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
