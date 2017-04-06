package io.vitess.jdbc;

import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.VTGateConn;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.proto.Vtrpc;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by naveen.nahata on 29/02/16.
 */
public class VitessVTGateManagerTest {

    public VTGateConn getVtGateConn() {
        Vtrpc.CallerID callerId = Vtrpc.CallerID.newBuilder().setPrincipal("username").build();
        Context ctx =
            Context.getDefault().withDeadlineAfter(Duration.millis(500)).withCallerId(callerId);
        RpcClient client = new GrpcClientFactory().create(ctx, new InetSocketAddress("host", 80));
        return new VTGateConn(client);
    }

    @Test public void testVtGateConnectionsConstructorMultipleVtGateConnections()
        throws SQLException, NoSuchFieldException, IllegalAccessException, IOException {
        VitessVTGateManager.close();
        Properties info = new Properties();
        info.setProperty("username", "user");
        VitessConnection connection = new VitessConnection(
                "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,10.33.17"
                        + ".233:15991/shipment/shipment?tabletType=master", info);
        VitessVTGateManager.VTGateConnections vtGateConnections =
            new VitessVTGateManager.VTGateConnections(connection);

        info.setProperty("username", "user");
        VitessConnection connection1 = new VitessConnection(
            "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,11.33.17"
                + ".233:15991/shipment/shipment?tabletType=master", info);
        VitessVTGateManager.VTGateConnections vtGateConnections1 =
            new VitessVTGateManager.VTGateConnections(connection1);

        Field privateMapField = VitessVTGateManager.class.
            getDeclaredField("vtGateConnHashMap");
        privateMapField.setAccessible(true);
        ConcurrentHashMap<String, VTGateConn> map =
            (ConcurrentHashMap<String, VTGateConn>) privateMapField.get(VitessVTGateManager.class);
        Assert.assertEquals(4, map.size());
        VitessVTGateManager.close();
    }

    @Test public void testVtGateConnectionsConstructor()
        throws SQLException, NoSuchFieldException, IllegalAccessException, IOException {
        VitessVTGateManager.close();
        Properties info = new Properties();
        info.setProperty("username", "user");
        VitessConnection connection = new VitessConnection(
            "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,10.33.17"
                + ".233:15991/shipment/shipment?tabletType=master", info);
        VitessVTGateManager.VTGateConnections vtGateConnections =
            new VitessVTGateManager.VTGateConnections(connection);
        Assert.assertEquals(vtGateConnections.getVtGateConnInstance() instanceof VTGateConn, true);
        VTGateConn vtGateConn = vtGateConnections.getVtGateConnInstance();
        Field privateMapField = VitessVTGateManager.class.
            getDeclaredField("vtGateConnHashMap");
        privateMapField.setAccessible(true);
        ConcurrentHashMap<String, VTGateConn> map =
            (ConcurrentHashMap<String, VTGateConn>) privateMapField.get(VitessVTGateManager.class);
        Assert.assertEquals(3, map.size());
        VitessVTGateManager.close();
    }

}
