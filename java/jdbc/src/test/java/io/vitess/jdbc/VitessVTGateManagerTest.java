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

import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.VTGateConnection;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.client.grpc.netty.NettyChannelBuilderProvider;
import io.vitess.client.grpc.netty.SimpleChannelBuilderProvider;
import io.vitess.proto.Vtrpc;

import org.joda.time.Duration;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by naveen.nahata on 29/02/16.
 */
public class VitessVTGateManagerTest {

  public VTGateConnection getVtGateConn() {
    Vtrpc.CallerID callerId = Vtrpc.CallerID.newBuilder().setPrincipal("username").build();
    Context ctx = Context.getDefault().withDeadlineAfter(Duration.millis(500))
        .withCallerId(callerId);
    RpcClient client = new GrpcClientFactory().create(ctx, "host:80");
    return new VTGateConnection(client);
  }

  @Test
  public void testVtGateConnectionsConstructorMultipleVtGateConnections()
      throws SQLException, NoSuchFieldException, IllegalAccessException, IOException {
    VitessVTGateManager.close();
    Properties info = new Properties();
    info.setProperty("username", "user");
    VitessConnection connection = new VitessConnection(
        "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,10.33.17"
            + ".233:15991/shipment/shipment?tabletType=master", info);
    VitessVTGateManager.VTGateConnections vtGateConnections =
        new VitessVTGateManager.VTGateConnections(
        connection);

    info.setProperty("username", "user");
    VitessConnection connection1 = new VitessConnection(
        "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,11.33.17"
            + ".233:15991/shipment/shipment?tabletType=master", info);
    VitessVTGateManager.VTGateConnections vtGateConnections1 =
        new VitessVTGateManager.VTGateConnections(
        connection1);

    Field privateMapField = VitessVTGateManager.class.
        getDeclaredField("vtGateConnHashMap");
    privateMapField.setAccessible(true);
    ConcurrentHashMap<String, VTGateConnection> map = (ConcurrentHashMap<String,
        VTGateConnection>) privateMapField
        .get(VitessVTGateManager.class);
    Assert.assertEquals(4, map.size());
    VitessVTGateManager.close();
  }

  @Test
  public void testVtGateConnectionsConstructor()
      throws SQLException, NoSuchFieldException, IllegalAccessException, IOException {
    VitessVTGateManager.close();
    Properties info = new Properties();
    info.setProperty("username", "user");
    VitessConnection connection = new VitessConnection(
        "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,10.33.17"
            + ".233:15991/shipment/shipment?tabletType=master", info);
    VitessVTGateManager.VTGateConnections vtGateConnections =
        new VitessVTGateManager.VTGateConnections(
        connection);
    Assert
        .assertEquals(vtGateConnections.getVtGateConnInstance() instanceof VTGateConnection, true);
    VTGateConnection vtGateConn = vtGateConnections.getVtGateConnInstance();
    Field privateMapField = VitessVTGateManager.class.
        getDeclaredField("vtGateConnHashMap");
    privateMapField.setAccessible(true);
    ConcurrentHashMap<String, VTGateConnection> map = (ConcurrentHashMap<String,
        VTGateConnection>) privateMapField
        .get(VitessVTGateManager.class);
    Assert.assertEquals(3, map.size());
    VitessVTGateManager.close();
  }

  @Test
  public void testGetChannelProviderFromProperties() throws SQLException {
    VitessVTGateManager.close();

    Properties info = new Properties();
    info.setProperty("grpcChannelBuilderProvider", SimpleChannelBuilderProvider.class.getCanonicalName());

    VitessConnection connection = new VitessConnection(
        "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,10.33.17"
            + ".233:15991/shipment/shipment?tabletType=master", info);

    NettyChannelBuilderProvider channelBuilderProvider = VitessVTGateManager.getChannelProviderFromProperties(connection);
    Assert.assertTrue(SimpleChannelBuilderProvider.class.isAssignableFrom(channelBuilderProvider.getClass()));
  }

}
