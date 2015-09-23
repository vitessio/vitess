package com.youtube.vitess.client.grpc;

import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.RpcClientTest;

import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;

/**
 * This tests GrpcClient with a mock vtgate server (go/cmd/vtgateclienttest).
 */
public class GrpcClientTest extends RpcClientTest {
  private static Process vtgateclienttest;
  private static int port;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    vtgateclienttest =
        new ProcessBuilder(
            Arrays.asList(vtRoot + "/bin/vtgateclienttest", "-logtostderr", "-grpc_port",
                Integer.toString(port), "-service_map", "grpc-vtgateservice")).start();

    client = new GrpcClientFactory().create(
        Context.getDefault().withDeadlineAfter(Duration.millis(5000)),
        new InetSocketAddress("localhost", port));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (client != null) {
      client.close();
    }
    if (vtgateclienttest != null) {
      vtgateclienttest.destroy();
    }
  }
}
