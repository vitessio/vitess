package io.client.grpc;

import io.vitess.client.Context;
import io.vitess.client.RpcClientTest;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.client.grpc.RetryingInterceptorConfig;

import org.joda.time.Duration;

import java.net.ServerSocket;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class GrpcClientWithRetriesTest extends RpcClientTest {

  private static Process vtgateclienttest;
  private static int port;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String vtRoot = System.getenv("VTROOT");
    if (vtRoot == null) {
      throw new RuntimeException("cannot find env variable VTROOT; make sure to source dev.env");
    }

    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    vtgateclienttest =
        new ProcessBuilder(
            Arrays.asList(
                vtRoot + "/bin/vtgateclienttest",
                "-logtostderr",
                "-grpc_port",
                Integer.toString(port),
                "-service_map",
                "grpc-vtgateservice"))
            .inheritIO()
            .start();

    client =
        new GrpcClientFactory(RetryingInterceptorConfig.exponentialConfig(5, 60, 2), false)
            .create(
                Context.getDefault().withDeadlineAfter(Duration.millis(5000)),
                "localhost:" + port);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (client != null) {
      client.close();
    }
    if (vtgateclienttest != null) {
      vtgateclienttest.destroy();
      vtgateclienttest.waitFor();
    }
  }
}
