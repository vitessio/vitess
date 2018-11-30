package io.client.grpc;

import io.vitess.client.Context;
import io.vitess.client.RpcClientTest;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.client.grpc.StaticAuthCredentials;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Arrays;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GrpcClientStaticAuthTest extends RpcClientTest {
  private static Process vtgateclienttest;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String vtRoot = System.getenv("VTROOT");
    if (vtRoot == null) {
      throw new RuntimeException("cannot find env variable VTROOT; make sure to source dev.env");
    }
    URI staticAuthFile = GrpcClientStaticAuthTest.class.getResource("grpc_static_auth.json").toURI();

    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();

    vtgateclienttest = new ProcessBuilder(Arrays.asList(
        vtRoot + "/bin/vtgateclienttest",
        "-logtostderr",
        "-grpc_port", Integer.toString(port),
        "-service_map", "grpc-vtgateservice",
        "-grpc_auth_mode", "static",
        "-grpc_auth_static_password_file", staticAuthFile.getPath()
    )).inheritIO().start();

    Context ctx = Context.getDefault().withDeadlineAfter(Duration.millis(5000L));
    client = new GrpcClientFactory()
        .setCallCredentials(new StaticAuthCredentials("test-username", "test-password"))
        .create(ctx, "localhost:" + port);
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
