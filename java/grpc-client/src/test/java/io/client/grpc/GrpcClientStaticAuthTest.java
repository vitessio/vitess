/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.client.grpc;

import com.google.common.base.Throwables;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.RpcClientTest;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.client.grpc.StaticAuthCredentials;
import io.vitess.proto.Vtgate.GetSrvKeyspaceRequest;

import org.joda.time.Duration;

import java.net.ServerSocket;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GrpcClientStaticAuthTest extends RpcClientTest {

  private static Process vtgateclienttest;
  private static int port;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String vtRoot = System.getenv("VTROOT");
    if (vtRoot == null) {
      throw new RuntimeException("cannot find env variable VTROOT; make sure to source dev.env");
    }
    URI staticAuthFile = GrpcClientStaticAuthTest.class.getResource("grpc_static_auth.json")
        .toURI();

    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
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

  @Test
  public void testWrongPassword() throws Exception {
    RpcClient client = new GrpcClientFactory()
        .setCallCredentials(new StaticAuthCredentials("test-username", "WRONG-password"))
        .create(Context.getDefault(), "localhost:" + port);
    try {
      client.getSrvKeyspace(Context.getDefault(), GetSrvKeyspaceRequest.getDefaultInstance()).get();
      Assert.fail();
    } catch (ExecutionException e) {
      StatusRuntimeException cause = (StatusRuntimeException) Throwables.getRootCause(e);
      Assert.assertSame(cause.getStatus().getCode(), Status.Code.PERMISSION_DENIED);
    }
    client.close();
  }
}
