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

package io.client.grpc;

import java.net.ServerSocket;
import java.util.Arrays;

import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import io.vitess.client.Context;
import io.vitess.client.RpcClientTest;
import io.vitess.client.grpc.GrpcClientFactory;

/**
 * This tests GrpcClient with a mock vtgate server (go/cmd/vtgateclienttest).
 */
@RunWith(JUnit4.class)
public class GrpcClientTest extends RpcClientTest {
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
        new GrpcClientFactory()
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
