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

import com.google.common.io.Files;

import io.vitess.client.Context;
import io.vitess.client.RpcClientTest;
import io.vitess.client.grpc.GrpcClientFactory;
import io.vitess.client.grpc.tls.TlsOptions;

import org.joda.time.Duration;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Paths;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This tests GrpcClient with a mock vtgate server (go/cmd/vtgateclienttest), over an SSL connection
 * with client authentication enabled.
 */
@RunWith(JUnit4.class)
public class GrpcClientTlsClientAuthTest extends RpcClientTest {

  private static Process vtgateclienttest;
  private static int port;
  private static File certDirectory;

  private static String caConfig;
  private static String caKey;
  private static String caCert;
  private static String caCertDer;
  private static String trustStore;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    certDirectory = Files.createTempDir();
    System.out.println("Using cert directory: " + certDirectory.getCanonicalPath());

    caConfig = certDirectory.getCanonicalPath() + File.separatorChar + "ca.config";
    caKey = certDirectory.getCanonicalPath() + File.separatorChar + "ca-key.pem";
    caCert = certDirectory.getCanonicalPath() + File.separatorChar + "ca-cert.pem";
    caCertDer = certDirectory.getCanonicalPath() + File.separatorChar + "ca-cert.der";
    trustStore = certDirectory.getCanonicalPath() + File.separatorChar + "ca-trustStore.jks";

    createCA();
    createTrustStore();
    createSignedCert("01", "server");
    createSignedCert("02", "client");
    createKeyStore("client");

    startVtgate();
    createClientConnection();
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

  private static void runProcess(final String command) throws IOException, InterruptedException {
    System.out.println("\nExecuting: " + command);
    int exitCode = new ProcessBuilder().inheritIO().command(command.split(" ")).start().waitFor();
    System.out.println("Exit code: " + exitCode + "\n");
  }

  private static void createCA() throws Exception {
    java.nio.file.Files.copy(
        GrpcClientTlsTest.class.getResourceAsStream("/ca.config"),
        Paths.get(caConfig)
    );

    final String createKey = String.format("openssl genrsa -out %s 2048", caKey);
    runProcess(createKey);

    final String createCert = String
        .format("openssl req -new -x509 -nodes -days 3600 -batch -config %s -key %s -out %s",
            caConfig, caKey, caCert);
    runProcess(createCert);
  }

  private static void createTrustStore() throws Exception {
    final String convertCaCert = String
        .format("openssl x509 -outform der -in %s -out %s", caCert, caCertDer);
    runProcess(convertCaCert);

    final String createTrustStore = String.format(
        "keytool -import -alias cacert -keystore %s -file %s -storepass passwd -trustcacerts -noprompt",
        trustStore, caCertDer);
    runProcess(createTrustStore);
  }

  private static void createSignedCert(final String serial, final String name) throws Exception {
    final String certConfig = certDirectory.getCanonicalPath() + File.separatorChar + "cert.config";
    if (!(new File(certConfig)).exists()) {
      java.nio.file.Files.copy(
          GrpcClientTlsTest.class.getResourceAsStream("/cert.config"),
          Paths.get(certConfig)
      );
    }
    final String key = certDirectory.getCanonicalPath() + File.separatorChar + name + "-key.pem";
    final String req = certDirectory.getCanonicalPath() + File.separatorChar + name + "-req.pem";
    final String cert = certDirectory.getCanonicalPath() + File.separatorChar + name + "-cert.pem";

    final String createKeyAndCSR = String.format(
        "openssl req -newkey rsa:2048 -days 3600 -nodes -batch -config %s -keyout %s -out %s",
        certConfig, key, req);
    runProcess(createKeyAndCSR);

    final String signKey = String
        .format("openssl x509 -req -in %s -days 3600 -CA %s -CAkey %s -set_serial %s -out %s", req,
            caCert, caKey, serial, cert);
    runProcess(signKey);
  }

  private static void createKeyStore(final String name) throws Exception {
    final String cert = certDirectory.getCanonicalPath() + File.separatorChar + name + "-cert.pem";
    final String key = certDirectory.getCanonicalPath() + File.separatorChar + name + "-key.pem";
    final String p12 = certDirectory.getCanonicalPath() + File.separatorChar + name + "-key.p12";
    final String keyStore =
        certDirectory.getCanonicalPath() + File.separatorChar + name + "-keyStore.jks";

    final String convertCert = String.format(
        "openssl pkcs12 -export -in %s -inkey %s -out %s -name cert -CAfile %s -caname root -passout pass:passwd",
        cert, key, p12, caCert);
    System.out.println(convertCert);
    new ProcessBuilder(convertCert.split(" ")).start().waitFor();

    final String createKeyStore = String.format(
        "keytool -importkeystore -deststorepass passwd -destkeystore %s -srckeystore %s -srcstoretype PKCS12 -alias cert -srcstorepass passwd",
        keyStore, p12);
    System.out.println(createKeyStore);
    new ProcessBuilder(createKeyStore.split(" ")).start().waitFor();
  }

  private static void startVtgate() throws Exception {
    final String vtRoot = System.getenv("VTROOT");
    if (vtRoot == null) {
      throw new RuntimeException("cannot find env variable VTROOT; make sure to source dev.env");
    }

    final ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    final String cert = certDirectory.getCanonicalPath() + File.separatorChar + "server-cert.pem";
    final String key = certDirectory.getCanonicalPath() + File.separatorChar + "server-key.pem";

    final String vtgateCommand = String.format(
        "%s -grpc_cert %s -grpc_key %s -grpc_ca %s -logtostderr -grpc_port %s -service_map grpc-vtgateservice",
        vtRoot + "/bin/vtgateclienttest", cert, key, caCert, Integer.toString(port));
    System.out.println(vtgateCommand);
    vtgateclienttest = new ProcessBuilder(vtgateCommand.split(" ")).inheritIO().start();
  }

  private static void createClientConnection() throws Exception {
    final String keyStore =
        certDirectory.getCanonicalPath() + File.separatorChar + "client-keyStore.jks";

    final TlsOptions tlsOptions = new TlsOptions()
        .keyStorePath(keyStore)
        .keyStorePassword("passwd")
        .keyAlias("cert")
        .trustStorePath(trustStore)
        .trustStorePassword("passwd")
        .trustAlias("cacert");

    client = new GrpcClientFactory()
        .createTls(
            Context.getDefault().withDeadlineAfter(Duration.millis(5000)),
            "localhost:" + port,
            tlsOptions
        );
  }
}
