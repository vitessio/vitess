package com.youtube.vitess.client.grpc;

import com.google.common.io.Files;
import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.RpcClientTest;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Paths;

/**
 * This tests GrpcClient with a mock vtgate server (go/cmd/vtgateclienttest), over an SSL connection.
 *
 * The SSL setup is adapted from "test/encrypted_transport.py"
 */
@RunWith(JUnit4.class)
public class GrpcClientTlsTest extends RpcClientTest {

    protected static Process vtgateclienttest;
    protected static int port;
    protected static File certDirectory;

    protected static String caConfig;
    protected static String caKey;
    protected static String caCert;
    protected static String caCertDer;
    protected static String trustStore;

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
        }
    }

    protected static void createCA() throws Exception {
        java.nio.file.Files.copy(
                GrpcClientTlsTest.class.getResourceAsStream("/ca.config"),
                Paths.get(caConfig)
        );

        final String createKey = String.format("openssl genrsa -out %s", caKey);
        System.out.println(createKey);
        new ProcessBuilder(createKey.split(" ")).start().waitFor();

        final String createCert = String.format("openssl req -new -x509 -nodes -days 3600 -batch -config %s -key %s -out %s", caConfig, caKey, caCert);
        System.out.println(createCert);
        new ProcessBuilder(createCert.split(" ")).start().waitFor();
    }

    protected static void createTrustStore() throws Exception {
        final String convertCaCert = String.format("openssl x509 -outform der -in %s -out %s", caCert, caCertDer);
        System.out.println(convertCaCert);
        new ProcessBuilder(convertCaCert.split(" ")).start().waitFor();

        final String createTrustStore = String.format("keytool -import -alias cacert -keystore %s -file %s -storepass passwd -trustcacerts -noprompt", trustStore, caCertDer);
        System.out.println(createTrustStore);
        new ProcessBuilder(createTrustStore.split(" ")).start().waitFor();
    }

    protected static void createSignedCert(final String serial, final String name) throws Exception {
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

        final String createKeyAndCSR = String.format("openssl req -newkey rsa:2048 -days 3600 -nodes -batch -config %s -keyout %s -out %s", certConfig, key, req);
        System.out.println(createKeyAndCSR);
        new ProcessBuilder(createKeyAndCSR.split(" ")).start().waitFor();

        final String signKey = String.format("openssl x509 -req -in %s -days 3600 -CA %s -CAkey %s -set_serial %s -out %s", req, caCert, caKey, serial, cert);
        System.out.println(signKey);
        new ProcessBuilder(signKey.split(" ")).start().waitFor();
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

        final String vtgate = String.format("%s -grpc_cert %s -grpc_key %s -logtostderr -grpc_port %s -service_map grpc-vtgateservice",
                vtRoot + "/bin/vtgateclienttest", cert, key, Integer.toString(port));
        System.out.println(vtgate);
        vtgateclienttest = new ProcessBuilder(vtgate.split(" ")).start();
    }

    private static void createClientConnection() throws Exception {
        final GrpcClientFactory.TlsOptions tlsOptions = new GrpcClientFactory.TlsOptions()
                .trustStorePath(trustStore)
                .trustStorePassword("passwd")
                .trustAlias("cacert");

        client = new GrpcClientFactory()
                        .createTls(
                                Context.getDefault().withDeadlineAfter(Duration.millis(5000)),
                                new InetSocketAddress("localhost", port),
                                tlsOptions
                        );
    }

}
