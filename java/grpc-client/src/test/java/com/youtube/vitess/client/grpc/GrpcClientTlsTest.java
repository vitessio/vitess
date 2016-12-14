package com.youtube.vitess.client.grpc;

import com.google.common.io.Files;
import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.RpcClientTest;
import org.apache.commons.io.IOUtils;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Paths;

/**
 * This tests GrpcClient with a mock vtgate server (go/cmd/vtgateclienttest), over an SSL connection.
 *
 * The SSL setup is adapted from "test/encrypted_transport.py"
 *
 * TODO: Create separate tests for SSL with client auth
 */
@RunWith(JUnit4.class)
public class GrpcClientTlsTest extends RpcClientTest {

    private static Process vtgateclienttest;
    private static int port;
    private static File certDirectory;

    private static String caConfig;
    private static String caKey;
    private static String caCert;
    private static String caCertDer;
    private static String trustStore;

    private static String config;
    private static String key;
    private static String req;
    private static String cert;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        certDirectory = Files.createTempDir();
        System.out.println("Using cert directory: " + certDirectory.getCanonicalPath());

        caConfig = certDirectory.getCanonicalPath() + File.separatorChar + "ca.config";
        caKey = certDirectory.getCanonicalPath() + File.separatorChar + "ca-key.pem";
        caCert = certDirectory.getCanonicalPath() + File.separatorChar + "ca-cert.pem";
        caCertDer = certDirectory.getCanonicalPath() + File.separatorChar + "ca-cert.der";
        trustStore = certDirectory.getCanonicalPath() + File.separatorChar + "trustStore.jks";

        createCA();
//        createSignedCert("ca", "01", "vtgate-server", "vtgate server");
        createSignedCert("ca", "01", "vtgate-server", "localhost");

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

    private static void createCA() throws Exception {
        java.nio.file.Files.copy(
                GrpcClientTlsTest.class.getResourceAsStream("/ca.config"),
                Paths.get(caConfig)
        );

        final String createKey = String.format("openssl genrsa -out %s", caKey);
        new ProcessBuilder(createKey.split(" ")).start().waitFor();

        final String createCert = String.format("openssl req -new -x509 -nodes -days 3600 -batch -config %s -key %s -out %s", caConfig, caKey, caCert);
        new ProcessBuilder(createCert.split(" ")).start().waitFor();

        final String convertCaCert = String.format("openssl x509 -outform der -in %s -out %s", caCert, caCertDer);
        new ProcessBuilder(convertCaCert.split(" ")).start().waitFor();

        final String createTrustStore = String.format("keytool -import -alias cacert -keystore %s -file %s -storepass passwd -trustcacerts -noprompt", trustStore, caCertDer);
        new ProcessBuilder(createTrustStore.split(" ")).start().waitFor();
    }

    private static void createSignedCert(final String ca, final String serial, final String name, final String commonName) throws Exception {
        config = certDirectory.getCanonicalPath() + File.separatorChar + name + ".config";
        key = certDirectory.getCanonicalPath() + File.separatorChar + name + "-key.pem";
        req = certDirectory.getCanonicalPath() + File.separatorChar + name + "-req.pem";
        cert = certDirectory.getCanonicalPath() + File.separatorChar + name + "-cert.pem";

        final StringWriter writer = new StringWriter();
        IOUtils.copy(GrpcClientTlsTest.class.getResourceAsStream("/cert-template.config"), writer, "UTF-8");
        final String content = writer.toString();
//        content.replaceAll("%s", commonName);
        java.nio.file.Files.write(Paths.get(config), content.getBytes("UTF-8"));

        final String createKeyAndCSR = String.format("openssl req -newkey rsa:2048 -days 3600 -nodes -batch -config %s -keyout %s -out %s", config, key, req);
        new ProcessBuilder(createKeyAndCSR.split(" ")).start().waitFor();

//        final String askHarshit = String.format("openssl rsa -in %s -out %s", key, key);
//        new ProcessBuilder(askHarshit.split(" ")).start().waitFor();

        final String signKey = String.format("openssl x509 -req -in %s -days 3600 -CA %s -CAkey %s -set_serial %s -out %s", req, caCert, caKey, serial, cert);
        new ProcessBuilder(signKey.split(" ")).start().waitFor();
    }

    private static void startVtgate() throws Exception {
        String vtRoot = System.getenv("VTROOT");
        if (vtRoot == null) {
            throw new RuntimeException("cannot find env variable VTROOT; make sure to source dev.env");
        }

        ServerSocket socket = new ServerSocket(0);
        port = socket.getLocalPort();
        socket.close();

//        "-grpc_ca",
//        certDirectory.getCanonicalPath() + File.separatorChar + "ca-cert.pem",
        final String vtgate = String.format("%s -grpc_cert %s -grpc_key %s -logtostderr -grpc_port %s -service_map grpc-vtgateservice",
                vtRoot + "/bin/vtgateclienttest",
                certDirectory.getCanonicalPath() + File.separatorChar + "vtgate-server-cert.pem",
                certDirectory.getCanonicalPath() + File.separatorChar + "vtgate-server-key.pem",
                Integer.toString(port));
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
