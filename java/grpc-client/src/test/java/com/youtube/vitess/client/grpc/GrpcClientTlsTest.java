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
import java.util.Arrays;

/**
 * This tests GrpcClient with a mock vtgate server (go/cmd/vtgateclienttest), over an SSL connection.
 *
 * The SSL setup is adapted from "test/encrypted_transport.py"
 */
@RunWith(JUnit4.class)
public class GrpcClientTlsTest extends RpcClientTest {

    private static File certDirectory;
    private static Process vtgateclienttest;
    private static int port;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        certDirectory = Files.createTempDir();
        System.out.println("Using cert directory: " + certDirectory.getCanonicalPath());

        createCA();
        createSignedCert("ca", "03", "vtgate-server", "vtgate server CA");
        createSignedCert("ca", "04", "vtgate-client", "vtgate client CA");

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
                Paths.get(certDirectory.toString() + File.separatorChar + "ca.config")
        );

        final Process createKey = new ProcessBuilder(
                // certDirectory.getCanonicalPath() + File.separatorChar + "ca-key.pem")
                Arrays.asList(
                        "openssl",
                        "genrsa",
                        "-out",
                        certDirectory.getCanonicalPath() + File.separatorChar + "ca-key.pem")
        ).start();
        createKey.waitFor();

        final Process createCert = new ProcessBuilder(
                // openssl req -new -x509 -nodes -days 3600 -batch -config ca.config -key ca-key.pem -out ca-cert.pem
                Arrays.asList(
                        "openssl",
                        "req",
                        "-new",
                        "-x509",
                        "-nodes",
                        "-days",
                        "3600",
                        "-batch",
                        "-config",
                        certDirectory.getCanonicalPath() + File.separatorChar + "ca.config",
                        "-key",
                        certDirectory.getCanonicalPath() + File.separatorChar + "ca-key.pem",
                        "-out",
                        certDirectory.getCanonicalPath() + File.separatorChar + "ca-cert.pem")
        ).start();
        createCert.waitFor();

        // TODO: Create JKS trustStore
    }

    private static void createSignedCert(final String ca, final String serial, final String name, final String commonName) throws Exception {
        final String config = certDirectory.getCanonicalPath() + File.separatorChar + name + ".config";
        final String key = certDirectory.getCanonicalPath() + File.separatorChar + name + "-key.pem";
        final String req = certDirectory.getCanonicalPath() + File.separatorChar + name + "-req.pem";
        final String cert = certDirectory.getCanonicalPath() + File.separatorChar + name + "-cert.pem";
        final String caCert = certDirectory.getCanonicalPath() + File.separatorChar + ca + "-cert.pem";
        final String caKey = certDirectory.getCanonicalPath() + File.separatorChar + ca + "-key.pem";

        final StringWriter writer = new StringWriter();
        IOUtils.copy(GrpcClientTlsTest.class.getResourceAsStream("/cert-template.config"), writer, "UTF-8");
        final String content = writer.toString();
        content.replaceAll("%s", commonName);
        java.nio.file.Files.write(Paths.get(config), content.getBytes("UTF-8"));

        final Process createKeyAndCSR = new ProcessBuilder(
                // openssl req -newkey rsa:2048 -days 3600 -nodes -batch -config cert-template.config -keyout vtgate-client-key.pem -out vtgate-client-req.pem
                Arrays.asList(
                        "openssl",
                        "req",
                        "-newkey",
                        "rsa:2048",
                        "-days",
                        "3600",
                        "-nodes",
                        "-batch",
                        "-config",
                        config,
                        "-keyout",
                        key,
                        "-out",
                        req)
        ).start();
        createKeyAndCSR.waitFor();

        final Process askHarshit = new ProcessBuilder(
                // openssl rsa -in vtgate-client-key.pem -out vtgate-client-key.pem
                Arrays.asList(
                        "openssl",
                        "rsa",
                        "-in",
                        key,
                        "-out",
                        key)
        ).start();
        askHarshit.waitFor();

        final Process signKey = new ProcessBuilder(
                // openssl x509 -req -in vtgate-client-req.pem -days 3600 -CA ca-cert.pem -CAkey ca-key.pem -set_serial 04 -out vtgate-client-cert.pem
                Arrays.asList(
                        "openssl",
                        "x509",
                        "-req",
                        "-in",
                        req,
                        "-days",
                        "3600",
                        "-CA",
                        caCert,
                        "-CAkey",
                        caKey,
                        "-set_serial",
                        serial,
                        "-out",
                        cert)
        ).start();
        signKey.waitFor();
    }

    private static void startVtgate() throws Exception {
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

// TODO: Uncomment the following lines once the corresponding client-side changes are added to "createClientConnection"
//                                "-grpc_cert",
//                                certDirectory.getCanonicalPath() + File.separatorChar + "vtgate-server-cert.pem",
//                                "-grpc_key",
//                                certDirectory.getCanonicalPath() + File.separatorChar + "vtgate-server-key.pem",
//                                "-grpc_ca",
//                                certDirectory.getCanonicalPath() + File.separatorChar + "ca-cert.pem",

                                "-logtostderr",
                                "-grpc_port",
                                Integer.toString(port),
                                "-service_map",
                                "grpc-vtgateservice"))
                        .start();
    }

    private static void createClientConnection() {
        // TODO: Write code to build a JKS keyStore and trustStore from the PEM files that have been created
        // TODO: Swap the "create(...)" call below with "createTls(...)"
        client =
                new GrpcClientFactory()
                        .create(
                                Context.getDefault().withDeadlineAfter(Duration.millis(5000)),
                                new InetSocketAddress("localhost", port));
    }

}
