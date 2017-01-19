package com.youtube.vitess.client.grpc;

import com.google.common.io.Files;
import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.grpc.tls.TlsOptions;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

/**
 * This tests GrpcClient with a mock vtgate server (go/cmd/vtgateclienttest), over an SSL connection with client
 * authentication enabled.
 */
@RunWith(JUnit4.class)
public class GrpcClientTlsClientAuthTest extends GrpcClientTlsTest {

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
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
    }

    private static void createKeyStore(final String name) throws Exception {
        final String cert = certDirectory.getCanonicalPath() + File.separatorChar + name + "-cert.pem";
        final String key = certDirectory.getCanonicalPath() + File.separatorChar + name + "-key.pem";
        final String p12 = certDirectory.getCanonicalPath() + File.separatorChar + name + "-key.p12";
        final String keyStore = certDirectory.getCanonicalPath() + File.separatorChar + name + "-keyStore.jks";

        final String convertCert = String.format("openssl pkcs12 -export -in %s -inkey %s -out %s -name cert -CAfile %s -caname root -passout pass:passwd", cert, key, p12, caCert);
        System.out.println(convertCert);
        new ProcessBuilder(convertCert.split(" ")).start().waitFor();

        final String createKeyStore = String.format("keytool -importkeystore -deststorepass passwd -destkeystore %s -srckeystore %s -srcstoretype PKCS12 -alias cert -srcstorepass passwd", keyStore, p12);
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

        final String vtgateCommand = String.format("%s -grpc_cert %s -grpc_key %s -grpc_ca %s -logtostderr -grpc_port %s -service_map grpc-vtgateservice",
                vtRoot + "/bin/vtgateclienttest", cert, key, caCert, Integer.toString(port));
        System.out.println(vtgateCommand);
        vtgateclienttest = new ProcessBuilder(vtgateCommand.split(" ")).inheritIO().start();
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
    }

    private static void createClientConnection() throws Exception {
        final String keyStore = certDirectory.getCanonicalPath() + File.separatorChar + "client-keyStore.jks";

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
                        new InetSocketAddress("localhost", port),
                        tlsOptions
                );
    }
}
