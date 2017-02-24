package tlstest

import (
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
)

// TestClientServer generates:
// - a root CA
// - a server intermediate CA, with a server.
// - a client intermediate CA, with a client.
// And then performs a few tests on them.
func TestClientServer(t *testing.T) {
	// Our test root.
	root, err := ioutil.TempDir("", "tlstest")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(root)

	// Create the certs and configs.
	CreateCA(root)

	CreateSignedCert(root, CA, "01", "servers", "Servers CA")
	CreateSignedCert(root, "servers", "01", "server-instance", "Server Instance")

	CreateSignedCert(root, CA, "02", "clients", "Clients CA")
	CreateSignedCert(root, "clients", "01", "client-instance", "Client Instance")
	serverConfig, err := grpcutils.TLSServerConfig(
		path.Join(root, "server-instance-cert.pem"),
		path.Join(root, "server-instance-key.pem"),
		path.Join(root, "clients-cert.pem"))
	if err != nil {
		t.Fatalf("TLSServerConfig failed: %v", err)
	}
	clientConfig, err := grpcutils.TLSClientConfig(
		path.Join(root, "client-instance-cert.pem"),
		path.Join(root, "client-instance-key.pem"),
		path.Join(root, "servers-cert.pem"),
		"Server Instance")
	if err != nil {
		t.Fatalf("TLSClientConfig failed: %v", err)
	}

	// Create a TLS server listener.
	listener, err := tls.Listen("tcp", ":0", serverConfig)
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	addr := listener.Addr().String()
	defer listener.Close()

	wg := sync.WaitGroup{}

	//
	// Positive case: accept on server side, connect a client, send data.
	//

	wg.Add(1)
	go func() {
		defer wg.Done()
		clientConn, err := tls.Dial("tcp", addr, clientConfig)
		if err != nil {
			t.Fatalf("Dial failed: %v", err)
		}

		clientConn.Write([]byte{42})
		clientConn.Close()
	}()

	serverConn, err := listener.Accept()
	if err != nil {
		t.Fatalf("Accept failed: %v", err)
	}

	result := make([]byte, 1)
	if n, err := serverConn.Read(result); (err != nil && err != io.EOF) || n != 1 {
		t.Fatalf("Read failed: %v %v", n, err)
	}
	if result[0] != 42 {
		t.Fatalf("Read returned wrong result: %v", result)
	}
	serverConn.Close()

	wg.Wait()

	//
	// Negative case: connect a client with wrong cert (using the
	// server cert on the client side).
	//

	badClientConfig, err := grpcutils.TLSClientConfig(
		path.Join(root, "server-instance-cert.pem"),
		path.Join(root, "server-instance-key.pem"),
		path.Join(root, "servers-cert.pem"),
		"Server Instance")
	if err != nil {
		t.Fatalf("TLSClientConfig failed: %v", err)
	}

	wg.Add(1)
	go func() {
		// We expect the Accept to work, but the first read to fail.
		defer wg.Done()
		serverConn, err := listener.Accept()
		if err != nil {
			t.Fatalf("Connection failed: %v", err)
		}

		// This will fail.
		result := make([]byte, 1)
		if n, err := serverConn.Read(result); err == nil {
			fmt.Printf("Was able to read from server: %v\n", n)
		}
		serverConn.Close()
	}()

	if _, err = tls.Dial("tcp", addr, badClientConfig); err == nil {
		t.Fatalf("Dial was expected to fail")
	}
	if !strings.Contains(err.Error(), "bad certificate") {
		t.Errorf("Wrong error returned: %v", err)
	}
	t.Logf("Dial returned: %v", err)
}
