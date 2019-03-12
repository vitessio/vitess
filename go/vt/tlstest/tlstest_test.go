/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tlstest

import (
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttls"
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
	CreateSignedCert(root, "servers", "01", "server-instance", "server.example.com")

	CreateSignedCert(root, CA, "02", "clients", "Clients CA")
	CreateSignedCert(root, "clients", "01", "client-instance", "Client Instance")
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-instance-cert.pem"),
		path.Join(root, "server-instance-key.pem"),
		path.Join(root, "clients-cert.pem"))
	if err != nil {
		t.Fatalf("TLSServerConfig failed: %v", err)
	}
	clientConfig, err := vttls.ClientConfig(
		path.Join(root, "client-instance-cert.pem"),
		path.Join(root, "client-instance-key.pem"),
		path.Join(root, "servers-cert.pem"),
		"server.example.com")
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
	// create a dialer with timeout
	dialer := new(net.Dialer)
	dialer.Timeout = 10 * time.Second

	wg := sync.WaitGroup{}

	//
	// Positive case: accept on server side, connect a client, send data.
	//
	var clientErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientConn, clientErr := tls.DialWithDialer(dialer, "tcp", addr, clientConfig)
		if clientErr == nil {
			clientConn.Write([]byte{42})
			clientConn.Close()
		}
	}()

	serverConn, err := listener.Accept()
	if clientErr != nil {
		t.Fatalf("Dial failed: %v", clientErr)
	}
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

	badClientConfig, err := vttls.ClientConfig(
		path.Join(root, "server-instance-cert.pem"),
		path.Join(root, "server-instance-key.pem"),
		path.Join(root, "servers-cert.pem"),
		"server.example.com")
	if err != nil {
		t.Fatalf("TLSClientConfig failed: %v", err)
	}

	var serverErr error
	wg.Add(1)
	go func() {
		// We expect the Accept to work, but the first read to fail.
		defer wg.Done()
		serverConn, serverErr := listener.Accept()
		// This will fail.
		if serverErr == nil {
			result := make([]byte, 1)
			if n, err := serverConn.Read(result); err == nil {
				fmt.Printf("Was able to read from server: %v\n", n)
			}
			serverConn.Close()
		}
	}()

	// When using TLS 1.2, the Dial will fail.
	// With TLS 1.3, the Dial will succeed and the first Read will fail.
	clientConn, err := tls.DialWithDialer(dialer, "tcp", addr, badClientConfig)
	if err != nil {
		if !strings.Contains(err.Error(), "bad certificate") {
			t.Errorf("Wrong error returned: %v", err)
		}
		return
	}
	wg.Wait()
	if serverErr != nil {
		t.Fatalf("Connection failed: %v", serverErr)
	}

	data := make([]byte, 1)
	_, err = clientConn.Read(data)
	if err == nil {
		t.Fatalf("Dial or first Read was expected to fail")
	}
	if !strings.Contains(err.Error(), "bad certificate") {
		t.Errorf("Wrong error returned: %v", err)
	}
}
