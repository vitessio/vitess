/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tlstest

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vttls"
)

func TestClientServerWithoutCombineCerts(t *testing.T) {
	testClientServer(t, false)
}

func TestClientServerWithCombineCerts(t *testing.T) {
	testClientServer(t, true)
}

// testClientServer generates:
// - a root CA
// - a server intermediate CA, with a server.
// - a client intermediate CA, with a client.
// And then performs a few tests on them.
func testClientServer(t *testing.T, combineCerts bool) {
	// Our test root.
	root, err := ioutil.TempDir("", "tlstest")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(root)

	clientServerKeyPairs := CreateClientServerCertPairs(root)
	serverCA := ""

	if combineCerts {
		serverCA = clientServerKeyPairs.ServerCA
	}

	serverConfig, err := vttls.ServerConfig(
		clientServerKeyPairs.ServerCert,
		clientServerKeyPairs.ServerKey,
		clientServerKeyPairs.ClientCA,
		serverCA)
	if err != nil {
		t.Fatalf("TLSServerConfig failed: %v", err)
	}
	clientConfig, err := vttls.ClientConfig(
		clientServerKeyPairs.ClientCert,
		clientServerKeyPairs.ClientKey,
		clientServerKeyPairs.ServerCA,
		clientServerKeyPairs.ServerName)
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
		clientServerKeyPairs.ServerCert,
		clientServerKeyPairs.ServerKey,
		clientServerKeyPairs.ServerCA,
		clientServerKeyPairs.ServerName)
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

func getServerConfigWithoutCombinedCerts(keypairs ClientServerKeyPairs) (*tls.Config, error) {
	return vttls.ServerConfig(
		keypairs.ServerCert,
		keypairs.ServerKey,
		keypairs.ClientCA,
		"")
}

func getServerConfigWithCombinedCerts(keypairs ClientServerKeyPairs) (*tls.Config, error) {
	return vttls.ServerConfig(
		keypairs.ServerCert,
		keypairs.ServerKey,
		keypairs.ClientCA,
		keypairs.ServerCA)
}

func getClientConfig(keypairs ClientServerKeyPairs) (*tls.Config, error) {
	return vttls.ClientConfig(
		keypairs.ClientCert,
		keypairs.ClientKey,
		keypairs.ServerCA,
		keypairs.ServerName)
}

func testServerTLSConfigCaching(t *testing.T, getServerConfig func(ClientServerKeyPairs) (*tls.Config, error)) {
	testConfigGeneration(t, "servertlstest", getServerConfig, func(config *tls.Config) *x509.CertPool {
		return config.ClientCAs
	})
}

func TestServerTLSConfigCachingWithoutCombinedCerts(t *testing.T) {
	testServerTLSConfigCaching(t, getServerConfigWithoutCombinedCerts)
}

func TestServerTLSConfigCachingWithCombinedCerts(t *testing.T) {
	testServerTLSConfigCaching(t, getServerConfigWithCombinedCerts)
}

func TestClientTLSConfigCaching(t *testing.T) {
	testConfigGeneration(t, "clienttlstest", getClientConfig, func(config *tls.Config) *x509.CertPool {
		return config.RootCAs
	})
}

func testConfigGeneration(t *testing.T, rootPrefix string, generateConfig func(ClientServerKeyPairs) (*tls.Config, error), getCertPool func(tlsConfig *tls.Config) *x509.CertPool) {
	// Our test root.
	root, err := ioutil.TempDir("", rootPrefix)
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(root)

	const configsToGenerate = 1

	firstClientServerKeyPairs := CreateClientServerCertPairs(root)
	secondClientServerKeyPairs := CreateClientServerCertPairs(root)

	firstExpectedConfig, _ := generateConfig(firstClientServerKeyPairs)
	secondExpectedConfig, _ := generateConfig(secondClientServerKeyPairs)
	firstConfigChannel := make(chan *tls.Config, configsToGenerate)
	secondConfigChannel := make(chan *tls.Config, configsToGenerate)

	var configCounter = 0

	for i := 1; i <= configsToGenerate; i++ {
		go func() {
			firstConfig, _ := generateConfig(firstClientServerKeyPairs)
			firstConfigChannel <- firstConfig
			secondConfig, _ := generateConfig(secondClientServerKeyPairs)
			secondConfigChannel <- secondConfig
		}()
	}

	for {
		select {
		case firstConfig := <-firstConfigChannel:
			assert.Equal(t, &firstExpectedConfig.Certificates, &firstConfig.Certificates)
			assert.Equal(t, getCertPool(firstExpectedConfig), getCertPool(firstConfig))
		case secondConfig := <-secondConfigChannel:
			assert.Equal(t, &secondExpectedConfig.Certificates, &secondConfig.Certificates)
			assert.Equal(t, getCertPool(secondExpectedConfig), getCertPool(secondConfig))
		}
		configCounter = configCounter + 1

		if configCounter >= 2*configsToGenerate {
			break
		}
	}

}

func testNumberOfCertsWithOrWithoutCombining(t *testing.T, numCertsExpected int, combine bool) {
	// Our test root.
	root, err := ioutil.TempDir("", "tlstest")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(root)

	clientServerKeyPairs := CreateClientServerCertPairs(root)
	serverCA := ""
	if combine {
		serverCA = clientServerKeyPairs.ServerCA
	}

	serverConfig, err := vttls.ServerConfig(
		clientServerKeyPairs.ServerCert,
		clientServerKeyPairs.ServerKey,
		clientServerKeyPairs.ClientCA,
		serverCA)

	if err != nil {
		t.Fatalf("TLSServerConfig failed: %v", err)
	}
	assert.Equal(t, numCertsExpected, len(serverConfig.Certificates[0].Certificate))
}

func TestNumberOfCertsWithoutCombining(t *testing.T) {
	testNumberOfCertsWithOrWithoutCombining(t, 1, false)
}

func TestNumberOfCertsWithCombining(t *testing.T) {
	testNumberOfCertsWithOrWithoutCombining(t, 2, true)
}
