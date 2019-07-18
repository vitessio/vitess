package endtoend

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"path"
	"reflect"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vttls"
)

const clientCertUsername = "Client Cert"

func TestValidCert(t *testing.T) {
	th := &TestHandler{}

	authServer := &mysql.AuthServerClientCert{
		Method: mysql.MysqlClearPassword,
	}

	// Create the listener, so we can get its host.
	l, err := mysql.NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Create the certs.
	root, err := ioutil.TempDir("", "TestSSLConnection")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(root)
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", clientCertUsername)

	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"))
	if err != nil {
		t.Fatalf("TLSServerConfig failed: %v", err)
	}
	l.TLSConfig = serverConfig
	go func() {
		l.Accept()
	}()

	// Setup the right parameters.
	params := &mysql.ConnParams{
		Host:  host,
		Port:  port,
		Uname: clientCertUsername,
		Pass:  "",
		// SSL flags.
		Flags:      mysql.CapabilityClientSSL,
		SslCa:      path.Join(root, "ca-cert.pem"),
		SslCert:    path.Join(root, "client-cert.pem"),
		SslKey:     path.Join(root, "client-key.pem"),
		ServerName: "server.example.com",
	}

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, params)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()

	// Make sure this went through SSL.
	result, err := conn.ExecuteFetch("ssl echo", 10000, true)
	if err != nil {
		t.Fatalf("ExecuteFetch failed: %v", err)
	}
	if result.Rows[0][0].ToString() != "ON" {
		t.Errorf("Got wrong result from ExecuteFetch(ssl echo): %v", result)
	}

	userData := th.lastConn.UserData.Get()
	if userData.Username != clientCertUsername {
		t.Errorf("userdata username is %v, expected %v", userData.Username, clientCertUsername)
	}

	expectedGroups := []string{"localhost", "127.0.0.1", clientCertUsername}
	if !reflect.DeepEqual(userData.Groups, expectedGroups) {
		t.Errorf("userdata groups is %v, expected %v", userData.Groups, expectedGroups)
	}

	// Send a ComQuit to avoid the error message on the server side.
	conn.WriteComQuit()
}

func TestNoCert(t *testing.T) {
	th := &TestHandler{}

	authServer := &mysql.AuthServerClientCert{
		Method: mysql.MysqlClearPassword,
	}

	// Create the listener, so we can get its host.
	l, err := mysql.NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Create the certs.
	root, err := ioutil.TempDir("", "TestSSLConnection")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(root)
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")

	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"))
	if err != nil {
		t.Fatalf("TLSServerConfig failed: %v", err)
	}
	l.TLSConfig = serverConfig
	go func() {
		l.Accept()
	}()

	// Setup the right parameters.
	params := &mysql.ConnParams{
		Host:       host,
		Port:       port,
		Uname:      "user1",
		Pass:       "",
		Flags:      mysql.CapabilityClientSSL,
		SslCa:      path.Join(root, "ca-cert.pem"),
		ServerName: "server.example.com",
	}

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, params)
	if err == nil {
		t.Errorf("Connect() should have errored due to no client cert")
	}
	if conn != nil {
		conn.Close()
	}
}
