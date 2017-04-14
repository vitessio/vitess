package mysqlconn

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
	"github.com/youtube/vitess/go/vt/tlstest"
)

// This file tests the handshake scenarios between our client and our server.

func TestClearTextClientAuth(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Method = MysqlClearPassword
	authServer.Entries["user1"] = &AuthServerStaticEntry{
		Password: "password1",
	}

	// Create the listener.
	l, err := NewListener("tcp", ":0", authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port
	go func() {
		l.Accept()
	}()

	// Setup the right parameters.
	params := &sqldb.ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	// Connection should fail, as server requires SSL for clear text auth.
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err == nil || !strings.Contains(err.Error(), "Cannot use clear text authentication over non-SSL connections") {
		t.Fatalf("unexpected connection error: %v", err)
	}

	// Change server side to allow clear text without auth.
	l.AllowClearTextWithoutTLS = true
	conn, err = Connect(ctx, params)
	if err != nil {
		t.Fatalf("unexpected connection error: %v", err)
	}
	defer conn.Close()

	// Run a 'select rows' command with results.
	result, err := conn.ExecuteFetch("select rows", 10000, true)
	if err != nil {
		t.Fatalf("ExecuteFetch failed: %v", err)
	}
	if !reflect.DeepEqual(result, selectRowsResult) {
		t.Errorf("Got wrong result from ExecuteFetch(select rows): %v", result)
	}

	// Send a ComQuit to avoid the error message on the server side.
	conn.writeComQuit()
}

// TestSSLConnection creates a server with TLS support, a client that
// also has SSL support, and connects them.
func TestSSLConnection(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = &AuthServerStaticEntry{
		Password: "password1",
	}

	// Create the listener, so we can get its host.
	l, err := NewListener("tcp", ":0", authServer, th)
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
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "IP:"+host)
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")

	// Create the server with TLS config.
	serverConfig, err := grpcutils.TLSServerConfig(
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
	params := &sqldb.ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
		// SSL flags.
		Flags:   CapabilityClientSSL,
		SslCa:   path.Join(root, "ca-cert.pem"),
		SslCert: path.Join(root, "client-cert.pem"),
		SslKey:  path.Join(root, "client-key.pem"),
	}

	t.Run("Basics", func(t *testing.T) {
		testSSLConnectionBasics(t, params)
	})

	// Make sure clear text auth works over SSL.
	t.Run("ClearText", func(t *testing.T) {
		authServer.Method = MysqlClearPassword
		testSSLConnectionClearText(t, params)
	})
}

func testSSLConnectionClearText(t *testing.T, params *sqldb.ConnParams) {
	// Create a client connection, connect.
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()
	if conn.User != "user1" {
		t.Errorf("Invalid conn.User, got %v was expecting user1", conn.User)
	}

	// Make sure this went through SSL.
	result, err := conn.ExecuteFetch("ssl echo", 10000, true)
	if err != nil {
		t.Fatalf("ExecuteFetch failed: %v", err)
	}
	if result.Rows[0][0].String() != "ON" {
		t.Errorf("Got wrong result from ExecuteFetch(ssl echo): %v", result)
	}

	// Send a ComQuit to avoid the error message on the server side.
	conn.writeComQuit()
}

func testSSLConnectionBasics(t *testing.T, params *sqldb.ConnParams) {
	// Create a client connection, connect.
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()
	if conn.User != "user1" {
		t.Errorf("Invalid conn.User, got %v was expecting user1", conn.User)
	}

	// Run a 'select rows' command with results.
	result, err := conn.ExecuteFetch("select rows", 10000, true)
	if err != nil {
		t.Fatalf("ExecuteFetch failed: %v", err)
	}
	if !reflect.DeepEqual(result, selectRowsResult) {
		t.Errorf("Got wrong result from ExecuteFetch(select rows): %v", result)
	}

	// Make sure this went through SSL.
	result, err = conn.ExecuteFetch("ssl echo", 10000, true)
	if err != nil {
		t.Fatalf("ExecuteFetch failed: %v", err)
	}
	if result.Rows[0][0].String() != "ON" {
		t.Errorf("Got wrong result from ExecuteFetch(ssl echo): %v", result)
	}

	// Send a ComQuit to avoid the error message on the server side.
	conn.writeComQuit()
}
