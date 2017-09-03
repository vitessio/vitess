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

package mysql

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	vtenv "github.com/youtube/vitess/go/vt/env"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
	"github.com/youtube/vitess/go/vt/tlstest"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var selectRowsResult = &sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "id",
			Type: querypb.Type_INT32,
		},
		{
			Name: "name",
			Type: querypb.Type_VARCHAR,
		},
	},
	Rows: [][]sqltypes.Value{
		{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
		},
		{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("20")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nicer name")),
		},
	},
	RowsAffected: 2,
}

type testHandler struct {
	lastConn *Conn
}

func (th *testHandler) NewConnection(c *Conn) {
	th.lastConn = c
}

func (th *testHandler) ConnectionClosed(c *Conn) {
}

func (th *testHandler) ComQuery(c *Conn, q []byte, callback func(*sqltypes.Result) error) error {
	switch query := string(q); query {
	case "error":
		return NewSQLError(ERUnknownComError, SSUnknownComError, "forced query handling error for: %v", query)
	case "panic":
		panic("test panic attack!")
	case "select rows":
		callback(selectRowsResult)
	case "error after send":
		callback(selectRowsResult)
		return NewSQLError(ERUnknownComError, SSUnknownComError, "forced query handling error for: %v", query)
	case "insert":
		callback(&sqltypes.Result{
			RowsAffected: 123,
			InsertID:     123456789,
		})
	case "schema echo":
		callback(&sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name: "schema_name",
					Type: querypb.Type_VARCHAR,
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(c.SchemaName)),
				},
			},
		})
	case "ssl echo":
		value := "OFF"
		if c.Capabilities&CapabilityClientSSL > 0 {
			value = "ON"
		}
		callback(&sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name: "ssl_flag",
					Type: querypb.Type_VARCHAR,
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(value)),
				},
			},
		})
	case "userData echo":
		callback(&sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name: "user",
					Type: querypb.Type_VARCHAR,
				},
				{
					Name: "user_data",
					Type: querypb.Type_VARCHAR,
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(c.User)),
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(c.UserData.Get().Username)),
				},
			},
		})
	default:
		callback(&sqltypes.Result{})
	}
	return nil
}

func TestConnectionWithoutSourceHost(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	l, err := NewListener("tcp", ":0", authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go func() {
		l.Accept()
	}()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	c, err := Connect(context.Background(), params)
	if err != nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}
	c.Close()
}

func TestConnectionWithSourceHost(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()

	authServer.Entries["user1"] = []*AuthServerStaticEntry{
		{
			Password:   "password1",
			UserData:   "userData1",
			SourceHost: "localhost",
		},
	}

	l, err := NewListener("tcp", ":0", authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go func() {
		l.Accept()
	}()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	_, err = Connect(context.Background(), params)
	// target is localhost, should not work from tcp connection
	if err == nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}
}

func TestConnectionUnixSocket(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()

	authServer.Entries["user1"] = []*AuthServerStaticEntry{
		{
			Password:   "password1",
			UserData:   "userData1",
			SourceHost: "localhost",
		},
	}

	unixSocket := "/tmp/mysql_vitess_test.sock"

	l, err := NewListener("unix", unixSocket, authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go func() {
		l.Accept()
	}()

	// Setup the right parameters.
	params := &ConnParams{
		UnixSocket: unixSocket,
		Uname:      "user1",
		Pass:       "password1",
	}

	c, err := Connect(context.Background(), params)
	if err != nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}
	c.Close()
}

func TestClientFoundRows(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	l, err := NewListener("tcp", ":0", authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go func() {
		l.Accept()
	}()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	// Test without flag.
	c, err := Connect(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}
	foundRows := th.lastConn.Capabilities & CapabilityClientFoundRows
	if foundRows != 0 {
		t.Errorf("FoundRows flag: %x, second bit must be 0", th.lastConn.Capabilities)
	}
	c.Close()
	if !c.IsClosed() {
		t.Errorf("IsClosed returned true on Close-d connection.")
	}

	// Test with flag.
	params.Flags |= CapabilityClientFoundRows
	c, err = Connect(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}
	foundRows = th.lastConn.Capabilities & CapabilityClientFoundRows
	if foundRows == 0 {
		t.Errorf("FoundRows flag: %x, second bit must be set", th.lastConn.Capabilities)
	}
	c.Close()
}

func TestServer(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	l, err := NewListener("tcp", ":0", authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go func() {
		l.Accept()
	}()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	initialTimingCounts := timings.Counts()
	initialConnAccept := connAccept.Get()
	initialConnSlow := connSlow.Get()

	l.SlowConnectWarnThreshold = time.Duration(time.Nanosecond * 1)

	// Run an 'error' command.
	output, ok := runMysql(t, params, "error")
	if ok {
		t.Fatalf("mysql should have failed: %v", output)
	}
	if !strings.Contains(output, "ERROR 1047 (08S01)") ||
		!strings.Contains(output, "forced query handling error for") {
		t.Errorf("Unexpected output for 'error'")
	}
	if connCount.Get() != 0 {
		t.Errorf("Expected ConnCount=0, got %d", connCount.Get())
	}
	if connAccept.Get()-initialConnAccept != 1 {
		t.Errorf("Expected ConnAccept delta=1, got %d", connAccept.Get()-initialConnAccept)
	}
	if connSlow.Get()-initialConnSlow != 1 {
		t.Errorf("Expected ConnSlow delta=1, got %d", connSlow.Get()-initialConnSlow)
	}

	expectedTimingDeltas := map[string]int64{
		"All":            2,
		connectTimingKey: 1,
		queryTimingKey:   1,
	}
	gotTimingCounts := timings.Counts()
	for key, got := range gotTimingCounts {
		expected := expectedTimingDeltas[key]
		delta := got - initialTimingCounts[key]
		if delta < expected {
			t.Errorf("Expected Timing count delta %s should be >= %d, got %d", key, expected, delta)
		}
	}

	// Set the slow connect threshold to something high that we don't expect to trigger
	l.SlowConnectWarnThreshold = time.Duration(time.Second * 1)

	// Run a 'panic' command, other side should panic, recover and
	// close the connection.
	output, ok = runMysql(t, params, "panic")
	if ok {
		t.Fatalf("mysql should have failed: %v", output)
	}
	if !strings.Contains(output, "ERROR 2013 (HY000)") ||
		!strings.Contains(output, "Lost connection to MySQL server during query") {
		t.Errorf("Unexpected output for 'panic'")
	}
	if connCount.Get() != 0 {
		t.Errorf("Expected ConnCount=0, got %d", connCount.Get())
	}
	if connAccept.Get()-initialConnAccept != 2 {
		t.Errorf("Expected ConnAccept delta=2, got %d", connAccept.Get()-initialConnAccept)
	}
	if connSlow.Get()-initialConnSlow != 1 {
		t.Errorf("Expected ConnSlow delta=1, got %d", connSlow.Get()-initialConnSlow)
	}

	// Run a 'select rows' command with results.
	output, ok = runMysql(t, params, "select rows")
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, "nice name") ||
		!strings.Contains(output, "nicer name") ||
		!strings.Contains(output, "2 rows in set") {
		t.Errorf("Unexpected output for 'select rows'")
	}

	// If there's an error after streaming has started,
	// we should get a 2013
	output, ok = runMysql(t, params, "error after send")
	if ok {
		t.Fatalf("mysql should have failed: %v", output)
	}
	if !strings.Contains(output, "ERROR 2013 (HY000)") ||
		!strings.Contains(output, "Lost connection to MySQL server during query") {
		t.Errorf("Unexpected output for 'panic'")
	}

	// Run an 'insert' command, no rows, but rows affected.
	output, ok = runMysql(t, params, "insert")
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, "Query OK, 123 rows affected") {
		t.Errorf("Unexpected output for 'insert'")
	}

	// Run a 'schema echo' command, to make sure db name is right.
	params.DbName = "XXXfancyXXX"
	output, ok = runMysql(t, params, "schema echo")
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, params.DbName) {
		t.Errorf("Unexpected output for 'schema echo'")
	}

	// Sanity check: make sure this didn't go through SSL
	output, ok = runMysql(t, params, "ssl echo")
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, "ssl_flag") ||
		!strings.Contains(output, "OFF") ||
		!strings.Contains(output, "1 row in set") {
		t.Errorf("Unexpected output for 'ssl echo': %v", output)
	}

	// UserData check: checks the server user data is correct.
	output, ok = runMysql(t, params, "userData echo")
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, "user1") ||
		!strings.Contains(output, "user_data") ||
		!strings.Contains(output, "userData1") {
		t.Errorf("Unexpected output for 'userData echo': %v", output)
	}

	// Permissions check: check a bad password is rejected.
	params.Pass = "bad"
	output, ok = runMysql(t, params, "select rows")
	if ok {
		t.Fatalf("mysql should have failed: %v", output)
	}
	if !strings.Contains(output, "1045") ||
		!strings.Contains(output, "28000") ||
		!strings.Contains(output, "Access denied") {
		t.Errorf("Unexpected output for invalid password: %v", output)
	}

	// Permissions check: check an unknown user is rejected.
	params.Pass = "password1"
	params.Uname = "user2"
	output, ok = runMysql(t, params, "select rows")
	if ok {
		t.Fatalf("mysql should have failed: %v", output)
	}
	if !strings.Contains(output, "1045") ||
		!strings.Contains(output, "28000") ||
		!strings.Contains(output, "Access denied") {
		t.Errorf("Unexpected output for invalid password: %v", output)
	}

	// Uncomment to leave setup up for a while, to run tests manually.
	//	fmt.Printf("Listening to server on host '%v' port '%v'.\n", host, port)
	//	time.Sleep(60 * time.Minute)
}

// TestClearTextServer creates a Server that needs clear text
// passwords from the client.
func TestClearTextServer(t *testing.T) {
	// If the database we're using is MariaDB, the client
	// is also the MariaDB client, that does support
	// clear text by default.
	isMariaDB := os.Getenv("MYSQL_FLAVOR") == "MariaDB"

	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	authServer.Method = MysqlClearPassword
	l, err := NewListener("tcp", ":0", authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go func() {
		l.Accept()
	}()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	// Run a 'select rows' command with results.  This should fail
	// as clear text is not enabled by default on the client
	// (except MariaDB).
	l.AllowClearTextWithoutTLS = true
	sql := "select rows"
	output, ok := runMysql(t, params, sql)
	if ok {
		if isMariaDB {
			t.Logf("mysql should have failed but returned: %v\nbut letting it go on MariaDB", output)
		} else {
			t.Fatalf("mysql should have failed but returned: %v", output)
		}
	} else {
		if strings.Contains(output, "No such file or directory") {
			t.Logf("skipping mysql clear text tests, as the clear text plugin cannot be loaded: %v", err)
			return
		}
		if !strings.Contains(output, "plugin not enabled") {
			t.Errorf("Unexpected output for 'select rows': %v", output)
		}
	}

	// Now enable clear text plugin in client, but server requires SSL.
	l.AllowClearTextWithoutTLS = false
	if !isMariaDB {
		sql = enableCleartextPluginPrefix + sql
	}
	output, ok = runMysql(t, params, sql)
	if ok {
		t.Fatalf("mysql should have failed but returned: %v", output)
	}
	if !strings.Contains(output, "Cannot use clear text authentication over non-SSL connections") {
		t.Errorf("Unexpected output for 'select rows': %v", output)
	}

	// Now enable clear text plugin, it should now work.
	l.AllowClearTextWithoutTLS = true
	output, ok = runMysql(t, params, sql)
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, "nice name") ||
		!strings.Contains(output, "nicer name") ||
		!strings.Contains(output, "2 rows in set") {
		t.Errorf("Unexpected output for 'select rows'")
	}

	// Change password, make sure server rejects us.
	params.Pass = "bad"
	output, ok = runMysql(t, params, sql)
	if ok {
		t.Fatalf("mysql should have failed but returned: %v", output)
	}
	if !strings.Contains(output, "Access denied for user 'user1'") {
		t.Errorf("Unexpected output for 'select rows': %v", output)
	}
}

// TestDialogServer creates a Server that uses the dialog plugin on the client.
func TestDialogServer(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	authServer.Method = MysqlDialog
	l, err := NewListener("tcp", ":0", authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	l.AllowClearTextWithoutTLS = true
	defer l.Close()
	go func() {
		l.Accept()
	}()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	sql := "select rows"
	output, ok := runMysql(t, params, sql)
	if strings.Contains(output, "No such file or directory") {
		t.Logf("skipping dialog plugin tests, as the dialog plugin cannot be loaded: %v", err)
		return
	}
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, "nice name") ||
		!strings.Contains(output, "nicer name") ||
		!strings.Contains(output, "2 rows in set") {
		t.Errorf("Unexpected output for 'select rows': %v", output)
	}
}

// TestTLSServer creates a Server with TLS support, then uses mysql
// client to connect to it.
func TestTLSServer(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
	}}

	// Create the listener, so we can get its host.
	// Below, we are enabling --ssl-verify-server-cert, which adds
	// a check that the common name of the certificate matches the
	// server host name we connect to.
	l, err := NewListener("tcp", ":0", authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()

	// Make sure hostname is added as an entry to /etc/hosts, otherwise ssl handshake will fail
	host, err := os.Hostname()
	if err != nil {
		t.Fatalf("Failed to get os Hostname: %v", err)
	}

	port := l.Addr().(*net.TCPAddr).Port

	// Create the certs.
	root, err := ioutil.TempDir("", "TestTLSServer")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(root)
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", host)
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
	params := &ConnParams{
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

	// Run a 'select rows' command with results.
	output, ok := runMysql(t, params, "select rows")
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, "nice name") ||
		!strings.Contains(output, "nicer name") ||
		!strings.Contains(output, "2 rows in set") {
		t.Errorf("Unexpected output for 'select rows'")
	}

	// make sure this went through SSL
	output, ok = runMysql(t, params, "ssl echo")
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, "ssl_flag") ||
		!strings.Contains(output, "ON") ||
		!strings.Contains(output, "1 row in set") {
		t.Errorf("Unexpected output for 'ssl echo': %v", output)
	}
}

const enableCleartextPluginPrefix = "enable-cleartext-plugin: "

// runMysql forks a mysql command line process connecting to the provided server.
func runMysql(t *testing.T, params *ConnParams, command string) (string, bool) {
	dir, err := vtenv.VtMysqlRoot()
	if err != nil {
		t.Fatalf("vtenv.VtMysqlRoot failed: %v", err)
	}
	name, err := binaryPath(dir, "mysql")
	if err != nil {
		t.Fatalf("binaryPath failed: %v", err)
	}
	// The args contain '-v' 3 times, to switch to very verbose output.
	// In particular, it has the message:
	// Query OK, 1 row affected (0.00 sec)
	args := []string{
		"-v", "-v", "-v",
	}
	if strings.HasPrefix(command, enableCleartextPluginPrefix) {
		command = command[len(enableCleartextPluginPrefix):]
		args = append(args, "--enable-cleartext-plugin")
	}
	args = append(args, "-e", command)
	if params.UnixSocket != "" {
		args = append(args, "-S", params.UnixSocket)
	} else {
		args = append(args,
			"-h", params.Host,
			"-P", fmt.Sprintf("%v", params.Port))
	}
	if params.Uname != "" {
		args = append(args, "-u", params.Uname)
	}
	if params.Pass != "" {
		args = append(args, "-p"+params.Pass)
	}
	if params.DbName != "" {
		args = append(args, "-D", params.DbName)
	}
	if params.Flags&CapabilityClientSSL > 0 {
		args = append(args,
			"--ssl",
			"--ssl-ca", params.SslCa,
			"--ssl-cert", params.SslCert,
			"--ssl-key", params.SslKey,
			"--ssl-verify-server-cert")
	}
	env := []string{
		"LD_LIBRARY_PATH=" + path.Join(dir, "lib/mysql"),
	}

	cmd := exec.Command(name, args...)
	cmd.Env = env
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	output := string(out)
	if err != nil {
		return output, false
	}
	return output, true
}

// binaryPath does a limited path lookup for a command,
// searching only within sbin and bin in the given root.
//
// FIXME(alainjobart) move this to vt/env, and use it from
// go/vt/mysqlctl too.
func binaryPath(root, binary string) (string, error) {
	subdirs := []string{"sbin", "bin"}
	for _, subdir := range subdirs {
		binPath := path.Join(root, subdir, binary)
		if _, err := os.Stat(binPath); err == nil {
			return binPath, nil
		}
	}
	return "", fmt.Errorf("%s not found in any of %s/{%s}",
		binary, root, strings.Join(subdirs, ","))
}
