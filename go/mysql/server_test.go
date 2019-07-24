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

	"vitess.io/vitess/go/sqltypes"
	vtenv "vitess.io/vitess/go/vt/env"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttls"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
	result   *sqltypes.Result
	err      error
	warnings uint16
}

func (th *testHandler) NewConnection(c *Conn) {
	th.lastConn = c
}

func (th *testHandler) ConnectionClosed(c *Conn) {

}

func (th *testHandler) ComQuery(c *Conn, query string, callback func(*sqltypes.Result) error) error {
	if th.result != nil {
		callback(th.result)
		return nil
	}

	switch query {
	case "error":
		return th.err
	case "panic":
		panic("test panic attack!")
	case "select rows":
		callback(selectRowsResult)
	case "error after send":
		callback(selectRowsResult)
		return th.err
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
		if strings.HasPrefix(query, benchmarkQueryPrefix) {
			callback(&sqltypes.Result{
				Fields: []*querypb.Field{
					{
						Name: "result",
						Type: querypb.Type_VARCHAR,
					},
				},
				Rows: [][]sqltypes.Value{
					{
						sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(query)),
					},
				},
			})
		}

		callback(&sqltypes.Result{})
	}
	return nil
}

func (th *testHandler) ComPrepare(c *Conn, query string, callback func(*sqltypes.Result) error) error {
	return nil
}

func (th *testHandler) ComStmtExecute(c *Conn, prepare *PrepareData, callback func(*sqltypes.Result) error) error {
	return nil
}

func (th *testHandler) WarningCount(c *Conn) uint16 {
	return th.warnings
}

func getHostPort(t *testing.T, a net.Addr) (string, int) {
	// For the host name, we resolve 'localhost' into an address.
	// This works around a few travis issues where IPv6 is not 100% enabled.
	hosts, err := net.LookupHost("localhost")
	if err != nil {
		t.Fatalf("LookupHost(localhost) failed: %v", err)
	}
	host := hosts[0]
	port := a.(*net.TCPAddr).Port
	t.Logf("listening on address '%v' port %v", host, port)
	return host, port
}

func TestConnectionFromListener(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	// Make sure we can create our own net.Listener for use with the mysql
	// listener
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("net.Listener failed: %v", err)
	}

	l, err := NewFromListener(listener, authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

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

func TestConnectionWithoutSourceHost(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

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

	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

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

func TestConnectionUseMysqlNativePasswordWithSourceHost(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()

	authServer.Entries["user1"] = []*AuthServerStaticEntry{
		{
			MysqlNativePassword: "*9E128DA0C64A6FCCCDCFBDD0FC0A2C967C6DB36F",
			UserData:            "userData1",
			SourceHost:          "localhost",
		},
	}

	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "mysql_password",
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

	unixSocket, err := ioutil.TempFile("", "mysql_vitess_test.sock")
	if err != nil {
		t.Fatalf("Failed to create temp file")
	}
	os.Remove(unixSocket.Name())

	l, err := NewListener("unix", unixSocket.Name(), authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	// Setup the right parameters.
	params := &ConnParams{
		UnixSocket: unixSocket.Name(),
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
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

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

func TestConnCounts(t *testing.T) {
	th := &testHandler{}

	initialNumUsers := len(connCountPerUser.Counts())

	user := "anotherNotYetConnectedUser1"
	passwd := "password1"

	authServer := NewAuthServerStatic()
	authServer.Entries[user] = []*AuthServerStaticEntry{{
		Password: passwd,
		UserData: "userData1",
	}}
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

	// Test with one new connection.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: user,
		Pass:  passwd,
	}

	c, err := Connect(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}

	connCounts := connCountPerUser.Counts()
	if l := len(connCounts); l-initialNumUsers != 1 {
		t.Errorf("Expected 1 new user, got %d", l)
	}
	checkCountsForUser(t, user, 1)

	// Test with a second new connection.
	c2, err := Connect(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}

	connCounts = connCountPerUser.Counts()
	// There is still only one new user.
	if l2 := len(connCounts); l2-initialNumUsers != 1 {
		t.Errorf("Expected 1 new user, got %d", l2)
	}
	checkCountsForUser(t, user, 2)

	// Test after closing connections. time.Sleep lets it work, but seems flakey.
	c.Close()
	//time.Sleep(10 * time.Millisecond)
	//checkCountsForUser(t, user, 1)

	c2.Close()
	//time.Sleep(10 * time.Millisecond)
	//checkCountsForUser(t, user, 0)
}

func checkCountsForUser(t *testing.T, user string, expected int64) {
	connCounts := connCountPerUser.Counts()

	userCount, ok := connCounts[user]
	if ok {
		if userCount != expected {
			t.Errorf("Expected connection count for user to be %d, got %d", expected, userCount)
		}
	} else {
		t.Errorf("No count found for user %s", user)
	}
}

func TestServer(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

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
	th.err = NewSQLError(ERUnknownComError, SSUnknownComError, "forced query error")
	output, ok := runMysql(t, params, "error")
	if ok {
		t.Fatalf("mysql should have failed: %v", output)
	}
	if !strings.Contains(output, "ERROR 1047 (08S01)") ||
		!strings.Contains(output, "forced query error") {
		t.Errorf("Unexpected output for 'error': %v", output)
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
	if strings.Contains(output, "warnings") {
		t.Errorf("Unexpected warnings in 'select rows'")
	}

	// Run a 'select rows' command with warnings
	th.warnings = 13
	output, ok = runMysql(t, params, "select rows")
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, "nice name") ||
		!strings.Contains(output, "nicer name") ||
		!strings.Contains(output, "2 rows in set") ||
		!strings.Contains(output, "13 warnings") {
		t.Errorf("Unexpected output for 'select rows': %v", output)
	}
	th.warnings = 0

	// If there's an error after streaming has started,
	// we should get a 2013
	th.err = NewSQLError(ERUnknownComError, SSUnknownComError, "forced error after send")
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
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

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
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	l.AllowClearTextWithoutTLS = true
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

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
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
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
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"))
	if err != nil {
		t.Fatalf("TLSServerConfig failed: %v", err)
	}
	l.TLSConfig = serverConfig
	go l.Accept()

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
	conn, err := Connect(context.Background(), params)
	//output, ok := runMysql(t, params, "select rows")
	if err != nil {
		t.Fatalf("mysql failed: %v", err)
	}
	results, err := conn.ExecuteFetch("select rows", 1000, true)
	if err != nil {
		t.Fatalf("mysql fetch failed: %v", err)
	}
	output := ""
	for _, row := range results.Rows {
		r := make([]string, 0)
		for _, col := range row {
			r = append(r, col.String())
		}
		output = output + strings.Join(r, ",") + "\n"
	}

	if results.Rows[0][1].ToString() != "nice name" ||
		results.Rows[1][1].ToString() != "nicer name" ||
		len(results.Rows) != 2 {
		t.Errorf("Unexpected output for 'select rows': %v", output)
	}

	// make sure this went through SSL
	results, err = conn.ExecuteFetch("ssl echo", 1000, true)
	if err != nil {
		t.Fatalf("mysql fetch failed: %v", err)
	}
	if results.Rows[0][0].ToString() != "ON" {
		t.Errorf("Unexpected output for 'ssl echo': %v", results)
	}

	checkCountForTLSVer(t, versionTLS12, 1)
	checkCountForTLSVer(t, versionNoTLS, 0)
	conn.Close()

}

// TestTLSRequired creates a Server with TLS required, then tests that an insecure mysql
// client is rejected
func TestTLSRequired(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
	}}

	// Create the listener, so we can get its host.
	// Below, we are enabling --ssl-verify-server-cert, which adds
	// a check that the common name of the certificate matches the
	// server host name we connect to.
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
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
	root, err := ioutil.TempDir("", "TestTLSRequired")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(root)
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", host)

	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"))
	if err != nil {
		t.Fatalf("TLSServerConfig failed: %v", err)
	}
	l.TLSConfig = serverConfig
	l.RequireSecureTransport = true
	go l.Accept()

	// Setup conn params without SSL.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	conn, err := Connect(context.Background(), params)
	if err == nil {
		t.Fatal("mysql should have failed")
	}
	if conn != nil {
		conn.Close()
	}

	// setup conn params with TLS
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")
	params.Flags = CapabilityClientSSL
	params.SslCa = path.Join(root, "ca-cert.pem")
	params.SslCert = path.Join(root, "client-cert.pem")
	params.SslKey = path.Join(root, "client-key.pem")

	conn, err = Connect(context.Background(), params)
	if err != nil {
		t.Fatalf("mysql failed: %v", err)
	}
	if conn != nil {
		conn.Close()
	}
}

func checkCountForTLSVer(t *testing.T, version string, expected int64) {
	connCounts := connCountByTLSVer.Counts()
	count, ok := connCounts[version]
	if ok {
		if count != expected {
			t.Errorf("Expected connection count for version %s to be %d, got %d", version, expected, count)
		}
	} else {
		t.Errorf("No count found for version %s", version)
	}
}

func TestErrorCodes(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	ctx := context.Background()
	client, err := Connect(ctx, params)
	if err != nil {
		t.Fatalf("error in connect: %v", err)
	}

	// Test that the right mysql errno/sqlstate are returned for various
	// internal vitess errors
	tests := []struct {
		err      error
		code     int
		sqlState string
		text     string
	}{
		{
			err: vterrors.Errorf(
				vtrpcpb.Code_INVALID_ARGUMENT,
				"invalid argument"),
			code:     ERUnknownError,
			sqlState: SSUnknownSQLState,
			text:     "invalid argument",
		},
		{
			err: vterrors.Errorf(
				vtrpcpb.Code_INVALID_ARGUMENT,
				"(errno %v) (sqlstate %v) invalid argument with errno", ERDupEntry, SSDupKey),
			code:     ERDupEntry,
			sqlState: SSDupKey,
			text:     "invalid argument with errno",
		},
		{
			err: vterrors.Errorf(
				vtrpcpb.Code_DEADLINE_EXCEEDED,
				"connection deadline exceeded"),
			code:     ERQueryInterrupted,
			sqlState: SSUnknownSQLState,
			text:     "deadline exceeded",
		},
		{
			err: vterrors.Errorf(
				vtrpcpb.Code_RESOURCE_EXHAUSTED,
				"query pool timeout"),
			code:     ERTooManyUserConnections,
			sqlState: SSUnknownSQLState,
			text:     "resource exhausted",
		},
		{
			err:      vterrors.Wrap(NewSQLError(ERVitessMaxRowsExceeded, SSUnknownSQLState, "Row count exceeded 10000"), "wrapped"),
			code:     ERVitessMaxRowsExceeded,
			sqlState: SSUnknownSQLState,
			text:     "resource exhausted",
		},
	}

	for _, test := range tests {
		th.err = NewSQLErrorFromError(test.err)
		result, err := client.ExecuteFetch("error", 100, false)
		if err == nil {
			t.Fatalf("mysql should have failed but returned: %v", result)
		}
		serr, ok := err.(*SQLError)
		if !ok {
			t.Fatalf("mysql should have returned a SQLError")
		}

		if serr.Number() != test.code {
			t.Errorf("error in %s: want code %v got %v", test.text, test.code, serr.Number())
		}

		if serr.SQLState() != test.sqlState {
			t.Errorf("error in %s: want sqlState %v got %v", test.text, test.sqlState, serr.SQLState())
		}

		if !strings.Contains(serr.Error(), test.err.Error()) {
			t.Errorf("error in %s: want err %v got %v", test.text, test.err.Error(), serr.Error())
		}
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

	t.Logf("Running mysql command: %v %v", name, args)
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

func TestListenerShutdown(t *testing.T) {
	th := &testHandler{}
	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatalf("Can't connect to listener: %v", err)
	}

	if err := conn.Ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	l.Shutdown()

	if err := conn.Ping(); err != nil {
		sqlErr, ok := err.(*SQLError)
		if !ok {
			t.Fatalf("Wrong error type: %T", err)
		}
		if sqlErr.Number() != ERServerShutdown {
			t.Fatalf("Unexpected sql error code: %d", sqlErr.Number())
		}
		if sqlErr.SQLState() != SSServerShutdown {
			t.Fatalf("Unexpected error sql state: %s", sqlErr.SQLState())
		}
		if sqlErr.Message != "Server shutdown in progress" {
			t.Fatalf("Unexpected error message: %s", sqlErr.Message)
		}
	} else {
		t.Fatalf("Ping should fail after shutdown")
	}
}

func TestParseConnAttrs(t *testing.T) {
	expected := map[string]string{
		"_client_version": "8.0.11",
		"program_name":    "mysql",
		"_pid":            "22850",
		"_platform":       "x86_64",
		"_os":             "linux-glibc2.12",
		"_client_name":    "libmysql",
	}

	data := []byte{0x70, 0x04, 0x5f, 0x70, 0x69, 0x64, 0x05, 0x32, 0x32, 0x38, 0x35, 0x30, 0x09, 0x5f, 0x70, 0x6c,
		0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x06, 0x78, 0x38, 0x36, 0x5f, 0x36, 0x34, 0x03, 0x5f, 0x6f,
		0x73, 0x0f, 0x6c, 0x69, 0x6e, 0x75, 0x78, 0x2d, 0x67, 0x6c, 0x69, 0x62, 0x63, 0x32, 0x2e, 0x31,
		0x32, 0x0c, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x08, 0x6c,
		0x69, 0x62, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x0f, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f,
		0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x06, 0x38, 0x2e, 0x30, 0x2e, 0x31, 0x31, 0x0c, 0x70,
		0x72, 0x6f, 0x67, 0x72, 0x61, 0x6d, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x05, 0x6d, 0x79, 0x73, 0x71, 0x6c}

	attrs, pos, err := parseConnAttrs(data, 0)
	if err != nil {
		t.Fatalf("Failed to read connection attributes: %v", err)
	}
	if pos != 113 {
		t.Fatalf("Unexpeded pos after reading connection attributes: %d intead of 113", pos)
	}
	for k, v := range expected {
		if val, ok := attrs[k]; ok {
			if val != v {
				t.Fatalf("Unexpected value found in attrs for key %s: got %s expected %s", k, val, v)
			}
		} else {
			t.Fatalf("Error reading key %s from connection attributes: attrs: %-v", k, attrs)
		}
	}
}
