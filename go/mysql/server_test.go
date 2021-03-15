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

package mysql

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
}

type testHandler struct {
	mu       sync.Mutex
	lastConn *Conn
	result   *sqltypes.Result
	err      error
	warnings uint16
}

func (th *testHandler) LastConn() *Conn {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.lastConn
}

func (th *testHandler) Result() *sqltypes.Result {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.result
}

func (th *testHandler) SetErr(err error) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.err = err
}

func (th *testHandler) Err() error {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.err
}

func (th *testHandler) SetWarnings(count uint16) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.warnings = count
}

func (th *testHandler) NewConnection(c *Conn) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.lastConn = c
}

func (th *testHandler) ConnectionClosed(_ *Conn) {
}

func (th *testHandler) ComQuery(c *Conn, query string, callback func(*sqltypes.Result) error) error {
	if result := th.Result(); result != nil {
		callback(result)
		return nil
	}

	switch query {
	case "error":
		return th.Err()
	case "panic":
		panic("test panic attack!")
	case "select rows":
		callback(selectRowsResult)
	case "error after send":
		callback(selectRowsResult)
		return th.Err()
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
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(c.schemaName)),
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
	case "50ms delay":
		callback(&sqltypes.Result{
			Fields: []*querypb.Field{{
				Name: "result",
				Type: querypb.Type_VARCHAR,
			}},
		})
		time.Sleep(50 * time.Millisecond)
		callback(&sqltypes.Result{
			Rows: [][]sqltypes.Value{{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("delayed")),
			}},
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

func (th *testHandler) ComPrepare(c *Conn, query string, bindVars map[string]*querypb.BindVariable) ([]*querypb.Field, error) {
	return nil, nil
}

func (th *testHandler) ComStmtExecute(c *Conn, prepare *PrepareData, callback func(*sqltypes.Result) error) error {
	return nil
}

func (th *testHandler) ComResetConnection(c *Conn) {

}

func (th *testHandler) WarningCount(c *Conn) uint16 {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.warnings
}

func getHostPort(t *testing.T, a net.Addr) (string, int) {
	// For the host name, we resolve 'localhost' into an address.
	// This works around a few travis issues where IPv6 is not 100% enabled.
	hosts, err := net.LookupHost("localhost")
	require.NoError(t, err, "LookupHost(localhost) failed")
	host := hosts[0]
	port := a.(*net.TCPAddr).Port
	t.Logf("listening on address '%v' port %v", host, port)
	return host, port
}

func TestConnectionFromListener(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()
	// Make sure we can create our own net.Listener for use with the mysql
	// listener
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err, "net.Listener failed")

	l, err := NewFromListener(listener, authServer, th, 0, 0)
	require.NoError(t, err, "NewListener failed")
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
	require.NoError(t, err, "Should be able to connect to server")
	c.Close()
}

func TestConnectionWithoutSourceHost(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
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
	require.NoError(t, err, "Should be able to connect to server")
	c.Close()
}

func TestConnectionWithSourceHost(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{
		{
			Password:   "password1",
			UserData:   "userData1",
			SourceHost: "localhost",
		},
	}
	defer authServer.close()

	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
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
	require.EqualError(t, err, "Access denied for user 'user1' (errno 1045) (sqlstate 28000)", "Should not be able to connect to server")
}

func TestConnectionUseMysqlNativePasswordWithSourceHost(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{
		{
			MysqlNativePassword: "*9E128DA0C64A6FCCCDCFBDD0FC0A2C967C6DB36F",
			UserData:            "userData1",
			SourceHost:          "localhost",
		},
	}
	defer authServer.close()

	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
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
	require.EqualError(t, err, "Access denied for user 'user1' (errno 1045) (sqlstate 28000)", "Should not be able to connect to server")
}

func TestConnectionUnixSocket(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{
		{
			Password:   "password1",
			UserData:   "userData1",
			SourceHost: "localhost",
		},
	}
	defer authServer.close()

	unixSocket, err := ioutil.TempFile("", "mysql_vitess_test.sock")
	require.NoError(t, err, "Failed to create temp file")

	os.Remove(unixSocket.Name())

	l, err := NewListener("unix", unixSocket.Name(), authServer, th, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
	defer l.Close()
	go l.Accept()

	// Setup the right parameters.
	params := &ConnParams{
		UnixSocket: unixSocket.Name(),
		Uname:      "user1",
		Pass:       "password1",
	}

	c, err := Connect(context.Background(), params)
	require.NoError(t, err, "Should be able to connect to server")
	c.Close()
}

func TestClientFoundRows(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
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
	require.NoError(t, err, "Connect failed")
	foundRows := th.LastConn().Capabilities & CapabilityClientFoundRows
	assert.Equal(t, uint32(0), foundRows, "FoundRows flag: %x, second bit must be 0", th.LastConn().Capabilities)
	c.Close()
	assert.True(t, c.IsClosed(), "IsClosed should be true on Close-d connection.")

	// Test with flag.
	params.Flags |= CapabilityClientFoundRows
	c, err = Connect(context.Background(), params)
	require.NoError(t, err, "Connect failed")
	foundRows = th.LastConn().Capabilities & CapabilityClientFoundRows
	assert.NotZero(t, foundRows, "FoundRows flag: %x, second bit must be set", th.LastConn().Capabilities)
	c.Close()
}

func TestConnCounts(t *testing.T) {
	th := &testHandler{}

	initialNumUsers := len(connCountPerUser.Counts())

	// FIXME: we should be able to ResetAll counters instead of computing a delta, but it doesn't work for some reason
	// connCountPerUser.ResetAll()

	user := "anotherNotYetConnectedUser1"
	passwd := "password1"

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries[user] = []*AuthServerStaticEntry{{
		Password: passwd,
		UserData: "userData1",
	}}
	defer authServer.close()
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
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
	require.NoError(t, err, "Connect failed")

	connCounts := connCountPerUser.Counts()
	assert.Equal(t, 1, len(connCounts)-initialNumUsers)
	checkCountsForUser(t, user, 1)

	// Test with a second new connection.
	c2, err := Connect(context.Background(), params)
	require.NoError(t, err)
	connCounts = connCountPerUser.Counts()
	// There is still only one new user.
	assert.Equal(t, 1, len(connCounts)-initialNumUsers)
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
	assert.True(t, ok, "No count found for user %s", user)
	assert.Equal(t, expected, userCount)
}

func TestServer(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err)
	l.SlowConnectWarnThreshold.Set(time.Nanosecond * 1)
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

	// Run a 'select rows' command with results.
	output, err := runMysqlWithErr(t, params, "select rows")
	require.NoError(t, err)

	assert.Contains(t, output, "nice name", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "nicer name", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "2 rows in set", "Unexpected output for 'select rows'")
	assert.NotContains(t, output, "warnings")

	// Run a 'select rows' command with warnings
	th.SetWarnings(13)
	output, err = runMysqlWithErr(t, params, "select rows")
	require.NoError(t, err)
	assert.Contains(t, output, "nice name", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "nicer name", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "2 rows in set", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "13 warnings", "Unexpected output for 'select rows'")
	th.SetWarnings(0)

	// If there's an error after streaming has started,
	// we should get a 2013
	th.SetErr(NewSQLError(ERUnknownComError, SSUnknownComError, "forced error after send"))
	output, err = runMysqlWithErr(t, params, "error after send")
	require.Error(t, err)
	assert.Contains(t, output, "ERROR 2013 (HY000)", "Unexpected output for 'panic'")
	assert.Contains(t, output, "Lost connection to MySQL server during query", "Unexpected output for 'panic'")

	// Run an 'insert' command, no rows, but rows affected.
	output, err = runMysqlWithErr(t, params, "insert")
	require.NoError(t, err)
	assert.Contains(t, output, "Query OK, 123 rows affected", "Unexpected output for 'insert'")

	// Run a 'schema echo' command, to make sure db name is right.
	params.DbName = "XXXfancyXXX"
	output, err = runMysqlWithErr(t, params, "schema echo")
	require.NoError(t, err)
	assert.Contains(t, output, params.DbName, "Unexpected output for 'schema echo'")

	// Sanity check: make sure this didn't go through SSL
	output, err = runMysqlWithErr(t, params, "ssl echo")
	require.NoError(t, err)
	assert.Contains(t, output, "ssl_flag")
	assert.Contains(t, output, "OFF")
	assert.Contains(t, output, "1 row in set", "Unexpected output for 'ssl echo': %v", output)

	// UserData check: checks the server user data is correct.
	output, err = runMysqlWithErr(t, params, "userData echo")
	require.NoError(t, err)
	assert.Contains(t, output, "user1")
	assert.Contains(t, output, "user_data")
	assert.Contains(t, output, "userData1", "Unexpected output for 'userData echo': %v", output)

	// Permissions check: check a bad password is rejected.
	params.Pass = "bad"
	output, err = runMysqlWithErr(t, params, "select rows")
	require.Error(t, err)
	assert.Contains(t, output, "1045")
	assert.Contains(t, output, "28000")
	assert.Contains(t, output, "Access denied", "Unexpected output for invalid password: %v", output)

	// Permissions check: check an unknown user is rejected.
	params.Pass = "password1"
	params.Uname = "user2"
	output, err = runMysqlWithErr(t, params, "select rows")
	require.Error(t, err)
	assert.Contains(t, output, "1045")
	assert.Contains(t, output, "28000")
	assert.Contains(t, output, "Access denied", "Unexpected output for invalid password: %v", output)

	// Uncomment to leave setup up for a while, to run tests manually.
	//	fmt.Printf("Listening to server on host '%v' port '%v'.\n", host, port)
	//	time.Sleep(60 * time.Minute)
}

func TestServerStats(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err)
	l.SlowConnectWarnThreshold.Set(time.Nanosecond * 1)
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

	timings.Reset()
	connAccept.Reset()
	connCount.Reset()
	connSlow.Reset()
	connRefuse.Reset()

	// Run an 'error' command.
	th.SetErr(NewSQLError(ERUnknownComError, SSUnknownComError, "forced query error"))
	output, ok := runMysql(t, params, "error")
	require.False(t, ok, "mysql should have failed: %v", output)

	assert.Contains(t, output, "ERROR 1047 (08S01)")
	assert.Contains(t, output, "forced query error", "Unexpected output for 'error': %v", output)

	assert.EqualValues(t, 0, connCount.Get(), "connCount")
	assert.EqualValues(t, 1, connAccept.Get(), "connAccept")
	assert.EqualValues(t, 1, connSlow.Get(), "connSlow")
	assert.EqualValues(t, 0, connRefuse.Get(), "connRefuse")

	expectedTimingDeltas := map[string]int64{
		"All":            2,
		connectTimingKey: 1,
		queryTimingKey:   1,
	}
	gotTimingCounts := timings.Counts()
	for key, got := range gotTimingCounts {
		expected := expectedTimingDeltas[key]
		assert.GreaterOrEqual(t, got, expected, "Expected Timing count delta %s should be >= %d, got %d", key, expected, got)
	}

	// Set the slow connect threshold to something high that we don't expect to trigger
	l.SlowConnectWarnThreshold.Set(time.Second * 1)

	// Run a 'panic' command, other side should panic, recover and
	// close the connection.
	output, err = runMysqlWithErr(t, params, "panic")
	require.Error(t, err)
	assert.Contains(t, output, "ERROR 2013 (HY000)")
	assert.Contains(t, output, "Lost connection to MySQL server during query", "Unexpected output for 'panic': %v", output)

	assert.EqualValues(t, 0, connCount.Get(), "connCount")
	assert.EqualValues(t, 2, connAccept.Get(), "connAccept")
	assert.EqualValues(t, 1, connSlow.Get(), "connSlow")
	assert.EqualValues(t, 0, connRefuse.Get(), "connRefuse")
}

// TestClearTextServer creates a Server that needs clear text
// passwords from the client.
func TestClearTextServer(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	authServer.method = MysqlClearPassword
	defer authServer.close()
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err)
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

	version, _ := runMysql(t, nil, "--version")
	isMariaDB := strings.Contains(version, "MariaDB")

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
	l.AllowClearTextWithoutTLS.Set(true)
	sql := "select rows"
	output, ok := runMysql(t, params, sql)
	if ok {
		if isMariaDB {
			t.Logf("mysql should have failed but returned: %v\nbut letting it go on MariaDB", output)
		} else {
			require.Fail(t, "mysql should have failed but returned: %v", output)
		}
	} else {
		if strings.Contains(output, "No such file or directory") {
			t.Logf("skipping mysql clear text tests, as the clear text plugin cannot be loaded: %v", err)
			return
		}
		assert.Contains(t, output, "plugin not enabled", "Unexpected output for 'select rows': %v", output)
	}

	// Now enable clear text plugin in client, but server requires SSL.
	l.AllowClearTextWithoutTLS.Set(false)
	if !isMariaDB {
		sql = enableCleartextPluginPrefix + sql
	}
	output, ok = runMysql(t, params, sql)
	assert.False(t, ok, "mysql should have failed but returned: %v", output)
	assert.Contains(t, output, "Cannot use clear text authentication over non-SSL connections", "Unexpected output for 'select rows': %v", output)

	// Now enable clear text plugin, it should now work.
	l.AllowClearTextWithoutTLS.Set(true)
	output, ok = runMysql(t, params, sql)
	require.True(t, ok, "mysql failed: %v", output)

	assert.Contains(t, output, "nice name", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "nicer name", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "2 rows in set", "Unexpected output for 'select rows'")

	// Change password, make sure server rejects us.
	params.Pass = "bad"
	output, ok = runMysql(t, params, sql)
	assert.False(t, ok, "mysql should have failed but returned: %v", output)
	assert.Contains(t, output, "Access denied for user 'user1'", "Unexpected output for 'select rows': %v", output)
}

// TestDialogServer creates a Server that uses the dialog plugin on the client.
func TestDialogServer(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	authServer.method = MysqlDialog
	defer authServer.close()
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err)
	l.AllowClearTextWithoutTLS.Set(true)
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
	require.True(t, ok, "mysql failed: %v", output)
	assert.Contains(t, output, "nice name", "Unexpected output for 'select rows': %v", output)
	assert.Contains(t, output, "nicer name", "Unexpected output for 'select rows': %v", output)
	assert.Contains(t, output, "2 rows in set", "Unexpected output for 'select rows': %v", output)
}

// TestTLSServer creates a Server with TLS support, then uses mysql
// client to connect to it.
func TestTLSServer(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
	}}
	defer authServer.close()

	// Create the listener, so we can get its host.
	// Below, we are enabling --ssl-verify-server-cert, which adds
	// a check that the common name of the certificate matches the
	// server host name we connect to.
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err)
	defer l.Close()

	// Make sure hostname is added as an entry to /etc/hosts, otherwise ssl handshake will fail
	host, err := os.Hostname()
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port

	// Create the certs.
	root, err := ioutil.TempDir("", "TestTLSServer")
	require.NoError(t, err)
	defer os.RemoveAll(root)
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", host)
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")

	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"),
		"")
	require.NoError(t, err)
	l.TLSConfig.Store(serverConfig)
	go l.Accept()

	connCountByTLSVer.ResetAll()
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
	require.NoError(t, err)
	results, err := conn.ExecuteFetch("select rows", 1000, true)
	require.NoError(t, err)
	output := ""
	for _, row := range results.Rows {
		r := make([]string, 0)
		for _, col := range row {
			r = append(r, col.String())
		}
		output = output + strings.Join(r, ",") + "\n"
	}

	assert.Equal(t, "nice name", results.Rows[0][1].ToString())
	assert.Equal(t, "nicer name", results.Rows[1][1].ToString())
	assert.Equal(t, 2, len(results.Rows))

	// make sure this went through SSL
	results, err = conn.ExecuteFetch("ssl echo", 1000, true)
	require.NoError(t, err)
	assert.Equal(t, "ON", results.Rows[0][0].ToString())

	// Find out which TLS version the connection actually used,
	// so we can check that the corresponding counter was incremented.
	tlsVersion := conn.conn.(*tls.Conn).ConnectionState().Version

	checkCountForTLSVer(t, tlsVersionToString(tlsVersion), 1)
	conn.Close()

}

// TestTLSRequired creates a Server with TLS required, then tests that an insecure mysql
// client is rejected
func TestTLSRequired(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
	}}
	defer authServer.close()

	// Create the listener, so we can get its host.
	// Below, we are enabling --ssl-verify-server-cert, which adds
	// a check that the common name of the certificate matches the
	// server host name we connect to.
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err)
	defer l.Close()

	// Make sure hostname is added as an entry to /etc/hosts, otherwise ssl handshake will fail
	host, err := os.Hostname()
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port

	// Create the certs.
	root, err := ioutil.TempDir("", "TestTLSRequired")
	require.NoError(t, err)
	defer os.RemoveAll(root)
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", host)

	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"),
		"")
	require.NoError(t, err)
	l.TLSConfig.Store(serverConfig)
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
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "Code: UNAVAILABLE")
	require.Contains(t, err.Error(), "server does not allow insecure connections, client must use SSL/TLS")
	require.Contains(t, err.Error(), "(errno 1105) (sqlstate HY000)")
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
	require.NoError(t, err)
	if conn != nil {
		conn.Close()
	}
}

func checkCountForTLSVer(t *testing.T, version string, expected int64) {
	connCounts := connCountByTLSVer.Counts()
	count, ok := connCounts[version]
	assert.True(t, ok, "No count found for version %s", version)
	assert.Equal(t, expected, count, "Unexpected connection count for version %s", version)
}

func TestErrorCodes(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err)
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
	require.NoError(t, err)

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
				"(errno %v) (sqlstate %v) invalid argument with errno", ERDupEntry, SSConstraintViolation),
			code:     ERDupEntry,
			sqlState: SSConstraintViolation,
			text:     "invalid argument with errno",
		},
		{
			err: vterrors.Errorf(
				vtrpcpb.Code_DEADLINE_EXCEEDED,
				"connection deadline exceeded"),
			code:     ERQueryInterrupted,
			sqlState: SSQueryInterrupted,
			text:     "deadline exceeded",
		},
		{
			err: vterrors.Errorf(
				vtrpcpb.Code_RESOURCE_EXHAUSTED,
				"query pool timeout"),
			code:     ERTooManyUserConnections,
			sqlState: SSClientError,
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
		t.Run(test.err.Error(), func(t *testing.T) {
			th.SetErr(NewSQLErrorFromError(test.err))
			rs, err := client.ExecuteFetch("error", 100, false)
			require.Error(t, err, "mysql should have failed but returned: %v", rs)
			serr, ok := err.(*SQLError)
			require.True(t, ok, "mysql should have returned a SQLError")

			assert.Equal(t, test.code, serr.Number(), "error in %s: want code %v got %v", test.text, test.code, serr.Number())
			assert.Equal(t, test.sqlState, serr.SQLState(), "error in %s: want sqlState %v got %v", test.text, test.sqlState, serr.SQLState())
			assert.Contains(t, serr.Error(), test.err.Error())
		})
	}
}

const enableCleartextPluginPrefix = "enable-cleartext-plugin: "

// runMysql forks a mysql command line process connecting to the provided server.
func runMysql(t *testing.T, params *ConnParams, command string) (string, bool) {
	output, err := runMysqlWithErr(t, params, command)
	if err != nil {
		return output, false
	}
	return output, true

}
func runMysqlWithErr(t *testing.T, params *ConnParams, command string) (string, error) {
	dir, err := vtenv.VtMysqlRoot()
	require.NoError(t, err)
	name, err := binaryPath(dir, "mysql")
	require.NoError(t, err)
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
	if command == "--version" {
		args = append(args, command)
	} else {
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
		return output, err
	}
	return output, nil
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
	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()
	l, err := NewListener("tcp", ":0", authServer, th, 0, 0, false)
	require.NoError(t, err)
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
	connRefuse.Reset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := Connect(ctx, params)
	require.NoError(t, err)

	err = conn.Ping()
	require.NoError(t, err)

	l.Shutdown()

	assert.EqualValues(t, 1, connRefuse.Get(), "connRefuse")

	err = conn.Ping()
	require.EqualError(t, err, "Server shutdown in progress (errno 1053) (sqlstate 08S01)")
	sqlErr, ok := err.(*SQLError)
	require.True(t, ok, "Wrong error type: %T", err)

	require.Equal(t, ERServerShutdown, sqlErr.Number())
	require.Equal(t, SSNetError, sqlErr.SQLState())
	require.Equal(t, "Server shutdown in progress", sqlErr.Message)
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
	require.NoError(t, err)
	require.Equal(t, 113, pos)
	for k, v := range expected {
		val, ok := attrs[k]
		require.True(t, ok, "Error reading key %s from connection attributes: attrs: %-v", k, attrs)
		require.Equal(t, v, val, "Unexpected value found in attrs for key %s", k)
	}
}

func TestServerFlush(t *testing.T) {
	defer func(saved time.Duration) { *mysqlServerFlushDelay = saved }(*mysqlServerFlushDelay)
	*mysqlServerFlushDelay = 10 * time.Millisecond

	th := &testHandler{}

	l, err := NewListener("tcp", ":0", &AuthServerNone{}, th, 0, 0, false)
	require.NoError(t, err)
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())
	params := &ConnParams{
		Host: host,
		Port: port,
	}

	c, err := Connect(context.Background(), params)
	require.NoError(t, err)
	defer c.Close()

	start := time.Now()
	err = c.ExecuteStreamFetch("50ms delay")
	require.NoError(t, err)

	flds, err := c.Fields()
	require.NoError(t, err)
	if duration, want := time.Since(start), 20*time.Millisecond; duration < *mysqlServerFlushDelay || duration > want {
		assert.Fail(t, "duration: %v, want between %v and %v", duration, *mysqlServerFlushDelay, want)
	}
	want1 := []*querypb.Field{{
		Name: "result",
		Type: querypb.Type_VARCHAR,
	}}
	assert.Equal(t, want1, flds)

	row, err := c.FetchNext()
	require.NoError(t, err)
	if duration, want := time.Since(start), 50*time.Millisecond; duration < want {
		assert.Fail(t, "duration: %v, want > %v", duration, want)
	}
	want2 := []sqltypes.Value{sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("delayed"))}
	assert.Equal(t, want2, row)

	row, err = c.FetchNext()
	require.NoError(t, err)
	assert.Nil(t, row)
}
