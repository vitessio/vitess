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

package vtgate

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/trace"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vtenv"
)

type testHandler struct {
	mysql.UnimplementedHandler
	lastConn *mysql.Conn
}

func (th *testHandler) NewConnection(c *mysql.Conn) {
	th.lastConn = c
}

func (th *testHandler) ComQuery(c *mysql.Conn, q string, callback func(*sqltypes.Result) error) error {
	// when creating a connection, we send a query to MySQL to set the connection's collation,
	// this query usually returns us something. however, we use testHandler which is a fake
	// implementation of MySQL that returns no results and no error for set queries, Vitess
	// interprets this as an error, we do not want to fail if we see such error.
	// for this reason, we send back an empty result to the caller.
	return callback(&sqltypes.Result{Fields: []*querypb.Field{}, Rows: [][]sqltypes.Value{}})
}

func (th *testHandler) ComPrepare(c *mysql.Conn, q string, b map[string]*querypb.BindVariable) ([]*querypb.Field, error) {
	return nil, nil
}

func (th *testHandler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return nil
}

func (th *testHandler) ComRegisterReplica(c *mysql.Conn, replicaHost string, replicaPort uint16, replicaUser string, replicaPassword string) error {
	return nil
}

func (th *testHandler) ComBinlogDump(c *mysql.Conn, logFile string, binlogPos uint32) error {
	return nil
}

func (th *testHandler) ComBinlogDumpGTID(c *mysql.Conn, logFile string, logPos uint64, gtidSet replication.GTIDSet) error {
	return nil
}

func (th *testHandler) WarningCount(c *mysql.Conn) uint16 {
	return 0
}

func (th *testHandler) Env() *vtenv.Environment {
	return vtenv.NewTestEnv()
}

func TestConnectionUnixSocket(t *testing.T) {
	th := &testHandler{}

	authServer := newTestAuthServerStatic()

	// Use tmp file to reserve a path, remove it immediately, we only care about
	// name in this context
	unixSocket, err := os.CreateTemp("", "mysql_vitess_test.sock")
	if err != nil {
		t.Fatalf("Failed to create temp file")
	}
	os.Remove(unixSocket.Name())

	l, err := newMysqlUnixSocket(unixSocket.Name(), authServer, th)
	if err != nil {
		t.Fatalf("NewUnixSocket failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	params := &mysql.ConnParams{
		UnixSocket: unixSocket.Name(),
		Uname:      "user1",
		Pass:       "password1",
	}

	c, err := mysql.Connect(context.Background(), params)
	if err != nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}
	c.Close()
}

func TestConnectionStaleUnixSocket(t *testing.T) {
	th := &testHandler{}

	authServer := newTestAuthServerStatic()

	// First let's create a file. In this way, we simulate
	// having a stale socket on disk that needs to be cleaned up.
	unixSocket, err := os.CreateTemp("", "mysql_vitess_test.sock")
	if err != nil {
		t.Fatalf("Failed to create temp file")
	}

	l, err := newMysqlUnixSocket(unixSocket.Name(), authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	params := &mysql.ConnParams{
		UnixSocket: unixSocket.Name(),
		Uname:      "user1",
		Pass:       "password1",
	}

	c, err := mysql.Connect(context.Background(), params)
	if err != nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}
	c.Close()
}

func TestConnectionRespectsExistingUnixSocket(t *testing.T) {
	th := &testHandler{}

	authServer := newTestAuthServerStatic()

	unixSocket, err := os.CreateTemp("", "mysql_vitess_test.sock")
	if err != nil {
		t.Fatalf("Failed to create temp file")
	}
	os.Remove(unixSocket.Name())

	l, err := newMysqlUnixSocket(unixSocket.Name(), authServer, th)
	if err != nil {
		t.Errorf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()
	_, err = newMysqlUnixSocket(unixSocket.Name(), authServer, th)
	want := "listen unix"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, want prefix %s", err, want)
	}
}

var newSpanOK = func(ctx context.Context, label string) (trace.Span, context.Context) {
	return trace.NoopSpan{}, context.Background()
}

var newFromStringOK = func(ctx context.Context, spanContext, label string) (trace.Span, context.Context, error) {
	return trace.NoopSpan{}, context.Background(), nil
}

func newFromStringFail(t *testing.T) func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
	return func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
		t.Fatalf("we didn't provide a parent span in the sql query. this should not have been called. got: %v", parentSpan)
		return trace.NoopSpan{}, context.Background(), nil
	}
}

func newFromStringError(t *testing.T) func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
	return func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
		return trace.NoopSpan{}, context.Background(), fmt.Errorf("")
	}
}

func newFromStringExpect(t *testing.T, expected string) func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
	return func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
		assert.Equal(t, expected, parentSpan)
		return trace.NoopSpan{}, context.Background(), nil
	}
}

func newSpanFail(t *testing.T) func(ctx context.Context, label string) (trace.Span, context.Context) {
	return func(ctx context.Context, label string) (trace.Span, context.Context) {
		t.Fatalf("we provided a span context but newFromString was not used as expected")
		return trace.NoopSpan{}, context.Background()
	}
}

func TestNoSpanContextPassed(t *testing.T) {
	_, _, err := startSpanTestable(context.Background(), "sql without comments", "someLabel", newSpanOK, newFromStringFail(t))
	assert.NoError(t, err)
}

func TestSpanContextNoPassedInButExistsInString(t *testing.T) {
	_, _, err := startSpanTestable(context.Background(), "SELECT * FROM SOMETABLE WHERE COL = \"/*VT_SPAN_CONTEXT=123*/", "someLabel", newSpanOK, newFromStringFail(t))
	assert.NoError(t, err)
}

func TestSpanContextPassedIn(t *testing.T) {
	_, _, err := startSpanTestable(context.Background(), "/*VT_SPAN_CONTEXT=123*/SQL QUERY", "someLabel", newSpanFail(t), newFromStringOK)
	assert.NoError(t, err)
}

func TestSpanContextPassedInEvenAroundOtherComments(t *testing.T) {
	_, _, err := startSpanTestable(context.Background(), "/*VT_SPAN_CONTEXT=123*/SELECT /*vt+ SCATTER_ERRORS_AS_WARNINGS */ col1, col2 FROM TABLE ", "someLabel",
		newSpanFail(t),
		newFromStringExpect(t, "123"))
	assert.NoError(t, err)
}

func TestSpanContextNotParsable(t *testing.T) {
	hasRun := false
	_, _, err := startSpanTestable(context.Background(), "/*VT_SPAN_CONTEXT=123*/SQL QUERY", "someLabel",
		func(c context.Context, s string) (trace.Span, context.Context) {
			hasRun = true
			return trace.NoopSpan{}, context.Background()
		},
		newFromStringError(t))
	assert.NoError(t, err)
	assert.True(t, hasRun, "Should have continued execution despite failure to parse VT_SPAN_CONTEXT")
}

func newTestAuthServerStatic() *mysql.AuthServerStatic {
	jsonConfig := "{\"user1\":{\"Password\":\"password1\", \"UserData\":\"userData1\", \"SourceHost\":\"localhost\"}}"
	return mysql.NewAuthServerStatic("", jsonConfig, 0)
}

func TestDefaultWorkloadEmpty(t *testing.T) {
	vh := &vtgateHandler{}
	mysqlDefaultWorkload = int32(querypb.ExecuteOptions_OLTP)
	sess := vh.session(&mysql.Conn{})
	if sess.Options.Workload != querypb.ExecuteOptions_OLTP {
		t.Fatalf("Expected default workload OLTP")
	}
}

func TestDefaultWorkloadOLAP(t *testing.T) {
	vh := &vtgateHandler{}
	mysqlDefaultWorkload = int32(querypb.ExecuteOptions_OLAP)
	sess := vh.session(&mysql.Conn{})
	if sess.Options.Workload != querypb.ExecuteOptions_OLAP {
		t.Fatalf("Expected default workload OLAP")
	}
}

func TestInitTLSConfigWithoutServerCA(t *testing.T) {
	testInitTLSConfig(t, false)
}

func TestInitTLSConfigWithServerCA(t *testing.T) {
	testInitTLSConfig(t, true)
}

func testInitTLSConfig(t *testing.T, serverCA bool) {
	// Create the certs.
	ctx := utils.LeakCheckContext(t)

	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateCRL(root, tlstest.CA)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")

	serverCACert := ""
	if serverCA {
		serverCACert = path.Join(root, "ca-cert.pem")
	}

	srv := &mysqlServer{tcpListener: &mysql.Listener{}}
	if err := initTLSConfig(ctx, srv, path.Join(root, "server-cert.pem"), path.Join(root, "server-key.pem"), path.Join(root, "ca-cert.pem"), path.Join(root, "ca-crl.pem"), serverCACert, true, tls.VersionTLS12); err != nil {
		t.Fatalf("init tls config failure due to: +%v", err)
	}

	serverConfig := srv.tcpListener.TLSConfig.Load()
	if serverConfig == nil {
		t.Fatalf("init tls config shouldn't create nil server config")
	}

	srv.sigChan <- syscall.SIGHUP
	time.Sleep(100 * time.Millisecond) // wait for signal handler

	if srv.tcpListener.TLSConfig.Load() == serverConfig {
		t.Fatalf("init tls config should have been recreated after SIGHUP")
	}
}

// TestKillMethods test the mysql plugin for kill method calls.
func TestKillMethods(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	vh := newVtgateHandler(&VTGate{executor: executor})

	// connection does not exist
	err := vh.KillQuery(12345)
	assert.ErrorContains(t, err, "Unknown thread id: 12345 (errno 1094) (sqlstate HY000)")

	err = vh.KillConnection(context.Background(), 12345)
	assert.ErrorContains(t, err, "Unknown thread id: 12345 (errno 1094) (sqlstate HY000)")

	// add a connection
	mysqlConn := mysql.GetTestConn()
	mysqlConn.ConnectionID = 1
	vh.connections[1] = mysqlConn

	// connection exists

	// updating context.
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	mysqlConn.UpdateCancelCtx(cancelFunc)

	// kill query
	err = vh.KillQuery(1)
	assert.NoError(t, err)
	require.EqualError(t, cancelCtx.Err(), "context canceled")

	// updating context.
	cancelCtx, cancelFunc = context.WithCancel(context.Background())
	mysqlConn.UpdateCancelCtx(cancelFunc)

	// kill connection
	err = vh.KillConnection(context.Background(), 1)
	assert.NoError(t, err)
	require.EqualError(t, cancelCtx.Err(), "context canceled")
	require.True(t, mysqlConn.IsMarkedForClose())
}

func TestGracefulShutdown(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)

	vh := newVtgateHandler(&VTGate{executor: executor, timings: timings, rowsReturned: rowsReturned, rowsAffected: rowsAffected})
	th := &testHandler{}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), th, 0, 0, false, false, 0, 0)
	require.NoError(t, err)
	defer listener.Close()

	// add a connection
	mysqlConn := mysql.GetTestServerConn(listener)
	mysqlConn.ConnectionID = 1
	mysqlConn.UserData = &mysql.StaticUserData{}
	vh.connections[1] = mysqlConn

	err = vh.ComQuery(mysqlConn, "select 1", func(result *sqltypes.Result) error {
		return nil
	})
	assert.NoError(t, err)

	listener.Shutdown()

	err = vh.ComQuery(mysqlConn, "select 1", func(result *sqltypes.Result) error {
		return nil
	})
	require.EqualError(t, err, "Server shutdown in progress (errno 1053) (sqlstate 08S01)")

	require.True(t, mysqlConn.IsMarkedForClose())
}

func TestGracefulShutdownWithTransaction(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)

	vh := newVtgateHandler(&VTGate{executor: executor, timings: timings, rowsReturned: rowsReturned, rowsAffected: rowsAffected})
	th := &testHandler{}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), th, 0, 0, false, false, 0, 0)
	require.NoError(t, err)
	defer listener.Close()

	// add a connection
	mysqlConn := mysql.GetTestServerConn(listener)
	mysqlConn.ConnectionID = 1
	mysqlConn.UserData = &mysql.StaticUserData{}
	vh.connections[1] = mysqlConn

	err = vh.ComQuery(mysqlConn, "BEGIN", func(result *sqltypes.Result) error {
		return nil
	})
	assert.NoError(t, err)

	err = vh.ComQuery(mysqlConn, "select 1", func(result *sqltypes.Result) error {
		return nil
	})
	assert.NoError(t, err)

	listener.Shutdown()

	err = vh.ComQuery(mysqlConn, "select 1", func(result *sqltypes.Result) error {
		return nil
	})
	assert.NoError(t, err)

	err = vh.ComQuery(mysqlConn, "COMMIT", func(result *sqltypes.Result) error {
		return nil
	})
	assert.NoError(t, err)

	require.False(t, mysqlConn.IsMarkedForClose())

	err = vh.ComQuery(mysqlConn, "select 1", func(result *sqltypes.Result) error {
		return nil
	})
	require.EqualError(t, err, "Server shutdown in progress (errno 1053) (sqlstate 08S01)")

	require.True(t, mysqlConn.IsMarkedForClose())
}
