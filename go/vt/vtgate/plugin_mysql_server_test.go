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
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/binlogacl"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

func (th *testHandler) ComQueryMulti(c *mysql.Conn, sql string, callback func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) error {
	qries, err := th.Env().Parser().SplitStatementToPieces(sql)
	if err != nil {
		return err
	}
	for i, query := range qries {
		firstPacket := true
		err = th.ComQuery(c, query, func(result *sqltypes.Result) error {
			err = callback(sqltypes.QueryResponse{QueryResult: result}, i < len(qries)-1, firstPacket)
			firstPacket = false
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (th *testHandler) ComPrepare(*mysql.Conn, string) ([]*querypb.Field, uint16, error) {
	return nil, 0, nil
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

func (th *testHandler) ComBinlogDumpGTID(c *mysql.Conn, logFile string, logPos uint64, gtidSet replication.GTIDSet, flags uint16) error {
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
	require.NoError(t, err, "Failed to create temp file")
	os.Remove(unixSocket.Name())

	l, err := newMysqlUnixSocket(unixSocket.Name(), authServer, th)
	require.NoError(t, err)
	defer l.Close()
	go l.Accept()

	params := &mysql.ConnParams{
		UnixSocket: unixSocket.Name(),
		Uname:      "user1",
		Pass:       "password1",
	}

	c, err := mysql.Connect(t.Context(), params)
	require.NoError(t, err)
	c.Close()
}

func TestConnectionStaleUnixSocket(t *testing.T) {
	th := &testHandler{}

	authServer := newTestAuthServerStatic()

	// First let's create a file. In this way, we simulate
	// having a stale socket on disk that needs to be cleaned up.
	unixSocket, err := os.CreateTemp("", "mysql_vitess_test.sock")
	require.NoError(t, err, "Failed to create temp file")

	l, err := newMysqlUnixSocket(unixSocket.Name(), authServer, th)
	require.NoError(t, err)
	defer l.Close()
	go l.Accept()

	params := &mysql.ConnParams{
		UnixSocket: unixSocket.Name(),
		Uname:      "user1",
		Pass:       "password1",
	}

	c, err := mysql.Connect(t.Context(), params)
	require.NoError(t, err)
	c.Close()
}

func TestConnectionRespectsExistingUnixSocket(t *testing.T) {
	th := &testHandler{}

	authServer := newTestAuthServerStatic()

	unixSocket, err := os.CreateTemp("", "mysql_vitess_test.sock")
	require.NoError(t, err, "Failed to create temp file")
	os.Remove(unixSocket.Name())

	l, err := newMysqlUnixSocket(unixSocket.Name(), authServer, th)
	require.NoError(t, err)
	defer l.Close()
	go l.Accept()
	_, err = newMysqlUnixSocket(unixSocket.Name(), authServer, th)
	want := "listen unix"
	require.Error(t, err)
	assert.Truef(t, strings.HasPrefix(err.Error(), want), "Error: %v, want prefix %s", err, want)
}

func TestNewConnectionSetsAutocommitStatusFlag(t *testing.T) {
	vh := &vtgateHandler{
		connections: make(map[uint32]*mysql.Conn),
	}

	c := &mysql.Conn{}
	assert.Zero(t, c.StatusFlags, "StatusFlags should be zero before NewConnection")

	vh.NewConnection(c)

	assert.NotEqual(t, uint16(0), c.StatusFlags&mysql.ServerStatusAutocommit,
		"NewConnection should set ServerStatusAutocommit flag to match VTGate's default session state")
}

var newSpanOK = func(ctx context.Context, label string) (trace.Span, context.Context) {
	return trace.NoopSpan{}, context.Background()
}

var newFromStringOK = func(ctx context.Context, spanContext, label string) (trace.Span, context.Context, error) {
	return trace.NoopSpan{}, context.Background(), nil
}

func newFromStringFail(t *testing.T) func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
	return func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
		require.Failf(t, "unexpected call", "we didn't provide a parent span in the sql query. this should not have been called. got: %v", parentSpan)
		return trace.NoopSpan{}, t.Context(), nil
	}
}

func newFromStringError(t *testing.T) func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
	return func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
		return trace.NoopSpan{}, t.Context(), errors.New("")
	}
}

func newFromStringExpect(t *testing.T, expected string) func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
	return func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
		assert.Equal(t, expected, parentSpan)
		return trace.NoopSpan{}, t.Context(), nil
	}
}

func newSpanFail(t *testing.T) func(ctx context.Context, label string) (trace.Span, context.Context) {
	return func(ctx context.Context, label string) (trace.Span, context.Context) {
		require.Fail(t, "we provided a span context but newFromString was not used as expected")
		return trace.NoopSpan{}, t.Context()
	}
}

func TestNoSpanContextPassed(t *testing.T) {
	_, _, err := startSpanTestable(t.Context(), "sql without comments", "someLabel", newSpanOK, newFromStringFail(t))
	assert.NoError(t, err)
}

func TestSpanContextNoPassedInButExistsInString(t *testing.T) {
	_, _, err := startSpanTestable(t.Context(), "SELECT * FROM SOMETABLE WHERE COL = \"/*VT_SPAN_CONTEXT=123*/", "someLabel", newSpanOK, newFromStringFail(t))
	assert.NoError(t, err)
}

func TestSpanContextPassedIn(t *testing.T) {
	_, _, err := startSpanTestable(t.Context(), "/*VT_SPAN_CONTEXT=123*/SQL QUERY", "someLabel", newSpanFail(t), newFromStringOK)
	assert.NoError(t, err)
}

func TestSpanContextPassedInEvenAroundOtherComments(t *testing.T) {
	_, _, err := startSpanTestable(t.Context(), "/*VT_SPAN_CONTEXT=123*/SELECT /*vt+ SCATTER_ERRORS_AS_WARNINGS */ col1, col2 FROM TABLE ", "someLabel",
		newSpanFail(t),
		newFromStringExpect(t, "123"))
	assert.NoError(t, err)
}

func TestSpanContextWithMultipleLeadingComments(t *testing.T) {
	_, _, err := startSpanTestable(t.Context(), "/*VT_SPAN_CONTEXT=123*//*vt+ SCATTER_ERRORS_AS_WARNINGS */ SELECT col1 FROM TABLE", "someLabel",
		newSpanFail(t), newFromStringExpect(t, "123"))
	assert.NoError(t, err)
}

func TestSpanContextNotParsable(t *testing.T) {
	hasRun := false
	_, _, err := startSpanTestable(t.Context(), "/*VT_SPAN_CONTEXT=123*/SQL QUERY", "someLabel",
		func(c context.Context, s string) (trace.Span, context.Context) {
			hasRun = true
			return trace.NoopSpan{}, t.Context()
		},
		newFromStringError(t))
	require.NoError(t, err)
	assert.True(t, hasRun, "Should have continued execution despite failure to parse VT_SPAN_CONTEXT")
}

func TestStartSpanFromPrepare_NoSpanContext(t *testing.T) {
	prepare := &mysql.PrepareData{PrepareStmt: "SELECT 1"}
	_, _, err := startSpanFromPrepareTestable(t.Context(), prepare, "someLabel", newSpanOK, newFromStringFail(t))
	require.NoError(t, err)
	require.NotNil(t, prepare.SpanContext)
	assert.Empty(t, *prepare.SpanContext)
}

func TestStartSpanFromPrepare_WithSpanContext(t *testing.T) {
	prepare := &mysql.PrepareData{PrepareStmt: "/*VT_SPAN_CONTEXT=123*/SELECT 1"}
	_, _, err := startSpanFromPrepareTestable(t.Context(), prepare, "someLabel",
		newSpanFail(t), newFromStringExpect(t, "123"))
	require.NoError(t, err)
	require.NotNil(t, prepare.SpanContext)
	assert.Equal(t, "123", *prepare.SpanContext)
}

func TestStartSpanFromPrepare_CachesSpanContext(t *testing.T) {
	prepare := &mysql.PrepareData{PrepareStmt: "/*VT_SPAN_CONTEXT=456*/SELECT 1"}
	// First call extracts and caches the span context.
	_, _, err := startSpanFromPrepareTestable(t.Context(), prepare, "someLabel",
		newSpanFail(t), newFromStringExpect(t, "456"))
	require.NoError(t, err)
	require.NotNil(t, prepare.SpanContext)
	assert.Equal(t, "456", *prepare.SpanContext)

	// Second call reuses the cached span context (PrepareStmt is not re-parsed).
	prepare.PrepareStmt = "modified query that would not match"
	_, _, err = startSpanFromPrepareTestable(t.Context(), prepare, "someLabel",
		newSpanFail(t), newFromStringExpect(t, "456"))
	assert.NoError(t, err)
}

func TestStartSpanFromPrepare_SpanContextNotParsable(t *testing.T) {
	prepare := &mysql.PrepareData{PrepareStmt: "/*VT_SPAN_CONTEXT=123*/SELECT 1"}
	newFromStringCalls := 0
	newSpanCalls := 0
	_, _, err := startSpanFromPrepareTestable(t.Context(), prepare, "someLabel",
		func(c context.Context, s string) (trace.Span, context.Context) {
			newSpanCalls++
			return trace.NoopSpan{}, t.Context()
		},
		func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
			newFromStringCalls++
			return trace.NoopSpan{}, t.Context(), errors.New("parse error")
		})
	require.NoError(t, err)
	assert.Equal(t, 1, newFromStringCalls, "newFromString should be called on first execution")
	assert.Equal(t, 1, newSpanCalls, "newSpan should be called as fallback")
	// After the first failure, the cached SpanContext should be cleared so
	// subsequent executions skip the parse attempt entirely.
	require.NotNil(t, prepare.SpanContext)
	assert.Empty(t, *prepare.SpanContext)

	// Second execution should not call newFromString again.
	newFromStringCalls = 0
	newSpanCalls = 0
	_, _, err = startSpanFromPrepareTestable(t.Context(), prepare, "someLabel",
		func(c context.Context, s string) (trace.Span, context.Context) {
			newSpanCalls++
			return trace.NoopSpan{}, t.Context()
		},
		func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
			newFromStringCalls++
			return trace.NoopSpan{}, t.Context(), errors.New("parse error")
		})
	require.NoError(t, err)
	assert.Equal(t, 0, newFromStringCalls, "newFromString should not be called after cached failure")
	assert.Equal(t, 1, newSpanCalls, "newSpan should be called directly")
}

func newTestAuthServerStatic() *mysql.AuthServerStatic {
	jsonConfig := "{\"user1\":{\"Password\":\"password1\", \"UserData\":\"userData1\", \"SourceHost\":\"localhost\"}}"
	return mysql.NewAuthServerStatic("", jsonConfig, 0)
}

func TestDefaultWorkloadEmpty(t *testing.T) {
	vh := &vtgateHandler{}
	mysqlDefaultWorkload = int32(querypb.ExecuteOptions_OLTP)
	sess := vh.session(&mysql.Conn{})
	require.Equal(t, querypb.ExecuteOptions_OLTP, sess.Options.Workload, "Expected default workload OLTP")
}

func TestDefaultWorkloadOLAP(t *testing.T) {
	vh := &vtgateHandler{}
	mysqlDefaultWorkload = int32(querypb.ExecuteOptions_OLAP)
	sess := vh.session(&mysql.Conn{})
	require.Equal(t, querypb.ExecuteOptions_OLAP, sess.Options.Workload, "Expected default workload OLAP")
}

func TestInitTLSConfigWithoutServerCA(t *testing.T) {
	testInitTLSConfig(t, false)
}

func TestInitTLSConfigWithServerCA(t *testing.T) {
	testInitTLSConfig(t, true)
}

func testInitTLSConfig(t *testing.T, serverCA bool) {
	synctest.Test(t, func(t *testing.T) {
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
			require.NoError(t, err)
		}

		serverConfig := srv.tcpListener.TLSConfig.Load()
		require.NotNil(t, serverConfig, "init tls config shouldn't create nil server config")

		srv.sigChan <- syscall.SIGHUP

		// wait for signal handler
		synctest.Wait()

		require.NotEqual(t, serverConfig, srv.tcpListener.TLSConfig.Load(), "init tls config should have been recreated after SIGHUP")
	})
}

// TestKillMethods test the mysql plugin for kill method calls.
func TestKillMethods(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)
	vh := newVtgateHandler(&VTGate{executor: executor})

	// connection does not exist
	err := vh.KillQuery(12345)
	require.ErrorContains(t, err, "Unknown thread id: 12345 (errno 1094) (sqlstate HY000)")

	err = vh.KillConnection(t.Context(), 12345)
	require.ErrorContains(t, err, "Unknown thread id: 12345 (errno 1094) (sqlstate HY000)")

	// add a connection
	mysqlConn := mysql.GetTestConn()
	mysqlConn.ConnectionID = 1
	vh.connections[1] = mysqlConn

	// connection exists

	// updating context.
	cancelCtx, cancelFunc := context.WithCancel(t.Context())
	mysqlConn.UpdateCancelCtx(cancelFunc)

	// kill query
	err = vh.KillQuery(1)
	require.NoError(t, err)
	require.EqualError(t, cancelCtx.Err(), "context canceled")

	// updating context.
	cancelCtx, cancelFunc = context.WithCancel(t.Context())
	mysqlConn.UpdateCancelCtx(cancelFunc)

	// kill connection
	err = vh.KillConnection(t.Context(), 1)
	require.NoError(t, err)
	require.EqualError(t, cancelCtx.Err(), "context canceled")
	require.True(t, mysqlConn.IsMarkedForClose())
}

func TestComQueryMulti(t *testing.T) {
	testcases := []struct {
		name           string
		sql            string
		olap           bool
		queryResponses []sqltypes.QueryResponse
		more           []bool
		firstPacket    []bool
		errExpected    bool
	}{
		{
			name: "Empty query",
			sql:  "",
			queryResponses: []sqltypes.QueryResponse{
				{QueryResult: nil, QueryError: sqlerror.NewSQLErrorFromError(sqlparser.ErrEmpty)},
			},
			more:        []bool{false},
			firstPacket: []bool{true},
			errExpected: false,
		}, {
			name: "Single query",
			sql:  "select 1",
			queryResponses: []sqltypes.QueryResponse{
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "1",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.NewInt64(1),
							},
						},
					},
					QueryError: nil,
				},
			},
			more:        []bool{false},
			firstPacket: []bool{true},
			errExpected: false,
		}, {
			name: "Multiple queries - success",
			sql:  "select 1; select 2; select 3;",
			queryResponses: []sqltypes.QueryResponse{
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "1",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.NewInt64(1),
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "2",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.NewInt64(2),
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "3",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.NewInt64(3),
							},
						},
					},
					QueryError: nil,
				},
			},
			more:        []bool{true, true, false},
			firstPacket: []bool{true, true, true},
			errExpected: false,
		}, {
			name: "Multiple queries - failure",
			sql:  "select 1; select 2; parsing error; select 3;",
			queryResponses: []sqltypes.QueryResponse{
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "1",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.NewInt64(1),
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "2",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.NewInt64(2),
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: nil,
					QueryError:  errors.New("syntax error at position 8 near 'parsing' (errno 1105) (sqlstate HY000)"),
				},
			},
			more:        []bool{true, true, false},
			firstPacket: []bool{true, true, true},
			errExpected: false,
		}, {
			name:           "Empty query - olap",
			sql:            "",
			olap:           true,
			queryResponses: []sqltypes.QueryResponse{},
			more:           []bool{false},
			firstPacket:    []bool{true},
			errExpected:    true,
		}, {
			name: "Single query - olap",
			sql:  "select 1",
			olap: true,
			queryResponses: []sqltypes.QueryResponse{
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "1",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "1",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "1",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.NewInt64(1),
							},
						},
					},
					QueryError: nil,
				},
			},
			more:        []bool{false, false, false},
			firstPacket: []bool{true, false, false},
			errExpected: false,
		}, {
			name: "Multiple queries - olap - success",
			sql:  "select 1; select 2; select 3;",
			olap: true,
			queryResponses: []sqltypes.QueryResponse{
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "1",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "1",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "1",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.NewInt64(1),
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "2",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "2",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "2",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.NewInt64(2),
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "3",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "3",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "3",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.NewInt64(3),
							},
						},
					},
					QueryError: nil,
				},
			},
			more:        []bool{true, true, true, true, true, true, false, false, false},
			firstPacket: []bool{true, false, false, true, false, false, true, false, false},
			errExpected: false,
		}, {
			name: "Multiple queries - olap - failure",
			sql:  "select 1; select 2; parsing error; select 3;",
			olap: true,
			queryResponses: []sqltypes.QueryResponse{
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "1",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "1",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "1",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.NewInt64(1),
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "2",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "2",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: &sqltypes.Result{
						Fields: []*querypb.Field{
							{
								Name:    "2",
								Type:    sqltypes.Int64,
								Flags:   uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG),
								Charset: collations.CollationBinaryID,
							},
						},
						Rows: [][]sqltypes.Value{
							{
								sqltypes.NewInt64(2),
							},
						},
					},
					QueryError: nil,
				},
				{
					QueryResult: nil,
					QueryError:  errors.New("syntax error at position 8 near 'parsing' (errno 1105) (sqlstate HY000)"),
				},
			},
			more:        []bool{true, true, true, true, true, true, false},
			firstPacket: []bool{true, false, false, true, false, false, true},
			errExpected: false,
		},
	}

	executor, _, _, _, _ := createExecutorEnv(t)
	th := &testHandler{}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	defer listener.Close()

	// add a connection
	mysqlConn := mysql.GetTestServerConn(listener)
	mysqlConn.ConnectionID = 1
	mysqlConn.UserData = &mysql.StaticUserData{}
	mysqlConn.Capabilities = mysqlConn.Capabilities | mysql.CapabilityClientMultiStatements
	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	vh.connections[1] = mysqlConn
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			vh.session(mysqlConn).Options.Workload = querypb.ExecuteOptions_OLTP
			if tt.olap {
				vh.session(mysqlConn).Options.Workload = querypb.ExecuteOptions_OLAP
			}
			idx := 0
			err = vh.ComQueryMulti(mysqlConn, tt.sql, func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error {
				assert.True(t, tt.queryResponses[idx].QueryResult.Equal(qr.QueryResult), "Result Got: %v", qr.QueryResult)
				if tt.queryResponses[idx].QueryError != nil {
					assert.Equal(t, tt.queryResponses[idx].QueryError.Error(), qr.QueryError.Error(), "Error Got: %v", qr.QueryError)
				} else {
					require.NoError(t, qr.QueryError, "Error Got: %v", qr.QueryError)
				}
				assert.Equal(t, tt.more[idx], more, idx)
				assert.Equal(t, tt.firstPacket[idx], firstPacket, idx)
				idx++
				return nil
			})
			assert.Equal(t, tt.errExpected, err != nil)
			assert.Equal(t, len(tt.queryResponses), idx)
		})
	}
}

func TestSlowQueryStatusFlagsComQuery(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)

	oldThreshold := slowQueryThreshold
	slowQueryThreshold = 5 * time.Millisecond
	t.Cleanup(func() {
		slowQueryThreshold = oldThreshold
		sbc1.ExecDelayResponse = 0
	})

	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
	})

	th := &testHandler{}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	defer listener.Close()

	mysqlConn := mysql.GetTestServerConn(listener)
	mysqlConn.ConnectionID = 1
	mysqlConn.UserData = &mysql.StaticUserData{}

	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	vh.connections[1] = mysqlConn

	sbc1.ExecDelayResponse = 20 * time.Millisecond
	err = vh.ComQuery(mysqlConn, "select id from user where id = 1", func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	assert.NotZero(t, mysqlConn.StatusFlags&mysql.ServerQueryWasSlow)

	sbc1.ExecDelayResponse = 0
	err = vh.ComQuery(mysqlConn, "select id from user where id = 1", func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	assert.Zero(t, mysqlConn.StatusFlags&mysql.ServerQueryWasSlow)
}

// waitForConnectionsClosed waits until the handler has run ConnectionClosed
// for every wire connection. Tests that accept real connections must drain
// them before returning: the server-side connection goroutine reads package
// globals (e.g. mysqlQueryTimeout) during teardown, which races with later
// tests mutating them.
func waitForConnectionsClosed(t *testing.T, vh *vtgateHandler) {
	t.Helper()
	require.Eventually(t, func() bool {
		return vh.numConnections() == 0
	}, 30*time.Second, time.Millisecond)
}

func TestSlowQueryStatusFlagsComQueryOKOnlyOLAPWire(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)

	oldThreshold := slowQueryThreshold
	oldDefaultWorkload := mysqlDefaultWorkload
	slowQueryThreshold = 5 * time.Millisecond
	mysqlDefaultWorkload = int32(querypb.ExecuteOptions_OLAP)
	t.Cleanup(func() {
		slowQueryThreshold = oldThreshold
		mysqlDefaultWorkload = oldDefaultWorkload
		sbc1.ExecDelayResponse = 0
	})

	sbc1.SetResults([]*sqltypes.Result{{RowsAffected: 1}})
	sbc1.ExecDelayResponse = 20 * time.Millisecond

	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), vh, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	defer listener.Close()
	defer waitForConnectionsClosed(t, vh)

	go listener.Accept()

	addr := listener.Addr().(*net.TCPAddr)
	params := &mysql.ConnParams{
		Host:  addr.IP.String(),
		Port:  addr.Port,
		Uname: "user1",
		Pass:  "password1",
	}

	conn, err := mysql.Connect(t.Context(), params)
	require.NoError(t, err)
	defer conn.Close()

	result, err := conn.ExecuteFetch("update user set name = 'foo' where id = 1", 100, true)
	require.NoError(t, err)
	assert.Empty(t, result.Fields)
	assert.NotZero(t, result.StatusFlags&mysql.ServerQueryWasSlow)
}

func TestSlowQueryStatusFlagsComQueryMultiOKOnlyOLAPFallbackWire(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)

	oldThreshold := slowQueryThreshold
	oldDefaultWorkload := mysqlDefaultWorkload
	slowQueryThreshold = 5 * time.Millisecond
	mysqlDefaultWorkload = int32(querypb.ExecuteOptions_OLAP)
	t.Cleanup(func() {
		slowQueryThreshold = oldThreshold
		mysqlDefaultWorkload = oldDefaultWorkload
		sbc1.ExecDelayResponse = 0
	})

	sbc1.SetResults([]*sqltypes.Result{{RowsAffected: 1}})
	sbc1.ExecDelayResponse = 20 * time.Millisecond

	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), vh, 0, 0, false, false, 0, 0, true)
	require.NoError(t, err)
	defer listener.Close()
	defer waitForConnectionsClosed(t, vh)

	go listener.Accept()

	addr := listener.Addr().(*net.TCPAddr)
	params := &mysql.ConnParams{
		Host:  addr.IP.String(),
		Port:  addr.Port,
		Uname: "user1",
		Pass:  "password1",
	}

	conn, err := mysql.Connect(t.Context(), params)
	require.NoError(t, err)
	defer conn.Close()

	result, err := conn.ExecuteFetch("update user set name = 'foo' where id = 1", 100, true)
	require.NoError(t, err)
	assert.Empty(t, result.Fields)
	assert.NotZero(t, result.StatusFlags&mysql.ServerQueryWasSlow)
}

func TestComQueryMultiOLAPDeferredOKRefreshesTransactionStatusWire(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)

	oldDefaultWorkload := mysqlDefaultWorkload
	mysqlDefaultWorkload = int32(querypb.ExecuteOptions_OLAP)
	t.Cleanup(func() {
		mysqlDefaultWorkload = oldDefaultWorkload
	})

	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), vh, 0, 0, false, false, 0, 0, true)
	require.NoError(t, err)
	defer listener.Close()
	defer waitForConnectionsClosed(t, vh)

	go listener.Accept()

	addr := listener.Addr().(*net.TCPAddr)
	params := &mysql.ConnParams{
		Host:  addr.IP.String(),
		Port:  addr.Port,
		Uname: "user1",
		Pass:  "password1",
		Flags: mysql.CapabilityClientMultiStatements,
	}

	conn, err := mysql.Connect(t.Context(), params)
	require.NoError(t, err)
	defer conn.Close()

	result, more, err := conn.ExecuteFetchMulti("select 1; begin", 100, true)
	require.NoError(t, err)
	require.True(t, more)
	assert.NotEmpty(t, result.Fields)
	assert.Zero(t, result.StatusFlags&mysql.ServerStatusInTrans)

	result, more, _, err = conn.ReadQueryResult(100, true)
	require.NoError(t, err)
	require.False(t, more)
	assert.Empty(t, result.Fields)
	assert.NotZero(t, result.StatusFlags&mysql.ServerStatusInTrans)
}

func TestSlowQueryStatusFlagsComStmtExecute(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)

	oldThreshold := slowQueryThreshold
	slowQueryThreshold = 5 * time.Millisecond
	t.Cleanup(func() {
		slowQueryThreshold = oldThreshold
		sbc1.ExecDelayResponse = 0
	})

	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
	})

	th := &testHandler{}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	defer listener.Close()

	mysqlConn := mysql.GetTestServerConn(listener)
	mysqlConn.ConnectionID = 1
	mysqlConn.UserData = &mysql.StaticUserData{}

	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	vh.connections[1] = mysqlConn

	prepare := &mysql.PrepareData{
		PrepareStmt: "select id from user where id = 1",
		BindVars:    map[string]*querypb.BindVariable{},
	}

	sbc1.ExecDelayResponse = 20 * time.Millisecond
	err = vh.ComStmtExecute(mysqlConn, prepare, func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	assert.NotZero(t, mysqlConn.StatusFlags&mysql.ServerQueryWasSlow)

	sbc1.ExecDelayResponse = 0
	err = vh.ComStmtExecute(mysqlConn, prepare, func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	assert.Zero(t, mysqlConn.StatusFlags&mysql.ServerQueryWasSlow)
}

func TestDeferFirstOKOnlyResultForwardsRowChunksAfterFields(t *testing.T) {
	fields := sqltypes.MakeTestFields("id", "int64")
	input := []*sqltypes.Result{
		{Fields: fields},
		{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(1)}}},
		{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(2)}}},
	}
	var results []*sqltypes.Result
	callback, deferredResult := deferFirstOKOnlyResult(func(result *sqltypes.Result) error {
		results = append(results, result)
		return nil
	})

	for _, result := range input {
		require.NoError(t, callback(result))
	}

	require.Nil(t, deferredResult())
	assertOLAPRowChunksAfterFields(t, fields, results)
}

func TestDeferFirstOKOnlyResultDefersOnlyFirstOKResult(t *testing.T) {
	okResult := &sqltypes.Result{RowsAffected: 1}
	callback, deferredResult := deferFirstOKOnlyResult(func(result *sqltypes.Result) error {
		require.Failf(t, "callback called for deferred OK-only result", "%v", result)
		return nil
	})

	err := callback(okResult)
	require.NoError(t, err)
	assert.Same(t, okResult, deferredResult())
}

func assertOLAPRowChunksAfterFields(t *testing.T, fields []*querypb.Field, results []*sqltypes.Result) {
	t.Helper()

	require.Len(t, results, 3)
	assert.Equal(t, fields, results[0].Fields)
	assert.Empty(t, results[0].Rows)
	assert.Empty(t, results[1].Fields)
	assert.Equal(t, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, results[1].Rows)
	assert.Empty(t, results[2].Fields)
	assert.Equal(t, [][]sqltypes.Value{{sqltypes.NewInt64(2)}}, results[2].Rows)
}

func TestSlowQueryStatusFlagsComQueryMulti(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)

	oldThreshold := slowQueryThreshold
	slowQueryThreshold = 5 * time.Millisecond
	t.Cleanup(func() {
		slowQueryThreshold = oldThreshold
		sbc1.ExecDelayResponse = 0
	})

	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
	})
	sbc1.ExecDelayResponse = 20 * time.Millisecond

	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), vh, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	defer listener.Close()
	defer waitForConnectionsClosed(t, vh)

	go listener.Accept()

	addr := listener.Addr().(*net.TCPAddr)
	params := &mysql.ConnParams{
		Host:  addr.IP.String(),
		Port:  addr.Port,
		Uname: "user1",
		Pass:  "password1",
		Flags: mysql.CapabilityClientMultiStatements,
	}

	conn, err := mysql.Connect(t.Context(), params)
	require.NoError(t, err)
	defer conn.Close()

	result, more, err := conn.ExecuteFetchMulti("select id from user where id = 1; select 1", 100, true)
	require.NoError(t, err)
	require.True(t, more)
	assert.NotZero(t, result.StatusFlags&mysql.ServerQueryWasSlow)

	result, more, _, err = conn.ReadQueryResult(100, true)
	require.NoError(t, err)
	require.False(t, more)
	assert.Zero(t, result.StatusFlags&mysql.ServerQueryWasSlow)
}

func TestSlowQueryStatusFlagsStreamExecuteMultiOLAP(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)

	oldThreshold := slowQueryThreshold
	slowQueryThreshold = 5 * time.Millisecond
	t.Cleanup(func() {
		slowQueryThreshold = oldThreshold
		sbc1.ExecDelayResponse = 0
	})

	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
	})
	sbc1.ExecDelayResponse = 20 * time.Millisecond

	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), &testHandler{}, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	defer listener.Close()

	mysqlConn := mysql.GetTestServerConn(listener)
	mysqlConn.ConnectionID = 1
	mysqlConn.UserData = &mysql.StaticUserData{}
	mysqlCtx := &vtgateMySQLConnection{handler: vh, conn: mysqlConn}
	session := &vtgatepb.Session{
		Autocommit:           true,
		EnableSystemSettings: true,
		TargetString:         "TestExecutor",
		Options: &querypb.ExecuteOptions{
			Workload: querypb.ExecuteOptions_OLAP,
		},
	}

	seenOKOnly := false
	session, err = vh.streamExecuteMultiQuery(t.Context(), mysqlConn, mysqlCtx, session, "select id from user where id = 1; set autocommit = 1", func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error {
		if firstPacket && len(qr.QueryResult.Fields) == 0 {
			seenOKOnly = true
			assert.False(t, more)
			assert.Zero(t, mysqlConn.StatusFlags&mysql.ServerQueryWasSlow)
		}
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, session)
	assert.True(t, seenOKOnly)
	assert.Equal(t, []bool{true, false}, mysqlCtx.slowQueryStates)
}

func TestSlowQueryStatusFlagsComQueryMultiOLAPErrorAfterSlowRowsWire(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)

	oldThreshold := slowQueryThreshold
	oldDefaultWorkload := mysqlDefaultWorkload
	slowQueryThreshold = 5 * time.Millisecond
	mysqlDefaultWorkload = int32(querypb.ExecuteOptions_OLAP)
	t.Cleanup(func() {
		slowQueryThreshold = oldThreshold
		mysqlDefaultWorkload = oldDefaultWorkload
		sbc1.ExecDelayResponse = 0
	})

	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
	})
	sbc1.ExecDelayResponse = 20 * time.Millisecond

	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), vh, 0, 0, false, false, 0, 0, true)
	require.NoError(t, err)
	defer listener.Close()
	defer waitForConnectionsClosed(t, vh)

	go listener.Accept()

	addr := listener.Addr().(*net.TCPAddr)
	params := &mysql.ConnParams{
		Host:  addr.IP.String(),
		Port:  addr.Port,
		Uname: "user1",
		Pass:  "password1",
		Flags: mysql.CapabilityClientMultiStatements,
	}

	conn, err := mysql.Connect(t.Context(), params)
	require.NoError(t, err)
	defer conn.Close()

	result, more, err := conn.ExecuteFetchMulti("select id from user where id = 1; select from", 100, true)
	require.NoError(t, err)
	require.True(t, more)
	assert.NotZero(t, result.StatusFlags&mysql.ServerQueryWasSlow)

	result, more, _, err = conn.ReadQueryResult(100, true)
	require.Error(t, err)
	assert.False(t, more)
	assert.Nil(t, result)
}

func TestStreamExecuteMultiOLAPTimeoutUsesParentContextPerStatement(t *testing.T) {
	executor, sbc1, _, _, _ := createExecutorEnv(t)

	oldTimeout := mysqlQueryTimeout
	mysqlQueryTimeout = time.Second
	sbc1.ExecDelayResponse = time.Millisecond
	t.Cleanup(func() {
		mysqlQueryTimeout = oldTimeout
		sbc1.ExecDelayResponse = 0
	})

	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	mysqlConn := mysql.GetTestServerConn(&mysql.Listener{})
	mysqlConn.ConnectionID = 1
	mysqlConn.UserData = &mysql.StaticUserData{}
	mysqlCtx := &vtgateMySQLConnection{handler: vh, conn: mysqlConn}
	session := &vtgatepb.Session{
		Autocommit:           true,
		EnableSystemSettings: true,
		TargetString:         "TestExecutor",
		Options: &querypb.ExecuteOptions{
			Workload: querypb.ExecuteOptions_OLAP,
		},
	}

	var moreFlags []bool
	session, err := vh.streamExecuteMultiQuery(t.Context(), mysqlConn, mysqlCtx, session, "select id from user where id = 1; select id from user where id = 1", func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error {
		if qr.QueryError != nil {
			return qr.QueryError
		}
		if firstPacket {
			moreFlags = append(moreFlags, more)
		}
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, session)
	assert.Equal(t, []bool{true, false}, moreFlags)
}

func TestGracefulShutdown(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)

	vh := newVtgateHandler(&VTGate{executor: executor, timings: timings, rowsReturned: rowsReturned, rowsAffected: rowsAffected, queryTextCharsProcessed: queryTextCharsProcessed})
	th := &testHandler{}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), th, 0, 0, false, false, 0, 0, false)
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
	require.NoError(t, err)
	err = vh.ComQueryMulti(mysqlConn, "select 1", func(res sqltypes.QueryResponse, more bool, firstPacket bool) error {
		return nil
	})
	require.NoError(t, err)

	listener.Shutdown()

	err = vh.ComQuery(mysqlConn, "select 1", func(result *sqltypes.Result) error {
		return nil
	})
	require.EqualError(t, err, "Server shutdown in progress (errno 1053) (sqlstate 08S01)")
	err = vh.ComQueryMulti(mysqlConn, "select 1", func(res sqltypes.QueryResponse, more bool, firstPacket bool) error {
		return nil
	})
	require.EqualError(t, err, "Server shutdown in progress (errno 1053) (sqlstate 08S01)")

	require.True(t, mysqlConn.IsMarkedForClose())
}

// TestShutdownDrainKeepsHeartbeatUntilDrained verifies that the temp-table
// keepalive is not cancelled until draining has finished. During
// --mysql-server-drain-onterm draining, existing client connections stay
// serviceable, so cancelling their keepalives early could let their reserved
// connections (and temp tables) be reclaimed by the tablet timeout.
func TestShutdownDrainKeepsHeartbeatUntilDrained(t *testing.T) {
	origDrain := mysqlDrainOnTerm
	mysqlDrainOnTerm = true
	t.Cleanup(func() { mysqlDrainOnTerm = origDrain })

	vh := &vtgateHandler{connections: map[uint32]*mysql.Conn{}}
	vh.connections[1] = &mysql.Conn{ConnectionID: 1}

	// Record how many connections were still connected when the heartbeat was
	// cancelled: it must be zero (drain already complete).
	var connsAtCancel atomic.Int64
	connsAtCancel.Store(-1)
	srv := &mysqlServer{
		vtgateHandle: vh,
		heartbeatCancel: func() {
			connsAtCancel.Store(int64(vh.numConnections()))
		},
	}

	// The client disconnects partway through the drain loop.
	go func() {
		time.Sleep(50 * time.Millisecond)
		vh.mu.Lock()
		delete(vh.connections, 1)
		vh.mu.Unlock()
	}()

	srv.shutdownMysqlProtocolAndDrain()
	require.Equal(t, int64(0), connsAtCancel.Load(),
		"the heartbeat must be cancelled only after all client connections have drained")
}

func TestComBinlogDumpGTID(t *testing.T) {
	// Save and restore original flag values
	originalBinlogDumpEnabled := enableBinlogDump.Get()
	defer enableBinlogDump.Set(originalBinlogDumpEnabled)

	// Enable binlog dump and authorize all users for this test
	enableBinlogDump.Set(true)
	binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("%"))
	defer binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers(""))

	// Create executor environment with sandbox connections
	executor, sbc1, _, _, _ := createExecutorEnv(t)

	// Create VTGate with the gateway
	vtg := newVTGate(executor, executor.resolver, nil, nil, executor.scatterConn.gateway)

	// Get the tablet alias from the sandbox connection
	tabletAlias := sbc1.Tablet().Alias

	// Create the vtgate handler
	vh := newVtgateHandler(vtg)
	th := &testHandler{}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	defer listener.Close()

	// Create a connection
	mysqlConn := mysql.GetTestServerConn(listener)
	mysqlConn.ConnectionID = 1
	mysqlConn.User = "testuser"
	mysqlConn.UserData = &mysql.StaticUserData{Username: "testuser"}
	vh.connections[1] = mysqlConn

	binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("%"))
	defer binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers(""))

	t.Run("unauthorized user", func(t *testing.T) {
		binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("cdcUser"))
		defer binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("%"))

		mysqlConn.User = "regularUser"
		mysqlConn.UserData = &mysql.StaticUserData{Username: "regularUser"}
		defer func() {
			mysqlConn.User = "testuser"
			mysqlConn.UserData = &mysql.StaticUserData{Username: "testuser"}
		}()

		targetString := "TestExecutor:-20@primary|" + topoproto.TabletAliasString(tabletAlias)
		vh.session(mysqlConn).TargetString = targetString

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not authorized to perform binlog dump operations")
	})

	t.Run("no target specified", func(t *testing.T) {
		// Clear any previous target
		vh.session(mysqlConn).TargetString = ""

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no target specified")
	})

	t.Run("target from session TargetString", func(t *testing.T) {
		// Set up empty responses
		sbc1.BinlogDumpError = nil
		sbc1.BinlogDumpResponses = []*binlogdatapb.BinlogDumpResponse{}

		// Set the session target (normally set by USE statement or parsed from username)
		targetString := "TestExecutor:-20@primary|" + topoproto.TabletAliasString(tabletAlias)
		vh.session(mysqlConn).TargetString = targetString
		mysqlConn.User = "testuser"

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.NoError(t, err)
	})

	t.Run("target without tablet alias routes via gateway", func(t *testing.T) {
		sbc1.BinlogDumpError = nil
		sbc1.BinlogDumpResponses = []*binlogdatapb.BinlogDumpResponse{}

		vh.session(mysqlConn).TargetString = "TestExecutor:-20@primary"

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.NoError(t, err)
	})

	t.Run("binlog dump with error from tablet", func(t *testing.T) {
		// Set up an error response
		sbc1.BinlogDumpError = errors.New("test binlog error")
		defer func() { sbc1.BinlogDumpError = nil }()

		targetString := "TestExecutor:-20@primary|" + topoproto.TabletAliasString(tabletAlias)
		vh.session(mysqlConn).TargetString = targetString

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "test binlog error")
	})

	t.Run("binlog dump with empty response succeeds", func(t *testing.T) {
		// Reset error and set up empty responses (no events to write)
		sbc1.BinlogDumpError = nil
		sbc1.BinlogDumpResponses = []*binlogdatapb.BinlogDumpResponse{}

		vh.session(mysqlConn).TargetString = "TestExecutor:-20@primary"

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.NoError(t, err)
	})

	t.Run("binlog dump with GTID set and empty response", func(t *testing.T) {
		// Reset error
		sbc1.BinlogDumpError = nil
		sbc1.BinlogDumpResponses = []*binlogdatapb.BinlogDumpResponse{}

		targetString := "TestExecutor:-20@primary|" + topoproto.TabletAliasString(tabletAlias)
		vh.session(mysqlConn).TargetString = targetString

		gtidSet, err := replication.ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-100")
		require.NoError(t, err)

		err = vh.ComBinlogDumpGTID(mysqlConn, "", 4, gtidSet, 0)
		require.NoError(t, err)
	})

	t.Run("invalid tablet alias in target", func(t *testing.T) {
		vh.session(mysqlConn).TargetString = "TestExecutor:-20@primary|invalid-alias"

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.Error(t, err)
		// The error could be about parsing the alias or not finding the tablet
		assert.True(t, strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "not found"),
			"Expected error about invalid alias or tablet not found, got: %v", err)
	})

	t.Run("nonexistent tablet alias", func(t *testing.T) {
		// Use a valid format but non-existent alias
		vh.session(mysqlConn).TargetString = "TestExecutor:-20@primary|aa-9999999"

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("filename is rejected", func(t *testing.T) {
		vh.session(mysqlConn).TargetString = "TestExecutor:-20@primary"

		err := vh.ComBinlogDumpGTID(mysqlConn, "binlog.000003", 4, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "binlog filename is not supported")
	})

	t.Run("filename is rejected even with tablet alias", func(t *testing.T) {
		targetString := "TestExecutor:-20@primary|" + topoproto.TabletAliasString(tabletAlias)
		vh.session(mysqlConn).TargetString = targetString

		err := vh.ComBinlogDumpGTID(mysqlConn, "binlog.000003", 4, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "binlog filename is not supported")
	})

	t.Run("position below minimum is rejected", func(t *testing.T) {
		vh.session(mysqlConn).TargetString = "TestExecutor:-20@primary"

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 3, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Client requested source to start replication from position < 4")
	})

	t.Run("non-default position is rejected", func(t *testing.T) {
		vh.session(mysqlConn).TargetString = "TestExecutor:-20@primary"

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 1234, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "only binlog position 4 is supported")
	})

	t.Run("non-default position is rejected even with tablet alias", func(t *testing.T) {
		targetString := "TestExecutor:-20@primary|" + topoproto.TabletAliasString(tabletAlias)
		vh.session(mysqlConn).TargetString = targetString

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 5, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "only binlog position 4 is supported")
	})

	t.Run("default position is allowed", func(t *testing.T) {
		sbc1.BinlogDumpError = nil
		sbc1.BinlogDumpResponses = []*binlogdatapb.BinlogDumpResponse{}

		vh.session(mysqlConn).TargetString = "TestExecutor:-20@primary"

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.NoError(t, err)
	})
}

func TestBinlogDumpACL(t *testing.T) {
	// Save and restore original flag values
	originalBinlogDumpEnabled := enableBinlogDump.Get()
	defer enableBinlogDump.Set(originalBinlogDumpEnabled)

	originalAuthorizedUsers := binlogacl.AuthorizedBinlogUsers.Get()
	defer binlogacl.AuthorizedBinlogUsers.Set(originalAuthorizedUsers)

	// Create executor environment with sandbox connections
	executor, sbc1, _, _, _ := createExecutorEnv(t)

	// Create VTGate with the gateway
	vtg := newVTGate(executor, executor.resolver, nil, nil, executor.scatterConn.gateway)

	// Get the tablet alias from the sandbox connection
	tabletAlias := sbc1.Tablet().Alias

	// Create the vtgate handler
	vh := newVtgateHandler(vtg)
	th := &testHandler{}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	defer listener.Close()

	// Create a connection with a specific user
	mysqlConn := mysql.GetTestServerConn(listener)
	mysqlConn.ConnectionID = 1
	mysqlConn.UserData = &mysql.StaticUserData{Username: "cdcuser"}
	mysqlConn.User = "cdcuser"
	vh.connections[1] = mysqlConn

	// Set up a valid target
	targetString := "TestExecutor:-20@primary|" + topoproto.TabletAliasString(tabletAlias)
	vh.session(mysqlConn).TargetString = targetString

	// Set up empty responses for successful cases
	sbc1.BinlogDumpError = nil
	sbc1.BinlogDumpResponses = []*binlogdatapb.BinlogDumpResponse{}

	t.Run("binlog dump disabled globally", func(t *testing.T) {
		enableBinlogDump.Set(false)

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "binlog dump is disabled")
	})

	t.Run("binlog dump enabled but user not authorized", func(t *testing.T) {
		enableBinlogDump.Set(true)
		// Don't set any authorized users (empty = no one authorized)
		binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers(""))

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not authorized to perform binlog dump")
		assert.Contains(t, err.Error(), "cdcuser")
	})

	t.Run("binlog dump enabled and user authorized via explicit list", func(t *testing.T) {
		enableBinlogDump.Set(true)
		binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("cdcuser,otheruser"))

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.NoError(t, err)
	})

	t.Run("binlog dump enabled and all users authorized via wildcard", func(t *testing.T) {
		enableBinlogDump.Set(true)
		binlogacl.AuthorizedBinlogUsers.Set(binlogacl.NewAuthorizedBinlogUsers("%"))

		err := vh.ComBinlogDumpGTID(mysqlConn, "", 4, nil, 0)
		require.NoError(t, err)
	})

	t.Run("ComBinlogDump returns unimplemented error", func(t *testing.T) {
		err := vh.ComBinlogDump(mysqlConn, "binlog.000001", 4)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "COM_BINLOG_DUMP is not supported")
	})
}

func TestBinlogStreamCallback_SpanningPacketClosesOnError(t *testing.T) {
	// When a MySQL packet spans multiple gRPC responses and the stream
	// errors mid-packet, streamBinlogDumpResponse must close the connection
	// without writing an ERR packet. Writing an ERR packet mid-packet would
	// corrupt the client stream since the client is still expecting the
	// remaining payload bytes.

	// Create a writable mysql.Conn backed by a pipe, capturing all bytes
	// written so we can verify no ERR packet was sent.
	clientPipe, serverPipe := net.Pipe()
	defer serverPipe.Close()

	var written bytes.Buffer
	copyDone := make(chan struct{})
	go func() {
		io.Copy(&written, serverPipe)
		close(copyDone)
	}()

	c := mysql.NewConnForTest(clientPipe)

	vh := &vtgateHandler{}
	var state binlogStreamState
	callback := vh.binlogStreamCallback(c, &state)

	// Build a gRPC response where a MySQL packet spans the response boundary.
	// The packet header declares a 1000-byte payload, but only 500 bytes
	// are present in this response.
	pktPayloadLen := 1000
	availablePayload := 500

	raw := make([]byte, mysql.PacketHeaderSize+availablePayload)
	// MySQL packet header: 3-byte little-endian length + 1-byte sequence
	raw[0] = byte(pktPayloadLen & 0xFF)
	raw[1] = byte((pktPayloadLen >> 8) & 0xFF)
	raw[2] = byte((pktPayloadLen >> 16) & 0xFF)
	raw[3] = 0 // sequence number

	// Simulate: callback processes the spanning response, then the stream errors.
	streamErr := errors.New("stream broken")
	err := vh.streamBinlogDumpResponse(c, "test", &state, func() error {
		if err := callback(&binlogdatapb.BinlogDumpResponse{Raw: raw}); err != nil {
			return err
		}
		return streamErr
	})
	require.NoError(t, err) // streamBinlogDumpResponse returns nil after handling the error

	// Close the write end so the capture goroutine finishes.
	clientPipe.Close()
	<-copyDone

	assert.True(t, c.IsMarkedForClose(), "connection should be marked for close")

	// The only bytes written should be the partial MySQL packet:
	// 4-byte header + 500 bytes payload = 504 bytes.
	// If an ERR packet was incorrectly written, there would be additional bytes.
	expectedBytes := mysql.PacketHeaderSize + availablePayload
	assert.Equal(t, expectedBytes, written.Len(),
		"only the partial packet should be written; extra bytes indicate a spurious ERR packet")
}

func TestBinlogStreamCallback_CompletePacketWritesErrOnError(t *testing.T) {
	// When a complete MySQL packet has been written and the stream errors
	// at a clean message boundary, streamBinlogDumpResponse should write
	// an ERR packet so the client knows what happened.

	clientPipe, serverPipe := net.Pipe()
	defer serverPipe.Close()

	var written bytes.Buffer
	copyDone := make(chan struct{})
	go func() {
		io.Copy(&written, serverPipe)
		close(copyDone)
	}()

	c := mysql.NewConnForTest(clientPipe)

	vh := &vtgateHandler{}
	var state binlogStreamState
	callback := vh.binlogStreamCallback(c, &state)

	// Build a gRPC response containing a complete, small MySQL packet.
	payload := []byte{0x00, 0xAA, 0xBB, 0xCC}
	raw := make([]byte, mysql.PacketHeaderSize+len(payload))
	raw[0] = byte(len(payload))
	raw[1] = 0
	raw[2] = 0
	raw[3] = 0 // sequence number
	copy(raw[mysql.PacketHeaderSize:], payload)

	// Simulate: callback processes the complete packet, then the stream errors.
	streamErr := errors.New("stream broken")
	err := vh.streamBinlogDumpResponse(c, "test", &state, func() error {
		if err := callback(&binlogdatapb.BinlogDumpResponse{Raw: raw}); err != nil {
			return err
		}
		return streamErr
	})
	require.NoError(t, err)

	clientPipe.Close()
	<-copyDone

	assert.True(t, c.IsMarkedForClose(), "connection should be marked for close")

	// The written bytes should contain the original packet PLUS an ERR packet.
	originalPacketSize := mysql.PacketHeaderSize + len(payload)
	assert.Greater(t, written.Len(), originalPacketSize,
		"an ERR packet should be written after the complete packet")

	// Verify the extra bytes start with a MySQL packet header whose payload
	// begins with the ERR marker (0xFF).
	errPacketStart := written.Bytes()[originalPacketSize:]
	require.GreaterOrEqual(t, len(errPacketStart), mysql.PacketHeaderSize+1,
		"ERR packet too short")
	assert.Equal(t, byte(mysql.ErrPacket), errPacketStart[mysql.PacketHeaderSize],
		"first payload byte of the error response should be the ERR packet marker")
}

func TestGracefulShutdownWithTransaction(t *testing.T) {
	executor, _, _, _, _ := createExecutorEnv(t)

	vh := newVtgateHandler(&VTGate{executor: executor, timings: timings, rowsReturned: rowsReturned, rowsAffected: rowsAffected, queryTextCharsProcessed: queryTextCharsProcessed})
	th := &testHandler{}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), th, 0, 0, false, false, 0, 0, false)
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
	require.NoError(t, err)

	err = vh.ComQuery(mysqlConn, "select 1", func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)

	listener.Shutdown()

	err = vh.ComQuery(mysqlConn, "select 1", func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)

	err = vh.ComQuery(mysqlConn, "COMMIT", func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)

	require.False(t, mysqlConn.IsMarkedForClose())

	err = vh.ComQuery(mysqlConn, "select 1", func(result *sqltypes.Result) error {
		return nil
	})
	require.EqualError(t, err, "Server shutdown in progress (errno 1053) (sqlstate 08S01)")

	require.True(t, mysqlConn.IsMarkedForClose())
}

// TestTempTableCommandTracking verifies that the command start/end hooks
// register only reserved shard sessions of a session that holds temporary
// tables, lock out the sweeper while a command is in flight, and deregister
// the connection once the temporary tables are gone.
// tempTargets builds a keepalive target map from the given targets, for tests.
func tempTargets(ts ...tempTableHeartbeatTarget) map[tempTableTargetKey]tempTableHeartbeatTarget {
	m := make(map[tempTableTargetKey]tempTableHeartbeatTarget, len(ts))
	for _, target := range ts {
		m[newTempTableTargetKey(target)] = target
	}
	return m
}

// firstTempTarget returns the single target in a one-entry target map.
func firstTempTarget(t *testing.T, targets map[tempTableTargetKey]tempTableHeartbeatTarget) tempTableHeartbeatTarget {
	t.Helper()
	require.Len(t, targets, 1)
	for _, target := range targets {
		return target
	}
	return tempTableHeartbeatTarget{}
}

func TestTempTableCommandTracking(t *testing.T) {
	vh := &vtgateHandler{}
	c := &mysql.Conn{ConnectionID: 7}

	target := &querypb.Target{Keyspace: "ks", Shard: "-", TabletType: topodatapb.TabletType_PRIMARY}
	alias := &topodatapb.TabletAlias{Cell: "aa", Uid: 1}

	// No temp tables -> the command leaves the connection unregistered.
	c.ClientData = &vtgatepb.Session{}
	vh.tempTableCommandEnd(c)
	_, ok := vh.tempTableConns.Load(c)
	require.False(t, ok)

	// Temp tables + a reserved shard session -> registered with one target.
	// A reserved id of 0 is excluded, and so is a shard session with an open
	// transaction: the tablet does not reset its transaction timer on beats,
	// so in-transaction connections are not kept alive (transactions remain
	// subject to the transaction timeout, temp tables or not).
	c.ClientData = &vtgatepb.Session{
		Options: &querypb.ExecuteOptions{HasCreatedTempTables: true},
		ShardSessions: []*vtgatepb.Session_ShardSession{
			{Target: target, TabletAlias: alias, ReservedId: 42},
			{Target: target, TabletAlias: alias, ReservedId: 0},
			{Target: target, TabletAlias: alias, ReservedId: 43, TransactionId: 43},
		},
	}
	vh.tempTableCommandEnd(c)
	v, ok := vh.tempTableConns.Load(c)
	require.True(t, ok)
	registered := v.(*tempTableConn)
	require.Len(t, registered.targets, 1)
	require.Equal(t, int64(42), firstTempTarget(t, registered.targets).reservedID)

	// Republishing the targets bumps the generation, so a sweep that
	// snapshotted the previous targets discards its results instead of
	// applying them to the new ones.
	genBefore := registered.gen
	vh.tempTableCommandEnd(c)
	require.Greater(t, registered.gen, genBefore, "command end must supersede in-flight sweeps")

	// A surviving target's consecutive-failure count is carried across a
	// republish. A command republishes the targets every time it settles, but an
	// unavailable tablet is still unavailable — resetting its count would route it
	// back through the unbounded healthy dispatch path, so unrelated client
	// activity could keep an outage from ever being gated.
	key := newTempTableTargetKey(tempTableHeartbeatTarget{alias: alias, reservedID: 42})
	registered.mu.Lock()
	failing := registered.targets[key]
	failing.failures = 3
	registered.targets[key] = failing
	registered.mu.Unlock()

	vh.tempTableCommandEnd(c)
	registered.mu.Lock()
	require.Equal(t, 3, registered.targets[key].failures,
		"a republish must preserve a surviving target's failure count")
	registered.mu.Unlock()

	// The temp-table flag being cleared deregisters the connection and clears
	// the published targets.
	c.ClientData = &vtgatepb.Session{Options: &querypb.ExecuteOptions{}}
	vh.tempTableCommandEnd(c)
	_, ok = vh.tempTableConns.Load(c)
	require.False(t, ok)
	require.Empty(t, registered.targets)

	// Closing the connection deregisters it as well.
	c.ClientData = &vtgatepb.Session{
		Options: &querypb.ExecuteOptions{HasCreatedTempTables: true},
		ShardSessions: []*vtgatepb.Session_ShardSession{
			{Target: target, TabletAlias: alias, ReservedId: 43},
		},
	}
	vh.tempTableCommandEnd(c)
	_, ok = vh.tempTableConns.Load(c)
	require.True(t, ok)
	vh.stopTempTableHeartbeats(c)
	_, ok = vh.tempTableConns.Load(c)
	require.False(t, ok)
}

// TestTempTableHeartbeatBatchesPerTablet verifies that all reserved connections
// on one tablet are refreshed with a single batched touch RPC carrying every
// id, no matter how many client connections hold them.
func TestTempTableHeartbeatBatchesPerTablet(t *testing.T) {
	vtg, sbc, ctx := createVtgateEnv(t)
	vh := newVtgateHandler(vtg)
	tablet := sbc.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}

	// Two connections on the same tablet.
	cA := &mysql.Conn{ConnectionID: 1}
	cB := &mysql.Conn{ConnectionID: 2}
	vh.tempTableConns.Store(cA, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: 1})})
	vh.tempTableConns.Store(cB, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: 2})})

	// A single sweep beats the tablet once, with a batch carrying both ids.
	before := sbc.ExecCount.Load()
	vh.sendTempTableHeartbeats(ctx)
	require.Equal(t, before+1, sbc.ExecCount.Load(), "the tablet must be beaten with exactly one batched RPC")
	opts := sbc.GetOptions()
	require.NotEmpty(t, opts)
	require.Len(t, opts[len(opts)-1].GetReservedConnKeepAliveIds(), 2, "the batch must carry both reserved connections' ids")
}

// TestTempTableHeartbeatSplitsOversizedBatch verifies that a tablet holding more
// reserved connections than the tablet's per-request limit is beaten in several
// batches of at most that limit, rather than one oversized request the tablet
// would reject — which would refresh none of them and eventually lose their
// temporary tables.
func TestTempTableHeartbeatSplitsOversizedBatch(t *testing.T) {
	vtg, sbc, ctx := createVtgateEnv(t)
	vh := newVtgateHandler(vtg)
	tablet := sbc.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}

	// One more reserved connection than a single batch can carry.
	const total = queryservice.ReservedConnKeepAliveMaxBatch + 1
	for i := range total {
		c := &mysql.Conn{ConnectionID: uint32(i + 1)}
		vh.tempTableConns.Store(c, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{
			target: target, alias: tablet.Alias, reservedID: int64(i + 1),
		})})
	}

	vh.sendTempTableHeartbeats(ctx)

	// Collect the reserved-id batches the tablet received.
	batches := 0
	seen := map[int64]struct{}{}
	for _, o := range sbc.GetOptions() {
		if !o.GetReservedConnKeepAlive() {
			continue
		}
		ids := o.GetReservedConnKeepAliveIds()
		require.LessOrEqual(t, len(ids), queryservice.ReservedConnKeepAliveMaxBatch,
			"no batch may exceed the tablet's per-request limit")
		batches++
		for _, id := range ids {
			seen[id] = struct{}{}
		}
	}
	require.Equal(t, 2, batches, "%d connections must be split into two batches", total)
	require.Len(t, seen, total, "every reserved connection must be refreshed across the batches")
}

// concurrentBeatConn is a tablet fake whose Execute records the peak number of
// beats in flight at once and blocks each until its context (the beat budget)
// ends. Unlike sandboxconn — which serializes Execute under a mutex — it lets a
// test observe whether a tablet's oversized-batch chunks are beaten concurrently
// or serialized.
type concurrentBeatConn struct {
	*sandboxconn.SandboxConn
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
}

func (c *concurrentBeatConn) Execute(ctx context.Context, session queryservice.Session, target *querypb.Target, sql string, bindVars map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	n := c.inFlight.Add(1)
	defer c.inFlight.Add(-1)
	for {
		m := c.maxInFlight.Load()
		if n <= m || c.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestTempTableHeartbeatChunksBeatConcurrently proves a tablet holding more than
// one batch of reserved connections has its chunks beaten concurrently within a
// single round. Serialized (the previous behavior), a stalled first chunk would
// hold every later chunk for its whole budget, so a later chunk's refresh would
// miss the interval-plus-one-budget bound and the next sweep would stay
// suppressed for the sum of the chunks' budgets.
func TestTempTableHeartbeatChunksBeatConcurrently(t *testing.T) {
	origInterval := tempTableHeartbeatTime
	tempTableHeartbeatTime = 30 * time.Second // budget 15s: chunks stay in flight
	t.Cleanup(func() { tempTableHeartbeatTime = origInterval })

	executor, _, _, _, ctx := createExecutorEnv(t)
	vsm := newVStreamManager(executor.resolver.resolver, executor.serv, "aa")
	vtg := newVTGate(executor, executor.resolver, vsm, nil, executor.scatterConn.gateway)
	vh := newVtgateHandler(vtg)
	hc := executor.scatterConn.gateway.hc.(*discovery.FakeHealthCheck)

	qs := hc.AddFakeTablet("aa", "beathost", 1, "beatks", "0", topodatapb.TabletType_PRIMARY, true, 1, nil,
		func(tablet *topodatapb.Tablet) queryservice.QueryService {
			return &concurrentBeatConn{SandboxConn: sandboxconn.NewSandboxConn(tablet)}
		})
	beat := qs.(*concurrentBeatConn)

	// Two batches' worth of reserved connections on the one tablet -> two chunks.
	tablet := beat.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}
	const total = 2 * queryservice.ReservedConnKeepAliveMaxBatch
	ts := make([]tempTableHeartbeatTarget, 0, total)
	for i := range total {
		ts = append(ts, tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: int64(i + 1)})
	}
	c := &mysql.Conn{ConnectionID: 1}
	vh.tempTableConns.Store(c, &tempTableConn{targets: tempTargets(ts...)})

	hbCtx, hbCancel := context.WithCancel(ctx)
	wg := vh.dispatchTempTableBeats(hbCtx)
	t.Cleanup(func() {
		hbCancel()
		wg.Wait()
	})

	require.Eventually(t, func() bool { return beat.maxInFlight.Load() >= 2 }, 5*time.Second, time.Millisecond,
		"a tablet's oversized-batch chunks must be beaten concurrently, not serialized")
}

// gatedBeatConn is a tablet fake whose Execute counts its calls and, when given a
// release channel, blocks until released (or the context ends) before returning
// the configured error. It lets a test hold a beat in flight and control exactly
// when it completes. Unlike sandboxconn it does not serialize Execute under a
// mutex, so independent tablets' beats run concurrently.
type gatedBeatConn struct {
	*sandboxconn.SandboxConn
	execCount atomic.Int64
	release   chan struct{}
	err       error
}

func (c *gatedBeatConn) Execute(ctx context.Context, session queryservice.Session, target *querypb.Target, sql string, bindVars map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	c.execCount.Add(1)
	if c.release != nil {
		select {
		case <-c.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if c.err != nil {
		return nil, c.err
	}
	return &sqltypes.Result{}, nil
}

// storeGatedFailingTablet registers a tablet backed by a gatedBeatConn and a
// reserved connection on it that is already marked failing, so the sweep routes
// it through the bounded failing lane.
func storeGatedFailingTablet(vh *vtgateHandler, hc *discovery.FakeHealthCheck, i int, release chan struct{}, err error) *gatedBeatConn {
	qs := hc.AddFakeTablet("aa", fmt.Sprintf("gated-%d", i), int32(i+1), "gatedks", fmt.Sprintf("g%d", i), topodatapb.TabletType_PRIMARY, true, 1, nil,
		func(tablet *topodatapb.Tablet) queryservice.QueryService {
			return &gatedBeatConn{SandboxConn: sandboxconn.NewSandboxConn(tablet), release: release, err: err}
		})
	gc := qs.(*gatedBeatConn)
	st := gc.Tablet()
	c := &mysql.Conn{ConnectionID: uint32(2000 + i)}
	vh.tempTableConns.Store(c, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{
		target:     &querypb.Target{Keyspace: st.Keyspace, Shard: st.Shard, TabletType: st.Type},
		alias:      st.Alias,
		reservedID: int64(2000 + i),
		failures:   1,
	})})
	return gc
}

// TestTempTableHeartbeatFailingLaneContactsRecovered proves the failing lane
// queues rather than drops: with every slot occupied by a stuck tablet, a tablet
// registered afterwards is not contacted while the lane is full, and is contacted
// the instant a slot frees — not deferred to the next tick. On the drop-if-full
// behavior it was never contacted within the round at all.
func TestTempTableHeartbeatFailingLaneContactsRecovered(t *testing.T) {
	origInterval := tempTableHeartbeatTime
	tempTableHeartbeatTime = 30 * time.Second
	t.Cleanup(func() { tempTableHeartbeatTime = origInterval })

	executor, _, _, _, ctx := createExecutorEnv(t)
	vsm := newVStreamManager(executor.resolver.resolver, executor.serv, "aa")
	vtg := newVTGate(executor, executor.resolver, vsm, nil, executor.scatterConn.gateway)
	vh := newVtgateHandler(vtg)
	hc := executor.scatterConn.gateway.hc.(*discovery.FakeHealthCheck)

	// Fill every failing-lane slot with a stuck tablet that blocks until released.
	release := make(chan struct{})
	var stuck []*gatedBeatConn
	for i := range tempTableFailingBeatConcurrency {
		stuck = append(stuck, storeGatedFailingTablet(vh, hc, i, release, assert.AnError))
	}
	hbCtx, hbCancel := context.WithCancel(ctx)
	var wgs []*sync.WaitGroup
	t.Cleanup(func() {
		hbCancel()
		close(release)
		for _, wg := range wgs {
			wg.Wait()
		}
	})
	wgs = append(wgs, vh.dispatchTempTableBeats(hbCtx))

	inFlight := func() int64 {
		var n int64
		for _, c := range stuck {
			n += c.execCount.Load()
		}
		return n
	}
	require.Eventually(t, func() bool { return inFlight() == int64(tempTableFailingBeatConcurrency) }, 5*time.Second, time.Millisecond,
		"every failing-lane slot must fill")

	// A recovered tablet — still marked failing until it is probed — registers
	// while the lane is full.
	recovered := storeGatedFailingTablet(vh, hc, tempTableFailingBeatConcurrency, nil /* no block */, nil /* success */)
	wgs = append(wgs, vh.dispatchTempTableBeats(hbCtx))

	// While the lane is full it waits (queued, not dropped)...
	require.Never(t, func() bool { return recovered.execCount.Load() > 0 }, 300*time.Millisecond, 10*time.Millisecond,
		"a queued tablet must not beat while the lane is full")
	// ...and the moment a slot frees it is contacted, without a new tick.
	release <- struct{}{}
	require.Eventually(t, func() bool { return recovered.execCount.Load() >= 1 }, 5*time.Second, time.Millisecond,
		"a recovered tablet must be contacted as soon as a lane slot frees")
}

// TestTempTableHeartbeatBeatOutcomeSurvivesRepublish proves a beat's outcome is
// applied to a target that survives a republish that happened while the beat was
// in flight. A command settling mid-beat bumps the generation but keeps the same
// reserved connection; discarding the outcome on the generation change (the old
// behavior) would drop the failure and leave the unavailable tablet on the
// uncapped path.
func TestTempTableHeartbeatBeatOutcomeSurvivesRepublish(t *testing.T) {
	origInterval := tempTableHeartbeatTime
	tempTableHeartbeatTime = 30 * time.Second
	t.Cleanup(func() { tempTableHeartbeatTime = origInterval })

	executor, _, _, _, ctx := createExecutorEnv(t)
	vsm := newVStreamManager(executor.resolver.resolver, executor.serv, "aa")
	vtg := newVTGate(executor, executor.resolver, vsm, nil, executor.scatterConn.gateway)
	vh := newVtgateHandler(vtg)
	hc := executor.scatterConn.gateway.hc.(*discovery.FakeHealthCheck)

	release := make(chan struct{})
	qs := hc.AddFakeTablet("aa", "republishhost", 1, "repubks", "0", topodatapb.TabletType_PRIMARY, true, 1, nil,
		func(tablet *topodatapb.Tablet) queryservice.QueryService {
			return &gatedBeatConn{SandboxConn: sandboxconn.NewSandboxConn(tablet), release: release, err: assert.AnError}
		})
	gc := qs.(*gatedBeatConn)
	tablet := gc.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}

	const reservedID = 77
	c := &mysql.Conn{ConnectionID: 1}
	c.ClientData = &vtgatepb.Session{
		Options:       &querypb.ExecuteOptions{HasCreatedTempTables: true},
		ShardSessions: []*vtgatepb.Session_ShardSession{{Target: target, TabletAlias: tablet.Alias, ReservedId: reservedID}},
	}
	vh.tempTableCommandEnd(c)
	v, ok := vh.tempTableConns.Load(c)
	require.True(t, ok)
	ttc := v.(*tempTableConn)
	key := newTempTableTargetKey(tempTableHeartbeatTarget{alias: tablet.Alias, reservedID: reservedID})

	hbCtx, hbCancel := context.WithCancel(ctx)
	t.Cleanup(hbCancel)
	wg := vh.dispatchTempTableBeats(hbCtx)

	// The beat reaches the tablet and blocks there.
	require.Eventually(t, func() bool { return gc.execCount.Load() >= 1 }, 5*time.Second, time.Millisecond,
		"the beat must reach the tablet")

	// A command settles while the beat is blocked, republishing the same target
	// (bumping the generation, preserving the key).
	vh.tempTableCommandEnd(c)

	// The beat now fails; its failure must still be recorded against the target.
	close(release)
	wg.Wait()

	ttc.mu.Lock()
	require.Equal(t, 1, ttc.targets[key].failures,
		"a beat outcome must apply to a target that survived a mid-flight republish")
	ttc.mu.Unlock()
}

// TestTempTableHeartbeatEvictsOnTabletTypeChange proves a beat rejected as a
// wrong tablet — as happens once the tablet's type changes out from under the
// reservation (REPLICA -> RDONLY), making it unreachable by its session — evicts
// the registration instead of refreshing it forever, so the orphaned reservation
// is reclaimed at the tablet's idle timeout rather than pinned open.
func TestTempTableHeartbeatEvictsOnTabletTypeChange(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)
	vsm := newVStreamManager(executor.resolver.resolver, executor.serv, "aa")
	vtg := newVTGate(executor, executor.resolver, vsm, nil, executor.scatterConn.gateway)
	vh := newVtgateHandler(vtg)
	hc := executor.scatterConn.gateway.hc.(*discovery.FakeHealthCheck)

	wrongTablet := vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "%s: REPLICA, want: RDONLY", vterrors.WrongTablet)
	qs := hc.AddFakeTablet("aa", "changedhost", 1, "changedks", "0", topodatapb.TabletType_REPLICA, true, 1, nil,
		func(tablet *topodatapb.Tablet) queryservice.QueryService {
			return &gatedBeatConn{SandboxConn: sandboxconn.NewSandboxConn(tablet), err: wrongTablet}
		})
	gc := qs.(*gatedBeatConn)
	st := gc.Tablet()
	c := &mysql.Conn{ConnectionID: 1}
	vh.tempTableConns.Store(c, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{
		target: &querypb.Target{Keyspace: st.Keyspace, Shard: st.Shard, TabletType: st.Type}, alias: st.Alias, reservedID: 5,
	})})

	vh.sendTempTableHeartbeats(ctx)

	_, ok := vh.tempTableConns.Load(c)
	require.False(t, ok, "a wrong-tablet beat must evict the stale registration, not keep refreshing it")
}

// TestTempTableHeartbeatTouchesRegistrationWithinOneInterval covers the
// scheduling guarantee that motivated dropping the stagger: a connection
// registered right after a sweep's snapshot is touched by the very next sweep,
// so its worst-case time-to-first-touch is one interval — never the ~2x that a
// snapshot-then-per-bucket-stagger schedule allowed. Each sweep snapshots the
// whole registry afresh and touches every tablet at once, so no registration
// can slip past a sweep and then wait out a further per-bucket delay.
func TestTempTableHeartbeatTouchesRegistrationWithinOneInterval(t *testing.T) {
	vtg, sbc, ctx := createVtgateEnv(t)
	vh := newVtgateHandler(vtg)
	tablet := sbc.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}

	// One sweep runs and takes its snapshot (an existing connection is touched).
	cOld := &mysql.Conn{ConnectionID: 1}
	vh.tempTableConns.Store(cOld, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: 1})})
	vh.sendTempTableHeartbeats(ctx)

	// A new connection registers immediately after that snapshot — the worst
	// case the reviewer flagged. The current sweep has already snapshotted, so it
	// does not touch the newcomer; the next sweep, one interval later, snapshots
	// afresh and does.
	cNew := &mysql.Conn{ConnectionID: 2}
	vh.tempTableConns.Store(cNew, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: 2})})

	vh.sendTempTableHeartbeats(ctx)
	opts := sbc.GetOptions()
	require.NotEmpty(t, opts)
	require.Contains(t, opts[len(opts)-1].GetReservedConnKeepAliveIds(), int64(2),
		"a connection registered right after a snapshot must be touched by the next sweep, within one interval")
}

// TestTempTableHeartbeatSchedulerCatchesLateRegistration runs the real
// background scheduler and proves the same guarantee end to end: a connection
// registered after the scheduler has already swept is picked up by a later
// tick, because every tick snapshots the registry afresh. The tablet reports
// the new connection's reserved id gone, so the scheduler evicts it the first
// time it touches it — an eviction observed race-free via the concurrent-safe
// registry map, rather than by reading the sandbox's options under the running
// scheduler.
func TestTempTableHeartbeatSchedulerCatchesLateRegistration(t *testing.T) {
	origInterval := tempTableHeartbeatTime
	tempTableHeartbeatTime = 250 * time.Millisecond
	t.Cleanup(func() { tempTableHeartbeatTime = origInterval })

	vtg, sbc, parent := createVtgateEnv(t)
	vh := newVtgateHandler(vtg)
	tablet := sbc.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}

	// The tablet will report the late-registered connection's reserved id (2)
	// gone. Set before the scheduler starts, so it is only read (never written)
	// while the scheduler runs.
	sbc.KeepAliveGoneIDs = map[int64]bool{2: true}

	ctx, cancel := context.WithCancel(parent)
	t.Cleanup(cancel)

	// An initial connection (never reported gone) lets us observe the scheduler
	// completing a sweep via the atomic exec count.
	cOld := &mysql.Conn{ConnectionID: 1}
	vh.tempTableConns.Store(cOld, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: 1})})
	vh.startTempTableHeartbeat(ctx)
	require.Eventually(t, func() bool { return sbc.ExecCount.Load() >= 1 }, 30*time.Second, 5*time.Millisecond,
		"the scheduler must complete an initial sweep")

	// Register a new connection right after that snapshot. Because every tick
	// snapshots the registry afresh, a later tick — at most one interval away —
	// picks it up and touches it; the tablet reports it gone, so it is evicted.
	cNew := &mysql.Conn{ConnectionID: 2}
	vh.tempTableConns.Store(cNew, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: 2})})
	require.Eventually(t, func() bool {
		_, ok := vh.tempTableConns.Load(cNew)
		return !ok
	}, 30*time.Second, 5*time.Millisecond,
		"the running scheduler must touch a connection registered after it started sweeping")
}

// TestTempTableHeartbeatSlowTabletIndependence proves that a beat stalled on a
// slow tablet does not delay another tablet's keepalive — the whole point of
// running each tablet on its own goroutine. It would fail on a scheduler that
// touched tablets sequentially.
func TestTempTableHeartbeatSlowTabletIndependence(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)
	vsm := newVStreamManager(executor.resolver.resolver, executor.serv, "aa")
	vtg := newVTGate(executor, executor.resolver, vsm, nil, executor.scatterConn.gateway)
	vh := newVtgateHandler(vtg)

	t1, t2 := sbc1.Tablet(), sbc2.Tablet()
	target1 := &querypb.Target{Keyspace: t1.Keyspace, Shard: t1.Shard, TabletType: t1.Type}
	target2 := &querypb.Target{Keyspace: t2.Keyspace, Shard: t2.Shard, TabletType: t2.Type}

	// t1 blocks every beat; t2 is fast.
	sbc1.ExecDelayResponse = 3 * time.Second
	cA := &mysql.Conn{ConnectionID: 1}
	cB := &mysql.Conn{ConnectionID: 2}
	vh.tempTableConns.Store(cA, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target1, alias: t1.Alias, reservedID: 1})})
	vh.tempTableConns.Store(cB, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target2, alias: t2.Alias, reservedID: 2})})

	// Every tablet is touched concurrently. The slow tablet (t1) blocks its own
	// goroutine, but the healthy tablet (t2) is in another goroutine and must be
	// beaten promptly regardless.
	go vh.sendTempTableHeartbeats(ctx)
	require.Eventually(t, func() bool { return sbc1.ExecCount.Load() >= 1 }, 30*time.Second, time.Millisecond,
		"the slow tablet's beat must be in flight")
	require.Eventually(t, func() bool { return sbc2.ExecCount.Load() >= 1 }, 2*time.Second, time.Millisecond,
		"the healthy tablet must be beaten promptly, not delayed by the stalled tablet")
}

// TestTempTableHeartbeatHealthyNotStarvedBySlowTablet proves that many
// connections pointing at a slow/unreachable tablet do not delay a healthy
// connection's keepalive on a different tablet — the correctness property that
// per-tablet grouping guarantees. It would fail on a flat worker pool where the
// slow connections occupy every worker.
func TestTempTableHeartbeatHealthyNotStarvedBySlowTablet(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)
	vsm := newVStreamManager(executor.resolver.resolver, executor.serv, "aa")
	vtg := newVTGate(executor, executor.resolver, vsm, nil, executor.scatterConn.gateway)
	vh := newVtgateHandler(vtg)

	t1, t2 := sbc1.Tablet(), sbc2.Tablet()
	target1 := &querypb.Target{Keyspace: t1.Keyspace, Shard: t1.Shard, TabletType: t1.Type}
	target2 := &querypb.Target{Keyspace: t2.Keyspace, Shard: t2.Shard, TabletType: t2.Type}

	// Many connections on the slow tablet — all folded into one batched RPC.
	sbc1.ExecDelayResponse = 3 * time.Second
	const slowConns = 32
	for i := range slowConns {
		c := &mysql.Conn{ConnectionID: uint32(i + 1)}
		vh.tempTableConns.Store(c, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target1, alias: t1.Alias, reservedID: int64(i + 1)})})
	}
	// One healthy connection on a different tablet.
	healthy := &mysql.Conn{ConnectionID: uint32(slowConns + 1)}
	vh.tempTableConns.Store(healthy, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target2, alias: t2.Alias, reservedID: 999999})})

	go vh.sendTempTableHeartbeats(ctx)

	// The healthy connection's tablet (t2) must be beaten promptly, in its own
	// goroutine, while the slow tablet (t1) blocks its own.
	require.Eventually(t, func() bool { return sbc2.ExecCount.Load() >= 1 }, 2*time.Second, time.Millisecond,
		"healthy connection must be beaten promptly, not starved behind the slow tablet")
}

// storeSlowTempTablet adds a tablet whose every beat blocks well past the budget
// to the healthcheck and registers a reserved connection on it. It returns the
// tablet's sandboxconn.
func storeSlowTempTablet(vh *vtgateHandler, hc *discovery.FakeHealthCheck, i int) *sandboxconn.SandboxConn {
	return storeTempTablet(vh, hc, i, 0 /* failures */)
}

// storeFailingTempTablet is like storeSlowTempTablet but pre-marks the reserved
// connection as failing, so the sweep routes it to the bounded failing lane.
func storeFailingTempTablet(vh *vtgateHandler, hc *discovery.FakeHealthCheck, i int) *sandboxconn.SandboxConn {
	return storeTempTablet(vh, hc, i, 1 /* failures */)
}

func storeTempTablet(vh *vtgateHandler, hc *discovery.FakeHealthCheck, i, failures int) *sandboxconn.SandboxConn {
	slow := hc.AddTestTablet("aa", fmt.Sprintf("host-%d", i), int32(i+1), "slowks", fmt.Sprintf("s%d", i), topodatapb.TabletType_PRIMARY, true, 1, nil)
	slow.ExecDelayResponse = 30 * time.Second
	st := slow.Tablet()
	c := &mysql.Conn{ConnectionID: uint32(1000 + i)}
	vh.tempTableConns.Store(c, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{
		target:     &querypb.Target{Keyspace: st.Keyspace, Shard: st.Shard, TabletType: st.Type},
		alias:      st.Alias,
		reservedID: int64(1000 + i),
		failures:   failures,
	})})
	return slow
}

// TestTempTableHeartbeatFailingLaneBoundedNoStarvation proves the two properties
// of the failing lane: no matter how many failing tablets there are, at most
// tempTableFailingBeatConcurrency of their beats are in flight at once, and a
// healthy tablet is still beaten because it bypasses the lane entirely.
func TestTempTableHeartbeatFailingLaneBoundedNoStarvation(t *testing.T) {
	origInterval := tempTableHeartbeatTime
	tempTableHeartbeatTime = 30 * time.Second // budget 15s: failing beats stay stuck
	t.Cleanup(func() { tempTableHeartbeatTime = origInterval })

	executor, sbcHealthy, _, _, ctx := createExecutorEnv(t)
	vsm := newVStreamManager(executor.resolver.resolver, executor.serv, "aa")
	vtg := newVTGate(executor, executor.resolver, vsm, nil, executor.scatterConn.gateway)
	vh := newVtgateHandler(vtg)
	hc := executor.scatterConn.gateway.hc.(*discovery.FakeHealthCheck)

	// Far more failing tablets than the failing lane holds.
	var failing []*sandboxconn.SandboxConn
	for i := range tempTableFailingBeatConcurrency * 3 {
		failing = append(failing, storeFailingTempTablet(vh, hc, i))
	}

	// One healthy tablet.
	th := sbcHealthy.Tablet()
	cHealthy := &mysql.Conn{ConnectionID: 1}
	vh.tempTableConns.Store(cHealthy, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{
		target: &querypb.Target{Keyspace: th.Keyspace, Shard: th.Shard, TabletType: th.Type}, alias: th.Alias, reservedID: 1,
	})})

	hbCtx, hbCancel := context.WithCancel(ctx)
	var wgs []*sync.WaitGroup
	t.Cleanup(func() {
		hbCancel()
		for _, wg := range wgs {
			wg.Wait()
		}
	})
	wgs = append(wgs, vh.dispatchTempTableBeats(hbCtx))

	// The healthy tablet is beaten — it never enters the failing lane.
	require.Eventually(t, func() bool { return sbcHealthy.ExecCount.Load() >= 1 }, 5*time.Second, 5*time.Millisecond,
		"the healthy tablet must be beaten, not starved by the failing lane")

	// Each failing beat increments its exec count on entry and then blocks past
	// the budget, so before any completes the sum is the number in flight.
	inFlight := func() int64 {
		var n int64
		for _, c := range failing {
			n += c.ExecCount.Load()
		}
		return n
	}
	require.Eventually(t, func() bool { return inFlight() >= int64(tempTableFailingBeatConcurrency) }, 5*time.Second, time.Millisecond,
		"the failing lane should fill to its cap")
	for range 50 {
		require.LessOrEqual(t, inFlight(), int64(tempTableFailingBeatConcurrency),
			"beats to failing tablets in flight must never exceed the lane cap")
	}
}

// TestTempTableHeartbeatBacklogDoesNotDelayHealthySweep proves that a backlog of
// stuck beats from unreachable tablets never delays a later healthy sweep: each
// tick dispatches without waiting, so the second tick beats the healthy tablet
// again even though many earlier beats are still stuck. It would fail if the
// tick blocked on the outstanding beats.
func TestTempTableHeartbeatBacklogDoesNotDelayHealthySweep(t *testing.T) {
	origInterval := tempTableHeartbeatTime
	tempTableHeartbeatTime = 30 * time.Second // budget 15s: slow beats stay stuck across both ticks
	t.Cleanup(func() { tempTableHeartbeatTime = origInterval })

	executor, sbcHealthy, _, _, ctx := createExecutorEnv(t)
	vsm := newVStreamManager(executor.resolver.resolver, executor.serv, "aa")
	vtg := newVTGate(executor, executor.resolver, vsm, nil, executor.scatterConn.gateway)
	vh := newVtgateHandler(vtg)
	hc := executor.scatterConn.gateway.hc.(*discovery.FakeHealthCheck)

	// More than six stuck tablets, each blocking every beat past the budget.
	const slowTablets = 10
	for i := range slowTablets {
		storeSlowTempTablet(vh, hc, i)
	}

	// One healthy tablet.
	th := sbcHealthy.Tablet()
	cHealthy := &mysql.Conn{ConnectionID: 1}
	vh.tempTableConns.Store(cHealthy, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{
		target:     &querypb.Target{Keyspace: th.Keyspace, Shard: th.Shard, TabletType: th.Type},
		alias:      th.Alias,
		reservedID: 1,
	})})

	hbCtx, hbCancel := context.WithCancel(ctx)
	var wgs []*sync.WaitGroup
	t.Cleanup(func() {
		hbCancel()
		for _, wg := range wgs {
			wg.Wait()
		}
	})

	// First tick: dispatch (non-blocking). The healthy tablet is beaten; the ten
	// slow tablets each start a beat that stays stuck.
	wgs = append(wgs, vh.dispatchTempTableBeats(hbCtx))
	require.Eventually(t, func() bool { return sbcHealthy.ExecCount.Load() >= 1 }, 5*time.Second, 5*time.Millisecond,
		"the first tick must beat the healthy tablet")

	// Second tick: the ten slow tablets are still in flight and are suppressed, so
	// this tick only re-dispatches the healthy tablet — which is beaten again even
	// though the backlog is still stuck.
	wgs = append(wgs, vh.dispatchTempTableBeats(hbCtx))
	require.Eventually(t, func() bool { return sbcHealthy.ExecCount.Load() >= 2 }, 5*time.Second, 5*time.Millisecond,
		"the second tick must beat the healthy tablet again, not wait on the stuck backlog")
}

// TestTempTableHeartbeatSuppressesInFlightTablet proves per-tablet in-flight
// suppression: a tablet whose beat is still running is not dispatched again on
// the next tick, so a broad outage holds at most one stuck beat per tablet rather
// than accumulating one every tick.
func TestTempTableHeartbeatSuppressesInFlightTablet(t *testing.T) {
	origInterval := tempTableHeartbeatTime
	tempTableHeartbeatTime = 30 * time.Second // budget 15s: the beat stays stuck across both ticks
	t.Cleanup(func() { tempTableHeartbeatTime = origInterval })

	executor, _, _, _, ctx := createExecutorEnv(t)
	vsm := newVStreamManager(executor.resolver.resolver, executor.serv, "aa")
	vtg := newVTGate(executor, executor.resolver, vsm, nil, executor.scatterConn.gateway)
	vh := newVtgateHandler(vtg)
	hc := executor.scatterConn.gateway.hc.(*discovery.FakeHealthCheck)

	slow := storeSlowTempTablet(vh, hc, 0)

	hbCtx, hbCancel := context.WithCancel(ctx)
	var wgs []*sync.WaitGroup
	t.Cleanup(func() {
		hbCancel()
		for _, wg := range wgs {
			wg.Wait()
		}
	})

	// First tick starts the beat, which enters the tablet and blocks.
	wgs = append(wgs, vh.dispatchTempTableBeats(hbCtx))
	require.Eventually(t, func() bool { return slow.ExecCount.Load() == 1 }, 5*time.Second, time.Millisecond,
		"the first tick must start exactly one beat")

	// Further ticks are suppressed while that beat is in flight, so the exec count
	// stays at one — no accumulating backlog.
	for range 5 {
		wgs = append(wgs, vh.dispatchTempTableBeats(hbCtx))
	}
	for range 50 {
		require.Equal(t, int64(1), slow.ExecCount.Load(),
			"a tablet with a beat in flight must not be dispatched again")
	}
}

// TestTempTableHeartbeatMultiTargetNoStarvation proves that a session with a
// stalled secondary reserved connection does not starve an unrelated healthy
// session on the first session's fast tablet. Because each target is
// scheduled under its own tablet, the slow secondary sits in the slow
// tablet's pool and never occupies the fast tablet's.
func TestTempTableHeartbeatMultiTargetNoStarvation(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)
	vsm := newVStreamManager(executor.resolver.resolver, executor.serv, "aa")
	vtg := newVTGate(executor, executor.resolver, vsm, nil, executor.scatterConn.gateway)
	vh := newVtgateHandler(vtg)

	t1, t2 := sbc1.Tablet(), sbc2.Tablet()
	target1 := &querypb.Target{Keyspace: t1.Keyspace, Shard: t1.Shard, TabletType: t1.Type}
	target2 := &querypb.Target{Keyspace: t2.Keyspace, Shard: t2.Shard, TabletType: t2.Type}

	// sbc2 (the secondary tablet) is slow.
	sbc2.ExecDelayResponse = 3 * time.Second

	// Several multi-target sessions (fast conn on t1, slow one on t2), plus a
	// healthy t1-only session. On whole-connection grouping the multi sessions
	// would each block on their slow t2 target while holding up t1, starving the
	// healthy t1-only session; grouping each target under its own tablet keeps
	// the slow t2 targets in t2's batch so t1 stays responsive.
	const multiConns = 16
	for i := range multiConns {
		mc := &mysql.Conn{ConnectionID: uint32(i + 1)}
		vh.tempTableConns.Store(mc, &tempTableConn{targets: tempTargets(
			tempTableHeartbeatTarget{target: target1, alias: t1.Alias, reservedID: int64(2*i + 1)},
			tempTableHeartbeatTarget{target: target2, alias: t2.Alias, reservedID: int64(2*i + 2)},
		)})
	}
	healthy := &mysql.Conn{ConnectionID: uint32(multiConns + 1)}
	vh.tempTableConns.Store(healthy, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target1, alias: t1.Alias, reservedID: 999999})})

	go vh.sendTempTableHeartbeats(ctx)

	// The fast tablet's batched touch must run promptly on its own goroutine,
	// without waiting on the slow t2 batch. Each tablet gets one batched RPC,
	// so ExecCount reflects RPCs, not targets; if t1 were serialized behind t2
	// it would not start until t2's delay elapsed. ExecCount is atomic, so
	// this read does not race the concurrent sweep.
	require.Eventually(t, func() bool { return sbc1.ExecCount.Load() >= 1 }, 2*time.Second, time.Millisecond,
		"the fast tablet's batched touch must run without waiting on the slow secondary tablet")
}

// TestTempTableHeartbeatReservedIDCollision verifies that a beat result is
// matched to its target by (tablet, reserved id), not reserved id alone —
// reserved ids are only unique within a tablet, so a session reserved on two
// tablets can hold the same id twice. A gone beat for one tablet must not
// evict the other tablet's live target.
func TestTempTableHeartbeatReservedIDCollision(t *testing.T) {
	executor, sbc1, sbc2, _, ctx := createExecutorEnv(t)
	vsm := newVStreamManager(executor.resolver.resolver, executor.serv, "aa")
	vtg := newVTGate(executor, executor.resolver, vsm, nil, executor.scatterConn.gateway)
	vh := newVtgateHandler(vtg)

	t1, t2 := sbc1.Tablet(), sbc2.Tablet()
	target1 := &querypb.Target{Keyspace: t1.Keyspace, Shard: t1.Shard, TabletType: t1.Type}
	target2 := &querypb.Target{Keyspace: t2.Keyspace, Shard: t2.Shard, TabletType: t2.Type}

	// One session with the SAME reserved id (5) on two different tablets.
	c := &mysql.Conn{ConnectionID: 1}
	vh.tempTableConns.Store(c, &tempTableConn{targets: tempTargets(
		tempTableHeartbeatTarget{target: target1, alias: t1.Alias, reservedID: 5},
		tempTableHeartbeatTarget{target: target2, alias: t2.Alias, reservedID: 5},
	)})

	// Only t2's reserved connection is gone; t1's is healthy.
	sbc2.KeepAliveGoneIDs = map[int64]bool{5: true}

	vh.sendTempTableHeartbeats(ctx)

	v, ok := vh.tempTableConns.Load(c)
	require.True(t, ok, "the healthy t1 target must keep the connection registered")
	remaining := v.(*tempTableConn).targets
	require.Len(t, remaining, 1, "only the gone t2 target must be evicted")
	require.True(t, topoproto.TabletAliasEqual(firstTempTarget(t, remaining).alias, t1.Alias),
		"the surviving target must be t1's live reserved connection, not evicted by t2's gone id")
}

// TestTempTableBeatBudget verifies the per-tablet RPC budget stays strictly
// below the heartbeat interval, so an unreachable tablet's touch always
// finishes before the next interval's touch for that same tablet should start.
// The 2s floor still applies when the interval is large enough.
func TestTempTableBeatBudget(t *testing.T) {
	cases := []struct {
		interval time.Duration
		want     time.Duration
	}{
		{1 * time.Second, 750 * time.Millisecond},  // capped at 3/4 interval
		{2 * time.Second, 1500 * time.Millisecond}, // capped at 3/4 interval
		{3 * time.Second, 2 * time.Second},         // 2s floor, below interval
		{4 * time.Second, 2 * time.Second},         // 2s floor == interval/2
		{10 * time.Second, 5 * time.Second},        // interval/2
		{30 * time.Second, 15 * time.Second},       // interval/2
	}
	for _, tc := range cases {
		got := tempTableBeatBudget(tc.interval)
		require.Equal(t, tc.want, got, "budget for interval %s", tc.interval)
		require.Less(t, got, tc.interval, "budget must stay below the interval for %s", tc.interval)
		// The worst-case gap the workload timeout must clear is the interval
		// plus one RPC round-trip (bounded by the budget), not the interval
		// alone, since the tablet refreshes the timer only on delivery.
		require.Equal(t, tc.interval+got, tempTableBeatWorstCaseGap(tc.interval),
			"worst-case gap for interval %s", tc.interval)
	}
}

// TestTempTableHeartbeatSweepWithinBudget verifies that a sweep containing an
// unreachable tablet still completes within the RPC budget — which is below the
// interval — so a slow tablet cannot stretch the keepalive cadence past the
// interval.
func TestTempTableHeartbeatSweepWithinBudget(t *testing.T) {
	origInterval := tempTableHeartbeatTime
	tempTableHeartbeatTime = 1 * time.Second // budget = 750ms
	t.Cleanup(func() { tempTableHeartbeatTime = origInterval })

	vtg, sbc, ctx := createVtgateEnv(t)
	vh := newVtgateHandler(vtg)
	tablet := sbc.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}

	// The tablet blocks far longer than the budget, so the beat must time out.
	sbc.ExecDelayResponse = 10 * time.Second
	c := &mysql.Conn{ConnectionID: 1}
	vh.tempTableConns.Store(c, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: 1})})

	start := time.Now()
	vh.sendTempTableHeartbeats(ctx)
	elapsed := time.Since(start)
	require.Less(t, elapsed, tempTableHeartbeatTime,
		"a sweep with an unreachable tablet must complete within the interval, not stretch cadence")
	require.GreaterOrEqual(t, elapsed, tempTableBeatBudget(tempTableHeartbeatTime),
		"the beat should run until the budget before timing out")
}

// TestTempTableHeartbeatDelayedDelivery proves the timing that the worst-case
// gap accounts for: the tablet refreshes the connection only when the beat is
// delivered, not when the sweep dispatches it. A tablet that takes D (< budget)
// to receive and run the touch still gets its connection refreshed, but only
// after D — so the effective gap is the interval plus this RPC round-trip, which
// is why the workload timeout must clear interval + budget, not just interval.
func TestTempTableHeartbeatDelayedDelivery(t *testing.T) {
	origInterval := tempTableHeartbeatTime
	tempTableHeartbeatTime = 4 * time.Second // budget = 2s
	t.Cleanup(func() { tempTableHeartbeatTime = origInterval })

	vtg, sbc, ctx := createVtgateEnv(t)
	vh := newVtgateHandler(vtg)
	tablet := sbc.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}

	// The tablet delays delivery by less than the budget: the refresh lands, but
	// only after the delay.
	const delay = 500 * time.Millisecond
	sbc.ExecDelayResponse = delay
	c := &mysql.Conn{ConnectionID: 1}
	vh.tempTableConns.Store(c, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: 1})})

	start := time.Now()
	vh.sendTempTableHeartbeats(ctx)
	elapsed := time.Since(start)

	require.GreaterOrEqual(t, elapsed, delay,
		"the effective keepalive is delayed by the RPC round-trip, so the real gap is interval + RPC latency")
	// The beat still succeeded within the budget: the target is kept, not failed.
	v, ok := vh.tempTableConns.Load(c)
	require.True(t, ok, "a beat delivered within the budget must keep the connection registered")
	require.Zero(t, firstTempTarget(t, v.(*tempTableConn).targets).failures,
		"a beat delivered within the budget must count as a successful refresh")
}

// TestTempTableHeartbeatOldTabletSafe verifies the mixed-version behavior: a
// keepalive always carries reserved id 0 (all ids go in the batch list), so a
// tablet predating the option runs the fallback query on a throwaway pooled
// connection — never a reserved one — and its ordinary result is not misparsed
// as a gone reserved id. The target therefore survives (it is just not kept
// alive until the tablet is upgraded), and no reserved connection is at risk.
func TestTempTableHeartbeatOldTabletSafe(t *testing.T) {
	vtg, sbc, ctx := createVtgateEnv(t)
	vh := newVtgateHandler(vtg)
	tablet := sbc.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}

	sbc.KeepAliveUnsupported = true // simulate an old tablet: runs the query, ordinary result
	c := &mysql.Conn{ConnectionID: 1}
	vh.tempTableConns.Store(c, &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: 7})})

	vh.sendTempTableHeartbeats(ctx)

	// The keepalive carried reserved id 0 (all ids in the list), so an old
	// tablet's fallback never touches the reserved connection.
	opts := sbc.GetOptions()
	require.NotEmpty(t, opts)
	require.True(t, opts[len(opts)-1].GetReservedConnKeepAlive())
	require.Equal(t, []int64{7}, opts[len(opts)-1].GetReservedConnKeepAliveIds())

	// The old tablet's ordinary result is not misparsed as a gone reserved id,
	// so the target is not evicted.
	v, ok := vh.tempTableConns.Load(c)
	require.True(t, ok, "an old tablet's ordinary result must not be misparsed as a gone reserved id")
	require.Len(t, v.(*tempTableConn).targets, 1)
}

// BenchmarkTempTableHeartbeatSnapshot measures the once-per-interval registry
// scan at a high connection count, validating that the work is proportional to
// the number of registered sessions.
func BenchmarkTempTableHeartbeatSnapshot(b *testing.B) {
	vh := &vtgateHandler{}
	alias := &topodatapb.TabletAlias{Cell: "aa", Uid: 1}
	target := &querypb.Target{Keyspace: "ks", Shard: "-", TabletType: topodatapb.TabletType_PRIMARY}
	const conns = 10000
	for i := range conns {
		c := &mysql.Conn{ConnectionID: uint32(i + 1)}
		vh.tempTableConns.Store(c, &tempTableConn{targets: tempTargets(
			tempTableHeartbeatTarget{target: target, alias: alias, reservedID: int64(i + 1)},
		)})
	}
	b.ResetTimer()
	for range b.N {
		_ = vh.snapshotTempTableBeats()
	}
}

// TestTempTableHeartbeatSweep verifies that a sweep pings the registered
// reserved connection, ignores a result for a target that no longer exists, and
// evicts a reserved connection that no longer exists.
func TestTempTableHeartbeatSweep(t *testing.T) {
	vtg, sbc, ctx := createVtgateEnv(t)
	vh := newVtgateHandler(vtg)

	tablet := sbc.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}
	c := &mysql.Conn{ConnectionID: 1}
	ttc := &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: 99})}
	vh.tempTableConns.Store(c, ttc)

	// A sweep pings the reserved connection.
	before := sbc.ExecCount.Load()
	vh.sendTempTableHeartbeats(ctx)
	require.Greater(t, sbc.ExecCount.Load(), before)
	found := slices.ContainsFunc(sbc.Queries, func(q *querypb.BoundQuery) bool {
		return q.Sql == "/* temp-table keepalive */ select 1"
	})
	require.True(t, found, "sandbox tablet should have received the heartbeat query")
	opts := sbc.GetOptions()
	require.NotEmpty(t, opts)
	require.True(t, opts[len(opts)-1].GetReservedConnKeepAlive(),
		"beats must be keepalive touches so mysqld's wait_timeout keeps counting only real user traffic")

	// A result for a target that no longer exists — the reserved connection was
	// released, or the connection closed, while the beat was in flight — is
	// ignored and must not disturb other targets. (A target that merely survived
	// a mid-flight generation bump is still applied; see
	// TestTempTableHeartbeatBeatOutcomeSurvivesRepublish.)
	missing := tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: 12345}
	vh.applyTempTableBeatResult(tempTableBeatItem{c: c, ttc: ttc, gen: ttc.gen, target: missing}, true /* gone */, assert.AnError)
	require.Len(t, ttc.targets, 1, "a result for an unknown target must not touch other targets")

	// A reserved connection the tablet reports gone is evicted so it is not
	// beaten (and warned about) on every sweep.
	sbc.KeepAliveGoneIDs = map[int64]bool{99: true}
	vh.sendTempTableHeartbeats(ctx)
	require.Empty(t, ttc.targets)
	_, ok := vh.tempTableConns.Load(c)
	require.False(t, ok)
	sbc.KeepAliveGoneIDs = nil

	// A transient failure keeps the target so it is retried on the next sweep.
	c2 := &mysql.Conn{ConnectionID: 2}
	ttc2 := &tempTableConn{targets: tempTargets(tempTableHeartbeatTarget{target: target, alias: tablet.Alias, reservedID: 100})}
	vh.tempTableConns.Store(c2, ttc2)
	sbc.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	vh.sendTempTableHeartbeats(ctx)
	require.Len(t, ttc2.targets, 1)
	require.Equal(t, 1, firstTempTarget(t, ttc2.targets).failures)
	_, ok = vh.tempTableConns.Load(c2)
	require.True(t, ok)

	// A successful beat resets the consecutive-failure count.
	vh.sendTempTableHeartbeats(ctx)
	require.Len(t, ttc2.targets, 1)
	require.Equal(t, 0, firstTempTarget(t, ttc2.targets).failures)

	// Repeated failures never evict on their own — only a confirmed
	// connection-closed error may — or transient network trouble would
	// silently disable the keepalives of a live connection. The counter
	// keeps accumulating to gate the transition logging.
	sbc.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 4
	for range 4 {
		vh.sendTempTableHeartbeats(ctx)
	}
	require.Len(t, ttc2.targets, 1)
	require.Equal(t, 4, firstTempTarget(t, ttc2.targets).failures)
	_, ok = vh.tempTableConns.Load(c2)
	require.True(t, ok)
}

// TestTempTableBeatContext verifies that beats carry the client's caller
// identity the same way the command path does: without an immediate caller
// id, tablets running with --queryserver-config-strict-table-acl reject the
// beat ("missing caller id") and the reserved connection would still be
// reclaimed despite the heartbeats.
func TestTempTableBeatContext(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	c := mysql.NewConnForTest(server)
	c.User = "app_user"
	c.UserData = &mysql.StaticUserData{Username: "acl_user", Groups: []string{"g1"}}

	ctx := tempTableBeatContext(t.Context(), c)
	im := callerid.ImmediateCallerIDFromContext(ctx)
	require.NotNil(t, im)
	require.Equal(t, "acl_user", im.Username)
	require.Equal(t, []string{"g1"}, im.Groups)
	ef := callerid.EffectiveCallerIDFromContext(ctx)
	require.NotNil(t, ef)
	require.Equal(t, "app_user", ef.Principal)

	// A connection without auth-provided identity leaves the context bare.
	bare := &mysql.Conn{ConnectionID: 3}
	bareCtx := tempTableBeatContext(t.Context(), bare)
	require.Nil(t, callerid.ImmediateCallerIDFromContext(bareCtx))
}

// TestComQueryTempTableHeartbeatRegistration verifies that creating a
// temporary table through the mysql protocol handler reserves a connection
// and registers it for background heartbeats, that an ordinary query does
// not, and that closing the connection deregisters it.
func TestComQueryTempTableHeartbeatRegistration(t *testing.T) {
	executor, _, _, sbclookup, _ := createExecutorEnv(t)
	th := &testHandler{}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	defer listener.Close()

	c := mysql.GetTestServerConn(listener)
	c.ConnectionID = 1
	c.UserData = &mysql.StaticUserData{}
	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	vh.connections[1] = c
	vh.session(c).TargetString = KsTestUnsharded

	// An ordinary query does not register the connection.
	err = vh.ComQuery(c, "select id from main1", func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)
	_, ok := vh.tempTableConns.Load(c)
	require.False(t, ok)

	// A CREATE TEMPORARY TABLE whose RPC errors after the tablet reserved
	// the connection (the tablet may have created the table before failing;
	// the reserved id is returned with the error) marks the session and
	// registers the reserved connection for heartbeats, so the maybe-created
	// table is kept alive rather than reclaimed by the idle timeout.
	sbclookup.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	err = vh.ComQuery(c, "create temporary table temp_t(id bigint)", func(*sqltypes.Result) error { return nil })
	require.Error(t, err)
	require.True(t, vh.session(c).GetOptions().GetHasCreatedTempTables())
	_, ok = vh.tempTableConns.Load(c)
	require.True(t, ok)
	// Clean up so the next successful create starts from a known state.
	vh.stopTempTableHeartbeats(c)

	// Creating a temporary table sets the session flag, reserves a
	// connection, and registers it for heartbeats.
	err = vh.ComQuery(c, "create temporary table temp_t(id bigint)", func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)
	require.True(t, vh.session(c).GetOptions().GetHasCreatedTempTables())
	v, ok := vh.tempTableConns.Load(c)
	require.True(t, ok)
	targets := v.(*tempTableConn).targets
	require.Len(t, targets, 1)
	require.NotZero(t, firstTempTarget(t, targets).reservedID)
	require.Equal(t, topoproto.TabletAliasString(sbclookup.Tablet().Alias), topoproto.TabletAliasString(firstTempTarget(t, targets).alias))

	// COM_RESET_CONNECTION releases the reserved connections, and the
	// temporary tables and applied session settings die with them: the
	// connection must be deregistered and the temp-table flag, the
	// reserved-connection mode, and the recorded system variables all
	// cleared — otherwise the next queries would be forced onto pointless
	// fresh reserved connections and could re-register for heartbeats.
	require.True(t, vh.session(c).GetInReservedConn())
	vh.ComResetConnection(c)
	require.False(t, vh.session(c).GetOptions().GetHasCreatedTempTables())
	require.False(t, vh.session(c).GetInReservedConn())
	require.Empty(t, vh.session(c).GetSystemVariables())
	require.Empty(t, vh.session(c).GetShardSessions())
	_, ok = vh.tempTableConns.Load(c)
	require.False(t, ok)

	// Creating a temporary table again after the reset re-registers.
	err = vh.ComQuery(c, "create temporary table temp_t(id bigint)", func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)
	_, ok = vh.tempTableConns.Load(c)
	require.True(t, ok)

	// Closing the connection deregisters it.
	vh.ConnectionClosed(c)
	_, ok = vh.tempTableConns.Load(c)
	require.False(t, ok)
}

// TestComQueryIngressBytes verifies that MySQL protocol query ingress bytes are
// attached to VTGate query log stats.
func TestComQueryIngressBytes(t *testing.T) {
	vtgate, sbc, _ := createVtgateEnv(t)
	query := "SELECT id FROM user WHERE id = 1"

	sbc.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT32}},
		Rows:   []sqltypes.Row{{sqltypes.NewInt32(42)}},
	}})

	vh := newVtgateHandler(vtgate)
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), vh, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	defer listener.Close()
	defer waitForConnectionsClosed(t, vh)

	go listener.Accept()

	addr := listener.Addr().(*net.TCPAddr)
	params := &mysql.ConnParams{
		Host:  addr.IP.String(),
		Port:  addr.Port,
		Uname: "user1",
		Pass:  "password1",
	}

	conn, err := mysql.Connect(t.Context(), params)
	require.NoError(t, err)
	defer conn.Close()

	subscriber := vtgate.executor.queryLogger.Subscribe("test")
	defer vtgate.executor.queryLogger.Unsubscribe(subscriber)

	result, err := conn.ExecuteFetch(query, 100, true)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 1)

	select {
	case sentStats := <-subscriber:
		require.NotNil(t, sentStats)
		assert.Equal(t, uint64(mysql.PacketHeaderSize+1+len(query)), sentStats.IngressBytes)
		assert.Equal(t, "select id from `user` where id = :id /* INT64 */", sentStats.SQL)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "LogStats should have been sent to queryLogger")
	}
}

// TestComQueryMultiOLAPIngressBytes verifies that streaming multi-statement
// queries attribute MySQL protocol ingress bytes to each logged statement.
func TestComQueryMultiOLAPIngressBytes(t *testing.T) {
	vtgate, sbc, _ := createVtgateEnv(t)
	query := "SELECT id FROM user WHERE id = 1; SELECT id FROM user WHERE id = 1234567890"

	oldDefaultWorkload := mysqlDefaultWorkload
	mysqlDefaultWorkload = int32(querypb.ExecuteOptions_OLAP)
	t.Cleanup(func() {
		mysqlDefaultWorkload = oldDefaultWorkload
	})

	sbc.SetResults([]*sqltypes.Result{
		{
			Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT32}},
			Rows:   []sqltypes.Row{{sqltypes.NewInt32(1)}},
		},
		{
			Fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT32}},
			Rows:   []sqltypes.Row{{sqltypes.NewInt32(2)}},
		},
	})

	vh := newVtgateHandler(vtgate)
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), vh, 0, 0, false, false, 0, 0, true)
	require.NoError(t, err)
	defer listener.Close()
	defer waitForConnectionsClosed(t, vh)

	go listener.Accept()

	addr := listener.Addr().(*net.TCPAddr)
	params := &mysql.ConnParams{
		Host:  addr.IP.String(),
		Port:  addr.Port,
		Uname: "user1",
		Pass:  "password1",
		Flags: mysql.CapabilityClientMultiStatements,
	}

	conn, err := mysql.Connect(t.Context(), params)
	require.NoError(t, err)
	defer conn.Close()

	subscriber := vtgate.executor.queryLogger.Subscribe("test")
	defer vtgate.executor.queryLogger.Unsubscribe(subscriber)

	result, more, err := conn.ExecuteFetchMulti(query, 100, true)
	require.NoError(t, err)
	require.True(t, more)
	require.Len(t, result.Rows, 1)

	result, more, _, err = conn.ReadQueryResult(100, true)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, result.Rows, 1)

	queries, err := vtgate.executor.Environment().Parser().SplitStatementToPieces(query)
	require.NoError(t, err)
	expectedIngressBytes := allocateStatementIngressBytes(uint64(mysql.PacketHeaderSize+1+len(query)), queries)

	for i, expectedIngressBytes := range expectedIngressBytes {
		select {
		case sentStats := <-subscriber:
			require.NotNil(t, sentStats)
			assert.Equal(t, expectedIngressBytes, sentStats.IngressBytes, "query %d", i)
		case <-time.After(30 * time.Second):
			require.FailNow(t, "LogStats should have been sent to queryLogger")
		}
	}
}

// TestComPrepareIngressBytes verifies that MySQL protocol prepare ingress bytes
// are attached to VTGate query log stats.
func TestComPrepareIngressBytes(t *testing.T) {
	vtgate, _, _ := createVtgateEnv(t)
	query := "SELECT id FROM user WHERE id = ?"

	vh := newVtgateHandler(vtgate)
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), vh, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	defer listener.Close()
	defer waitForConnectionsClosed(t, vh)

	go listener.Accept()

	addr := listener.Addr().(*net.TCPAddr)
	params := &mysql.ConnParams{
		Host:  addr.IP.String(),
		Port:  addr.Port,
		Uname: "user1",
		Pass:  "password1",
	}

	conn, err := mysql.Connect(t.Context(), params)
	require.NoError(t, err)
	defer conn.Close()

	subscriber := vtgate.executor.queryLogger.Subscribe("test")
	defer vtgate.executor.queryLogger.Unsubscribe(subscriber)

	packet := make([]byte, mysql.PacketHeaderSize+1+len(query))
	payloadLength := 1 + len(query)
	packet[0] = byte(payloadLength)
	packet[1] = byte(payloadLength >> 8)
	packet[2] = byte(payloadLength >> 16)
	packet[3] = 0
	packet[mysql.PacketHeaderSize] = mysql.ComPrepare
	copy(packet[mysql.PacketHeaderSize+1:], query)
	written, err := conn.GetRawConn().Write(packet)
	require.NoError(t, err)
	require.Equal(t, len(packet), written)

	select {
	case sentStats := <-subscriber:
		require.NotNil(t, sentStats)
		assert.Equal(t, uint64(mysql.PacketHeaderSize+1+len(query)), sentStats.IngressBytes)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "LogStats should have been sent to queryLogger")
	}
}
