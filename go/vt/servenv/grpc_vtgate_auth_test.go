/*
Copyright 2026 The Vitess Authors.

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

package servenv_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateservice"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
)

// placeholderUser and placeholderPassword are not a real credential. They are
// the static-auth identity this test configures in a temporary file.
const (
	placeholderUser     = "placeholder-user"
	placeholderPassword = "placeholder-password"

	// claimedPrincipal is a caller id supplied by the client. It is not an
	// authenticated identity.
	claimedPrincipal = "claimed-admin"
)

const (
	executeSQL      = "insert into app_table (id) values (1)"
	executeMultiSQL = "update app_table set id = 2; delete from app_table"
	batchSQL        = "delete from app_table where id = 1"
	streamSQL       = "select id from app_table"
	streamMultiSQL  = "select id from app_table; select name from app_table"
	prepareSQL      = "select id from app_table where id = ?"
	vstreamKeyspace = "app_ks"
	vstreamTable    = "app_table"
	binlogKeyspace  = "app_ks"
	binlogShard     = "0"
)

// TestVTGateGRPCRequiresAuthentication is the spec for VTGate's gRPC listener:
// with grpc-port set and grpc-vtgateservice enabled, a client that has not
// authenticated must not reach any Vitess RPC, including the streaming ones.
// An empty grpc-auth-mode must not leave the listener open to those calls.
// Refusing to create the server satisfies that. os.Exit does not, because
// this test has to observe the refusal.
//
// A configured static authenticator must still run the SQL from the request
// that presented the placeholder identity, and must ignore a caller id the
// client puts in the request body.
func TestVTGateGRPCRequiresAuthentication(t *testing.T) {
	t.Run("empty auth mode", func(t *testing.T) {
		svc, client, listening := startVTGateGRPC(t, "")
		if !listening {
			require.Empty(t, svc.snapshot())
			return
		}
		for _, call := range unauthenticatedCalls() {
			t.Run(call.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				before := len(svc.snapshot())
				got, err := call.invoke(ctx, client)
				after := svc.snapshot()
				if len(after) != before {
					t.Fatalf("%s ran without authentication: %v; response %#v", call.name, after[before:], got)
				}
				require.Error(t, err)
				require.Contains(t, []codes.Code{codes.Unauthenticated, codes.PermissionDenied}, status.Code(err))
				require.Nil(t, got)
			})
		}
	})

	t.Run("static auth", func(t *testing.T) {
		svc, client, listening := startVTGateGRPC(t, "static")
		require.True(t, listening, "grpc-auth-mode static must still listen")

		t.Run("caller id is not authentication", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err := client.Execute(ctx, executeRequest(executeSQL))
			require.Empty(t, svc.snapshot())
			require.Error(t, err)
			require.Equal(t, codes.Unauthenticated, status.Code(err))
		})

		t.Run("wrong password does not run the query", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ctx = metadata.AppendToOutgoingContext(ctx, "username", placeholderUser, "password", "wrong-placeholder-password")
			_, err := client.Execute(ctx, executeRequest(executeSQL))
			require.Empty(t, svc.snapshot())
			require.Error(t, err)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		})

		t.Run("authenticated sql is the sql that was sent", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ctx = metadata.AppendToOutgoingContext(ctx, "username", placeholderUser, "password", placeholderPassword)
			first, err := client.Execute(ctx, executeRequest(executeSQL))
			require.NoError(t, err)
			require.Nil(t, first.GetError())
			second, err := client.Execute(ctx, executeRequest(streamSQL))
			require.NoError(t, err)
			require.Nil(t, second.GetError())

			calls := svc.snapshot()
			require.Len(t, calls, 2)
			require.Equal(t, "Execute", calls[0].method)
			require.Equal(t, executeSQL, calls[0].sql)
			require.Equal(t, placeholderUser, calls[0].staticUser)
			require.NotEqual(t, claimedPrincipal, calls[0].staticUser)
			require.Equal(t, "Execute", calls[1].method)
			require.Equal(t, streamSQL, calls[1].sql)
			require.Equal(t, placeholderUser, calls[1].staticUser)
		})
	})
}

type vtgateCall struct {
	name   string
	invoke func(context.Context, vtgateservicepb.VitessClient) (any, error)
}

func unauthenticatedCalls() []vtgateCall {
	return []vtgateCall{
		{name: "Execute", invoke: func(ctx context.Context, client vtgateservicepb.VitessClient) (any, error) {
			return client.Execute(ctx, executeRequest(executeSQL))
		}},
		{name: "ExecuteMulti", invoke: func(ctx context.Context, client vtgateservicepb.VitessClient) (any, error) {
			return client.ExecuteMulti(ctx, &vtgatepb.ExecuteMultiRequest{
				CallerId: claimedCaller(),
				Sql:      executeMultiSQL,
			})
		}},
		{name: "ExecuteBatch", invoke: func(ctx context.Context, client vtgateservicepb.VitessClient) (any, error) {
			return client.ExecuteBatch(ctx, &vtgatepb.ExecuteBatchRequest{
				CallerId: claimedCaller(),
				Queries:  []*querypb.BoundQuery{{Sql: batchSQL}},
			})
		}},
		{name: "StreamExecute", invoke: func(ctx context.Context, client vtgateservicepb.VitessClient) (any, error) {
			stream, err := client.StreamExecute(ctx, &vtgatepb.StreamExecuteRequest{
				CallerId: claimedCaller(),
				Query:    &querypb.BoundQuery{Sql: streamSQL},
			})
			if err != nil {
				return nil, err
			}
			return stream.Recv()
		}},
		{name: "StreamExecuteMulti", invoke: func(ctx context.Context, client vtgateservicepb.VitessClient) (any, error) {
			stream, err := client.StreamExecuteMulti(ctx, &vtgatepb.StreamExecuteMultiRequest{
				CallerId: claimedCaller(),
				Sql:      streamMultiSQL,
			})
			if err != nil {
				return nil, err
			}
			return stream.Recv()
		}},
		{name: "Prepare", invoke: func(ctx context.Context, client vtgateservicepb.VitessClient) (any, error) {
			return client.Prepare(ctx, &vtgatepb.PrepareRequest{
				CallerId: claimedCaller(),
				Query:    &querypb.BoundQuery{Sql: prepareSQL},
			})
		}},
		{name: "CloseSession", invoke: func(ctx context.Context, client vtgateservicepb.VitessClient) (any, error) {
			return client.CloseSession(ctx, &vtgatepb.CloseSessionRequest{CallerId: claimedCaller()})
		}},
		{name: "VStream", invoke: func(ctx context.Context, client vtgateservicepb.VitessClient) (any, error) {
			stream, err := client.VStream(ctx, &vtgatepb.VStreamRequest{
				CallerId: claimedCaller(),
				Vgtid: &binlogdatapb.VGtid{ShardGtids: []*binlogdatapb.ShardGtid{{
					Keyspace: vstreamKeyspace,
					Shard:    "0",
					Gtid:     "current",
				}}},
				Filter: &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: vstreamTable}}},
			})
			if err != nil {
				return nil, err
			}
			return stream.Recv()
		}},
		{name: "BinlogDumpGTID", invoke: func(ctx context.Context, client vtgateservicepb.VitessClient) (any, error) {
			stream, err := client.BinlogDumpGTID(ctx, &vtgatepb.BinlogDumpGTIDRequest{
				CallerId:       claimedCaller(),
				Keyspace:       binlogKeyspace,
				Shard:          binlogShard,
				BinlogPosition: 4,
				GtidSet:        "current",
			})
			if err != nil {
				return nil, err
			}
			return stream.Recv()
		}},
	}
}

func claimedCaller() *vtrpcpb.CallerID {
	return &vtrpcpb.CallerID{Principal: claimedPrincipal}
}

func executeRequest(sql string) *vtgatepb.ExecuteRequest {
	return &vtgatepb.ExecuteRequest{
		CallerId: claimedCaller(),
		Query:    &querypb.BoundQuery{Sql: sql},
	}
}

// startVTGateGRPC builds the process gRPC server the same way vtgate does:
// grpc-port set, grpc-vtgateservice enabled, then createGRPCServer and
// serveGRPC. authMode "" leaves grpc-auth-mode unset. listening is false
// when that server was not created.
func startVTGateGRPC(t *testing.T, authMode string) (*recordingVTGate, vtgateservicepb.VitessClient, bool) {
	t.Helper()

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	credsPath := ""
	if authMode == "static" {
		body, err := json.Marshal([]servenv.StaticAuthConfigEntry{{
			Username: placeholderUser,
			Password: placeholderPassword,
		}})
		require.NoError(t, err)
		credsPath = t.TempDir() + "/grpc-auth.json"
		require.NoError(t, os.WriteFile(credsPath, body, 0o600))
	}

	restore, server := servenv.SetupGRPCTestListener(port, authMode, credsPath)
	t.Cleanup(restore)
	svc := &recordingVTGate{}
	if server == nil {
		return svc, nil, false
	}
	require.True(t, servenv.GRPCCheckServiceMap("vtgateservice"))
	grpcvtgateservice.RegisterForTest(server, svc)
	stopOrca := servenv.ServeGRPCTestServer()
	t.Cleanup(stopOrca)
	t.Cleanup(server.Stop)

	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 20*time.Millisecond)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return svc, vtgateservicepb.NewVitessClient(conn), true
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

type recordedCall struct {
	method     string
	sql        string
	immediate  string
	staticUser string
}

func (c recordedCall) String() string {
	return fmt.Sprintf("%s sql=%q immediate=%q static-user=%q", c.method, c.sql, c.immediate, c.staticUser)
}

// recordingVTGate is the service the real Vitess gRPC handlers call. A call
// recorded here has already passed the gRPC server and grpcvtgateservice.
type recordingVTGate struct {
	mu    sync.Mutex
	calls []recordedCall
}

func (r *recordingVTGate) snapshot() []recordedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedCall, len(r.calls))
	copy(out, r.calls)
	return out
}

func (r *recordingVTGate) record(ctx context.Context, method, sql string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	immediate := ""
	if caller := callerid.ImmediateCallerIDFromContext(ctx); caller != nil {
		immediate = caller.Username
	}
	r.calls = append(r.calls, recordedCall{
		method:     method,
		sql:        sql,
		immediate:  immediate,
		staticUser: servenv.StaticAuthUsernameFromContext(ctx),
	})
}

func (r *recordingVTGate) Execute(ctx context.Context, _ vtgateservice.MySQLConnection, session *vtgatepb.Session, sql string, _ map[string]*querypb.BindVariable, _ bool) (*vtgatepb.Session, *sqltypes.Result, error) {
	r.record(ctx, "Execute", sql)
	return session, &sqltypes.Result{
		Fields: []*querypb.Field{{Name: "id", Type: sqltypes.Int64}},
		Rows:   []sqltypes.Row{{sqltypes.NewInt64(1)}},
	}, nil
}

func (r *recordingVTGate) ExecuteMulti(ctx context.Context, _ vtgateservice.MySQLConnection, session *vtgatepb.Session, sql string) (*vtgatepb.Session, []*sqltypes.Result, error) {
	r.record(ctx, "ExecuteMulti", sql)
	return session, []*sqltypes.Result{{
		Fields: []*querypb.Field{{Name: "id", Type: sqltypes.Int64}},
		Rows:   []sqltypes.Row{{sqltypes.NewInt64(1)}},
	}}, nil
}

func (r *recordingVTGate) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, _ []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	joined := ""
	for i, sql := range sqlList {
		if i > 0 {
			joined += "; "
		}
		joined += sql
	}
	r.record(ctx, "ExecuteBatch", joined)
	return session, []sqltypes.QueryResponse{{
		QueryResult: &sqltypes.Result{
			Fields: []*querypb.Field{{Name: "id", Type: sqltypes.Int64}},
			Rows:   []sqltypes.Row{{sqltypes.NewInt64(1)}},
		},
	}}, nil
}

func (r *recordingVTGate) StreamExecute(ctx context.Context, _ vtgateservice.MySQLConnection, session *vtgatepb.Session, sql string, _ map[string]*querypb.BindVariable, _ bool, callback func(*sqltypes.Result) error) (*vtgatepb.Session, error) {
	r.record(ctx, "StreamExecute", sql)
	err := callback(&sqltypes.Result{
		Fields: []*querypb.Field{{Name: "id", Type: sqltypes.Int64}},
		Rows:   []sqltypes.Row{{sqltypes.NewInt64(1)}},
	})
	return session, err
}

func (r *recordingVTGate) StreamExecuteMulti(ctx context.Context, _ vtgateservice.MySQLConnection, session *vtgatepb.Session, sql string, callback func(sqltypes.QueryResponse, bool, bool) error) (*vtgatepb.Session, error) {
	r.record(ctx, "StreamExecuteMulti", sql)
	err := callback(sqltypes.QueryResponse{QueryResult: &sqltypes.Result{
		Fields: []*querypb.Field{{Name: "id", Type: sqltypes.Int64}},
		Rows:   []sqltypes.Row{{sqltypes.NewInt64(1)}},
	}}, false, true)
	return session, err
}

func (r *recordingVTGate) Prepare(ctx context.Context, session *vtgatepb.Session, sql string) (*vtgatepb.Session, []*querypb.Field, uint16, error) {
	r.record(ctx, "Prepare", sql)
	return session, []*querypb.Field{{Name: "id", Type: sqltypes.Int64}}, 1, nil
}

func (r *recordingVTGate) CloseSession(ctx context.Context, _ *vtgatepb.Session) error {
	r.record(ctx, "CloseSession", "")
	return nil
}

func (r *recordingVTGate) VStream(ctx context.Context, _ topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, _ *vtgatepb.VStreamFlags, send func([]*binlogdatapb.VEvent) error) error {
	target := ""
	if vgtid != nil && len(vgtid.ShardGtids) > 0 {
		target = vgtid.ShardGtids[0].Keyspace
	}
	match := ""
	if filter != nil && len(filter.Rules) > 0 {
		match = filter.Rules[0].Match
	}
	r.record(ctx, "VStream", target+"/"+match)
	return send([]*binlogdatapb.VEvent{{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			TableName: match,
			Keyspace:  target,
			RowChanges: []*binlogdatapb.RowChange{{
				After: &querypb.Row{Lengths: []int64{1}, Values: []byte("1")},
			}},
		},
	}})
}

func (r *recordingVTGate) BinlogDumpGTID(ctx context.Context, req *vtgatepb.BinlogDumpGTIDRequest, send func(*vtgatepb.BinlogDumpResponse) error) error {
	r.record(ctx, "BinlogDumpGTID", req.GetKeyspace()+"/"+req.GetShard())
	return send(&vtgatepb.BinlogDumpResponse{Raw: []byte("binlog-row")})
}

func (r *recordingVTGate) HandlePanic(*error) {}
