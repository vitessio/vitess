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

package grpctabletconn

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// rawResultCharsetRecorder is a query service that records, for each method
// that returns rows, whether its caller marked the call as one that converts
// the text of the result itself.
type rawResultCharsetRecorder struct {
	queryservice.QueryService

	mu   sync.Mutex
	seen map[string]bool
}

func (r *rawResultCharsetRecorder) record(ctx context.Context, method string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen[method] = queryservice.IsRawResultCharset(ctx)
}

func (r *rawResultCharsetRecorder) HandlePanic(err *error) {}

func (r *rawResultCharsetRecorder) Execute(ctx context.Context, session queryservice.Session, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	r.record(ctx, "Execute")
	return &sqltypes.Result{}, nil
}

func (r *rawResultCharsetRecorder) StreamExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	r.record(ctx, "StreamExecute")
	return nil
}

func (r *rawResultCharsetRecorder) BeginExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (queryservice.TransactionState, *sqltypes.Result, error) {
	r.record(ctx, "BeginExecute")
	return queryservice.TransactionState{}, &sqltypes.Result{}, nil
}

func (r *rawResultCharsetRecorder) BeginStreamExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.TransactionState, error) {
	r.record(ctx, "BeginStreamExecute")
	return queryservice.TransactionState{}, nil
}

func (r *rawResultCharsetRecorder) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	r.record(ctx, "MessageStream")
	return nil
}

func (r *rawResultCharsetRecorder) ReserveExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (queryservice.ReservedState, *sqltypes.Result, error) {
	r.record(ctx, "ReserveExecute")
	return queryservice.ReservedState{}, &sqltypes.Result{}, nil
}

func (r *rawResultCharsetRecorder) ReserveStreamExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.ReservedState, error) {
	r.record(ctx, "ReserveStreamExecute")
	return queryservice.ReservedState{}, nil
}

func (r *rawResultCharsetRecorder) ReserveBeginExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (queryservice.ReservedTransactionState, *sqltypes.Result, error) {
	r.record(ctx, "ReserveBeginExecute")
	return queryservice.ReservedTransactionState{}, &sqltypes.Result{}, nil
}

func (r *rawResultCharsetRecorder) ReserveBeginStreamExecute(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.ReservedTransactionState, error) {
	r.record(ctx, "ReserveBeginStreamExecute")
	return queryservice.ReservedTransactionState{}, nil
}

// TestRawResultCharsetOverGRPC checks that the mark of
// queryservice.ContextWithRawResultCharset reaches the tablet through every
// request that returns rows, and that a call without the mark reaches it
// without one.
func TestRawResultCharsetOverGRPC(t *testing.T) {
	recorder := &rawResultCharsetRecorder{seen: make(map[string]bool)}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	grpcqueryservice.Register(server, recorder)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	addr := listener.Addr().(*net.TCPAddr)
	conn, err := DialTablet(t.Context(), &topodatapb.Tablet{
		Keyspace: tabletconntest.TestTarget.Keyspace,
		Shard:    tabletconntest.TestTarget.Shard,
		Type:     tabletconntest.TestTarget.TabletType,
		Alias:    tabletconntest.TestAlias,
		Hostname: addr.IP.String(),
		PortMap:  map[string]int32{"grpc": int32(addr.Port)},
	}, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })

	target := tabletconntest.TestTarget
	discard := func(*sqltypes.Result) error { return nil }
	calls := map[string]func(ctx context.Context) error{
		"Execute": func(ctx context.Context) error {
			_, err := conn.Execute(ctx, nil, target, "select 1", nil, 0, 0, nil)
			return err
		},
		"StreamExecute": func(ctx context.Context) error {
			return conn.StreamExecute(ctx, nil, target, "select 1", nil, 0, 0, nil, discard)
		},
		"BeginExecute": func(ctx context.Context) error {
			_, _, err := conn.BeginExecute(ctx, nil, target, nil, "select 1", nil, 0, nil)
			return err
		},
		"BeginStreamExecute": func(ctx context.Context) error {
			_, err := conn.BeginStreamExecute(ctx, nil, target, nil, "select 1", nil, 0, nil, discard)
			return err
		},
		"MessageStream": func(ctx context.Context) error {
			return conn.MessageStream(ctx, target, "msg", discard)
		},
		"ReserveExecute": func(ctx context.Context) error {
			_, _, err := conn.ReserveExecute(ctx, nil, target, nil, "select 1", nil, 0, nil)
			return err
		},
		"ReserveStreamExecute": func(ctx context.Context) error {
			_, err := conn.ReserveStreamExecute(ctx, nil, target, nil, "select 1", nil, 0, nil, discard)
			return err
		},
		"ReserveBeginExecute": func(ctx context.Context) error {
			_, _, err := conn.ReserveBeginExecute(ctx, nil, target, nil, nil, "select 1", nil, nil)
			return err
		},
		"ReserveBeginStreamExecute": func(ctx context.Context) error {
			_, err := conn.ReserveBeginStreamExecute(ctx, nil, target, nil, nil, "select 1", nil, nil, discard)
			return err
		},
	}

	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, call(t.Context()))
			recorder.mu.Lock()
			seen, called := recorder.seen[name]
			recorder.mu.Unlock()
			require.True(t, called)
			assert.False(t, seen, "a call without the mark")

			require.NoError(t, call(queryservice.ContextWithRawResultCharset(t.Context())))
			recorder.mu.Lock()
			seen = recorder.seen[name]
			recorder.mu.Unlock()
			assert.True(t, seen, "a call with the mark")
		})
	}
}
