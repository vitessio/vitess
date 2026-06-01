/*
Copyright 2025 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// rawStreamingService overrides the *Raw methods on the fake query service so
// they stream deterministic byte chunks. This lets us exercise the gRPC
// client's one-stream-per-query (no pool) lifecycle end to end.
type rawStreamingService struct {
	*tabletconntest.FakeQueryService
	chunks          [][]byte
	insertID        uint64
	insertIDChanged bool
	// streamErr, when set, is returned after the chunks have been streamed,
	// simulating a query that fails mid-stream. The real gRPC handler delivers
	// it in-band via the terminal done=true message.
	streamErr error
}

func (s *rawStreamingService) sendChunks(callback func(raw []byte) error) error {
	for _, c := range s.chunks {
		if err := callback(c); err != nil {
			return err
		}
	}
	return s.streamErr
}

func (s *rawStreamingService) StreamExecuteRaw(ctx context.Context, session queryservice.Session, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, reservedID int64, options *querypb.ExecuteOptions, buf []byte, callback func(raw []byte) error) (queryservice.StreamExecuteRawState, error) {
	err := s.sendChunks(callback)
	return queryservice.StreamExecuteRawState{InsertID: s.insertID, InsertIDChanged: s.insertIDChanged}, err
}

func (s *rawStreamingService) BeginStreamExecuteRaw(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions, buf []byte, callback func(raw []byte) error) (queryservice.TransactionState, error) {
	err := s.sendChunks(callback)
	return queryservice.TransactionState{TransactionID: 1234, TabletAlias: tabletconntest.TestAlias, InsertID: s.insertID, InsertIDChanged: s.insertIDChanged}, err
}

func (s *rawStreamingService) ReserveStreamExecuteRaw(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, buf []byte, callback func(raw []byte) error) (queryservice.ReservedState, error) {
	err := s.sendChunks(callback)
	return queryservice.ReservedState{ReservedID: 5678, TabletAlias: tabletconntest.TestAlias, InsertID: s.insertID, InsertIDChanged: s.insertIDChanged}, err
}

func (s *rawStreamingService) ReserveBeginStreamExecuteRaw(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, buf []byte, callback func(raw []byte) error) (queryservice.ReservedTransactionState, error) {
	err := s.sendChunks(callback)
	return queryservice.ReservedTransactionState{TransactionID: 1234, ReservedID: 5678, TabletAlias: tabletconntest.TestAlias, InsertID: s.insertID, InsertIDChanged: s.insertIDChanged}, err
}

// dialRawStreamingTablet stands up a real gRPC query server backed by svc and
// returns a connected client.
func dialRawStreamingTablet(ctx context.Context, t *testing.T, svc queryservice.QueryService) queryservice.QueryService {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := listener.Addr().(*net.TCPAddr).Port

	server := grpc.NewServer()
	grpcqueryservice.Register(server, svc)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	conn, err := DialTablet(ctx, &topodatapb.Tablet{
		Keyspace: tabletconntest.TestTarget.Keyspace,
		Shard:    tabletconntest.TestTarget.Shard,
		Type:     tabletconntest.TestTarget.TabletType,
		Alias:    tabletconntest.TestAlias,
		Hostname: host,
		PortMap:  map[string]int32{"grpc": int32(port)},
	}, grpcclient.FailFast(false))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(ctx) })

	return conn
}

// TestStreamExecuteRawNoPool verifies that the pool-less client opens and
// closes a fresh bidi stream for every query: several sequential calls on the
// same connection each round-trip their raw bytes correctly.
func TestStreamExecuteRawNoPool(t *testing.T) {
	ctx := t.Context()
	svc := &rawStreamingService{
		FakeQueryService: tabletconntest.CreateFakeServer(t),
		chunks:           [][]byte{[]byte("hello "), []byte("world")},
	}
	conn := dialRawStreamingTablet(ctx, t, svc)

	for range 3 {
		var got []byte
		state, err := conn.StreamExecuteRaw(ctx, nil, tabletconntest.TestTarget, "select 1", nil, 0, 0, nil, nil, func(raw []byte) error {
			got = append(got, raw...)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, "hello world", string(got))
		require.False(t, state.InsertIDChanged)
	}
}

// TestStreamExecuteRawInsertID verifies the terminal done=true message carries
// the out-of-band LAST_INSERT_ID back to the client. Because MySQL hardcodes
// last_insert_id=0 in a streamed SELECT terminator, the value rides the proto
// response rather than the raw bytes, including the explicit changed bit so a
// genuine LAST_INSERT_ID(0) is distinguishable from "unset".
func TestStreamExecuteRawInsertID(t *testing.T) {
	ctx := t.Context()
	svc := &rawStreamingService{
		FakeQueryService: tabletconntest.CreateFakeServer(t),
		chunks:           [][]byte{[]byte("rows")},
		insertID:         0,
		insertIDChanged:  true,
	}
	conn := dialRawStreamingTablet(ctx, t, svc)

	state, err := conn.StreamExecuteRaw(ctx, nil, tabletconntest.TestTarget, "select last_insert_id(0)", nil, 0, 0, nil, nil, func(raw []byte) error {
		return nil
	})
	require.NoError(t, err)
	require.True(t, state.InsertIDChanged)
	require.Equal(t, uint64(0), state.InsertID)
}

// TestBeginStreamExecuteRawNoPool verifies the terminal done=true message
// carries the transaction state back to the client.
func TestBeginStreamExecuteRawNoPool(t *testing.T) {
	ctx := t.Context()
	svc := &rawStreamingService{
		FakeQueryService: tabletconntest.CreateFakeServer(t),
		chunks:           [][]byte{[]byte("abc")},
	}
	conn := dialRawStreamingTablet(ctx, t, svc)

	var got []byte
	state, err := conn.BeginStreamExecuteRaw(ctx, nil, tabletconntest.TestTarget, nil, "select 1", nil, 0, nil, nil, func(raw []byte) error {
		got = append(got, raw...)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "abc", string(got))
	require.Equal(t, int64(1234), state.TransactionID)
}

// TestStreamExecuteRawServerError verifies that a query error raised after some
// bytes have already streamed is delivered in-band via the terminal done=true
// message: the chunks sent before the error still reach the client, and the
// error surfaces with its code intact.
func TestStreamExecuteRawServerError(t *testing.T) {
	ctx := t.Context()
	svc := &rawStreamingService{
		FakeQueryService: tabletconntest.CreateFakeServer(t),
		chunks:           [][]byte{[]byte("partial")},
		streamErr:        vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "boom mid-stream"),
	}
	conn := dialRawStreamingTablet(ctx, t, svc)

	var got []byte
	_, err := conn.StreamExecuteRaw(ctx, nil, tabletconntest.TestTarget, "select 1", nil, 0, 0, nil, nil, func(raw []byte) error {
		got = append(got, raw...)
		return nil
	})
	require.Equal(t, "partial", string(got))
	require.ErrorContains(t, err, "boom mid-stream")
	require.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
}

// TestStreamExecuteRawCallbackError verifies that when the client's callback
// returns an error mid-stream, that error is what the call returns (and the
// client tears the stream down via its deferred cancel).
func TestStreamExecuteRawCallbackError(t *testing.T) {
	ctx := t.Context()
	svc := &rawStreamingService{
		FakeQueryService: tabletconntest.CreateFakeServer(t),
		chunks:           [][]byte{[]byte("a"), []byte("b"), []byte("c")},
	}
	conn := dialRawStreamingTablet(ctx, t, svc)

	calls := 0
	_, err := conn.StreamExecuteRaw(ctx, nil, tabletconntest.TestTarget, "select 1", nil, 0, 0, nil, nil, func(raw []byte) error {
		calls++
		if calls == 2 {
			return vterrors.Errorf(vtrpcpb.Code_ABORTED, "client gave up")
		}
		return nil
	})
	require.ErrorContains(t, err, "client gave up")
}

// TestReserveStreamExecuteRawNoPool exercises the ReserveStreamExecuteRaw client
// variant's one-stream-per-query lifecycle and terminal state delivery.
func TestReserveStreamExecuteRawNoPool(t *testing.T) {
	ctx := t.Context()
	svc := &rawStreamingService{
		FakeQueryService: tabletconntest.CreateFakeServer(t),
		chunks:           [][]byte{[]byte("xyz")},
	}
	conn := dialRawStreamingTablet(ctx, t, svc)

	var got []byte
	state, err := conn.ReserveStreamExecuteRaw(ctx, nil, tabletconntest.TestTarget, nil, "select 1", nil, 0, nil, nil, func(raw []byte) error {
		got = append(got, raw...)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "xyz", string(got))
	require.Equal(t, int64(5678), state.ReservedID)
}

// TestReserveBeginStreamExecuteRawNoPool exercises the ReserveBeginStreamExecuteRaw
// client variant's lifecycle and terminal state delivery.
func TestReserveBeginStreamExecuteRawNoPool(t *testing.T) {
	ctx := t.Context()
	svc := &rawStreamingService{
		FakeQueryService: tabletconntest.CreateFakeServer(t),
		chunks:           [][]byte{[]byte("xyz")},
	}
	conn := dialRawStreamingTablet(ctx, t, svc)

	var got []byte
	state, err := conn.ReserveBeginStreamExecuteRaw(ctx, nil, tabletconntest.TestTarget, nil, nil, "select 1", nil, nil, nil, func(raw []byte) error {
		got = append(got, raw...)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "xyz", string(got))
	require.Equal(t, int64(1234), state.TransactionID)
	require.Equal(t, int64(5678), state.ReservedID)
}
