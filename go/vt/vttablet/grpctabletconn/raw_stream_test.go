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
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"
)

// rawStreamingService overrides the *Raw methods on the fake query service so
// they stream deterministic byte chunks. This lets us exercise the gRPC
// client's one-stream-per-query (no pool) lifecycle end to end.
type rawStreamingService struct {
	*tabletconntest.FakeQueryService
	chunks          [][]byte
	insertID        uint64
	insertIDChanged bool
}

func (s *rawStreamingService) StreamExecuteRaw(ctx context.Context, session queryservice.Session, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, reservedID int64, options *querypb.ExecuteOptions, buf []byte, callback func(raw []byte) error) (queryservice.StreamExecuteRawState, error) {
	for _, c := range s.chunks {
		if err := callback(c); err != nil {
			return queryservice.StreamExecuteRawState{}, err
		}
	}
	return queryservice.StreamExecuteRawState{InsertID: s.insertID, InsertIDChanged: s.insertIDChanged}, nil
}

func (s *rawStreamingService) BeginStreamExecuteRaw(ctx context.Context, session queryservice.Session, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions, buf []byte, callback func(raw []byte) error) (queryservice.TransactionState, error) {
	for _, c := range s.chunks {
		if err := callback(c); err != nil {
			return queryservice.TransactionState{}, err
		}
	}
	return queryservice.TransactionState{TransactionID: 1234, TabletAlias: tabletconntest.TestAlias, InsertID: s.insertID, InsertIDChanged: s.insertIDChanged}, nil
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
