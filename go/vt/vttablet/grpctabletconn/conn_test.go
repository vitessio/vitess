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

package grpctabletconn

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	queryservicepb "vitess.io/vitess/go/vt/proto/queryservice"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This test makes sure the go rpc service works
func TestGRPCTabletConn(t *testing.T) {
	// fake service
	service := tabletconntest.CreateFakeServer(t)

	// listen on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a gRPC server and listen on the port
	server := grpc.NewServer()
	grpcqueryservice.Register(server, service)
	go server.Serve(listener)

	// run the test suite
	tabletconntest.TestSuite(t, protocolName, &topodatapb.Tablet{
		Keyspace: tabletconntest.TestTarget.Keyspace,
		Shard:    tabletconntest.TestTarget.Shard,
		Type:     tabletconntest.TestTarget.TabletType,
		Alias:    tabletconntest.TestAlias,
		Hostname: host,
		PortMap: map[string]int32{
			"grpc": int32(port),
		},
	}, service, nil)
}

// This test makes sure the go rpc client auth works
func TestGRPCTabletAuthConn(t *testing.T) {
	// fake service
	service := tabletconntest.CreateFakeServer(t)

	// listen on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a gRPC server and listen on the port
	var opts []grpc.ServerOption
	opts = append(opts, grpc.StreamInterceptor(servenv.FakeAuthStreamInterceptor))
	opts = append(opts, grpc.UnaryInterceptor(servenv.FakeAuthUnaryInterceptor))
	server := grpc.NewServer(opts...)

	grpcqueryservice.Register(server, service)
	go server.Serve(listener)

	authJSON := `{
         "Username": "valid",
         "Password": "valid"
        }`

	f, err := os.CreateTemp("", "static_auth_creds.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	if _, err := io.WriteString(f, authJSON); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// run the test suite
	tabletconntest.TestSuite(t, protocolName, &topodatapb.Tablet{
		Keyspace: tabletconntest.TestTarget.Keyspace,
		Shard:    tabletconntest.TestTarget.Shard,
		Type:     tabletconntest.TestTarget.TabletType,
		Alias:    tabletconntest.TestAlias,
		Hostname: host,
		PortMap: map[string]int32{
			"grpc": int32(port),
		},
	}, service, f)
}

// mockQueryClient is a mock query client that returns an error from Streaming calls,
// but only after storing the context that was passed to the RPC.
type mockQueryClient struct {
	lastCallCtx context.Context
	queryservicepb.QueryClient
}

func (m *mockQueryClient) StreamExecute(ctx context.Context, in *querypb.StreamExecuteRequest, opts ...grpc.CallOption) (queryservicepb.Query_StreamExecuteClient, error) {
	m.lastCallCtx = ctx
	return nil, fmt.Errorf("A general error")
}

func (m *mockQueryClient) BeginStreamExecute(ctx context.Context, in *querypb.BeginStreamExecuteRequest, opts ...grpc.CallOption) (queryservicepb.Query_BeginStreamExecuteClient, error) {
	m.lastCallCtx = ctx
	return nil, fmt.Errorf("A general error")
}

func (m *mockQueryClient) ReserveStreamExecute(ctx context.Context, in *querypb.ReserveStreamExecuteRequest, opts ...grpc.CallOption) (queryservicepb.Query_ReserveStreamExecuteClient, error) {
	m.lastCallCtx = ctx
	return nil, fmt.Errorf("A general error")
}

func (m *mockQueryClient) ReserveBeginStreamExecute(ctx context.Context, in *querypb.ReserveBeginStreamExecuteRequest, opts ...grpc.CallOption) (queryservicepb.Query_ReserveBeginStreamExecuteClient, error) {
	m.lastCallCtx = ctx
	return nil, fmt.Errorf("A general error")
}

func (m *mockQueryClient) VStream(ctx context.Context, in *binlogdatapb.VStreamRequest, opts ...grpc.CallOption) (queryservicepb.Query_VStreamClient, error) {
	m.lastCallCtx = ctx
	return nil, fmt.Errorf("A general error")
}

func (m *mockQueryClient) VStreamRows(ctx context.Context, in *binlogdatapb.VStreamRowsRequest, opts ...grpc.CallOption) (queryservicepb.Query_VStreamRowsClient, error) {
	m.lastCallCtx = ctx
	return nil, fmt.Errorf("A general error")
}

func (m *mockQueryClient) VStreamTables(ctx context.Context, in *binlogdatapb.VStreamTablesRequest, opts ...grpc.CallOption) (queryservicepb.Query_VStreamTablesClient, error) {
	m.lastCallCtx = ctx
	return nil, fmt.Errorf("A general error")
}

func (m *mockQueryClient) VStreamResults(ctx context.Context, in *binlogdatapb.VStreamResultsRequest, opts ...grpc.CallOption) (queryservicepb.Query_VStreamResultsClient, error) {
	m.lastCallCtx = ctx
	return nil, fmt.Errorf("A general error")
}

func (m *mockQueryClient) GetSchema(ctx context.Context, in *querypb.GetSchemaRequest, opts ...grpc.CallOption) (queryservicepb.Query_GetSchemaClient, error) {
	m.lastCallCtx = ctx
	return nil, fmt.Errorf("A general error")
}

var _ queryservicepb.QueryClient = (*mockQueryClient)(nil)

// TestGoRoutineLeakPrevention tests that after all the RPCs that stream queries, we end up closing the context that was passed to it, to prevent go routines from being leaked.
func TestGoRoutineLeakPrevention(t *testing.T) {
	mqc := &mockQueryClient{}
	qc := &gRPCQueryClient{
		mu: sync.RWMutex{},
		cc: &grpc.ClientConn{},
		c:  mqc,
	}
	_ = qc.StreamExecute(context.Background(), nil, "", nil, 0, 0, nil, func(result *sqltypes.Result) error {
		return nil
	})
	require.Error(t, mqc.lastCallCtx.Err())

	_, _ = qc.BeginStreamExecute(context.Background(), nil, nil, "", nil, 0, nil, func(result *sqltypes.Result) error {
		return nil
	})
	require.Error(t, mqc.lastCallCtx.Err())

	_, _ = qc.ReserveBeginStreamExecute(context.Background(), nil, nil, nil, "", nil, nil, func(result *sqltypes.Result) error {
		return nil
	})
	require.Error(t, mqc.lastCallCtx.Err())

	_, _ = qc.ReserveStreamExecute(context.Background(), nil, nil, "", nil, 0, nil, func(result *sqltypes.Result) error {
		return nil
	})
	require.Error(t, mqc.lastCallCtx.Err())

	_ = qc.VStream(context.Background(), &binlogdatapb.VStreamRequest{}, func(events []*binlogdatapb.VEvent) error {
		return nil
	})
	require.Error(t, mqc.lastCallCtx.Err())

	_ = qc.VStreamRows(context.Background(), &binlogdatapb.VStreamRowsRequest{}, func(response *binlogdatapb.VStreamRowsResponse) error {
		return nil
	})
	require.Error(t, mqc.lastCallCtx.Err())

	_ = qc.VStreamResults(context.Background(), nil, "", func(response *binlogdatapb.VStreamResultsResponse) error {
		return nil
	})
	require.Error(t, mqc.lastCallCtx.Err())

	_ = qc.VStreamTables(context.Background(), &binlogdatapb.VStreamTablesRequest{}, func(response *binlogdatapb.VStreamTablesResponse) error {
		return nil
	})
	require.Error(t, mqc.lastCallCtx.Err())

	_ = qc.GetSchema(context.Background(), nil, querypb.SchemaTableType_TABLES, nil, func(schemaRes *querypb.GetSchemaResponse) error {
		return nil
	})
	require.Error(t, mqc.lastCallCtx.Err())
}
