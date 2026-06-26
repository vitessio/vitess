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

package grpcvtgateservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
)

type mockVTGateService struct {
	executeResult             *sqltypes.Result
	executeError              error
	executeMultiResults       []*sqltypes.Result
	executeMultiIngressBytes  []uint64
	streamResults             []*sqltypes.Result
	executeIngressBytes       []uint64
	executeBatchIngressBytes  []uint64
	prepareIngressBytes       []uint64
	streamExecuteIngressBytes []uint64
}

func (m *mockVTGateService) Execute(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, prepared bool) (*vtgatepb.Session, *sqltypes.Result, error) {
	if ingressBytes, ok := vtgateservice.IngressBytesFromContext(ctx); ok {
		m.executeIngressBytes = append(m.executeIngressBytes, ingressBytes)
	}
	return session, m.executeResult, m.executeError
}

func (m *mockVTGateService) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, bindVariablesList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	if ingressBytes, ok := vtgateservice.IngressBytesFromContext(ctx); ok {
		m.executeBatchIngressBytes = append(m.executeBatchIngressBytes, ingressBytes)
	}
	return session, nil, nil
}

func (m *mockVTGateService) StreamExecute(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, prepared bool, callback func(*sqltypes.Result) error) (*vtgatepb.Session, error) {
	if ingressBytes, ok := vtgateservice.IngressBytesFromContext(ctx); ok {
		m.streamExecuteIngressBytes = append(m.streamExecuteIngressBytes, ingressBytes)
	}
	for _, result := range m.streamResults {
		if err := callback(result); err != nil {
			return session, err
		}
	}
	return session, m.executeError
}

func (m *mockVTGateService) Prepare(ctx context.Context, session *vtgatepb.Session, sql string) (*vtgatepb.Session, []*querypb.Field, uint16, error) {
	if ingressBytes, ok := vtgateservice.IngressBytesFromContext(ctx); ok {
		m.prepareIngressBytes = append(m.prepareIngressBytes, ingressBytes)
	}
	return session, nil, 0, nil
}

func (m *mockVTGateService) ExecuteMulti(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, session *vtgatepb.Session, sqlString string) (*vtgatepb.Session, []*sqltypes.Result, error) {
	if ingressBytes, ok := vtgateservice.IngressBytesFromContext(ctx); ok {
		m.executeMultiIngressBytes = append(m.executeMultiIngressBytes, ingressBytes)
	}
	return session, m.executeMultiResults, m.executeError
}

func (m *mockVTGateService) StreamExecuteMulti(ctx context.Context, mysqlCtx vtgateservice.MySQLConnection, session *vtgatepb.Session, sqlString string, callback func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) (*vtgatepb.Session, error) {
	for i, result := range m.streamResults {
		qr := sqltypes.QueryResponse{QueryResult: result}
		more := i < len(m.streamResults)-1
		if err := callback(qr, more, true); err != nil {
			return session, err
		}
	}
	return session, m.executeError
}

func (m *mockVTGateService) CloseSession(ctx context.Context, session *vtgatepb.Session) error {
	return nil
}

func (m *mockVTGateService) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags, send func([]*binlogdatapb.VEvent) error) error {
	return nil
}

func (m *mockVTGateService) BinlogDumpGTID(ctx context.Context, req *vtgatepb.BinlogDumpGTIDRequest, send func(*vtgatepb.BinlogDumpResponse) error) error {
	return nil
}

func (m *mockVTGateService) HandlePanic(err *error) {}

type fakeStreamExecuteServer struct {
	ctx       context.Context
	responses []*vtgatepb.StreamExecuteResponse
}

func (s *fakeStreamExecuteServer) Send(response *vtgatepb.StreamExecuteResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

func (s *fakeStreamExecuteServer) SetHeader(metadata.MD) error {
	return nil
}

func (s *fakeStreamExecuteServer) SendHeader(metadata.MD) error {
	return nil
}

func (s *fakeStreamExecuteServer) SetTrailer(metadata.MD) {}

func (s *fakeStreamExecuteServer) Context() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *fakeStreamExecuteServer) SendMsg(any) error {
	return nil
}

func (s *fakeStreamExecuteServer) RecvMsg(any) error {
	return nil
}

// TestGRPCExecuteSetsIngressBytes verifies that unary Execute stores the request
// size estimate in the forwarded context.
func TestGRPCExecuteSetsIngressBytes(t *testing.T) {
	mockService := &mockVTGateService{
		executeResult: &sqltypes.Result{},
	}
	grpcVTGate := &VTGate{server: mockService}
	request := &vtgatepb.ExecuteRequest{
		Query: &querypb.BoundQuery{
			Sql: "SELECT id FROM test",
		},
		Session: &vtgatepb.Session{Autocommit: true},
	}

	_, err := grpcVTGate.Execute(context.Background(), request)

	require.NoError(t, err)
	assert.Equal(t, []uint64{uint64(request.SizeVT())}, mockService.executeIngressBytes)
}

// TestGRPCStreamExecuteSetsIngressBytes verifies that streaming Execute stores
// the request size estimate in the forwarded context.
func TestGRPCStreamExecuteSetsIngressBytes(t *testing.T) {
	mockService := &mockVTGateService{
		streamResults: []*sqltypes.Result{{}},
	}
	grpcVTGate := &VTGate{server: mockService}
	request := &vtgatepb.StreamExecuteRequest{
		Query: &querypb.BoundQuery{
			Sql: "SELECT id FROM test",
		},
		Session: &vtgatepb.Session{Autocommit: true},
	}
	stream := &fakeStreamExecuteServer{ctx: context.Background()}

	err := grpcVTGate.StreamExecute(request, stream)

	require.NoError(t, err)
	assert.Equal(t, []uint64{uint64(request.SizeVT())}, mockService.streamExecuteIngressBytes)
}

// TestGRPCExecuteMultiSetsIngressBytes verifies that ExecuteMulti carries the
// whole request size estimate into VTGate.
func TestGRPCExecuteMultiSetsIngressBytes(t *testing.T) {
	mockService := &mockVTGateService{
		executeMultiResults: []*sqltypes.Result{{}, {}},
	}
	grpcVTGate := &VTGate{server: mockService}
	request := &vtgatepb.ExecuteMultiRequest{
		Sql:     "select 1;select 222222",
		Session: &vtgatepb.Session{Autocommit: true},
	}

	_, err := grpcVTGate.ExecuteMulti(context.Background(), request)

	require.NoError(t, err)
	assert.Equal(t, []uint64{uint64(request.SizeVT())}, mockService.executeMultiIngressBytes)
}

// TestGRPCExecuteBatchSetsIngressBytes verifies that ExecuteBatch carries the
// whole request size estimate into VTGate.
func TestGRPCExecuteBatchSetsIngressBytes(t *testing.T) {
	mockService := &mockVTGateService{}
	grpcVTGate := &VTGate{server: mockService}
	request := &vtgatepb.ExecuteBatchRequest{
		Queries: []*querypb.BoundQuery{
			{Sql: "select 1"},
			{Sql: "select 222222"},
		},
		Session: &vtgatepb.Session{Autocommit: true},
	}

	_, err := grpcVTGate.ExecuteBatch(context.Background(), request)

	require.NoError(t, err)
	assert.Equal(t, []uint64{uint64(request.SizeVT())}, mockService.executeBatchIngressBytes)
}

// TestGRPCPrepareSetsIngressBytes verifies that Prepare stores the request
// size estimate in the forwarded context.
func TestGRPCPrepareSetsIngressBytes(t *testing.T) {
	mockService := &mockVTGateService{}
	grpcVTGate := &VTGate{server: mockService}
	request := &vtgatepb.PrepareRequest{
		Query: &querypb.BoundQuery{
			Sql: "SELECT id FROM test WHERE id = ?",
		},
		Session: &vtgatepb.Session{Autocommit: true},
	}

	_, err := grpcVTGate.Prepare(context.Background(), request)

	require.NoError(t, err)
	assert.Equal(t, []uint64{uint64(request.SizeVT())}, mockService.prepareIngressBytes)
}
