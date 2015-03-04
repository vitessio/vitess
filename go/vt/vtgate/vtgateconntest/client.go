// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgateconntest provides the test methods to make sure a
// vtgateconn/vtgateservice pair over RPC works correctly.
package vtgateconntest

import (
	"reflect"
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"golang.org/x/net/context"
)

// fakeVTGateService has the server side of this fake
type fakeVTGateService struct {
	t *testing.T
}

// Execute is part of the VTGateService interface
func (f *fakeVTGateService) Execute(ctx context.Context, query *proto.Query, reply *proto.QueryResult) error {
	if query.Sql != executeQuery {
		f.t.Fatalf("invalid Execute.Query.Sql: got %v expected %v", query.Sql, executeQuery)
	}
	if !reflect.DeepEqual(query.BindVariables, executeBindVars) {
		f.t.Fatalf("invalid Execute.Query.BindVariables: got %v expected %v", query.BindVariables, executeBindVars)
	}
	if query.TabletType != executeTabletType {
		f.t.Fatalf("invalid Execute.Query.TabletType: got %v expected %v", query.TabletType, executeTabletType)
	}
	*reply = proto.QueryResult{
		Result: &executeQueryResult,
	}
	return nil
}

const executeQuery = "executeQuery"

var executeBindVars = map[string]interface{}{
	"bind1": int64(0),
}

const executeTabletType = topo.TYPE_RDONLY

var executeQueryResult = mproto.QueryResult{
	Fields: []mproto.Field{
		mproto.Field{
			Name: "field1",
			Type: 42,
		},
		mproto.Field{
			Name: "field2",
			Type: 73,
		},
	},
	RowsAffected: 123,
	InsertId:     72,
	Rows: [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("row1 value1")),
			sqltypes.MakeString([]byte("row1 value2")),
		},
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("row2 value1")),
			sqltypes.MakeString([]byte("row2 value2")),
		},
	},
}

func testExecute(t *testing.T, conn vtgateconn.VTGateConn) {
	ctx := context.Background()
	qr, err := conn.Execute(ctx, executeQuery, executeBindVars, executeTabletType)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if !reflect.DeepEqual(*qr, executeQueryResult) {
		t.Errorf("Unexpected result from Execute: got %v wanted %v", qr, executeQueryResult)
	}
}

// ExecuteShard is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteShard(ctx context.Context, query *proto.QueryShard, reply *proto.QueryResult) error {
	return nil
}

// ExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) error {
	return nil
}

// ExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) error {
	return nil
}

// ExecuteEntityIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteEntityIds(ctx context.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) error {
	return nil
}

// ExecuteBatchShard is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchShard(ctx context.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	return nil
}

// ExecuteBatchKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchKeyspaceIds(ctx context.Context, batchQuery *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error {
	return nil
}

// StreamExecute is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(*proto.QueryResult) error) error {
	return nil
}

// StreamExecuteShard is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteShard(ctx context.Context, query *proto.QueryShard, sendReply func(*proto.QueryResult) error) error {
	return nil
}

// StreamExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) error {
	return nil
}

// StreamExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, sendReply func(*proto.QueryResult) error) error {
	return nil
}

// Begin is part of the VTGateService interface
func (f *fakeVTGateService) Begin(ctx context.Context, outSession *proto.Session) error {
	return nil
}

// Commit is part of the VTGateService interface
func (f *fakeVTGateService) Commit(ctx context.Context, inSession *proto.Session) error {
	return nil
}

// Rollback is part of the VTGateService interface
func (f *fakeVTGateService) Rollback(ctx context.Context, inSession *proto.Session) error {
	return nil
}

// SplitQuery is part of the VTGateService interface
func (f *fakeVTGateService) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	return nil
}

// CreateFakeServer returns the fake server for the tests
func CreateFakeServer(t *testing.T) vtgateservice.VTGateService {
	return &fakeVTGateService{t}
}

// TestSuite runs all the tests
func TestSuite(t *testing.T, conn vtgateconn.VTGateConn) {
	testExecute(t, conn)
}
