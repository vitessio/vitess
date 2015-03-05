// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package tabletconntest provides the test methods to make sure a
// tabletconn/queryservice pair over RPC works correctly.
package tabletconntest

import (
	"reflect"
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"golang.org/x/net/context"
)

// fakeQueryService has the server side of this fake
type fakeQueryService struct {
	t *testing.T
}

// TestKeyspace is the Keyspace we use for this test
const TestKeyspace = "test_keyspace"

// TestShard is the Shard we use for this test
const TestShard = "test_shard"

const testSessionId int64 = 5678

// GetSessionId is part of the queryservice.QueryService interface
func (f *fakeQueryService) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	if sessionParams.Keyspace != TestKeyspace {
		f.t.Fatalf("invalid keyspace: got %v expected %v", sessionParams.Keyspace, TestKeyspace)
	}
	if sessionParams.Shard != TestShard {
		f.t.Fatalf("invalid shard: got %v expected %v", sessionParams.Shard, TestShard)
	}
	sessionInfo.SessionId = testSessionId
	return nil
}

// Begin is part of the queryservice.QueryService interface
func (f *fakeQueryService) Begin(ctx context.Context, session *proto.Session, txInfo *proto.TransactionInfo) error {
	return nil
}

// Commit is part of the queryservice.QueryService interface
func (f *fakeQueryService) Commit(ctx context.Context, session *proto.Session) error {
	return nil
}

// Rollback is part of the queryservice.QueryService interface
func (f *fakeQueryService) Rollback(ctx context.Context, session *proto.Session) error {
	return nil
}

// Execute is part of the queryservice.QueryService interface
func (f *fakeQueryService) Execute(ctx context.Context, query *proto.Query, reply *mproto.QueryResult) error {
	if query.Sql != executeQuery {
		f.t.Fatalf("invalid Execute.Query.Sql: got %v expected %v", query.Sql, executeQuery)
	}
	if !reflect.DeepEqual(query.BindVariables, executeBindVars) {
		f.t.Fatalf("invalid Execute.Query.BindVariables: got %v expected %v", query.BindVariables, executeBindVars)
	}
	if query.SessionId != testSessionId {
		f.t.Fatalf("invalid Execute.Query.SessionId: got %v expected %v", query.SessionId, testSessionId)
	}
	if query.TransactionId != executeTransactionId {
		f.t.Fatalf("invalid Execute.Query.TransactionId: got %v expected %v", query.TransactionId, executeTransactionId)
	}
	*reply = executeQueryResult
	return nil
}

const executeQuery = "executeQuery"

var executeBindVars = map[string]interface{}{
	"bind1": int64(0),
}

const executeTransactionId int64 = 678

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

func testExecute(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	qr, err := conn.Execute(ctx, executeQuery, executeBindVars, executeTransactionId)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if !reflect.DeepEqual(*qr, executeQueryResult) {
		t.Errorf("Unexpected result from Execute: got %v wanted %v", qr, executeQueryResult)
	}
}

// StreamExecute is part of the queryservice.QueryService interface
func (f *fakeQueryService) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(*mproto.QueryResult) error) error {
	return nil
}

// ExecuteBatch is part of the queryservice.QueryService interface
func (f *fakeQueryService) ExecuteBatch(ctx context.Context, queryList *proto.QueryList, reply *proto.QueryResultList) error {
	return nil
}

// SplitQuery is part of the queryservice.QueryService interface
func (f *fakeQueryService) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	return nil
}

// CreateFakeServer returns the fake server for the tests
func CreateFakeServer(t *testing.T) queryservice.QueryService {
	return &fakeQueryService{t}
}

// TestSuite runs all the tests
func TestSuite(t *testing.T, conn tabletconn.TabletConn) {
	testExecute(t, conn)
}
