// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgateconntest provides the test methods to make sure a
// vtgateconn/vtgateservice pair over RPC works correctly.
package vtgateconntest

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"golang.org/x/net/context"
)

// fakeVTGateService has the server side of this fake
type fakeVTGateService struct {
	t      *testing.T
	panics bool
}

// Execute is part of the VTGateService interface
func (f *fakeVTGateService) Execute(ctx context.Context, query *proto.Query, reply *proto.QueryResult) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[query.Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", query.Sql)
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.execQuery)
		return nil
	}
	*reply = *execCase.reply
	return nil
}

// ExecuteShard is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteShard(ctx context.Context, query *proto.QueryShard, reply *proto.QueryResult) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[query.Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", query.Sql)
	}
	if !reflect.DeepEqual(query, execCase.shardQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.shardQuery)
		return nil
	}
	*reply = *execCase.reply
	return nil
}

// ExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[query.Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", query.Sql)
	}
	if !reflect.DeepEqual(query, execCase.keyspaceIdQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.keyspaceIdQuery)
		return nil
	}
	*reply = *execCase.reply
	return nil
}

// ExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[query.Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", query.Sql)
	}
	if !reflect.DeepEqual(query, execCase.keyRangeQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.keyRangeQuery)
		return nil
	}
	*reply = *execCase.reply
	return nil
}

// ExecuteEntityIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteEntityIds(ctx context.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[query.Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", query.Sql)
	}
	if !reflect.DeepEqual(query, execCase.entityIdsQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.entityIdsQuery)
		return nil
	}
	*reply = *execCase.reply
	return nil
}

// ExecuteBatchShard is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchShard(ctx context.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[batchQuery.Queries[0].Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", batchQuery.Queries[0].Sql)
	}
	if !reflect.DeepEqual(batchQuery, execCase.batchQueryShard) {
		f.t.Errorf("Execute: %+v, want %+v", batchQuery, execCase.batchQueryShard)
		return nil
	}
	reply.Error = execCase.reply.Error
	if reply.Error == "" && execCase.reply.Result != nil {
		reply.List = []mproto.QueryResult{*execCase.reply.Result}
	}
	reply.Session = execCase.reply.Session
	return nil
}

// ExecuteBatchKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchKeyspaceIds(ctx context.Context, batchQuery *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[batchQuery.Queries[0].Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", batchQuery.Queries[0].Sql)
	}
	if !reflect.DeepEqual(batchQuery, execCase.keyspaceIdBatchQuery) {
		f.t.Errorf("Execute: %+v, want %+v", batchQuery, execCase.keyspaceIdBatchQuery)
		return nil
	}
	reply.Error = execCase.reply.Error
	if reply.Error == "" && execCase.reply.Result != nil {
		reply.List = []mproto.QueryResult{*execCase.reply.Result}
	}
	reply.Session = execCase.reply.Session
	return nil
}

// StreamExecute is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(*proto.QueryResult) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[query.Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", query.Sql)
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.execQuery)
		return nil
	}
	if execCase.reply.Result != nil {
		result := proto.QueryResult{Result: &mproto.QueryResult{}}
		result.Result.Fields = execCase.reply.Result.Fields
		if err := sendReply(&result); err != nil {
			return err
		}
		for _, row := range execCase.reply.Result.Rows {
			result := proto.QueryResult{Result: &mproto.QueryResult{}}
			result.Result.Rows = [][]sqltypes.Value{row}
			if err := sendReply(&result); err != nil {
				return err
			}
		}
	}
	if execCase.reply.Error != "" {
		return errors.New(execCase.reply.Error)
	}
	return nil
}

// StreamExecuteShard is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteShard(ctx context.Context, query *proto.QueryShard, sendReply func(*proto.QueryResult) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[query.Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", query.Sql)
	}
	if !reflect.DeepEqual(query, execCase.shardQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.shardQuery)
		return nil
	}
	if execCase.reply.Result != nil {
		result := proto.QueryResult{Result: &mproto.QueryResult{}}
		result.Result.Fields = execCase.reply.Result.Fields
		if err := sendReply(&result); err != nil {
			return err
		}
		for _, row := range execCase.reply.Result.Rows {
			result := proto.QueryResult{Result: &mproto.QueryResult{}}
			result.Result.Rows = [][]sqltypes.Value{row}
			if err := sendReply(&result); err != nil {
				return err
			}
		}
	}
	if execCase.reply.Error != "" {
		return errors.New(execCase.reply.Error)
	}
	return nil
}

// StreamExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[query.Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", query.Sql)
	}
	if !reflect.DeepEqual(query, execCase.keyRangeQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.keyRangeQuery)
		return nil
	}
	if execCase.reply.Result != nil {
		result := proto.QueryResult{Result: &mproto.QueryResult{}}
		result.Result.Fields = execCase.reply.Result.Fields
		if err := sendReply(&result); err != nil {
			return err
		}
		for _, row := range execCase.reply.Result.Rows {
			result := proto.QueryResult{Result: &mproto.QueryResult{}}
			result.Result.Rows = [][]sqltypes.Value{row}
			if err := sendReply(&result); err != nil {
				return err
			}
		}
	}
	if execCase.reply.Error != "" {
		return errors.New(execCase.reply.Error)
	}
	return nil
}

// StreamExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, sendReply func(*proto.QueryResult) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[query.Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", query.Sql)
	}
	if !reflect.DeepEqual(query, execCase.keyspaceIdQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.keyspaceIdQuery)
		return nil
	}
	if execCase.reply.Result != nil {
		result := proto.QueryResult{Result: &mproto.QueryResult{}}
		result.Result.Fields = execCase.reply.Result.Fields
		if err := sendReply(&result); err != nil {
			return err
		}
		for _, row := range execCase.reply.Result.Rows {
			result := proto.QueryResult{Result: &mproto.QueryResult{}}
			result.Result.Rows = [][]sqltypes.Value{row}
			if err := sendReply(&result); err != nil {
				return err
			}
		}
	}
	if execCase.reply.Error != "" {
		return errors.New(execCase.reply.Error)
	}
	return nil
}

// Begin is part of the VTGateService interface
func (f *fakeVTGateService) Begin(ctx context.Context, outSession *proto.Session) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	*outSession = *session1
	return nil
}

// Commit is part of the VTGateService interface
func (f *fakeVTGateService) Commit(ctx context.Context, inSession *proto.Session) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	if !reflect.DeepEqual(inSession, session2) {
		return errors.New("commit: session mismatch")
	}
	return nil
}

// Rollback is part of the VTGateService interface
func (f *fakeVTGateService) Rollback(ctx context.Context, inSession *proto.Session) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	if !reflect.DeepEqual(inSession, session2) {
		return errors.New("rollback: session mismatch")
	}
	return nil
}

// SplitQuery is part of the VTGateService interface
func (f *fakeVTGateService) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	if !reflect.DeepEqual(req, splitQueryRequest) {
		f.t.Errorf("SplitQuery has wrong input: got %#v wanted %#v", req, splitQueryRequest)
	}
	*reply = *splitQueryResult
	return nil
}

// CreateFakeServer returns the fake server for the tests
func CreateFakeServer(t *testing.T) vtgateservice.VTGateService {
	return &fakeVTGateService{
		t:      t,
		panics: false,
	}
}

// HandlePanic is part of the VTGateService interface
func (f *fakeVTGateService) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}

// TestSuite runs all the tests
func TestSuite(t *testing.T, impl vtgateconn.Impl, fakeServer vtgateservice.VTGateService) {
	vtgateconn.RegisterDialer("test", func(ctx context.Context, address string, timeout time.Duration) (vtgateconn.Impl, error) {
		return impl, nil
	})
	conn, _ := vtgateconn.DialProtocol(context.Background(), "test", "", 0)

	testExecute(t, conn)
	testExecuteShard(t, conn)
	testExecuteKeyspaceIds(t, conn)
	testExecuteKeyRanges(t, conn)
	testExecuteEntityIds(t, conn)
	testExecuteBatchShard(t, conn)
	testExecuteBatchKeyspaceIds(t, conn)
	testStreamExecute(t, conn)
	testStreamExecuteShard(t, conn)
	testStreamExecuteKeyRanges(t, conn)
	testStreamExecuteKeyspaceIds(t, conn)
	testTxPass(t, conn)
	testTxPassNotInTransaction(t, conn)
	testTxFail(t, conn)
	testSplitQuery(t, conn)

	// force a panic at every call, then test that works
	fakeServer.(*fakeVTGateService).panics = true
	testExecutePanic(t, conn)
	testExecuteShardPanic(t, conn)
	testExecuteKeyspaceIdsPanic(t, conn)
	testExecuteKeyRangesPanic(t, conn)
	testExecuteEntityIdsPanic(t, conn)
	testExecuteBatchShardPanic(t, conn)
	testExecuteBatchKeyspaceIdsPanic(t, conn)
	testStreamExecutePanic(t, conn)
	testStreamExecuteShardPanic(t, conn)
	testStreamExecuteKeyRangesPanic(t, conn)
	testStreamExecuteKeyspaceIdsPanic(t, conn)
	testBeginPanic(t, conn)
	testSplitQueryPanic(t, conn)
}

func expectPanic(t *testing.T, err error) {
	expected1 := "test forced panic"
	expected2 := "uncaught panic"
	if err == nil || !strings.Contains(err.Error(), expected1) || !strings.Contains(err.Error(), expected2) {
		t.Fatalf("Expected a panic error with '%v' or '%v' but got: %v", expected1, expected2, err)
	}
}

func testExecute(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	qr, err := conn.Execute(ctx, execCase.execQuery.Sql, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.reply.Result)
	}

	_, err = conn.Execute(ctx, "none", nil, "")
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	_, err = conn.Execute(ctx, "errorRequst", nil, "")
	want = "app error"
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecutePanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	_, err := conn.Execute(ctx, execCase.execQuery.Sql, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	expectPanic(t, err)
}

func testExecuteShard(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	qr, err := conn.ExecuteShard(ctx, execCase.shardQuery.Sql, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.reply.Result)
	}

	_, err = conn.ExecuteShard(ctx, "none", "", []string{}, nil, "")
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	_, err = conn.ExecuteShard(ctx, "errorRequst", "", []string{}, nil, "")
	want = "app error"
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteShardPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	_, err := conn.ExecuteShard(ctx, execCase.execQuery.Sql, "ks", []string{"1", "2"}, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	expectPanic(t, err)
}

func testExecuteKeyspaceIds(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	qr, err := conn.ExecuteKeyspaceIds(ctx, execCase.keyspaceIdQuery.Sql, execCase.keyspaceIdQuery.Keyspace, execCase.keyspaceIdQuery.KeyspaceIds, execCase.keyspaceIdQuery.BindVariables, execCase.keyspaceIdQuery.TabletType)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.reply.Result)
	}

	_, err = conn.ExecuteKeyspaceIds(ctx, "none", "", []key.KeyspaceId{}, nil, "")
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	_, err = conn.ExecuteKeyspaceIds(ctx, execCase.keyspaceIdQuery.Sql, execCase.keyspaceIdQuery.Keyspace, execCase.keyspaceIdQuery.KeyspaceIds, execCase.keyspaceIdQuery.BindVariables, execCase.keyspaceIdQuery.TabletType)
	want = "app error"
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteKeyspaceIdsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	_, err := conn.ExecuteKeyspaceIds(ctx, execCase.keyspaceIdQuery.Sql, execCase.keyspaceIdQuery.Keyspace, execCase.keyspaceIdQuery.KeyspaceIds, execCase.keyspaceIdQuery.BindVariables, execCase.keyspaceIdQuery.TabletType)
	expectPanic(t, err)
}

func testExecuteKeyRanges(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	qr, err := conn.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.Sql, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.reply.Result)
	}

	_, err = conn.ExecuteKeyRanges(ctx, "none", "", []key.KeyRange{}, nil, "")
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	_, err = conn.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.Sql, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	want = "app error"
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteKeyRangesPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	_, err := conn.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.Sql, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	expectPanic(t, err)
}

func testExecuteEntityIds(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	qr, err := conn.ExecuteEntityIds(ctx, execCase.entityIdsQuery.Sql, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.reply.Result)
	}

	_, err = conn.ExecuteEntityIds(ctx, "none", "", "", []proto.EntityId{}, nil, "")
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	_, err = conn.ExecuteEntityIds(ctx, execCase.entityIdsQuery.Sql, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType)
	want = "app error"
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteEntityIdsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	_, err := conn.ExecuteEntityIds(ctx, execCase.entityIdsQuery.Sql, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType)
	expectPanic(t, err)
}

func testExecuteBatchShard(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	ql, err := conn.ExecuteBatchShard(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.Keyspace, execCase.batchQueryShard.Shards, execCase.batchQueryShard.TabletType)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(&ql[0], execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", ql, execCase.reply.Result)
	}

	_, err = conn.ExecuteBatchShard(ctx, []tproto.BoundQuery{tproto.BoundQuery{Sql: "none"}}, "", []string{}, "")
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	_, err = conn.ExecuteBatchShard(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.Keyspace, execCase.batchQueryShard.Shards, execCase.batchQueryShard.TabletType)
	want = "app error"
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteBatchShardPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	_, err := conn.ExecuteBatchShard(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.Keyspace, execCase.batchQueryShard.Shards, execCase.batchQueryShard.TabletType)
	expectPanic(t, err)
}

func testExecuteBatchKeyspaceIds(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	ql, err := conn.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIdBatchQuery.Queries, execCase.keyspaceIdBatchQuery.Keyspace, execCase.keyspaceIdBatchQuery.KeyspaceIds, execCase.keyspaceIdBatchQuery.TabletType)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(&ql[0], execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", ql, execCase.reply.Result)
	}

	_, err = conn.ExecuteBatchKeyspaceIds(ctx, []tproto.BoundQuery{tproto.BoundQuery{Sql: "none"}}, "", []key.KeyspaceId{}, "")
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	_, err = conn.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIdBatchQuery.Queries, execCase.keyspaceIdBatchQuery.Keyspace, execCase.keyspaceIdBatchQuery.KeyspaceIds, execCase.keyspaceIdBatchQuery.TabletType)
	want = "app error"
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteBatchKeyspaceIdsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	_, err := conn.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIdBatchQuery.Queries, execCase.keyspaceIdBatchQuery.Keyspace, execCase.keyspaceIdBatchQuery.KeyspaceIds, execCase.keyspaceIdBatchQuery.TabletType)
	expectPanic(t, err)
}

func testStreamExecute(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	packets, errFunc := conn.StreamExecute(ctx, execCase.execQuery.Sql, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	var qr mproto.QueryResult
	for packet := range packets {
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.reply.Result
	wantResult.RowsAffected = 0
	wantResult.InsertId = 0
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, wantResult)
	}
	err := errFunc()
	if err != nil {
		t.Error(err)
	}

	packets, errFunc = conn.StreamExecute(ctx, "none", nil, "")
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	packets, errFunc = conn.StreamExecute(ctx, "errorRequst", nil, "")
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want = "app error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testStreamExecutePanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	packets, errFunc := conn.StreamExecute(ctx, execCase.execQuery.Sql, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if _, ok := <-packets; ok {
		t.Fatalf("Received packets instead of panic?")
	}
	err := errFunc()
	expectPanic(t, err)
}

func testStreamExecuteShard(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	packets, errFunc := conn.StreamExecuteShard(ctx, execCase.shardQuery.Sql, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	var qr mproto.QueryResult
	for packet := range packets {
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.reply.Result
	wantResult.RowsAffected = 0
	wantResult.InsertId = 0
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, wantResult)
	}
	err := errFunc()
	if err != nil {
		t.Error(err)
	}

	packets, errFunc = conn.StreamExecuteShard(ctx, "none", "", []string{}, nil, "")
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	packets, errFunc = conn.StreamExecuteShard(ctx, execCase.shardQuery.Sql, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want = "app error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testStreamExecuteShardPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	packets, errFunc := conn.StreamExecuteShard(ctx, execCase.shardQuery.Sql, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if _, ok := <-packets; ok {
		t.Fatalf("Received packets instead of panic?")
	}
	err := errFunc()
	expectPanic(t, err)
}

func testStreamExecuteKeyRanges(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	packets, errFunc := conn.StreamExecuteKeyRanges(ctx, execCase.keyRangeQuery.Sql, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	var qr mproto.QueryResult
	for packet := range packets {
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.reply.Result
	wantResult.RowsAffected = 0
	wantResult.InsertId = 0
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, wantResult)
	}
	err := errFunc()
	if err != nil {
		t.Error(err)
	}

	packets, errFunc = conn.StreamExecuteKeyRanges(ctx, "none", "", []key.KeyRange{}, nil, "")
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	packets, errFunc = conn.StreamExecuteKeyRanges(ctx, execCase.keyRangeQuery.Sql, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want = "app error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testStreamExecuteKeyRangesPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	packets, errFunc := conn.StreamExecuteKeyRanges(ctx, execCase.keyRangeQuery.Sql, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	if _, ok := <-packets; ok {
		t.Fatalf("Received packets instead of panic?")
	}
	err := errFunc()
	expectPanic(t, err)
}

func testStreamExecuteKeyspaceIds(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	packets, errFunc := conn.StreamExecuteKeyspaceIds(ctx, execCase.keyspaceIdQuery.Sql, execCase.keyspaceIdQuery.Keyspace, execCase.keyspaceIdQuery.KeyspaceIds, execCase.keyspaceIdQuery.BindVariables, execCase.keyspaceIdQuery.TabletType)
	var qr mproto.QueryResult
	for packet := range packets {
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.reply.Result
	wantResult.RowsAffected = 0
	wantResult.InsertId = 0
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, wantResult)
	}
	err := errFunc()
	if err != nil {
		t.Error(err)
	}

	packets, errFunc = conn.StreamExecuteKeyspaceIds(ctx, "none", "", []key.KeyspaceId{}, nil, "")
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	packets, errFunc = conn.StreamExecuteKeyspaceIds(ctx, execCase.keyspaceIdQuery.Sql, execCase.keyspaceIdQuery.Keyspace, execCase.keyspaceIdQuery.KeyspaceIds, execCase.keyspaceIdQuery.BindVariables, execCase.keyspaceIdQuery.TabletType)
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want = "app error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testStreamExecuteKeyspaceIdsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["request1"]
	packets, errFunc := conn.StreamExecuteKeyspaceIds(ctx, execCase.keyspaceIdQuery.Sql, execCase.keyspaceIdQuery.Keyspace, execCase.keyspaceIdQuery.KeyspaceIds, execCase.keyspaceIdQuery.BindVariables, execCase.keyspaceIdQuery.TabletType)
	if _, ok := <-packets; ok {
		t.Fatalf("Received packets instead of panic?")
	}
	err := errFunc()
	expectPanic(t, err)
}

func testTxPass(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["txRequest"]

	// Execute
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.Execute(ctx, execCase.execQuery.Sql, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, false)
	if err != nil {
		t.Error(err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteShard
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteShard(ctx, execCase.shardQuery.Sql, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType, false)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteKeyspaceIds
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteKeyspaceIds(ctx, execCase.keyspaceIdQuery.Sql, execCase.keyspaceIdQuery.Keyspace, execCase.keyspaceIdQuery.KeyspaceIds, execCase.keyspaceIdQuery.BindVariables, execCase.keyspaceIdQuery.TabletType, false)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteKeyRanges
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.Sql, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, false)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteEntityIds
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteEntityIds(ctx, execCase.entityIdsQuery.Sql, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType, false)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteBatchShard
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteBatchShard(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.Keyspace, execCase.batchQueryShard.Shards, execCase.batchQueryShard.TabletType, false)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteBatchKeyspaceIds
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIdBatchQuery.Queries, execCase.keyspaceIdBatchQuery.Keyspace, execCase.keyspaceIdBatchQuery.KeyspaceIds, execCase.keyspaceIdBatchQuery.TabletType, false)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}
}

func testTxPassNotInTransaction(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	execCase := execMap["txRequestNIT"]

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.Execute(ctx, execCase.execQuery.Sql, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteShard(ctx, execCase.shardQuery.Sql, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteKeyspaceIds(ctx, execCase.keyspaceIdQuery.Sql, execCase.keyspaceIdQuery.Keyspace, execCase.keyspaceIdQuery.KeyspaceIds, execCase.keyspaceIdQuery.BindVariables, execCase.keyspaceIdQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.Sql, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteEntityIds(ctx, execCase.entityIdsQuery.Sql, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteBatchShard(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.Keyspace, execCase.batchQueryShard.Shards, execCase.batchQueryShard.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIdBatchQuery.Queries, execCase.keyspaceIdBatchQuery.Keyspace, execCase.keyspaceIdBatchQuery.KeyspaceIds, execCase.keyspaceIdBatchQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	// no rollback necessary
}

func testBeginPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	_, err := conn.Begin(ctx)
	expectPanic(t, err)
}

func testTxFail(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	err = tx.Commit(ctx)
	want := "commit: session mismatch"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Commit: %v, want %v", err, want)
	}

	_, err = tx.Execute(ctx, "", nil, "", false)
	want = "execute: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("Execute: %v, want %v", err, want)
	}

	_, err = tx.ExecuteShard(ctx, "", "", nil, nil, "", false)
	want = "executeShard: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShard: %v, want %v", err, want)
	}

	_, err = tx.ExecuteKeyspaceIds(ctx, "", "", nil, nil, "", false)
	want = "executeKeyspaceIds: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShard: %v, want %v", err, want)
	}

	_, err = tx.ExecuteKeyRanges(ctx, "", "", nil, nil, "", false)
	want = "executeKeyRanges: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShard: %v, want %v", err, want)
	}

	_, err = tx.ExecuteEntityIds(ctx, "", "", "", nil, nil, "", false)
	want = "executeEntityIds: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShard: %v, want %v", err, want)
	}

	_, err = tx.ExecuteBatchShard(ctx, nil, "", nil, "", false)
	want = "executeBatchShard: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShard: %v, want %v", err, want)
	}

	_, err = tx.ExecuteBatchKeyspaceIds(ctx, nil, "", nil, "", false)
	want = "executeBatchKeyspaceIds: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShard: %v, want %v", err, want)
	}

	err = tx.Commit(ctx)
	want = "commit: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("Commit: %v, want %v", err, want)
	}

	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	want = "rollback: session mismatch"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Rollback: %v, want %v", err, want)
	}
}

func testSplitQuery(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	qsl, err := conn.SplitQuery(ctx, splitQueryRequest.Keyspace, splitQueryRequest.Query, splitQueryRequest.SplitCount)
	if err != nil {
		t.Fatalf("SplitQuery failed: %v", err)
	}
	if !reflect.DeepEqual(qsl, splitQueryResult.Splits) {
		t.Errorf("SplitQuery returned worng result: got %v wanted %v", qsl, splitQueryResult.Splits)
	}
}

func testSplitQueryPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()
	_, err := conn.SplitQuery(ctx, splitQueryRequest.Keyspace, splitQueryRequest.Query, splitQueryRequest.SplitCount)
	expectPanic(t, err)
}

var execMap = map[string]struct {
	execQuery            *proto.Query
	shardQuery           *proto.QueryShard
	keyspaceIdQuery      *proto.KeyspaceIdQuery
	keyRangeQuery        *proto.KeyRangeQuery
	entityIdsQuery       *proto.EntityIdsQuery
	batchQueryShard      *proto.BatchQueryShard
	keyspaceIdBatchQuery *proto.KeyspaceIdBatchQuery
	reply                *proto.QueryResult
	err                  error
}{
	"request1": {
		execQuery: &proto.Query{
			Sql: "request1",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		shardQuery: &proto.QueryShard{
			Sql: "request1",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:   "ks",
			Shards:     []string{"1", "2"},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		keyspaceIdQuery: &proto.KeyspaceIdQuery{
			Sql: "request1",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyspaceIds: []key.KeyspaceId{
				key.KeyspaceId("a"),
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		keyRangeQuery: &proto.KeyRangeQuery{
			Sql: "request1",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyRanges: []key.KeyRange{
				key.KeyRange{
					Start: key.KeyspaceId("s"),
					End:   key.KeyspaceId("e"),
				},
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		entityIdsQuery: &proto.EntityIdsQuery{
			Sql: "request1",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:         "ks",
			EntityColumnName: "column",
			EntityKeyspaceIDs: []proto.EntityId{
				proto.EntityId{
					ExternalID: []byte{105, 100, 49},
					KeyspaceID: key.KeyspaceId("k"),
				},
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		batchQueryShard: &proto.BatchQueryShard{
			Queries: []tproto.BoundQuery{
				tproto.BoundQuery{
					Sql: "request1",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
				},
			},
			Keyspace:   "ks",
			Shards:     []string{"-80", "80-"},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		keyspaceIdBatchQuery: &proto.KeyspaceIdBatchQuery{
			Queries: []tproto.BoundQuery{
				tproto.BoundQuery{
					Sql: "request1",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
				},
			},
			Keyspace: "ks",
			KeyspaceIds: []key.KeyspaceId{
				key.KeyspaceId("ki1"),
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		reply: &proto.QueryResult{
			Result:  &result1,
			Session: nil,
			Error:   "",
		},
	},
	"errorRequst": {
		execQuery: &proto.Query{
			Sql:           "errorRequst",
			BindVariables: map[string]interface{}{},
			TabletType:    "",
			Session:       nil,
		},
		shardQuery: &proto.QueryShard{
			Sql:           "errorRequst",
			BindVariables: map[string]interface{}{},
			TabletType:    "",
			Keyspace:      "",
			Shards:        []string{},
			Session:       nil,
		},
		keyspaceIdQuery: &proto.KeyspaceIdQuery{
			Sql: "errorRequst",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyspaceIds: []key.KeyspaceId{
				key.KeyspaceId("a"),
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		keyRangeQuery: &proto.KeyRangeQuery{
			Sql: "errorRequst",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyRanges: []key.KeyRange{
				key.KeyRange{
					Start: key.KeyspaceId("s"),
					End:   key.KeyspaceId("e"),
				},
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		entityIdsQuery: &proto.EntityIdsQuery{
			Sql: "errorRequst",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:         "ks",
			EntityColumnName: "column",
			EntityKeyspaceIDs: []proto.EntityId{
				proto.EntityId{
					ExternalID: []byte{105, 100, 49},
					KeyspaceID: key.KeyspaceId("k"),
				},
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		batchQueryShard: &proto.BatchQueryShard{
			Queries: []tproto.BoundQuery{
				tproto.BoundQuery{
					Sql: "errorRequst",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
				},
			},
			Keyspace:   "ks",
			Shards:     []string{"-80", "80-"},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		keyspaceIdBatchQuery: &proto.KeyspaceIdBatchQuery{
			Queries: []tproto.BoundQuery{
				tproto.BoundQuery{
					Sql: "errorRequst",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
				},
			},
			Keyspace: "ks",
			KeyspaceIds: []key.KeyspaceId{
				key.KeyspaceId("ki1"),
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    nil,
		},
		reply: &proto.QueryResult{
			Result:  nil,
			Session: nil,
			Error:   "app error",
		},
	},
	"txRequest": {
		execQuery: &proto.Query{
			Sql:           "txRequest",
			BindVariables: map[string]interface{}{},
			TabletType:    "",
			Session:       session1,
		},
		shardQuery: &proto.QueryShard{
			Sql:           "txRequest",
			BindVariables: map[string]interface{}{},
			TabletType:    "",
			Keyspace:      "",
			Shards:        []string{},
			Session:       session1,
		},
		keyspaceIdQuery: &proto.KeyspaceIdQuery{
			Sql: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyspaceIds: []key.KeyspaceId{
				key.KeyspaceId("a"),
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    session1,
		},
		keyRangeQuery: &proto.KeyRangeQuery{
			Sql: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyRanges: []key.KeyRange{
				key.KeyRange{
					Start: key.KeyspaceId("s"),
					End:   key.KeyspaceId("e"),
				},
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    session1,
		},
		entityIdsQuery: &proto.EntityIdsQuery{
			Sql: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:         "ks",
			EntityColumnName: "column",
			EntityKeyspaceIDs: []proto.EntityId{
				proto.EntityId{
					ExternalID: []byte{105, 100, 49},
					KeyspaceID: key.KeyspaceId("k"),
				},
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    session1,
		},
		batchQueryShard: &proto.BatchQueryShard{
			Queries: []tproto.BoundQuery{
				tproto.BoundQuery{
					Sql: "txRequest",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
				},
			},
			Keyspace:   "ks",
			Shards:     []string{"-80", "80-"},
			TabletType: topo.TYPE_RDONLY,
			Session:    session1,
		},
		keyspaceIdBatchQuery: &proto.KeyspaceIdBatchQuery{
			Queries: []tproto.BoundQuery{
				tproto.BoundQuery{
					Sql: "txRequest",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
				},
			},
			Keyspace: "ks",
			KeyspaceIds: []key.KeyspaceId{
				key.KeyspaceId("ki1"),
			},
			TabletType: topo.TYPE_RDONLY,
			Session:    session1,
		},
		reply: &proto.QueryResult{
			Result:  nil,
			Session: session2,
			Error:   "",
		},
	},
	"txRequestNIT": {
		execQuery: &proto.Query{
			Sql:              "txRequestNIT",
			BindVariables:    map[string]interface{}{},
			TabletType:       "",
			Session:          session1,
			NotInTransaction: true,
		},
		shardQuery: &proto.QueryShard{
			Sql:              "txRequestNIT",
			BindVariables:    map[string]interface{}{},
			TabletType:       "",
			Keyspace:         "",
			Shards:           []string{},
			Session:          session1,
			NotInTransaction: true,
		},
		keyspaceIdQuery: &proto.KeyspaceIdQuery{
			Sql: "txRequestNIT",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyspaceIds: []key.KeyspaceId{
				key.KeyspaceId("a"),
			},
			TabletType:       topo.TYPE_RDONLY,
			Session:          session1,
			NotInTransaction: true,
		},
		keyRangeQuery: &proto.KeyRangeQuery{
			Sql: "txRequestNIT",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyRanges: []key.KeyRange{
				key.KeyRange{
					Start: key.KeyspaceId("s"),
					End:   key.KeyspaceId("e"),
				},
			},
			TabletType:       topo.TYPE_RDONLY,
			Session:          session1,
			NotInTransaction: true,
		},
		entityIdsQuery: &proto.EntityIdsQuery{
			Sql: "txRequestNIT",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:         "ks",
			EntityColumnName: "column",
			EntityKeyspaceIDs: []proto.EntityId{
				proto.EntityId{
					ExternalID: []byte{105, 100, 49},
					KeyspaceID: key.KeyspaceId("k"),
				},
			},
			TabletType:       topo.TYPE_RDONLY,
			Session:          session1,
			NotInTransaction: true,
		},
		batchQueryShard: &proto.BatchQueryShard{
			Queries: []tproto.BoundQuery{
				tproto.BoundQuery{
					Sql: "txRequestNIT",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
				},
			},
			Keyspace:         "ks",
			Shards:           []string{"-80", "80-"},
			TabletType:       topo.TYPE_RDONLY,
			Session:          session1,
			NotInTransaction: true,
		},
		keyspaceIdBatchQuery: &proto.KeyspaceIdBatchQuery{
			Queries: []tproto.BoundQuery{
				tproto.BoundQuery{
					Sql: "txRequestNIT",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
				},
			},
			Keyspace: "ks",
			KeyspaceIds: []key.KeyspaceId{
				key.KeyspaceId("ki1"),
			},
			TabletType:       topo.TYPE_RDONLY,
			Session:          session1,
			NotInTransaction: true,
		},
		reply: &proto.QueryResult{
			Result:  nil,
			Session: session1,
			Error:   "",
		},
	},
}

var result1 = mproto.QueryResult{
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

var session1 = &proto.Session{
	InTransaction: true,
	ShardSessions: []*proto.ShardSession{},
}

var session2 = &proto.Session{
	InTransaction: true,
	ShardSessions: []*proto.ShardSession{
		&proto.ShardSession{
			Keyspace:      "ks",
			Shard:         "1",
			TabletType:    topo.TYPE_MASTER,
			TransactionId: 1,
		},
	},
}

var splitQueryRequest = &proto.SplitQueryRequest{
	Keyspace: "ks",
	Query: tproto.BoundQuery{
		Sql: "in for SplitQuery",
		BindVariables: map[string]interface{}{
			"bind1": int64(43),
		},
	},
	SplitCount: 13,
}

var splitQueryResult = &proto.SplitQueryResult{
	Splits: []proto.SplitQueryPart{
		proto.SplitQueryPart{
			Query: &proto.KeyRangeQuery{
				Sql: "out for SplitQuery",
				BindVariables: map[string]interface{}{
					"bind1": int64(1114444),
				},
				Keyspace:   "ksout",
				KeyRanges:  []key.KeyRange{},
				TabletType: topo.TYPE_RDONLY,
			},
			Size: 12344,
		},
	},
}
