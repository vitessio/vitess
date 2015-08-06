// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package tabletconntest provides the test methods to make sure a
// tabletconn/queryservice pair over RPC works correctly.
package tabletconntest

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/query"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
	pbv "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// FakeQueryService has the server side of this fake
type FakeQueryService struct {
	t                        *testing.T
	panics                   bool
	streamExecutePanicsEarly bool
	panicWait                chan struct{}

	// if set, we will also check Target, ImmediateCallerId and EffectiveCallerId
	checkExtraFields bool
}

// HandlePanic is part of the queryservice.QueryService interface
func (f *FakeQueryService) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("caught test panic: %v", x)
	}
}

// testTarget is the target we use for this test
var testTarget = &pb.Target{
	Keyspace:   "test_keyspace",
	Shard:      "test_shard",
	TabletType: pbt.TabletType_REPLICA,
}

var testCallerID = &pbv.CallerID{
	Principal:    "test_principal",
	Component:    "test_component",
	Subcomponent: "test_subcomponent",
}

var testVTGateCallerID = &pb.VTGateCallerID{
	Username: "test_username",
}

const testAsTransaction bool = true

const testSessionID int64 = 5678

func (f *FakeQueryService) checkTargetCallerID(ctx context.Context, name string, target *pb.Target) {
	if !reflect.DeepEqual(target, testTarget) {
		f.t.Errorf("invalid Target for %v: got %#v expected %#v", name, target, testTarget)
	}
	ef := callerid.EffectiveCallerIDFromContext(ctx)
	if ef == nil {
		f.t.Errorf("no effective caller id for %v", name)
	} else {
		if !reflect.DeepEqual(ef, testCallerID) {
			f.t.Errorf("invalid effective caller id for %v: got %v expected %v", name, ef, testCallerID)
		}
	}
	im := callerid.ImmediateCallerIDFromContext(ctx)
	if im == nil {
		f.t.Errorf("no immediate caller id for %v", name)
	} else {
		if !reflect.DeepEqual(im, testVTGateCallerID) {
			f.t.Errorf("invalid immediate caller id for %v: got %v expected %v", name, im, testVTGateCallerID)
		}
	}
}

// GetSessionId is part of the queryservice.QueryService interface
func (f *FakeQueryService) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	if sessionParams.Keyspace != testTarget.Keyspace {
		f.t.Errorf("invalid keyspace: got %v expected %v", sessionParams.Keyspace, testTarget.Keyspace)
	}
	if sessionParams.Shard != testTarget.Shard {
		f.t.Errorf("invalid shard: got %v expected %v", sessionParams.Shard, testTarget.Shard)
	}
	sessionInfo.SessionId = testSessionID
	return nil
}

// Begin is part of the queryservice.QueryService interface
func (f *FakeQueryService) Begin(ctx context.Context, target *pb.Target, session *proto.Session, txInfo *proto.TransactionInfo) error {
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if f.checkExtraFields {
		f.checkTargetCallerID(ctx, "Begin", target)
	} else {
		if session.SessionId != testSessionID {
			f.t.Errorf("Begin: invalid SessionId: got %v expected %v", session.SessionId, testSessionID)
		}
	}
	if session.TransactionId != 0 {
		f.t.Errorf("Begin: invalid TransactionId: got %v expected 0", session.TransactionId)
	}
	txInfo.TransactionId = beginTransactionID
	return nil
}

const beginTransactionID int64 = 9990

func testBegin(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testBegin")
	ctx := context.Background()
	transactionID, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if transactionID != beginTransactionID {
		t.Errorf("Unexpected result from Begin: got %v wanted %v", transactionID, beginTransactionID)
	}
}

func testBeginPanics(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testBeginPanics")
	ctx := context.Background()
	if _, err := conn.Begin(ctx); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

func testBegin2(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testBegin2")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	transactionID, err := conn.Begin2(ctx)
	if err != nil {
		t.Fatalf("Begin2 failed: %v", err)
	}
	if transactionID != beginTransactionID {
		t.Errorf("Unexpected result from Begin2: got %v wanted %v", transactionID, beginTransactionID)
	}
}

func testBegin2Panics(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testBegin2Panics")
	ctx := context.Background()
	if _, err := conn.Begin2(ctx); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// Commit is part of the queryservice.QueryService interface
func (f *FakeQueryService) Commit(ctx context.Context, target *pb.Target, session *proto.Session) error {
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if f.checkExtraFields {
		f.checkTargetCallerID(ctx, "Commit", target)
	} else {
		if session.SessionId != testSessionID {
			f.t.Errorf("Commit: invalid SessionId: got %v expected %v", session.SessionId, testSessionID)
		}
	}
	if session.TransactionId != commitTransactionID {
		f.t.Errorf("Commit: invalid TransactionId: got %v expected %v", session.TransactionId, commitTransactionID)
	}
	return nil
}

const commitTransactionID int64 = 999044

func testCommit(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testCommit")
	ctx := context.Background()
	err := conn.Commit(ctx, commitTransactionID)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func testCommitPanics(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testCommitPanics")
	ctx := context.Background()
	if err := conn.Commit(ctx, commitTransactionID); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

func testCommit2(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testCommit2")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	err := conn.Commit2(ctx, commitTransactionID)
	if err != nil {
		t.Fatalf("Commit2 failed: %v", err)
	}
}

func testCommit2Panics(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testCommit2Panics")
	ctx := context.Background()
	if err := conn.Commit2(ctx, commitTransactionID); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// Rollback is part of the queryservice.QueryService interface
func (f *FakeQueryService) Rollback(ctx context.Context, target *pb.Target, session *proto.Session) error {
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if f.checkExtraFields {
		f.checkTargetCallerID(ctx, "Rollback", target)
	} else {
		if session.SessionId != testSessionID {
			f.t.Errorf("Rollback: invalid SessionId: got %v expected %v", session.SessionId, testSessionID)
		}
	}
	if session.TransactionId != rollbackTransactionID {
		f.t.Errorf("Rollback: invalid TransactionId: got %v expected %v", session.TransactionId, rollbackTransactionID)
	}
	return nil
}

const rollbackTransactionID int64 = 999044

func testRollback(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testRollback")
	ctx := context.Background()
	err := conn.Rollback(ctx, rollbackTransactionID)
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
}

func testRollbackPanics(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testRollbackPanics")
	ctx := context.Background()
	if err := conn.Rollback(ctx, rollbackTransactionID); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

func testRollback2(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testRollback2")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	err := conn.Rollback2(ctx, rollbackTransactionID)
	if err != nil {
		t.Fatalf("Rollback2 failed: %v", err)
	}
}

func testRollback2Panics(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testRollback2Panics")
	ctx := context.Background()
	if err := conn.Rollback2(ctx, rollbackTransactionID); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// Execute is part of the queryservice.QueryService interface
func (f *FakeQueryService) Execute(ctx context.Context, target *pb.Target, query *proto.Query, reply *mproto.QueryResult) error {
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if query.Sql != executeQuery {
		f.t.Errorf("invalid Execute.Query.Sql: got %v expected %v", query.Sql, executeQuery)
	}
	if !reflect.DeepEqual(query.BindVariables, executeBindVars) {
		f.t.Errorf("invalid Execute.Query.BindVariables: got %v expected %v", query.BindVariables, executeBindVars)
	}
	if f.checkExtraFields {
		f.checkTargetCallerID(ctx, "Execute", target)
	} else {
		if query.SessionId != testSessionID {
			f.t.Errorf("invalid Execute.Query.SessionId: got %v expected %v", query.SessionId, testSessionID)
		}
	}
	if query.TransactionId != executeTransactionID {
		f.t.Errorf("invalid Execute.Query.TransactionId: got %v expected %v", query.TransactionId, executeTransactionID)
	}
	*reply = executeQueryResult
	return nil
}

const executeQuery = "executeQuery"

var executeBindVars = map[string]interface{}{
	"bind1": int64(1114444),
}

const executeTransactionID int64 = 678

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
	t.Log("testExecute")
	ctx := context.Background()
	qr, err := conn.Execute(ctx, executeQuery, executeBindVars, executeTransactionID)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if !reflect.DeepEqual(*qr, executeQueryResult) {
		t.Errorf("Unexpected result from Execute: got %v wanted %v", qr, executeQueryResult)
	}
}

func testExecute2(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testExecute2")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	qr, err := conn.Execute2(ctx, executeQuery, executeBindVars, executeTransactionID)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if !reflect.DeepEqual(*qr, executeQueryResult) {
		t.Errorf("Unexpected result from Execute: got %v wanted %v", qr, executeQueryResult)
	}
}

func testExecutePanics(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testExecutePanics")
	ctx := context.Background()
	if _, err := conn.Execute(ctx, executeQuery, executeBindVars, executeTransactionID); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

func testExecute2Panics(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testExecute2Panics")
	ctx := context.Background()
	if _, err := conn.Execute2(ctx, executeQuery, executeBindVars, executeTransactionID); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// StreamExecute is part of the queryservice.QueryService interface
func (f *FakeQueryService) StreamExecute(ctx context.Context, target *pb.Target, query *proto.Query, sendReply func(*mproto.QueryResult) error) error {
	if f.panics && f.streamExecutePanicsEarly {
		panic(fmt.Errorf("test-triggered panic early"))
	}
	if query.Sql != streamExecuteQuery {
		f.t.Errorf("invalid StreamExecute.Query.Sql: got %v expected %v", query.Sql, streamExecuteQuery)
	}
	if !reflect.DeepEqual(query.BindVariables, streamExecuteBindVars) {
		f.t.Errorf("invalid StreamExecute.Query.BindVariables: got %v expected %v", query.BindVariables, streamExecuteBindVars)
	}
	if f.checkExtraFields {
		f.checkTargetCallerID(ctx, "StreamExecute", target)
	} else {
		if query.SessionId != testSessionID {
			f.t.Errorf("invalid StreamExecute.Query.SessionId: got %v expected %v", query.SessionId, testSessionID)
		}
	}
	if err := sendReply(&streamExecuteQueryResult1); err != nil {
		f.t.Errorf("sendReply1 failed: %v", err)
	}
	if f.panics && !f.streamExecutePanicsEarly {
		// wait until the client gets the response, then panics
		<-f.panicWait
		f.panicWait = make(chan struct{}) // for next test
		panic(fmt.Errorf("test-triggered panic late"))
	}
	if err := sendReply(&streamExecuteQueryResult2); err != nil {
		f.t.Errorf("sendReply2 failed: %v", err)
	}
	return nil
}

const streamExecuteQuery = "streamExecuteQuery"

var streamExecuteBindVars = map[string]interface{}{
	"bind1": int64(93848000),
}

const streamExecuteTransactionID int64 = 6789992

var streamExecuteQueryResult1 = mproto.QueryResult{
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
}

var streamExecuteQueryResult2 = mproto.QueryResult{
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

func testStreamExecute(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testStreamExecute")
	ctx := context.Background()
	stream, errFunc, err := conn.StreamExecute(ctx, streamExecuteQuery, streamExecuteBindVars, streamExecuteTransactionID)
	if err != nil {
		t.Fatalf("StreamExecute failed: %v", err)
	}
	qr, ok := <-stream
	if !ok {
		t.Fatalf("StreamExecute failed: cannot read result1")
	}
	if len(qr.Rows) == 0 {
		qr.Rows = nil
	}
	if !reflect.DeepEqual(*qr, streamExecuteQueryResult1) {
		t.Errorf("Unexpected result1 from StreamExecute: got %v wanted %v", qr, streamExecuteQueryResult1)
	}
	qr, ok = <-stream
	if !ok {
		t.Fatalf("StreamExecute failed: cannot read result2")
	}
	if len(qr.Fields) == 0 {
		qr.Fields = nil
	}
	if !reflect.DeepEqual(*qr, streamExecuteQueryResult2) {
		t.Errorf("Unexpected result2 from StreamExecute: got %v wanted %v", qr, streamExecuteQueryResult2)
	}
	qr, ok = <-stream
	if ok {
		t.Fatalf("StreamExecute channel wasn't closed")
	}
	if err := errFunc(); err != nil {
		t.Fatalf("StreamExecute errFunc failed: %v", err)
	}
}

func testStreamExecutePanics(t *testing.T, conn tabletconn.TabletConn, fake *FakeQueryService) {
	t.Log("testStreamExecutePanics")
	// early panic is before sending the Fields, that is returned
	// by the StreamExecute call itself, or as the first error
	// by ErrFunc
	ctx := context.Background()
	fake.streamExecutePanicsEarly = true
	stream, errFunc, err := conn.StreamExecute(ctx, streamExecuteQuery, streamExecuteBindVars, streamExecuteTransactionID)
	if err != nil {
		if !strings.Contains(err.Error(), "caught test panic") {
			t.Fatalf("unexpected panic error: %v", err)
		}
	} else {
		_, ok := <-stream
		if ok {
			t.Fatalf("StreamExecute early panic should not return anything")
		}
		err = errFunc()
		if err == nil || !strings.Contains(err.Error(), "caught test panic") {
			t.Fatalf("unexpected panic error: %v", err)
		}
	}

	// late panic is after sending Fields
	fake.streamExecutePanicsEarly = false
	stream, errFunc, err = conn.StreamExecute(ctx, streamExecuteQuery, streamExecuteBindVars, streamExecuteTransactionID)
	if err != nil {
		t.Fatalf("StreamExecute failed: %v", err)
	}
	qr, ok := <-stream
	if !ok {
		t.Fatalf("StreamExecute failed: cannot read result1")
	}
	if len(qr.Rows) == 0 {
		qr.Rows = nil
	}
	if !reflect.DeepEqual(*qr, streamExecuteQueryResult1) {
		t.Errorf("Unexpected result1 from StreamExecute: got %v wanted %v", qr, streamExecuteQueryResult1)
	}
	close(fake.panicWait)
	if _, ok := <-stream; ok {
		t.Fatalf("StreamExecute returned more results")
	}
	if err := errFunc(); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

func testStreamExecute2(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testStreamExecute2")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	stream, errFunc, err := conn.StreamExecute2(ctx, streamExecuteQuery, streamExecuteBindVars, streamExecuteTransactionID)
	if err != nil {
		t.Fatalf("StreamExecute2 failed: %v", err)
	}
	qr, ok := <-stream
	if !ok {
		t.Fatalf("StreamExecute2 failed: cannot read result1")
	}
	if len(qr.Rows) == 0 {
		qr.Rows = nil
	}
	if !reflect.DeepEqual(*qr, streamExecuteQueryResult1) {
		t.Errorf("Unexpected result1 from StreamExecute2: got %v wanted %v", qr, streamExecuteQueryResult1)
	}
	qr, ok = <-stream
	if !ok {
		t.Fatalf("StreamExecute2 failed: cannot read result2")
	}
	if len(qr.Fields) == 0 {
		qr.Fields = nil
	}
	if !reflect.DeepEqual(*qr, streamExecuteQueryResult2) {
		t.Errorf("Unexpected result2 from StreamExecute2: got %v wanted %v", qr, streamExecuteQueryResult2)
	}
	qr, ok = <-stream
	if ok {
		t.Fatalf("StreamExecute2 channel wasn't closed")
	}
	if err := errFunc(); err != nil {
		t.Fatalf("StreamExecute2 errFunc failed: %v", err)
	}
}

func testStreamExecute2Panics(t *testing.T, conn tabletconn.TabletConn, fake *FakeQueryService) {
	t.Log("testStreamExecute2Panics")
	// early panic is before sending the Fields, that is returned
	// by the StreamExecute2 call itself, or as the first error
	// by ErrFunc
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	fake.streamExecutePanicsEarly = true
	stream, errFunc, err := conn.StreamExecute2(ctx, streamExecuteQuery, streamExecuteBindVars, streamExecuteTransactionID)
	if err != nil {
		if !strings.Contains(err.Error(), "caught test panic") {
			t.Fatalf("unexpected panic error: %v", err)
		}
	} else {
		_, ok := <-stream
		if ok {
			t.Fatalf("StreamExecute early panic should not return anything")
		}
		err = errFunc()
		if err == nil || !strings.Contains(err.Error(), "caught test panic") {
			t.Fatalf("unexpected panic error: %v", err)
		}
	}

	// late panic is after sending Fields
	fake.streamExecutePanicsEarly = false
	stream, errFunc, err = conn.StreamExecute2(ctx, streamExecuteQuery, streamExecuteBindVars, streamExecuteTransactionID)
	if err != nil {
		t.Fatalf("StreamExecute2 failed: %v", err)
	}
	qr, ok := <-stream
	if !ok {
		t.Fatalf("StreamExecute2 failed: cannot read result1")
	}
	if len(qr.Rows) == 0 {
		qr.Rows = nil
	}
	if !reflect.DeepEqual(*qr, streamExecuteQueryResult1) {
		t.Errorf("Unexpected result1 from StreamExecute2: got %v wanted %v", qr, streamExecuteQueryResult1)
	}
	close(fake.panicWait)
	if _, ok := <-stream; ok {
		t.Fatalf("StreamExecute2 returned more results")
	}
	if err := errFunc(); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// ExecuteBatch is part of the queryservice.QueryService interface
func (f *FakeQueryService) ExecuteBatch(ctx context.Context, target *pb.Target, queryList *proto.QueryList, reply *proto.QueryResultList) error {
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if !reflect.DeepEqual(queryList.Queries, executeBatchQueries) {
		f.t.Errorf("invalid ExecuteBatch.QueryList.Queries: got %v expected %v", queryList.Queries, executeBatchQueries)
	}
	if f.checkExtraFields {
		f.checkTargetCallerID(ctx, "ExecuteBatch", target)
	} else {
		if queryList.SessionId != testSessionID {
			f.t.Errorf("invalid ExecuteBatch.QueryList.SessionId: got %v expected %v", queryList.SessionId, testSessionID)
		}
	}
	if queryList.AsTransaction != testAsTransaction {
		f.t.Errorf("invalid ExecuteBatch.QueryList.AsTransaction: got %v expected %v", queryList.AsTransaction, testAsTransaction)
	}
	if queryList.TransactionId != executeBatchTransactionID {
		f.t.Errorf("invalid ExecuteBatch.QueryList.TransactionId: got %v expected %v", queryList.TransactionId, executeBatchTransactionID)
	}
	*reply = executeBatchQueryResultList
	return nil
}

var executeBatchQueries = []proto.BoundQuery{
	proto.BoundQuery{
		Sql: "executeBatchQueries1",
		BindVariables: map[string]interface{}{
			"bind1": int64(43),
		},
	},
	proto.BoundQuery{
		Sql: "executeBatchQueries2",
		BindVariables: map[string]interface{}{
			"bind2": int64(72),
		},
	},
}

const executeBatchTransactionID int64 = 678

var executeBatchQueryResultList = proto.QueryResultList{
	List: []mproto.QueryResult{
		mproto.QueryResult{
			Fields: []mproto.Field{
				mproto.Field{
					Name: "field1",
					Type: 46,
				},
			},
			RowsAffected: 1232,
			InsertId:     712,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("row1 value1")),
				},
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("row2 value1")),
				},
			},
		},
		mproto.QueryResult{
			Fields: []mproto.Field{
				mproto.Field{
					Name: "field1",
					Type: 42,
				},
			},
			RowsAffected: 12333,
			InsertId:     74442,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("row1 value1")),
					sqltypes.MakeString([]byte("row1 value2")),
				},
			},
		},
	},
}

func testExecuteBatch(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testExecuteBatch")
	ctx := context.Background()
	qrl, err := conn.ExecuteBatch(ctx, executeBatchQueries, true, executeBatchTransactionID)
	if err != nil {
		t.Fatalf("ExecuteBatch failed: %v", err)
	}
	if !reflect.DeepEqual(*qrl, executeBatchQueryResultList) {
		t.Errorf("Unexpected result from Execute: got %v wanted %v", qrl, executeBatchQueryResultList)
	}
}

func testExecuteBatchPanics(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testExecuteBatchPanics")
	ctx := context.Background()
	if _, err := conn.ExecuteBatch(ctx, executeBatchQueries, true, executeBatchTransactionID); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

func testExecuteBatch2(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testExecuteBatch2")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	qrl, err := conn.ExecuteBatch2(ctx, executeBatchQueries, true, executeBatchTransactionID)
	if err != nil {
		t.Fatalf("ExecuteBatch failed: %v", err)
	}
	if !reflect.DeepEqual(*qrl, executeBatchQueryResultList) {
		t.Errorf("Unexpected result from ExecuteBatch: got %v wanted %v", qrl, executeBatchQueryResultList)
	}
}

func testExecuteBatch2Panics(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testExecuteBatch2Panics")
	ctx := context.Background()
	if _, err := conn.ExecuteBatch2(ctx, executeBatchQueries, true, executeBatchTransactionID); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// SplitQuery is part of the queryservice.QueryService interface
func (f *FakeQueryService) SplitQuery(ctx context.Context, target *pb.Target, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if f.checkExtraFields {
		f.checkTargetCallerID(ctx, "SplitQuery", target)
	}
	if !reflect.DeepEqual(req.Query, splitQueryBoundQuery) {
		f.t.Errorf("invalid SplitQuery.SplitQueryRequest.Query: got %v expected %v", req.Query, splitQueryBoundQuery)
	}
	if req.SplitColumn != splitQuerySplitColumn {
		f.t.Errorf("invalid SplitQuery.SplitQueryRequest.SplitColumn: got %v expected %v", req.SplitColumn, splitQuerySplitColumn)
	}
	if req.SplitCount != splitQuerySplitCount {
		f.t.Errorf("invalid SplitQuery.SplitQueryRequest.SplitCount: got %v expected %v", req.SplitCount, splitQuerySplitCount)
	}
	reply.Queries = splitQueryQuerySplitList
	return nil
}

var splitQueryBoundQuery = proto.BoundQuery{
	Sql: "splitQuery",
	BindVariables: map[string]interface{}{
		"bind1": int64(43),
	},
}

const splitQuerySplitColumn = "nice_column_to_split"
const splitQuerySplitCount = 372

var splitQueryQuerySplitList = []proto.QuerySplit{
	proto.QuerySplit{
		Query: proto.BoundQuery{
			Sql: "splitQuery",
			BindVariables: map[string]interface{}{
				"bind1":       int64(43),
				"keyspace_id": int64(3333),
			},
		},
		RowCount: 4456,
	},
}

func testSplitQuery(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testSplitQuery")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	qsl, err := conn.SplitQuery(ctx, splitQueryBoundQuery, splitQuerySplitColumn, splitQuerySplitCount)
	if err != nil {
		t.Fatalf("SplitQuery failed: %v", err)
	}
	if !reflect.DeepEqual(qsl, splitQueryQuerySplitList) {
		t.Errorf("Unexpected result from SplitQuery: got %v wanted %v", qsl, splitQueryQuerySplitList)
	}
}

func testSplitQueryPanics(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testSplitQueryPanics")
	ctx := context.Background()
	if _, err := conn.SplitQuery(ctx, splitQueryBoundQuery, splitQuerySplitColumn, splitQuerySplitCount); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// this test is a bit of a hack: we write something on the channel
// upon registration, and we also return an error, so the streaming query
// ends right there. Otherwise we have no real way to trigger a real
// communication error, that ends the streaming.
var testStreamHealthStreamHealthResponse = &pb.StreamHealthResponse{
	Target: &pb.Target{
		Keyspace:   "test_keyspace",
		Shard:      "test_shard",
		TabletType: pbt.TabletType_RDONLY,
	},
	TabletExternallyReparentedTimestamp: 1234589,
	RealtimeStats: &pb.RealtimeStats{
		HealthError:         "random error",
		SecondsBehindMaster: 234,
		CpuUsage:            1.0,
	},
}
var testStreamHealthError = "to trigger a server error"

// The server side should write the response to the stream, then wait for
// this channel to close, then return the error
var streamHealthSynchronization chan struct{}

// StreamHealthRegister is part of the queryservice.QueryService interface
func (f *FakeQueryService) StreamHealthRegister(c chan<- *pb.StreamHealthResponse) (int, error) {
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	c <- testStreamHealthStreamHealthResponse
	<-streamHealthSynchronization
	return 0, fmt.Errorf(testStreamHealthError)
}

// StreamHealthUnregister is part of the queryservice.QueryService interface
func (f *FakeQueryService) StreamHealthUnregister(int) error {
	return nil
}

func testStreamHealth(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testStreamHealth")
	streamHealthSynchronization = make(chan struct{})
	ctx := context.Background()

	c, errFunc, err := conn.StreamHealth(ctx)
	if err != nil {
		t.Fatalf("StreamHealth failed: %v", err)
	}
	// channel should have one response, then closed
	shr, ok := <-c
	if !ok {
		t.Fatalf("StreamHealth got no response")
	}

	if !reflect.DeepEqual(*shr, *testStreamHealthStreamHealthResponse) {
		t.Errorf("invalid StreamHealthResponse: got %v expected %v", *shr, *testStreamHealthStreamHealthResponse)
	}

	// close streamHealthSynchronization so server side knows we
	// got the response, and it can send the error
	close(streamHealthSynchronization)

	_, ok = <-c
	if ok {
		t.Fatalf("StreamHealth wasn't closed")
	}
	err = errFunc()
	if !strings.Contains(err.Error(), testStreamHealthError) {
		t.Fatalf("StreamHealth failed with the wrong error: %v", err)
	}
}

func testStreamHealthPanics(t *testing.T, conn tabletconn.TabletConn) {
	t.Log("testStreamHealthPanics")
	ctx := context.Background()

	c, errFunc, err := conn.StreamHealth(ctx)
	if err != nil {
		t.Fatalf("StreamHealth failed: %v", err)
	}
	// channel should have no response, just closed
	_, ok := <-c
	if ok {
		t.Fatalf("StreamHealth wasn't closed")
	}
	err = errFunc()
	if err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// CreateFakeServer returns the fake server for the tests
func CreateFakeServer(t *testing.T) *FakeQueryService {
	return &FakeQueryService{
		t:      t,
		panics: false,
		streamExecutePanicsEarly: false,
		panicWait:                make(chan struct{}),
	}
}

// TestSuite runs all the tests
func TestSuite(t *testing.T, protocol string, endPoint *pbt.EndPoint, fake *FakeQueryService) {
	// make sure we use the right client
	*tabletconn.TabletProtocol = protocol

	// create a connection, using sessionId
	ctx := context.Background()
	conn, err := tabletconn.GetDialer()(ctx, endPoint, testTarget.Keyspace, testTarget.Shard, pbt.TabletType_UNKNOWN, 30*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	// run the normal tests
	testBegin(t, conn)
	testCommit(t, conn)
	testRollback(t, conn)
	testExecute(t, conn)
	testStreamExecute(t, conn)
	testExecuteBatch(t, conn)
	testSplitQuery(t, conn)
	testStreamHealth(t, conn)

	// create a new connection that expects the extra fields
	conn.Close()
	conn, err = tabletconn.GetDialer()(ctx, endPoint, testTarget.Keyspace, testTarget.Shard, pbt.TabletType_REPLICA, 30*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	// run the tests that expect extra fields
	fake.checkExtraFields = true
	testBegin2(t, conn)
	testCommit2(t, conn)
	testRollback2(t, conn)
	testExecute2(t, conn)
	testStreamExecute2(t, conn)
	testExecuteBatch2(t, conn)
	testSplitQuery(t, conn)

	// force panics, make sure they're caught (with extra fields)
	fake.panics = true
	testBegin2Panics(t, conn)
	testCommit2Panics(t, conn)
	testRollback2Panics(t, conn)
	testExecute2Panics(t, conn)
	testStreamExecute2Panics(t, conn, fake)
	testExecuteBatch2Panics(t, conn)
	testSplitQueryPanics(t, conn)
	testStreamHealthPanics(t, conn)

	// force panic without extra fields
	conn.Close()
	conn, err = tabletconn.GetDialer()(ctx, endPoint, testTarget.Keyspace, testTarget.Shard, pbt.TabletType_UNKNOWN, 30*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	fake.checkExtraFields = false
	testBeginPanics(t, conn)
	testCommitPanics(t, conn)
	testRollbackPanics(t, conn)
	testExecutePanics(t, conn)
	testExecuteBatchPanics(t, conn)
	testStreamExecutePanics(t, conn, fake)
	fake.panics = false
	conn.Close()
}
