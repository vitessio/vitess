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

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/vterrors"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// FakeQueryService has the server side of this fake
type FakeQueryService struct {
	t                        *testing.T
	hasError                 bool
	panics                   bool
	streamExecutePanicsEarly bool
	panicWait                chan struct{}
	errorWait                chan struct{}

	// if set, we check target, if not set we check sessionId
	checkTarget bool
}

// HandlePanic is part of the queryservice.QueryService interface
func (f *FakeQueryService) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("caught test panic: %v", x)
	}
}

const expectedErrMatch string = "error: generic error"
const expectedCode vtrpcpb.ErrorCode = vtrpcpb.ErrorCode_BAD_INPUT

var testTabletError = tabletserver.NewTabletError(tabletserver.ErrFail, expectedCode, "generic error")

// Verifies the returned error has the properties that we expect.
func verifyError(t *testing.T, err error, method string) {
	if err == nil {
		t.Errorf("%s was expecting an error, didn't get one", method)
		return
	}
	code := vterrors.RecoverVtErrorCode(err)
	if code != expectedCode {
		t.Errorf("Unexpected server code from %s: got %v, wanted %v", method, code, expectedCode)
	}
	verifyErrorExceptServerCode(t, err, method)
}

func verifyErrorExceptServerCode(t *testing.T, err error, method string) {
	if err == nil {
		t.Errorf("%s was expecting an error, didn't get one", method)
		return
	}

	if se, ok := err.(*tabletconn.ServerError); ok {
		if se.Code != tabletconn.ERR_NORMAL {
			t.Errorf("Unexpected error code from %s: got %v, wanted %v", method, se.Code, tabletconn.ERR_NORMAL)
		}
	} else {
		t.Errorf("Unexpected error type from %s: got %v, wanted *tabletconn.ServerError", method, reflect.TypeOf(err))
	}
	if !strings.Contains(err.Error(), expectedErrMatch) {
		t.Errorf("Unexpected error from %s: got %v, wanted err containing %v", method, err, expectedErrMatch)
	}
}

// testTarget is the target we use for this test
var testTarget = &querypb.Target{
	Keyspace:   "test_keyspace",
	Shard:      "test_shard",
	TabletType: topodatapb.TabletType_REPLICA,
}

var testCallerID = &vtrpcpb.CallerID{
	Principal:    "test_principal",
	Component:    "test_component",
	Subcomponent: "test_subcomponent",
}

var testVTGateCallerID = &querypb.VTGateCallerID{
	Username: "test_username",
}

const testAsTransaction bool = true

const testSessionID int64 = 5678

func (f *FakeQueryService) checkSessionTargetCallerID(ctx context.Context, name string, target *querypb.Target, sessionID int64) {
	if f.checkTarget {
		if !reflect.DeepEqual(target, testTarget) {
			f.t.Errorf("invalid Target for %v: got %#v expected %#v", name, target, testTarget)
		}
	} else {
		if sessionID != testSessionID {
			f.t.Errorf("invalid sessionID for %v: got %v expected %v", name, sessionID, testSessionID)
		}
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
func (f *FakeQueryService) GetSessionId(keyspace, shard string) (int64, error) {
	if keyspace != testTarget.Keyspace {
		f.t.Errorf("invalid keyspace: got %v expected %v", keyspace, testTarget.Keyspace)
	}
	if shard != testTarget.Shard {
		f.t.Errorf("invalid shard: got %v expected %v", shard, testTarget.Shard)
	}
	return testSessionID, nil
}

// Begin is part of the queryservice.QueryService interface
func (f *FakeQueryService) Begin(ctx context.Context, target *querypb.Target, sessionID int64) (int64, error) {
	if f.hasError {
		return 0, testTabletError
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkSessionTargetCallerID(ctx, "Begin", target, sessionID)
	return beginTransactionID, nil
}

const beginTransactionID int64 = 9990

func testBegin(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	transactionID, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if transactionID != beginTransactionID {
		t.Errorf("Unexpected result from Begin: got %v wanted %v", transactionID, beginTransactionID)
	}
}

func testBeginError(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	_, err := conn.Begin(ctx)
	verifyError(t, err, "Begin")
}

func testBeginPanics(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	if _, err := conn.Begin(ctx); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// Commit is part of the queryservice.QueryService interface
func (f *FakeQueryService) Commit(ctx context.Context, target *querypb.Target, sessionID, transactionID int64) error {
	if f.hasError {
		return testTabletError
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkSessionTargetCallerID(ctx, "Commit", target, sessionID)
	if transactionID != commitTransactionID {
		f.t.Errorf("Commit: invalid TransactionId: got %v expected %v", transactionID, commitTransactionID)
	}
	return nil
}

const commitTransactionID int64 = 999044

func testCommit(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	err := conn.Commit(ctx, commitTransactionID)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func testCommitError(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	err := conn.Commit(ctx, commitTransactionID)
	verifyError(t, err, "Commit")
}

func testCommitPanics(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	if err := conn.Commit(ctx, commitTransactionID); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// Rollback is part of the queryservice.QueryService interface
func (f *FakeQueryService) Rollback(ctx context.Context, target *querypb.Target, sessionID, transactionID int64) error {
	if f.hasError {
		return testTabletError
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkSessionTargetCallerID(ctx, "Rollback", target, sessionID)
	if transactionID != rollbackTransactionID {
		f.t.Errorf("Rollback: invalid TransactionId: got %v expected %v", transactionID, rollbackTransactionID)
	}
	return nil
}

const rollbackTransactionID int64 = 999044

func testRollback(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	err := conn.Rollback(ctx, rollbackTransactionID)
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
}

func testRollbackError(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	err := conn.Rollback(ctx, commitTransactionID)
	verifyError(t, err, "Rollback")
}

func testRollbackPanics(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	if err := conn.Rollback(ctx, rollbackTransactionID); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// Execute is part of the queryservice.QueryService interface
func (f *FakeQueryService) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sessionID, transactionID int64) (*sqltypes.Result, error) {
	if f.hasError {
		return nil, testTabletError
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if sql != executeQuery {
		f.t.Errorf("invalid Execute.Query.Sql: got %v expected %v", sql, executeQuery)
	}
	if !reflect.DeepEqual(bindVariables, executeBindVars) {
		f.t.Errorf("invalid Execute.BindVariables: got %v expected %v", bindVariables, executeBindVars)
	}
	f.checkSessionTargetCallerID(ctx, "Execute", target, sessionID)
	if transactionID != executeTransactionID {
		f.t.Errorf("invalid Execute.TransactionId: got %v expected %v", transactionID, executeTransactionID)
	}
	return &executeQueryResult, nil
}

const executeQuery = "executeQuery"

var executeBindVars = map[string]interface{}{
	"bind1": int64(1114444),
}

const executeTransactionID int64 = 678

var executeQueryResult = sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "field1",
			Type: sqltypes.Int8,
		},
		{
			Name: "field2",
			Type: sqltypes.Char,
		},
	},
	RowsAffected: 123,
	InsertID:     72,
	Rows: [][]sqltypes.Value{
		{
			sqltypes.MakeTrusted(sqltypes.Int8, []byte("1")),
			sqltypes.NULL,
		},
		{
			sqltypes.MakeTrusted(sqltypes.Int8, []byte("2")),
			sqltypes.MakeTrusted(sqltypes.Char, []byte("row2 value2")),
		},
	},
}

func testExecute(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	qr, err := conn.Execute(ctx, executeQuery, executeBindVars, executeTransactionID)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if !reflect.DeepEqual(*qr, executeQueryResult) {
		t.Errorf("Unexpected result from Execute: got %v wanted %v", qr, executeQueryResult)
	}
}

func testExecuteError(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	_, err := conn.Execute(ctx, executeQuery, executeBindVars, executeTransactionID)
	verifyError(t, err, "Execute")
}

func testExecutePanics(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	if _, err := conn.Execute(ctx, executeQuery, executeBindVars, executeTransactionID); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// StreamExecute is part of the queryservice.QueryService interface
func (f *FakeQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sessionID int64, sendReply func(*sqltypes.Result) error) error {
	if f.panics && f.streamExecutePanicsEarly {
		panic(fmt.Errorf("test-triggered panic early"))
	}
	if sql != streamExecuteQuery {
		f.t.Errorf("invalid StreamExecute.Sql: got %v expected %v", sql, streamExecuteQuery)
	}
	if !reflect.DeepEqual(bindVariables, streamExecuteBindVars) {
		f.t.Errorf("invalid StreamExecute.BindVariables: got %v expected %v", bindVariables, streamExecuteBindVars)
	}
	f.checkSessionTargetCallerID(ctx, "StreamExecute", target, sessionID)
	if err := sendReply(&streamExecuteQueryResult1); err != nil {
		f.t.Errorf("sendReply1 failed: %v", err)
	}
	if f.panics && !f.streamExecutePanicsEarly {
		// wait until the client gets the response, then panics
		<-f.panicWait
		f.panicWait = make(chan struct{}) // for next test
		panic(fmt.Errorf("test-triggered panic late"))
	}
	if f.hasError {
		// wait until the client has the response, since all streaming implementation may not
		// send previous messages if an error has been triggered.
		<-f.errorWait
		f.errorWait = make(chan struct{}) // for next test
		return testTabletError
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

var streamExecuteQueryResult1 = sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "field1",
			Type: sqltypes.Int8,
		},
		{
			Name: "field2",
			Type: sqltypes.Char,
		},
	},
}

var streamExecuteQueryResult2 = sqltypes.Result{
	Rows: [][]sqltypes.Value{
		{
			sqltypes.MakeTrusted(sqltypes.Int8, []byte("1")),
			sqltypes.MakeTrusted(sqltypes.Char, []byte("row1 value2")),
		},
		{
			sqltypes.MakeTrusted(sqltypes.Int8, []byte("2")),
			sqltypes.MakeTrusted(sqltypes.Char, []byte("row2 value2")),
		},
	},
}

func testStreamExecute(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
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

func testStreamExecuteError(t *testing.T, conn tabletconn.TabletConn, fake *FakeQueryService) {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
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
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, ok = <-stream
	if ok {
		t.Fatalf("StreamExecute channel wasn't closed")
	}
	err = errFunc()
	verifyError(t, err, "StreamExecute")
}

func testStreamExecutePanics(t *testing.T, conn tabletconn.TabletConn, fake *FakeQueryService) {
	// early panic is before sending the Fields, that is returned
	// by the StreamExecute call itself, or as the first error
	// by ErrFunc
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
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

// ExecuteBatch is part of the queryservice.QueryService interface
func (f *FakeQueryService) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, sessionID int64, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	if f.hasError {
		return nil, testTabletError
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if !reflect.DeepEqual(queries, executeBatchQueries) {
		f.t.Errorf("invalid ExecuteBatch.Queries: got %v expected %v", queries, executeBatchQueries)
	}
	f.checkSessionTargetCallerID(ctx, "ExecuteBatch", target, sessionID)
	if asTransaction != testAsTransaction {
		f.t.Errorf("invalid ExecuteBatch.AsTransaction: got %v expected %v", asTransaction, testAsTransaction)
	}
	if transactionID != executeBatchTransactionID {
		f.t.Errorf("invalid ExecuteBatch.TransactionId: got %v expected %v", transactionID, executeBatchTransactionID)
	}
	return executeBatchQueryResultList, nil
}

var executeBatchQueries = []querytypes.BoundQuery{
	{
		Sql: "executeBatchQueries1",
		BindVariables: map[string]interface{}{
			"bind1": int64(43),
		},
	},
	{
		Sql: "executeBatchQueries2",
		BindVariables: map[string]interface{}{
			"bind2": int64(72),
		},
	},
}

const executeBatchTransactionID int64 = 678

var executeBatchQueryResultList = []sqltypes.Result{
	{
		Fields: []*querypb.Field{
			{
				Name: "field1",
				Type: sqltypes.Int8,
			},
		},
		RowsAffected: 1232,
		InsertID:     712,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(sqltypes.Int8, []byte("1")),
			},
			{
				sqltypes.MakeTrusted(sqltypes.Int8, []byte("2")),
			},
		},
	},
	{
		Fields: []*querypb.Field{
			{
				Name: "field1",
				Type: sqltypes.VarBinary,
			},
		},
		RowsAffected: 12333,
		InsertID:     74442,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeString([]byte("row1 value1")),
			},
			{
				sqltypes.MakeString([]byte("row1 value2")),
			},
		},
	},
}

func testExecuteBatch(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	qrl, err := conn.ExecuteBatch(ctx, executeBatchQueries, true, executeBatchTransactionID)
	if err != nil {
		t.Fatalf("ExecuteBatch failed: %v", err)
	}
	if !reflect.DeepEqual(qrl, executeBatchQueryResultList) {
		t.Errorf("Unexpected result from ExecuteBatch: got %v wanted %v", qrl, executeBatchQueryResultList)
	}
}

func testExecuteBatchError(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	_, err := conn.ExecuteBatch(ctx, executeBatchQueries, true, executeBatchTransactionID)
	verifyError(t, err, "ExecuteBatch")
}

func testExecuteBatchPanics(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	if _, err := conn.ExecuteBatch(ctx, executeBatchQueries, true, executeBatchTransactionID); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// SplitQuery is part of the queryservice.QueryService interface
func (f *FakeQueryService) SplitQuery(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64, sessionID int64) ([]querytypes.QuerySplit, error) {
	if f.hasError {
		return nil, testTabletError
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkSessionTargetCallerID(ctx, "SplitQuery", target, sessionID)
	if !reflect.DeepEqual(querytypes.BoundQuery{
		Sql:           sql,
		BindVariables: bindVariables,
	}, splitQueryBoundQuery) {
		f.t.Errorf("invalid SplitQuery.SplitQueryRequest.Query: got %v expected %v", querytypes.QueryAsString(sql, bindVariables), splitQueryBoundQuery)
	}
	if splitColumn != splitQuerySplitColumn {
		f.t.Errorf("invalid SplitQuery.SplitColumn: got %v expected %v", splitColumn, splitQuerySplitColumn)
	}
	if splitCount != splitQuerySplitCount {
		f.t.Errorf("invalid SplitQuery.SplitCount: got %v expected %v", splitCount, splitQuerySplitCount)
	}
	return splitQueryQuerySplitList, nil
}

var splitQueryBoundQuery = querytypes.BoundQuery{
	Sql: "splitQuery",
	BindVariables: map[string]interface{}{
		"bind1": int64(43),
	},
}

const splitQuerySplitColumn = "nice_column_to_split"
const splitQuerySplitCount = 372

var splitQueryQuerySplitList = []querytypes.QuerySplit{
	{
		Sql: "splitQuery",
		BindVariables: map[string]interface{}{
			"bind1":       int64(43),
			"keyspace_id": int64(3333),
		},
		RowCount: 4456,
	},
}

func testSplitQuery(t *testing.T, conn tabletconn.TabletConn) {
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

func testSplitQueryError(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	_, err := conn.SplitQuery(ctx, splitQueryBoundQuery, splitQuerySplitColumn, splitQuerySplitCount)
	verifyError(t, err, "SplitQuery")
}

func testSplitQueryPanics(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	if _, err := conn.SplitQuery(ctx, splitQueryBoundQuery, splitQuerySplitColumn, splitQuerySplitCount); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error: %v", err)
	}
}

// this test is a bit of a hack: we write something on the channel
// upon registration, and we also return an error, so the streaming query
// ends right there. Otherwise we have no real way to trigger a real
// communication error, that ends the streaming.
var testStreamHealthStreamHealthResponse = &querypb.StreamHealthResponse{
	Target: &querypb.Target{
		Keyspace:   "test_keyspace",
		Shard:      "test_shard",
		TabletType: topodatapb.TabletType_RDONLY,
	},
	Serving: true,
	TabletExternallyReparentedTimestamp: 1234589,
	RealtimeStats: &querypb.RealtimeStats{
		HealthError:                            "random error",
		SecondsBehindMaster:                    234,
		BinlogPlayersCount:                     1,
		SecondsBehindMasterFilteredReplication: 2,
		CpuUsage: 1.0,
	},
}
var testStreamHealthErrorMsg = "to trigger a server error"

// StreamHealthRegister is part of the queryservice.QueryService interface
func (f *FakeQueryService) StreamHealthRegister(c chan<- *querypb.StreamHealthResponse) (int, error) {
	if f.hasError {
		return 0, fmt.Errorf(testStreamHealthErrorMsg)
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	c <- testStreamHealthStreamHealthResponse
	return 1, nil
}

// StreamHealthUnregister is part of the queryservice.QueryService interface
func (f *FakeQueryService) StreamHealthUnregister(int) error {
	return nil
}

func testStreamHealth(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()

	stream, err := conn.StreamHealth(ctx)
	if err != nil {
		t.Fatalf("StreamHealth failed: %v", err)
	}
	// channel should have one response, then closed
	shr, err := stream.Recv()
	if err != nil {
		t.Fatalf("StreamHealth got no response")
	}

	if !reflect.DeepEqual(*shr, *testStreamHealthStreamHealthResponse) {
		t.Errorf("invalid StreamHealthResponse: got %v expected %v", *shr, *testStreamHealthStreamHealthResponse)
	}
}

func testStreamHealthError(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	stream, err := conn.StreamHealth(ctx)
	if err != nil {
		t.Fatalf("StreamHealth failed: %v", err)
	}
	_, err = stream.Recv()
	if err == nil || !strings.Contains(err.Error(), testStreamHealthErrorMsg) {
		t.Fatalf("StreamHealth failed with the wrong error: %v", err)
	}
}

func testStreamHealthPanics(t *testing.T, conn tabletconn.TabletConn) {
	ctx := context.Background()
	stream, err := conn.StreamHealth(ctx)
	if err != nil {
		t.Fatalf("StreamHealth failed: %v", err)
	}
	_, err = stream.Recv()
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
		errorWait:                make(chan struct{}),
	}
}

// TestSuite runs all the tests
func TestSuite(t *testing.T, protocol string, endPoint *topodatapb.EndPoint, fake *FakeQueryService) {
	// make sure we use the right client
	*tabletconn.TabletProtocol = protocol

	// create a connection, using sessionId
	ctx := context.Background()
	conn, err := tabletconn.GetDialer()(ctx, endPoint, testTarget.Keyspace, testTarget.Shard, topodatapb.TabletType_UNKNOWN, 30*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	fake.checkTarget = false

	// run the tests
	testBegin(t, conn)
	testCommit(t, conn)
	testRollback(t, conn)
	testExecute(t, conn)
	testStreamExecute(t, conn)
	testExecuteBatch(t, conn)
	testSplitQuery(t, conn)
	testStreamHealth(t, conn)

	// fake should return an error, make sure errors are handled properly
	fake.hasError = true
	testBeginError(t, conn)
	testCommitError(t, conn)
	testRollbackError(t, conn)
	testExecuteError(t, conn)
	testStreamExecuteError(t, conn, fake)
	testExecuteBatchError(t, conn)
	testSplitQueryError(t, conn)
	testStreamHealthError(t, conn)
	fake.hasError = false

	// force panics, make sure they're caught
	fake.panics = true
	testBeginPanics(t, conn)
	testCommitPanics(t, conn)
	testRollbackPanics(t, conn)
	testExecutePanics(t, conn)
	testStreamExecutePanics(t, conn, fake)
	testExecuteBatchPanics(t, conn)
	testSplitQueryPanics(t, conn)
	testStreamHealthPanics(t, conn)
	fake.panics = false

	// create a new connection that expects the target
	conn.Close()
	conn, err = tabletconn.GetDialer()(ctx, endPoint, testTarget.Keyspace, testTarget.Shard, topodatapb.TabletType_REPLICA, 30*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	fake.checkTarget = true

	// run the tests
	testBegin(t, conn)
	testCommit(t, conn)
	testRollback(t, conn)
	testExecute(t, conn)
	testStreamExecute(t, conn)
	testExecuteBatch(t, conn)
	testSplitQuery(t, conn)
	testStreamHealth(t, conn)

	// fake should return an error, make sure errors are handled properly
	fake.hasError = true
	testBeginError(t, conn)
	testCommitError(t, conn)
	testRollbackError(t, conn)
	testExecuteError(t, conn)
	testStreamExecuteError(t, conn, fake)
	testExecuteBatchError(t, conn)
	testSplitQueryError(t, conn)
	testStreamHealthError(t, conn)
	fake.hasError = false

	// force panics, make sure they're caught
	fake.panics = true
	testBeginPanics(t, conn)
	testCommitPanics(t, conn)
	testRollbackPanics(t, conn)
	testExecutePanics(t, conn)
	testStreamExecutePanics(t, conn, fake)
	testExecuteBatchPanics(t, conn)
	testSplitQueryPanics(t, conn)
	testStreamHealthPanics(t, conn)
	fake.panics = false

	// and we're done
	conn.Close()
}
