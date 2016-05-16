// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package tabletconntest provides the test methods to make sure a
// tabletconn/queryservice pair over RPC works correctly.
package tabletconntest

import (
	"errors"
	"fmt"
	"io"
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
	t *testing.T

	// these fields are used to simulate and synchronize on errors
	hasError      bool
	hasBeginError bool
	tabletError   *tabletserver.TabletError
	errorWait     chan struct{}

	// these fields are used to simulate and synchronize on panics
	panics                   bool
	streamExecutePanicsEarly bool
	panicWait                chan struct{}

	// expectedTransactionID is what transactionID to expect for Execute
	expectedTransactionID int64
}

// HandlePanic is part of the queryservice.QueryService interface
func (f *FakeQueryService) HandlePanic(err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("caught test panic: %v", x)
	}
}

// testErrorHelper will check one instance of each error type,
// to make sure we propagate the errors properly.
func testErrorHelper(t *testing.T, f *FakeQueryService, name string, ef func(context.Context) error) {
	errors := []*tabletserver.TabletError{
		// A few generic errors
		tabletserver.NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "generic error"),
		tabletserver.NewTabletError(vtrpcpb.ErrorCode_UNKNOWN_ERROR, "uncaught panic"),
		tabletserver.NewTabletError(vtrpcpb.ErrorCode_UNAUTHENTICATED, "missing caller id"),
		tabletserver.NewTabletError(vtrpcpb.ErrorCode_PERMISSION_DENIED, "table acl error: nil acl"),

		// Client will retry on this specific error
		tabletserver.NewTabletError(vtrpcpb.ErrorCode_QUERY_NOT_SERVED, "Query disallowed due to rule: %v", "cool rule"),

		// Client may retry on another server on this specific error
		tabletserver.NewTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, "Could not verify strict mode"),

		// This is usually transaction pool full
		tabletserver.NewTabletError(vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED, "Transaction pool connection limit exceeded"),

		// Transaction expired or was unknown
		tabletserver.NewTabletError(vtrpcpb.ErrorCode_NOT_IN_TX, "Transaction 12"),
	}
	for _, e := range errors {
		f.tabletError = e
		ctx := context.Background()
		err := ef(ctx)
		if err == nil {
			t.Errorf("error wasn't returned for %v?", name)
			continue
		}

		// First we check the recoverable vtrpc code is right.
		code := vterrors.RecoverVtErrorCode(err)
		if code != e.ErrorCode {
			t.Errorf("unexpected server code from %v: got %v, wanted %v", name, code, e.ErrorCode)
		}

		// Double-check we always get a ServerError, although
		// we don't really care that much.
		if _, ok := err.(*tabletconn.ServerError); !ok {
			t.Errorf("error wasn't a tabletconn.ServerError for %v?", name)
			continue
		}

		// and last we check we preserve the text, with the right prefix
		if !strings.Contains(err.Error(), e.Prefix()+e.Message) {
			t.Errorf("client error message '%v' for %v doesn't contain expected server text message '%v'", err.Error(), name, e.Prefix()+e.Message)
		}
	}
	f.tabletError = nil
}

func testPanicHelper(t *testing.T, f *FakeQueryService, name string, pf func(context.Context) error) {
	f.panics = true
	ctx := context.Background()
	if err := pf(ctx); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error for %v: %v", name, err)
	}
	f.panics = false
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

func (f *FakeQueryService) checkTargetCallerID(ctx context.Context, name string, target *querypb.Target) {
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

// Begin is part of the queryservice.QueryService interface
func (f *FakeQueryService) Begin(ctx context.Context, target *querypb.Target) (int64, error) {
	if f.hasBeginError {
		return 0, f.tabletError
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "Begin", target)
	return beginTransactionID, nil
}

const beginTransactionID int64 = 9990

func testBegin(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
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

func testBeginError(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.hasBeginError = true
	testErrorHelper(t, f, "Begin", func(ctx context.Context) error {
		_, err := conn.Begin(ctx)
		return err
	})
	f.hasBeginError = false
}

func testBeginPanics(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	testPanicHelper(t, f, "Begin", func(ctx context.Context) error {
		_, err := conn.Begin(ctx)
		return err
	})
}

// Commit is part of the queryservice.QueryService interface
func (f *FakeQueryService) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	if f.hasError {
		return f.tabletError
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "Commit", target)
	if transactionID != commitTransactionID {
		f.t.Errorf("Commit: invalid TransactionId: got %v expected %v", transactionID, commitTransactionID)
	}
	return nil
}

const commitTransactionID int64 = 999044

func testCommit(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	err := conn.Commit(ctx, commitTransactionID)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func testCommitError(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.hasError = true
	testErrorHelper(t, f, "Commit", func(ctx context.Context) error {
		return conn.Commit(ctx, commitTransactionID)
	})
	f.hasError = false
}

func testCommitPanics(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	testPanicHelper(t, f, "Commit", func(ctx context.Context) error {
		return conn.Commit(ctx, commitTransactionID)
	})
}

// Rollback is part of the queryservice.QueryService interface
func (f *FakeQueryService) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	if f.hasError {
		return f.tabletError
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "Rollback", target)
	if transactionID != rollbackTransactionID {
		f.t.Errorf("Rollback: invalid TransactionId: got %v expected %v", transactionID, rollbackTransactionID)
	}
	return nil
}

const rollbackTransactionID int64 = 999044

func testRollback(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	err := conn.Rollback(ctx, rollbackTransactionID)
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
}

func testRollbackError(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.hasError = true
	testErrorHelper(t, f, "Rollback", func(ctx context.Context) error {
		return conn.Rollback(ctx, commitTransactionID)
	})
}

func testRollbackPanics(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	testPanicHelper(t, f, "Rollback", func(ctx context.Context) error {
		return conn.Rollback(ctx, rollbackTransactionID)
	})
}

// Execute is part of the queryservice.QueryService interface
func (f *FakeQueryService) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
	if f.hasError {
		return nil, f.tabletError
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
	f.checkTargetCallerID(ctx, "Execute", target)
	if transactionID != f.expectedTransactionID {
		f.t.Errorf("invalid Execute.TransactionId: got %v expected %v", transactionID, f.expectedTransactionID)
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

func testExecute(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.expectedTransactionID = executeTransactionID
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

func testExecuteError(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.hasError = true
	testErrorHelper(t, f, "Execute", func(ctx context.Context) error {
		_, err := conn.Execute(ctx, executeQuery, executeBindVars, executeTransactionID)
		return err
	})
	f.hasError = false
}

func testExecutePanics(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	testPanicHelper(t, f, "Execute", func(ctx context.Context) error {
		_, err := conn.Execute(ctx, executeQuery, executeBindVars, executeTransactionID)
		return err
	})
}

func testBeginExecute(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.expectedTransactionID = beginTransactionID
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	qr, transactionID, err := conn.BeginExecute(ctx, executeQuery, executeBindVars)
	if err != nil {
		t.Fatalf("BeginExecute failed: %v", err)
	}
	if transactionID != beginTransactionID {
		t.Errorf("Unexpected result from BeginExecute: got %v wanted %v", transactionID, beginTransactionID)
	}
	if !reflect.DeepEqual(*qr, executeQueryResult) {
		t.Errorf("Unexpected result from BeginExecute: got %v wanted %v", qr, executeQueryResult)
	}
}

func testBeginExecuteErrorInBegin(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.hasBeginError = true
	testErrorHelper(t, f, "BeginExecute.Begin", func(ctx context.Context) error {
		_, transactionID, err := conn.BeginExecute(ctx, executeQuery, executeBindVars)
		if transactionID != 0 {
			t.Errorf("Unexpected transactionID from BeginExecute: got %v wanted 0", transactionID)
		}
		return err
	})
	f.hasBeginError = false
}

func testBeginExecuteErrorInExecute(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.hasError = true
	testErrorHelper(t, f, "BeginExecute.Execute", func(ctx context.Context) error {
		ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
		_, transactionID, err := conn.BeginExecute(ctx, executeQuery, executeBindVars)
		if transactionID != beginTransactionID {
			t.Errorf("Unexpected transactionID from BeginExecute: got %v wanted %v", transactionID, beginTransactionID)
		}
		return err
	})
	f.hasError = false
}

func testBeginExecutePanics(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	testPanicHelper(t, f, "BeginExecute", func(ctx context.Context) error {
		_, _, err := conn.BeginExecute(ctx, executeQuery, executeBindVars)
		return err
	})
}

// StreamExecute is part of the queryservice.QueryService interface
func (f *FakeQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, sendReply func(*sqltypes.Result) error) error {
	if f.panics && f.streamExecutePanicsEarly {
		panic(fmt.Errorf("test-triggered panic early"))
	}
	if sql != streamExecuteQuery {
		f.t.Errorf("invalid StreamExecute.Sql: got %v expected %v", sql, streamExecuteQuery)
	}
	if !reflect.DeepEqual(bindVariables, streamExecuteBindVars) {
		f.t.Errorf("invalid StreamExecute.BindVariables: got %v expected %v", bindVariables, streamExecuteBindVars)
	}
	f.checkTargetCallerID(ctx, "StreamExecute", target)
	if err := sendReply(&streamExecuteQueryResult1); err != nil {
		f.t.Errorf("sendReply1 failed: %v", err)
	}
	if f.panics && !f.streamExecutePanicsEarly {
		// wait until the client gets the response, then panics
		<-f.panicWait
		panic(fmt.Errorf("test-triggered panic late"))
	}
	if f.hasError {
		// wait until the client has the response, since all
		// streaming implementation may not send previous
		// messages if an error has been triggered.
		<-f.errorWait
		return f.tabletError
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

func testStreamExecute(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	stream, err := conn.StreamExecute(ctx, streamExecuteQuery, streamExecuteBindVars)
	if err != nil {
		t.Fatalf("StreamExecute failed: %v", err)
	}
	qr, err := stream.Recv()
	if err != nil {
		t.Fatalf("StreamExecute failed: cannot read result1: %v", err)
	}
	if len(qr.Rows) == 0 {
		qr.Rows = nil
	}
	if !reflect.DeepEqual(*qr, streamExecuteQueryResult1) {
		t.Errorf("Unexpected result1 from StreamExecute: got %v wanted %v", qr, streamExecuteQueryResult1)
	}
	qr, err = stream.Recv()
	if err != nil {
		t.Fatalf("StreamExecute failed: cannot read result2: %v", err)
	}
	if len(qr.Fields) == 0 {
		qr.Fields = nil
	}
	if !reflect.DeepEqual(*qr, streamExecuteQueryResult2) {
		t.Errorf("Unexpected result2 from StreamExecute: got %v wanted %v", qr, streamExecuteQueryResult2)
	}
	qr, err = stream.Recv()
	if err != io.EOF {
		t.Fatalf("StreamExecute errFunc failed: %v", err)
	}
}

func testStreamExecuteError(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.hasError = true
	testErrorHelper(t, f, "StreamExecute", func(ctx context.Context) error {
		f.errorWait = make(chan struct{})
		ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
		stream, err := conn.StreamExecute(ctx, streamExecuteQuery, streamExecuteBindVars)
		if err != nil {
			t.Fatalf("StreamExecute failed: %v", err)
		}
		qr, err := stream.Recv()
		if err != nil {
			t.Fatalf("StreamExecute failed: cannot read result1: %v", err)
		}
		if len(qr.Rows) == 0 {
			qr.Rows = nil
		}
		if !reflect.DeepEqual(*qr, streamExecuteQueryResult1) {
			t.Errorf("Unexpected result1 from StreamExecute: got %v wanted %v", qr, streamExecuteQueryResult1)
		}
		// signal to the server that the first result has been received
		close(f.errorWait)
		// After 1 result, we expect to get an error (no more results).
		qr, err = stream.Recv()
		if err == nil {
			t.Fatalf("StreamExecute channel wasn't closed")
		}
		return err
	})
	f.hasError = false
}

func testStreamExecutePanics(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	// early panic is before sending the Fields, that is returned
	// by the StreamExecute call itself, or as the first error
	// by ErrFunc
	f.streamExecutePanicsEarly = true
	testPanicHelper(t, f, "StreamExecute.Early", func(ctx context.Context) error {
		ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
		stream, err := conn.StreamExecute(ctx, streamExecuteQuery, streamExecuteBindVars)
		if err != nil {
			return err
		}
		_, err = stream.Recv()
		return err
	})

	// late panic is after sending Fields
	f.streamExecutePanicsEarly = false
	testPanicHelper(t, f, "StreamExecute.Late", func(ctx context.Context) error {
		f.panicWait = make(chan struct{})
		ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
		stream, err := conn.StreamExecute(ctx, streamExecuteQuery, streamExecuteBindVars)
		if err != nil {
			t.Fatalf("StreamExecute failed: %v", err)
		}
		qr, err := stream.Recv()
		if err != nil {
			t.Fatalf("StreamExecute failed: cannot read result1: %v", err)
		}
		if len(qr.Rows) == 0 {
			qr.Rows = nil
		}
		if !reflect.DeepEqual(*qr, streamExecuteQueryResult1) {
			t.Errorf("Unexpected result1 from StreamExecute: got %v wanted %v", qr, streamExecuteQueryResult1)
		}
		close(f.panicWait)
		_, err = stream.Recv()
		return err
	})
}

// ExecuteBatch is part of the queryservice.QueryService interface
func (f *FakeQueryService) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	if f.hasError {
		return nil, f.tabletError
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	if !reflect.DeepEqual(queries, executeBatchQueries) {
		f.t.Errorf("invalid ExecuteBatch.Queries: got %v expected %v", queries, executeBatchQueries)
	}
	f.checkTargetCallerID(ctx, "ExecuteBatch", target)
	if asTransaction != testAsTransaction {
		f.t.Errorf("invalid ExecuteBatch.AsTransaction: got %v expected %v", asTransaction, testAsTransaction)
	}
	if transactionID != f.expectedTransactionID {
		f.t.Errorf("invalid ExecuteBatch.TransactionId: got %v expected %v", transactionID, f.expectedTransactionID)
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

func testExecuteBatch(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.expectedTransactionID = executeBatchTransactionID
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

func testExecuteBatchError(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.hasError = true
	testErrorHelper(t, f, "ExecuteBatch", func(ctx context.Context) error {
		_, err := conn.ExecuteBatch(ctx, executeBatchQueries, true, executeBatchTransactionID)
		return err
	})
	f.hasError = true
}

func testExecuteBatchPanics(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	testPanicHelper(t, f, "ExecuteBatch", func(ctx context.Context) error {
		_, err := conn.ExecuteBatch(ctx, executeBatchQueries, true, executeBatchTransactionID)
		return err
	})
}

func testBeginExecuteBatch(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.expectedTransactionID = beginTransactionID
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	qrl, transactionID, err := conn.BeginExecuteBatch(ctx, executeBatchQueries, true)
	if err != nil {
		t.Fatalf("BeginExecuteBatch failed: %v", err)
	}
	if transactionID != beginTransactionID {
		t.Errorf("Unexpected result from BeginExecuteBatch: got %v wanted %v", transactionID, beginTransactionID)
	}
	if !reflect.DeepEqual(qrl, executeBatchQueryResultList) {
		t.Errorf("Unexpected result from ExecuteBatch: got %v wanted %v", qrl, executeBatchQueryResultList)
	}
}

func testBeginExecuteBatchErrorInBegin(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.hasBeginError = true
	testErrorHelper(t, f, "BeginExecuteBatch.Begin", func(ctx context.Context) error {
		_, transactionID, err := conn.BeginExecuteBatch(ctx, executeBatchQueries, true)
		if transactionID != 0 {
			t.Errorf("Unexpected transactionID from BeginExecuteBatch: got %v wanted 0", transactionID)
		}
		return err
	})
	f.hasBeginError = false
}

func testBeginExecuteBatchErrorInExecuteBatch(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.hasError = true
	testErrorHelper(t, f, "BeginExecute.ExecuteBatch", func(ctx context.Context) error {
		ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
		_, transactionID, err := conn.BeginExecuteBatch(ctx, executeBatchQueries, true)
		if transactionID != beginTransactionID {
			t.Errorf("Unexpected transactionID from BeginExecuteBatch: got %v wanted %v", transactionID, beginTransactionID)
		}
		return err
	})
	f.hasError = false
}

func testBeginExecuteBatchPanics(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	testPanicHelper(t, f, "BeginExecuteBatch", func(ctx context.Context) error {
		_, _, err := conn.BeginExecuteBatch(ctx, executeBatchQueries, true)
		return err
	})
}

// SplitQuery is part of the queryservice.QueryService interface
func (f *FakeQueryService) SplitQuery(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error) {
	if f.hasError {
		return nil, f.tabletError
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "SplitQuery", target)
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

// SplitQueryV2 is part of the queryservice.QueryService interface
// TODO(erez): Rename to SplitQuery after migration to SplitQuery V2 is done.
func (f *FakeQueryService) SplitQueryV2(
	ctx context.Context,
	target *querypb.Target,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) ([]querytypes.QuerySplit, error) {

	if f.hasError {
		return nil, f.tabletError
	}
	if f.panics {
		panic(fmt.Errorf("test-triggered panic"))
	}
	f.checkTargetCallerID(ctx, "SplitQueryV2", target)
	if !reflect.DeepEqual(querytypes.BoundQuery{
		Sql:           sql,
		BindVariables: bindVariables,
	}, splitQueryV2BoundQuery) {
		f.t.Errorf("invalid SplitQuery.SplitQueryRequest.Query: got %v expected %v",
			querytypes.QueryAsString(sql, bindVariables), splitQueryBoundQuery)
	}
	if !reflect.DeepEqual(splitColumns, splitQueryV2SplitColumns) {
		f.t.Errorf("invalid SplitQuery.SplitColumn: got %v expected %v",
			splitColumns, splitQueryV2SplitColumns)
	}
	if !reflect.DeepEqual(splitCount, splitQueryV2SplitCount) {
		f.t.Errorf("invalid SplitQuery.SplitCount: got %v expected %v",
			splitCount, splitQueryV2SplitCount)
	}
	if !reflect.DeepEqual(numRowsPerQueryPart, splitQueryV2NumRowsPerQueryPart) {
		f.t.Errorf("invalid SplitQuery.numRowsPerQueryPart: got %v expected %v",
			numRowsPerQueryPart, splitQueryV2NumRowsPerQueryPart)
	}
	if algorithm != splitQueryV2Algorithm {
		f.t.Errorf("invalid SplitQuery.algorithm: got %v expected %v",
			algorithm, splitQueryV2Algorithm)
	}
	return splitQueryQueryV2SplitList, nil
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

// TODO(erez): Rename to SplitQuery after migration to SplitQuery V2 is done.
var splitQueryV2SplitColumns = []string{"nice_column_to_split"}

const splitQueryV2SplitCount = 372

var splitQueryV2BoundQuery = querytypes.BoundQuery{
	Sql: "splitQuery",
	BindVariables: map[string]interface{}{
		"bind1": int64(43),
	},
}

const splitQueryV2NumRowsPerQueryPart = 123
const splitQueryV2Algorithm = querypb.SplitQueryRequest_FULL_SCAN

var splitQueryQueryV2SplitList = []querytypes.QuerySplit{
	{
		Sql: "splitQuery",
		BindVariables: map[string]interface{}{
			"bind1":       int64(43),
			"keyspace_id": int64(3333),
		},
		RowCount: 4456,
	},
}

func testSplitQuery(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
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

// TODO(erez): Rename to SplitQuery after migration to SplitQuery V2 is done.
func testSplitQueryV2(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, testVTGateCallerID)
	qsl, err := conn.SplitQueryV2(
		ctx,
		splitQueryV2BoundQuery,
		splitQueryV2SplitColumns,
		splitQueryV2SplitCount,
		splitQueryV2NumRowsPerQueryPart,
		splitQueryV2Algorithm,
	)
	if err != nil {
		t.Fatalf("SplitQuery failed: %v", err)
	}
	if !reflect.DeepEqual(qsl, splitQueryQueryV2SplitList) {
		t.Errorf("Unexpected result from SplitQuery: got %v wanted %v", qsl, splitQueryQuerySplitList)
	}
}

func testSplitQueryError(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.hasError = true
	testErrorHelper(t, f, "SplitQuery", func(ctx context.Context) error {
		_, err := conn.SplitQuery(ctx, splitQueryBoundQuery, splitQuerySplitColumn, splitQuerySplitCount)
		return err
	})
	f.hasError = false
}

func testSplitQueryPanics(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	testPanicHelper(t, f, "SplitQuery", func(ctx context.Context) error {
		_, err := conn.SplitQuery(ctx, splitQueryBoundQuery, splitQuerySplitColumn, splitQuerySplitCount)
		return err
	})
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
		return 0, errors.New(testStreamHealthErrorMsg)
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

func testStreamHealth(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
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

func testStreamHealthError(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	f.hasError = true
	ctx := context.Background()
	stream, err := conn.StreamHealth(ctx)
	if err != nil {
		t.Fatalf("StreamHealth failed: %v", err)
	}
	_, err = stream.Recv()
	if err == nil || !strings.Contains(err.Error(), testStreamHealthErrorMsg) {
		t.Fatalf("StreamHealth failed with the wrong error: %v", err)
	}
	f.hasError = false
}

func testStreamHealthPanics(t *testing.T, conn tabletconn.TabletConn, f *FakeQueryService) {
	testPanicHelper(t, f, "StreamHealth", func(ctx context.Context) error {
		stream, err := conn.StreamHealth(ctx)
		if err != nil {
			t.Fatalf("StreamHealth failed: %v", err)
		}
		_, err = stream.Recv()
		return err
	})
}

// CreateFakeServer returns the fake server for the tests
func CreateFakeServer(t *testing.T) *FakeQueryService {
	return &FakeQueryService{
		t: t,
	}
}

// TestSuite runs all the tests
func TestSuite(t *testing.T, protocol string, tablet *topodatapb.Tablet, fake *FakeQueryService) {
	tests := []func(*testing.T, tabletconn.TabletConn, *FakeQueryService){
		// positive test cases
		testBegin,
		testCommit,
		testRollback,
		testExecute,
		testBeginExecute,
		testStreamExecute,
		testExecuteBatch,
		testBeginExecuteBatch,
		testSplitQuery,
		testStreamHealth,

		// error test cases
		testBeginError,
		testCommitError,
		testRollbackError,
		testExecuteError,
		testBeginExecuteErrorInBegin,
		testBeginExecuteErrorInExecute,
		testStreamExecuteError,
		testExecuteBatchError,
		testBeginExecuteBatchErrorInBegin,
		testBeginExecuteBatchErrorInExecuteBatch,
		testSplitQueryError,
		testStreamHealthError,

		// panic test cases
		testBeginPanics,
		testCommitPanics,
		testRollbackPanics,
		testExecutePanics,
		testBeginExecutePanics,
		testStreamExecutePanics,
		testExecuteBatchPanics,
		testBeginExecuteBatchPanics,
		testSplitQueryPanics,
		testStreamHealthPanics,
	}

	// make sure we use the right client
	*tabletconn.TabletProtocol = protocol

	// create a connection
	ctx := context.Background()
	conn, err := tabletconn.GetDialer()(ctx, tablet, testTarget.Keyspace, testTarget.Shard, topodatapb.TabletType_REPLICA, 30*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	// run the tests
	for _, c := range tests {
		c(t, conn, fake)
	}

	// and we're done
	conn.Close()
}
