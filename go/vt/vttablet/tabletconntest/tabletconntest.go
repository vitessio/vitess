/*
Copyright 2017 Google Inc.

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

// Package tabletconntest provides the test methods to make sure a
// tabletconn/queryservice pair over RPC works correctly.
package tabletconntest

import (
	"io"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
	"github.com/youtube/vitess/go/vt/vttablet/tabletconn"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// testErrorHelper will check one instance of each error type,
// to make sure we propagate the errors properly.
func testErrorHelper(t *testing.T, f *FakeQueryService, name string, ef func(context.Context) error) {
	errors := []error{
		// A few generic errors
		vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "generic error"),
		vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "uncaught panic"),
		vterrors.Errorf(vtrpcpb.Code_UNAUTHENTICATED, "missing caller id"),
		vterrors.Errorf(vtrpcpb.Code_PERMISSION_DENIED, "table acl error: nil acl"),

		// Client will retry on this specific error
		vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "query disallowed due to rule: %v", "cool rule"),

		// Client may retry on another server on this specific error
		vterrors.Errorf(vtrpcpb.Code_INTERNAL, "could not verify strict mode"),

		// This is usually transaction pool full
		vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "transaction pool connection limit exceeded"),

		// Transaction expired or was unknown
		vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction 12"),
	}
	for _, e := range errors {
		f.TabletError = e
		ctx := context.Background()
		err := ef(ctx)
		if err == nil {
			t.Errorf("error wasn't returned for %v?", name)
			continue
		}

		// First we check the recoverable vtrpc code is right.
		code := vterrors.Code(err)
		wantcode := vterrors.Code(e)
		if code != wantcode {
			t.Errorf("unexpected server code from %v: got %v, wanted %v", name, code, wantcode)
		}

		if !strings.Contains(err.Error(), e.Error()) {
			t.Errorf("client error message '%v' for %v doesn't contain expected server text message '%v'", err.Error(), name, e)
		}
	}
	f.TabletError = nil
}

func testPanicHelper(t *testing.T, f *FakeQueryService, name string, pf func(context.Context) error) {
	f.Panics = true
	ctx := context.Background()
	if err := pf(ctx); err == nil || !strings.Contains(err.Error(), "caught test panic") {
		t.Fatalf("unexpected panic error for %v: %v", name, err)
	}
	f.Panics = false
}

func testBegin(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testBegin")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	transactionID, err := conn.Begin(ctx, TestTarget, TestExecuteOptions)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if transactionID != BeginTransactionID {
		t.Errorf("Unexpected result from Begin: got %v wanted %v", transactionID, BeginTransactionID)
	}
}

func testBeginError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testBeginError")
	f.HasBeginError = true
	testErrorHelper(t, f, "Begin", func(ctx context.Context) error {
		_, err := conn.Begin(ctx, TestTarget, nil)
		return err
	})
	f.HasBeginError = false
}

func testBeginPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testBeginPanics")
	testPanicHelper(t, f, "Begin", func(ctx context.Context) error {
		_, err := conn.Begin(ctx, TestTarget, nil)
		return err
	})
}

func testCommit(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testCommit")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	err := conn.Commit(ctx, TestTarget, CommitTransactionID)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func testCommitError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testCommitError")
	f.HasError = true
	testErrorHelper(t, f, "Commit", func(ctx context.Context) error {
		return conn.Commit(ctx, TestTarget, CommitTransactionID)
	})
	f.HasError = false
}

func testCommitPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testCommitPanics")
	testPanicHelper(t, f, "Commit", func(ctx context.Context) error {
		return conn.Commit(ctx, TestTarget, CommitTransactionID)
	})
}

func testRollback(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testRollback")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	err := conn.Rollback(ctx, TestTarget, RollbackTransactionID)
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
}

func testRollbackError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testRollbackError")
	f.HasError = true
	testErrorHelper(t, f, "Rollback", func(ctx context.Context) error {
		return conn.Rollback(ctx, TestTarget, CommitTransactionID)
	})
	f.HasError = false
}

func testRollbackPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testRollbackPanics")
	testPanicHelper(t, f, "Rollback", func(ctx context.Context) error {
		return conn.Rollback(ctx, TestTarget, RollbackTransactionID)
	})
}

func testPrepare(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testPrepare")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	err := conn.Prepare(ctx, TestTarget, CommitTransactionID, Dtid)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
}

func testPrepareError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testPrepareError")
	f.HasError = true
	testErrorHelper(t, f, "Prepare", func(ctx context.Context) error {
		return conn.Prepare(ctx, TestTarget, CommitTransactionID, Dtid)
	})
	f.HasError = false
}

func testPreparePanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testPreparePanics")
	testPanicHelper(t, f, "Prepare", func(ctx context.Context) error {
		return conn.Prepare(ctx, TestTarget, CommitTransactionID, Dtid)
	})
}

func testCommitPrepared(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testCommitPrepared")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	err := conn.CommitPrepared(ctx, TestTarget, Dtid)
	if err != nil {
		t.Fatalf("CommitPrepared failed: %v", err)
	}
}

func testCommitPreparedError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testCommitPreparedError")
	f.HasError = true
	testErrorHelper(t, f, "CommitPrepared", func(ctx context.Context) error {
		return conn.CommitPrepared(ctx, TestTarget, Dtid)
	})
	f.HasError = false
}

func testCommitPreparedPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testCommitPreparedPanics")
	testPanicHelper(t, f, "CommitPrepared", func(ctx context.Context) error {
		return conn.CommitPrepared(ctx, TestTarget, Dtid)
	})
}

func testRollbackPrepared(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testRollbackPrepared")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	err := conn.RollbackPrepared(ctx, TestTarget, Dtid, RollbackTransactionID)
	if err != nil {
		t.Fatalf("RollbackPrepared failed: %v", err)
	}
}

func testRollbackPreparedError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testRollbackPreparedError")
	f.HasError = true
	testErrorHelper(t, f, "RollbackPrepared", func(ctx context.Context) error {
		return conn.RollbackPrepared(ctx, TestTarget, Dtid, RollbackTransactionID)
	})
	f.HasError = false
}

func testRollbackPreparedPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testRollbackPreparedPanics")
	testPanicHelper(t, f, "RollbackPrepared", func(ctx context.Context) error {
		return conn.RollbackPrepared(ctx, TestTarget, Dtid, RollbackTransactionID)
	})
}

func testCreateTransaction(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testCreateTransaction")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	err := conn.CreateTransaction(ctx, TestTarget, Dtid, Participants)
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}
}

func testCreateTransactionError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testCreateTransactionError")
	f.HasError = true
	testErrorHelper(t, f, "CreateTransaction", func(ctx context.Context) error {
		return conn.CreateTransaction(ctx, TestTarget, Dtid, Participants)
	})
	f.HasError = false
}

func testCreateTransactionPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testCreateTransactionPanics")
	testPanicHelper(t, f, "CreateTransaction", func(ctx context.Context) error {
		return conn.CreateTransaction(ctx, TestTarget, Dtid, Participants)
	})
}

func testStartCommit(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testStartCommit")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	err := conn.StartCommit(ctx, TestTarget, CommitTransactionID, Dtid)
	if err != nil {
		t.Fatalf("StartCommit failed: %v", err)
	}
}

func testStartCommitError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testStartCommitError")
	f.HasError = true
	testErrorHelper(t, f, "StartCommit", func(ctx context.Context) error {
		return conn.StartCommit(ctx, TestTarget, CommitTransactionID, Dtid)
	})
	f.HasError = false
}

func testStartCommitPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testStartCommitPanics")
	testPanicHelper(t, f, "StartCommit", func(ctx context.Context) error {
		return conn.StartCommit(ctx, TestTarget, CommitTransactionID, Dtid)
	})
}

func testSetRollback(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testSetRollback")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	err := conn.SetRollback(ctx, TestTarget, Dtid, RollbackTransactionID)
	if err != nil {
		t.Fatalf("SetRollback failed: %v", err)
	}
}

func testSetRollbackError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testSetRollbackError")
	f.HasError = true
	testErrorHelper(t, f, "SetRollback", func(ctx context.Context) error {
		return conn.SetRollback(ctx, TestTarget, Dtid, RollbackTransactionID)
	})
	f.HasError = false
}

func testSetRollbackPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testSetRollbackPanics")
	testPanicHelper(t, f, "SetRollback", func(ctx context.Context) error {
		return conn.SetRollback(ctx, TestTarget, Dtid, RollbackTransactionID)
	})
}

func testConcludeTransaction(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testConcludeTransaction")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	err := conn.ConcludeTransaction(ctx, TestTarget, Dtid)
	if err != nil {
		t.Fatalf("ConcludeTransaction failed: %v", err)
	}
}

func testConcludeTransactionError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testConcludeTransactionError")
	f.HasError = true
	testErrorHelper(t, f, "ConcludeTransaction", func(ctx context.Context) error {
		return conn.ConcludeTransaction(ctx, TestTarget, Dtid)
	})
	f.HasError = false
}

func testConcludeTransactionPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testConcludeTransactionPanics")
	testPanicHelper(t, f, "ConcludeTransaction", func(ctx context.Context) error {
		return conn.ConcludeTransaction(ctx, TestTarget, Dtid)
	})
}

func testReadTransaction(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testReadTransaction")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	metadata, err := conn.ReadTransaction(ctx, TestTarget, Dtid)
	if err != nil {
		t.Fatalf("ReadTransaction failed: %v", err)
	}
	if !proto.Equal(metadata, Metadata) {
		t.Errorf("Unexpected result from Execute: got %v wanted %v", metadata, Metadata)
	}
}

func testReadTransactionError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testReadTransactionError")
	f.HasError = true
	testErrorHelper(t, f, "ReadTransaction", func(ctx context.Context) error {
		_, err := conn.ReadTransaction(ctx, TestTarget, Dtid)
		return err
	})
	f.HasError = false
}

func testReadTransactionPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testReadTransactionPanics")
	testPanicHelper(t, f, "ReadTransaction", func(ctx context.Context) error {
		_, err := conn.ReadTransaction(ctx, TestTarget, Dtid)
		return err
	})
}

func testExecute(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testExecute")
	f.ExpectedTransactionID = ExecuteTransactionID
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	qr, err := conn.Execute(ctx, TestTarget, ExecuteQuery, ExecuteBindVars, ExecuteTransactionID, TestExecuteOptions)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if !qr.Equal(&ExecuteQueryResult) {
		t.Errorf("Unexpected result from Execute: got %v wanted %v", qr, ExecuteQueryResult)
	}
}

func testExecuteError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testExecuteError")
	f.HasError = true
	testErrorHelper(t, f, "Execute", func(ctx context.Context) error {
		_, err := conn.Execute(ctx, TestTarget, ExecuteQuery, ExecuteBindVars, ExecuteTransactionID, TestExecuteOptions)
		return err
	})
	f.HasError = false
}

func testExecutePanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testExecutePanics")
	testPanicHelper(t, f, "Execute", func(ctx context.Context) error {
		_, err := conn.Execute(ctx, TestTarget, ExecuteQuery, ExecuteBindVars, ExecuteTransactionID, TestExecuteOptions)
		return err
	})
}

func testBeginExecute(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testBeginExecute")
	f.ExpectedTransactionID = BeginTransactionID
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	qr, transactionID, err := conn.BeginExecute(ctx, TestTarget, ExecuteQuery, ExecuteBindVars, TestExecuteOptions)
	if err != nil {
		t.Fatalf("BeginExecute failed: %v", err)
	}
	if transactionID != BeginTransactionID {
		t.Errorf("Unexpected result from BeginExecute: got %v wanted %v", transactionID, BeginTransactionID)
	}
	if !qr.Equal(&ExecuteQueryResult) {
		t.Errorf("Unexpected result from BeginExecute: got %v wanted %v", qr, ExecuteQueryResult)
	}
}

func testBeginExecuteErrorInBegin(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testBeginExecuteErrorInBegin")
	f.HasBeginError = true
	testErrorHelper(t, f, "BeginExecute.Begin", func(ctx context.Context) error {
		_, transactionID, err := conn.BeginExecute(ctx, TestTarget, ExecuteQuery, ExecuteBindVars, TestExecuteOptions)
		if transactionID != 0 {
			t.Errorf("Unexpected transactionID from BeginExecute: got %v wanted 0", transactionID)
		}
		return err
	})
	f.HasBeginError = false
}

func testBeginExecuteErrorInExecute(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testBeginExecuteErrorInExecute")
	f.HasError = true
	testErrorHelper(t, f, "BeginExecute.Execute", func(ctx context.Context) error {
		ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
		_, transactionID, err := conn.BeginExecute(ctx, TestTarget, ExecuteQuery, ExecuteBindVars, TestExecuteOptions)
		if transactionID != BeginTransactionID {
			t.Errorf("Unexpected transactionID from BeginExecute: got %v wanted %v", transactionID, BeginTransactionID)
		}
		return err
	})
	f.HasError = false
}

func testBeginExecutePanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testBeginExecutePanics")
	testPanicHelper(t, f, "BeginExecute", func(ctx context.Context) error {
		_, _, err := conn.BeginExecute(ctx, TestTarget, ExecuteQuery, ExecuteBindVars, TestExecuteOptions)
		return err
	})
}

func testStreamExecute(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testStreamExecute")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	i := 0
	err := conn.StreamExecute(ctx, TestTarget, StreamExecuteQuery, StreamExecuteBindVars, TestExecuteOptions, func(qr *sqltypes.Result) error {
		switch i {
		case 0:
			if len(qr.Rows) == 0 {
				qr.Rows = nil
			}
			if !qr.Equal(&StreamExecuteQueryResult1) {
				t.Errorf("Unexpected result1 from StreamExecute: got %v wanted %v", qr, StreamExecuteQueryResult1)
			}
		case 1:
			if len(qr.Fields) == 0 {
				qr.Fields = nil
			}
			if !qr.Equal(&StreamExecuteQueryResult2) {
				t.Errorf("Unexpected result2 from StreamExecute: got %v wanted %v", qr, StreamExecuteQueryResult2)
			}
		default:
			t.Fatal("callback should not be called any more")
		}
		i++
		if i >= 2 {
			return io.EOF
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func testStreamExecuteError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testStreamExecuteError")
	f.HasError = true
	testErrorHelper(t, f, "StreamExecute", func(ctx context.Context) error {
		f.ErrorWait = make(chan struct{})
		ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
		return conn.StreamExecute(ctx, TestTarget, StreamExecuteQuery, StreamExecuteBindVars, TestExecuteOptions, func(qr *sqltypes.Result) error {
			// For some errors, the call can be retried.
			select {
			case <-f.ErrorWait:
				return nil
			default:
			}
			if len(qr.Rows) == 0 {
				qr.Rows = nil
			}
			if !qr.Equal(&StreamExecuteQueryResult1) {
				t.Errorf("Unexpected result1 from StreamExecute: got %v wanted %v", qr, StreamExecuteQueryResult1)
			}
			// signal to the server that the first result has been received
			close(f.ErrorWait)
			return nil
		})
	})
	f.HasError = false
}

func testStreamExecutePanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testStreamExecutePanics")
	// early panic is before sending the Fields, that is returned
	// by the StreamExecute call itself, or as the first error
	// by ErrFunc
	f.StreamExecutePanicsEarly = true
	testPanicHelper(t, f, "StreamExecute.Early", func(ctx context.Context) error {
		ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
		return conn.StreamExecute(ctx, TestTarget, StreamExecuteQuery, StreamExecuteBindVars, TestExecuteOptions, func(qr *sqltypes.Result) error {
			return nil
		})
	})

	// late panic is after sending Fields
	f.StreamExecutePanicsEarly = false
	testPanicHelper(t, f, "StreamExecute.Late", func(ctx context.Context) error {
		f.PanicWait = make(chan struct{})
		ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
		return conn.StreamExecute(ctx, TestTarget, StreamExecuteQuery, StreamExecuteBindVars, TestExecuteOptions, func(qr *sqltypes.Result) error {
			// For some errors, the call can be retried.
			select {
			case <-f.PanicWait:
				return nil
			default:
			}
			if len(qr.Rows) == 0 {
				qr.Rows = nil
			}
			if !qr.Equal(&StreamExecuteQueryResult1) {
				t.Errorf("Unexpected result1 from StreamExecute: got %v wanted %v", qr, StreamExecuteQueryResult1)
			}
			// signal to the server that the first result has been received
			close(f.PanicWait)
			return nil
		})
	})
}

func testExecuteBatch(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testExecuteBatch")
	f.ExpectedTransactionID = ExecuteBatchTransactionID
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	qrl, err := conn.ExecuteBatch(ctx, TestTarget, ExecuteBatchQueries, TestAsTransaction, ExecuteBatchTransactionID, TestExecuteOptions)
	if err != nil {
		t.Fatalf("ExecuteBatch failed: %v", err)
	}
	if !sqltypes.ResultsEqual(qrl, ExecuteBatchQueryResultList) {
		t.Errorf("Unexpected result from ExecuteBatch: got %v wanted %v", qrl, ExecuteBatchQueryResultList)
	}
}

func testExecuteBatchError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testExecuteBatchError")
	f.HasError = true
	testErrorHelper(t, f, "ExecuteBatch", func(ctx context.Context) error {
		_, err := conn.ExecuteBatch(ctx, TestTarget, ExecuteBatchQueries, TestAsTransaction, ExecuteBatchTransactionID, TestExecuteOptions)
		return err
	})
	f.HasError = true
}

func testExecuteBatchPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testExecuteBatchPanics")
	testPanicHelper(t, f, "ExecuteBatch", func(ctx context.Context) error {
		_, err := conn.ExecuteBatch(ctx, TestTarget, ExecuteBatchQueries, TestAsTransaction, ExecuteBatchTransactionID, TestExecuteOptions)
		return err
	})
}

func testBeginExecuteBatch(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testBeginExecuteBatch")
	f.ExpectedTransactionID = BeginTransactionID
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	qrl, transactionID, err := conn.BeginExecuteBatch(ctx, TestTarget, ExecuteBatchQueries, true, TestExecuteOptions)
	if err != nil {
		t.Fatalf("BeginExecuteBatch failed: %v", err)
	}
	if transactionID != BeginTransactionID {
		t.Errorf("Unexpected result from BeginExecuteBatch: got %v wanted %v", transactionID, BeginTransactionID)
	}
	if !sqltypes.ResultsEqual(qrl, ExecuteBatchQueryResultList) {
		t.Errorf("Unexpected result from ExecuteBatch: got %v wanted %v", qrl, ExecuteBatchQueryResultList)
	}
}

func testBeginExecuteBatchErrorInBegin(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testBeginExecuteBatchErrorInBegin")
	f.HasBeginError = true
	testErrorHelper(t, f, "BeginExecuteBatch.Begin", func(ctx context.Context) error {
		_, transactionID, err := conn.BeginExecuteBatch(ctx, TestTarget, ExecuteBatchQueries, true, TestExecuteOptions)
		if transactionID != 0 {
			t.Errorf("Unexpected transactionID from BeginExecuteBatch: got %v wanted 0", transactionID)
		}
		return err
	})
	f.HasBeginError = false
}

func testBeginExecuteBatchErrorInExecuteBatch(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testBeginExecuteBatchErrorInExecuteBatch")
	f.HasError = true
	testErrorHelper(t, f, "BeginExecute.ExecuteBatch", func(ctx context.Context) error {
		ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
		_, transactionID, err := conn.BeginExecuteBatch(ctx, TestTarget, ExecuteBatchQueries, true, TestExecuteOptions)
		if transactionID != BeginTransactionID {
			t.Errorf("Unexpected transactionID from BeginExecuteBatch: got %v wanted %v", transactionID, BeginTransactionID)
		}
		return err
	})
	f.HasError = false
}

func testBeginExecuteBatchPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testBeginExecuteBatchPanics")
	testPanicHelper(t, f, "BeginExecuteBatch", func(ctx context.Context) error {
		_, _, err := conn.BeginExecuteBatch(ctx, TestTarget, ExecuteBatchQueries, true, TestExecuteOptions)
		return err
	})
}

func testMessageStream(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testMessageStream")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	var got *sqltypes.Result
	err := conn.MessageStream(ctx, TestTarget, MessageName, func(qr *sqltypes.Result) error {
		got = qr
		return nil
	})
	if err != nil {
		t.Fatalf("MessageStream failed: %v", err)
	}
	if !got.Equal(MessageStreamResult) {
		t.Errorf("Unexpected result from MessageStream: got %v wanted %v", got, MessageStreamResult)
	}
}

func testMessageStreamError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testMessageStreamError")
	f.HasError = true
	testErrorHelper(t, f, "MessageStream", func(ctx context.Context) error {
		ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
		return conn.MessageStream(ctx, TestTarget, MessageName, func(qr *sqltypes.Result) error { return nil })
	})
	f.HasError = false
}

func testMessageStreamPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testMessageStreamPanics")
	testPanicHelper(t, f, "MessageStream", func(ctx context.Context) error {
		err := conn.MessageStream(ctx, TestTarget, MessageName, func(qr *sqltypes.Result) error { return nil })
		return err
	})
}

func testMessageAck(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testMessageAck")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	count, err := conn.MessageAck(ctx, TestTarget, MessageName, MessageIDs)
	if err != nil {
		t.Fatalf("MessageAck failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Unexpected result from MessageAck: got %v wanted 1", count)
	}
}

func testMessageAckError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testMessageAckError")
	f.HasError = true
	testErrorHelper(t, f, "MessageAck", func(ctx context.Context) error {
		ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
		_, err := conn.MessageAck(ctx, TestTarget, MessageName, MessageIDs)
		return err
	})
	f.HasError = false
}

func testMessageAckPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testMessageAckPanics")
	testPanicHelper(t, f, "MessageAck", func(ctx context.Context) error {
		_, err := conn.MessageAck(ctx, TestTarget, MessageName, MessageIDs)
		return err
	})
}

func testSplitQuery(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testSplitQuery")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	qsl, err := conn.SplitQuery(
		ctx,
		TestTarget,
		SplitQueryBoundQuery,
		SplitQuerySplitColumns,
		SplitQuerySplitCount,
		SplitQueryNumRowsPerQueryPart,
		SplitQueryAlgorithm,
	)
	if err != nil {
		t.Fatalf("SplitQuery failed: %v", err)
	}
	if !querytypes.QuerySplitsEqual(qsl, SplitQueryQuerySplitList) {
		t.Errorf("Unexpected result from SplitQuery: got %v wanted %v", qsl, SplitQueryQuerySplitList)
	}
}

func testSplitQueryError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testSplitQueryError")
	f.HasError = true
	testErrorHelper(t, f, "SplitQuery", func(ctx context.Context) error {
		_, err := conn.SplitQuery(
			ctx,
			TestTarget,
			SplitQueryBoundQuery,
			SplitQuerySplitColumns,
			SplitQuerySplitCount,
			SplitQueryNumRowsPerQueryPart,
			SplitQueryAlgorithm,
		)
		return err
	})
	f.HasError = false
}

func testSplitQueryPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testSplitQueryPanics")
	testPanicHelper(t, f, "SplitQuery", func(ctx context.Context) error {
		_, err := conn.SplitQuery(
			ctx,
			TestTarget,
			SplitQueryBoundQuery,
			SplitQuerySplitColumns,
			SplitQuerySplitCount,
			SplitQueryNumRowsPerQueryPart,
			SplitQueryAlgorithm,
		)
		return err
	})
}

// this test is a bit of a hack: we write something on the channel
// upon registration, and we also return an error, so the streaming query
// ends right there. Otherwise we have no real way to trigger a real
// communication error, that ends the streaming.
func testStreamHealth(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testStreamHealth")
	ctx := context.Background()

	var health *querypb.StreamHealthResponse
	err := conn.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
		health = shr
		return io.EOF
	})
	if err != nil {
		t.Fatalf("StreamHealth failed: %v", err)
	}
	if !proto.Equal(health, TestStreamHealthStreamHealthResponse) {
		t.Errorf("invalid StreamHealthResponse: got %v expected %v", *health, *TestStreamHealthStreamHealthResponse)
	}
}

func testStreamHealthError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testStreamHealthError")
	f.HasError = true
	ctx := context.Background()
	err := conn.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
		t.Fatalf("Unexpected call to callback")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), TestStreamHealthErrorMsg) {
		t.Fatalf("StreamHealth failed with the wrong error: %v", err)
	}
	f.HasError = false
}

func testStreamHealthPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testStreamHealthPanics")
	testPanicHelper(t, f, "StreamHealth", func(ctx context.Context) error {
		return conn.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
			t.Fatalf("Unexpected call to callback")
			return nil
		})
	})
}

func testUpdateStream(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testUpdateStream")
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
	i := 0
	err := conn.UpdateStream(ctx, TestTarget, UpdateStreamPosition, UpdateStreamTimestamp, func(qr *querypb.StreamEvent) error {
		switch i {
		case 0:
			if !proto.Equal(qr, &UpdateStreamStreamEvent1) {
				t.Errorf("Unexpected result1 from UpdateStream: got %v wanted %v", qr, UpdateStreamStreamEvent1)
			}
		case 1:
			if !proto.Equal(qr, &UpdateStreamStreamEvent2) {
				t.Errorf("Unexpected result2 from UpdateStream: got %v wanted %v", qr, UpdateStreamStreamEvent2)
			}
		default:
			t.Fatal("callback should not be called any more")
		}
		i++
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateStream failed: %v", err)
	}
}

func testUpdateStreamError(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testUpdateStreamError")
	f.HasError = true
	testErrorHelper(t, f, "UpdateStream", func(ctx context.Context) error {
		f.ErrorWait = make(chan struct{})
		ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
		return conn.UpdateStream(ctx, TestTarget, UpdateStreamPosition, UpdateStreamTimestamp, func(qr *querypb.StreamEvent) error {
			// For some errors, the call can be retried.
			select {
			case <-f.ErrorWait:
				return nil
			default:
			}
			if !proto.Equal(qr, &UpdateStreamStreamEvent1) {
				t.Errorf("Unexpected result1 from UpdateStream: got %v wanted %v", qr, UpdateStreamStreamEvent1)
			}
			// signal to the server that the first result has been received
			close(f.ErrorWait)
			return nil
		})
	})
	f.HasError = false
}

func testUpdateStreamPanics(t *testing.T, conn queryservice.QueryService, f *FakeQueryService) {
	t.Log("testUpdateStreamPanics")
	// early panic is before sending the Fields, that is returned
	// by the UpdateStream call itself, or as the first error
	// by ErrFunc
	f.UpdateStreamPanicsEarly = true
	testPanicHelper(t, f, "UpdateStream.Early", func(ctx context.Context) error {
		ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
		return conn.UpdateStream(ctx, TestTarget, UpdateStreamPosition, UpdateStreamTimestamp, func(qr *querypb.StreamEvent) error {
			return nil
		})
	})

	// late panic is after sending Fields
	f.UpdateStreamPanicsEarly = false
	testPanicHelper(t, f, "UpdateStream.Late", func(ctx context.Context) error {
		f.PanicWait = make(chan struct{})
		ctx = callerid.NewContext(ctx, TestCallerID, TestVTGateCallerID)
		i := 0
		return conn.UpdateStream(ctx, TestTarget, UpdateStreamPosition, UpdateStreamTimestamp, func(qr *querypb.StreamEvent) error {
			// For some errors, the call can be retried.
			select {
			case <-f.PanicWait:
				return nil
			default:
			}
			switch i {
			case 0:
				if !proto.Equal(qr, &UpdateStreamStreamEvent1) {
					t.Errorf("Unexpected result1 from UpdateStream: got %v wanted %v", qr, UpdateStreamStreamEvent1)
				}
				close(f.PanicWait)
			default:
				t.Fatal("callback should not be called any more")
			}
			i++
			return nil
		})
	})
}

// TestSuite runs all the tests.
// If fake.TestingGateway is set, we only test the calls that can go through
// a gateway.
func TestSuite(t *testing.T, protocol string, tablet *topodatapb.Tablet, fake *FakeQueryService) {
	tests := []func(*testing.T, queryservice.QueryService, *FakeQueryService){
		// positive test cases
		testBegin,
		testCommit,
		testRollback,
		testPrepare,
		testCommitPrepared,
		testRollbackPrepared,
		testCreateTransaction,
		testStartCommit,
		testSetRollback,
		testConcludeTransaction,
		testReadTransaction,
		testExecute,
		testBeginExecute,
		testStreamExecute,
		testExecuteBatch,
		testBeginExecuteBatch,
		testMessageStream,
		testMessageAck,
		testSplitQuery,
		testUpdateStream,

		// error test cases
		testBeginError,
		testCommitError,
		testRollbackError,
		testPrepareError,
		testCommitPreparedError,
		testRollbackPreparedError,
		testCreateTransactionError,
		testStartCommitError,
		testSetRollbackError,
		testConcludeTransactionError,
		testReadTransactionError,
		testExecuteError,
		testBeginExecuteErrorInBegin,
		testBeginExecuteErrorInExecute,
		testStreamExecuteError,
		testExecuteBatchError,
		testBeginExecuteBatchErrorInBegin,
		testBeginExecuteBatchErrorInExecuteBatch,
		testMessageStreamError,
		testMessageAckError,
		testSplitQueryError,
		testUpdateStreamError,

		// panic test cases
		testBeginPanics,
		testCommitPanics,
		testRollbackPanics,
		testPreparePanics,
		testCommitPreparedPanics,
		testRollbackPreparedPanics,
		testCreateTransactionPanics,
		testStartCommitPanics,
		testSetRollbackPanics,
		testConcludeTransactionPanics,
		testReadTransactionPanics,
		testExecutePanics,
		testBeginExecutePanics,
		testStreamExecutePanics,
		testExecuteBatchPanics,
		testBeginExecuteBatchPanics,
		testMessageStreamPanics,
		testMessageAckPanics,
		testSplitQueryPanics,
		testUpdateStreamPanics,
	}

	if !fake.TestingGateway {
		tests = append(tests, []func(*testing.T, queryservice.QueryService, *FakeQueryService){
			// positive test cases
			testStreamHealth,

			// error test cases
			testStreamHealthError,

			// panic test cases
			testStreamHealthPanics,
		}...)
	}

	// make sure we use the right client
	*tabletconn.TabletProtocol = protocol

	// create a connection
	conn, err := tabletconn.GetDialer()(tablet, 30*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	// run the tests
	for _, c := range tests {
		c(t, conn, fake)
	}

	// and we're done
	conn.Close(context.Background())
}
