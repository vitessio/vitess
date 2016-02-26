// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqldb"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"golang.org/x/net/context"
)

func TestTabletErrorCode(t *testing.T) {
	tErr := NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "error")
	wantCode := vtrpcpb.ErrorCode_INTERNAL_ERROR
	code := tErr.VtErrorCode()
	if wantCode != code {
		t.Errorf("VtErrorCode() => %v, want %v", code, wantCode)
	}
}

func TestTabletErrorRetriableErrorTypeOverwrite(t *testing.T) {
	sqlErr := sqldb.NewSQLError(mysql.ErrOptionPreventsStatement, "read-only")
	tabletErr := NewTabletErrorSQL(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, sqlErr)
	if tabletErr.ErrorType != ErrRetry || tabletErr.ErrorCode != vtrpcpb.ErrorCode_QUERY_NOT_SERVED {
		t.Fatalf("tablet error should have error type ErrRetry and error code %v", vtrpcpb.ErrorCode_QUERY_NOT_SERVED)
	}
}

func TestTabletErrorMsgTooLong(t *testing.T) {
	buf := make([]byte, 2*maxErrLen)
	for i := 0; i < 2*maxErrLen; i++ {
		buf[i] = 'a'
	}
	msg := string(buf)
	sqlErr := sqldb.NewSQLError(mysql.ErrDupEntry, msg)
	tabletErr := NewTabletErrorSQL(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, sqlErr)
	if tabletErr.ErrorType != ErrFatal || tabletErr.ErrorCode != vtrpcpb.ErrorCode_INTEGRITY_ERROR {
		t.Fatalf("tablet error should have error type ErrFatal and error code %v", vtrpcpb.ErrorCode_INTEGRITY_ERROR)
	}
	if tabletErr.Message != string(buf[:maxErrLen]) {
		t.Fatalf("message should be capped, only %d character will be shown", maxErrLen)
	}
}

func TestTabletErrorConnError(t *testing.T) {
	tabletErr := NewTabletErrorSQL(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(1999, "test"))
	if IsConnErr(tabletErr) {
		t.Fatalf("table error: %v is not a connection error", tabletErr)
	}
	tabletErr = NewTabletErrorSQL(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(2000, "test"))
	if !IsConnErr(tabletErr) {
		t.Fatalf("table error: %v is a connection error", tabletErr)
	}
	tabletErr = NewTabletErrorSQL(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(mysql.ErrServerLost, "test"))
	if IsConnErr(tabletErr) {
		t.Fatalf("table error: %v is not a connection error", tabletErr)
	}
	want := "fatal: the query was killed either because it timed out or was canceled: test (errno 2013)"
	if tabletErr.Error() != want {
		t.Fatalf("tablet error: %v, want %s", tabletErr, want)
	}
	sqlErr := sqldb.NewSQLError(1998, "test")
	if IsConnErr(sqlErr) {
		t.Fatalf("sql error: %v is not a connection error", sqlErr)
	}
	sqlErr = sqldb.NewSQLError(2001, "test")
	if !IsConnErr(sqlErr) {
		t.Fatalf("sql error: %v is a connection error", sqlErr)
	}

	err := fmt.Errorf("(errno 2005)")
	if !IsConnErr(err) {
		t.Fatalf("error: %v is a connection error", err)
	}

	err = fmt.Errorf("(errno 123456789012345678901234567890)")
	if IsConnErr(err) {
		t.Fatalf("error: %v is not a connection error", err)
	}
}

func TestTabletErrorPrefix(t *testing.T) {
	tabletErr := NewTabletErrorSQL(ErrRetry, vtrpcpb.ErrorCode_QUERY_NOT_SERVED, sqldb.NewSQLError(2000, "test"))
	if tabletErr.Prefix() != "retry: " {
		t.Fatalf("tablet error with error type: ErrRetry should has prefix: 'retry: '")
	}
	tabletErr = NewTabletErrorSQL(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(2000, "test"))
	if tabletErr.Prefix() != "fatal: " {
		t.Fatalf("tablet error with error type: ErrFatal should has prefix: 'fatal: '")
	}
	tabletErr = NewTabletErrorSQL(ErrTxPoolFull, vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED, sqldb.NewSQLError(2000, "test"))
	if tabletErr.Prefix() != "tx_pool_full: " {
		t.Fatalf("tablet error with error type: ErrTxPoolFull should has prefix: 'tx_pool_full: '")
	}
	tabletErr = NewTabletErrorSQL(ErrNotInTx, vtrpcpb.ErrorCode_NOT_IN_TX, sqldb.NewSQLError(2000, "test"))
	if tabletErr.Prefix() != "not_in_tx: " {
		t.Fatalf("tablet error with error type: ErrNotInTx should has prefix: 'not_in_tx: '")
	}
}

func TestTabletErrorRecordStats(t *testing.T) {
	tabletErr := NewTabletErrorSQL(ErrRetry, vtrpcpb.ErrorCode_QUERY_NOT_SERVED, sqldb.NewSQLError(2000, "test"))
	queryServiceStats := NewQueryServiceStats("", false)
	retryCounterBefore := queryServiceStats.InfoErrors.Counts()["Retry"]
	tabletErr.RecordStats(queryServiceStats)
	retryCounterAfter := queryServiceStats.InfoErrors.Counts()["Retry"]
	if retryCounterAfter-retryCounterBefore != 1 {
		t.Fatalf("tablet error with error type ErrRetry should increase Retry error count by 1")
	}

	tabletErr = NewTabletErrorSQL(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(2000, "test"))
	fatalCounterBefore := queryServiceStats.InfoErrors.Counts()["Fatal"]
	tabletErr.RecordStats(queryServiceStats)
	fatalCounterAfter := queryServiceStats.InfoErrors.Counts()["Fatal"]
	if fatalCounterAfter-fatalCounterBefore != 1 {
		t.Fatalf("tablet error with error type ErrFatal should increase Fatal error count by 1")
	}

	tabletErr = NewTabletErrorSQL(ErrTxPoolFull, vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED, sqldb.NewSQLError(2000, "test"))
	txPoolFullCounterBefore := queryServiceStats.ErrorStats.Counts()["TxPoolFull"]
	tabletErr.RecordStats(queryServiceStats)
	txPoolFullCounterAfter := queryServiceStats.ErrorStats.Counts()["TxPoolFull"]
	if txPoolFullCounterAfter-txPoolFullCounterBefore != 1 {
		t.Fatalf("tablet error with error type ErrTxPoolFull should increase TxPoolFull error count by 1")
	}

	tabletErr = NewTabletErrorSQL(ErrNotInTx, vtrpcpb.ErrorCode_NOT_IN_TX, sqldb.NewSQLError(2000, "test"))
	notInTxCounterBefore := queryServiceStats.ErrorStats.Counts()["NotInTx"]
	tabletErr.RecordStats(queryServiceStats)
	notInTxCounterAfter := queryServiceStats.ErrorStats.Counts()["NotInTx"]
	if notInTxCounterAfter-notInTxCounterBefore != 1 {
		t.Fatalf("tablet error with error type ErrNotInTx should increase NotInTx error count by 1")
	}

	tabletErr = NewTabletErrorSQL(ErrFail, vtrpcpb.ErrorCode_UNKNOWN_ERROR, sqldb.NewSQLError(mysql.ErrDupEntry, "test"))
	dupKeyCounterBefore := queryServiceStats.InfoErrors.Counts()["DupKey"]
	tabletErr.RecordStats(queryServiceStats)
	dupKeyCounterAfter := queryServiceStats.InfoErrors.Counts()["DupKey"]
	if dupKeyCounterAfter-dupKeyCounterBefore != 1 {
		t.Fatalf("sql error with error type mysql.ErrDupEntry should increase DupKey error count by 1")
	}

	tabletErr = NewTabletErrorSQL(ErrFail, vtrpcpb.ErrorCode_UNKNOWN_ERROR, sqldb.NewSQLError(mysql.ErrLockWaitTimeout, "test"))
	lockWaitTimeoutCounterBefore := queryServiceStats.ErrorStats.Counts()["Deadlock"]
	tabletErr.RecordStats(queryServiceStats)
	lockWaitTimeoutCounterAfter := queryServiceStats.ErrorStats.Counts()["Deadlock"]
	if lockWaitTimeoutCounterAfter-lockWaitTimeoutCounterBefore != 1 {
		t.Fatalf("sql error with error type mysql.ErrLockWaitTimeout should increase Deadlock error count by 1")
	}

	tabletErr = NewTabletErrorSQL(ErrFail, vtrpcpb.ErrorCode_UNKNOWN_ERROR, sqldb.NewSQLError(mysql.ErrLockDeadlock, "test"))
	deadlockCounterBefore := queryServiceStats.ErrorStats.Counts()["Deadlock"]
	tabletErr.RecordStats(queryServiceStats)
	deadlockCounterAfter := queryServiceStats.ErrorStats.Counts()["Deadlock"]
	if deadlockCounterAfter-deadlockCounterBefore != 1 {
		t.Fatalf("sql error with error type mysql.ErrLockDeadlock should increase Deadlock error count by 1")
	}

	tabletErr = NewTabletErrorSQL(ErrFail, vtrpcpb.ErrorCode_UNKNOWN_ERROR, sqldb.NewSQLError(mysql.ErrOptionPreventsStatement, "test"))
	failCounterBefore := queryServiceStats.ErrorStats.Counts()["Fail"]
	tabletErr.RecordStats(queryServiceStats)
	failCounterAfter := queryServiceStats.ErrorStats.Counts()["Fail"]
	if failCounterAfter-failCounterBefore != 1 {
		t.Fatalf("sql error with error type mysql.ErrOptionPreventsStatement should increase Fail error count by 1")
	}
}

func TestTabletErrorHandleUncaughtError(t *testing.T) {
	var err error
	logStats := newLogStats("TestTabletErrorHandleError", context.Background())
	queryServiceStats := NewQueryServiceStats("", false)
	defer func() {
		_, ok := err.(*TabletError)
		if !ok {
			t.Fatalf("error should be a TabletError, but got error: %v", err)
		}
	}()
	defer handleError(&err, logStats, queryServiceStats)
	panic("unknown error")
}

func TestTabletErrorHandleRetryError(t *testing.T) {
	var err error
	tabletErr := NewTabletErrorSQL(ErrRetry, vtrpcpb.ErrorCode_QUERY_NOT_SERVED, sqldb.NewSQLError(1000, "test"))
	logStats := newLogStats("TestTabletErrorHandleError", context.Background())
	queryServiceStats := NewQueryServiceStats("", false)
	defer func() {
		_, ok := err.(*TabletError)
		if !ok {
			t.Fatalf("error should be a TabletError, but got error: %v", err)
		}
	}()
	defer handleError(&err, logStats, queryServiceStats)
	panic(tabletErr)
}

func TestTabletErrorHandleTxPoolFullError(t *testing.T) {
	var err error
	tabletErr := NewTabletErrorSQL(ErrTxPoolFull, vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED, sqldb.NewSQLError(1000, "test"))
	logStats := newLogStats("TestTabletErrorHandleError", context.Background())
	queryServiceStats := NewQueryServiceStats("", false)
	defer func() {
		_, ok := err.(*TabletError)
		if !ok {
			t.Fatalf("error should be a TabletError, but got error: %v", err)
		}
	}()
	defer handleError(&err, logStats, queryServiceStats)
	panic(tabletErr)
}

func TestTabletErrorLogUncaughtErr(t *testing.T) {
	queryServiceStats := NewQueryServiceStats("", false)
	panicCountBefore := queryServiceStats.InternalErrors.Counts()["Panic"]
	defer func() {
		panicCountAfter := queryServiceStats.InternalErrors.Counts()["Panic"]
		if panicCountAfter-panicCountBefore != 1 {
			t.Fatalf("Panic count should increase by 1 for uncaught panic")
		}
	}()
	defer logError(queryServiceStats)
	panic("unknown error")
}

func TestTabletErrorTxPoolFull(t *testing.T) {
	tabletErr := NewTabletErrorSQL(ErrTxPoolFull, vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED, sqldb.NewSQLError(1000, "test"))
	queryServiceStats := NewQueryServiceStats("", false)
	defer func() {
		err := recover()
		if err != nil {
			t.Fatalf("error should have been handled already")
		}
	}()
	defer logError(queryServiceStats)
	panic(tabletErr)
}

func TestTabletErrorFatal(t *testing.T) {
	tabletErr := NewTabletErrorSQL(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(1000, "test"))
	queryServiceStats := NewQueryServiceStats("", false)
	defer func() {
		err := recover()
		if err != nil {
			t.Fatalf("error should have been handled already")
		}
	}()
	defer logError(queryServiceStats)
	panic(tabletErr)
}
