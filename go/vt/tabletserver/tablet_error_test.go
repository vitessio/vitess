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
)

func TestTabletErrorCode(t *testing.T) {
	tErr := NewTabletError(vtrpcpb.ErrorCode_INTERNAL_ERROR, "error")
	wantCode := vtrpcpb.ErrorCode_INTERNAL_ERROR
	code := tErr.VtErrorCode()
	if wantCode != code {
		t.Errorf("VtErrorCode() => %v, want %v", code, wantCode)
	}
}

func TestTabletErrorRetriableErrorTypeOverwrite(t *testing.T) {
	sqlErr := sqldb.NewSQLError(mysql.ErrOptionPreventsStatement, "HY000", "read-only")
	tabletErr := NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqlErr)
	if tabletErr.ErrorCode != vtrpcpb.ErrorCode_QUERY_NOT_SERVED {
		t.Fatalf("got: %v wanted: QUERY_NOT_SERVED", tabletErr.ErrorCode)
	}
}

func TestTabletErrorMsgTooLong(t *testing.T) {
	buf := make([]byte, 2*maxErrLen)
	for i := 0; i < 2*maxErrLen; i++ {
		buf[i] = 'a'
	}
	msg := string(buf)
	sqlErr := sqldb.NewSQLError(mysql.ErrDupEntry, "23000", msg)
	tabletErr := NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqlErr)
	if tabletErr.ErrorCode != vtrpcpb.ErrorCode_INTEGRITY_ERROR {
		t.Fatalf("got %v wanted INTEGRITY_ERROR", tabletErr.ErrorCode)
	}
	if tabletErr.Message != string(buf[:maxErrLen]) {
		t.Fatalf("message should be capped, only %d character will be shown", maxErrLen)
	}
}

func TestTabletErrorConnError(t *testing.T) {
	tabletErr := NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(1999, "HY000", "test"))
	if IsConnErr(tabletErr) {
		t.Fatalf("table error: %v is not a connection error", tabletErr)
	}
	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(2000, "HY000", "test"))
	if !IsConnErr(tabletErr) {
		t.Fatalf("table error: %v is a connection error", tabletErr)
	}
	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(mysql.ErrServerLost, "HY000", "test"))
	if IsConnErr(tabletErr) {
		t.Fatalf("table error: %v is not a connection error", tabletErr)
	}
	want := "fatal: the query was killed either because it timed out or was canceled: test (errno 2013) (sqlstate HY000)"
	if tabletErr.Error() != want {
		t.Fatalf("tablet error: %v, want %s", tabletErr, want)
	}
	sqlErr := sqldb.NewSQLError(1998, "HY000", "test")
	if IsConnErr(sqlErr) {
		t.Fatalf("sql error: %v is not a connection error", sqlErr)
	}
	sqlErr = sqldb.NewSQLError(2001, "HY000", "test")
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
	tabletErr := NewTabletErrorSQL(vtrpcpb.ErrorCode_QUERY_NOT_SERVED, sqldb.NewSQLError(2000, "HY000", "test"))
	if tabletErr.Prefix() != "retry: " {
		t.Fatalf("tablet error with error code: QUERY_NOT_SERVED should has prefix: 'retry: '")
	}
	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(2000, "HY000", "test"))
	if tabletErr.Prefix() != "fatal: " {
		t.Fatalf("tablet error with error code: INTERNAL_ERROR should has prefix: 'fatal: '")
	}
	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED, sqldb.NewSQLError(2000, "HY000", "test"))
	if tabletErr.Prefix() != "tx_pool_full: " {
		t.Fatalf("tablet error with error code: RESOURCE_EXHAUSTED should has prefix: 'tx_pool_full: '")
	}
	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_NOT_IN_TX, sqldb.NewSQLError(2000, "HY000", "test"))
	if tabletErr.Prefix() != "not_in_tx: " {
		t.Fatalf("tablet error with error code: NOT_IN_TX should has prefix: 'not_in_tx: '")
	}
}

func TestTabletErrorRecordStats(t *testing.T) {
	tabletErr := NewTabletErrorSQL(vtrpcpb.ErrorCode_QUERY_NOT_SERVED, sqldb.NewSQLError(2000, "HY000", "test"))
	queryServiceStats := NewQueryServiceStats("", false)
	retryCounterBefore := queryServiceStats.InfoErrors.Counts()["Retry"]
	tabletErr.RecordStats(queryServiceStats)
	retryCounterAfter := queryServiceStats.InfoErrors.Counts()["Retry"]
	if retryCounterAfter-retryCounterBefore != 1 {
		t.Fatalf("tablet error with error code QUERY_NOT_SERVED should increase Retry error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(2000, "HY000", "test"))
	fatalCounterBefore := queryServiceStats.InfoErrors.Counts()["Fatal"]
	tabletErr.RecordStats(queryServiceStats)
	fatalCounterAfter := queryServiceStats.InfoErrors.Counts()["Fatal"]
	if fatalCounterAfter-fatalCounterBefore != 1 {
		t.Fatalf("tablet error with error code INTERNAL_ERROR should increase Fatal error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED, sqldb.NewSQLError(2000, "HY000", "test"))
	txPoolFullCounterBefore := queryServiceStats.ErrorStats.Counts()["TxPoolFull"]
	tabletErr.RecordStats(queryServiceStats)
	txPoolFullCounterAfter := queryServiceStats.ErrorStats.Counts()["TxPoolFull"]
	if txPoolFullCounterAfter-txPoolFullCounterBefore != 1 {
		t.Fatalf("tablet error with error code RESOURCE_EXHAUSTED should increase TxPoolFull error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_NOT_IN_TX, sqldb.NewSQLError(2000, "HY000", "test"))
	notInTxCounterBefore := queryServiceStats.ErrorStats.Counts()["NotInTx"]
	tabletErr.RecordStats(queryServiceStats)
	notInTxCounterAfter := queryServiceStats.ErrorStats.Counts()["NotInTx"]
	if notInTxCounterAfter-notInTxCounterBefore != 1 {
		t.Fatalf("tablet error with error code NOT_IN_TX should increase NotInTx error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, sqldb.NewSQLError(mysql.ErrDupEntry, "23000", "test"))
	dupKeyCounterBefore := queryServiceStats.InfoErrors.Counts()["DupKey"]
	tabletErr.RecordStats(queryServiceStats)
	dupKeyCounterAfter := queryServiceStats.InfoErrors.Counts()["DupKey"]
	if dupKeyCounterAfter-dupKeyCounterBefore != 1 {
		t.Fatalf("sql error with SQL error mysql.ErrDupEntry should increase DupKey error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, sqldb.NewSQLError(mysql.ErrLockWaitTimeout, "HY000", "test"))
	lockWaitTimeoutCounterBefore := queryServiceStats.ErrorStats.Counts()["Deadlock"]
	tabletErr.RecordStats(queryServiceStats)
	lockWaitTimeoutCounterAfter := queryServiceStats.ErrorStats.Counts()["Deadlock"]
	if lockWaitTimeoutCounterAfter-lockWaitTimeoutCounterBefore != 1 {
		t.Fatalf("sql error with SQL error mysql.ErrLockWaitTimeout should increase Deadlock error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, sqldb.NewSQLError(mysql.ErrLockDeadlock, "40001", "test"))
	deadlockCounterBefore := queryServiceStats.ErrorStats.Counts()["Deadlock"]
	tabletErr.RecordStats(queryServiceStats)
	deadlockCounterAfter := queryServiceStats.ErrorStats.Counts()["Deadlock"]
	if deadlockCounterAfter-deadlockCounterBefore != 1 {
		t.Fatalf("sql error with SQL error mysql.ErrLockDeadlock should increase Deadlock error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, sqldb.NewSQLError(mysql.ErrOptionPreventsStatement, "HY000", "test"))
	failCounterBefore := queryServiceStats.ErrorStats.Counts()["Fail"]
	tabletErr.RecordStats(queryServiceStats)
	failCounterAfter := queryServiceStats.ErrorStats.Counts()["Fail"]
	if failCounterAfter-failCounterBefore != 1 {
		t.Fatalf("sql error with SQL error mysql.ErrOptionPreventsStatement should increase Fail error count by 1")
	}
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
	tabletErr := NewTabletErrorSQL(vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED, sqldb.NewSQLError(1000, "HY000", "test"))
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
	tabletErr := NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(1000, "HY000", "test"))
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
