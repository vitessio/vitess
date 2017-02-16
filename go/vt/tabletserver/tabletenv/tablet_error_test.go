// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletenv

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/mysqlconn"
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
	sqlErr := sqldb.NewSQLError(mysqlconn.EROptionPreventsStatement, mysqlconn.SSUnknownSQLState, "read-only")
	tabletErr := NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqlErr)
	if tabletErr.ErrorCode != vtrpcpb.ErrorCode_QUERY_NOT_SERVED {
		t.Fatalf("got: %v wanted: QUERY_NOT_SERVED", tabletErr.ErrorCode)
	}

	sqlErr = sqldb.NewSQLError(mysqlconn.ERDupEntry, mysqlconn.SSDupKey, "error")
	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqlErr)
	if tabletErr.ErrorCode != vtrpcpb.ErrorCode_INTEGRITY_ERROR {
		t.Fatalf("got: %v wanted: INTEGRITY_ERROR", tabletErr.ErrorCode)
	}

	sqlErr = sqldb.NewSQLError(mysqlconn.ERDataTooLong, mysqlconn.SSDataTooLong, "error")
	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqlErr)
	if tabletErr.ErrorCode != vtrpcpb.ErrorCode_BAD_INPUT {
		t.Fatalf("got: %v wanted: BAD_INPUT", tabletErr.ErrorCode)
	}

	sqlErr = sqldb.NewSQLError(mysqlconn.ERDataOutOfRange, mysqlconn.SSDataOutOfRange, "error")
	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqlErr)
	if tabletErr.ErrorCode != vtrpcpb.ErrorCode_BAD_INPUT {
		t.Fatalf("got: %v wanted: BAD_INPUT", tabletErr.ErrorCode)
	}
}

func TestTabletErrorRetriableErrorTypeOverwrite2(t *testing.T) {
}

func TestTabletErrorMsgTooLong(t *testing.T) {
	buf := make([]byte, 2*maxErrLen)
	for i := 0; i < 2*maxErrLen; i++ {
		buf[i] = 'a'
	}
	msg := string(buf)
	sqlErr := sqldb.NewSQLError(mysqlconn.ERDupEntry, mysqlconn.SSDupKey, msg)
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
		t.Fatalf("tablet error: %v is not a connection error", tabletErr)
	}
	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(2000, mysqlconn.SSUnknownSQLState, "test"))
	if !IsConnErr(tabletErr) {
		t.Fatalf("tablet error: %v is a connection error", tabletErr)
	}
	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(mysqlconn.CRServerLost, mysqlconn.SSUnknownSQLState, "test"))
	if IsConnErr(tabletErr) {
		t.Fatalf("tablet error: %v is not a connection error", tabletErr)
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
	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED_LEGACY, sqldb.NewSQLError(2000, "HY000", "test"))
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
	retryCounterBefore := InfoErrors.Counts()["Retry"]
	tabletErr.RecordStats()
	retryCounterAfter := InfoErrors.Counts()["Retry"]
	if retryCounterAfter-retryCounterBefore != 1 {
		t.Fatalf("tablet error with error code QUERY_NOT_SERVED should increase Retry error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(2000, "HY000", "test"))
	fatalCounterBefore := ErrorStats.Counts()["Fatal"]
	tabletErr.RecordStats()
	fatalCounterAfter := ErrorStats.Counts()["Fatal"]
	if fatalCounterAfter-fatalCounterBefore != 1 {
		t.Fatalf("tablet error with error code INTERNAL_ERROR should increase Fatal error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED_LEGACY, sqldb.NewSQLError(2000, "HY000", "test"))
	txPoolFullCounterBefore := ErrorStats.Counts()["TxPoolFull"]
	tabletErr.RecordStats()
	txPoolFullCounterAfter := ErrorStats.Counts()["TxPoolFull"]
	if txPoolFullCounterAfter-txPoolFullCounterBefore != 1 {
		t.Fatalf("tablet error with error code RESOURCE_EXHAUSTED should increase TxPoolFull error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_NOT_IN_TX, sqldb.NewSQLError(2000, "HY000", "test"))
	notInTxCounterBefore := ErrorStats.Counts()["NotInTx"]
	tabletErr.RecordStats()
	notInTxCounterAfter := ErrorStats.Counts()["NotInTx"]
	if notInTxCounterAfter-notInTxCounterBefore != 1 {
		t.Fatalf("tablet error with error code NOT_IN_TX should increase NotInTx error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, sqldb.NewSQLError(mysqlconn.ERDupEntry, mysqlconn.SSDupKey, "test"))
	dupKeyCounterBefore := InfoErrors.Counts()["DupKey"]
	tabletErr.RecordStats()
	dupKeyCounterAfter := InfoErrors.Counts()["DupKey"]
	if dupKeyCounterAfter-dupKeyCounterBefore != 1 {
		t.Fatalf("sql error with SQL error mysqlconn.ERDupEntry should increase DupKey error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, sqldb.NewSQLError(mysqlconn.ERLockWaitTimeout, mysqlconn.SSUnknownSQLState, "test"))
	lockWaitTimeoutCounterBefore := ErrorStats.Counts()["Deadlock"]
	tabletErr.RecordStats()
	lockWaitTimeoutCounterAfter := ErrorStats.Counts()["Deadlock"]
	if lockWaitTimeoutCounterAfter-lockWaitTimeoutCounterBefore != 1 {
		t.Fatalf("sql error with SQL error mysqlconn.ERLockWaitTimeout should increase Deadlock error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, sqldb.NewSQLError(mysqlconn.ERLockDeadlock, mysqlconn.SSLockDeadlock, "test"))
	deadlockCounterBefore := ErrorStats.Counts()["Deadlock"]
	tabletErr.RecordStats()
	deadlockCounterAfter := ErrorStats.Counts()["Deadlock"]
	if deadlockCounterAfter-deadlockCounterBefore != 1 {
		t.Fatalf("sql error with SQL error mysqlconn.ERLockDeadlock should increase Deadlock error count by 1")
	}

	tabletErr = NewTabletErrorSQL(vtrpcpb.ErrorCode_UNKNOWN_ERROR, sqldb.NewSQLError(mysqlconn.EROptionPreventsStatement, mysqlconn.SSUnknownSQLState, "test"))
	failCounterBefore := ErrorStats.Counts()["Fail"]
	tabletErr.RecordStats()
	failCounterAfter := ErrorStats.Counts()["Fail"]
	if failCounterAfter-failCounterBefore != 1 {
		t.Fatalf("sql error with SQL error mysqlconn.EROptionPreventsStatement should increase Fail error count by 1")
	}
}

func TestTabletErrorLogUncaughtErr(t *testing.T) {
	panicCountBefore := InternalErrors.Counts()["Panic"]
	defer func() {
		panicCountAfter := InternalErrors.Counts()["Panic"]
		if panicCountAfter-panicCountBefore != 1 {
			t.Fatalf("Panic count should increase by 1 for uncaught panic")
		}
	}()
	defer LogError()
	panic("unknown error")
}

func TestTabletErrorTxPoolFull(t *testing.T) {
	tabletErr := NewTabletErrorSQL(vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED_LEGACY, sqldb.NewSQLError(1000, "HY000", "test"))
	defer func() {
		err := recover()
		if err != nil {
			t.Fatalf("error should have been handled already")
		}
	}()
	defer LogError()
	panic(tabletErr)
}

func TestTabletErrorFatal(t *testing.T) {
	tabletErr := NewTabletErrorSQL(vtrpcpb.ErrorCode_INTERNAL_ERROR, sqldb.NewSQLError(1000, "HY000", "test"))
	defer func() {
		err := recover()
		if err != nil {
			t.Fatalf("error should have been handled already")
		}
	}()
	defer LogError()
	panic(tabletErr)
}
