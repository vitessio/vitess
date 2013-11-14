// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
)

// This file uses the sandbox_test framework.

func TestScatterConnExecute(t *testing.T) {
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	testScatterConnGeneric(t, func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(blm, "", 1*time.Millisecond, 3)
		return stc.Execute("query", nil, "", shards)
	})
}

func TestScatterConnExecuteBatch(t *testing.T) {
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	testScatterConnGeneric(t, func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(blm, "", 1*time.Millisecond, 3)
		queries := []tproto.BoundQuery{{"query", nil}}
		qrs, err := stc.ExecuteBatch(queries, "", shards)
		if err != nil {
			return nil, err
		}
		return &qrs.List[0], err
	})
}

func TestScatterConnStreamExecute(t *testing.T) {
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	testScatterConnGeneric(t, func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(blm, "", 1*time.Millisecond, 3)
		qr := new(mproto.QueryResult)
		err := stc.StreamExecute("query", nil, "", shards, func(r interface{}) error {
			appendResult(qr, r.(*mproto.QueryResult))
			return nil
		})
		return qr, err
	})
}

func testScatterConnGeneric(t *testing.T, f func(shards []string) (*mproto.QueryResult, error)) {
	// no shard
	resetSandbox()
	qr, err := f(nil)
	if qr.RowsAffected != 0 {
		t.Errorf("want 0, got %v", qr.RowsAffected)
	}
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	// single shard
	resetSandbox()
	sbc := &sandboxConn{mustFailServer: 1}
	testConns["0:1"] = sbc
	qr, err = f([]string{"0"})
	want := "error: err, shard: (.0.), address: 0:1"
	// Verify server error string.
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure that we tried only once.
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}

	// two shards
	resetSandbox()
	sbc0 := &sandboxConn{mustFailServer: 1}
	testConns["0:1"] = sbc0
	sbc1 := &sandboxConn{mustFailServer: 1}
	testConns["1:1"] = sbc1
	_, err = f([]string{"0", "1"})
	// Verify server errors are consolidated.
	want = "error: err, shard: (.0.), address: 0:1\nerror: err, shard: (.1.), address: 1:1"
	if err == nil || err.Error() != want {
		t.Errorf("\nwant\n%s\ngot\n%v", want, err)
	}
	// Ensure that we tried only once.
	if sbc0.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc0.ExecCount)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc1.ExecCount)
	}

	// duplicate shards
	resetSandbox()
	sbc = &sandboxConn{}
	testConns["0:1"] = sbc
	qr, err = f([]string{"0", "0"})
	// Ensure that we executed only once.
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}

	// no errors
	resetSandbox()
	sbc0 = &sandboxConn{}
	testConns["0:1"] = sbc0
	sbc1 = &sandboxConn{}
	testConns["1:1"] = sbc1
	qr, err = f([]string{"0", "1"})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if sbc0.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc0.ExecCount)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc1.ExecCount)
	}
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
	if len(qr.Rows) != 2 {
		t.Errorf("want 2, got %v", len(qr.Rows))
	}
}

func TestScatterConnExecuteTxTimeout(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc0 := &sandboxConn{}
	testConns["0:1"] = sbc0
	sbc1 := &sandboxConn{mustFailTxPool: 1}
	testConns["1:1"] = sbc1
	stc := NewScatterConn(blm, "", 1*time.Millisecond, 3)

	for i := 0; i < 2; i++ {
		stc.Begin()
		stc.Execute("query", nil, "", []string{"0"})
		// Shard 0 must be in transaction
		if sbc0.TransactionId() == 0 {
			t.Errorf("want non-zero, got 0")
		}
		if len(stc.commitOrder) != 1 {
			t.Errorf("want 2, got %d", len(stc.commitOrder))
		}

		var want string
		switch i {
		case 0:
			sbc1.mustFailTxPool = 3
			want = "tx_pool_full: err, shard: (.1.), address: 1:1"
		case 1:
			sbc1.mustFailNotTx = 1
			want = "not_in_tx: err, shard: (.1.), address: 1:1"
		}
		_, err := stc.Execute("query1", nil, "", []string{"1"})
		// All transactions must be rolled back.
		if err == nil || err.Error() != want {
			t.Errorf("want %s, got %v", want, err)
		}
		if sbc0.TransactionId() != 0 {
			t.Errorf("want 0, got %v", sbc0.TransactionId())
		}
		if len(stc.commitOrder) != 0 {
			t.Errorf("want 0, got %d", len(stc.commitOrder))
		}
	}
}

func TestScatterConnExecuteTxChange(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	stc := NewScatterConn(blm, "", 1*time.Millisecond, 3)
	stc.Begin()
	stc.Execute("query", nil, "", []string{"0"})
	sbc.Rollback()
	sbc.Begin()
	_, err := stc.Execute("query", nil, "", []string{"0"})
	want := "not_in_tx: connection is in a different transaction, shard: (.0.), address: 0:1"
	// Ensure that we detect the case where the underlying
	// connection is in a different
	// transaction than the one we started.
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}

func TestScatterConnExecuteUnexpectedTx(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	stc := NewScatterConn(blm, "", 1*time.Millisecond, 3)
	// This call is to make sure shard_conn points to a real connection.
	stc.Execute("query", nil, "", []string{"0"})

	sbc.Begin()
	_, err := stc.Execute("query", nil, "", []string{"0"})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// Ensure that rollback was called on the underlying connection.
	if sbc.RollbackCount != 1 {
		t.Errorf("want 1, got %d", sbc.RollbackCount)
	}
}

func TestScatterConnStreamExecuteTx(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	stc := NewScatterConn(blm, "", 1*time.Millisecond, 3)
	stc.Begin()
	err := stc.StreamExecute("query", nil, "", []string{"0"}, func(interface{}) error {
		return nil
	})
	// No support for streaming in a transaction.
	want := "cannot stream in a transaction"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}

func TestScatterConnStreamExecuteSendError(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	stc := NewScatterConn(blm, "", 1*time.Millisecond, 3)
	err := stc.StreamExecute("query", nil, "", []string{"0"}, func(interface{}) error {
		return fmt.Errorf("send error")
	})
	want := "send error"
	// Ensure that we handle send errors.
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}

func TestScatterConnBeginFail(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	stc := NewScatterConn(blm, "", 1*time.Millisecond, 3)
	stc.Begin()
	err := stc.Begin()
	// Disallow Begin if we're already in a transaction.
	want := "cannot begin: already in a transaction"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}

func TestScatterConnCommitSuccess(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc0 := &sandboxConn{}
	testConns["0:1"] = sbc0
	sbc1 := &sandboxConn{mustFailTxPool: 1}
	testConns["1:1"] = sbc1
	stc := NewScatterConn(blm, "", 1*time.Millisecond, 3)

	stc.Begin()
	// Sequence the executes to ensure commit order
	stc.Execute("query1", nil, "", []string{"0"})
	stc.Execute("query1", nil, "", []string{"1"})
	sbc0.mustFailServer = 1
	stc.Commit()
	if sbc0.TransactionId() != 0 {
		t.Errorf("want 0, got %d", sbc0.TransactionId())
	}
	if sbc1.TransactionId() != 0 {
		t.Errorf("want 0, got %d", sbc1.TransactionId())
	}
	if sbc0.CommitCount != 1 {
		t.Errorf("want 1, got %d", sbc0.CommitCount)
	}
	if sbc1.RollbackCount != 1 {
		t.Errorf("want 1, got %d", sbc1.RollbackCount)
	}
}

func TestScatterConnBeginRetry(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc0 := &sandboxConn{}
	testConns["0:1"] = sbc0
	sbc1 := &sandboxConn{mustFailTxPool: 1}
	testConns["1:1"] = sbc1
	stc := NewScatterConn(blm, "", 1*time.Millisecond, 3)

	stc.Begin()
	stc.Execute("query1", nil, "", []string{"0", "1"})
	if sbc0.TransactionId() == 0 {
		t.Errorf("want non-zero, got 0")
	}
	// sbc1 should still be in a transaction because
	// tx_pool_full should cause a transparent retry
	// without aborting the transaction.
	if sbc1.TransactionId() == 0 {
		t.Errorf("want non-zero, got 0")
	}
	stc.Commit()
	if sbc0.TransactionId() != 0 {
		t.Errorf("want 0, got %d", sbc0.TransactionId())
	}
	if sbc1.TransactionId() != 0 {
		t.Errorf("want 0, got %d", sbc1.TransactionId())
	}
	if sbc0.CommitCount != 1 {
		t.Errorf("want 1, got %d", sbc0.CommitCount)
	}
	if sbc1.CommitCount != 1 {
		t.Errorf("want 1, got %d", sbc1.CommitCount)
	}
}

func TestScatterConnClose(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	stc := NewScatterConn(blm, "", 1*time.Millisecond, 3)
	stc.Execute("query1", nil, "", []string{"0"})
	stc.Close()
	if sbc.CloseCount != 1 {
		t.Errorf("want 1, got %d", sbc.CommitCount)
	}
}
