// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
)

// This file uses the sandbox_test framework.

func TestVTConnExecute(t *testing.T) {
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	testVTConnGeneric(t, func(shards []string) (*mproto.QueryResult, error) {
		vtc := NewVTConn(blm, "sandbox", "", 1*time.Millisecond, 3)
		return vtc.Execute("query", nil, "", shards)
	})
}

func TestVTConnStreamExecute(t *testing.T) {
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	testVTConnGeneric(t, func(shards []string) (*mproto.QueryResult, error) {
		vtc := NewVTConn(blm, "sandbox", "", 1*time.Millisecond, 3)
		qr := new(mproto.QueryResult)
		err := vtc.StreamExecute("query", nil, "", shards, func(r interface{}) error {
			appendResult(qr, r.(*mproto.QueryResult))
			return nil
		})
		return qr, err
	})
}

func testVTConnGeneric(t *testing.T, f func(shards []string) (*mproto.QueryResult, error)) {
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

func TestVTConnExecuteTxTimeout(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc0 := &sandboxConn{}
	testConns["0:1"] = sbc0
	sbc1 := &sandboxConn{mustFailTxPool: 1}
	testConns["1:1"] = sbc1
	vtc := NewVTConn(blm, "sandbox", "", 1*time.Millisecond, 3)

	for i := 0; i < 2; i++ {
		vtc.Begin()
		vtc.Execute("query", nil, "", []string{"0"})
		// Shard 0 must be in transaction
		if sbc0.TransactionId() == 0 {
			t.Errorf("want non-zer, got 0")
		}
		if len(vtc.commitOrder) != 1 {
			t.Errorf("want 2, got %d", len(vtc.commitOrder))
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
		_, err := vtc.Execute("query1", nil, "", []string{"1"})
		// All transactions must be rolled back.
		if err == nil || err.Error() != want {
			t.Errorf("want %s, got %v", want, err)
		}
		if sbc0.TransactionId() != 0 {
			t.Errorf("want 0, got %v", sbc0.TransactionId())
		}
		if len(vtc.commitOrder) != 0 {
			t.Errorf("want 0, got %d", len(vtc.commitOrder))
		}
	}
}

func TestVTConnExecuteTxChange(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	vtc := NewVTConn(blm, "sandbox", "", 1*time.Millisecond, 3)
	vtc.Begin()
	vtc.Execute("query", nil, "", []string{"0"})
	sbc.Rollback()
	sbc.Begin()
	_, err := vtc.Execute("query", nil, "", []string{"0"})
	want := "not_in_tx: connection is in a different transaction, shard: (.0.), address: 0:1"
	// Ensure that we detect the case where the underlying
	// connection is in a different
	// transaction than the one we started.
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}

func TestVTConnExecuteUnexpectedTx(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	vtc := NewVTConn(blm, "sandbox", "", 1*time.Millisecond, 3)
	// This call is to make sure shard_conn points to a real connection.
	vtc.Execute("query", nil, "", []string{"0"})

	sbc.Begin()
	_, err := vtc.Execute("query", nil, "", []string{"0"})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// Ensure that rollback was called on the underlying connection.
	if sbc.RollbackCount != 1 {
		t.Errorf("want 1, got %d", sbc.RollbackCount)
	}
}

func TestVTConnStreamExecuteTx(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	vtc := NewVTConn(blm, "sandbox", "", 1*time.Millisecond, 3)
	vtc.Begin()
	err := vtc.StreamExecute("query", nil, "", []string{"0"}, func(interface{}) error {
		return nil
	})
	// No support for streaming in a transaction.
	want := "cannot stream in a transaction"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}

func TestVTConnStreamExecuteSendError(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	vtc := NewVTConn(blm, "sandbox", "", 1*time.Millisecond, 3)
	err := vtc.StreamExecute("query", nil, "", []string{"0"}, func(interface{}) error {
		return fmt.Errorf("send error")
	})
	want := "send error"
	// Ensure that we handle send errors.
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}

func TestVTConnBeginFail(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	vtc := NewVTConn(blm, "sandbox", "", 1*time.Millisecond, 3)
	vtc.Begin()
	err := vtc.Begin()
	// Disallow Begin if we're already in a transaction.
	want := "cannot begin: already in a transaction"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}

func TestVTConnCommitSuccess(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc0 := &sandboxConn{}
	testConns["0:1"] = sbc0
	sbc1 := &sandboxConn{mustFailTxPool: 1}
	testConns["1:1"] = sbc1
	vtc := NewVTConn(blm, "sandbox", "", 1*time.Millisecond, 3)

	vtc.Begin()
	// Sequence the executes to ensure commit order
	vtc.Execute("query1", nil, "", []string{"0"})
	vtc.Execute("query1", nil, "", []string{"1"})
	sbc0.mustFailServer = 1
	vtc.Commit()
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

func TestVTConnBeginRetry(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc0 := &sandboxConn{}
	testConns["0:1"] = sbc0
	sbc1 := &sandboxConn{mustFailTxPool: 1}
	testConns["1:1"] = sbc1
	vtc := NewVTConn(blm, "sandbox", "", 1*time.Millisecond, 3)

	vtc.Begin()
	vtc.Execute("query1", nil, "", []string{"0", "1"})
	if sbc0.TransactionId() == 0 {
		t.Errorf("want non-zero, got 0")
	}
	// sbc1 should still be in a transaction because
	// tx_pool_full should cause a transparent retry
	// without aborting the transaction.
	if sbc1.TransactionId() == 0 {
		t.Errorf("want non-zero, got 0")
	}
	vtc.Commit()
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

func TestVTConnClose(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	vtc := NewVTConn(blm, "sandbox", "", 1*time.Millisecond, 3)
	vtc.Execute("query1", nil, "", []string{"0"})
	vtc.Close()
	if sbc.CloseCount != 1 {
		t.Errorf("want 1, got %d", sbc.CommitCount)
	}
}
