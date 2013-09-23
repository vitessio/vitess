// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
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
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
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
	want = "error: err, shard: (.0.), address: 0:1\nerror: err, shard: (.1.), address: 1:1"
	if err == nil || err.Error() != want {
		t.Errorf("\nwant\n%s\ngot\n%v", want, err)
	}
	if sbc0.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc0.ExecCount)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc1.ExecCount)
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
}

func TestVTConnExecuteTx(t *testing.T) {
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sbc0 := &sandboxConn{}
	testConns["0:1"] = sbc0
	sbc1 := &sandboxConn{mustFailTxPool: 1}
	testConns["1:1"] = sbc1
	vtc := NewVTConn(blm, "sandbox", "", 1*time.Millisecond, 3)
	// Instantiate actual connections by executing a query
	vtc.Execute("query", nil, "", []string{"0", "1"})

	for i := 0; i < 2; i++ {
		vtc.Begin()
		vtc.Execute("query", nil, "", []string{"0"})
		// Shard 0 must be in transaction
		if sbc0.TransactionId() == 0 {
			t.Errorf("want non-zer, got 0")
		}
		if len(vtc.transactionConns) != 1 {
			t.Errorf("want 2, got %d", len(vtc.transactionConns))
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
		// all transactions must be rolled back
		if err == nil || err.Error() != want {
			t.Errorf("want %s, got %v", want, err)
		}
		if sbc0.TransactionId() != 0 {
			t.Errorf("want 0, got %v", sbc0.TransactionId())
		}
		if len(vtc.transactionConns) != 0 {
			t.Errorf("want 0, got %d", len(vtc.transactionConns))
		}
	}
}
