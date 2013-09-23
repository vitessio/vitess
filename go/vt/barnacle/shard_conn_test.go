// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package barnacle

import (
	"testing"
	"time"
)

// This file uses the sandbox_test framework.

func TestShardConnExecute(t *testing.T) {
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	testShardConnGeneric(t, func() error {
		sdc := NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
		_, err := sdc.Execute("query", nil)
		return err
	})
}

func TestShardConnExecuteStream(t *testing.T) {
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	testShardConnGeneric(t, func() error {
		sdc := NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
		_, errfunc := sdc.StreamExecute("query", nil)
		return errfunc()
	})
}

func TestShardConnBegin(t *testing.T) {
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	testShardConnGeneric(t, func() error {
		sdc := NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
		return sdc.Begin()
	})
}

func testShardConnGeneric(t *testing.T, f func() error) {
	// Topo failure
	resetSandbox()
	endPointMustFail = 1
	err := f()
	if err == nil || err.Error() != "topo error" {
		t.Errorf("want topo error, got %v", err)
	}
	if endPointCounter != 1 {
		t.Errorf("want 1, got %v", endPointCounter)
	}

	// Connect failure
	resetSandbox()
	dialMustFail = 3
	err = f()
	if err == nil || err.Error() != "conn error" {
		t.Errorf("want conn error, got %v", err)
	}
	if dialCounter != 3 {
		t.Errorf("want 3, got %v", dialCounter)
	}

	// retry error (multiple failure)
	resetSandbox()
	sbc := &sandboxConn{mustFailRetry: 3}
	testConns["0:1"] = sbc
	err = f()
	if err == nil || err.Error() != "retry: err" {
		t.Errorf("want retry: errnil, got %v", err)
	}
	if dialCounter != 3 {
		t.Errorf("want 3, got %v", dialCounter)
	}
	if sbc.ExecCount != 3 {
		t.Errorf("want 3, got %v", sbc.ExecCount)
	}

	// retry error (one failure)
	resetSandbox()
	sbc = &sandboxConn{mustFailRetry: 1}
	testConns["0:1"] = sbc
	err = f()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if dialCounter != 2 {
		t.Errorf("want 2, got %v", dialCounter)
	}
	if sbc.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc.ExecCount)
	}

	// fatal error (one failure)
	resetSandbox()
	sbc = &sandboxConn{mustFailRetry: 1}
	testConns["0:1"] = sbc
	err = f()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if dialCounter != 2 {
		t.Errorf("want 2, got %v", dialCounter)
	}
	if sbc.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc.ExecCount)
	}

	// server error
	resetSandbox()
	sbc = &sandboxConn{mustFailServer: 1}
	testConns["0:1"] = sbc
	err = f()
	if err == nil || err.Error() != "error: err" {
		t.Errorf("want error: err, got %v", err)
	}
	if dialCounter != 1 {
		t.Errorf("want 1, got %v", dialCounter)
	}
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}

	// conn error (one failure)
	resetSandbox()
	sbc = &sandboxConn{mustFailConn: 1}
	testConns["0:1"] = sbc
	err = f()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if dialCounter != 2 {
		t.Errorf("want 2, got %v", dialCounter)
	}
	if sbc.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc.ExecCount)
	}

	// conn error (in transaction)
	resetSandbox()
	sbc = &sandboxConn{mustFailConn: 1, inTransaction: true}
	testConns["0:1"] = sbc
	err = f()
	if err == nil || err.Error() != "error: conn" {
		t.Errorf("want error: conn, got %v", err)
	}
	if dialCounter != 1 {
		t.Errorf("want 1, got %v", dialCounter)
	}
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}

	// no failures
	resetSandbox()
	sbc = &sandboxConn{}
	testConns["0:1"] = sbc
	err = f()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if dialCounter != 1 {
		t.Errorf("want 1, got %v", dialCounter)
	}
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}
}

func TestShardConnBeginOther(t *testing.T) {
	// already in transaction
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	sdc := NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
	testConns["0:1"] = &sandboxConn{inTransaction: true}
	// call Execute to cause connection to be opened
	sdc.Execute("query", nil)
	err := sdc.Begin()
	if err == nil || err.Error() != "cannot begin: already in transaction" {
		t.Errorf("want cannot begin: already in transaction, got %v", err)
	}

	// tx_pool_full
	resetSandbox()
	sbc := &sandboxConn{mustFailTxPool: 1}
	testConns["0:1"] = sbc
	sdc = NewShardConn(blm, "sandbox", "", "0", "", 10*time.Millisecond, 3)
	startTime := time.Now()
	err = sdc.Begin()
	if time.Now().Sub(startTime) < (10 * time.Millisecond) {
		t.Errorf("want >10ms, got %v", time.Now().Sub(startTime))
	}
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if dialCounter != 1 {
		t.Errorf("want 1, got %v", dialCounter)
	}
	if sbc.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc.ExecCount)
	}
}

func TestShardConnCommit(t *testing.T) {
	// not in transaction
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	testConns["0:1"] = &sandboxConn{}
	sdc := NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
	sdc.Execute("query", nil)
	err := sdc.Commit()
	if err == nil || err.Error() != "cannot commit: not in transaction" {
		t.Errorf("want cannot commit: not in transaction, got %v", err)
	}

	// valid commit
	testConns["0:1"] = &sandboxConn{inTransaction: true}
	sdc = NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
	sdc.Execute("query", nil)
	err = sdc.Commit()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	// commit fail
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	sdc = NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
	sdc.Execute("query", nil)
	sbc.mustFailServer = 1
	sbc.inTransaction = true
	err = sdc.Commit()
	if err == nil || err.Error() != "error: err" {
		t.Errorf("want error: err, got %v", err)
	}
}

func TestShardConnRollback(t *testing.T) {
	// not in transaction
	resetSandbox()
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	testConns["0:1"] = &sandboxConn{}
	sdc := NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
	sdc.Execute("query", nil)
	err := sdc.Rollback()
	if err == nil || err.Error() != "cannot rollback: not in transaction" {
		t.Errorf("want cannot rollback: not in transaction, got %v", err)
	}

	// valid rollback
	testConns["0:1"] = &sandboxConn{inTransaction: true}
	sdc = NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
	sdc.Execute("query", nil)
	err = sdc.Rollback()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	// rollback fail
	sbc := &sandboxConn{}
	testConns["0:1"] = sbc
	sdc = NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
	sdc.Execute("query", nil)
	sbc.mustFailServer = 1
	sbc.inTransaction = true
	err = sdc.Rollback()
	if err == nil || err.Error() != "error: err" {
		t.Errorf("want error: err, got %v", err)
	}
}
