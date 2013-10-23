// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"testing"
	"time"

	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
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

func TestShardConnExecuteBatch(t *testing.T) {
	blm := NewBalancerMap(new(sandboxTopo), "aa", "vt")
	testShardConnGeneric(t, func() error {
		sdc := NewShardConn(blm, "sandbox", "", "0", "", 1*time.Millisecond, 3)
		queries := []tproto.BoundQuery{{"query", nil}}
		_, err := sdc.ExecuteBatch(queries)
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
	want := "topo error, shard: (.0.), address: "
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	if endPointCounter != 1 {
		t.Errorf("want 1, got %v", endPointCounter)
	}

	// Connect failure
	resetSandbox()
	dialMustFail = 3
	err = f()
	want = "conn error, shard: (.0.), address: "
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure we dialed 3 times before failing.
	if dialCounter != 3 {
		t.Errorf("want 3, got %v", dialCounter)
	}

	// retry error (multiple failure)
	resetSandbox()
	sbc := &sandboxConn{mustFailRetry: 3}
	testConns["0:1"] = sbc
	err = f()
	want = "retry: err, shard: (.0.), address: "
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure we dialed 3 times before failing.
	if dialCounter != 3 {
		t.Errorf("want 3, got %v", dialCounter)
	}
	// Ensure we executed 3 times before failing.
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
	// Ensure we dialed twice (second one succeeded)
	if dialCounter != 2 {
		t.Errorf("want 2, got %v", dialCounter)
	}
	// Ensure we executed twice (second one succeeded)
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
	// Ensure we dialed twice (second one succeeded)
	if dialCounter != 2 {
		t.Errorf("want 2, got %v", dialCounter)
	}
	// Ensure we executed twice (second one succeeded)
	if sbc.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc.ExecCount)
	}

	// server error
	resetSandbox()
	sbc = &sandboxConn{mustFailServer: 1}
	testConns["0:1"] = sbc
	err = f()
	want = "error: err, shard: (.0.), address: 0:1"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure we did not redial.
	if dialCounter != 1 {
		t.Errorf("want 1, got %v", dialCounter)
	}
	// Ensure we did not re-execute.
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
	// Ensure we dialed twice (second one succeeded)
	if dialCounter != 2 {
		t.Errorf("want 2, got %v", dialCounter)
	}
	// Ensure we executed twice (second one succeeded)
	if sbc.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc.ExecCount)
	}

	// conn error (in transaction)
	resetSandbox()
	sbc = &sandboxConn{mustFailConn: 1, transactionId: 1}
	testConns["0:1"] = sbc
	err = f()
	want = "error: conn, shard: (.0.), address: "
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure we did not redial.
	if dialCounter != 1 {
		t.Errorf("want 1, got %v", dialCounter)
	}
	// One rollback followed by execution.
	if sbc.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc.ExecCount)
	}
	// Ensure one of those ExecCounts was a Rollback
	if sbc.RollbackCount != 1 {
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
	testConns["0:1"] = &sandboxConn{transactionId: 1}
	// call Execute to cause connection to be opened
	sdc.Execute("query", nil)
	err := sdc.Begin()
	// Begin should not be allowed if already in a transaction.
	want := "cannot begin: already in transaction, shard: (.0.), address: 0:1"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}

	// tx_pool_full
	resetSandbox()
	sbc := &sandboxConn{mustFailTxPool: 1}
	testConns["0:1"] = sbc
	sdc = NewShardConn(blm, "sandbox", "", "0", "", 10*time.Millisecond, 3)
	startTime := time.Now()
	err = sdc.Begin()
	// If transaction pool is full, Begin should wait and retry.
	if time.Now().Sub(startTime) < (10 * time.Millisecond) {
		t.Errorf("want >10ms, got %v", time.Now().Sub(startTime))
	}
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// There should have been no redial.
	if dialCounter != 1 {
		t.Errorf("want 1, got %v", dialCounter)
	}
	// Account for 2 calls to Begin.
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
	// Commit should fail if we're not in a transaction.
	want := "cannot commit: not in transaction, shard: (.0.), address: 0:1"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}

	// valid commit
	testConns["0:1"] = &sandboxConn{transactionId: 1}
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
	sbc.transactionId = 1
	err = sdc.Commit()
	// Commit should fail if server returned an error.
	want = "error: err, shard: (.0.), address: 0:1"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
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
	// Rollback should fail if we're not in a transaction.
	want := "cannot rollback: not in transaction, shard: (.0.), address: 0:1"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}

	// valid rollback
	testConns["0:1"] = &sandboxConn{transactionId: 1}
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
	sbc.transactionId = 1
	err = sdc.Rollback()
	want = "error: err, shard: (.0.), address: 0:1"
	// Rollback should fail if server returned an error.
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
}
