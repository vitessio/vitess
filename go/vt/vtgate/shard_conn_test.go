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
	testShardConnGeneric(t, func() error {
		sdc := NewShardConn(new(sandboxTopo), "aa", "", "0", "", 1*time.Millisecond, 3)
		_, err := sdc.Execute(nil, "query", nil, 0)
		return err
	})
	testShardConnTransact(t, func() error {
		sdc := NewShardConn(new(sandboxTopo), "aa", "", "0", "", 1*time.Millisecond, 3)
		_, err := sdc.Execute(nil, "query", nil, 1)
		return err
	})
}

func TestShardConnExecuteBatch(t *testing.T) {
	testShardConnGeneric(t, func() error {
		sdc := NewShardConn(new(sandboxTopo), "aa", "", "0", "", 1*time.Millisecond, 3)
		queries := []tproto.BoundQuery{{"query", nil}}
		_, err := sdc.ExecuteBatch(nil, queries, 0)
		return err
	})
	testShardConnTransact(t, func() error {
		sdc := NewShardConn(new(sandboxTopo), "aa", "", "0", "", 1*time.Millisecond, 3)
		queries := []tproto.BoundQuery{{"query", nil}}
		_, err := sdc.ExecuteBatch(nil, queries, 1)
		return err
	})
}

func TestShardConnExecuteStream(t *testing.T) {
	testShardConnGeneric(t, func() error {
		sdc := NewShardConn(new(sandboxTopo), "aa", "", "0", "", 1*time.Millisecond, 3)
		_, errfunc := sdc.StreamExecute(nil, "query", nil, 0)
		return errfunc()
	})
	testShardConnTransact(t, func() error {
		sdc := NewShardConn(new(sandboxTopo), "aa", "", "0", "", 1*time.Millisecond, 3)
		_, errfunc := sdc.StreamExecute(nil, "query", nil, 1)
		return errfunc()
	})
}

func TestShardConnBegin(t *testing.T) {
	testShardConnGeneric(t, func() error {
		sdc := NewShardConn(new(sandboxTopo), "aa", "", "0", "", 1*time.Millisecond, 3)
		_, err := sdc.Begin(nil)
		return err
	})
}

func TestShardConnCommi(t *testing.T) {
	testShardConnTransact(t, func() error {
		sdc := NewShardConn(new(sandboxTopo), "aa", "", "0", "", 1*time.Millisecond, 3)
		return sdc.Commit(nil, 1)
	})
}

func TestShardConnRollback(t *testing.T) {
	testShardConnTransact(t, func() error {
		sdc := NewShardConn(new(sandboxTopo), "aa", "", "0", "", 1*time.Millisecond, 3)
		return sdc.Rollback(nil, 1)
	})
}

func testShardConnGeneric(t *testing.T, f func() error) {
	// Topo failure
	resetSandbox()
	endPointMustFail = 1
	err := f()
	want := "endpoints fetch error: topo error, shard, host: .0."
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	if endPointCounter != 1 {
		t.Errorf("want 1, got %v", endPointCounter)
	}

	// Connect failure
	resetSandbox()
	dialMustFail = 4
	err = f()
	want = "conn error, shard, host: .0."
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure we dialed 4 times before failing.
	if dialCounter != 4 {
		t.Errorf("want 4, got %v", dialCounter)
	}

	// retry error (multiple failure)
	resetSandbox()
	sbc := &sandboxConn{mustFailRetry: 4}
	testConns[0] = sbc
	err = f()
	want = "retry: err, shard, host: .0., {Uid:0 Host:0 NamedPortMap:map[vt:1]}"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure we dialed 4 times before failing.
	if dialCounter != 4 {
		t.Errorf("want 4, got %v", dialCounter)
	}
	// Ensure we executed 4 times before failing.
	if sbc.ExecCount != 4 {
		t.Errorf("want 4, got %v", sbc.ExecCount)
	}

	// retry error (one failure)
	resetSandbox()
	sbc = &sandboxConn{mustFailRetry: 1}
	testConns[0] = sbc
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
	testConns[0] = sbc
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
	testConns[0] = sbc
	err = f()
	want = "error: err, shard, host: .0., {Uid:0 Host:0 NamedPortMap:map[vt:1]}"
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
	testConns[0] = sbc
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

	// no failures
	resetSandbox()
	sbc = &sandboxConn{}
	testConns[0] = sbc
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

func testShardConnTransact(t *testing.T, f func() error) {
	// retry error
	resetSandbox()
	sbc := &sandboxConn{mustFailRetry: 3}
	testConns[0] = sbc
	err := f()
	want := "retry: err, shard, host: .0., {Uid:0 Host:0 NamedPortMap:map[vt:1]}"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Should not retry if we're in transaction
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}

	// conn error
	resetSandbox()
	sbc = &sandboxConn{mustFailConn: 3}
	testConns[0] = sbc
	err = f()
	want = "error: conn, shard, host: .0., {Uid:0 Host:0 NamedPortMap:map[vt:1]}"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Should not retry if we're in transaction
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}
}

func TestShardConnBeginOther(t *testing.T) {
	// tx_pool_full
	resetSandbox()
	sbc := &sandboxConn{mustFailTxPool: 1}
	testConns[0] = sbc
	sdc := NewShardConn(new(sandboxTopo), "aa", "", "0", "", 10*time.Millisecond, 3)
	startTime := time.Now()
	_, err := sdc.Begin(nil)
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
