// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/context"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
)

// This file uses the sandbox_test framework.

func TestShardConnExecute(t *testing.T) {
	testShardConnGeneric(t, "TestShardConnExecute", func() error {
		sdc := NewShardConn(&context.DummyContext{}, new(sandboxTopo), "aa", "TestShardConnExecute", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		_, err := sdc.Execute(nil, "query", nil, 0)
		return err
	})
	testShardConnTransact(t, "TestShardConnExecute", func() error {
		sdc := NewShardConn(&context.DummyContext{}, new(sandboxTopo), "aa", "TestShardConnExecute", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		_, err := sdc.Execute(nil, "query", nil, 1)
		return err
	})
}

func TestShardConnExecuteBatch(t *testing.T) {
	testShardConnGeneric(t, "TestShardConnExecuteBatch", func() error {
		sdc := NewShardConn(&context.DummyContext{}, new(sandboxTopo), "aa", "TestShardConnExecuteBatch", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		queries := []tproto.BoundQuery{{"query", nil}}
		_, err := sdc.ExecuteBatch(nil, queries, 0)
		return err
	})
	testShardConnTransact(t, "TestShardConnExecuteBatch", func() error {
		sdc := NewShardConn(&context.DummyContext{}, new(sandboxTopo), "aa", "TestShardConnExecuteBatch", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		queries := []tproto.BoundQuery{{"query", nil}}
		_, err := sdc.ExecuteBatch(nil, queries, 1)
		return err
	})
}

func TestShardConnExecuteStream(t *testing.T) {
	testShardConnGeneric(t, "TestShardConnExecuteStream", func() error {
		sdc := NewShardConn(&context.DummyContext{}, new(sandboxTopo), "aa", "TestShardConnExecuteStream", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		_, errfunc := sdc.StreamExecute(nil, "query", nil, 0)
		return errfunc()
	})
	testShardConnTransact(t, "TestShardConnExecuteStream", func() error {
		sdc := NewShardConn(&context.DummyContext{}, new(sandboxTopo), "aa", "TestShardConnExecuteStream", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		_, errfunc := sdc.StreamExecute(nil, "query", nil, 1)
		return errfunc()
	})
}

func TestShardConnBegin(t *testing.T) {
	testShardConnGeneric(t, "TestShardConnBegin", func() error {
		sdc := NewShardConn(&context.DummyContext{}, new(sandboxTopo), "aa", "TestShardConnBegin", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		_, err := sdc.Begin(nil)
		return err
	})
}

func TestShardConnCommit(t *testing.T) {
	testShardConnTransact(t, "TestShardConnCommit", func() error {
		sdc := NewShardConn(&context.DummyContext{}, new(sandboxTopo), "aa", "TestShardConnCommit", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		return sdc.Commit(nil, 1)
	})
}

func TestShardConnRollback(t *testing.T) {
	testShardConnTransact(t, "TestShardConnRollback", func() error {
		sdc := NewShardConn(&context.DummyContext{}, new(sandboxTopo), "aa", "TestShardConnRollback", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		return sdc.Rollback(nil, 1)
	})
}

func testShardConnGeneric(t *testing.T, name string, f func() error) {
	// Topo failure
	s := createSandbox(name)
	s.EndPointMustFail = 1
	err := f()
	want := fmt.Sprintf("endpoints fetch error: topo error, shard, host: %v.0., {Uid:0 Host: NamedPortMap:map[] Health:map[]}", name)
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	if s.EndPointCounter != 1 {
		t.Errorf("want 1, got %v", s.EndPointCounter)
	}

	// Connect failure
	s.Reset()
	s.DialMustFail = 4
	err = f()
	want = fmt.Sprintf("conn error, shard, host: %v.0., {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}", name)
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure we dialed 4 times before failing.
	if s.DialCounter != 4 {
		t.Errorf("want 4, got %v", s.DialCounter)
	}

	// retry error (multiple failure)
	s.Reset()
	sbc := &sandboxConn{mustFailRetry: 4}
	s.MapTestConn("0", sbc)
	err = f()
	want = fmt.Sprintf("retry: err, shard, host: %v.0., {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}", name)
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure we dialed 4 times before failing.
	if s.DialCounter != 4 {
		t.Errorf("want 4, got %v", s.DialCounter)
	}
	// Ensure we executed 4 times before failing.
	if sbc.ExecCount != 4 {
		t.Errorf("want 4, got %v", sbc.ExecCount)
	}

	// retry error (one failure)
	s.Reset()
	sbc = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("0", sbc)
	err = f()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// Ensure we dialed twice (second one succeeded)
	if s.DialCounter != 2 {
		t.Errorf("want 2, got %v", s.DialCounter)
	}
	// Ensure we executed twice (second one succeeded)
	if sbc.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc.ExecCount)
	}

	// fatal error (one failure)
	s.Reset()
	sbc = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("0", sbc)
	err = f()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// Ensure we dialed twice (second one succeeded)
	if s.DialCounter != 2 {
		t.Errorf("want 2, got %v", s.DialCounter)
	}
	// Ensure we executed twice (second one succeeded)
	if sbc.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc.ExecCount)
	}

	// server error
	s.Reset()
	sbc = &sandboxConn{mustFailServer: 1}
	s.MapTestConn("0", sbc)
	err = f()
	want = fmt.Sprintf("error: err, shard, host: %v.0., {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}", name)
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure we did not redial.
	if s.DialCounter != 1 {
		t.Errorf("want 1, got %v", s.DialCounter)
	}
	// Ensure we did not re-execute.
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}

	// conn error (one failure)
	s.Reset()
	sbc = &sandboxConn{mustFailConn: 1}
	s.MapTestConn("0", sbc)
	err = f()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// Ensure we dialed twice (second one succeeded)
	if s.DialCounter != 2 {
		t.Errorf("want 2, got %v", s.DialCounter)
	}
	// Ensure we executed twice (second one succeeded)
	if sbc.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc.ExecCount)
	}

	// no failures
	s.Reset()
	sbc = &sandboxConn{}
	s.MapTestConn("0", sbc)
	err = f()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if s.DialCounter != 1 {
		t.Errorf("want 1, got %v", s.DialCounter)
	}
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}
}

func testShardConnTransact(t *testing.T, name string, f func() error) {
	// retry error
	s := createSandbox(name)
	sbc := &sandboxConn{mustFailRetry: 3}
	s.MapTestConn("0", sbc)
	err := f()
	want := fmt.Sprintf("retry: err, shard, host: %v.0., {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}", name)
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Should not retry if we're in transaction
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}

	// conn error
	s.Reset()
	sbc = &sandboxConn{mustFailConn: 3}
	s.MapTestConn("0", sbc)
	err = f()
	want = fmt.Sprintf("error: conn, shard, host: %v.0., {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}", name)
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
	s := createSandbox("TestShardConnBeginOther")
	sbc := &sandboxConn{mustFailTxPool: 1}
	s.MapTestConn("0", sbc)
	sdc := NewShardConn(&context.DummyContext{}, new(sandboxTopo), "aa", "TestShardConnBeginOther", "0", "", 10*time.Millisecond, 3, 1*time.Millisecond)
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
	if s.DialCounter != 1 {
		t.Errorf("want 1, got %v", s.DialCounter)
	}
	// Account for 2 calls to Begin.
	if sbc.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc.ExecCount)
	}
}
