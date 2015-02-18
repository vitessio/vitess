// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"testing"
	"time"

	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// This file uses the sandbox_test framework.

var (
	retryCount  = 3
	retryDelay  = 1 * time.Millisecond
	connTimeout = 1 * time.Millisecond
)

func TestShardConnExecute(t *testing.T) {
	testShardConnGeneric(t, "TestShardConnExecute", func() error {
		sdc := NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnExecute", "0", "", retryDelay, retryCount, connTimeout)
		_, err := sdc.Execute(context.Background(), "query", nil, 0)
		return err
	})
	testShardConnTransact(t, "TestShardConnExecute", func() error {
		sdc := NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnExecute", "0", "", retryDelay, retryCount, connTimeout)
		_, err := sdc.Execute(context.Background(), "query", nil, 1)
		return err
	})
}

func TestShardConnExecuteBatch(t *testing.T) {
	testShardConnGeneric(t, "TestShardConnExecuteBatch", func() error {
		sdc := NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnExecuteBatch", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		queries := []tproto.BoundQuery{{"query", nil}}
		_, err := sdc.ExecuteBatch(context.Background(), queries, 0)
		return err
	})
	testShardConnTransact(t, "TestShardConnExecuteBatch", func() error {
		sdc := NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnExecuteBatch", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		queries := []tproto.BoundQuery{{"query", nil}}
		_, err := sdc.ExecuteBatch(context.Background(), queries, 1)
		return err
	})
}

func TestShardConnExecuteStream(t *testing.T) {
	testShardConnGeneric(t, "TestShardConnExecuteStream", func() error {
		sdc := NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnExecuteStream", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		_, errfunc := sdc.StreamExecute(context.Background(), "query", nil, 0)
		return errfunc()
	})
	testShardConnTransact(t, "TestShardConnExecuteStream", func() error {
		sdc := NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnExecuteStream", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		_, errfunc := sdc.StreamExecute(context.Background(), "query", nil, 1)
		return errfunc()
	})
}

func TestShardConnBegin(t *testing.T) {
	testShardConnGeneric(t, "TestShardConnBegin", func() error {
		sdc := NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnBegin", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		_, err := sdc.Begin(context.Background())
		return err
	})
}

func TestShardConnCommit(t *testing.T) {
	testShardConnTransact(t, "TestShardConnCommit", func() error {
		sdc := NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnCommit", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		return sdc.Commit(context.Background(), 1)
	})
}

func TestShardConnRollback(t *testing.T) {
	testShardConnTransact(t, "TestShardConnRollback", func() error {
		sdc := NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnRollback", "0", "", 1*time.Millisecond, 3, 1*time.Millisecond)
		return sdc.Rollback(context.Background(), 1)
	})
}

func testShardConnGeneric(t *testing.T, name string, f func() error) {
	// Topo failure
	s := createSandbox(name)
	s.EndPointMustFail = retryCount + 1
	err := f()
	want := fmt.Sprintf("shard, host: %v.0., {Uid:0 Host: NamedPortMap:map[] Health:map[]}, endpoints fetch error: topo error", name)
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	if s.EndPointCounter != retryCount+1 {
		t.Errorf("want %v, got %v", (retryCount + 1), s.EndPointCounter)
	}

	// Connect failure
	s.Reset()
	sbc := &sandboxConn{}
	s.MapTestConn("0", sbc)
	s.DialMustFail = 4
	err = f()
	want = fmt.Sprintf("shard, host: %v.0., %+v, conn error %+v", name, topo.EndPoint{}, sbc.EndPoint())
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure we dialed 4 times before failing.
	if s.DialCounter != 4 {
		t.Errorf("want 4, got %v", s.DialCounter)
	}

	// no valid endpoints as the only connection is marked down
	// **** It tests the behavior when retryCount is odd.
	// When the retryCount is even, the error should be "retry: err". ****
	s.Reset()
	sbc = &sandboxConn{mustFailRetry: retryCount + 1}
	s.MapTestConn("0", sbc)
	err = f()
	want = fmt.Sprintf("shard, host: %v.0., %+v, no valid endpoint", name, topo.EndPoint{})
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure we dialed 2 times before failing.
	if s.DialCounter != 2 {
		t.Errorf("want 2, got %v", s.DialCounter)
	}
	// Ensure we executed 2 times before failing.
	if sbc.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc.ExecCount)
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
	want = fmt.Sprintf("shard, host: %v.0., {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}, error: err", name)
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
	want := fmt.Sprintf("shard, host: %v.0., {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}, retry: err", name)
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
	want = fmt.Sprintf("shard, host: %v.0., {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}, error: conn", name)
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
	sdc := NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnBeginOther", "0", "", 10*time.Millisecond, 3, 1*time.Millisecond)
	_, err := sdc.Begin(context.Background())
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

func TestShardConnReconnect(t *testing.T) {
	retryDelay := 10 * time.Millisecond
	retryCount := 5
	s := createSandbox("TestShardConnReconnect")
	// case 1: resolved 0 endpoint, return error
	sdc := NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnReconnect", "0", "", retryDelay, retryCount, 1*time.Millisecond)
	startTime := time.Now()
	_, err := sdc.Execute(context.Background(), "query", nil, 0)
	execDuration := time.Now().Sub(startTime)
	if execDuration < (retryDelay * time.Duration(retryCount)) {
		t.Errorf("retry too fast, want %v, got %v", retryDelay*time.Duration(retryCount), execDuration)
	}
	if execDuration > retryDelay*time.Duration(retryCount+1) {
		t.Errorf("retry too slow, want %v, got %v", retryDelay*time.Duration(retryCount+1), execDuration)
	}
	if err == nil {
		t.Errorf("want error, got nil")
	}
	if s.EndPointCounter != retryCount+1 {
		t.Errorf("want %v, got %v", retryCount+1, s.EndPointCounter)
	}

	// case 2.1: resolve 1 endpoint and connect failed -> resolve and retry without spamming
	s.Reset()
	s.DialMustFail = 1
	sbc := &sandboxConn{}
	s.MapTestConn("0", sbc)
	sdc = NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnReconnect", "0", "", retryDelay, retryCount, 1*time.Millisecond)
	timeStart := time.Now()
	sdc.Execute(context.Background(), "query", nil, 0)
	timeDuration := time.Now().Sub(timeStart)
	if timeDuration < retryDelay {
		t.Errorf("want no spam delay %v, got %v", retryDelay, timeDuration)
	}
	if timeDuration > retryDelay*2 {
		t.Errorf("want instant resolve %v, got %v", retryDelay, timeDuration)
	}
	if s.EndPointCounter != 2 {
		t.Errorf("want 2, got %v", s.EndPointCounter)
	}

	// case 2.2: resolve 1 endpoint and execute failed -> resolve and retry without spamming
	s.Reset()
	sbc = &sandboxConn{mustFailConn: 1}
	s.MapTestConn("0", sbc)
	sdc = NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnReconnect", "0", "", retryDelay, retryCount, 1*time.Millisecond)
	timeStart = time.Now()
	sdc.Execute(context.Background(), "query", nil, 0)
	timeDuration = time.Now().Sub(timeStart)
	if timeDuration < retryDelay {
		t.Errorf("want no spam delay %v, got %v", retryDelay, timeDuration)
	}
	if timeDuration > retryDelay*2 {
		t.Errorf("want instant resolve %v, got %v", retryDelay, timeDuration)
	}
	if s.EndPointCounter != 3 {
		t.Errorf("want 3, got %v", s.EndPointCounter)
	}

	// case 3.1: resolve 3 endpoints, failed connection to 1st one -> resolve and connect to 2nd one
	s.Reset()
	s.DialMustFail = 1
	sbc0 := &sandboxConn{}
	sbc1 := &sandboxConn{}
	sbc2 := &sandboxConn{}
	s.MapTestConn("0", sbc0)
	s.MapTestConn("0", sbc1)
	s.MapTestConn("0", sbc2)
	sdc = NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnReconnect", "0", "", retryDelay, retryCount, 1*time.Millisecond)
	timeStart = time.Now()
	sdc.Execute(context.Background(), "query", nil, 0)
	timeDuration = time.Now().Sub(timeStart)
	if timeDuration >= retryDelay {
		t.Errorf("want no delay, got %v", timeDuration)
	}
	if sbc0.ExecCount+sbc1.ExecCount+sbc2.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc0.ExecCount+sbc1.ExecCount+sbc2.ExecCount)
	}
	if s.EndPointCounter != 1 {
		t.Errorf("want 1, got %v", s.EndPointCounter)
	}

	// case 3.2: resolve 3 endpoints, failed execution on 1st one -> resolve and execute on 2nd one
	s.Reset()
	countConnUse := 0
	onConnUse := func(conn *sandboxConn) {
		if countConnUse == 0 {
			conn.mustFailConn = 1
		}
		countConnUse++
	}
	sbc0 = &sandboxConn{onConnUse: onConnUse}
	sbc1 = &sandboxConn{onConnUse: onConnUse}
	sbc2 = &sandboxConn{onConnUse: onConnUse}
	s.MapTestConn("0", sbc0)
	s.MapTestConn("0", sbc1)
	s.MapTestConn("0", sbc2)
	sdc = NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnReconnect", "0", "", retryDelay, retryCount, 1*time.Millisecond)
	timeStart = time.Now()
	sdc.Execute(context.Background(), "query", nil, 0)
	timeDuration = time.Now().Sub(timeStart)
	if timeDuration >= retryDelay {
		t.Errorf("want no delay, got %v", timeDuration)
	}
	if sbc0.ExecCount+sbc1.ExecCount+sbc2.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc0.ExecCount+sbc1.ExecCount+sbc2.ExecCount)
	}
	if sbc0.ExecCount > 1 || sbc1.ExecCount > 1 || sbc2.ExecCount > 1 {
		t.Errorf("want no more than 1, got %v,%v,%v", sbc0.ExecCount, sbc1.ExecCount, sbc2.ExecCount)
	}
	if s.EndPointCounter != 2 {
		t.Errorf("want 2, got %v", s.EndPointCounter)
	}

	// case 4: resolve 3 endpoints, failed connection to 1st, failed execution on 2nd -> resolve and execute on 3rd one
	s.Reset()
	s.DialMustFail = 1
	countConnUse = 0
	onConnUse = func(conn *sandboxConn) {
		if countConnUse == 0 {
			conn.mustFailConn = 1
		}
		countConnUse++
	}
	sbc0 = &sandboxConn{onConnUse: onConnUse}
	sbc1 = &sandboxConn{onConnUse: onConnUse}
	sbc2 = &sandboxConn{onConnUse: onConnUse}
	s.MapTestConn("0", sbc0)
	s.MapTestConn("0", sbc1)
	s.MapTestConn("0", sbc2)
	sdc = NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnReconnect", "0", "", retryDelay, retryCount, 1*time.Millisecond)
	timeStart = time.Now()
	sdc.Execute(context.Background(), "query", nil, 0)
	timeDuration = time.Now().Sub(timeStart)
	if timeDuration >= retryDelay {
		t.Errorf("want no delay, got %v", timeDuration)
	}
	if sbc0.ExecCount+sbc1.ExecCount+sbc2.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc0.ExecCount+sbc1.ExecCount+sbc2.ExecCount)
	}
	if sbc0.ExecCount > 1 || sbc1.ExecCount > 1 || sbc2.ExecCount > 1 {
		t.Errorf("want no more than 1, got %v,%v,%v", sbc0.ExecCount, sbc1.ExecCount, sbc2.ExecCount)
	}
	if s.EndPointCounter != 2 {
		t.Errorf("want 2, got %v", s.EndPointCounter)
	}

	// case 5: always resolve the same 3 endpoints, all 3 execution failed -> resolve and use the first one
	s.Reset()
	var firstConn *sandboxConn
	countConnUse = 0
	onConnUse = func(conn *sandboxConn) {
		if countConnUse == 0 {
			firstConn = conn
		}
		countConnUse++
	}
	sbc0 = &sandboxConn{mustFailConn: 1, onConnUse: onConnUse}
	sbc1 = &sandboxConn{mustFailConn: 1, onConnUse: onConnUse}
	sbc2 = &sandboxConn{mustFailConn: 1, onConnUse: onConnUse}
	s.MapTestConn("0", sbc0)
	s.MapTestConn("0", sbc1)
	s.MapTestConn("0", sbc2)
	sdc = NewShardConn(context.Background(), new(sandboxTopo), "aa", "TestShardConnReconnect", "0", "", retryDelay, retryCount, 1*time.Millisecond)
	timeStart = time.Now()
	sdc.Execute(context.Background(), "query", nil, 0)
	timeDuration = time.Now().Sub(timeStart)
	if timeDuration < retryDelay {
		t.Errorf("want no spam delay %v, got %v", retryDelay, timeDuration)
	}
	if timeDuration > retryDelay*2 {
		t.Errorf("want instant resolve %v, got %v", retryDelay, timeDuration)
	}
	for _, conn := range []*sandboxConn{sbc0, sbc1, sbc2} {
		wantExecCount := 1
		if conn == firstConn {
			wantExecCount = 2
		}
		if int(conn.ExecCount) != wantExecCount {
			t.Errorf("want %v, got %v", wantExecCount, conn.ExecCount)
		}
	}
	if s.EndPointCounter != 5 {
		t.Errorf("want 5, got %v", s.EndPointCounter)
	}

	// case 6: resolve 3 endpoints with 1st execution failed, resolve to a new set without the failed one -> try a random one
	s.Reset()
	firstConn = nil
	onConnUse = func(conn *sandboxConn) {
		if firstConn == nil {
			firstConn = conn
			conn.mustFailConn = 1
		}
	}
	sbc0 = &sandboxConn{onConnUse: onConnUse}
	sbc1 = &sandboxConn{onConnUse: onConnUse}
	sbc2 = &sandboxConn{onConnUse: onConnUse}
	sbc3 := &sandboxConn{}
	s.MapTestConn("0", sbc0)
	s.MapTestConn("0", sbc1)
	s.MapTestConn("0", sbc2)
	countGetEndPoints := 0
	onGetEndPoints := func(st *sandboxTopo) {
		if countGetEndPoints == 1 {
			s.MapTestConn("0", sbc3)
			s.DeleteTestConn("0", firstConn)
		}
		countGetEndPoints++
	}
	sdc = NewShardConn(context.Background(), &sandboxTopo{callbackGetEndPoints: onGetEndPoints}, "aa", "TestShardConnReconnect", "0", "", retryDelay, retryCount, 1*time.Millisecond)
	timeStart = time.Now()
	sdc.Execute(context.Background(), "query", nil, 0)
	timeDuration = time.Now().Sub(timeStart)
	if timeDuration >= retryDelay {
		t.Errorf("want no delay, got %v", timeDuration)
	}
	if firstConn.ExecCount != 1 {
		t.Errorf("want 1, got %v", firstConn.ExecCount)
	}
	totalExecCount := 0
	for _, conn := range s.TestConns["0"] {
		totalExecCount += int(conn.(*sandboxConn).ExecCount)
	}
	if totalExecCount != 1 {
		t.Errorf("want 1, got %v", totalExecCount)
	}
	if s.EndPointCounter != 2 {
		t.Errorf("want 2, got %v", s.EndPointCounter)
	}

	// case 7: resolve 3 bad endpoints with execution failed
	// upon resolve, 2nd bad endpoint changed address (once only) but still fails on execution
	// -> should only use the 1st endpoint after all other endpoints are tried out
	s.Reset()
	var secondConn *sandboxConn
	countConnUse = 0
	onConnUse = func(conn *sandboxConn) {
		if countConnUse == 0 {
			firstConn = conn
		} else if countConnUse == 1 {
			secondConn = conn
		}
		countConnUse++
	}
	sbc0 = &sandboxConn{mustFailConn: 1, onConnUse: onConnUse}
	sbc1 = &sandboxConn{mustFailConn: 1, onConnUse: onConnUse}
	sbc2 = &sandboxConn{mustFailConn: 1, onConnUse: onConnUse}
	sbc3 = &sandboxConn{mustFailConn: 1}
	s.MapTestConn("0", sbc0)
	s.MapTestConn("0", sbc1)
	s.MapTestConn("0", sbc2)
	countGetEndPoints = 0
	onGetEndPoints = func(st *sandboxTopo) {
		if countGetEndPoints == 2 {
			s.MapTestConn("0", sbc3)
			s.DeleteTestConn("0", secondConn)
		}
		countGetEndPoints++
	}
	sdc = NewShardConn(context.Background(), &sandboxTopo{callbackGetEndPoints: onGetEndPoints}, "aa", "TestShardConnReconnect", "0", "", retryDelay, retryCount, 1*time.Millisecond)
	timeStart = time.Now()
	sdc.Execute(context.Background(), "query", nil, 0)
	timeDuration = time.Now().Sub(timeStart)
	if timeDuration < retryDelay {
		t.Errorf("want no spam delay %v, got %v", retryDelay, timeDuration)
	}
	if timeDuration > retryDelay*2 {
		t.Errorf("want instant resolve %v, got %v", retryDelay, timeDuration)
	}
	if secondConn.ExecCount != 1 {
		t.Errorf("want 1, got %v", secondConn.ExecCount)
	}
	if firstConn.ExecCount != 2 {
		t.Errorf("want 2, got %v", firstConn.ExecCount)
	}
	for _, conn := range s.TestConns["0"] {
		if conn != firstConn && conn.(*sandboxConn).ExecCount != 1 {
			t.Errorf("want 1, got %v", conn.(*sandboxConn).ExecCount)
		}
	}
	if s.EndPointCounter != 6 {
		t.Errorf("want 6, got %v", s.EndPointCounter)
	}

	// case 8: resolve 3 bad endpoints with execution failed,
	// after resolve, all endpoints are valid on new addresses
	// -> random use an endpoint without delay
	s.Reset()
	firstConn = nil
	onConnUse = func(conn *sandboxConn) {
		if firstConn == nil {
			firstConn = conn
		}
	}
	sbc0 = &sandboxConn{mustFailConn: 1, onConnUse: onConnUse}
	sbc1 = &sandboxConn{mustFailConn: 1, onConnUse: onConnUse}
	sbc2 = &sandboxConn{mustFailConn: 1, onConnUse: onConnUse}
	sbc3 = &sandboxConn{}
	sbc4 := &sandboxConn{}
	sbc5 := &sandboxConn{}
	s.MapTestConn("0", sbc0)
	s.MapTestConn("0", sbc1)
	s.MapTestConn("0", sbc2)
	countGetEndPoints = 0
	onGetEndPoints = func(st *sandboxTopo) {
		if countGetEndPoints == 1 {
			s.MapTestConn("0", sbc3)
			s.MapTestConn("0", sbc4)
			s.MapTestConn("0", sbc5)
			s.DeleteTestConn("0", sbc0)
			s.DeleteTestConn("0", sbc1)
			s.DeleteTestConn("0", sbc2)
		}
		countGetEndPoints++
	}
	sdc = NewShardConn(context.Background(), &sandboxTopo{callbackGetEndPoints: onGetEndPoints}, "aa", "TestShardConnReconnect", "0", "", retryDelay, retryCount, 1*time.Millisecond)
	timeStart = time.Now()
	sdc.Execute(context.Background(), "query", nil, 0)
	timeDuration = time.Now().Sub(timeStart)
	if timeDuration >= retryDelay {
		t.Errorf("want no delay, got %v", timeDuration)
	}
	if firstConn.ExecCount != 1 {
		t.Errorf("want 1, got %v", firstConn.ExecCount)
	}
	for _, conn := range []*sandboxConn{sbc0, sbc1, sbc2} {
		if conn != firstConn && conn.ExecCount != 0 {
			t.Errorf("want 0, got %v", conn.ExecCount)
		}
	}
	if sbc3.ExecCount+sbc4.ExecCount+sbc5.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc3.ExecCount+sbc4.ExecCount+sbc5.ExecCount)
	}
	if s.EndPointCounter != 2 {
		t.Errorf("want 2, got %v", s.EndPointCounter)
	}
}
