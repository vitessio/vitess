// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"
)

// This file uses the sandbox_test framework.

func TestExecuteKeyspaceAlias(t *testing.T) {
	testVerticalSplitGeneric(t, false, func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)
		return stc.Execute(context.Background(), "query", nil, KsTestUnshardedServedFrom, shards, topo.TYPE_RDONLY, nil)
	})
}

func TestBatchExecuteKeyspaceAlias(t *testing.T) {
	testVerticalSplitGeneric(t, false, func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)
		queries := []tproto.BoundQuery{{"query", nil}}
		qrs, err := stc.ExecuteBatch(context.Background(), queries, KsTestUnshardedServedFrom, shards, topo.TYPE_RDONLY, nil)
		if err != nil {
			return nil, err
		}
		return &qrs.List[0], err
	})
}

func TestStreamExecuteKeyspaceAlias(t *testing.T) {
	testVerticalSplitGeneric(t, true, func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)
		qr := new(mproto.QueryResult)
		err := stc.StreamExecute(context.Background(), "query", nil, KsTestUnshardedServedFrom, shards, topo.TYPE_RDONLY, nil, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
}

func TestInTransactionKeyspaceAlias(t *testing.T) {
	s := createSandbox(KsTestUnshardedServedFrom)
	sbc := &sandboxConn{mustFailRetry: 3}
	s.MapTestConn("0", sbc)

	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 1*time.Millisecond)
	session := NewSafeSession(&proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      KsTestUnshardedServedFrom,
			Shard:         "0",
			TabletType:    topo.TYPE_MASTER,
			TransactionId: 1,
		}},
	})
	_, err := stc.Execute(context.Background(), "query", nil, KsTestUnshardedServedFrom, []string{"0"}, topo.TYPE_MASTER, session)
	want := "shard, host: TestUnshardedServedFrom.0.master, {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}, retry: err"
	if err == nil || err.Error() != want {
		t.Errorf("want '%v', got '%v'", want, err)
	}
	// Ensure that we tried once, no retry here
	// since we are in a transaction.
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}
}

func testVerticalSplitGeneric(t *testing.T, isStreaming bool, f func(shards []string) (*mproto.QueryResult, error)) {
	// Retry Error, for keyspace that is redirected should succeed.
	s := createSandbox(KsTestUnshardedServedFrom)
	sbc := &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("0", sbc)
	_, err := f([]string{"0"})
	if isStreaming {
		want := "shard, host: TestUnshardedServedFrom.0.rdonly, {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}, retry: err"
		if err == nil || err.Error() != want {
			t.Errorf("want '%v', got '%v'", want, err)
		}
		if sbc.ExecCount != 1 {
			t.Errorf("want 1, got %v", sbc.ExecCount)
		}
	} else {
		if err != nil {
			t.Errorf("want nil, got %v", err)
		}
		// Ensure that we tried 2 times, 1 for retry and 1 for redirect.
		if sbc.ExecCount != 2 {
			t.Errorf("want 2, got %v", sbc.ExecCount)
		}
	}

	// Fatal Error, for keyspace that is redirected should succeed.
	s.Reset()
	sbc = &sandboxConn{mustFailFatal: 1}
	s.MapTestConn("0", sbc)
	_, err = f([]string{"0"})
	if isStreaming {
		want := "shard, host: TestUnshardedServedFrom.0.rdonly, {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}, fatal: err"
		if err == nil || err.Error() != want {
			t.Errorf("want '%v', got '%v'", want, err)
		}
		if sbc.ExecCount != 1 {
			t.Errorf("want 1, got %v", sbc.ExecCount)
		}
	} else {
		if err != nil {
			t.Errorf("want nil, got %v", err)
		}
		// Ensure that we tried 2 times, 1 for retry and 1 for redirect.
		if sbc.ExecCount != 2 {
			t.Errorf("want 1, got %v", sbc.ExecCount)
		}
	}

	//  Error, for keyspace that is redirected should succeed.
	s.Reset()
	sbc = &sandboxConn{mustFailServer: 3}
	s.MapTestConn("0", sbc)
	_, err = f([]string{"0"})
	want := "shard, host: TestUnshardedServedFrom.0.rdonly, {Uid:0 Host:0 NamedPortMap:map[vt:1] Health:map[]}, error: err"
	if err == nil || err.Error() != want {
		t.Errorf("want '%v', got '%v'", want, err)
	}
	// Ensure that we tried once, no retry here.
	if sbc.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc.ExecCount)
	}
}
