// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"
)

// This file uses the sandbox_test framework.

func TestExecuteKeyspaceAlias(t *testing.T) {
	testVerticalSplitGeneric(t, false, func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		return stc.Execute(context.Background(), "query", nil, KsTestUnshardedServedFrom, shards, topo.TYPE_RDONLY, nil, false)
	})
}

func TestBatchExecuteKeyspaceAlias(t *testing.T) {
	testVerticalSplitGeneric(t, false, func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		queries := []proto.BoundShardQuery{{
			Sql:           "query",
			BindVariables: nil,
			Keyspace:      KsTestUnshardedServedFrom,
			Shards:        shards,
		}}
		scatterRequest := boundShardQueriesToScatterBatchRequest(queries)
		qrs, err := stc.ExecuteBatch(context.Background(), scatterRequest, topo.TYPE_RDONLY, false, nil)
		if err != nil {
			return nil, err
		}
		return &qrs.List[0], err
	})
}

func TestStreamExecuteKeyspaceAlias(t *testing.T) {
	testVerticalSplitGeneric(t, true, func(shards []string) (*mproto.QueryResult, error) {
		stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
		qr := new(mproto.QueryResult)
		err := stc.StreamExecute(context.Background(), "query", nil, KsTestUnshardedServedFrom, shards, topo.TYPE_RDONLY, nil, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		}, false)
		return qr, err
	})
}

func TestInTransactionKeyspaceAlias(t *testing.T) {
	s := createSandbox(KsTestUnshardedServedFrom)
	sbc := &sandboxConn{mustFailRetry: 3}
	s.MapTestConn("0", sbc)

	stc := NewScatterConn(new(sandboxTopo), "", "aa", 1*time.Millisecond, 3, 2*time.Millisecond, 1*time.Millisecond, 24*time.Hour)
	session := NewSafeSession(&proto.Session{
		InTransaction: true,
		ShardSessions: []*proto.ShardSession{{
			Keyspace:      KsTestUnshardedServedFrom,
			Shard:         "0",
			TabletType:    topo.TYPE_MASTER,
			TransactionId: 1,
		}},
	})
	_, err := stc.Execute(context.Background(), "query", nil, KsTestUnshardedServedFrom, []string{"0"}, topo.TYPE_MASTER, session, false)
	want := "shard, host: TestUnshardedServedFrom.0.master, host:\"0\" port_map:<key:\"vt\" value:1 > , retry: err"
	if err == nil || err.Error() != want {
		t.Errorf("want '%v', got '%v'", want, err)
	}
	// Ensure that we tried once, no retry here
	// since we are in a transaction.
	if execCount := sbc.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
}

func testVerticalSplitGeneric(t *testing.T, isStreaming bool, f func(shards []string) (*mproto.QueryResult, error)) {
	// Retry Error, for keyspace that is redirected should succeed.
	s := createSandbox(KsTestUnshardedServedFrom)
	sbc := &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("0", sbc)
	_, err := f([]string{"0"})
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// Ensure that we tried 2 times, 1 for retry and 1 for redirect.
	if execCount := sbc.ExecCount.Get(); execCount != 2 {
		t.Errorf("want 2, got %v", execCount)
	}

	// Fatal Error, for keyspace that is redirected should succeed.
	s.Reset()
	sbc = &sandboxConn{mustFailFatal: 1}
	s.MapTestConn("0", sbc)
	_, err = f([]string{"0"})
	if isStreaming {
		want := "shard, host: TestUnshardedServedFrom.0.rdonly, host:\"0\" port_map:<key:\"vt\" value:1 > , fatal: err"
		if err == nil || err.Error() != want {
			t.Errorf("want '%v', got '%v'", want, err)
		}
		// Ensure that we tried only once.
		if execCount := sbc.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
	} else {
		if err != nil {
			t.Errorf("want nil, got %v", err)
		}
		// Ensure that we tried 2 times, 1 for retry and 1 for redirect.
		if execCount := sbc.ExecCount.Get(); execCount != 2 {
			t.Errorf("want 2, got %v", execCount)
		}
	}

	//  Error, for keyspace that is redirected should succeed.
	s.Reset()
	sbc = &sandboxConn{mustFailServer: 3}
	s.MapTestConn("0", sbc)
	_, err = f([]string{"0"})
	want := "shard, host: TestUnshardedServedFrom.0.rdonly, host:\"0\" port_map:<key:\"vt\" value:1 > , error: err"
	if err == nil || err.Error() != want {
		t.Errorf("want '%v', got '%v'", want, err)
	}
	// Ensure that we tried once, no retry here.
	if execCount := sbc.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
}
