/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgate

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/gateway"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file uses the sandbox_test framework.

func TestScatterConnExecute(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecute", func(sc *ScatterConn, shards []string) (*sqltypes.Result, error) {
		res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
		rss, err := res.ResolveDestination(context.Background(), "TestScatterConnExecute", topodatapb.TabletType_REPLICA, key.DestinationShards(shards))
		if err != nil {
			return nil, err
		}

		return sc.Execute(context.Background(), "query", nil, rss, topodatapb.TabletType_REPLICA, nil, false, nil)
	})
}

func TestScatterConnExecuteMulti(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecuteMultiShard", func(sc *ScatterConn, shards []string) (*sqltypes.Result, error) {
		res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
		rss, err := res.ResolveDestination(context.Background(), "TestScatterConnExecuteMultiShard", topodatapb.TabletType_REPLICA, key.DestinationShards(shards))
		if err != nil {
			return nil, err
		}

		queries := make([]*querypb.BoundQuery, len(rss))
		for i := range rss {
			queries[i] = &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			}
		}

		qr, errs := sc.ExecuteMultiShard(context.Background(), rss, queries, topodatapb.TabletType_REPLICA, nil, false, false)
		return qr, vterrors.Aggregate(errs)
	})
}

func TestScatterConnExecuteBatch(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecuteBatch", func(sc *ScatterConn, shards []string) (*sqltypes.Result, error) {
		queries := []*vtgatepb.BoundShardQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: "TestScatterConnExecuteBatch",
			Shards:   shards,
		}}
		res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
		scatterRequest, err := boundShardQueriesToScatterBatchRequest(context.Background(), res, queries, topodatapb.TabletType_REPLICA)
		if err != nil {
			return nil, err
		}
		qrs, err := sc.ExecuteBatch(context.Background(), scatterRequest, topodatapb.TabletType_REPLICA, false, nil, nil)
		if err != nil {
			return nil, err
		}
		return &qrs[0], err
	})
}

func TestScatterConnStreamExecute(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnStreamExecute", func(sc *ScatterConn, shards []string) (*sqltypes.Result, error) {
		res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
		rss, err := res.ResolveDestination(context.Background(), "TestScatterConnStreamExecute", topodatapb.TabletType_REPLICA, key.DestinationShards(shards))
		if err != nil {
			return nil, err
		}

		qr := new(sqltypes.Result)
		err = sc.StreamExecute(context.Background(), "query", nil, rss, topodatapb.TabletType_REPLICA, nil, func(r *sqltypes.Result) error {
			qr.AppendResult(r)
			return nil
		})
		return qr, err
	})
}

func TestScatterConnStreamExecuteMulti(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnStreamExecuteMulti", func(sc *ScatterConn, shards []string) (*sqltypes.Result, error) {
		res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
		rss, err := res.ResolveDestination(context.Background(), "TestScatterConnStreamExecuteMulti", topodatapb.TabletType_REPLICA, key.DestinationShards(shards))
		if err != nil {
			return nil, err
		}
		bvs := make([]map[string]*querypb.BindVariable, len(rss))
		qr := new(sqltypes.Result)
		err = sc.StreamExecuteMulti(context.Background(), "query", rss, bvs, topodatapb.TabletType_REPLICA, nil, func(r *sqltypes.Result) error {
			qr.AppendResult(r)
			return nil
		})
		return qr, err
	})
}

// verifyScatterConnError checks that a returned error has the expected message,
// type, and error code.
func verifyScatterConnError(t *testing.T, err error, wantErr string, wantCode vtrpcpb.Code) {
	if err == nil || err.Error() != wantErr {
		t.Errorf("wanted error: %s, got error: %v", wantErr, err)
	}
	if code := vterrors.Code(err); code != wantCode {
		t.Errorf("wanted error code: %s, got: %v", wantCode, code)
	}
}

func testScatterConnGeneric(t *testing.T, name string, f func(sc *ScatterConn, shards []string) (*sqltypes.Result, error)) {
	hc := discovery.NewFakeHealthCheck()

	// no shard
	s := createSandbox(name)
	sc := newTestScatterConn(hc, new(sandboxTopo), "aa")
	qr, err := f(sc, nil)
	if qr.RowsAffected != 0 {
		t.Errorf("want 0, got %v", qr.RowsAffected)
	}
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}

	// single shard
	s.Reset()
	sc = newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc := hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	qr, err = f(sc, []string{"0"})
	want := fmt.Sprintf("target: %v.0.replica, used tablet: aa-0 (0): INVALID_ARGUMENT error", name)
	// Verify server error string.
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure that we tried only once.
	if execCount := sbc.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// two shards
	s.Reset()
	hc.Reset()
	sc = newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, name, "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = f(sc, []string{"0", "1"})
	// Verify server errors are consolidated.
	want = fmt.Sprintf("target: %v.0.replica, used tablet: aa-0 (0): INVALID_ARGUMENT error\ntarget: %v.1.replica, used tablet: aa-0 (1): INVALID_ARGUMENT error", name, name)
	verifyScatterConnError(t, err, want, vtrpcpb.Code_INVALID_ARGUMENT)
	// Ensure that we tried only once.
	if execCount := sbc0.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// two shards with different errors
	s.Reset()
	hc.Reset()
	sc = newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 = hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1", 1, name, "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	sbc1.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1
	_, err = f(sc, []string{"0", "1"})
	// Verify server errors are consolidated.
	want = fmt.Sprintf("target: %v.0.replica, used tablet: aa-0 (0): INVALID_ARGUMENT error\ntarget: %v.1.replica, used tablet: aa-0 (1): RESOURCE_EXHAUSTED error", name, name)
	// We should only surface the higher priority error code
	verifyScatterConnError(t, err, want, vtrpcpb.Code_INVALID_ARGUMENT)
	// Ensure that we tried only once.
	if execCount := sbc0.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// duplicate shards
	s.Reset()
	hc.Reset()
	sc = newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc = hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	qr, err = f(sc, []string{"0", "0"})
	// Ensure that we executed only once.
	if execCount := sbc.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// no errors
	s.Reset()
	hc.Reset()
	sc = newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 = hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1", 1, name, "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	qr, err = f(sc, []string{"0", "1"})
	if err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if execCount := sbc0.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
	if len(qr.Rows) != 2 {
		t.Errorf("want 2, got %v", len(qr.Rows))
	}
}

func TestMultiExecs(t *testing.T) {
	createSandbox("TestMultiExecs")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestMultiExecs", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestMultiExecs", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	rss := []*srvtopo.ResolvedShard{
		{
			Target: &querypb.Target{
				Keyspace: "TestMultiExecs",
				Shard:    "0",
			},
			QueryService: sbc0,
		},
		{
			Target: &querypb.Target{
				Keyspace: "TestMultiExecs",
				Shard:    "1",
			},
			QueryService: sbc1,
		},
	}
	queries := []*querypb.BoundQuery{
		{
			Sql: "query1",
			BindVariables: map[string]*querypb.BindVariable{
				"bv0": sqltypes.Int64BindVariable(0),
			},
		},
		{
			Sql: "query2",
			BindVariables: map[string]*querypb.BindVariable{
				"bv1": sqltypes.Int64BindVariable(1),
			},
		},
	}

	_, _ = sc.ExecuteMultiShard(context.Background(), rss, queries, topodatapb.TabletType_REPLICA, nil, false, false)
	if len(sbc0.Queries) == 0 || len(sbc1.Queries) == 0 {
		t.Fatalf("didn't get expected query")
	}
	wantVars0 := map[string]*querypb.BindVariable{
		"bv0": queries[0].BindVariables["bv0"],
	}
	if !reflect.DeepEqual(sbc0.Queries[0].BindVariables, wantVars0) {
		t.Errorf("got %v, want %v", sbc0.Queries[0].BindVariables, wantVars0)
	}
	wantVars1 := map[string]*querypb.BindVariable{
		"bv1": queries[1].BindVariables["bv1"],
	}
	if !reflect.DeepEqual(sbc1.Queries[0].BindVariables, wantVars1) {
		t.Errorf("got %+v, want %+v", sbc0.Queries[0].BindVariables, wantVars1)
	}
	sbc0.Queries = nil
	sbc1.Queries = nil

	rss = []*srvtopo.ResolvedShard{
		{
			Target: &querypb.Target{
				Keyspace: "TestMultiExecs",
				Shard:    "0",
			},
			QueryService: sbc0,
		},
		{
			Target: &querypb.Target{
				Keyspace: "TestMultiExecs",
				Shard:    "1",
			},
			QueryService: sbc1,
		},
	}
	bvs := []map[string]*querypb.BindVariable{
		{
			"bv0": sqltypes.Int64BindVariable(0),
		},
		{
			"bv1": sqltypes.Int64BindVariable(1),
		},
	}
	_ = sc.StreamExecuteMulti(context.Background(), "query", rss, bvs, topodatapb.TabletType_REPLICA, nil, func(*sqltypes.Result) error {
		return nil
	})
	if !reflect.DeepEqual(sbc0.Queries[0].BindVariables, wantVars0) {
		t.Errorf("got %+v, want %+v", sbc0.Queries[0].BindVariables, wantVars0)
	}
	if !reflect.DeepEqual(sbc1.Queries[0].BindVariables, wantVars1) {
		t.Errorf("got %+v, want %+v", sbc0.Queries[0].BindVariables, wantVars1)
	}
}

func TestScatterConnStreamExecuteSendError(t *testing.T) {
	createSandbox("TestScatterConnStreamExecuteSendError")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, new(sandboxTopo), "aa")
	hc.AddTestTablet("aa", "0", 1, "TestScatterConnStreamExecuteSendError", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
	rss, err := res.ResolveDestination(context.Background(), "TestScatterConnStreamExecuteSendError", topodatapb.TabletType_REPLICA, key.DestinationShard("0"))
	if err != nil {
		t.Fatalf("ResolveDestination failed: %v", err)
	}
	err = sc.StreamExecute(context.Background(), "query", nil, rss, topodatapb.TabletType_REPLICA, nil, func(*sqltypes.Result) error {
		return fmt.Errorf("send error")
	})
	want := "send error"
	// Ensure that we handle send errors.
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("got %s, must contain %v", err, want)
	}
}

func TestScatterConnQueryNotInTransaction(t *testing.T) {
	s := createSandbox("TestScatterConnQueryNotInTransaction")
	hc := discovery.NewFakeHealthCheck()

	// case 1: read query (not in transaction) followed by write query, not in the same shard.
	hc.Reset()
	sc := newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestScatterConnQueryNotInTransaction", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestScatterConnQueryNotInTransaction", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
	rss0, err := res.ResolveDestination(context.Background(), "TestScatterConnQueryNotInTransaction", topodatapb.TabletType_REPLICA, key.DestinationShard("0"))
	if err != nil {
		t.Fatalf("ResolveDestination(0) failed: %v", err)
	}
	rss1, err := res.ResolveDestination(context.Background(), "TestScatterConnQueryNotInTransaction", topodatapb.TabletType_REPLICA, key.DestinationShard("1"))
	if err != nil {
		t.Fatalf("ResolveDestination(1) failed: %v", err)
	}

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_REPLICA, session, true, nil)
	sc.Execute(context.Background(), "query1", nil, rss1, topodatapb.TabletType_REPLICA, session, false, nil)

	wantSession := vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestScatterConnQueryNotInTransaction",
				Shard:      "1",
				TabletType: topodatapb.TabletType_REPLICA,
			},
			TransactionId: 1,
		}},
	}
	if !proto.Equal(&wantSession, session.Session) {
		t.Errorf("want\n%+v\ngot\n%+v", wantSession, *session.Session)
	}
	sc.txConn.Commit(context.Background(), session)
	{
		execCount0 := sbc0.ExecCount.Get()
		execCount1 := sbc1.ExecCount.Get()
		if execCount0 != 1 || execCount1 != 1 {
			t.Errorf("want 1/1, got %d/%d", execCount0, execCount1)
		}
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}
	if commitCount := sbc1.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}

	// case 2: write query followed by read query (not in transaction), not in the same shard.
	s.Reset()
	hc.Reset()
	sc = newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 = hc.AddTestTablet("aa", "0", 1, "TestScatterConnQueryNotInTransaction", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1", 1, "TestScatterConnQueryNotInTransaction", "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	session = NewSafeSession(&vtgatepb.Session{InTransaction: true})

	res = srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
	rss0, err = res.ResolveDestination(context.Background(), "TestScatterConnQueryNotInTransaction", topodatapb.TabletType_REPLICA, key.DestinationShard("0"))
	if err != nil {
		t.Fatalf("ResolveDestination(0) failed: %v", err)
	}
	rss1, err = res.ResolveDestination(context.Background(), "TestScatterConnQueryNotInTransaction", topodatapb.TabletType_REPLICA, key.DestinationShard("1"))
	if err != nil {
		t.Fatalf("ResolveDestination(1) failed: %v", err)
	}

	sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_REPLICA, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, rss1, topodatapb.TabletType_REPLICA, session, true, nil)

	wantSession = vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestScatterConnQueryNotInTransaction",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
			},
			TransactionId: 1,
		}},
	}
	if !proto.Equal(&wantSession, session.Session) {
		t.Errorf("want\n%+v\ngot\n%+v", wantSession, *session.Session)
	}
	sc.txConn.Commit(context.Background(), session)
	{
		execCount0 := sbc0.ExecCount.Get()
		execCount1 := sbc1.ExecCount.Get()
		if execCount0 != 1 || execCount1 != 1 {
			t.Errorf("want 1/1, got %d/%d", execCount0, execCount1)
		}
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	if commitCount := sbc1.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}

	// case 3: write query followed by read query, in the same shard.
	s.Reset()
	hc.Reset()
	sc = newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 = hc.AddTestTablet("aa", "0", 1, "TestScatterConnQueryNotInTransaction", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1", 1, "TestScatterConnQueryNotInTransaction", "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	session = NewSafeSession(&vtgatepb.Session{InTransaction: true})

	res = srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
	rss0, err = res.ResolveDestination(context.Background(), "TestScatterConnQueryNotInTransaction", topodatapb.TabletType_REPLICA, key.DestinationShard("0"))
	if err != nil {
		t.Fatalf("ResolveDestination(0) failed: %v", err)
	}
	rss1, err = res.ResolveDestination(context.Background(), "TestScatterConnQueryNotInTransaction", topodatapb.TabletType_REPLICA, key.DestinationShards([]string{"0", "1"}))
	if err != nil {
		t.Fatalf("ResolveDestination(1) failed: %v", err)
	}

	sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_REPLICA, session, false, nil)
	sc.Execute(context.Background(), "query1", nil, rss1, topodatapb.TabletType_REPLICA, session, true, nil)

	wantSession = vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestScatterConnQueryNotInTransaction",
				Shard:      "0",
				TabletType: topodatapb.TabletType_REPLICA,
			},
			TransactionId: 1,
		}},
	}
	if !proto.Equal(&wantSession, session.Session) {
		t.Errorf("want\n%+v\ngot\n%+v", wantSession, *session.Session)
	}
	sc.txConn.Commit(context.Background(), session)
	{
		execCount0 := sbc0.ExecCount.Get()
		execCount1 := sbc1.ExecCount.Get()
		if execCount0 != 2 || execCount1 != 1 {
			t.Errorf("want 2/1, got %d/%d", execCount0, execCount1)
		}
	}
	if commitCount := sbc0.CommitCount.Get(); commitCount != 1 {
		t.Errorf("want 1, got %d", commitCount)
	}
	if commitCount := sbc1.CommitCount.Get(); commitCount != 0 {
		t.Errorf("want 0, got %d", commitCount)
	}
}

func TestScatterConnSingleDB(t *testing.T) {
	createSandbox("TestScatterConnSingleDB")
	hc := discovery.NewFakeHealthCheck()

	hc.Reset()
	sc := newTestScatterConn(hc, new(sandboxTopo), "aa")
	hc.AddTestTablet("aa", "0", 1, "TestScatterConnSingleDB", "0", topodatapb.TabletType_MASTER, true, 1, nil)
	hc.AddTestTablet("aa", "1", 1, "TestScatterConnSingleDB", "1", topodatapb.TabletType_MASTER, true, 1, nil)

	res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
	rss0, err := res.ResolveDestination(context.Background(), "TestScatterConnSingleDB", topodatapb.TabletType_MASTER, key.DestinationShard("0"))
	if err != nil {
		t.Fatalf("ResolveDestination(0) failed: %v", err)
	}
	rss1, err := res.ResolveDestination(context.Background(), "TestScatterConnSingleDB", topodatapb.TabletType_MASTER, key.DestinationShard("1"))
	if err != nil {
		t.Fatalf("ResolveDestination(1) failed: %v", err)
	}

	want := "multi-db transaction attempted"

	// SingleDb (legacy)
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true, SingleDb: true})
	_, err = sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Error(err)
	}
	_, err = sc.Execute(context.Background(), "query1", nil, rss1, topodatapb.TabletType_MASTER, session, false, nil)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Multi DB exec: %v, must contain %s", err, want)
	}

	// TransactionMode_SINGLE in session
	session = NewSafeSession(&vtgatepb.Session{InTransaction: true, TransactionMode: vtgatepb.TransactionMode_SINGLE})
	_, err = sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Error(err)
	}
	_, err = sc.Execute(context.Background(), "query1", nil, rss1, topodatapb.TabletType_MASTER, session, false, nil)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Multi DB exec: %v, must contain %s", err, want)
	}

	// TransactionMode_SINGLE in txconn
	sc.txConn.mode = vtgatepb.TransactionMode_SINGLE
	session = NewSafeSession(&vtgatepb.Session{InTransaction: true})
	_, err = sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Error(err)
	}
	_, err = sc.Execute(context.Background(), "query1", nil, rss1, topodatapb.TabletType_MASTER, session, false, nil)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Multi DB exec: %v, must contain %s", err, want)
	}

	// TransactionMode_MULTI in txconn. Should not fail.
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI
	session = NewSafeSession(&vtgatepb.Session{InTransaction: true})
	_, err = sc.Execute(context.Background(), "query1", nil, rss0, topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Error(err)
	}
	_, err = sc.Execute(context.Background(), "query1", nil, rss1, topodatapb.TabletType_MASTER, session, false, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestAppendResult(t *testing.T) {
	qr := new(sqltypes.Result)
	innerqr1 := &sqltypes.Result{
		Fields: []*querypb.Field{},
		Rows:   [][]sqltypes.Value{},
	}
	innerqr2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "foo", Type: sqltypes.Int8},
		},
		RowsAffected: 1,
		InsertID:     1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarBinary("abcd")},
		},
	}
	// test one empty result
	qr.AppendResult(innerqr1)
	qr.AppendResult(innerqr2)
	if len(qr.Fields) != 1 {
		t.Errorf("want 1, got %v", len(qr.Fields))
	}
	if qr.RowsAffected != 1 {
		t.Errorf("want 1, got %v", qr.RowsAffected)
	}
	if qr.InsertID != 1 {
		t.Errorf("want 1, got %v", qr.InsertID)
	}
	if len(qr.Rows) != 1 {
		t.Errorf("want 1, got %v", len(qr.Rows))
	}
	// test two valid results
	qr = new(sqltypes.Result)
	qr.AppendResult(innerqr2)
	qr.AppendResult(innerqr2)
	if len(qr.Fields) != 1 {
		t.Errorf("want 1, got %v", len(qr.Fields))
	}
	if qr.RowsAffected != 2 {
		t.Errorf("want 2, got %v", qr.RowsAffected)
	}
	if qr.InsertID != 1 {
		t.Errorf("want 1, got %v", qr.InsertID)
	}
	if len(qr.Rows) != 2 {
		t.Errorf("want 2, got %v", len(qr.Rows))
	}
}

// MockShuffleQueryPartsRandomGenerator implements the ShuffleQueryPartsRandomGeneratorInterface
// and returns a canned set of responses given in 'intNResults' for successive calls to its Intn()
// method.
type mockShuffleQueryPartsRandomGenerator struct {
	intNResults []int
}

func (mockRandGen *mockShuffleQueryPartsRandomGenerator) Intn(unused int) int {
	if len(mockRandGen.intNResults) == 0 {
		panic("MockShuffleQueryPartsRandomGenerator exhausted.")
	}
	result := mockRandGen.intNResults[0]
	mockRandGen.intNResults = mockRandGen.intNResults[1:]
	return result
}

func TestShuffleQueryParts(t *testing.T) {
	mockRandGen := &mockShuffleQueryPartsRandomGenerator{
		intNResults: []int{1, 0},
	}
	oldGen := injectShuffleQueryPartsRandomGenerator(mockRandGen)
	queryPart1 := vtgatepb.SplitQueryResponse_Part{
		Query: &querypb.BoundQuery{Sql: "part_1"},
	}
	queryPart2 := vtgatepb.SplitQueryResponse_Part{
		Query: &querypb.BoundQuery{Sql: "part_2"},
	}
	queryPart3 := vtgatepb.SplitQueryResponse_Part{
		Query: &querypb.BoundQuery{Sql: "part_3"},
	}
	queryParts := []*vtgatepb.SplitQueryResponse_Part{&queryPart1, &queryPart2, &queryPart3}
	queryPartsExpectedOutput := []*vtgatepb.SplitQueryResponse_Part{
		&queryPart3, &queryPart1, &queryPart2,
	}
	shuffleQueryParts(queryParts)
	if !sqltypes.SplitQueryResponsePartsEqual(queryParts, queryPartsExpectedOutput) {
		t.Errorf("want: %+v, got %+v", queryPartsExpectedOutput, queryParts)
	}

	// Return the generator to what it was to avoid disrupting other tests.
	injectShuffleQueryPartsRandomGenerator(oldGen)
}

func newTestScatterConn(hc discovery.HealthCheck, serv srvtopo.Server, cell string) *ScatterConn {
	// The topo.Server is used to start watching the cells described
	// in '-cells_to_watch' command line parameter, which is
	// empty by default. So it's unused in this test, set to nil.
	gw := gateway.GetCreator()(context.Background(), hc, serv, cell, 3)
	tc := NewTxConn(gw, vtgatepb.TransactionMode_TWOPC)
	return NewScatterConn("", tc, gw, hc)
}
