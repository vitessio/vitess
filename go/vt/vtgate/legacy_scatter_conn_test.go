/*
Copyright 2019 The Vitess Authors.

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

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/assert"

	"context"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
)

// This file uses the sandbox_test framework.

func TestLegacyExecuteFailOnAutocommit(t *testing.T) {

	createSandbox("TestExecuteFailOnAutocommit")
	hc := discovery.NewFakeLegacyHealthCheck()
	sc := newTestLegacyScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestExecuteFailOnAutocommit", "0", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestExecuteFailOnAutocommit", "1", topodatapb.TabletType_MASTER, true, 1, nil)

	rss := []*srvtopo.ResolvedShard{
		{
			Target: &querypb.Target{
				Keyspace:   "TestExecuteFailOnAutocommit",
				Shard:      "0",
				TabletType: topodatapb.TabletType_MASTER,
			},
			Gateway: sbc0,
		},
		{
			Target: &querypb.Target{
				Keyspace:   "TestExecuteFailOnAutocommit",
				Shard:      "1",
				TabletType: topodatapb.TabletType_MASTER,
			},
			Gateway: sbc1,
		},
	}
	queries := []*querypb.BoundQuery{
		{
			// This will fail to go to shard. It will be rejected at vtgate.
			Sql: "query1",
			BindVariables: map[string]*querypb.BindVariable{
				"bv0": sqltypes.Int64BindVariable(0),
			},
		},
		{
			// This will go to shard.
			Sql: "query2",
			BindVariables: map[string]*querypb.BindVariable{
				"bv1": sqltypes.Int64BindVariable(1),
			},
		},
	}
	// shard 0 - has transaction
	// shard 1 - does not have transaction.
	session := &vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{
			{
				Target:        &querypb.Target{Keyspace: "TestExecuteFailOnAutocommit", Shard: "0", TabletType: topodatapb.TabletType_MASTER, Cell: "aa"},
				TransactionId: 123,
				TabletAlias:   nil,
			},
		},
		Autocommit: false,
	}
	_, errs := sc.ExecuteMultiShard(ctx, rss, queries, NewSafeSession(session), true /*autocommit*/, false)
	err := vterrors.Aggregate(errs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "in autocommit mode, transactionID should be zero but was: 123")
	utils.MustMatch(t, 0, len(sbc0.Queries), "")
	utils.MustMatch(t, []*querypb.BoundQuery{queries[1]}, sbc1.Queries, "")
}

func TestScatterConnExecuteMulti(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnExecuteMultiShard", func(sc *ScatterConn, shards []string) (*sqltypes.Result, error) {
		res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
		rss, err := res.ResolveDestination(ctx, "TestScatterConnExecuteMultiShard", topodatapb.TabletType_REPLICA, key.DestinationShards(shards))
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

		qr, errs := sc.ExecuteMultiShard(ctx, rss, queries, NewSafeSession(nil), false /*autocommit*/, false)
		return qr, vterrors.Aggregate(errs)
	})
}

func TestScatterConnStreamExecute(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnStreamExecute", func(sc *ScatterConn, shards []string) (*sqltypes.Result, error) {
		res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
		rss, err := res.ResolveDestination(ctx, "TestScatterConnStreamExecute", topodatapb.TabletType_REPLICA, key.DestinationShards(shards))
		if err != nil {
			return nil, err
		}

		qr := new(sqltypes.Result)
		err = sc.StreamExecute(ctx, "query", nil, rss, nil, func(r *sqltypes.Result) error {
			qr.AppendResult(r)
			return nil
		})
		return qr, err
	})
}

func TestScatterConnStreamExecuteMulti(t *testing.T) {
	testScatterConnGeneric(t, "TestScatterConnStreamExecuteMulti", func(sc *ScatterConn, shards []string) (*sqltypes.Result, error) {
		res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
		rss, err := res.ResolveDestination(ctx, "TestScatterConnStreamExecuteMulti", topodatapb.TabletType_REPLICA, key.DestinationShards(shards))
		if err != nil {
			return nil, err
		}
		bvs := make([]map[string]*querypb.BindVariable, len(rss))
		qr := new(sqltypes.Result)
		err = sc.StreamExecuteMulti(ctx, "query", rss, bvs, nil, func(r *sqltypes.Result) error {
			qr.AppendResult(r)
			return nil
		})
		return qr, err
	})
}

// verifyScatterConnError checks that a returned error has the expected message,
// type, and error code.
func verifyScatterConnError(t *testing.T, err error, wantErr string, wantCode vtrpcpb.Code) {
	t.Helper()
	assert.EqualError(t, err, wantErr)
	assert.Equal(t, wantCode, vterrors.Code(err))
}

func testScatterConnGeneric(t *testing.T, name string, f func(sc *ScatterConn, shards []string) (*sqltypes.Result, error)) {
	hc := discovery.NewFakeLegacyHealthCheck()

	// no shard
	s := createSandbox(name)
	sc := newTestLegacyScatterConn(hc, new(sandboxTopo), "aa")
	qr, err := f(sc, nil)
	require.NoError(t, err)
	if qr.RowsAffected != 0 {
		t.Errorf("want 0, got %v", qr.RowsAffected)
	}

	// single shard
	s.Reset()
	sc = newTestLegacyScatterConn(hc, new(sandboxTopo), "aa")
	sbc := hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = f(sc, []string{"0"})
	want := fmt.Sprintf("target: %v.0.replica: INVALID_ARGUMENT error", name)
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
	sc = newTestLegacyScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, name, "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	sbc1.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	_, err = f(sc, []string{"0", "1"})
	// Verify server errors are consolidated.
	want = fmt.Sprintf("target: %v.0.replica: INVALID_ARGUMENT error\ntarget: %v.1.replica: INVALID_ARGUMENT error", name, name)
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
	sc = newTestLegacyScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 = hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1", 1, name, "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	sbc1.MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1
	_, err = f(sc, []string{"0", "1"})
	// Verify server errors are consolidated.
	want = fmt.Sprintf("target: %v.0.replica: INVALID_ARGUMENT error\ntarget: %v.1.replica: RESOURCE_EXHAUSTED error", name, name)
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
	sc = newTestLegacyScatterConn(hc, new(sandboxTopo), "aa")
	sbc = hc.AddTestTablet("aa", "0", 1, name, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	_, _ = f(sc, []string{"0", "0"})
	// Ensure that we executed only once.
	if execCount := sbc.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// no errors
	s.Reset()
	hc.Reset()
	sc = newTestLegacyScatterConn(hc, new(sandboxTopo), "aa")
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
	if qr.RowsAffected != 0 {
		t.Errorf("want 0, got %v", qr.RowsAffected)
	}
	if len(qr.Rows) != 2 {
		t.Errorf("want 2, got %v", len(qr.Rows))
	}
}

func TestMaxMemoryRows(t *testing.T) {
	save := *maxMemoryRows
	*maxMemoryRows = 3
	defer func() { *maxMemoryRows = save }()

	createSandbox("TestMaxMemoryRows")
	hc := discovery.NewFakeLegacyHealthCheck()
	sc := newTestLegacyScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestMaxMemoryRows", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestMaxMemoryRows", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
	rss, _, err := res.ResolveDestinations(ctx, "TestMaxMemoryRows", topodatapb.TabletType_REPLICA, nil,
		[]key.Destination{key.DestinationShard("0"), key.DestinationShard("1")})
	require.NoError(t, err)

	session := NewSafeSession(&vtgatepb.Session{InTransaction: true})
	queries := []*querypb.BoundQuery{{
		Sql:           "query1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "query1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	tworows := &sqltypes.Result{
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}, {
			sqltypes.NewInt64(1),
		}},
		RowsAffected: 1,
		InsertID:     1,
	}

	testCases := []struct {
		ignoreMaxMemoryRows bool
		err                 string
	}{
		{true, ""},
		{false, "in-memory row count exceeded allowed limit of 3"},
	}

	for _, test := range testCases {
		sbc0.SetResults([]*sqltypes.Result{tworows, tworows})
		sbc1.SetResults([]*sqltypes.Result{tworows, tworows})

		_, errs := sc.ExecuteMultiShard(ctx, rss, queries, session, false, test.ignoreMaxMemoryRows)
		if test.ignoreMaxMemoryRows {
			require.NoError(t, err)
		} else {
			assert.EqualError(t, errs[0], test.err)
		}
	}
}

func TestLegaceHealthCheckFailsOnReservedConnections(t *testing.T) {
	keyspace := "keyspace"
	createSandbox(keyspace)
	hc := discovery.NewFakeLegacyHealthCheck()
	sc := newTestLegacyScatterConn(hc, new(sandboxTopo), "aa")

	res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: false, InReservedConn: true})
	destinations := []key.Destination{key.DestinationShard("0")}
	rss, _, err := res.ResolveDestinations(ctx, keyspace, topodatapb.TabletType_REPLICA, nil, destinations)
	require.NoError(t, err)

	var queries []*querypb.BoundQuery

	for range rss {
		queries = append(queries, &querypb.BoundQuery{
			Sql:           "query1",
			BindVariables: map[string]*querypb.BindVariable{},
		})
	}

	_, errs := sc.ExecuteMultiShard(ctx, rss, queries, session, false, false)
	require.Error(t, vterrors.Aggregate(errs))
}

func executeOnShards(t *testing.T, res *srvtopo.Resolver, keyspace string, sc *ScatterConn, session *SafeSession, destinations []key.Destination) {
	t.Helper()
	require.Empty(t, executeOnShardsReturnsErr(t, res, keyspace, sc, session, destinations))
}

func executeOnShardsReturnsErr(t *testing.T, res *srvtopo.Resolver, keyspace string, sc *ScatterConn, session *SafeSession, destinations []key.Destination) error {
	t.Helper()
	rss, _, err := res.ResolveDestinations(ctx, keyspace, topodatapb.TabletType_REPLICA, nil, destinations)
	require.NoError(t, err)

	var queries []*querypb.BoundQuery

	for range rss {
		queries = append(queries, &querypb.BoundQuery{
			Sql:           "query1",
			BindVariables: map[string]*querypb.BindVariable{},
		})
	}

	_, errs := sc.ExecuteMultiShard(ctx, rss, queries, session, false, false)
	return vterrors.Aggregate(errs)
}

func TestMultiExecs(t *testing.T) {
	createSandbox("TestMultiExecs")
	hc := discovery.NewFakeLegacyHealthCheck()
	sc := newTestLegacyScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestMultiExecs", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestMultiExecs", "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	rss := []*srvtopo.ResolvedShard{
		{
			Target: &querypb.Target{
				Keyspace: "TestMultiExecs",
				Shard:    "0",
			},
			Gateway: sbc0,
		},
		{
			Target: &querypb.Target{
				Keyspace: "TestMultiExecs",
				Shard:    "1",
			},
			Gateway: sbc1,
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

	_, _ = sc.ExecuteMultiShard(ctx, rss, queries, NewSafeSession(nil), false, false)
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
			Gateway: sbc0,
		},
		{
			Target: &querypb.Target{
				Keyspace: "TestMultiExecs",
				Shard:    "1",
			},
			Gateway: sbc1,
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
	_ = sc.StreamExecuteMulti(ctx, "query", rss, bvs, nil, func(*sqltypes.Result) error {
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
	hc := discovery.NewFakeLegacyHealthCheck()
	sc := newTestLegacyScatterConn(hc, new(sandboxTopo), "aa")
	hc.AddTestTablet("aa", "0", 1, "TestScatterConnStreamExecuteSendError", "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
	rss, err := res.ResolveDestination(ctx, "TestScatterConnStreamExecuteSendError", topodatapb.TabletType_REPLICA, key.DestinationShard("0"))
	if err != nil {
		t.Fatalf("ResolveDestination failed: %v", err)
	}
	err = sc.StreamExecute(ctx, "query", nil, rss, nil, func(*sqltypes.Result) error {
		return fmt.Errorf("send error")
	})
	want := "send error"
	// Ensure that we handle send errors.
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("got %s, must contain %v", err, want)
	}
}

func TestScatterConnSingleDB(t *testing.T) {
	createSandbox("TestScatterConnSingleDB")
	hc := discovery.NewFakeLegacyHealthCheck()

	hc.Reset()
	sc := newTestLegacyScatterConn(hc, new(sandboxTopo), "aa")
	hc.AddTestTablet("aa", "0", 1, "TestScatterConnSingleDB", "0", topodatapb.TabletType_MASTER, true, 1, nil)
	hc.AddTestTablet("aa", "1", 1, "TestScatterConnSingleDB", "1", topodatapb.TabletType_MASTER, true, 1, nil)

	res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")
	rss0, err := res.ResolveDestination(ctx, "TestScatterConnSingleDB", topodatapb.TabletType_MASTER, key.DestinationShard("0"))
	require.NoError(t, err)
	rss1, err := res.ResolveDestination(ctx, "TestScatterConnSingleDB", topodatapb.TabletType_MASTER, key.DestinationShard("1"))
	require.NoError(t, err)

	want := "multi-db transaction attempted"

	// TransactionMode_SINGLE in session
	session := NewSafeSession(&vtgatepb.Session{InTransaction: true, TransactionMode: vtgatepb.TransactionMode_SINGLE})
	queries := []*querypb.BoundQuery{{Sql: "query1"}}
	_, errors := sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	require.Empty(t, errors)
	_, errors = sc.ExecuteMultiShard(ctx, rss1, queries, session, false, false)
	require.Error(t, errors[0])
	assert.Contains(t, errors[0].Error(), want)

	// TransactionMode_SINGLE in txconn
	sc.txConn.mode = vtgatepb.TransactionMode_SINGLE
	session = NewSafeSession(&vtgatepb.Session{InTransaction: true})
	_, errors = sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	require.Empty(t, errors)
	_, errors = sc.ExecuteMultiShard(ctx, rss1, queries, session, false, false)
	require.Error(t, errors[0])
	assert.Contains(t, errors[0].Error(), want)

	// TransactionMode_MULTI in txconn. Should not fail.
	sc.txConn.mode = vtgatepb.TransactionMode_MULTI
	session = NewSafeSession(&vtgatepb.Session{InTransaction: true})
	_, errors = sc.ExecuteMultiShard(ctx, rss0, queries, session, false, false)
	require.Empty(t, errors)
	_, errors = sc.ExecuteMultiShard(ctx, rss1, queries, session, false, false)
	require.Empty(t, errors)
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

func TestReservePrequeries(t *testing.T) {
	keyspace := "keyspace"
	createSandbox(keyspace)
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, keyspace, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, keyspace, "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	// empty results
	sbc0.SetResults([]*sqltypes.Result{{}})
	sbc1.SetResults([]*sqltypes.Result{{}})

	res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")

	session := NewSafeSession(&vtgatepb.Session{
		InTransaction:  false,
		InReservedConn: true,
		SystemVariables: map[string]string{
			"s1": "'value'",
			"s2": "42",
		},
	})
	destinations := []key.Destination{key.DestinationShard("0")}

	executeOnShards(t, res, keyspace, sc, session, destinations)
	assert.Equal(t, 2+1, len(sbc0.StringQueries()))
}

func newTestLegacyScatterConn(hc discovery.LegacyHealthCheck, serv srvtopo.Server, cell string) *ScatterConn {
	// The topo.Server is used to start watching the cells described
	// in '-cells_to_watch' command line parameter, which is
	// empty by default. So it's unused in this test, set to nil.
	gw := GatewayCreator()(ctx, hc, serv, cell, 3)
	tc := NewTxConn(gw, vtgatepb.TransactionMode_TWOPC)
	return NewLegacyScatterConn("", tc, gw, hc)
}

func newTestScatterConn(hc discovery.HealthCheck, serv srvtopo.Server, cell string) *ScatterConn {
	// The topo.Server is used to start watching the cells described
	// in '-cells_to_watch' command line parameter, which is
	// empty by default. So it's unused in this test, set to nil.
	gw := NewTabletGateway(ctx, hc, serv, cell)
	tc := NewTxConn(gw, vtgatepb.TransactionMode_TWOPC)
	return NewScatterConn("", tc, gw)
}

var ctx = context.Background()
