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
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
)

// This file uses the sandbox_test framework.

func TestExecuteFailOnAutocommit(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	createSandbox("TestExecuteFailOnAutocommit")
	hc := discovery.NewFakeHealthCheck(nil)
	sc := newTestScatterConn(ctx, hc, newSandboxForCells(ctx, []string{"aa"}), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestExecuteFailOnAutocommit", "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestExecuteFailOnAutocommit", "1", topodatapb.TabletType_PRIMARY, true, 1, nil)

	rss := []*srvtopo.ResolvedShard{
		{
			Target: &querypb.Target{
				Keyspace:   "TestExecuteFailOnAutocommit",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			Gateway: sbc0,
		},
		{
			Target: &querypb.Target{
				Keyspace:   "TestExecuteFailOnAutocommit",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
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
				Target:        &querypb.Target{Keyspace: "TestExecuteFailOnAutocommit", Shard: "0", TabletType: topodatapb.TabletType_PRIMARY, Cell: "aa"},
				TransactionId: 123,
				TabletAlias:   nil,
			},
		},
		Autocommit: false,
	}
	_, errs := sc.ExecuteMultiShard(ctx, nil, rss, queries, econtext.NewSafeSession(session), true /*autocommit*/, false, false, nullResultsObserver{}, false)
	err := vterrors.Aggregate(errs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "in autocommit mode, transactionID should be zero but was: 123")
	utils.MustMatch(t, 0, len(sbc0.Queries), "")
	utils.MustMatch(t, []*querypb.BoundQuery{queries[1]}, sbc1.Queries, "")
}

func TestFetchLastInsertIDResets(t *testing.T) {
	// This test verifies that each ExecuteMultiShard call passes the requested
	// FetchLastInsertID to the shards without mutating the shared session options.
	ks := "TestFetchLastInsertIDResets"
	ctx := utils.LeakCheckContext(t)

	createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	sc := newTestScatterConn(ctx, hc, newSandboxForCells(ctx, []string{"aa"}), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, ks, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, ks, "1", topodatapb.TabletType_PRIMARY, true, 1, nil)

	rss := []*srvtopo.ResolvedShard{{
		Target: &querypb.Target{
			Keyspace:   ks,
			Shard:      "0",
			TabletType: topodatapb.TabletType_PRIMARY,
		},
		Gateway: sbc0,
	}, {
		Target: &querypb.Target{
			Keyspace:   ks,
			Shard:      "1",
			TabletType: topodatapb.TabletType_PRIMARY,
		},
		Gateway: sbc1,
	}}
	queries := []*querypb.BoundQuery{{
		Sql: "query1",
		BindVariables: map[string]*querypb.BindVariable{
			"bv0": sqltypes.Int64BindVariable(0),
		},
	}, {
		Sql: "query2",
		BindVariables: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
		},
	}}
	tests := []struct {
		name               string
		initialSessionOpts *querypb.ExecuteOptions
		fetchLastInsertID  bool
		expectSessionNil   bool
		expectFetchLastID  *bool // nil means checkLastOptionNil, otherwise checkLastOption(*bool)
	}{
		{
			name:               "no session options, fetchLastInsertID = false",
			initialSessionOpts: nil,
			fetchLastInsertID:  false,
			expectSessionNil:   true,
			expectFetchLastID:  nil,
		},
		{
			name:               "no session options, fetchLastInsertID = true",
			initialSessionOpts: nil,
			fetchLastInsertID:  true,
			expectSessionNil:   true,

			expectFetchLastID: new(true),
		},
		{
			name:               "session options set, fetchLastInsertID = false",
			initialSessionOpts: &querypb.ExecuteOptions{},
			fetchLastInsertID:  false,
			expectSessionNil:   false,
			expectFetchLastID:  new(false),
		},
		{
			name:               "session options set, fetchLastInsertID = true",
			initialSessionOpts: &querypb.ExecuteOptions{},
			fetchLastInsertID:  true,
			expectSessionNil:   false,
			expectFetchLastID:  new(true),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := econtext.NewSafeSession(nil)
			session.Options = tt.initialSessionOpts

			checkLastOption := func(expected bool) {
				require.Len(t, sbc0.Options, 1)
				options := sbc0.Options[0]
				assert.Equal(t, expected, options.FetchLastInsertId)
				sbc0.Options = nil
			}
			checkLastOptionNil := func() {
				require.Len(t, sbc0.Options, 1)
				assert.Nil(t, sbc0.Options[0])
				sbc0.Options = nil
			}

			_, errs := sc.ExecuteMultiShard(ctx, nil, rss, queries, session, true /*autocommit*/, false, false, nullResultsObserver{}, tt.fetchLastInsertID)
			require.NoError(t, vterrors.Aggregate(errs))

			// The shared session options must not be mutated by the call; the
			// requested FetchLastInsertId travels on a per-call copy instead.
			if tt.expectSessionNil {
				assert.Nil(t, session.Options)
			} else {
				assert.NotNil(t, session.Options)
				assert.False(t, session.Options.FetchLastInsertId)
			}

			if tt.expectFetchLastID == nil {
				checkLastOptionNil()
			} else {
				checkLastOption(*tt.expectFetchLastID)
			}
		})
	}
}

func TestScatterConnSharedOptionsNoRace(t *testing.T) {
	// A streamed UNION runs each source through its own StreamExecuteMulti
	// against the same session. Setting FetchLastInsertId on the shared session
	// options in place raced with the sources marshalling those options for
	// their gRPC requests. The options must be copied per call.
	// ExecuteMultiShard shares the same session and copies the options the same
	// way, so it is raced here too. Meant to run under -race, where the shared
	// write would otherwise be reported.
	ks := "TestScatterConnSharedOptionsNoRace"
	ctx := utils.LeakCheckContext(t)

	createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	sc := newTestScatterConn(ctx, hc, newSandboxForCells(ctx, []string{"aa"}), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, ks, "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, ks, "1", topodatapb.TabletType_PRIMARY, true, 1, nil)
	// The sandboxconn only serializes Execute and StreamExecute against
	// themselves, not against each other; mixing both here needs the shared
	// Queries slice locked.
	sbc0.RequireQueriesLocking()
	sbc1.RequireQueriesLocking()

	rss := []*srvtopo.ResolvedShard{{
		Target:  &querypb.Target{Keyspace: ks, Shard: "0", TabletType: topodatapb.TabletType_PRIMARY},
		Gateway: sbc0,
	}, {
		Target:  &querypb.Target{Keyspace: ks, Shard: "1", TabletType: topodatapb.TabletType_PRIMARY},
		Gateway: sbc1,
	}}
	bindVars := []map[string]*querypb.BindVariable{nil, nil}
	queries := []*querypb.BoundQuery{{Sql: "query1"}, {Sql: "query2"}}

	// A single session shared across all concurrent calls, as a streamed UNION does.
	session := econtext.NewSafeSession(&vtgatepb.Session{Options: &querypb.ExecuteOptions{}})

	var wg sync.WaitGroup
	for i := range 16 {
		fetchLastInsertID := i%2 == 0
		wg.Go(func() {
			errs := sc.StreamExecuteMulti(ctx, nil, "query", rss, bindVars, session, true /*autocommit*/, false, func(*sqltypes.Result) error {
				return nil
			}, nullResultsObserver{}, fetchLastInsertID)
			assert.NoError(t, vterrors.Aggregate(errs))
		})
		wg.Go(func() {
			_, errs := sc.ExecuteMultiShard(ctx, nil, rss, queries, session, true /*autocommit*/, false, false, nullResultsObserver{}, fetchLastInsertID)
			assert.NoError(t, vterrors.Aggregate(errs))
		})
	}
	wg.Wait()

	// The shared session options must not have been mutated by the concurrent calls.
	assert.False(t, session.Options.FetchLastInsertId)
}

func TestExecutePanic(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	createSandbox("TestExecutePanic")
	hc := discovery.NewFakeHealthCheck(nil)
	sc := newTestScatterConn(ctx, hc, newSandboxForCells(ctx, []string{"aa"}), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, "TestExecutePanic", "0", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1", 1, "TestExecutePanic", "1", topodatapb.TabletType_PRIMARY, true, 1, nil)
	sbc0.SetPanic(42)
	sbc1.SetPanic(42)
	rss := []*srvtopo.ResolvedShard{
		{
			Target: &querypb.Target{
				Keyspace:   "TestExecutePanic",
				Shard:      "0",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			Gateway: sbc0,
		},
		{
			Target: &querypb.Target{
				Keyspace:   "TestExecutePanic",
				Shard:      "1",
				TabletType: topodatapb.TabletType_PRIMARY,
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
				Target:        &querypb.Target{Keyspace: "TestExecutePanic", Shard: "0", TabletType: topodatapb.TabletType_PRIMARY, Cell: "aa"},
				TransactionId: 123,
				TabletAlias:   nil,
			},
		},
		Autocommit: false,
	}

	original := log.Error
	defer func() {
		log.Error = original
	}()

	var logMessage string
	log.Error = func(msg string, _ ...slog.Attr) {
		logMessage = msg
	}

	assert.Panics(t, func() {
		_, _ = sc.ExecuteMultiShard(ctx, nil, rss, queries, econtext.NewSafeSession(session), true /*autocommit*/, false, false, nullResultsObserver{}, false)
	})
	require.Contains(t, logMessage, "(*ScatterConn).multiGoTransaction")
}

func TestReservedOnMultiReplica(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	keyspace := "keyspace"
	createSandbox(keyspace)
	hc := discovery.NewFakeHealthCheck(nil)
	sc := newTestScatterConn(ctx, hc, newSandboxForCells(ctx, []string{"aa"}), "aa")
	sbc0_1 := hc.AddTestTablet("aa", "0", 1, keyspace, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc0_2 := hc.AddTestTablet("aa", "2", 1, keyspace, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	//	sbc1 := hc.AddTestTablet("aa", "1", 1, keyspace, "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	// empty results
	sbc0_1.SetResults([]*sqltypes.Result{{}})
	sbc0_2.SetResults([]*sqltypes.Result{{}})

	res := srvtopo.NewResolver(newSandboxForCells(ctx, []string{"aa"}), sc.gateway, "aa")

	session := econtext.NewSafeSession(&vtgatepb.Session{InTransaction: false, InReservedConn: true})
	destinations := []key.ShardDestination{key.DestinationShard("0")}
	for range 10 {
		executeOnShards(t, ctx, res, keyspace, sc, session, destinations)
		assert.EqualValues(t, 1, sbc0_1.ReserveCount.Load()+sbc0_2.ReserveCount.Load(), "sbc0 reserve count")
		assert.EqualValues(t, 0, sbc0_1.BeginCount.Load()+sbc0_2.BeginCount.Load(), "sbc0 begin count")
	}
}

func TestReservedBeginTableDriven(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	type testAction struct {
		transaction, reserved    bool
		shards                   []string
		sbc0Reserve, sbc1Reserve int64
		sbc0Begin, sbc1Begin     int64
	}
	type testCase struct {
		name    string
		actions []testAction
	}

	tests := []testCase{{
		name: "begin",
		actions: []testAction{
			{
				shards:      []string{"0"},
				transaction: true,
				sbc0Begin:   1,
			}, {
				shards:      []string{"0", "1"},
				transaction: true,
				sbc1Begin:   1,
			}, {
				shards:      []string{"0", "1"},
				transaction: true,
				// nothing needs to be done
			},
		},
	}, {
		name: "reserve",
		actions: []testAction{
			{
				shards:      []string{"1"},
				reserved:    true,
				sbc1Reserve: 1,
			}, {
				shards:      []string{"0", "1"},
				reserved:    true,
				sbc0Reserve: 1,
			}, {
				shards:   []string{"0", "1"},
				reserved: true,
				// nothing needs to be done
			},
		},
	}, {
		name: "reserve everywhere",
		actions: []testAction{
			{
				shards:      []string{"0", "1"},
				reserved:    true,
				sbc0Reserve: 1,
				sbc1Reserve: 1,
			},
		},
	}, {
		name: "begin then reserve",
		actions: []testAction{
			{
				shards:      []string{"0"},
				transaction: true,
				sbc0Begin:   1,
			}, {
				shards:      []string{"0", "1"},
				transaction: true,
				reserved:    true,
				sbc0Reserve: 1,
				sbc1Reserve: 1,
				sbc1Begin:   1,
			},
		},
	}, {
		name: "reserve then begin",
		actions: []testAction{
			{
				shards:      []string{"1"},
				reserved:    true,
				sbc1Reserve: 1,
			}, {
				shards:      []string{"0"},
				transaction: true,
				reserved:    true,
				sbc0Reserve: 1,
				sbc0Begin:   1,
			}, {
				shards:      []string{"0", "1"},
				transaction: true,
				reserved:    true,
				sbc1Begin:   1,
			},
		},
	}, {
		name: "reserveBegin",
		actions: []testAction{
			{
				shards:      []string{"1"},
				transaction: true,
				reserved:    true,
				sbc1Reserve: 1,
				sbc1Begin:   1,
			}, {
				shards:      []string{"0"},
				transaction: true,
				reserved:    true,
				sbc0Reserve: 1,
				sbc0Begin:   1,
			}, {
				shards:      []string{"0", "1"},
				transaction: true,
				reserved:    true,
				// nothing needs to be done
			},
		},
	}, {
		name: "reserveBegin everywhere",
		actions: []testAction{
			{
				shards:      []string{"0", "1"},
				transaction: true,
				reserved:    true,
				sbc0Reserve: 1,
				sbc0Begin:   1,
				sbc1Reserve: 1,
				sbc1Begin:   1,
			},
		},
	}}
	for _, test := range tests {
		keyspace := "keyspace"
		createSandbox(keyspace)
		hc := discovery.NewFakeHealthCheck(nil)
		sc := newTestScatterConn(ctx, hc, newSandboxForCells(ctx, []string{"aa"}), "aa")
		sbc0 := hc.AddTestTablet("aa", "0", 1, keyspace, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
		sbc1 := hc.AddTestTablet("aa", "1", 1, keyspace, "1", topodatapb.TabletType_REPLICA, true, 1, nil)

		// empty results
		sbc0.SetResults([]*sqltypes.Result{{}})
		sbc1.SetResults([]*sqltypes.Result{{}})

		res := srvtopo.NewResolver(newSandboxForCells(ctx, []string{"aa"}), sc.gateway, "aa")

		t.Run(test.name, func(t *testing.T) {
			session := econtext.NewSafeSession(&vtgatepb.Session{})
			for _, action := range test.actions {
				session.Session.InTransaction = action.transaction
				session.Session.InReservedConn = action.reserved
				var destinations []key.ShardDestination
				for _, shard := range action.shards {
					destinations = append(destinations, key.DestinationShard(shard))
				}
				executeOnShards(t, ctx, res, keyspace, sc, session, destinations)
				assert.Equal(t, action.sbc0Reserve, sbc0.ReserveCount.Load(), "sbc0 reserve count")
				assert.Equal(t, action.sbc0Begin, sbc0.BeginCount.Load(), "sbc0 begin count")
				assert.Equal(t, action.sbc1Reserve, sbc1.ReserveCount.Load(), "sbc1 reserve count")
				assert.Equal(t, action.sbc1Begin, sbc1.BeginCount.Load(), "sbc1 begin count")
				sbc0.BeginCount.Store(0)
				sbc0.ReserveCount.Store(0)
				sbc1.BeginCount.Store(0)
				sbc1.ReserveCount.Store(0)
			}
		})
	}
}

func TestReservedConnFail(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	keyspace := "keyspace"
	createSandbox(keyspace)
	hc := discovery.NewFakeHealthCheck(nil)
	sc := newTestScatterConn(ctx, hc, newSandboxForCells(ctx, []string{"aa"}), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, keyspace, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	_ = hc.AddTestTablet("aa", "1", 1, keyspace, "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	res := srvtopo.NewResolver(newSandboxForCells(ctx, []string{"aa"}), sc.gateway, "aa")

	session := econtext.NewSafeSession(&vtgatepb.Session{InTransaction: false, InReservedConn: true})
	destinations := []key.ShardDestination{key.DestinationShard("0")}

	executeOnShards(t, ctx, res, keyspace, sc, session, destinations)
	assert.Len(t, session.ShardSessions, 1)
	oldRId := session.Session.ShardSessions[0].ReservedId

	sbc0.EphemeralShardErr = sqlerror.NewSQLError(sqlerror.CRServerGone, sqlerror.SSUnknownSQLState, "lost connection")
	_ = executeOnShardsReturnsErr(t, ctx, res, keyspace, sc, session, destinations)
	assert.Len(t, sbc0.Queries, 3, "1 for the successful run, one for the failed attempt, and one for the retry")
	require.Len(t, session.ShardSessions, 1)
	assert.NotEqual(t, oldRId, session.Session.ShardSessions[0].ReservedId, "should have recreated a reserved connection since the last connection was lost")
	oldRId = session.Session.ShardSessions[0].ReservedId

	sbc0.Queries = nil
	sbc0.EphemeralShardErr = sqlerror.NewSQLError(sqlerror.ERQueryInterrupted, sqlerror.SSUnknownSQLState, "transaction 123 not found")
	_ = executeOnShardsReturnsErr(t, ctx, res, keyspace, sc, session, destinations)
	assert.Len(t, sbc0.Queries, 2, "one for the failed attempt, and one for the retry")
	require.Len(t, session.ShardSessions, 1)
	assert.NotEqual(t, oldRId, session.Session.ShardSessions[0].ReservedId, "should have recreated a reserved connection since the last connection was lost")
	oldRId = session.Session.ShardSessions[0].ReservedId

	sbc0.Queries = nil
	sbc0.EphemeralShardErr = sqlerror.NewSQLError(sqlerror.ERQueryInterrupted, sqlerror.SSUnknownSQLState, "transaction 123 ended at 2020-01-20")
	_ = executeOnShardsReturnsErr(t, ctx, res, keyspace, sc, session, destinations)
	assert.Len(t, sbc0.Queries, 2, "one for the failed attempt, and one for the retry")
	require.Len(t, session.ShardSessions, 1)
	assert.NotEqual(t, oldRId, session.Session.ShardSessions[0].ReservedId, "should have recreated a reserved connection since the last connection was lost")
	oldRId = session.Session.ShardSessions[0].ReservedId

	sbc0.Queries = nil
	sbc0.EphemeralShardErr = sqlerror.NewSQLError(sqlerror.ERQueryInterrupted, sqlerror.SSUnknownSQLState, "transaction 123 in use: for tx killer rollback")
	_ = executeOnShardsReturnsErr(t, ctx, res, keyspace, sc, session, destinations)
	assert.Len(t, sbc0.Queries, 2, "one for the failed attempt, and one for the retry")
	require.Len(t, session.ShardSessions, 1)
	assert.NotEqual(t, oldRId, session.Session.ShardSessions[0].ReservedId, "should have recreated a reserved connection since the last connection was lost")
	oldRId = session.Session.ShardSessions[0].ReservedId

	sbc0.Queries = nil
	sbc0.EphemeralShardErr = vterrors.New(vtrpcpb.Code_CLUSTER_EVENT, "operation not allowed in state NOT_SERVING during query: query1")
	_ = executeOnShardsReturnsErr(t, ctx, res, keyspace, sc, session, destinations)
	assert.Len(t, sbc0.Queries, 2, "one for the failed attempt, and one for the retry")
	require.Len(t, session.ShardSessions, 1)
	assert.NotEqual(t, oldRId, session.Session.ShardSessions[0].ReservedId, "should have recreated a reserved connection since the last connection was lost")
	oldRId = session.Session.ShardSessions[0].ReservedId

	sbc0.Queries = nil
	sbc0.EphemeralShardErr = vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "invalid tablet type: REPLICA, want: PRIMARY")
	_ = executeOnShardsReturnsErr(t, ctx, res, keyspace, sc, session, destinations)
	assert.Len(t, sbc0.Queries, 2, "one for the failed attempt, and one for the retry")
	require.Len(t, session.ShardSessions, 1)
	assert.NotEqual(t, oldRId, session.Session.ShardSessions[0].ReservedId, "should have recreated a reserved connection since the last connection was lost")
	oldRId = session.Session.ShardSessions[0].ReservedId
	oldAlias := session.Session.ShardSessions[0].TabletAlias

	// Test Setup
	tablet0 := sbc0.Tablet()
	ths := hc.GetHealthyTabletStats(&querypb.Target{
		Keyspace:   tablet0.GetKeyspace(),
		Shard:      tablet0.GetShard(),
		TabletType: tablet0.GetType(),
	})
	sbc0Th := ths[0]
	sbc0Th.Serving = false
	sbc0.NotServing = true
	sbc0Rep := hc.AddTestTablet("aa", "0", 2, keyspace, "0", topodatapb.TabletType_REPLICA, true, 1, nil)

	sbc0.Queries = nil
	sbc0.ExecCount.Store(0)
	_ = executeOnShardsReturnsErr(t, ctx, res, keyspace, sc, session, destinations)
	assert.EqualValues(t, 1, sbc0.ExecCount.Load(), "first attempt should be made on original tablet")
	assert.Empty(t, sbc0.Queries, "no query should be executed on it")
	assert.Len(t, sbc0Rep.Queries, 1, "this attempt on new healthy tablet should pass")
	require.Len(t, session.ShardSessions, 1)
	assert.NotEqual(t, oldRId, session.Session.ShardSessions[0].ReservedId, "should have recreated a reserved connection since the last connection was lost")
	assert.NotEqual(t, oldAlias, session.Session.ShardSessions[0].TabletAlias, "tablet alias should have changed as this is a different tablet")
	oldRId = session.Session.ShardSessions[0].ReservedId
	oldAlias = session.Session.ShardSessions[0].TabletAlias

	// Test Setup
	tablet0Rep := sbc0Rep.Tablet()
	newThs := hc.GetHealthyTabletStats(&querypb.Target{
		Keyspace:   tablet0Rep.GetKeyspace(),
		Shard:      tablet0Rep.GetShard(),
		TabletType: tablet0Rep.GetType(),
	})
	sbc0RepTh := newThs[0]
	sbc0RepTh.Target = &querypb.Target{
		Keyspace:   tablet0Rep.GetKeyspace(),
		Shard:      tablet0Rep.GetShard(),
		TabletType: topodatapb.TabletType_SPARE,
	}
	sbc0Rep.Tablet().Type = topodatapb.TabletType_SPARE
	sbc0Th.Serving = true
	sbc0.NotServing = false
	sbc0.ExecCount.Store(0)

	sbc0Rep.Queries = nil
	sbc0Rep.ExecCount.Store(0)
	_ = executeOnShardsReturnsErr(t, ctx, res, keyspace, sc, session, destinations)
	assert.EqualValues(t, 1, sbc0Rep.ExecCount.Load(), "first attempt should be made on the changed tablet type")
	assert.Empty(t, sbc0Rep.Queries, "no query should be executed on it")
	assert.Len(t, sbc0.Queries, 1, "this attempt should pass as it is on new healthy tablet and matches the target")
	require.Len(t, session.ShardSessions, 1)
	assert.NotEqual(t, oldRId, session.Session.ShardSessions[0].ReservedId, "should have recreated a reserved connection since the last connection was lost")
	assert.NotEqual(t, oldAlias, session.Session.ShardSessions[0].TabletAlias, "tablet alias should have changed as this is a different tablet")
}

// TestEndActionRollbackMarking pins which shard-error codes mark the
// transaction for rollback. The DEADLINE_EXCEEDED and CANCELED rows are the
// contract the tablet's brief-hold wait-out (TxPool.GetAndLock) relies on: a
// caller whose own context ends the wait for an internal keepalive or
// activity-refresh hold never acquired the connection, so its error must not
// roll back an otherwise untouched transaction — which the ABORTED shape
// would.
func TestEndActionRollbackMarking(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	keyspace := "keyspace"
	createSandbox(keyspace)
	hc := discovery.NewFakeHealthCheck(nil)
	sc := newTestScatterConn(ctx, hc, newSandboxForCells(ctx, []string{"aa"}), "aa")
	target := &querypb.Target{Keyspace: keyspace, Shard: "0", TabletType: topodatapb.TabletType_PRIMARY}

	cases := []struct {
		name         string
		err          error
		mustRollback bool
	}{{
		name: "deadline-ended brief-hold wait",
		err: vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED,
			"transaction 123: context deadline exceeded: in use: for temp-table activity refresh"),
		mustRollback: false,
	}, {
		name: "canceled brief-hold wait",
		err: vterrors.Errorf(vtrpcpb.Code_CANCELED,
			"transaction 123: context canceled: in use: for reserved connection keepalive"),
		mustRollback: false,
	}, {
		name:         "aborted in-use",
		err:          vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction 123: in use: for query"),
		mustRollback: true,
	}, {
		name:         "resource exhausted",
		err:          vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "pool full"),
		mustRollback: true,
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			session := econtext.NewSafeSession(&vtgatepb.Session{InTransaction: true})
			startTime, statsKey := sc.startAction("Execute", target)
			err := tc.err
			sc.endAction(startTime, &concurrency.AllErrorRecorder{}, statsKey, &err, session)
			assert.Equal(t, tc.mustRollback, session.MustRollback())
		})
	}
}

func TestIsConnClosed(t *testing.T) {
	testCases := []struct {
		name      string
		err       error
		conClosed bool
	}{{
		"server gone",
		sqlerror.NewSQLError(sqlerror.CRServerGone, sqlerror.SSNetError, ""),
		true,
	}, {
		"connection lost",
		sqlerror.NewSQLError(sqlerror.CRServerLost, sqlerror.SSNetError, ""),
		true,
	}, {
		"tx ended",
		sqlerror.NewSQLError(sqlerror.ERQueryInterrupted, sqlerror.SSUnknownSQLState, "transaction 111 ended at ..."),
		true,
	}, {
		"tx not found",
		sqlerror.NewSQLError(sqlerror.ERQueryInterrupted, sqlerror.SSUnknownSQLState, "transaction 111 not found ..."),
		true,
	}, {
		"tx not found missing tx id",
		sqlerror.NewSQLError(sqlerror.ERQueryInterrupted, sqlerror.SSUnknownSQLState, "transaction not found"),
		false,
	}, {
		"tx getting killed by tx killer",
		sqlerror.NewSQLError(sqlerror.ERQueryInterrupted, sqlerror.SSUnknownSQLState, "transaction 111 in use: for tx killer rollback"),
		true,
	}}

	for _, tCase := range testCases {
		t.Run(tCase.name, func(t *testing.T) {
			assert.Equal(t, tCase.conClosed, wasConnectionClosed(tCase.err))
		})
	}
}

// TestActionInfoWithTabletAlias tests the actionInfo function with tablet-specific routing.
func TestActionInfoWithTabletAlias(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	target := &querypb.Target{
		Keyspace:   "ks",
		Shard:      "-80",
		TabletType: topodatapb.TabletType_PRIMARY,
	}
	tabletAlias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}

	t.Run("non-transactional with tablet alias", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{})
		session.SetTargetTabletAlias(tabletAlias)

		info, shardSession, err := actionInfo(ctx, target, session, false, false, vtgatepb.TransactionMode_MULTI)
		require.NoError(t, err)
		assert.Nil(t, shardSession)
		assert.Equal(t, nothing, info.actionNeeded)
		assert.Equal(t, tabletAlias, info.alias)
	})

	t.Run("transaction begin with tablet alias", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{
			InTransaction: true,
		})
		session.SetTargetTabletAlias(tabletAlias)

		info, shardSession, err := actionInfo(ctx, target, session, false, false, vtgatepb.TransactionMode_MULTI)
		require.NoError(t, err)
		assert.Nil(t, shardSession)
		assert.Equal(t, begin, info.actionNeeded)
		assert.Equal(t, tabletAlias, info.alias)
	})

	t.Run("existing transaction with different tablet alias errors", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{
			InTransaction: true,
			ShardSessions: []*vtgatepb.Session_ShardSession{{
				Target:        target,
				TransactionId: 12345,
				TabletAlias:   &topodatapb.TabletAlias{Cell: "zone1", Uid: 50},
			}},
		})
		session.SetTargetTabletAlias(tabletAlias) // zone1-100, different from zone1-50

		_, _, err := actionInfo(ctx, target, session, false, false, vtgatepb.TransactionMode_MULTI)
		require.ErrorContains(t, err, "cannot change tablet target mid-transaction")
	})

	t.Run("no tablet alias - existing behavior", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{})

		info, shardSession, err := actionInfo(ctx, target, session, false, false, vtgatepb.TransactionMode_MULTI)
		require.NoError(t, err)
		assert.Nil(t, shardSession)
		assert.Equal(t, nothing, info.actionNeeded)
		assert.Nil(t, info.alias)
	})
}

// A statement that asks for the session's settings on its connection gets a reserve
// action for itself, in or out of a transaction, without the session being pinned.
func TestActionInfoSettingsForStatement(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	target := &querypb.Target{Keyspace: "ks", Shard: "-80", TabletType: topodatapb.TabletType_PRIMARY}

	t.Run("plain session", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{})
		info, _, err := actionInfo(ctx, target, session, false, true, vtgatepb.TransactionMode_MULTI)
		require.NoError(t, err)
		assert.Equal(t, reserve, info.actionNeeded)
		assert.False(t, session.InReservedConn())
	})

	t.Run("in a transaction the settings go on the transaction's connection", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{
			InTransaction: true,
			ShardSessions: []*vtgatepb.Session_ShardSession{{Target: target, TransactionId: 12345}},
		})
		info, _, err := actionInfo(ctx, target, session, false, true, vtgatepb.TransactionMode_MULTI)
		require.NoError(t, err)
		assert.Equal(t, reserve, info.actionNeeded)
		assert.EqualValues(t, 12345, info.transactionID)
	})

	t.Run("a new transaction reserves and begins", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{InTransaction: true})
		info, _, err := actionInfo(ctx, target, session, false, true, vtgatepb.TransactionMode_MULTI)
		require.NoError(t, err)
		assert.Equal(t, reserveBegin, info.actionNeeded)
	})

	t.Run("a pinned session keeps its connection", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{
			InReservedConn: true,
			ShardSessions:  []*vtgatepb.Session_ShardSession{{Target: target, ReservedId: 7}},
		})
		info, _, err := actionInfo(ctx, target, session, false, true, vtgatepb.TransactionMode_MULTI)
		require.NoError(t, err)
		assert.Equal(t, nothing, info.actionNeeded)
		assert.EqualValues(t, 7, info.reservedID)
	})

	t.Run("without the request the session decides", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{})
		info, _, err := actionInfo(ctx, target, session, false, false, vtgatepb.TransactionMode_MULTI)
		require.NoError(t, err)
		assert.Equal(t, nothing, info.actionNeeded)
	})
}

// The settings travel with the statement; whether the session ends up pinned is the
// tablet's answer: none for a statement served from the settings pool, a reserved
// id for one that needed a connection of its own.
func TestSettingsForStatementPinsOnlyWhenTheTabletReserved(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	keyspace := "keyspace"
	createSandbox(keyspace)
	hc := discovery.NewFakeHealthCheck(nil)
	sc := newTestScatterConn(ctx, hc, newSandboxForCells(ctx, []string{"aa"}), "aa")
	sbc := hc.AddTestTablet("aa", "0", 1, keyspace, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	res := srvtopo.NewResolver(newSandboxForCells(ctx, []string{"aa"}), sc.gateway, "aa")
	rss, _, err := res.ResolveDestinations(ctx, keyspace, topodatapb.TabletType_REPLICA, nil, []key.ShardDestination{key.DestinationShard("0")})
	require.NoError(t, err)
	queries := []*querypb.BoundQuery{{Sql: "query1", BindVariables: map[string]*querypb.BindVariable{}}}
	execute := func(t *testing.T, session *econtext.SafeSession, settingsForStatement bool) {
		t.Helper()
		_, errs := sc.ExecuteMultiShard(ctx, nil, rss, queries, session, false, settingsForStatement, false, nullResultsObserver{}, false)
		require.NoError(t, vterrors.Aggregate(errs))
	}
	newSession := func() *econtext.SafeSession {
		return econtext.NewSafeSession(&vtgatepb.Session{SystemVariables: map[string]string{"sql_safe_updates": "1"}})
	}

	t.Run("served from the settings pool", func(t *testing.T) {
		sbc.NoReservation = true
		defer func() { sbc.NoReservation = false }()
		sbc.ReserveCount.Store(0)
		session := newSession()
		execute(t, session, true)
		assert.EqualValues(t, 1, sbc.ReserveCount.Load())
		assert.False(t, session.InReservedConn())
		assert.Empty(t, session.ShardSessions)

		// the next statement of the session is a plain execute again
		execute(t, session, false)
		assert.EqualValues(t, 1, sbc.ReserveCount.Load())
	})

	t.Run("reserved by the tablet", func(t *testing.T) {
		sbc.ReserveCount.Store(0)
		session := newSession()
		execute(t, session, true)
		assert.EqualValues(t, 1, sbc.ReserveCount.Load())
		assert.True(t, session.InReservedConn())
		require.Len(t, session.ShardSessions, 1)
		assert.NotZero(t, session.ShardSessions[0].ReservedId)

		// the next statement of the session runs on the reserved connection
		execute(t, session, false)
		assert.EqualValues(t, 1, sbc.ReserveCount.Load())
	})
}
