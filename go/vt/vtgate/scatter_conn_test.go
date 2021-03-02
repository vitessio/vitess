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
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/key"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
)

// This file uses the sandbox_test framework.

func TestExecuteFailOnAutocommit(t *testing.T) {

	createSandbox("TestExecuteFailOnAutocommit")
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, new(sandboxTopo), "aa")
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

func TestReservedOnMultiReplica(t *testing.T) {
	keyspace := "keyspace"
	createSandbox(keyspace)
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0_1 := hc.AddTestTablet("aa", "0", 1, keyspace, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	sbc0_2 := hc.AddTestTablet("aa", "2", 1, keyspace, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	//	sbc1 := hc.AddTestTablet("aa", "1", 1, keyspace, "1", topodatapb.TabletType_REPLICA, true, 1, nil)

	// empty results
	sbc0_1.SetResults([]*sqltypes.Result{{}})
	sbc0_2.SetResults([]*sqltypes.Result{{}})

	res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: false, InReservedConn: true})
	destinations := []key.Destination{key.DestinationShard("0")}
	for i := 0; i < 10; i++ {
		executeOnShards(t, res, keyspace, sc, session, destinations)
		assert.EqualValues(t, 1, sbc0_1.ReserveCount.Get()+sbc0_2.ReserveCount.Get(), "sbc0 reserve count")
		assert.EqualValues(t, 0, sbc0_1.BeginCount.Get()+sbc0_2.BeginCount.Get(), "sbc0 begin count")
	}
}

func TestReservedBeginTableDriven(t *testing.T) {
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
			}},
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
			}},
	}, {
		name: "reserve everywhere",
		actions: []testAction{
			{
				shards:      []string{"0", "1"},
				reserved:    true,
				sbc0Reserve: 1,
				sbc1Reserve: 1,
			}},
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
			}},
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
			}},
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
			}},
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
			}},
	}}
	for _, test := range tests {
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

		t.Run(test.name, func(t *testing.T) {
			session := NewSafeSession(&vtgatepb.Session{})
			for _, action := range test.actions {
				session.Session.InTransaction = action.transaction
				session.Session.InReservedConn = action.reserved
				var destinations []key.Destination
				for _, shard := range action.shards {
					destinations = append(destinations, key.DestinationShard(shard))
				}
				executeOnShards(t, res, keyspace, sc, session, destinations)
				assert.EqualValues(t, action.sbc0Reserve, sbc0.ReserveCount.Get(), "sbc0 reserve count")
				assert.EqualValues(t, action.sbc0Begin, sbc0.BeginCount.Get(), "sbc0 begin count")
				assert.EqualValues(t, action.sbc1Reserve, sbc1.ReserveCount.Get(), "sbc1 reserve count")
				assert.EqualValues(t, action.sbc1Begin, sbc1.BeginCount.Get(), "sbc1 begin count")
				sbc0.BeginCount.Set(0)
				sbc0.ReserveCount.Set(0)
				sbc1.BeginCount.Set(0)
				sbc1.ReserveCount.Set(0)
			}
		})
	}
}

func TestReservedConnFail(t *testing.T) {
	keyspace := "keyspace"
	createSandbox(keyspace)
	hc := discovery.NewFakeHealthCheck()
	sc := newTestScatterConn(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "0", 1, keyspace, "0", topodatapb.TabletType_REPLICA, true, 1, nil)
	_ = hc.AddTestTablet("aa", "1", 1, keyspace, "1", topodatapb.TabletType_REPLICA, true, 1, nil)
	res := srvtopo.NewResolver(&sandboxTopo{}, sc.gateway, "aa")

	session := NewSafeSession(&vtgatepb.Session{InTransaction: false, InReservedConn: true})
	destinations := []key.Destination{key.DestinationShard("0")}

	executeOnShards(t, res, keyspace, sc, session, destinations)
	assert.Equal(t, 1, len(session.ShardSessions))
	oldRId := session.Session.ShardSessions[0].ReservedId

	sbc0.EphemeralShardErr = mysql.NewSQLError(mysql.CRServerGone, mysql.SSUnknownSQLState, "lost connection")
	_ = executeOnShardsReturnsErr(t, res, keyspace, sc, session, destinations)
	assert.Equal(t, 3, len(sbc0.Queries), "1 for the successful run, one for the failed attempt, and one for the retry")
	require.Equal(t, 1, len(session.ShardSessions))
	assert.NotEqual(t, oldRId, session.Session.ShardSessions[0].ReservedId, "should have recreated a reserved connection since the last connection was lost")
	oldRId = session.Session.ShardSessions[0].ReservedId

	sbc0.Queries = nil
	sbc0.EphemeralShardErr = mysql.NewSQLError(mysql.ERQueryInterrupted, mysql.SSUnknownSQLState, "transaction 123 not found")
	_ = executeOnShardsReturnsErr(t, res, keyspace, sc, session, destinations)
	assert.Equal(t, 2, len(sbc0.Queries), "one for the failed attempt, and one for the retry")
	require.Equal(t, 1, len(session.ShardSessions))
	assert.NotEqual(t, oldRId, session.Session.ShardSessions[0].ReservedId, "should have recreated a reserved connection since the last connection was lost")
	oldRId = session.Session.ShardSessions[0].ReservedId

	sbc0.Queries = nil
	sbc0.EphemeralShardErr = mysql.NewSQLError(mysql.ERQueryInterrupted, mysql.SSUnknownSQLState, "transaction 123 ended at 2020-01-20")
	_ = executeOnShardsReturnsErr(t, res, keyspace, sc, session, destinations)
	assert.Equal(t, 2, len(sbc0.Queries), "one for the failed attempt, and one for the retry")
	require.Equal(t, 1, len(session.ShardSessions))
	assert.NotEqual(t, oldRId, session.Session.ShardSessions[0].ReservedId, "should have recreated a reserved connection since the last connection was lost")
}

func TestIsConnClosed(t *testing.T) {
	var testCases = []struct {
		name      string
		err       error
		conClosed bool
	}{{
		"server gone",
		mysql.NewSQLError(mysql.CRServerGone, mysql.SSNetError, ""),
		true,
	}, {
		"connection lost",
		mysql.NewSQLError(mysql.CRServerLost, mysql.SSNetError, ""),
		true,
	}, {
		"tx ended",
		mysql.NewSQLError(mysql.ERQueryInterrupted, mysql.SSUnknownSQLState, "transaction 111 ended at ..."),
		true,
	}, {
		"tx not found",
		mysql.NewSQLError(mysql.ERQueryInterrupted, mysql.SSUnknownSQLState, "transaction 111 not found ..."),
		true,
	}, {
		"tx not found missing tx id",
		mysql.NewSQLError(mysql.ERQueryInterrupted, mysql.SSUnknownSQLState, "transaction not found"),
		false,
	}}

	for _, tCase := range testCases {
		t.Run(tCase.name, func(t *testing.T) {
			assert.Equal(t, tCase.conClosed, wasConnectionClosed(tCase.err))
		})
	}
}
