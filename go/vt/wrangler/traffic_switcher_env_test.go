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

package wrangler

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

const (
	streamInfoQuery    = "select id, source, message, cell, tablet_types, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where workflow='%s' and db_name='vt_%s'"
	streamExtInfoQuery = "select id, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, transaction_timestamp, time_heartbeat, time_throttled, component_throttled, message, tags, workflow_type, workflow_sub_type, defer_secondary_keys, rows_copied from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'"
	copyStateQuery     = "select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (%s) and id in (select max(id) from _vt.copy_state where vrepl_id in (%s) group by vrepl_id, table_name)"
	maxValForSequence  = "select max(`id`) as maxval from `vt_%s`.`%s`"
)

var (
	streamInfoKs         = fmt.Sprintf(streamInfoQuery, "test", "ks")
	reverseStreamInfoKs1 = fmt.Sprintf(streamInfoQuery, "test_reverse", "ks1")
	streamInfoKs2        = fmt.Sprintf(streamInfoQuery, "test", "ks2")

	streamExtInfoKs2        = fmt.Sprintf(streamExtInfoQuery, "ks2", "test")
	reverseStreamExtInfoKs2 = fmt.Sprintf(streamExtInfoQuery, "ks2", "test_reverse")
	reverseStreamExtInfoKs1 = fmt.Sprintf(streamExtInfoQuery, "ks1", "test_reverse")
	streamExtInfoKs         = fmt.Sprintf(streamExtInfoQuery, "ks", "test")
)

type testMigraterEnv struct {
	ts                  *topo.Server
	wr                  *Wrangler
	sourcePrimaries     []*fakeTablet
	targetPrimaries     []*fakeTablet
	additionalPrimaries []*fakeTablet
	dbSourceClients     []*fakeDBClient
	dbTargetClients     []*fakeDBClient
	dbAdditionalClients []*fakeDBClient
	allDBClients        []*fakeDBClient
	targetKeyspace      string
	sourceShards        []string
	targetShards        []string
	sourceKeyRanges     []*topodatapb.KeyRange
	targetKeyRanges     []*topodatapb.KeyRange
	tmeDB               *fakesqldb.DB
	mu                  sync.Mutex
}

// testShardMigraterEnv has some convenience functions for adding expected queries.
// They are approximate and should be only used to test other features like stream migration.
// Use explicit queries for testing the actual shard migration.
type testShardMigraterEnv struct {
	testMigraterEnv
}

// tablet picker requires these to be set, otherwise it errors out. also the values need to match an existing
// tablet, otherwise it sleeps until it retries, causing tests to timeout and hence break
// we set these for each new migater env to be the first source shard
// the tests don't depend on which tablet is picked, so this works for now
type testTabletPickerChoice struct {
	keyspace string
	shard    string
}

var tpChoice *testTabletPickerChoice

func newTestTableMigrater(ctx context.Context, t *testing.T) *testMigraterEnv {
	return newTestTableMigraterCustom(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"}, "select * %s")
}

// newTestTableMigraterCustom creates a customized test tablet migrater.
// fmtQuery should be of the form: 'select a, b %s group by a'.
// The test will Sprintf a from clause and where clause as needed.
func newTestTableMigraterCustom(ctx context.Context, t *testing.T, sourceShards, targetShards []string, fmtQuery string) *testMigraterEnv {
	tme := &testMigraterEnv{}
	tme.ts = memorytopo.NewServer(ctx, "cell1", "cell2")
	tme.wr = New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), tme.ts, tmclient.NewTabletManagerClient())
	tme.wr.sem = semaphore.NewWeighted(1)
	tme.sourceShards = sourceShards
	tme.targetShards = targetShards
	tme.tmeDB = fakesqldb.New(t)
	useSequences := false
	if len(sourceShards) == 1 && len(targetShards) > 1 {
		useSequences = true
	}
	expectVDiffQueries(tme.tmeDB)
	tabletID := 10
	for _, shard := range sourceShards {
		tme.sourcePrimaries = append(tme.sourcePrimaries, newFakeTablet(t, tme.wr, "cell1", uint32(tabletID), topodatapb.TabletType_PRIMARY, tme.tmeDB, TabletKeyspaceShard(t, "ks1", shard)))
		tabletID += 10

		_, sourceKeyRange, err := topo.ValidateShardName(shard)
		if err != nil {
			t.Fatal(err)
		}
		tme.sourceKeyRanges = append(tme.sourceKeyRanges, sourceKeyRange)
	}
	tpChoiceTablet := tme.sourcePrimaries[0].Tablet
	tpChoice = &testTabletPickerChoice{
		keyspace: tpChoiceTablet.Keyspace,
		shard:    tpChoiceTablet.Shard,
	}
	for _, shard := range targetShards {
		tme.targetPrimaries = append(tme.targetPrimaries, newFakeTablet(t, tme.wr, "cell1", uint32(tabletID), topodatapb.TabletType_PRIMARY, tme.tmeDB, TabletKeyspaceShard(t, "ks2", shard)))
		tabletID += 10

		_, targetKeyRange, err := topo.ValidateShardName(shard)
		if err != nil {
			t.Fatal(err)
		}
		tme.targetKeyRanges = append(tme.targetKeyRanges, targetKeyRange)
	}

	dialerName := fmt.Sprintf("TrafficSwitcherTest-%s-%d", t.Name(), rand.Intn(1000000000))
	tabletconn.RegisterDialer(dialerName, func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		tme.mu.Lock()
		defer tme.mu.Unlock()
		allPrimaries := append(tme.sourcePrimaries, tme.targetPrimaries...)
		for _, ft := range append(allPrimaries, tme.additionalPrimaries...) {
			if ft.Tablet.Alias.Uid == tablet.Alias.Uid {
				return ft, nil
			}
		}
		return nil, nil
	})
	tabletconntest.SetProtocol("go.vt.wrangler.traffic_switcher_env_test", dialerName)

	vs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "hash",
				}},
			},
			"t2": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "hash",
				}},
			},
		},
	}
	schema := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name: "t1",
			},
			{
				Name: "t2",
			},
		},
	}
	tme.setPrimarySchemas(schema)
	if len(sourceShards) != 1 {
		if err := tme.ts.SaveVSchema(ctx, "ks1", vs); err != nil {
			t.Fatal(err)
		}
	}
	if len(targetShards) != 1 {
		if err := tme.ts.SaveVSchema(ctx, "ks2", vs); err != nil {
			t.Fatal(err)
		}
	}
	if useSequences {
		// Add another unsharded keyspace with sequence tables in
		// order to test sequence handling.
		uvs := &vschemapb.Keyspace{
			Sharded: false,
			Tables: map[string]*vschemapb.Table{
				"t1_seq": {
					Type: vindexes.TypeSequence,
				},
				"t2_seq": {
					Type: vindexes.TypeSequence,
				},
			},
		}
		tabletID += 10
		gfdb := fakesqldb.New(t)
		tme.additionalPrimaries = append(tme.additionalPrimaries, newFakeTablet(t, tme.wr, "cell1", uint32(tabletID), topodatapb.TabletType_PRIMARY, gfdb, TabletKeyspaceShard(t, "global", "0")))
		if err := tme.ts.SaveVSchema(ctx, "global", uvs); err != nil {
			t.Fatal(err)
		}

		// Now use these sequence tables in the target sharded keyspace.
		tks := vs.CloneVT()
		tks.Tables["t1"].AutoIncrement = &vschemapb.AutoIncrement{
			Column:   "id",
			Sequence: "t1_seq",
		}
		tks.Tables["t2"].AutoIncrement = &vschemapb.AutoIncrement{
			Column:   "id",
			Sequence: "t2_seq",
		}
		if err := tme.ts.SaveVSchema(ctx, "ks2", tks); err != nil {
			t.Fatal(err)
		}

		// Now tell the fakesqldb used by the target keyspace tablets to expect
		// the sequence management related queries against the target keyspace.
		tme.tmeDB.AddQuery(fmt.Sprintf(maxValForSequence, "ks2", "t1"),
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"maxval",
					"int64",
				),
				"NULL",
			),
		)
		tme.tmeDB.AddQuery(fmt.Sprintf(maxValForSequence, "ks2", "t2"),
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"maxval",
					"int64",
				),
				"7",
			),
		)

		// Now tell the fakesqldb used by the global keyspace tablets to expect
		// the sequence management related queries against the target keyspace.
		gfdb.AddQuery(
			sqlparser.BuildParsedQuery(sqlInitSequenceTable, sqlescape.EscapeID("vt_global"), sqlescape.EscapeID("t1_seq"), 1, 1, 1).Query,
			&sqltypes.Result{RowsAffected: 0},
		)
		gfdb.AddQuery(
			sqlparser.BuildParsedQuery(sqlInitSequenceTable, sqlescape.EscapeID("vt_global"), sqlescape.EscapeID("t2_seq"), 8, 8, 8).Query,
			&sqltypes.Result{RowsAffected: 0},
		)
	}
	if err := tme.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		t.Fatal(err)
	}
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), tme.ts, "ks1", []string{"cell1"}, false)
	if err != nil {
		t.Fatal(err)
	}
	err = topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), tme.ts, "ks2", []string{"cell1"}, false)
	if err != nil {
		t.Fatal(err)
	}

	tme.startTablets(t)
	tme.createDBClients(ctx, t)
	tme.setPrimaryPositions()
	now := time.Now().Unix()
	for i, targetShard := range targetShards {
		var streamInfoRows []string
		var streamExtInfoRows []string
		var vreplIDs []string
		for j, sourceShard := range sourceShards {
			bls := &binlogdatapb.BinlogSource{
				Keyspace: "ks1",
				Shard:    sourceShard,
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: fmt.Sprintf(fmtQuery, fmt.Sprintf("from t1 where in_keyrange('%s')", targetShard)),
					}, {
						Match:  "t2",
						Filter: fmt.Sprintf(fmtQuery, fmt.Sprintf("from t2 where in_keyrange('%s')", targetShard)),
					}},
				},
			}
			streamInfoRows = append(streamInfoRows, fmt.Sprintf("%d|%v||||1|0|0", j+1, bls))
			streamExtInfoRows = append(streamExtInfoRows, fmt.Sprintf("%d|||||Running|vt_ks1|%d|%d|0|0||1||0", j+1, now, now))
			vreplIDs = append(vreplIDs, strconv.FormatInt(int64(j+1), 10))
		}
		vreplIDsJoined := strings.Join(vreplIDs, ", ")
		tme.dbTargetClients[i].addInvariant(fmt.Sprintf(copyStateQuery, vreplIDsJoined, vreplIDsJoined), noResult)
		tme.dbTargetClients[i].addInvariant(streamInfoKs2, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|varchar|varchar|varchar|int64|int64|int64"),
			streamInfoRows...))
		tme.dbTargetClients[i].addInvariant(streamExtInfoKs2, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|pos|stop_pos|max_replication_lag|state|db_name|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|message|tags|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|int64|int64|int64|varchar|varchar|int64|int64|int64|int64|int64|varchar|varchar|int64|int64|int64"),
			streamExtInfoRows...))
		tme.dbTargetClients[i].addInvariant(reverseStreamExtInfoKs2, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|pos|stop_pos|max_replication_lag|state|db_name|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|message|tags|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|int64|int64|int64|varchar|varchar|int64|int64|int64|int64|int64|varchar|varchar|int64|int64|int64"),
			streamExtInfoRows...))
	}

	for i, sourceShard := range sourceShards {
		var streamInfoRows []string
		var vreplIDs []string
		for j, targetShard := range targetShards {
			bls := &binlogdatapb.BinlogSource{
				Keyspace: "ks2",
				Shard:    targetShard,
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: fmt.Sprintf(fmtQuery, fmt.Sprintf("from t1 where in_keyrange('%s')", sourceShard)),
					}, {
						Match:  "t2",
						Filter: fmt.Sprintf(fmtQuery, fmt.Sprintf("from t2 where in_keyrange('%s')", sourceShard)),
					}},
				},
			}
			streamInfoRows = append(streamInfoRows, fmt.Sprintf("%d|%v||||1|0|0", j+1, bls))
			vreplIDs = append(vreplIDs, strconv.FormatInt(int64(j+1), 10))
		}
		vreplIDsJoined := strings.Join(vreplIDs, ", ")
		tme.dbTargetClients[i].addInvariant(fmt.Sprintf(copyStateQuery, vreplIDsJoined, vreplIDsJoined), noResult)
		tme.dbSourceClients[i].addInvariant(reverseStreamInfoKs1, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|varchar|varchar|varchar|int64|int64|int64"),
			streamInfoRows...),
		)
	}

	if err := topotools.SaveRoutingRules(ctx, tme.wr.ts, map[string][]string{
		"t1":     {"ks1.t1"},
		"ks2.t1": {"ks1.t1"},
		"t2":     {"ks1.t2"},
		"ks2.t2": {"ks1.t2"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tme.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		t.Fatal(err)
	}

	tme.targetKeyspace = "ks2"
	return tme
}

// newTestTablePartialMigrater creates a test tablet migrater
// specifically for partial or shard by shard migrations.
// The shards must be the same on the source and target, and we
// must be moving a subset of them.
// fmtQuery should be of the form: 'select a, b %s group by a'.
// The test will Sprintf a from clause and where clause as needed.
func newTestTablePartialMigrater(ctx context.Context, t *testing.T, shards, shardsToMove []string, fmtQuery string) *testMigraterEnv {
	require.Greater(t, len(shards), 1, "shard by shard migrations can only be done on sharded keyspaces")
	tme := &testMigraterEnv{}
	tme.ts = memorytopo.NewServer(ctx, "cell1", "cell2")
	tme.wr = New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), tme.ts, tmclient.NewTabletManagerClient())
	tme.wr.sem = semaphore.NewWeighted(1)
	tme.sourceShards = shards
	tme.targetShards = shards
	tme.tmeDB = fakesqldb.New(t)
	expectVDiffQueries(tme.tmeDB)
	tabletID := 10
	for _, shard := range tme.sourceShards {
		tme.sourcePrimaries = append(tme.sourcePrimaries, newFakeTablet(t, tme.wr, "cell1", uint32(tabletID), topodatapb.TabletType_PRIMARY, tme.tmeDB, TabletKeyspaceShard(t, "ks1", shard)))
		tabletID += 10

		_, sourceKeyRange, err := topo.ValidateShardName(shard)
		if err != nil {
			t.Fatal(err)
		}
		tme.sourceKeyRanges = append(tme.sourceKeyRanges, sourceKeyRange)
	}
	tpChoiceTablet := tme.sourcePrimaries[0].Tablet
	tpChoice = &testTabletPickerChoice{
		keyspace: tpChoiceTablet.Keyspace,
		shard:    tpChoiceTablet.Shard,
	}
	for _, shard := range tme.targetShards {
		tme.targetPrimaries = append(tme.targetPrimaries, newFakeTablet(t, tme.wr, "cell1", uint32(tabletID), topodatapb.TabletType_PRIMARY, tme.tmeDB, TabletKeyspaceShard(t, "ks2", shard)))
		tabletID += 10

		_, targetKeyRange, err := topo.ValidateShardName(shard)
		if err != nil {
			t.Fatal(err)
		}
		tme.targetKeyRanges = append(tme.targetKeyRanges, targetKeyRange)
	}

	dialerName := fmt.Sprintf("TrafficSwitcherTest-%s-%d", t.Name(), rand.Intn(1000000000))
	tabletconn.RegisterDialer(dialerName, func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		tme.mu.Lock()
		defer tme.mu.Unlock()
		for _, ft := range append(tme.sourcePrimaries, tme.targetPrimaries...) {
			if ft.Tablet.Alias.Uid == tablet.Alias.Uid {
				return ft, nil
			}
		}
		return nil, nil
	})
	tabletconntest.SetProtocol("go.vt.wrangler.traffic_switcher_env_test", dialerName)

	vs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "hash",
				}},
			},
			"t2": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "hash",
				}},
			},
		},
	}
	err := tme.ts.SaveVSchema(ctx, "ks1", vs)
	require.NoError(t, err)
	err = tme.ts.SaveVSchema(ctx, "ks2", vs)
	require.NoError(t, err)
	err = tme.ts.RebuildSrvVSchema(ctx, nil)
	require.NoError(t, err)
	err = topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), tme.ts, "ks1", []string{"cell1"}, false)
	require.NoError(t, err)
	err = topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), tme.ts, "ks2", []string{"cell1"}, false)
	require.NoError(t, err)

	tme.startTablets(t)
	tme.createDBClients(ctx, t)
	tme.setPrimaryPositions()
	now := time.Now().Unix()

	for i, shard := range shards {
		var streamInfoRows []string
		var streamExtInfoRows []string
		var vreplIDs []string
		for _, shardToMove := range shardsToMove {
			if shardToMove == shard {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks1",
					Shard:    shard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf(fmtQuery, fmt.Sprintf("from t1 where in_keyrange('%s')", shard)),
						}, {
							Match:  "t2",
							Filter: fmt.Sprintf(fmtQuery, fmt.Sprintf("from t2 where in_keyrange('%s')", shard)),
						}},
					},
				}
				streamInfoRows = append(streamInfoRows, fmt.Sprintf("%d|%v||||1|0|0", i+1, bls))
				streamExtInfoRows = append(streamExtInfoRows, fmt.Sprintf("%d|||||Running|vt_ks1|%d|%d|0|0|||1||0", i+1, now, now))
				vreplIDs = append(vreplIDs, strconv.FormatInt(int64(i+1), 10))
			}
		}
		vreplIDsJoined := strings.Join(vreplIDs, ", ")
		tme.dbTargetClients[i].addInvariant(fmt.Sprintf(copyStateQuery, vreplIDsJoined, vreplIDsJoined), noResult)
		log.Infof("Adding streamInfoKs2 invariant for shard %s,  client %s,rows %q",
			shard, tme.dbTargetClients[i].name, streamExtInfoRows)
		tme.dbTargetClients[i].addInvariant(streamInfoKs2, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|varchar|varchar|varchar|int64|int64|int64"),
			streamInfoRows...))
		tme.dbTargetClients[i].addInvariant(streamExtInfoKs2, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|pos|stop_pos|max_replication_lag|state|db_name|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|message|tags|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|int64|int64|int64|varchar|varchar|int64|int64|int64|int64|int64|varchar|varchar|int64|int64|int64"),
			streamExtInfoRows...))
		tme.dbTargetClients[i].addInvariant(reverseStreamExtInfoKs2, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|pos|stop_pos|max_replication_lag|state|db_name|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|message|tags|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|int64|int64|int64|varchar|varchar|int64|int64|int64|int64|int64|varchar|varchar|int64|int64|int64"),
			streamExtInfoRows...))
	}

	for i, shard := range shards {
		var streamInfoRows []string
		var vreplIDs []string
		for _, shardToMove := range shardsToMove {
			if shardToMove == shard {
				bls := &binlogdatapb.BinlogSource{
					Keyspace: "ks2",
					Shard:    shard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{{
							Match:  "t1",
							Filter: fmt.Sprintf(fmtQuery, fmt.Sprintf("from t1 where in_keyrange('%s')", shard)),
						}, {
							Match:  "t2",
							Filter: fmt.Sprintf(fmtQuery, fmt.Sprintf("from t2 where in_keyrange('%s')", shard)),
						}},
					},
				}
				streamInfoRows = append(streamInfoRows, fmt.Sprintf("%d|%v||||1|0|0", i+1, bls))
				vreplIDs = append(vreplIDs, strconv.FormatInt(int64(i+1), 10))
			}
		}
		vreplIDsJoined := strings.Join(vreplIDs, ", ")
		tme.dbTargetClients[i].addInvariant(fmt.Sprintf(copyStateQuery, vreplIDsJoined, vreplIDsJoined), noResult)
		tme.dbSourceClients[i].addInvariant(reverseStreamInfoKs1, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|varchar|varchar|varchar|int64|int64|int64"),
			streamInfoRows...),
		)
	}
	tme.targetKeyspace = "ks2"
	return tme
}

func newTestShardMigrater(ctx context.Context, t *testing.T, sourceShards, targetShards []string) *testShardMigraterEnv {
	tme := &testShardMigraterEnv{}
	tme.ts = memorytopo.NewServer(ctx, "cell1", "cell2")
	tme.wr = New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), tme.ts, tmclient.NewTabletManagerClient())
	tme.sourceShards = sourceShards
	tme.targetShards = targetShards
	tme.tmeDB = fakesqldb.New(t)
	expectVDiffQueries(tme.tmeDB)
	tme.wr.sem = semaphore.NewWeighted(1)

	tabletID := 10
	for _, shard := range sourceShards {
		tme.sourcePrimaries = append(tme.sourcePrimaries, newFakeTablet(t, tme.wr, "cell1", uint32(tabletID), topodatapb.TabletType_PRIMARY, tme.tmeDB, TabletKeyspaceShard(t, "ks", shard)))
		tabletID += 10

		_, sourceKeyRange, err := topo.ValidateShardName(shard)
		if err != nil {
			t.Fatal(err)
		}
		tme.sourceKeyRanges = append(tme.sourceKeyRanges, sourceKeyRange)
	}
	tpChoiceTablet := tme.sourcePrimaries[0].Tablet
	tpChoice = &testTabletPickerChoice{
		keyspace: tpChoiceTablet.Keyspace,
		shard:    tpChoiceTablet.Shard,
	}

	for _, shard := range targetShards {
		tme.targetPrimaries = append(tme.targetPrimaries, newFakeTablet(t, tme.wr, "cell1", uint32(tabletID), topodatapb.TabletType_PRIMARY, tme.tmeDB, TabletKeyspaceShard(t, "ks", shard)))
		tabletID += 10

		_, targetKeyRange, err := topo.ValidateShardName(shard)
		if err != nil {
			t.Fatal(err)
		}
		tme.targetKeyRanges = append(tme.targetKeyRanges, targetKeyRange)
	}

	dialerName := fmt.Sprintf("TrafficSwitcherTest-%s-%d", t.Name(), rand.Intn(1000000000))
	tabletconn.RegisterDialer(dialerName, func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		tme.mu.Lock()
		defer tme.mu.Unlock()
		for _, ft := range append(tme.sourcePrimaries, tme.targetPrimaries...) {
			if ft.Tablet.Alias.Uid == tablet.Alias.Uid {
				return ft, nil
			}
		}
		return nil, nil
	})
	tabletconntest.SetProtocol("go.vt.wrangler.traffic_switcher_env_test", dialerName)

	vs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"thash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Columns: []string{"c1"},
					Name:    "thash",
				}},
			},
			"t2": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Columns: []string{"c1"},
					Name:    "thash",
				}},
			},
			"t3": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Columns: []string{"c1"},
					Name:    "thash",
				}},
			},
		},
	}
	if err := tme.ts.SaveVSchema(ctx, "ks", vs); err != nil {
		t.Fatal(err)
	}
	if err := tme.ts.RebuildSrvVSchema(ctx, nil); err != nil {
		t.Fatal(err)
	}
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), tme.ts, "ks", nil, false)
	if err != nil {
		t.Fatal(err)
	}

	tme.startTablets(t)
	tme.createDBClients(ctx, t)
	tme.setPrimaryPositions()
	now := time.Now().Unix()
	for i, targetShard := range targetShards {
		var rows, rowsRdOnly []string
		var streamExtInfoRows []string
		var vreplIDs []string
		for j, sourceShard := range sourceShards {
			if !key.KeyRangeIntersect(tme.targetKeyRanges[i], tme.sourceKeyRanges[j]) {
				continue
			}
			bls := &binlogdatapb.BinlogSource{
				Keyspace: "ks",
				Shard:    sourceShard,
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "/.*",
						Filter: targetShard,
					}},
				},
			}
			rows = append(rows, fmt.Sprintf("%d|%v||||1|0|0", j+1, bls))
			rowsRdOnly = append(rows, fmt.Sprintf("%d|%v|||RDONLY|1|0|0", j+1, bls))
			streamExtInfoRows = append(streamExtInfoRows, fmt.Sprintf("%d|||||Running|vt_ks1|%d|%d|0|0|||", j+1, now, now))
			vreplIDs = append(vreplIDs, strconv.FormatInt(int64(j+1), 10))
		}
		vreplIDsJoined := strings.Join(vreplIDs, ", ")
		tme.dbTargetClients[i].addInvariant(fmt.Sprintf(copyStateQuery, vreplIDsJoined, vreplIDsJoined), noResult)
		tme.dbTargetClients[i].addInvariant(streamInfoKs, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|varchar|varchar|varchar|int64|int64|int64"),
			rows...),
		)
		tme.dbTargetClients[i].addInvariant(streamInfoKs+"-rdonly", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|varchar|varchar|varchar|int64|int64|int64"),
			rowsRdOnly...),
		)
		tme.dbTargetClients[i].addInvariant(streamExtInfoKs, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|pos|stop_pos|max_replication_lag|state|db_name|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|message|tags",
			"int64|varchar|int64|int64|int64|varchar|varchar|int64|int64|int64|int64|varchar|varchar|varchar"),
			streamExtInfoRows...))
	}

	tme.targetKeyspace = "ks"
	for i, dbclient := range tme.dbSourceClients {
		var streamExtInfoRows []string
		var vreplIDs []string
		dbclient.addInvariant(streamInfoKs, &sqltypes.Result{})
		for j := range targetShards {
			streamExtInfoRows = append(streamExtInfoRows, fmt.Sprintf("%d|||||Running|vt_ks|%d|%d|0|0|||", j+1, now, now))
			vreplIDs = append(vreplIDs, strconv.FormatInt(int64(j+1), 10))
		}
		vreplIDsJoined := strings.Join(vreplIDs, ", ")
		tme.dbSourceClients[i].addInvariant(fmt.Sprintf(copyStateQuery, vreplIDsJoined, vreplIDsJoined), noResult)
		tme.dbSourceClients[i].addInvariant(streamExtInfoKs, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|pos|stop_pos|max_replication_lag|state|db_name|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|message|tags",
			"int64|varchar|int64|int64|int64|varchar|varchar|int64|int64|int64|int64|varchar|varchar|varchar"),
			streamExtInfoRows...))
	}
	return tme
}

func (tme *testMigraterEnv) startTablets(t *testing.T) {
	tme.mu.Lock()
	defer tme.mu.Unlock()
	allPrimarys := append(tme.sourcePrimaries, tme.targetPrimaries...)
	allPrimarys = append(allPrimarys, tme.additionalPrimaries...)
	for _, primary := range allPrimarys {
		primary.StartActionLoop(t, tme.wr)
	}
	// Wait for the shard record primaries to be set.
	for _, primary := range allPrimarys {
		primaryFound := false
		for i := 0; i < 10; i++ {
			si, err := tme.wr.ts.GetShard(context.Background(), primary.Tablet.Keyspace, primary.Tablet.Shard)
			if err != nil {
				t.Fatal(err)
			}
			if si.PrimaryAlias != nil {
				primaryFound = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !primaryFound {
			t.Fatalf("shard primary did not get updated for tablet: %v", primary)
		}
	}
}

func (tme *testMigraterEnv) stopTablets(t *testing.T) {
	for _, primary := range tme.sourcePrimaries {
		primary.StopActionLoop(t)
	}
	for _, primary := range tme.targetPrimaries {
		primary.StopActionLoop(t)
	}
	for _, primary := range tme.additionalPrimaries {
		primary.StopActionLoop(t)
	}
}

func (tme *testMigraterEnv) createDBClients(ctx context.Context, t *testing.T) {
	tme.mu.Lock()
	defer tme.mu.Unlock()
	for _, primary := range tme.sourcePrimaries {
		dbclient := newFakeDBClient(primary.Tablet.Alias.String())
		tme.dbSourceClients = append(tme.dbSourceClients, dbclient)
		dbClientFactory := func() binlogplayer.DBClient { return dbclient }
		// Replace existing engine with a new one
		primary.TM.VREngine = vreplication.NewTestEngine(tme.ts, primary.Tablet.GetAlias().GetCell(), primary.FakeMysqlDaemon, dbClientFactory, dbClientFactory, dbclient.DBName(), nil)
		primary.TM.VREngine.Open(ctx)
	}
	for _, primary := range tme.targetPrimaries {
		log.Infof("Adding as targetPrimary %s", primary.Tablet.Alias)
		dbclient := newFakeDBClient(primary.Tablet.Alias.String())
		tme.dbTargetClients = append(tme.dbTargetClients, dbclient)
		dbClientFactory := func() binlogplayer.DBClient { return dbclient }
		// Replace existing engine with a new one
		primary.TM.VREngine = vreplication.NewTestEngine(tme.ts, primary.Tablet.GetAlias().GetCell(), primary.FakeMysqlDaemon, dbClientFactory, dbClientFactory, dbclient.DBName(), nil)
		primary.TM.VREngine.Open(ctx)
	}
	for _, primary := range tme.additionalPrimaries {
		log.Infof("Adding as additionalPrimary %s", primary.Tablet.Alias)
		dbclient := newFakeDBClient(primary.Tablet.Alias.String())
		tme.dbAdditionalClients = append(tme.dbTargetClients, dbclient)
	}
	tme.allDBClients = append(tme.dbSourceClients, tme.dbTargetClients...)
	tme.allDBClients = append(tme.allDBClients, tme.dbAdditionalClients...)
}

func (tme *testMigraterEnv) setPrimaryPositions() {
	for _, primary := range tme.sourcePrimaries {
		primary.FakeMysqlDaemon.CurrentPrimaryPosition = replication.Position{
			GTIDSet: replication.MariadbGTIDSet{
				5: replication.MariadbGTID{
					Domain:   5,
					Server:   456,
					Sequence: 892,
				},
			},
		}
	}
	for _, primary := range tme.targetPrimaries {
		primary.FakeMysqlDaemon.CurrentPrimaryPosition = replication.Position{
			GTIDSet: replication.MariadbGTIDSet{
				5: replication.MariadbGTID{
					Domain:   5,
					Server:   456,
					Sequence: 893,
				},
			},
		}
	}
}

func (tme *testMigraterEnv) setPrimarySchemas(schema *tabletmanagerdatapb.SchemaDefinition) {
	for _, primary := range tme.sourcePrimaries {
		primary.FakeMysqlDaemon.Schema = schema
	}
	for _, primary := range tme.targetPrimaries {
		primary.FakeMysqlDaemon.Schema = schema
	}
}

func (tme *testMigraterEnv) expectNoPreviousJournals() {
	// validate that no previous journals exist
	for _, dbclient := range tme.dbSourceClients {
		dbclient.addQueryRE(tsCheckJournals, &sqltypes.Result{}, nil)
	}
}

func (tme *testMigraterEnv) expectNoPreviousReverseJournals() {
	// validate that no previous journals exist
	for _, dbclient := range tme.dbTargetClients {
		dbclient.addQueryRE(tsCheckJournals, &sqltypes.Result{}, nil)
	}
}

func (tme *testShardMigraterEnv) forAllStreams(f func(i, j int)) {
	for i := range tme.targetShards {
		for j := range tme.sourceShards {
			if !key.KeyRangeIntersect(tme.targetKeyRanges[i], tme.sourceKeyRanges[j]) {
				continue
			}
			f(i, j)
		}
	}
}

func (tme *testShardMigraterEnv) expectCheckJournals() {
	for _, dbclient := range tme.dbSourceClients {
		dbclient.addQueryRE("select val from _vt.resharding_journal where id=.*", &sqltypes.Result{}, nil)
	}
}

func (tme *testShardMigraterEnv) expectWaitForCatchup() {
	state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos|state|message",
		"varchar|varchar|varchar"),
		"MariaDB/5-456-892|Running",
	)
	tme.forAllStreams(func(i, j int) {
		tme.dbTargetClients[i].addQuery(fmt.Sprintf("select pos, state, message from _vt.vreplication where id=%d", j+1), state, nil)

		// mi.waitForCatchup-> mi.wr.tmc.VReplicationExec('stopped for cutover')
		tme.dbTargetClients[i].addQuery(fmt.Sprintf("select id from _vt.vreplication where id = %d", j+1), &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(int64(j + 1))}}}, nil)
		tme.dbTargetClients[i].addQuery(fmt.Sprintf("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (%d)", j+1), &sqltypes.Result{}, nil)
		tme.dbTargetClients[i].addQuery(fmt.Sprintf("select * from _vt.vreplication where id = %d", j+1), stoppedResult(j+1), nil)
	})
}

func (tme *testShardMigraterEnv) expectDeleteReverseVReplication() {
	// NOTE: this is not a faithful reproduction of what should happen.
	// The ids returned are not accurate.
	for _, dbclient := range tme.dbSourceClients {
		dbclient.addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test_reverse'", resultid12, nil)
		dbclient.addQuery("delete from _vt.vreplication where id in (1, 2)", &sqltypes.Result{}, nil)
		dbclient.addQuery("delete from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
		dbclient.addQuery("delete from _vt.post_copy_action where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
	}
}

func (tme *testShardMigraterEnv) expectCreateReverseVReplication() {
	tme.expectDeleteReverseVReplication()
	tme.forAllStreams(func(i, j int) {
		tme.dbSourceClients[j].addQueryRE(fmt.Sprintf("insert into _vt.vreplication.*%s.*%s.*MariaDB/5-456-893.*Stopped", tme.targetShards[i], key.KeyRangeString(tme.sourceKeyRanges[j])), &sqltypes.Result{InsertID: uint64(j + 1)}, nil)
		tme.dbSourceClients[j].addQuery(fmt.Sprintf("select * from _vt.vreplication where id = %d", j+1), stoppedResult(j+1), nil)
	})
}

func (tme *testShardMigraterEnv) expectCreateJournals() {
	for _, dbclient := range tme.dbSourceClients {
		dbclient.addQueryRE("insert into _vt.resharding_journal.*", &sqltypes.Result{}, nil)
	}
}

func (tme *testShardMigraterEnv) expectStartReverseVReplication() {
	// NOTE: this is not a faithful reproduction of what should happen.
	// The ids returned are not accurate.
	for _, dbclient := range tme.dbSourceClients {
		dbclient.addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test_reverse'", resultid34, nil)
		dbclient.addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (3, 4)", &sqltypes.Result{}, nil)
		dbclient.addQuery("select * from _vt.vreplication where id = 3", runningResult(3), nil)
		dbclient.addQuery("select * from _vt.vreplication where id = 4", runningResult(4), nil)
	}
}

func (tme *testShardMigraterEnv) expectFrozenTargetVReplication() {
	// NOTE: this is not a faithful reproduction of what should happen.
	// The ids returned are not accurate.
	for _, dbclient := range tme.dbTargetClients {
		dbclient.addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid12, nil)
		dbclient.addQuery("update _vt.vreplication set message = 'FROZEN' where id in (1, 2)", &sqltypes.Result{}, nil)
		dbclient.addQuery("select * from _vt.vreplication where id = 1", stoppedResult(1), nil)
		dbclient.addQuery("select * from _vt.vreplication where id = 2", stoppedResult(2), nil)
	}
}

func (tme *testShardMigraterEnv) expectDeleteTargetVReplication() {
	// NOTE: this is not a faithful reproduction of what should happen.
	// The ids returned are not accurate.
	for _, dbclient := range tme.dbTargetClients {
		dbclient.addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid12, nil)
		dbclient.addQuery("delete from _vt.vreplication where id in (1, 2)", &sqltypes.Result{}, nil)
		dbclient.addQuery("delete from _vt.copy_state where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
		dbclient.addQuery("delete from _vt.post_copy_action where vrepl_id in (1, 2)", &sqltypes.Result{}, nil)
	}
}

func (tme *testShardMigraterEnv) expectCancelStreamMigrations() {
	for _, dbclient := range tme.dbTargetClients {
		dbclient.addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", &sqltypes.Result{}, nil)
	}
	for _, dbclient := range tme.dbSourceClients {
		dbclient.addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)
	}
	tme.expectDeleteReverseVReplication()
}

func (tme *testShardMigraterEnv) expectNoPreviousJournals() {
	// validate that no previous journals exist
	for _, dbclient := range tme.dbSourceClients {
		dbclient.addQueryRE(tsCheckJournals, &sqltypes.Result{}, nil)
	}
}

func (tme *testMigraterEnv) close(t *testing.T) {
	tme.mu.Lock()
	defer tme.mu.Unlock()
	tme.stopTablets(t)
	for _, dbclient := range tme.dbSourceClients {
		dbclient.Close()
	}
	for _, dbclient := range tme.dbTargetClients {
		dbclient.Close()
	}
	for _, dbclient := range tme.dbAdditionalClients {
		dbclient.Close()
	}
	tme.tmeDB.CloseAllConnections()
	tme.ts.Close()
	tme.wr.tmc.Close()
	tme.wr = nil
	tme.tmeDB.Close()
}
