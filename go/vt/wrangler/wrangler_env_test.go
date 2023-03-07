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
	"sync"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	testStopPosition          = "MariaDB/5-456-892"
	testSourceGtid            = "MariaDB/5-456-893"
	testTargetPrimaryPosition = "MariaDB/6-456-892"
)

type testWranglerEnv struct {
	wr         *Wrangler
	workflow   string
	topoServ   *topo.Server
	cell       string
	tabletType topodatapb.TabletType
	tmc        *testWranglerTMClient
	mu         sync.Mutex
}

//----------------------------------------------
// testWranglerEnv

func newWranglerTestEnv(t testing.TB, sourceShards, targetShards []string, query string, positions map[string]string, timeUpdated int64) *testWranglerEnv {
	env := &testWranglerEnv{
		workflow:   "wrWorkflow",
		topoServ:   memorytopo.NewServer("zone1"),
		cell:       "zone1",
		tabletType: topodatapb.TabletType_REPLICA,
		tmc:        newTestWranglerTMClient(),
	}
	env.wr = New(logutil.NewConsoleLogger(), env.topoServ, env.tmc)
	env.tmc.tablets = make(map[int]*testWranglerTablet)

	// Generate a unique dialer name.
	dialerName := fmt.Sprintf("WranglerTest-%s-%d", t.Name(), rand.Intn(1000000000))
	tabletconn.RegisterDialer(dialerName, func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		env.mu.Lock()
		defer env.mu.Unlock()
		if qs, ok := env.tmc.tablets[int(tablet.Alias.Uid)]; ok {
			return qs, nil
		}
		// some tests don't require the query service. Earlier we were returning an error for such cases but the tablet picker
		// now logs a warning and spams the logs. Hence we return a fake service instead
		return newFakeTestWranglerTablet(), nil
	})
	tabletconntest.SetProtocol("go.vt.wrangler.wrangler_env_test", dialerName)

	tabletID := 100
	for _, shard := range sourceShards {
		_ = env.addTablet(tabletID, "source", shard, topodatapb.TabletType_PRIMARY)
		_ = env.addTablet(tabletID+1, "source", shard, topodatapb.TabletType_REPLICA)
		env.tmc.waitpos[tabletID+1] = testStopPosition

		tabletID += 10
	}
	tabletID = 200
	for _, shard := range targetShards {
		primary := env.addTablet(tabletID, "target", shard, topodatapb.TabletType_PRIMARY)
		_ = env.addTablet(tabletID+1, "target", shard, topodatapb.TabletType_REPLICA)

		var rows []string
		var posRows []string
		var bls *binlogdatapb.BinlogSource
		for j, sourceShard := range sourceShards {
			bls = &binlogdatapb.BinlogSource{
				Keyspace: "source",
				Shard:    sourceShard,
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: query,
					}},
				},
			}
			rows = append(rows, fmt.Sprintf("%d|%v||||0|0|0", j+1, bls))
			position := testStopPosition
			if pos := positions[sourceShard+shard]; pos != "" {
				position = pos
			}
			posRows = append(posRows, fmt.Sprintf("%v|%s", bls, position))

			env.tmc.setVRResults(
				primary.tablet,
				fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos='%s', message='synchronizing for wrangler test' where id=%d", testSourceGtid, j+1),
				&sqltypes.Result{},
			)
		}
		// migrater buildMigrationTargets
		env.tmc.setVRResults(
			primary.tablet,
			"select id, source, message, cell, tablet_types, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name = 'vt_target' and workflow = 'wrWorkflow'",
			sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
				"int64|varchar|varchar|varchar|varchar|int64|int64|int64"),
				rows...,
			),
		)

		env.tmc.setVRResults(primary.tablet, "update _vt.vreplication set state = 'Stopped', message = 'for wrangler test' where db_name = 'vt_target' and workflow = 'wrWorkflow'", &sqltypes.Result{RowsAffected: 1})
		env.tmc.setVRResults(primary.tablet, "update _vt.vreplication set state = 'Stopped' where db_name = 'vt_target' and workflow = 'wrWorkflow'", &sqltypes.Result{RowsAffected: 1})
		env.tmc.setVRResults(primary.tablet, "delete from _vt.vreplication where message != '' and db_name = 'vt_target' and workflow = 'wrWorkflow'", &sqltypes.Result{RowsAffected: 1})
		env.tmc.setVRResults(primary.tablet, "insert into _vt.vreplication(state, workflow, db_name) values ('Running', 'wk1', 'ks1'), ('Stopped', 'wk1', 'ks1')", &sqltypes.Result{RowsAffected: 2})

		result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"id|source|pos|stop_pos|max_replication_lag|state|db_name|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|message|tags|workflow_type|workflow_sub_type|defer_secondary_keys|rows_copied",
			"int64|varchar|varchar|varchar|int64|varchar|varchar|int64|int64|int64|int64|int64|varchar|varchar|varchar|int64|int64|int64|int64"),
			fmt.Sprintf("1|%v|MySQL56/14b68925-696a-11ea-aee7-fec597a91f5e:1-3||0|Running|vt_target|%d|0|%d|0||||||0|1000", bls, timeUpdated, timeUpdated),
		)
		env.tmc.setVRResults(primary.tablet, "select id, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, transaction_timestamp, time_heartbeat, time_throttled, component_throttled, message, tags, workflow_type, workflow_sub_type, defer_secondary_keys, rows_copied from _vt.vreplication where db_name = 'vt_target' and workflow = 'wrWorkflow'", result)
		env.tmc.setVRResults(
			primary.tablet,
			"select source, pos from _vt.vreplication where db_name='vt_target' and workflow='wrWorkflow'",
			sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"source|pos",
				"varchar|varchar"),
				posRows...,
			),
		)
		result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"workflow",
			"varchar"),
			"wrWorkflow",
		)
		env.tmc.setVRResults(primary.tablet, "select distinct workflow from _vt.vreplication where state != 'Stopped' and db_name = 'vt_target'", result)

		result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"table|lastpk",
			"varchar|varchar"),
			"t1|pk1",
		)

		env.tmc.setVRResults(primary.tablet, "select table_name, lastpk from _vt.copy_state where vrepl_id = 1 and id in (select max(id) from _vt.copy_state where vrepl_id = 1 group by vrepl_id, table_name)", result)

		env.tmc.setVRResults(primary.tablet, "select id, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, transaction_timestamp, time_heartbeat, time_throttled, component_throttled, message, tags from _vt.vreplication where db_name = 'vt_target' and workflow = 'bad'", &sqltypes.Result{})

		env.tmc.setVRResults(primary.tablet, "select id, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, transaction_timestamp, time_heartbeat, time_throttled, component_throttled, message, tags from _vt.vreplication where db_name = 'vt_target' and workflow = 'badwf'", &sqltypes.Result{})
		env.tmc.vrpos[tabletID] = testSourceGtid
		env.tmc.pos[tabletID] = testTargetPrimaryPosition

		env.tmc.waitpos[tabletID+1] = testTargetPrimaryPosition

		env.tmc.setVRResults(primary.tablet, "update _vt.vreplication set state='Running', message='', stop_pos='' where db_name='vt_target' and workflow='wrWorkflow'", &sqltypes.Result{})

		result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"workflow",
			"varchar"),
			"wrWorkflow", "wrWorkflow2",
		)
		env.tmc.setVRResults(primary.tablet, "select distinct workflow from _vt.vreplication where db_name = 'vt_target'", result)
		tabletID += 10
	}
	primary := env.addTablet(300, "target2", "0", topodatapb.TabletType_PRIMARY)
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"workflow",
		"varchar"),
		"wrWorkflow", "wrWorkflow2",
	)
	env.tmc.setVRResults(primary.tablet, "select distinct workflow from _vt.vreplication where db_name = 'vt_target2'", result)
	return env
}

func (env *testWranglerEnv) close() {
	env.mu.Lock()
	defer env.mu.Unlock()
	for _, t := range env.tmc.tablets {
		env.topoServ.DeleteTablet(context.Background(), t.tablet.Alias)
	}
	env.tmc.tablets = nil
}

func newFakeTestWranglerTablet() *testWranglerTablet {
	id := 999
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "fake",
			Uid:  uint32(id),
		},
		Keyspace: "fake",
		Shard:    "fake",
		KeyRange: &topodatapb.KeyRange{},
		Type:     topodatapb.TabletType_PRIMARY,
		PortMap: map[string]int32{
			"test": int32(id),
		},
	}
	return newTestWranglerTablet(tablet)
}

func (env *testWranglerEnv) addTablet(id int, keyspace, shard string, tabletType topodatapb.TabletType) *testWranglerTablet {
	env.mu.Lock()
	defer env.mu.Unlock()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: env.cell,
			Uid:  uint32(id),
		},
		Keyspace: keyspace,
		Shard:    shard,
		KeyRange: &topodatapb.KeyRange{},
		Type:     tabletType,
		PortMap: map[string]int32{
			"test": int32(id),
		},
	}
	env.tmc.tablets[id] = newTestWranglerTablet(tablet)
	if err := env.wr.TopoServer().InitTablet(context.Background(), tablet, false /* allowPrimaryOverride */, true /* createShardAndKeyspace */, false /* allowUpdate */); err != nil {
		panic(err)
	}
	if tabletType == topodatapb.TabletType_PRIMARY {
		_, err := env.wr.ts.UpdateShardFields(context.Background(), keyspace, shard, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = tablet.Alias
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	env.tmc.tablets[id].queryResults = make(map[string]*querypb.QueryResult)
	return env.tmc.tablets[id]
}

//----------------------------------------------
// testWranglerTablet

type testWranglerTablet struct {
	queryservice.QueryService
	tablet       *topodatapb.Tablet
	queryResults map[string]*querypb.QueryResult
	gotQueries   []string
}

func newTestWranglerTablet(tablet *topodatapb.Tablet) *testWranglerTablet {
	return &testWranglerTablet{
		QueryService: fakes.ErrorQueryService,
		tablet:       tablet,
	}
}

func (tvt *testWranglerTablet) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	return callback(&querypb.StreamHealthResponse{
		Serving: true,
		Target: &querypb.Target{
			Keyspace:   tvt.tablet.Keyspace,
			Shard:      tvt.tablet.Shard,
			TabletType: tvt.tablet.Type,
		},
		RealtimeStats: &querypb.RealtimeStats{},
	})
}

//----------------------------------------------
// testWranglerTMClient

type testWranglerTMClient struct {
	tmclient.TabletManagerClient
	tablets   map[int]*testWranglerTablet
	schema    *tabletmanagerdatapb.SchemaDefinition
	vrQueries map[int]map[string]*querypb.QueryResult
	waitpos   map[int]string
	vrpos     map[int]string
	pos       map[int]string
}

func newTestWranglerTMClient() *testWranglerTMClient {
	return &testWranglerTMClient{
		vrQueries: make(map[int]map[string]*querypb.QueryResult),
		waitpos:   make(map[int]string),
		vrpos:     make(map[int]string),
		pos:       make(map[int]string),
	}
}

func (tmc *testWranglerTMClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return tmc.schema, nil
}

func (tmc *testWranglerTMClient) setVRResults(tablet *topodatapb.Tablet, query string, result *sqltypes.Result) {
	queries, ok := tmc.vrQueries[int(tablet.Alias.Uid)]
	if !ok {
		queries = make(map[string]*querypb.QueryResult)
		tmc.vrQueries[int(tablet.Alias.Uid)] = queries
	}
	queries[query] = sqltypes.ResultToProto3(result)
}

func (tmc *testWranglerTMClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	result, ok := tmc.vrQueries[int(tablet.Alias.Uid)][query]
	if !ok {
		return nil, fmt.Errorf("query %q not found for tablet %d", query, tablet.Alias.Uid)
	}
	return result, nil
}

func (tmc *testWranglerTMClient) ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (*querypb.QueryResult, error) {
	t := tmc.tablets[int(tablet.Alias.Uid)]
	t.gotQueries = append(t.gotQueries, string(req.Query))
	result, ok := t.queryResults[string(req.Query)]
	if !ok {
		result = &querypb.QueryResult{}
		log.Errorf("Query: %s, Result :%v\n", string(req.Query), result)
	}
	return result, nil
}
