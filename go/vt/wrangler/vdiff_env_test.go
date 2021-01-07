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
	"flag"
	"fmt"
	"sync"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/logutil"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

const (
	// vdiffStopPosition is the default stop position for the target vreplication.
	// It can be overridden with the positons argument to newTestVDiffEnv.
	vdiffStopPosition = "MariaDB/5-456-892"
	// vdiffSourceGtid should be the position reported by the source side VStreamResults.
	// It's expected to be higher the vdiffStopPosition.
	vdiffSourceGtid = "MariaDB/5-456-893"
	// vdiffTargetMasterPosition is the master position of the target after
	// vreplication has been synchronized.
	vdiffTargetMasterPosition = "MariaDB/6-456-892"
)

type testVDiffEnv struct {
	wr         *Wrangler
	workflow   string
	topoServ   *topo.Server
	cell       string
	tabletType topodatapb.TabletType
	tmc        *testVDiffTMClient

	mu      sync.Mutex
	tablets map[int]*testVDiffTablet
}

// vdiffEnv has to be a global for RegisterDialer to work.
var vdiffEnv *testVDiffEnv

func init() {
	tabletconn.RegisterDialer("VDiffTest", func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		vdiffEnv.mu.Lock()
		defer vdiffEnv.mu.Unlock()
		if qs, ok := vdiffEnv.tablets[int(tablet.Alias.Uid)]; ok {
			return qs, nil
		}
		return nil, fmt.Errorf("tablet %d not found", tablet.Alias.Uid)
	})
}

//----------------------------------------------
// testVDiffEnv

func newTestVDiffEnv(sourceShards, targetShards []string, query string, positions map[string]string) *testVDiffEnv {
	flag.Set("tablet_protocol", "VDiffTest")
	env := &testVDiffEnv{
		workflow:   "vdiffTest",
		tablets:    make(map[int]*testVDiffTablet),
		topoServ:   memorytopo.NewServer("cell"),
		cell:       "cell",
		tabletType: topodatapb.TabletType_REPLICA,
		tmc:        newTestVDiffTMClient(),
	}
	env.wr = New(logutil.NewConsoleLogger(), env.topoServ, env.tmc)

	tabletID := 100
	for _, shard := range sourceShards {
		_ = env.addTablet(tabletID, "source", shard, topodatapb.TabletType_MASTER)
		_ = env.addTablet(tabletID+1, "source", shard, topodatapb.TabletType_REPLICA)
		env.tmc.waitpos[tabletID+1] = vdiffStopPosition

		tabletID += 10
	}
	tabletID = 200
	for _, shard := range targetShards {
		master := env.addTablet(tabletID, "target", shard, topodatapb.TabletType_MASTER)
		_ = env.addTablet(tabletID+1, "target", shard, topodatapb.TabletType_REPLICA)

		var rows []string
		var posRows []string
		for j, sourceShard := range sourceShards {
			bls := &binlogdatapb.BinlogSource{
				Keyspace: "source",
				Shard:    sourceShard,
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{{
						Match:  "t1",
						Filter: query,
					}},
				},
			}
			rows = append(rows, fmt.Sprintf("%d|%v|||", j+1, bls))
			position := vdiffStopPosition
			if pos := positions[sourceShard+shard]; pos != "" {
				position = pos
			}
			posRows = append(posRows, fmt.Sprintf("%v|%s", bls, position))

			// vdiff.syncTargets. This actually happens after stopTargets.
			// But this is one statement per stream.
			env.tmc.setVRResults(
				master.tablet,
				fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos='%s', message='synchronizing for vdiff' where id=%d", vdiffSourceGtid, j+1),
				&sqltypes.Result{},
			)
		}
		// migrater buildMigrationTargets
		env.tmc.setVRResults(
			master.tablet,
			"select id, source, message, cell, tablet_types from _vt.vreplication where workflow='vdiffTest' and db_name='vt_target'",
			sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|source|message|cell|tablet_types",
				"int64|varchar|varchar|varchar|varchar"),
				rows...,
			),
		)

		// vdiff.stopTargets
		env.tmc.setVRResults(master.tablet, "update _vt.vreplication set state='Stopped', message='for vdiff' where db_name='vt_target' and workflow='vdiffTest'", &sqltypes.Result{})
		env.tmc.setVRResults(
			master.tablet,
			"select source, pos from _vt.vreplication where db_name='vt_target' and workflow='vdiffTest'",
			sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"source|pos",
				"varchar|varchar"),
				posRows...,
			),
		)

		// vdiff.syncTargets (continued)
		env.tmc.vrpos[tabletID] = vdiffSourceGtid
		env.tmc.pos[tabletID] = vdiffTargetMasterPosition

		// vdiff.startQueryStreams
		env.tmc.waitpos[tabletID+1] = vdiffTargetMasterPosition

		// vdiff.restartTargets
		env.tmc.setVRResults(master.tablet, "update _vt.vreplication set state='Running', message='', stop_pos='' where db_name='vt_target' and workflow='vdiffTest'", &sqltypes.Result{})

		tabletID += 10
	}
	vdiffEnv = env
	return env
}

func (env *testVDiffEnv) close() {
	env.mu.Lock()
	defer env.mu.Unlock()
	for _, t := range env.tablets {
		env.topoServ.DeleteTablet(context.Background(), t.tablet.Alias)
	}
	env.tablets = nil
}

func (env *testVDiffEnv) addTablet(id int, keyspace, shard string, tabletType topodatapb.TabletType) *testVDiffTablet {
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
	env.tablets[id] = newTestVDiffTablet(tablet)
	if err := env.wr.InitTablet(context.Background(), tablet, false /* allowMasterOverride */, true /* createShardAndKeyspace */, false /* allowUpdate */); err != nil {
		panic(err)
	}
	if tabletType == topodatapb.TabletType_MASTER {
		_, err := env.wr.ts.UpdateShardFields(context.Background(), keyspace, shard, func(si *topo.ShardInfo) error {
			si.MasterAlias = tablet.Alias
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	return env.tablets[id]
}

//----------------------------------------------
// testVDiffTablet

type testVDiffTablet struct {
	queryservice.QueryService
	tablet  *topodatapb.Tablet
	queries map[string][]*binlogdatapb.VStreamResultsResponse
}

func newTestVDiffTablet(tablet *topodatapb.Tablet) *testVDiffTablet {
	return &testVDiffTablet{
		QueryService: fakes.ErrorQueryService,
		tablet:       tablet,
		queries:      make(map[string][]*binlogdatapb.VStreamResultsResponse),
	}
}

func (tvt *testVDiffTablet) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
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

func (tvt *testVDiffTablet) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	results, ok := tvt.queries[query]
	if !ok {
		return fmt.Errorf("query %q not in list", query)
	}
	for _, result := range results {
		if err := send(result); err != nil {
			return err
		}
	}
	return nil
}

func (tvt *testVDiffTablet) setResults(query string, gtid string, results []*sqltypes.Result) {
	vrs := []*binlogdatapb.VStreamResultsResponse{{
		Fields: results[0].Fields,
		Gtid:   gtid,
	}}

	for _, result := range results[1:] {
		vr := &binlogdatapb.VStreamResultsResponse{
			Rows: sqltypes.RowsToProto3(result.Rows),
		}
		vrs = append(vrs, vr)
	}
	tvt.queries[query] = vrs
}

//----------------------------------------------
// testVDiffTMCclient

type testVDiffTMClient struct {
	tmclient.TabletManagerClient
	schema    *tabletmanagerdatapb.SchemaDefinition
	vrQueries map[int]map[string]*querypb.QueryResult
	waitpos   map[int]string
	vrpos     map[int]string
	pos       map[int]string
}

func newTestVDiffTMClient() *testVDiffTMClient {
	return &testVDiffTMClient{
		vrQueries: make(map[int]map[string]*querypb.QueryResult),
		waitpos:   make(map[int]string),
		vrpos:     make(map[int]string),
		pos:       make(map[int]string),
	}
}

func (tmc *testVDiffTMClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return tmc.schema, nil
}

func (tmc *testVDiffTMClient) setVRResults(tablet *topodatapb.Tablet, query string, result *sqltypes.Result) {
	queries, ok := tmc.vrQueries[int(tablet.Alias.Uid)]
	if !ok {
		queries = make(map[string]*querypb.QueryResult)
		tmc.vrQueries[int(tablet.Alias.Uid)] = queries
	}
	queries[query] = sqltypes.ResultToProto3(result)
}

func (tmc *testVDiffTMClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	result, ok := tmc.vrQueries[int(tablet.Alias.Uid)][query]
	if !ok {
		return nil, fmt.Errorf("query %q not found for tablet %d", query, tablet.Alias.Uid)
	}
	return result, nil
}

func (tmc *testVDiffTMClient) WaitForPosition(ctx context.Context, tablet *topodatapb.Tablet, pos string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if pos != tmc.waitpos[int(tablet.Alias.Uid)] {
		return fmt.Errorf("waitpos %s not reached for tablet %d", pos, tablet.Alias.Uid)
	}
	return nil
}

func (tmc *testVDiffTMClient) VReplicationWaitForPos(ctx context.Context, tablet *topodatapb.Tablet, id int, pos string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if pos != tmc.vrpos[int(tablet.Alias.Uid)] {
		return fmt.Errorf("vrpos %s not reached for tablet %d", pos, tablet.Alias.Uid)
	}
	return nil
}

func (tmc *testVDiffTMClient) MasterPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	pos, ok := tmc.pos[int(tablet.Alias.Uid)]
	if !ok {
		return "", fmt.Errorf("no master position for %d", tablet.Alias.Uid)
	}
	return pos, nil
}
