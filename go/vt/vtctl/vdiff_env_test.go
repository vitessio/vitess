/*
Copyright 2022 The Vitess Authors.

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

package vtctl

import (
	"context"
	"fmt"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	// vdiffStopPosition is the default stop position for the target vreplication.
	// It can be overridden with the positons argument to newTestVDiffEnv.
	vdiffStopPosition = "MySQL56/d834e6b8-7cbf-11ed-a1eb-0242ac120002:1-892"
	// vdiffSourceGtid should be the position reported by the source side VStreamResults.
	// It's expected to be higher the vdiffStopPosition.
	vdiffSourceGtid = "MySQL56/d834e6b8-7cbf-11ed-a1eb-0242ac120002:1-893"
	// vdiffTargetPrimaryPosition is the primary position of the target after
	// vreplication has been synchronized.
	vdiffTargetPrimaryPosition = "MySQL56/e34d6fb6-7cbf-11ed-a1eb-0242ac120002:1-892"
)

type testVDiffEnv struct {
	wr         *wrangler.Wrangler
	workflow   string
	topoServ   *topo.Server
	cell       string
	tabletType topodatapb.TabletType
	tmc        *testVDiffTMClient
	cmdlog     *logutil.MemoryLogger

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
	tabletconntest.SetProtocol("go.vt.vtctl.vdiff_env_test", "VDiffTest")
	env := &testVDiffEnv{
		workflow:   "vdiffTest",
		tablets:    make(map[int]*testVDiffTablet),
		topoServ:   memorytopo.NewServer("cell"),
		cell:       "cell",
		tabletType: topodatapb.TabletType_REPLICA,
		tmc:        newTestVDiffTMClient(),
		cmdlog:     logutil.NewMemoryLogger(),
	}
	env.wr = wrangler.NewTestWrangler(env.cmdlog, env.topoServ, env.tmc)

	tabletID := 100
	for _, shard := range sourceShards {
		_ = env.addTablet(tabletID, "source", shard, topodatapb.TabletType_PRIMARY)
		env.tmc.waitpos[tabletID+1] = vdiffStopPosition

		tabletID += 10
	}
	tabletID = 200
	for _, shard := range targetShards {
		primary := env.addTablet(tabletID, "target", shard, topodatapb.TabletType_PRIMARY)

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
				primary.tablet,
				fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos='%s', message='synchronizing for vdiff' where id=%d", vdiffSourceGtid, j+1),
				&sqltypes.Result{},
			)
		}
		// migrater buildMigrationTargets
		env.tmc.setVRResults(
			primary.tablet,
			"select id, source, message, cell, tablet_types, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where workflow='vdiffTest' and db_name='vt_target'",
			sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
				"int64|varchar|varchar|varchar|varchar|int64|int64|int64"),
				rows...,
			),
		)

		// vdiff.stopTargets
		env.tmc.setVRResults(primary.tablet, "update _vt.vreplication set state='Stopped', message='for vdiff' where db_name='vt_target' and workflow='vdiffTest'", &sqltypes.Result{})
		env.tmc.setVRResults(
			primary.tablet,
			"select source, pos from _vt.vreplication where db_name='vt_target' and workflow='vdiffTest'",
			sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"source|pos",
				"varchar|varchar"),
				posRows...,
			),
		)

		// vdiff.syncTargets (continued)
		env.tmc.vrpos[tabletID] = vdiffSourceGtid
		env.tmc.pos[tabletID] = vdiffTargetPrimaryPosition

		// vdiff.startQueryStreams
		env.tmc.waitpos[tabletID+1] = vdiffTargetPrimaryPosition

		// vdiff.restartTargets
		env.tmc.setVRResults(primary.tablet, "update _vt.vreplication set state='Running', message='', stop_pos='' where db_name='vt_target' and workflow='vdiffTest'", &sqltypes.Result{})

		tabletID += 10
	}
	env.cmdlog.Clear()
	return env
}

func (env *testVDiffEnv) close() {
	env.mu.Lock()
	defer env.mu.Unlock()
	for _, t := range env.tablets {
		env.topoServ.DeleteTablet(context.Background(), t.tablet.Alias)
	}
	env.tablets = nil
	env.cmdlog.Clear()
	env.topoServ.Close()
	env.wr = nil
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
	if err := env.topoServ.InitTablet(context.Background(), tablet, false /* allowPrimaryOverride */, true /* createShardAndKeyspace */, false /* allowUpdate */); err != nil {
		panic(err)
	}
	if tabletType == topodatapb.TabletType_PRIMARY {
		_, err := env.topoServ.UpdateShardFields(context.Background(), keyspace, shard, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = tablet.Alias
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
	tablet *topodatapb.Tablet
}

func newTestVDiffTablet(tablet *topodatapb.Tablet) *testVDiffTablet {
	return &testVDiffTablet{
		QueryService: fakes.ErrorQueryService,
		tablet:       tablet,
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

//----------------------------------------------
// testVDiffTMCclient

type testVDiffTMClient struct {
	tmclient.TabletManagerClient
	vrQueries  map[int]map[string]*querypb.QueryResult
	vdRequests map[int]map[string]*tabletmanagerdatapb.VDiffResponse
	waitpos    map[int]string
	vrpos      map[int]string
	pos        map[int]string
}

func newTestVDiffTMClient() *testVDiffTMClient {
	return &testVDiffTMClient{
		vrQueries:  make(map[int]map[string]*querypb.QueryResult),
		vdRequests: make(map[int]map[string]*tabletmanagerdatapb.VDiffResponse),
		waitpos:    make(map[int]string),
		vrpos:      make(map[int]string),
		pos:        make(map[int]string),
	}
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

func (tmc *testVDiffTMClient) setVDResults(tablet *topodatapb.Tablet, req *tabletmanagerdatapb.VDiffRequest, res *tabletmanagerdatapb.VDiffResponse) {
	reqs, ok := tmc.vdRequests[int(tablet.Alias.Uid)]
	if !ok {
		reqs = make(map[string]*tabletmanagerdatapb.VDiffResponse)
		tmc.vdRequests[int(tablet.Alias.Uid)] = reqs
	}
	reqs[req.VdiffUuid] = res
}

func (tmc *testVDiffTMClient) VDiff(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error) {
	resp, ok := tmc.vdRequests[int(tablet.Alias.Uid)][req.VdiffUuid]
	if !ok {
		return nil, fmt.Errorf("request %+v not found for tablet %d", req, tablet.Alias.Uid)
	}
	return resp, nil
}
