/*
Copyright 2023 The Vitess Authors.

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

package vdiff

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/workflow"
	"vitess.io/vitess/go/vt/vtenv"
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
	ws             *workflow.Server
	sourceKeyspace string
	targetKeyspace string
	workflow       string
	topoServ       *topo.Server
	cell           string
	tabletType     topodatapb.TabletType
	tmc            *testVDiffTMClient
	out            io.Writer // Capture command output

	mu      sync.Mutex
	tablets map[int]*testVDiffTablet
}

//----------------------------------------------
// testVDiffEnv

func newTestVDiffEnv(t testing.TB, ctx context.Context, sourceShards, targetShards []string, query string, positions map[string]string) *testVDiffEnv {
	env := &testVDiffEnv{
		sourceKeyspace: "sourceks",
		targetKeyspace: "targetks",
		workflow:       "vdiffTest",
		tablets:        make(map[int]*testVDiffTablet),
		topoServ:       memorytopo.NewServer(ctx, "cell"),
		cell:           "cell",
		tabletType:     topodatapb.TabletType_REPLICA,
		tmc:            newTestVDiffTMClient(),
	}
	env.ws = workflow.NewServer(vtenv.NewTestEnv(), env.topoServ, env.tmc)
	env.tmc.testEnv = env

	// Generate a unique dialer name.
	dialerName := fmt.Sprintf("VDiffTest-%s-%d", t.Name(), rand.IntN(1000000000))
	tabletconn.RegisterDialer(dialerName, func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		env.mu.Lock()
		defer env.mu.Unlock()
		if qs, ok := env.tablets[int(tablet.Alias.Uid)]; ok {
			return qs, nil
		}
		return nil, fmt.Errorf("tablet %d not found", tablet.Alias.Uid)
	})
	tabletconntest.SetProtocol("go.cmd.vtctldclient.vreplication.vdiff_env_test", dialerName)

	tabletID := 100
	for _, shard := range sourceShards {
		_ = env.addTablet(tabletID, env.sourceKeyspace, shard, topodatapb.TabletType_PRIMARY)
		env.tmc.waitpos[tabletID+1] = vdiffStopPosition

		tabletID += 10
	}
	tabletID = 200
	for _, shard := range targetShards {
		primary := env.addTablet(tabletID, env.targetKeyspace, shard, topodatapb.TabletType_PRIMARY)

		var rows []string
		var posRows []string
		for j, sourceShard := range sourceShards {
			bls := &binlogdatapb.BinlogSource{
				Keyspace: env.sourceKeyspace,
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
	env.resetOutput()
	return env
}

func (env *testVDiffEnv) getOutput() string {
	env.mu.Lock()
	defer env.mu.Unlock()
	bb, ok := env.out.(*bytes.Buffer)
	if !ok {
		panic(fmt.Sprintf("unexpected output type for test env: %T", env.out))
	}
	return bb.String()
}

func (env *testVDiffEnv) resetOutput() {
	env.mu.Lock()
	defer env.mu.Unlock()
	env.out = &bytes.Buffer{}
}

func (env *testVDiffEnv) close() {
	env.mu.Lock()
	defer env.mu.Unlock()
	for _, t := range env.tablets {
		_ = env.topoServ.DeleteTablet(context.Background(), t.tablet.Alias)
	}
	env.tablets = nil
	env.topoServ.Close()
	env.ws = nil
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

	testEnv *testVDiffEnv // For access to the test environment
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

func (tmc *testVDiffTMClient) ReadVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
	id := int32(1)
	resp := &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
		Workflow: "vdiffTest",
	}

	sourceShards, _ := tmc.testEnv.topoServ.GetShardNames(ctx, tmc.testEnv.sourceKeyspace)
	streams := make([]*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream, 0, len(sourceShards))
	for _, shard := range sourceShards {
		streams = append(streams, &tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
			Id: id,
			Bls: &binlogdatapb.BinlogSource{
				Keyspace: tmc.testEnv.sourceKeyspace,
				Shard:    shard,
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{
						{
							Match: ".*",
						},
					},
				},
			},
		})
		id++
	}
	resp.Streams = streams

	return resp, nil
}
