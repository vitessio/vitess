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

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

type testVTCtlEnv struct {
	wr         *wrangler.Wrangler
	topoServ   *topo.Server
	cell       string
	tabletType topodatapb.TabletType
	tmc        *testVTCtlTMClient
	cmdlog     *logutil.MemoryLogger

	mu      sync.Mutex
	tablets map[int]*testVTCtlTablet
}

// vtctlEnv has to be a global for RegisterDialer to work.
var vtctlEnv *testVTCtlEnv

func init() {
	tabletconn.RegisterDialer("VTCtlTest", func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		vtctlEnv.mu.Lock()
		defer vtctlEnv.mu.Unlock()
		if qs, ok := vtctlEnv.tablets[int(tablet.Alias.Uid)]; ok {
			return qs, nil
		}
		return nil, fmt.Errorf("tablet %d not found", tablet.Alias.Uid)
	})
}

//----------------------------------------------
// testVTCtlEnv

func newTestVTCtlEnv() *testVTCtlEnv {
	tabletconntest.SetProtocol("go.vt.vtctl.vtctl_env_test", "VTCtlTest")
	cellName := "cell1"
	env := &testVTCtlEnv{
		tablets:    make(map[int]*testVTCtlTablet),
		topoServ:   memorytopo.NewServer(cellName),
		cell:       cellName,
		tabletType: topodatapb.TabletType_REPLICA,
		tmc:        newTestVTCtlTMClient(),
		cmdlog:     logutil.NewMemoryLogger(),
	}
	env.wr = wrangler.NewTestWrangler(env.cmdlog, env.topoServ, env.tmc)
	return env
}

func (env *testVTCtlEnv) close() {
	env.mu.Lock()
	defer env.mu.Unlock()
	for _, t := range env.tablets {
		env.topoServ.DeleteTablet(context.Background(), t.tablet.Alias)
	}
	env.tablets = nil
	env.cmdlog.Clear()
	env.tmc.clearResults()
	env.topoServ.Close()
	env.wr = nil
}

func (env *testVTCtlEnv) addTablet(id int, keyspace, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType) *testVTCtlTablet {
	env.mu.Lock()
	defer env.mu.Unlock()
	ctx := context.Background()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: env.cell,
			Uid:  uint32(id),
		},
		Keyspace: keyspace,
		Shard:    shard,
		KeyRange: keyRange,
		Type:     tabletType,
		PortMap: map[string]int32{
			"test": int32(id),
		},
	}
	env.tablets[id] = newTestVTCtlTablet(tablet)
	if err := env.topoServ.InitTablet(ctx, tablet, false /* allowPrimaryOverride */, true /* createShardAndKeyspace */, false /* allowUpdate */); err != nil {
		panic(err)
	}
	if tabletType == topodatapb.TabletType_PRIMARY {
		_, err := env.topoServ.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = tablet.Alias
			return nil
		})
		if err != nil {
			panic(err)
		}
		emptySrvVSchema := &vschemapb.SrvVSchema{
			RoutingRules:      &vschemapb.RoutingRules{},
			ShardRoutingRules: &vschemapb.ShardRoutingRules{},
		}
		if err = env.topoServ.UpdateSrvVSchema(ctx, env.cell, emptySrvVSchema); err != nil {
			panic(err)
		}
	}
	return env.tablets[id]
}

//----------------------------------------------
// testVTCtlTablet

type testVTCtlTablet struct {
	queryservice.QueryService
	tablet *topodatapb.Tablet
}

func newTestVTCtlTablet(tablet *topodatapb.Tablet) *testVTCtlTablet {
	return &testVTCtlTablet{
		QueryService: fakes.ErrorQueryService,
		tablet:       tablet,
	}
}

func (tvt *testVTCtlTablet) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
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
// testVTCtlTMClient

type testVTCtlTMClient struct {
	tmclient.TabletManagerClient
	vrQueries  map[int]map[string]*querypb.QueryResult
	dbaQueries map[int]map[string]*querypb.QueryResult
}

func newTestVTCtlTMClient() *testVTCtlTMClient {
	return &testVTCtlTMClient{
		vrQueries:  make(map[int]map[string]*querypb.QueryResult),
		dbaQueries: make(map[int]map[string]*querypb.QueryResult),
	}
}

func (tmc *testVTCtlTMClient) setVRResults(tablet *topodatapb.Tablet, query string, result *sqltypes.Result) {
	queries, ok := tmc.vrQueries[int(tablet.Alias.Uid)]
	if !ok {
		queries = make(map[string]*querypb.QueryResult)
		tmc.vrQueries[int(tablet.Alias.Uid)] = queries
	}
	queries[query] = sqltypes.ResultToProto3(result)
}

func (tmc *testVTCtlTMClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	result, ok := tmc.vrQueries[int(tablet.Alias.Uid)][query]
	if !ok {
		return nil, fmt.Errorf("query %q not found for VReplicationExec() on tablet %d", query, tablet.Alias.Uid)
	}
	return result, nil
}

func (tmc *testVTCtlTMClient) setDBAResults(tablet *topodatapb.Tablet, query string, result *sqltypes.Result) {
	queries, ok := tmc.dbaQueries[int(tablet.Alias.Uid)]
	if !ok {
		queries = make(map[string]*querypb.QueryResult)
		tmc.dbaQueries[int(tablet.Alias.Uid)] = queries
	}
	queries[query] = sqltypes.ResultToProto3(result)
}

func (tmc *testVTCtlTMClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	result, ok := tmc.dbaQueries[int(tablet.Alias.Uid)][string(req.Query)]
	if !ok {
		return nil, fmt.Errorf("query %q not found for ExecuteFetchAsDba() on tablet %d", req.Query, tablet.Alias.Uid)
	}
	return result, nil
}

func (tmc *testVTCtlTMClient) clearResults() {
	tmc.vrQueries = make(map[int]map[string]*querypb.QueryResult)
	tmc.dbaQueries = make(map[int]map[string]*querypb.QueryResult)
}
