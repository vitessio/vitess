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
	"fmt"
	"regexp"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

type testResharderEnv struct {
	wr       *Wrangler
	keyspace string
	workflow string
	sources  []string
	targets  []string
	tablets  map[int]*topodatapb.Tablet
	topoServ *topo.Server
	cell     string
	tmc      *testResharderTMClient
}

//----------------------------------------------
// testResharderEnv

func newTestResharderEnv(sources, targets []string) *testResharderEnv {
	env := &testResharderEnv{
		keyspace: "ks",
		workflow: "resharderTest",
		sources:  sources,
		targets:  targets,
		tablets:  make(map[int]*topodatapb.Tablet),
		topoServ: memorytopo.NewServer("cell"),
		cell:     "cell",
		tmc:      newTestResharderTMClient(),
	}
	env.wr = New(logutil.NewConsoleLogger(), env.topoServ, env.tmc)

	tabletID := 100
	for _, shard := range sources {
		master := env.addTablet(tabletID, env.keyspace, shard, topodatapb.TabletType_MASTER)

		// wr.validateNewWorkflow
		env.tmc.setVRResults(master, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
		// readRefStreams
		env.tmc.setVRResults(master, fmt.Sprintf("select workflow, source, cell, tablet_types from _vt.vreplication where db_name='vt_%s'", env.keyspace), &sqltypes.Result{})

		tabletID += 10
	}
	tabletID = 200
	for _, shard := range targets {
		master := env.addTablet(tabletID, env.keyspace, shard, topodatapb.TabletType_MASTER)

		// wr.validateNewWorkflow
		env.tmc.setVRResults(master, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
		// validateTargets
		env.tmc.setVRResults(master, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s'", env.keyspace), &sqltypes.Result{})

		tabletID += 10
	}
	return env
}

func (env *testResharderEnv) close() {
	for _, t := range env.tablets {
		env.deleteTablet(t)
	}
}

func (env *testResharderEnv) addTablet(id int, keyspace, shard string, tabletType topodatapb.TabletType) *topodatapb.Tablet {
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
	env.tablets[id] = tablet
	if err := env.wr.InitTablet(context.Background(), tablet, false /* allowMasterOverride */, true /* createShardAndKeyspace */, false /* allowUpdate */); err != nil {
		panic(err)
	}
	return tablet
}

func (env *testResharderEnv) deleteTablet(tablet *topodatapb.Tablet) {
	env.topoServ.DeleteTablet(context.Background(), tablet.Alias)
	delete(env.tablets, int(tablet.Alias.Uid))
}

//----------------------------------------------
// testResharderTMClient

type testResharderTMClient struct {
	tmclient.TabletManagerClient
	schema      *tabletmanagerdatapb.SchemaDefinition
	vrQueries   map[int]map[string]*querypb.QueryResult
	vrQueriesRE map[int]map[string]*querypb.QueryResult
}

func newTestResharderTMClient() *testResharderTMClient {
	return &testResharderTMClient{
		vrQueries:   make(map[int]map[string]*querypb.QueryResult),
		vrQueriesRE: make(map[int]map[string]*querypb.QueryResult),
	}
}

func (tmc *testResharderTMClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return tmc.schema, nil
}

func (tmc *testResharderTMClient) setVRResults(tablet *topodatapb.Tablet, query string, result *sqltypes.Result) {
	queries, ok := tmc.vrQueries[int(tablet.Alias.Uid)]
	if !ok {
		queries = make(map[string]*querypb.QueryResult)
		tmc.vrQueries[int(tablet.Alias.Uid)] = queries
	}
	queries[query] = sqltypes.ResultToProto3(result)
}

func (tmc *testResharderTMClient) setVRResultsRE(tablet *topodatapb.Tablet, query string, result *sqltypes.Result) {
	queriesRE, ok := tmc.vrQueriesRE[int(tablet.Alias.Uid)]
	if !ok {
		queriesRE = make(map[string]*querypb.QueryResult)
		tmc.vrQueriesRE[int(tablet.Alias.Uid)] = queriesRE
	}
	queriesRE[query] = sqltypes.ResultToProto3(result)
}

func (tmc *testResharderTMClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	result, ok := tmc.vrQueries[int(tablet.Alias.Uid)][query]
	if ok {
		return result, nil
	}
	queriesRE, ok := tmc.vrQueriesRE[int(tablet.Alias.Uid)]
	if ok {
		for re, result := range queriesRE {
			if regexp.MustCompile(re).MatchString(query) {
				return result, nil
			}
		}
	}
	return nil, fmt.Errorf("query %q not found for tablet %d", query, tablet.Alias.Uid)
}
