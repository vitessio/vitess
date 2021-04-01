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
	"runtime/debug"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"

	"context"

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

var (
	testMode = "" //"debug"
)

//----------------------------------------------
// testResharderEnv

func getPartition(t *testing.T, shards []string) *topodatapb.SrvKeyspace_KeyspacePartition {
	partition := &topodatapb.SrvKeyspace_KeyspacePartition{
		ServedType:      topodatapb.TabletType_MASTER,
		ShardReferences: []*topodatapb.ShardReference{},
	}
	for _, shard := range shards {
		keyRange, err := key.ParseShardingSpec(shard)
		require.NoError(t, err)
		require.Equal(t, 1, len(keyRange))
		partition.ShardReferences = append(partition.ShardReferences, &topodatapb.ShardReference{
			Name:     shard,
			KeyRange: keyRange[0],
		})
	}
	return partition
}
func initTopo(t *testing.T, topo *topo.Server, keyspace string, sources, targets, cells []string) {
	ctx := context.Background()
	srvKeyspace := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{},
	}
	srvKeyspace.Partitions = append(srvKeyspace.Partitions, getPartition(t, sources))
	srvKeyspace.Partitions = append(srvKeyspace.Partitions, getPartition(t, targets))
	for _, cell := range cells {
		topo.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace)
	}
	topo.ValidateSrvKeyspace(ctx, keyspace, strings.Join(cells, ","))
}

func newTestResharderEnv(t *testing.T, sources, targets []string) *testResharderEnv {
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
	initTopo(t, env.topoServ, "ks", sources, targets, []string{"cell"})
	tabletID := 100
	for _, shard := range sources {
		_ = env.addTablet(tabletID, env.keyspace, shard, topodatapb.TabletType_MASTER)
		tabletID += 10
	}
	tabletID = 200
	for _, shard := range targets {
		_ = env.addTablet(tabletID, env.keyspace, shard, topodatapb.TabletType_MASTER)
		tabletID += 10
	}
	return env
}

func (env *testResharderEnv) expectValidation() {
	for _, tablet := range env.tablets {
		tabletID := int(tablet.Alias.Uid)
		// wr.validateNewWorkflow
		env.tmc.expectVRQuery(tabletID, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
		env.tmc.expectVRQuery(tabletID, rsSelectFrozenQuery, &sqltypes.Result{})

		if tabletID >= 200 {
			// validateTargets
			env.tmc.expectVRQuery(tabletID, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s'", env.keyspace), &sqltypes.Result{})
		}
	}
}

func (env *testResharderEnv) expectNoRefStream() {
	for _, tablet := range env.tablets {
		tabletID := int(tablet.Alias.Uid)
		if tabletID < 200 {
			// readRefStreams
			env.tmc.expectVRQuery(tabletID, fmt.Sprintf("select workflow, source, cell, tablet_types from _vt.vreplication where db_name='vt_%s' and message != 'FROZEN'", env.keyspace), &sqltypes.Result{})
		}
	}
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
	if tabletType == topodatapb.TabletType_MASTER {
		_, err := env.wr.ts.UpdateShardFields(context.Background(), keyspace, shard, func(si *topo.ShardInfo) error {
			si.MasterAlias = tablet.Alias
			return nil
		})
		if err != nil {
			panic(err)
		}
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
	schema *tabletmanagerdatapb.SchemaDefinition

	mu        sync.Mutex
	vrQueries map[int][]*queryResult
}

type queryResult struct {
	query  string
	result *querypb.QueryResult
}

func newTestResharderTMClient() *testResharderTMClient {
	return &testResharderTMClient{
		vrQueries: make(map[int][]*queryResult),
	}
}

func (tmc *testResharderTMClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return tmc.schema, nil
}

func (tmc *testResharderTMClient) expectVRQuery(tabletID int, query string, result *sqltypes.Result) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	tmc.vrQueries[tabletID] = append(tmc.vrQueries[tabletID], &queryResult{
		query:  query,
		result: sqltypes.ResultToProto3(result),
	})
}

func (tmc *testResharderTMClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	if testMode == "debug" {
		fmt.Printf("Got: %d:%s\n", tablet.Alias.Uid, query)
	}
	qrs := tmc.vrQueries[int(tablet.Alias.Uid)]
	if len(qrs) == 0 {
		if testMode == "debug" {
			fmt.Printf("Want: %d:%s, Stack:\n%v\n", tablet.Alias.Uid, query, debug.Stack())
		}
		return nil, fmt.Errorf("tablet %v does not expect any more queries: %s", tablet, query)
	}
	matched := false
	if qrs[0].query[0] == '/' {
		matched = regexp.MustCompile(qrs[0].query[1:]).MatchString(query)
	} else {
		matched = query == qrs[0].query
	}
	if !matched {
		return nil, fmt.Errorf("tablet %v: unexpected query %s, want: %s", tablet, query, qrs[0].query)
	}
	tmc.vrQueries[int(tablet.Alias.Uid)] = qrs[1:]
	return qrs[0].result, nil
}

func (tmc *testResharderTMClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, query []byte, maxRows int, disableBinlogs, reloadSchema bool) (*querypb.QueryResult, error) {
	// Reuse VReplicationExec
	return tmc.VReplicationExec(ctx, tablet, string(query))
}

func (tmc *testResharderTMClient) verifyQueries(t *testing.T) {
	t.Helper()

	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	for tabletID, qrs := range tmc.vrQueries {
		if len(qrs) != 0 {
			var list []string
			for _, qr := range qrs {
				list = append(list, qr.query)
			}
			t.Errorf("tablet %v: following queries were not run during the test: \n%v", tabletID, strings.Join(list, "\n"))
		}
	}
}
