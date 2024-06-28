/*
Copyright 2024 The Vitess Authors.

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

package workflow

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	_flag "vitess.io/vitess/go/internal/flag"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	defaultCellName         = "cell"
	startingSourceTabletUID = 100
	startingTargetTabletUID = 200
	tabletUIDStep           = 10
)

type testKeyspace struct {
	KeyspaceName string
	ShardNames   []string
}

type queryResult struct {
	query  string
	result *querypb.QueryResult
}

func TestMain(m *testing.M) {
	_flag.ParseFlagsForTest()
	os.Exit(m.Run())
}

type testEnv struct {
	ws                             *Server
	ts                             *topo.Server
	tmc                            *testTMClient
	sourceKeyspace, targetKeyspace *testKeyspace
	// Keyed first by keyspace name, then tablet UID.
	tablets map[string]map[int]*topodatapb.Tablet
	cell    string
}

func newTestEnv(t *testing.T, ctx context.Context, cell string, sourceKeyspace, targetKeyspace *testKeyspace) *testEnv {
	t.Helper()
	env := &testEnv{
		ts:             memorytopo.NewServer(ctx, cell),
		sourceKeyspace: sourceKeyspace,
		targetKeyspace: targetKeyspace,
		tablets:        make(map[string]map[int]*topodatapb.Tablet),
		cell:           cell,
	}
	venv := vtenv.NewTestEnv()
	env.tmc = newTestTMClient(env)
	env.ws = NewServer(venv, env.ts, env.tmc)

	serving := true
	tabletID := startingSourceTabletUID
	for _, shardName := range sourceKeyspace.ShardNames {
		_ = env.addTablet(t, ctx, tabletID, sourceKeyspace.KeyspaceName, shardName, topodatapb.TabletType_PRIMARY, serving)
		tabletID += tabletUIDStep
	}

	isReshard := func() bool {
		return sourceKeyspace.KeyspaceName == targetKeyspace.KeyspaceName &&
			!slices.Equal(sourceKeyspace.ShardNames, targetKeyspace.ShardNames)
	}

	if isReshard() {
		serving = false
	}
	tabletID = startingTargetTabletUID
	for _, shardName := range targetKeyspace.ShardNames {
		_ = env.addTablet(t, ctx, tabletID, targetKeyspace.KeyspaceName, shardName, topodatapb.TabletType_PRIMARY, serving)
		tabletID += tabletUIDStep
	}

	if isReshard() {
		initSrvKeyspace(t, env.ts, targetKeyspace.KeyspaceName, sourceKeyspace.ShardNames, targetKeyspace.ShardNames, []string{cell})
	}

	err := env.ts.RebuildSrvVSchema(ctx, nil)
	require.NoError(t, err)

	return env
}

func initSrvKeyspace(t *testing.T, topo *topo.Server, keyspace string, sources, targets, cells []string) {
	ctx := context.Background()
	srvKeyspace := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{},
	}
	getPartition := func(t *testing.T, shards []string) *topodatapb.SrvKeyspace_KeyspacePartition {
		partition := &topodatapb.SrvKeyspace_KeyspacePartition{
			ServedType:      topodatapb.TabletType_PRIMARY,
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
	srvKeyspace.Partitions = append(srvKeyspace.Partitions, getPartition(t, sources))
	srvKeyspace.Partitions = append(srvKeyspace.Partitions, getPartition(t, targets))
	for _, cell := range cells {
		err := topo.UpdateSrvKeyspace(ctx, cell, keyspace, srvKeyspace)
		require.NoError(t, err)
	}
	err := topo.ValidateSrvKeyspace(ctx, keyspace, strings.Join(cells, ","))
	require.NoError(t, err)
}

func (env *testEnv) close() {
	for _, k := range maps.Values(env.tablets) {
		for _, t := range maps.Values(k) {
			env.deleteTablet(t)
		}
	}
}

func (env *testEnv) addTablet(t *testing.T, ctx context.Context, id int, keyspace, shard string, tabletType topodatapb.TabletType, serving bool) *topodatapb.Tablet {
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
	if env.tablets[keyspace] == nil {
		env.tablets[keyspace] = make(map[int]*topodatapb.Tablet)
	}
	env.tablets[keyspace][id] = tablet
	err := env.ws.ts.InitTablet(ctx, tablet, false /* allowPrimaryOverride */, true /* createShardAndKeyspace */, false /* allowUpdate */)
	require.NoError(t, err)
	if tabletType == topodatapb.TabletType_PRIMARY {
		_, err = env.ws.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = tablet.Alias
			si.IsPrimaryServing = serving
			return nil
		})
		require.NoError(t, err)
	}
	return tablet
}

// addTableRoutingRules adds routing rules from the test env's source keyspace to
// its target keyspace for the given tablet types and tables.
func (env *testEnv) addTableRoutingRules(t *testing.T, ctx context.Context, tabletTypes []topodatapb.TabletType, tables []string) {
	ks := env.targetKeyspace.KeyspaceName
	rules := make(map[string][]string, len(tables)*(len(tabletTypes)*3))
	for _, tabletType := range tabletTypes {
		for _, tableName := range tables {
			toTarget := []string{ks + "." + tableName}
			tt := strings.ToLower(tabletType.String())
			if tabletType == topodatapb.TabletType_PRIMARY {
				rules[tableName] = toTarget
				rules[ks+"."+tableName] = toTarget
				rules[env.sourceKeyspace.KeyspaceName+"."+tableName] = toTarget
			} else {
				rules[tableName+"@"+tt] = toTarget
				rules[ks+"."+tableName+"@"+tt] = toTarget
				rules[env.sourceKeyspace.KeyspaceName+"."+tableName+"@"+tt] = toTarget
			}
		}
	}
	err := topotools.SaveRoutingRules(ctx, env.ts, rules)
	require.NoError(t, err)
	err = env.ts.RebuildSrvVSchema(ctx, nil)
	require.NoError(t, err)
}

func (env *testEnv) deleteTablet(tablet *topodatapb.Tablet) {
	_ = env.ts.DeleteTablet(context.Background(), tablet.Alias)
	delete(env.tablets[tablet.Keyspace], int(tablet.Alias.Uid))
}

type testTMClient struct {
	tmclient.TabletManagerClient
	schema map[string]*tabletmanagerdatapb.SchemaDefinition

	mu                                 sync.Mutex
	vrQueries                          map[int][]*queryResult
	createVReplicationWorkflowRequests map[uint32]*tabletmanagerdatapb.CreateVReplicationWorkflowRequest
	readVReplicationWorkflowRequests   map[uint32]*tabletmanagerdatapb.ReadVReplicationWorkflowRequest

	env     *testEnv    // For access to the env config from tmc methods.
	reverse atomic.Bool // Are we reversing traffic?
}

func newTestTMClient(env *testEnv) *testTMClient {
	return &testTMClient{
		schema:                             make(map[string]*tabletmanagerdatapb.SchemaDefinition),
		vrQueries:                          make(map[int][]*queryResult),
		createVReplicationWorkflowRequests: make(map[uint32]*tabletmanagerdatapb.CreateVReplicationWorkflowRequest),
		readVReplicationWorkflowRequests:   make(map[uint32]*tabletmanagerdatapb.ReadVReplicationWorkflowRequest),
		env:                                env,
	}
}

func (tmc *testTMClient) CreateVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.CreateVReplicationWorkflowRequest) (*tabletmanagerdatapb.CreateVReplicationWorkflowResponse, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if expect := tmc.createVReplicationWorkflowRequests[tablet.Alias.Uid]; expect != nil {
		if !proto.Equal(expect, req) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected CreateVReplicationWorkflow request: got %+v, want %+v", req, expect)
		}
	}
	res := sqltypes.MakeTestResult(sqltypes.MakeTestFields("rowsaffected", "int64"), "1")
	return &tabletmanagerdatapb.CreateVReplicationWorkflowResponse{Result: sqltypes.ResultToProto3(res)}, nil
}

func (tmc *testTMClient) ReadVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if expect := tmc.readVReplicationWorkflowRequests[tablet.Alias.Uid]; expect != nil {
		if !proto.Equal(expect, req) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected ReadVReplicationWorkflow request: got %+v, want %+v", req, expect)
		}
	}
	workflowType := binlogdatapb.VReplicationWorkflowType_MoveTables
	if strings.Contains(req.Workflow, "lookup") {
		workflowType = binlogdatapb.VReplicationWorkflowType_CreateLookupIndex
	}
	res := &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
		Workflow:     req.Workflow,
		WorkflowType: workflowType,
		Streams:      make([]*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream, 0, 2),
	}
	rules := make([]*binlogdatapb.Rule, len(tmc.schema))
	for i, table := range maps.Keys(tmc.schema) {
		rules[i] = &binlogdatapb.Rule{
			Match:  table,
			Filter: fmt.Sprintf("select * from %s", table),
		}
	}
	blsKs := tmc.env.sourceKeyspace
	if tmc.reverse.Load() && tablet.Keyspace == tmc.env.sourceKeyspace.KeyspaceName {
		blsKs = tmc.env.targetKeyspace
	}
	for i, shard := range blsKs.ShardNames {
		stream := &tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
			Id: int32(i + 1),
			Bls: &binlogdatapb.BinlogSource{
				Keyspace: blsKs.KeyspaceName,
				Shard:    shard,
				Tables:   maps.Keys(tmc.schema),
				Filter: &binlogdatapb.Filter{
					Rules: rules,
				},
			},
		}
		res.Streams = append(res.Streams, stream)
	}

	return res, nil
}

func (tmc *testTMClient) DeleteVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.DeleteVReplicationWorkflowRequest) (response *tabletmanagerdatapb.DeleteVReplicationWorkflowResponse, err error) {
	return &tabletmanagerdatapb.DeleteVReplicationWorkflowResponse{
		Result: &querypb.QueryResult{
			RowsAffected: 1,
		},
	}, nil
}

func (tmc *testTMClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	schemaDefn := &tabletmanagerdatapb.SchemaDefinition{}
	for _, table := range req.Tables {
		if table == "/.*/" {
			// Special case of all tables in keyspace.
			for key, tableDefn := range tmc.schema {
				if strings.HasPrefix(key, tablet.Keyspace+".") {
					schemaDefn.TableDefinitions = append(schemaDefn.TableDefinitions, tableDefn.TableDefinitions...)
				}
			}
			break
		}

		key := tablet.Keyspace + "." + table
		tableDefn := tmc.schema[key]
		if tableDefn == nil {
			continue
		}
		schemaDefn.TableDefinitions = append(schemaDefn.TableDefinitions, tableDefn.TableDefinitions...)
	}
	return schemaDefn, nil
}

func (tmc *testTMClient) expectVRQuery(tabletID int, query string, result *sqltypes.Result) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	tmc.vrQueries[tabletID] = append(tmc.vrQueries[tabletID], &queryResult{
		query:  query,
		result: sqltypes.ResultToProto3(result),
	})
}

func (tmc *testTMClient) expectVRQueryResultOnKeyspaceTablets(keyspace string, queryResult *queryResult) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	for uid := range tmc.env.tablets[keyspace] {
		tmc.vrQueries[uid] = append(tmc.vrQueries[uid], queryResult)
	}
}

func (tmc *testTMClient) expectCreateVReplicationWorkflowRequest(tabletID uint32, req *tabletmanagerdatapb.CreateVReplicationWorkflowRequest) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	tmc.createVReplicationWorkflowRequests[tabletID] = req
}

func (tmc *testTMClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	qrs := tmc.vrQueries[int(tablet.Alias.Uid)]
	if len(qrs) == 0 {
		return nil, fmt.Errorf("tablet %v does not expect any more queries: %s", tablet, query)
	}
	matched := false
	if qrs[0].query[0] == '/' {
		matched = regexp.MustCompile(qrs[0].query[1:]).MatchString(query)
	} else {
		matched = query == qrs[0].query
	}
	if !matched {
		return nil, fmt.Errorf("tablet %v:\nunexpected query\n%s\nwant:\n%s", tablet, query, qrs[0].query)
	}
	tmc.vrQueries[int(tablet.Alias.Uid)] = qrs[1:]
	return qrs[0].result, nil
}

func (tmc *testTMClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	// Reuse VReplicationExec.
	return tmc.VReplicationExec(ctx, tablet, string(req.Query))
}

func (tmc *testTMClient) ExecuteFetchAsAllPrivs(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (*querypb.QueryResult, error) {
	return nil, nil
}

// Note: ONLY breaks up change.SQL into individual statements and executes it. Does NOT fully implement ApplySchema.
func (tmc *testTMClient) ApplySchema(ctx context.Context, tablet *topodatapb.Tablet, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	stmts := strings.Split(change.SQL, ";")

	for _, stmt := range stmts {
		_, err := tmc.ExecuteFetchAsDba(ctx, tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
			Query:        []byte(stmt),
			MaxRows:      0,
			ReloadSchema: true,
		})
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (tmc *testTMClient) VDiff(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error) {
	return &tabletmanagerdatapb.VDiffResponse{
		Id:        1,
		VdiffUuid: req.VdiffUuid,
		Output: &querypb.QueryResult{
			RowsAffected: 1,
		},
	}, nil
}

func (tmc *testTMClient) UpdateVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.UpdateVReplicationWorkflowRequest) (*tabletmanagerdatapb.UpdateVReplicationWorkflowResponse, error) {
	return &tabletmanagerdatapb.UpdateVReplicationWorkflowResponse{
		Result: &querypb.QueryResult{
			RowsAffected: 1,
		},
	}, nil
}

func (tmc *testTMClient) PrimaryPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	return position, nil
}

func (tmc *testTMClient) WaitForPosition(ctx context.Context, tablet *topodatapb.Tablet, pos string) error {
	return nil
}

func (tmc *testTMClient) VReplicationWaitForPos(ctx context.Context, tablet *topodatapb.Tablet, id int32, pos string) error {
	return nil
}
