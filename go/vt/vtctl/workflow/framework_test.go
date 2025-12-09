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
	"cmp"
	"context"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
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

var defaultTabletTypes = []topodatapb.TabletType{
	topodatapb.TabletType_PRIMARY,
	topodatapb.TabletType_REPLICA,
	topodatapb.TabletType_RDONLY,
}

type testKeyspace struct {
	KeyspaceName string
	ShardNames   []string
}

type queryResult struct {
	query  string
	result *querypb.QueryResult
	err    error
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
			require.Len(t, keyRange, 1)
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
		Hostname: "localhost", // Without a hostname the RefreshState call is skipped
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

func (env *testEnv) saveRoutingRules(t *testing.T, rules map[string][]string) {
	err := topotools.SaveRoutingRules(context.Background(), env.ts, rules)
	require.NoError(t, err)
	err = env.ts.RebuildSrvVSchema(context.Background(), nil)
	require.NoError(t, err)
}

func (env *testEnv) updateTableRoutingRules(t *testing.T, ctx context.Context,
	tabletTypes []topodatapb.TabletType, tables []string, sourceKeyspace, targetKeyspace, toKeyspace string) {
	if len(tabletTypes) == 0 {
		tabletTypes = defaultTabletTypes
	}
	rr, err := env.ts.GetRoutingRules(ctx)
	require.NoError(t, err)
	rules := topotools.GetRoutingRulesMap(rr)
	for _, tabletType := range tabletTypes {
		for _, tableName := range tables {
			toTarget := []string{toKeyspace + "." + tableName}
			tt := strings.ToLower(tabletType.String())
			if tabletType == topodatapb.TabletType_PRIMARY {
				rules[tableName] = toTarget
				rules[targetKeyspace+"."+tableName] = toTarget
				rules[sourceKeyspace+"."+tableName] = toTarget
			} else {
				rules[tableName+"@"+tt] = toTarget
				rules[targetKeyspace+"."+tableName+"@"+tt] = toTarget
				rules[sourceKeyspace+"."+tableName+"@"+tt] = toTarget
			}
		}
	}
	env.saveRoutingRules(t, rules)
}

func (env *testEnv) deleteTablet(tablet *topodatapb.Tablet) {
	_ = env.ts.DeleteTablet(context.Background(), tablet.Alias)
	delete(env.tablets[tablet.Keyspace], int(tablet.Alias.Uid))
}

func (env *testEnv) confirmRoutingAllTablesToTarget(t *testing.T) {
	t.Helper()
	env.tmc.mu.Lock()
	defer env.tmc.mu.Unlock()
	wantRR := make(map[string][]string)
	for _, sd := range env.tmc.schema {
		for _, td := range sd.TableDefinitions {
			for _, tt := range []string{"", "@rdonly", "@replica"} {
				wantRR[td.Name+tt] = []string{fmt.Sprintf("%s.%s", env.targetKeyspace.KeyspaceName, td.Name)}
				wantRR[fmt.Sprintf("%s.%s", env.sourceKeyspace.KeyspaceName, td.Name+tt)] = []string{fmt.Sprintf("%s.%s", env.targetKeyspace.KeyspaceName, td.Name)}
				wantRR[fmt.Sprintf("%s.%s", env.targetKeyspace.KeyspaceName, td.Name+tt)] = []string{fmt.Sprintf("%s.%s", env.targetKeyspace.KeyspaceName, td.Name)}
			}
		}
	}
	checkRouting(t, env.ws, wantRR)
}

type testTMClient struct {
	tmclient.TabletManagerClient
	schema map[string]*tabletmanagerdatapb.SchemaDefinition

	mu                                 sync.Mutex
	vrQueries                          map[int][]*queryResult
	createVReplicationWorkflowRequests map[uint32]*createVReplicationWorkflowRequestResponse
	getMaxValueForSequencesRequests    map[uint32]*getMaxValueForSequencesRequestResponse
	updateSequenceTablesRequests       map[uint32]*tabletmanagerdatapb.UpdateSequenceTablesRequest
	readVReplicationWorkflowRequests   map[uint32]*readVReplicationWorkflowRequestResponse
	updateVReplicationWorklowsRequests map[uint32]*tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest
	updateVReplicationWorklowRequests  map[uint32][]*updateVReplicationWorkflowRequestResponse
	applySchemaRequests                map[uint32][]*applySchemaRequestResponse
	primaryPositions                   map[uint32]string
	vdiffRequests                      map[uint32]*vdiffRequestResponse
	refreshStateErrors                 map[uint32]error

	// Stack of ReadVReplicationWorkflowsResponse to return, in order, for each shard
	readVReplicationWorkflowsResponses       map[string][]*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse
	validateVReplicationPermissionsResponses map[uint32]*validateVReplicationPermissionsResponse

	env     *testEnv    // For access to the env config from tmc methods.
	reverse atomic.Bool // Are we reversing traffic?
	frozen  atomic.Bool // Are the workflows frozen?

	// Can be used to return an error if an unexpected request is made or
	// an expected request is NOT made.
	strict bool
}

func newTestTMClient(env *testEnv) *testTMClient {
	return &testTMClient{
		schema:                             make(map[string]*tabletmanagerdatapb.SchemaDefinition),
		vrQueries:                          make(map[int][]*queryResult),
		createVReplicationWorkflowRequests: make(map[uint32]*createVReplicationWorkflowRequestResponse),
		getMaxValueForSequencesRequests:    make(map[uint32]*getMaxValueForSequencesRequestResponse),
		updateSequenceTablesRequests:       make(map[uint32]*tabletmanagerdatapb.UpdateSequenceTablesRequest),
		readVReplicationWorkflowRequests:   make(map[uint32]*readVReplicationWorkflowRequestResponse),
		updateVReplicationWorklowsRequests: make(map[uint32]*tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest),
		updateVReplicationWorklowRequests:  make(map[uint32][]*updateVReplicationWorkflowRequestResponse),
		applySchemaRequests:                make(map[uint32][]*applySchemaRequestResponse),
		readVReplicationWorkflowsResponses: make(map[string][]*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse),
		primaryPositions:                   make(map[uint32]string),
		vdiffRequests:                      make(map[uint32]*vdiffRequestResponse),
		refreshStateErrors:                 make(map[uint32]error),
		env:                                env,
	}
}

func (tmc *testTMClient) CreateVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.CreateVReplicationWorkflowRequest) (*tabletmanagerdatapb.CreateVReplicationWorkflowResponse, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if expect := tmc.createVReplicationWorkflowRequests[tablet.Alias.Uid]; expect != nil {
		if expect.req != nil && !proto.Equal(expect.req, req) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected CreateVReplicationWorkflow request on tablet %s: got %+v, want %+v",
				topoproto.TabletAliasString(tablet.Alias), req, expect)
		}
		if expect.res != nil {
			return expect.res, expect.err
		}
	}
	res := sqltypes.MakeTestResult(sqltypes.MakeTestFields("rowsaffected", "int64"), "1")
	return &tabletmanagerdatapb.CreateVReplicationWorkflowResponse{Result: sqltypes.ResultToProto3(res)}, nil
}

func (tmc *testTMClient) GetWorkflowKey(keyspace, shard string) string {
	return fmt.Sprintf("%s/%s", keyspace, shard)
}

func (tmc *testTMClient) ReadVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	if expect := tmc.readVReplicationWorkflowRequests[tablet.Alias.Uid]; expect != nil {
		if !proto.Equal(expect.req, req) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected ReadVReplicationWorkflow request on tablet %s: got %+v, want %+v",
				topoproto.TabletAliasString(tablet.Alias), req, expect)
		}
		if expect.res != nil {
			return expect.res, expect.err
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
			Filter: "select * from " + table,
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
		if tmc.frozen.Load() {
			stream.Message = Frozen
		}
		res.Streams = append(res.Streams, stream)
	}

	return res, nil
}

func (tmc *testTMClient) DeleteTableData(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.DeleteTableDataRequest) (response *tabletmanagerdatapb.DeleteTableDataResponse, err error) {
	return &tabletmanagerdatapb.DeleteTableDataResponse{}, nil
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
			for key, schemaDefinition := range tmc.schema {
				if strings.HasPrefix(key, tablet.Keyspace+".") {
					schemaDefn.TableDefinitions = append(schemaDefn.TableDefinitions, schemaDefinition.TableDefinitions...)
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
	for key, schemaDefinition := range tmc.schema {
		if strings.HasPrefix(key, tablet.Keyspace) {
			schemaDefn.DatabaseSchema = schemaDefinition.DatabaseSchema
			break
		}
	}
	return schemaDefn, nil
}

func (tmc *testTMClient) expectUpdateSequenceTablesRequest(tabletID uint32, req *tabletmanagerdatapb.UpdateSequenceTablesRequest) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	tmc.updateSequenceTablesRequests[tabletID] = req
}

func (tmc *testTMClient) expectGetMaxValueForSequencesRequest(tabletID uint32, reqres *getMaxValueForSequencesRequestResponse) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	tmc.getMaxValueForSequencesRequests[tabletID] = reqres
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

func (tmc *testTMClient) expectCreateVReplicationWorkflowRequest(tabletID uint32, req *createVReplicationWorkflowRequestResponse) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	tmc.createVReplicationWorkflowRequests[tabletID] = req
}

func (tmc *testTMClient) expectCreateVReplicationWorkflowRequestOnTargetTablets(req *createVReplicationWorkflowRequestResponse) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	for _, tablet := range tmc.env.tablets[tmc.env.targetKeyspace.KeyspaceName] {
		tmc.createVReplicationWorkflowRequests[tablet.Alias.Uid] = req
	}
}

func (tmc *testTMClient) expectReadVReplicationWorkflowRequest(tabletID uint32, req *readVReplicationWorkflowRequestResponse) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	tmc.readVReplicationWorkflowRequests[tabletID] = req
}

func (tmc *testTMClient) expectReadVReplicationWorkflowRequestOnTargetTablets(req *readVReplicationWorkflowRequestResponse) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	for _, tablet := range tmc.env.tablets[tmc.env.targetKeyspace.KeyspaceName] {
		tmc.readVReplicationWorkflowRequests[tablet.Alias.Uid] = req
	}
}

func (tmc *testTMClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	qrs := tmc.vrQueries[int(tablet.Alias.Uid)]
	if len(qrs) == 0 {
		return nil, fmt.Errorf("tablet %v does not expect any more queries: %q", tablet, query)
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
	return qrs[0].result, qrs[0].err
}

func (tmc *testTMClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	// Reuse VReplicationExec.
	return tmc.VReplicationExec(ctx, tablet, string(req.Query))
}

func (tmc *testTMClient) ExecuteFetchAsAllPrivs(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (*querypb.QueryResult, error) {
	return nil, nil
}

func (tmc *testTMClient) ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (*querypb.QueryResult, error) {
	// Reuse VReplicationExec.
	return tmc.VReplicationExec(ctx, tablet, string(req.Query))
}

func (tmc *testTMClient) expectApplySchemaRequest(tabletID uint32, req *applySchemaRequestResponse) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if tmc.applySchemaRequests == nil {
		tmc.applySchemaRequests = make(map[uint32][]*applySchemaRequestResponse)
	}

	tmc.applySchemaRequests[tabletID] = append(tmc.applySchemaRequests[tabletID], req)
}

func (tmc *testTMClient) expectValidateVReplicationPermissionsResponse(tabletID uint32, req *validateVReplicationPermissionsResponse) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if tmc.validateVReplicationPermissionsResponses == nil {
		tmc.validateVReplicationPermissionsResponses = make(map[uint32]*validateVReplicationPermissionsResponse)
	}

	tmc.validateVReplicationPermissionsResponses[tabletID] = req
}

// Note: ONLY breaks up change.SQL into individual statements and executes it. Does NOT fully implement ApplySchema.
func (tmc *testTMClient) ApplySchema(ctx context.Context, tablet *topodatapb.Tablet, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if requests, ok := tmc.applySchemaRequests[tablet.Alias.Uid]; ok {
		if len(requests) == 0 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected ApplySchema request on tablet %s: got %+v",
				topoproto.TabletAliasString(tablet.Alias), change)
		}
		expect := requests[0]
		if expect.matchSqlOnly {
			matched := false
			if expect.change.SQL[0] == '/' {
				matched = regexp.MustCompile("(?i)" + expect.change.SQL[1:]).MatchString(change.SQL)
			} else {
				matched = strings.EqualFold(change.SQL, expect.change.SQL)
			}
			if !matched {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected ApplySchema request on tablet %s: got %+v, want %+v",
					topoproto.TabletAliasString(tablet.Alias), change, expect.change)
			}
		} else if !reflect.DeepEqual(change, expect.change) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected ApplySchema request on tablet %s: got %+v, want %+v",
				topoproto.TabletAliasString(tablet.Alias), change, expect.change)
		}
		tmc.applySchemaRequests[tablet.Alias.Uid] = tmc.applySchemaRequests[tablet.Alias.Uid][1:]
		return expect.res, expect.err
	}

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

type vdiffRequestResponse struct {
	req *tabletmanagerdatapb.VDiffRequest
	res *tabletmanagerdatapb.VDiffResponse
	err error
}

type createVReplicationWorkflowRequestResponse struct {
	req *tabletmanagerdatapb.CreateVReplicationWorkflowRequest
	res *tabletmanagerdatapb.CreateVReplicationWorkflowResponse
	err error
}

type readVReplicationWorkflowRequestResponse struct {
	req *tabletmanagerdatapb.ReadVReplicationWorkflowRequest
	res *tabletmanagerdatapb.ReadVReplicationWorkflowResponse
	err error
}

type applySchemaRequestResponse struct {
	matchSqlOnly bool
	change       *tmutils.SchemaChange
	res          *tabletmanagerdatapb.SchemaChangeResult
	err          error
}

type updateVReplicationWorkflowRequestResponse struct {
	req *tabletmanagerdatapb.UpdateVReplicationWorkflowRequest
	res *tabletmanagerdatapb.UpdateVReplicationWorkflowResponse
	err error
}

type getMaxValueForSequencesRequestResponse struct {
	req *tabletmanagerdatapb.GetMaxValueForSequencesRequest
	res *tabletmanagerdatapb.GetMaxValueForSequencesResponse
	err error
}

type validateVReplicationPermissionsResponse struct {
	res *tabletmanagerdatapb.ValidateVReplicationPermissionsResponse
	err error
}

func (tmc *testTMClient) expectVDiffRequest(tablet *topodatapb.Tablet, vrr *vdiffRequestResponse) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if tmc.vdiffRequests == nil {
		tmc.vdiffRequests = make(map[uint32]*vdiffRequestResponse)
	}
	tmc.vdiffRequests[tablet.Alias.Uid] = vrr
}

func (tmc *testTMClient) VDiff(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if vrr, ok := tmc.vdiffRequests[tablet.Alias.Uid]; ok {
		if !proto.Equal(vrr.req, req) {
			return nil, fmt.Errorf("unexpected VDiff request on tablet %s; got %+v, want %+v",
				topoproto.TabletAliasString(tablet.Alias), req, vrr.req)
		}
		delete(tmc.vdiffRequests, tablet.Alias.Uid)
		return vrr.res, vrr.err
	}
	if tmc.strict {
		return nil, fmt.Errorf("unexpected VDiff request on tablet %s: %+v",
			topoproto.TabletAliasString(tablet.Alias), req)
	}

	return &tabletmanagerdatapb.VDiffResponse{
		Id:        1,
		VdiffUuid: req.VdiffUuid,
		Output: &querypb.QueryResult{
			RowsAffected: 1,
		},
	}, nil
}

func (tmc *testTMClient) confirmVDiffRequests(t *testing.T) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	reqString := func([]*vdiffRequestResponse) string {
		str := strings.Builder{}
		for _, vrr := range tmc.vdiffRequests {
			str.WriteString(fmt.Sprintf("\n%+v", vrr.req))
		}
		return str.String()
	}

	require.Len(t, tmc.vdiffRequests, 0, "expected VDiff requests not made: %s", reqString(maps.Values(tmc.vdiffRequests)))
}

func (tmc *testTMClient) HasVReplicationWorkflows(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.HasVReplicationWorkflowsRequest) (*tabletmanagerdatapb.HasVReplicationWorkflowsResponse, error) {
	return &tabletmanagerdatapb.HasVReplicationWorkflowsResponse{
		Has: false,
	}, nil
}

func (tmc *testTMClient) ResetSequences(ctx context.Context, tablet *topodatapb.Tablet, tables []string) error {
	return nil
}

func (tmc *testTMClient) ReadVReplicationWorkflows(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ReadVReplicationWorkflowsRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	workflowKey := tmc.GetWorkflowKey(tablet.Keyspace, tablet.Shard)
	if resp := tmc.getVReplicationWorkflowsResponse(workflowKey); resp != nil {
		return resp, nil
	}
	workflowType := binlogdatapb.VReplicationWorkflowType_MoveTables
	if len(req.IncludeWorkflows) > 0 {
		for _, wf := range req.IncludeWorkflows {
			if strings.Contains(wf, "lookup") {
				workflowType = binlogdatapb.VReplicationWorkflowType_CreateLookupIndex
			}
		}
		ks := tmc.env.sourceKeyspace
		if tmc.reverse.Load() {
			ks = tmc.env.targetKeyspace
		}
		return &tabletmanagerdatapb.ReadVReplicationWorkflowsResponse{
			Workflows: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
				{
					Workflow:     req.IncludeWorkflows[0],
					WorkflowType: workflowType,
					Streams: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
						{
							Id:    1,
							State: binlogdatapb.VReplicationWorkflowState_Running,
							Bls: &binlogdatapb.BinlogSource{
								Keyspace: ks.KeyspaceName,
								Shard:    ks.ShardNames[0],
								Filter: &binlogdatapb.Filter{
									Rules: []*binlogdatapb.Rule{
										{
											Match: "/.*/",
										},
									},
								},
							},
							Pos:           position,
							TimeUpdated:   protoutil.TimeToProto(time.Now()),
							TimeHeartbeat: protoutil.TimeToProto(time.Now()),
						},
					},
				},
			},
		}, nil
	} else {
		return &tabletmanagerdatapb.ReadVReplicationWorkflowsResponse{}, nil
	}
}

func (tmc *testTMClient) UpdateVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.UpdateVReplicationWorkflowRequest) (*tabletmanagerdatapb.UpdateVReplicationWorkflowResponse, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	if requests := tmc.updateVReplicationWorklowRequests[tablet.Alias.Uid]; len(requests) > 0 {
		expect := requests[0]
		if !proto.Equal(expect.req, req) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected UpdateVReplicationWorkflow request on tablet %s: got %+v, want %+v",
				topoproto.TabletAliasString(tablet.Alias), req, expect.req)
		}
		tmc.updateVReplicationWorklowRequests[tablet.Alias.Uid] = tmc.updateVReplicationWorklowRequests[tablet.Alias.Uid][1:]
		return expect.res, expect.err
	}
	return &tabletmanagerdatapb.UpdateVReplicationWorkflowResponse{
		Result: &querypb.QueryResult{
			RowsAffected: 1,
		},
	}, nil
}

func (tmc *testTMClient) UpdateVReplicationWorkflows(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest) (*tabletmanagerdatapb.UpdateVReplicationWorkflowsResponse, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	if expect := tmc.updateVReplicationWorklowsRequests[tablet.Alias.Uid]; expect != nil {
		if !proto.Equal(expect, req) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected UpdateVReplicationWorkflows request on tablet %s: got %+v, want %+v",
				topoproto.TabletAliasString(tablet.Alias), req, expect)
		}
	}
	delete(tmc.updateVReplicationWorklowsRequests, tablet.Alias.Uid)
	return nil, nil
}

func (tmc *testTMClient) ValidateVReplicationPermissions(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ValidateVReplicationPermissionsRequest) (*tabletmanagerdatapb.ValidateVReplicationPermissionsResponse, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	if resp, ok := tmc.validateVReplicationPermissionsResponses[tablet.Alias.Uid]; ok {
		return resp.res, resp.err
	}
	return &tabletmanagerdatapb.ValidateVReplicationPermissionsResponse{
		User: "vt_filtered",
		Ok:   true,
	}, nil
}

func (tmc *testTMClient) setPrimaryPosition(tablet *topodatapb.Tablet, position string) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	if tmc.primaryPositions == nil {
		tmc.primaryPositions = make(map[uint32]string)
	}
	tmc.primaryPositions[tablet.Alias.Uid] = position
}

func (tmc *testTMClient) PrimaryPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	if tmc.primaryPositions != nil && tmc.primaryPositions[tablet.Alias.Uid] != "" {
		return tmc.primaryPositions[tablet.Alias.Uid], nil
	}
	return position, nil
}

func (tmc *testTMClient) WaitForPosition(ctx context.Context, tablet *topodatapb.Tablet, pos string) error {
	return nil
}

func (tmc *testTMClient) VReplicationWaitForPos(ctx context.Context, tablet *topodatapb.Tablet, id int32, pos string) error {
	return nil
}

func (tmc *testTMClient) SetRefreshStateError(tablet *topodatapb.Tablet, err error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if tmc.refreshStateErrors == nil {
		tmc.refreshStateErrors = make(map[uint32]error)
	}
	tmc.refreshStateErrors[tablet.Alias.Uid] = err
}

func (tmc *testTMClient) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if tmc.refreshStateErrors == nil {
		tmc.refreshStateErrors = make(map[uint32]error)
	}
	return tmc.refreshStateErrors[tablet.Alias.Uid]
}

func (tmc *testTMClient) AddVReplicationWorkflowsResponse(key string, resp *tabletmanagerdatapb.ReadVReplicationWorkflowsResponse) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	tmc.readVReplicationWorkflowsResponses[key] = append(tmc.readVReplicationWorkflowsResponses[key], resp)
}

func (tmc *testTMClient) AddUpdateVReplicationRequests(tabletUID uint32, req *tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	tmc.updateVReplicationWorklowsRequests[tabletUID] = req
}

func (tmc *testTMClient) AddUpdateVReplicationWorkflowRequestResponse(tabletUID uint32, reqres *updateVReplicationWorkflowRequestResponse) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	tmc.updateVReplicationWorklowRequests[tabletUID] = append(tmc.updateVReplicationWorklowRequests[tabletUID], reqres)
}

func (tmc *testTMClient) getVReplicationWorkflowsResponse(key string) *tabletmanagerdatapb.ReadVReplicationWorkflowsResponse {
	if len(tmc.readVReplicationWorkflowsResponses) == 0 {
		return nil
	}
	responses, ok := tmc.readVReplicationWorkflowsResponses[key]
	if !ok || len(responses) == 0 {
		return nil
	}
	resp := tmc.readVReplicationWorkflowsResponses[key][0]
	tmc.readVReplicationWorkflowsResponses[key] = tmc.readVReplicationWorkflowsResponses[key][1:]
	return resp
}

func (tmc *testTMClient) ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet, waitPosition string) error {
	return nil
}

func (tmc *testTMClient) UpdateSequenceTables(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.UpdateSequenceTablesRequest) (*tabletmanagerdatapb.UpdateSequenceTablesResponse, error) {
	expect := tmc.updateSequenceTablesRequests[tablet.Alias.Uid]
	if expect == nil {
		return nil, nil
	}
	slices.SortFunc(expect.Sequences, func(x, y *tabletmanagerdatapb.UpdateSequenceTablesRequest_SequenceMetadata) int {
		return cmp.Compare(x.BackingTableName, y.BackingTableName)
	})
	slices.SortFunc(req.Sequences, func(x, y *tabletmanagerdatapb.UpdateSequenceTablesRequest_SequenceMetadata) int {
		return cmp.Compare(x.BackingTableName, y.BackingTableName)
	})
	if !proto.Equal(expect, req) {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected UpdateSequenceTables request on tablet %s: got %+v, want %+v",
			topoproto.TabletAliasString(tablet.Alias), req, expect)
	}
	delete(tmc.updateSequenceTablesRequests, tablet.Alias.Uid)
	return &tabletmanagerdatapb.UpdateSequenceTablesResponse{}, nil
}

func (tmc *testTMClient) GetMaxValueForSequences(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.GetMaxValueForSequencesRequest) (*tabletmanagerdatapb.GetMaxValueForSequencesResponse, error) {
	expect := tmc.getMaxValueForSequencesRequests[tablet.Alias.Uid]
	if expect == nil {
		return &tabletmanagerdatapb.GetMaxValueForSequencesResponse{
			MaxValuesBySequenceTable: map[string]int64{},
		}, nil
	}
	slices.SortFunc(expect.req.Sequences, func(x, y *tabletmanagerdatapb.GetMaxValueForSequencesRequest_SequenceMetadata) int {
		return cmp.Compare(x.BackingTableName, y.BackingTableName)
	})
	slices.SortFunc(req.Sequences, func(x, y *tabletmanagerdatapb.GetMaxValueForSequencesRequest_SequenceMetadata) int {
		return cmp.Compare(x.BackingTableName, y.BackingTableName)
	})
	if !proto.Equal(expect.req, req) {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected GetMaxValueForSequences request on tablet %s: got %+v, want %+v",
			topoproto.TabletAliasString(tablet.Alias), req, expect.req)
	}
	delete(tmc.getMaxValueForSequencesRequests, tablet.Alias.Uid)
	return expect.res, expect.err
}

//
// Utility / helper functions.
//

func checkRouting(t *testing.T, ws *Server, want map[string][]string) {
	t.Helper()
	ctx := context.Background()
	got, err := topotools.GetRoutingRules(ctx, ws.ts)
	require.NoError(t, err)
	require.EqualValues(t, got, want, "routing rules don't match: got: %v, want: %v", got, want)
	cells, err := ws.ts.GetCellInfoNames(ctx)
	require.NoError(t, err)
	for _, cell := range cells {
		checkCellRouting(t, ws, cell, want)
	}
}

func checkCellRouting(t *testing.T, ws *Server, cell string, want map[string][]string) {
	t.Helper()
	ctx := context.Background()
	svs, err := ws.ts.GetSrvVSchema(ctx, cell)
	require.NoError(t, err)
	got := make(map[string][]string, len(svs.RoutingRules.Rules))
	for _, rr := range svs.RoutingRules.Rules {
		got[rr.FromTable] = append(got[rr.FromTable], rr.ToTables...)
	}
	require.EqualValues(t, got, want, "routing rules don't match for cell %s: got: %v, want: %v", cell, got, want)
}

func checkDenyList(t *testing.T, ts *topo.Server, keyspace, shard string, want []string) {
	t.Helper()
	ctx := context.Background()
	si, err := ts.GetShard(ctx, keyspace, shard)
	require.NoError(t, err)
	tc := si.GetTabletControl(topodatapb.TabletType_PRIMARY)
	var got []string
	if tc != nil {
		got = tc.DeniedTables
	}
	require.EqualValues(t, got, want, "denied tables for %s/%s: got: %v, want: %v", keyspace, shard, got, want)
}

func checkServedTypes(t *testing.T, ts *topo.Server, keyspace, shard string, want int) {
	t.Helper()
	ctx := context.Background()
	si, err := ts.GetShard(ctx, keyspace, shard)
	require.NoError(t, err)
	servedTypes, err := ts.GetShardServingTypes(ctx, si)
	require.NoError(t, err)
	require.Equal(t, want, len(servedTypes), "shard %s/%s has wrong served types: got: %v, want: %v",
		keyspace, shard, len(servedTypes), want)
}

func checkCellServedTypes(t *testing.T, ts *topo.Server, keyspace, shard, cell string, want int) {
	t.Helper()
	ctx := context.Background()
	srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
	require.NoError(t, err)
	count := 0
outer:
	for _, partition := range srvKeyspace.GetPartitions() {
		for _, ref := range partition.ShardReferences {
			if ref.Name == shard {
				count++
				continue outer
			}
		}
	}
	require.Equal(t, want, count, "serving types for %s/%s in cell %s: got: %d, want: %d", keyspace, shard, cell, count, want)
}

func checkIfPrimaryServing(t *testing.T, ts *topo.Server, keyspace, shard string, want bool) {
	t.Helper()
	ctx := context.Background()
	si, err := ts.GetShard(ctx, keyspace, shard)
	require.NoError(t, err)
	require.Equal(t, want, si.IsPrimaryServing, "primary serving for %s/%s: got: %v, want: %v", keyspace, shard, si.IsPrimaryServing, want)
}

func checkIfTableExistInVSchema(ctx context.Context, t *testing.T, ts *topo.Server, keyspace, table string) bool {
	vschema, err := ts.GetVSchema(ctx, keyspace)
	require.NoError(t, err)
	require.NotNil(t, vschema)
	_, ok := vschema.Tables[table]
	return ok
}
