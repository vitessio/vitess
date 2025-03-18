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

package workflow

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type testMaterializerEnv struct {
	ws       *Server
	ms       *vtctldatapb.MaterializeSettings
	sources  []string
	targets  []string
	tablets  map[int]*topodatapb.Tablet
	topoServ *topo.Server
	cell     string
	tmc      *testMaterializerTMClient
	venv     *vtenv.Environment
}

// ----------------------------------------------
// testMaterializerEnv

func newTestMaterializerEnv(t *testing.T, ctx context.Context, ms *vtctldatapb.MaterializeSettings, sourceShards, targetShards []string) *testMaterializerEnv {
	t.Helper()

	tmc := newTestMaterializerTMClient(ms.SourceKeyspace, sourceShards, ms.TableSettings)
	topoServ := memorytopo.NewServer(ctx, "cell")
	venv := vtenv.NewTestEnv()
	env := &testMaterializerEnv{
		ms:       ms,
		sources:  sourceShards,
		targets:  targetShards,
		tablets:  make(map[int]*topodatapb.Tablet),
		topoServ: topoServ,
		cell:     "cell",
		tmc:      tmc,
		ws:       NewServer(venv, topoServ, tmc),
		venv:     venv,
	}

	require.NoError(t, topoServ.CreateKeyspace(ctx, ms.SourceKeyspace, &topodatapb.Keyspace{}))
	require.NoError(t, topoServ.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{Name: ms.SourceKeyspace, Keyspace: &vschemapb.Keyspace{}}))
	if ms.SourceKeyspace != ms.TargetKeyspace {
		require.NoError(t, topoServ.CreateKeyspace(ctx, ms.TargetKeyspace, &topodatapb.Keyspace{}))
		require.NoError(t, topoServ.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{Name: ms.TargetKeyspace, Keyspace: &vschemapb.Keyspace{}}))
	}
	logger := logutil.NewConsoleLogger()
	require.NoError(t, topoServ.RebuildSrvVSchema(ctx, []string{"cell"}))

	tabletID := startingSourceTabletUID
	sourceShardsMap := make(map[string]any)
	for _, shard := range sourceShards {
		sourceShardsMap[shard] = nil
		require.NoError(t, topoServ.CreateShard(ctx, ms.SourceKeyspace, shard))
		_ = env.addTablet(t, tabletID, env.ms.SourceKeyspace, shard, topodatapb.TabletType_PRIMARY)
		tabletID += tabletUIDStep
	}

	require.NoError(t, topotools.RebuildKeyspace(ctx, logger, topoServ, ms.SourceKeyspace, []string{"cell"}, false))

	tabletID = startingTargetTabletUID
	for _, shard := range targetShards {
		if ms.SourceKeyspace == ms.TargetKeyspace {
			if _, ok := sourceShardsMap[shard]; ok {
				continue
			}
		}
		require.NoError(t, topoServ.CreateShard(ctx, ms.TargetKeyspace, shard))
		_ = env.addTablet(t, tabletID, env.ms.TargetKeyspace, shard, topodatapb.TabletType_PRIMARY)
		tabletID += tabletUIDStep
	}

	for _, ts := range ms.TableSettings {
		tableName := ts.TargetTable
		table, err := venv.Parser().TableFromStatement(ts.SourceExpression)
		if err == nil {
			tableName = sqlparser.String(table.Name)
		}
		var (
			cols   []string
			fields []*querypb.Field
		)
		if ts.CreateDdl != "" {
			stmt, err := env.venv.Parser().ParseStrictDDL(ts.CreateDdl)
			require.NoError(t, err)
			ddl, ok := stmt.(*sqlparser.CreateTable)
			require.True(t, ok)
			cols = make([]string, len(ddl.TableSpec.Columns))
			fields = make([]*querypb.Field, len(ddl.TableSpec.Columns))
			for i, col := range ddl.TableSpec.Columns {
				cols[i] = col.Name.String()
				fields[i] = &querypb.Field{
					Name: col.Name.String(),
					Type: col.Type.SQLType(),
				}
			}
		}
		env.tmc.schema[ms.SourceKeyspace+"."+tableName] = &tabletmanagerdatapb.SchemaDefinition{
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
				Name:    tableName,
				Schema:  ts.CreateDdl,
				Columns: cols,
				Fields:  fields,
			}},
		}
		env.tmc.schema[ms.TargetKeyspace+"."+ts.TargetTable] = &tabletmanagerdatapb.SchemaDefinition{
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
				Name:    ts.TargetTable,
				Schema:  ts.CreateDdl,
				Columns: cols,
				Fields:  fields,
			}},
		}
	}

	if ms.SourceKeyspace != ms.TargetKeyspace {
		require.NoError(t, topotools.RebuildKeyspace(ctx, logger, topoServ, ms.TargetKeyspace, []string{"cell"}, false))
	}

	return env
}

func (env *testMaterializerEnv) close() {
	for _, t := range env.tablets {
		env.deleteTablet(t)
	}
}

func (env *testMaterializerEnv) addTablet(t *testing.T, id int, keyspace, shard string, tabletType topodatapb.TabletType) *topodatapb.Tablet {
	keyRanges, err := key.ParseShardingSpec(shard)
	require.NoError(t, err)
	require.Len(t, keyRanges, 1)
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: env.cell,
			Uid:  uint32(id),
		},
		Keyspace: keyspace,
		Shard:    shard,
		KeyRange: keyRanges[0],
		Type:     tabletType,
		PortMap: map[string]int32{
			"test": int32(id),
		},
	}
	env.tablets[id] = tablet
	if err := env.ws.ts.InitTablet(context.Background(), tablet, false /* allowPrimaryOverride */, true /* createShardAndKeyspace */, false /* allowUpdate */); err != nil {
		panic(err)
	}
	if tabletType == topodatapb.TabletType_PRIMARY {
		_, err := env.ws.ts.UpdateShardFields(context.Background(), keyspace, shard, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = tablet.Alias
			si.IsPrimaryServing = true
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	return tablet
}

func (env *testMaterializerEnv) deleteTablet(tablet *topodatapb.Tablet) {
	_ = env.topoServ.DeleteTablet(context.Background(), tablet.Alias)
	delete(env.tablets, int(tablet.Alias.Uid))
}

// ----------------------------------------------
// testMaterializerTMClient
type readVReplicationWorkflowFunc = func(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error)

type testMaterializerTMClient struct {
	tmclient.TabletManagerClient
	keyspace      string
	schema        map[string]*tabletmanagerdatapb.SchemaDefinition
	sourceShards  []string
	tableSettings []*vtctldatapb.TableMaterializeSettings

	mu                                 sync.Mutex
	vrQueries                          map[int][]*queryResult
	fetchAsAllPrivsQueries             map[int]map[string]*queryResult
	createVReplicationWorkflowRequests map[uint32]*createVReplicationWorkflowRequestResponse

	// Used to confirm the number of times WorkflowDelete was called.
	workflowDeleteCalls int

	// Used to override the response to ReadVReplicationWorkflow.
	readVReplicationWorkflow readVReplicationWorkflowFunc

	// Responses to GetSchema RPCs for individual tablets.
	getSchemaResponses map[uint32]*tabletmanagerdatapb.SchemaDefinition
}

func newTestMaterializerTMClient(keyspace string, sourceShards []string, tableSettings []*vtctldatapb.TableMaterializeSettings) *testMaterializerTMClient {
	return &testMaterializerTMClient{
		keyspace:                           keyspace,
		schema:                             make(map[string]*tabletmanagerdatapb.SchemaDefinition),
		sourceShards:                       sourceShards,
		tableSettings:                      tableSettings,
		vrQueries:                          make(map[int][]*queryResult),
		fetchAsAllPrivsQueries:             make(map[int]map[string]*queryResult),
		createVReplicationWorkflowRequests: make(map[uint32]*createVReplicationWorkflowRequestResponse),
		getSchemaResponses:                 make(map[uint32]*tabletmanagerdatapb.SchemaDefinition),
	}
}

func (tmc *testMaterializerTMClient) CreateVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.CreateVReplicationWorkflowRequest) (*tabletmanagerdatapb.CreateVReplicationWorkflowResponse, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	if expect := tmc.createVReplicationWorkflowRequests[tablet.Alias.Uid]; expect != nil {
		if expect.req != nil && !proto.Equal(expect.req, request) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected CreateVReplicationWorkflow request on tablet %s: got %+v, want %+v",
				topoproto.TabletAliasString(tablet.Alias), request, expect)
		}
		if expect.res != nil {
			return expect.res, expect.err
		}
	}
	res := sqltypes.MakeTestResult(sqltypes.MakeTestFields("rowsaffected", "int64"), "1")
	return &tabletmanagerdatapb.CreateVReplicationWorkflowResponse{Result: sqltypes.ResultToProto3(res)}, nil
}

func (tmc *testMaterializerTMClient) ReadVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if tmc.readVReplicationWorkflow != nil {
		return tmc.readVReplicationWorkflow(ctx, tablet, request)
	}

	workflowType := binlogdatapb.VReplicationWorkflowType_MoveTables
	if strings.Contains(request.Workflow, "lookup") {
		workflowType = binlogdatapb.VReplicationWorkflowType_CreateLookupIndex
	}

	rules := make([]*binlogdatapb.Rule, len(tmc.tableSettings))
	if len(rules) == 0 {
		rules = append(rules, &binlogdatapb.Rule{Match: "table1"})
	} else {
		for i, tableSetting := range tmc.tableSettings {
			rules[i] = &binlogdatapb.Rule{
				Match:  tableSetting.TargetTable,
				Filter: tableSetting.SourceExpression,
			}
		}
	}

	streams := make([]*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream, len(tmc.sourceShards))
	for i, shard := range tmc.sourceShards {
		streams[i] = &tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
			Id: int32(i + 1),
			Bls: &binlogdatapb.BinlogSource{
				Keyspace: tmc.keyspace,
				Shard:    shard,
				Filter: &binlogdatapb.Filter{
					Rules: rules,
				},
			},
		}
	}
	return &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
		Workflow:     request.Workflow,
		WorkflowType: workflowType,
		Streams:      streams,
	}, nil
}

func (tmc *testMaterializerTMClient) DeleteVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.DeleteVReplicationWorkflowRequest) (response *tabletmanagerdatapb.DeleteVReplicationWorkflowResponse, err error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	tmc.workflowDeleteCalls++
	return &tabletmanagerdatapb.DeleteVReplicationWorkflowResponse{
		Result: &querypb.QueryResult{
			RowsAffected: 1,
		},
	}, nil
}

func (tmc *testMaterializerTMClient) SetGetSchemaResponse(tabletUID int, res *tabletmanagerdatapb.SchemaDefinition) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if tmc.getSchemaResponses == nil {
		tmc.getSchemaResponses = make(map[uint32]*tabletmanagerdatapb.SchemaDefinition)
	}
	tmc.getSchemaResponses[uint32(tabletUID)] = res
}

func (tmc *testMaterializerTMClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if tmc.getSchemaResponses != nil && tmc.getSchemaResponses[tablet.Alias.Uid] != nil {
		return tmc.getSchemaResponses[tablet.Alias.Uid], nil
	}

	schemaDefn := &tabletmanagerdatapb.SchemaDefinition{}
	for _, table := range request.Tables {
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

func (tmc *testMaterializerTMClient) expectVRQuery(tabletID int, query string, result *sqltypes.Result) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	tmc.vrQueries[tabletID] = append(tmc.vrQueries[tabletID], &queryResult{
		query:  query,
		result: sqltypes.ResultToProto3(result),
	})
}

func (tmc *testMaterializerTMClient) expectFetchAsAllPrivsQuery(tabletID int, query string, result *sqltypes.Result) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if tmc.fetchAsAllPrivsQueries[tabletID] == nil {
		tmc.fetchAsAllPrivsQueries[tabletID] = make(map[string]*queryResult)
	}

	tmc.fetchAsAllPrivsQueries[tabletID][query] = &queryResult{
		query:  query,
		result: sqltypes.ResultToProto3(result),
	}
}

func (tmc *testMaterializerTMClient) expectCreateVReplicationWorkflowRequest(tabletID uint32, req *createVReplicationWorkflowRequestResponse) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	tmc.createVReplicationWorkflowRequests[tabletID] = req
}

func (tmc *testMaterializerTMClient) verifyQueries(t *testing.T) {
	t.Helper()
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	for tabletID, qrs := range tmc.vrQueries {
		if len(qrs) != 0 {
			var list []string
			for _, qr := range qrs {
				list = append(list, qr.query)
			}
			t.Errorf("tablet %v: found queries that were expected but never got executed by the test: %v", tabletID, list)
		}
	}
}

func (tmc *testMaterializerTMClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
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
	return qrs[0].result, nil
}

func (tmc *testMaterializerTMClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	// Reuse VReplicationExec
	return tmc.VReplicationExec(ctx, tablet, string(req.Query))
}

func (tmc *testMaterializerTMClient) ExecuteFetchAsAllPrivs(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (*querypb.QueryResult, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	if resultsForTablet, ok := tmc.fetchAsAllPrivsQueries[int(tablet.Alias.Uid)]; ok {
		if result, ok := resultsForTablet[string(req.Query)]; ok {
			return result.result, result.err
		}
	}

	return nil, fmt.Errorf("%w: no ExecuteFetchAsAllPrivs result set for tablet %d", assert.AnError, int(tablet.Alias.Uid))
}

// Note: ONLY breaks up change.SQL into individual statements and executes it. Does NOT fully implement ApplySchema.
func (tmc *testMaterializerTMClient) ApplySchema(ctx context.Context, tablet *topodatapb.Tablet, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
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

func (tmc *testMaterializerTMClient) VDiff(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error) {
	return &tabletmanagerdatapb.VDiffResponse{
		Id:        1,
		VdiffUuid: req.VdiffUuid,
		Output: &querypb.QueryResult{
			RowsAffected: 1,
		},
	}, nil
}

func (tmc *testMaterializerTMClient) HasVReplicationWorkflows(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.HasVReplicationWorkflowsRequest) (*tabletmanagerdatapb.HasVReplicationWorkflowsResponse, error) {
	return &tabletmanagerdatapb.HasVReplicationWorkflowsResponse{
		Has: false,
	}, nil
}

func (tmc *testMaterializerTMClient) ReadVReplicationWorkflows(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ReadVReplicationWorkflowsRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse, error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	workflowType := binlogdatapb.VReplicationWorkflowType_MoveTables
	if len(req.IncludeWorkflows) > 0 {
		for _, wf := range req.IncludeWorkflows {
			if strings.Contains(wf, "lookup") {
				workflowType = binlogdatapb.VReplicationWorkflowType_CreateLookupIndex
			}
		}
		streams := make([]*tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream, len(tmc.sourceShards))
		for i, shard := range tmc.sourceShards {
			streams[i] = &tabletmanagerdatapb.ReadVReplicationWorkflowResponse_Stream{
				Id:    1,
				State: binlogdatapb.VReplicationWorkflowState_Running,
				Bls: &binlogdatapb.BinlogSource{
					Keyspace: tmc.keyspace,
					Shard:    shard,
					Filter: &binlogdatapb.Filter{
						Rules: []*binlogdatapb.Rule{
							{
								Match: ".*",
							},
						},
					},
				},
				Pos:           position,
				TimeUpdated:   protoutil.TimeToProto(time.Now()),
				TimeHeartbeat: protoutil.TimeToProto(time.Now()),
			}
		}
		return &tabletmanagerdatapb.ReadVReplicationWorkflowsResponse{
			Workflows: []*tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
				{
					Workflow:     req.IncludeWorkflows[0],
					WorkflowType: workflowType,
					Streams:      streams,
				},
			},
		}, nil
	} else {
		return &tabletmanagerdatapb.ReadVReplicationWorkflowsResponse{}, nil
	}
}

func (tmc *testMaterializerTMClient) UpdateVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.UpdateVReplicationWorkflowRequest) (*tabletmanagerdatapb.UpdateVReplicationWorkflowResponse, error) {
	return &tabletmanagerdatapb.UpdateVReplicationWorkflowResponse{
		Result: &querypb.QueryResult{
			RowsAffected: 1,
		},
	}, nil
}
