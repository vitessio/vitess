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
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
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
		ts:             memorytopo.NewServer(ctx, defaultCellName),
		sourceKeyspace: sourceKeyspace,
		targetKeyspace: targetKeyspace,
		tablets:        make(map[string]map[int]*topodatapb.Tablet),
		cell:           cell,
	}
	venv := vtenv.NewTestEnv()
	env.tmc = newTestTMClient(env)
	env.ws = NewServer(venv, env.ts, env.tmc)

	tabletID := startingSourceTabletUID
	for _, shardName := range sourceKeyspace.ShardNames {
		_ = env.addTablet(tabletID, sourceKeyspace.KeyspaceName, shardName, topodatapb.TabletType_PRIMARY)
		tabletID += tabletUIDStep
	}
	if sourceKeyspace.KeyspaceName != targetKeyspace.KeyspaceName {
		tabletID = startingTargetTabletUID
		for _, shardName := range targetKeyspace.ShardNames {
			_ = env.addTablet(tabletID, targetKeyspace.KeyspaceName, shardName, topodatapb.TabletType_PRIMARY)
			tabletID += tabletUIDStep
		}
	}
	err := env.ts.RebuildSrvVSchema(ctx, nil)
	require.NoError(t, err)

	return env
}

func (env *testEnv) close() {
	for _, k := range maps.Values(env.tablets) {
		for _, t := range maps.Values(k) {
			env.deleteTablet(t)
		}
	}
}

func (env *testEnv) addTablet(id int, keyspace, shard string, tabletType topodatapb.TabletType) *topodatapb.Tablet {
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

	// Used to confirm the number of times WorkflowDelete was called.
	workflowDeleteCalls int

	env *testEnv

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

func (tmc *testTMClient) CreateVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.CreateVReplicationWorkflowRequest) (*tabletmanagerdatapb.CreateVReplicationWorkflowResponse, error) {
	if expect := tmc.createVReplicationWorkflowRequests[tablet.Alias.Uid]; expect != nil {
		if !proto.Equal(expect, request) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected CreateVReplicationWorkflow request: got %+v, want %+v", request, expect)
		}
	}
	res := sqltypes.MakeTestResult(sqltypes.MakeTestFields("rowsaffected", "int64"), "1")
	return &tabletmanagerdatapb.CreateVReplicationWorkflowResponse{Result: sqltypes.ResultToProto3(res)}, nil
}

func (tmc *testTMClient) ReadVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
	if expect := tmc.readVReplicationWorkflowRequests[tablet.Alias.Uid]; expect != nil {
		if !proto.Equal(expect, request) {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected ReadVReplicationWorkflow request: got %+v, want %+v", request, expect)
		}
	}
	workflowType := binlogdatapb.VReplicationWorkflowType_MoveTables
	if strings.Contains(request.Workflow, "lookup") {
		workflowType = binlogdatapb.VReplicationWorkflowType_CreateLookupIndex
	}
	res := &tabletmanagerdatapb.ReadVReplicationWorkflowResponse{
		Workflow:     request.Workflow,
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

func (tmc *testTMClient) DeleteVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.DeleteVReplicationWorkflowRequest) (response *tabletmanagerdatapb.DeleteVReplicationWorkflowResponse, err error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	tmc.workflowDeleteCalls++
	return &tabletmanagerdatapb.DeleteVReplicationWorkflowResponse{
		Result: &querypb.QueryResult{
			RowsAffected: 1,
		},
	}, nil
}

func (tmc *testTMClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
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

func (tmc *testTMClient) verifyQueries(t *testing.T) {
	t.Helper()
	tmc.mu.Lock()
	defer tmc.mu.Unlock()

	for tabletID, qrs := range tmc.vrQueries {
		if len(qrs) != 0 {
			var list []string
			for _, qr := range qrs {
				list = append(list, qr.query)
			}
			require.Failf(t, "missing query", "tablet %v: found queries that were expected but never got executed by the test: %v", tabletID, list)
		}
	}
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
	// Reuse VReplicationExec
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

func (tmc *testTMClient) HasVReplicationWorkflows(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.HasVReplicationWorkflowsRequest) (*tabletmanagerdatapb.HasVReplicationWorkflowsResponse, error) {
	return &tabletmanagerdatapb.HasVReplicationWorkflowsResponse{
		Has: false,
	}, nil
}

func (tmc *testTMClient) ReadVReplicationWorkflows(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ReadVReplicationWorkflowsRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse, error) {
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
							Pos:           "MySQL56/" + position,
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
