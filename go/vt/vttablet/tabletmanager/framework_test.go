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

package tabletmanager

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclienttest"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	gtidFlavor   = "MySQL56"
	gtidPosition = "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-220"
)

func init() {
	tabletconn.RegisterDialer("grpc", func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		return &tabletconntest.FakeQueryService{
			StreamHealthResponse: &querypb.StreamHealthResponse{
				Serving: true,
				Target: &querypb.Target{
					Keyspace:   tablet.Keyspace,
					Shard:      tablet.Shard,
					TabletType: tablet.Type,
					Cell:       tablet.Alias.Cell,
				},
				RealtimeStats: &querypb.RealtimeStats{},
			},
		}, nil
	})
}

type testEnv struct {
	mu        sync.Mutex
	ctx       context.Context
	ts        *topo.Server
	cells     []string
	mysqld    *mysqlctl.FakeMysqlDaemon
	tmc       *fakeTMClient
	dbName    string
	protoName string
}

func newTestEnv(t *testing.T, ctx context.Context, sourceKeyspace string, sourceShards []string) *testEnv {
	tenv := &testEnv{
		ctx:       context.Background(),
		tmc:       newFakeTMClient(),
		cells:     []string{"zone1"},
		dbName:    "tmtestdb",
		protoName: t.Name(),
	}
	tenv.mu.Lock()
	defer tenv.mu.Unlock()
	tenv.ts = memorytopo.NewServer(ctx, tenv.cells...)
	tenv.tmc.sourceKeyspace = sourceKeyspace
	tenv.tmc.sourceShards = sourceShards
	tenv.tmc.schema = defaultSchema

	tabletconn.RegisterDialer(t.Name(), func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		tenv.mu.Lock()
		defer tenv.mu.Unlock()
		if qs, ok := tenv.tmc.tablets[int(tablet.Alias.Uid)]; ok {
			return qs, nil
		}
		return nil, fmt.Errorf("tablet %d not found", tablet.Alias.Uid)
	})
	tabletconntest.SetProtocol(fmt.Sprintf("go.vt.vttablet.tabletmanager.framework_test_%s", t.Name()), tenv.protoName)
	tmclient.RegisterTabletManagerClientFactory(t.Name(), func() tmclient.TabletManagerClient {
		return tenv.tmc
	})
	tmclienttest.SetProtocol(fmt.Sprintf("go.vt.vttablet.tabletmanager.framework_test_%s", t.Name()), tenv.protoName)

	tenv.mysqld = mysqlctl.NewFakeMysqlDaemon(fakesqldb.New(t))
	var err error
	tenv.mysqld.CurrentPrimaryPosition, err = replication.ParsePosition(gtidFlavor, gtidPosition)
	require.NoError(t, err)

	return tenv
}

func (tenv *testEnv) close() {
	tenv.mu.Lock()
	defer tenv.mu.Unlock()
	tenv.ts.Close()
	tenv.mysqld.Close()
}

//--------------------------------------
// Tablets

func (tenv *testEnv) addTablet(t *testing.T, id int, keyspace, shard string) *fakeTabletConn {
	tenv.mu.Lock()
	defer tenv.mu.Unlock()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: tenv.cells[0],
			Uid:  uint32(id),
		},
		Keyspace: keyspace,
		Shard:    shard,
		Type:     topodatapb.TabletType_PRIMARY,
		PortMap: map[string]int32{
			tenv.protoName: int32(id),
		},
	}
	if err := tenv.ts.InitTablet(tenv.ctx, tablet, false /* allowPrimaryOverride */, true /* createShardAndKeyspace */, false /* allowUpdate */); err != nil {
		panic(err)
	}
	if _, err := tenv.ts.UpdateShardFields(tenv.ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = tablet.Alias
		si.IsPrimaryServing = true
		return nil
	}); err != nil {
		panic(err)
	}
	if err := tenv.ts.EnsureVSchema(tenv.ctx, keyspace); err != nil {
		panic(err)
	}

	vrdbClient := binlogplayer.NewMockDBClient(t)
	vrdbClient.Tag = fmt.Sprintf("tablet:%d", id)
	tenv.tmc.tablets[id] = &fakeTabletConn{
		tablet:     tablet,
		vrdbClient: vrdbClient,
	}

	dbClientFactory := func() binlogplayer.DBClient {
		return tenv.tmc.tablets[id].vrdbClient
	}
	tenv.tmc.tablets[id].vrengine = vreplication.NewTestEngine(tenv.ts, tenv.cells[0], tenv.mysqld, dbClientFactory, dbClientFactory, tenv.dbName, nil)
	tenv.tmc.tablets[id].vrdbClient.ExpectRequest(fmt.Sprintf("select * from _vt.vreplication where db_name='%s'", tenv.dbName), &sqltypes.Result{}, nil)
	tenv.tmc.tablets[id].vrengine.Open(tenv.ctx)
	require.True(t, tenv.tmc.tablets[id].vrengine.IsOpen(), "vreplication engine was not open")

	tenv.tmc.tablets[id].tm = &TabletManager{
		VREngine: tenv.tmc.tablets[id].vrengine,
		DBConfigs: &dbconfigs.DBConfigs{
			DBName: tenv.dbName,
		},
	}

	return tenv.tmc.tablets[id]
}

func (tenv *testEnv) deleteTablet(tablet *topodatapb.Tablet) {
	tenv.mu.Lock()
	defer tenv.mu.Unlock()
	tenv.tmc.tablets[int(tablet.Alias.Uid)].vrdbClient.Close()
	tenv.tmc.tablets[int(tablet.Alias.Uid)].vrengine.Close()
	tenv.ts.DeleteTablet(tenv.ctx, tablet.Alias)
	// This is not automatically removed from shard replication, which results in log spam.
	topo.DeleteTabletReplicationData(tenv.ctx, tenv.ts, tablet)
}

// fakeTabletConn implements the TabletConn and QueryService interfaces.
type fakeTabletConn struct {
	queryservice.QueryService
	tablet     *topodatapb.Tablet
	tm         *TabletManager
	vrdbClient *binlogplayer.MockDBClient
	vrengine   *vreplication.Engine
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (queryservice.TransactionState, error) {
	return queryservice.TransactionState{
		TransactionID: 1,
	}, nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	return 0, nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	return 0, nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	return nil, nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return nil, nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) BeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (queryservice.TransactionState, *sqltypes.Result, error) {
	return queryservice.TransactionState{
		TransactionID: 1,
	}, nil, nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) BeginStreamExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.TransactionState, error) {
	return queryservice.TransactionState{
		TransactionID: 1,
	}, nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	return 0, nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) VStream(ctx context.Context, request *binlogdatapb.VStreamRequest, send func([]*binlogdatapb.VEvent) error) error {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) VStreamRows(ctx context.Context, request *binlogdatapb.VStreamRowsRequest, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) HandlePanic(err *error) {
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) ReserveBeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (queryservice.ReservedTransactionState, *sqltypes.Result, error) {
	return queryservice.ReservedTransactionState{
		ReservedID: 1,
	}, nil, nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) ReserveBeginStreamExecute(ctx context.Context, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.ReservedTransactionState, error) {
	return queryservice.ReservedTransactionState{
		ReservedID: 1,
	}, nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) ReserveExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (queryservice.ReservedState, *sqltypes.Result, error) {
	return queryservice.ReservedState{
		ReservedID: 1,
	}, nil, nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) ReserveStreamExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (queryservice.ReservedState, error) {
	return queryservice.ReservedState{
		ReservedID: 1,
	}, nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) GetSchema(ctx context.Context, target *querypb.Target, tableType querypb.SchemaTableType, tableNames []string, callback func(schemaRes *querypb.GetSchemaResponse) error) error {
	return nil
}

// fakeTabletConn implements the QueryService interface.
func (ftc *fakeTabletConn) Close(ctx context.Context) error {
	return nil
}

func (ftc *fakeTabletConn) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	return callback(&querypb.StreamHealthResponse{
		Serving: true,
		Target: &querypb.Target{
			Keyspace:   ftc.tablet.Keyspace,
			Shard:      ftc.tablet.Shard,
			TabletType: ftc.tablet.Type,
			Cell:       ftc.tablet.Alias.Cell,
		},
		RealtimeStats: &querypb.RealtimeStats{},
	})
}

//----------------------------------------------
// fakeTMClient

type fakeTMClient struct {
	tmclient.TabletManagerClient
	sourceKeyspace string
	sourceShards   []string
	tablets        map[int]*fakeTabletConn
	schema         *tabletmanagerdatapb.SchemaDefinition
	tabletSchemas  map[int]*tabletmanagerdatapb.SchemaDefinition
	vreQueries     map[int]map[string]*querypb.QueryResult

	mu sync.Mutex
	// Keep track of how many times GetSchema is called per tablet.
	getSchemaCounts map[string]int
	// Used to confirm the number of times WorkflowDelete was called.
	workflowDeleteCalls int
}

func newFakeTMClient() *fakeTMClient {
	return &fakeTMClient{
		tablets:         make(map[int]*fakeTabletConn),
		vreQueries:      make(map[int]map[string]*querypb.QueryResult),
		schema:          &tabletmanagerdatapb.SchemaDefinition{},
		tabletSchemas:   make(map[int]*tabletmanagerdatapb.SchemaDefinition), // If we need to override the global schema for a tablet
		getSchemaCounts: make(map[string]int),
	}
}

// Note: ONLY breaks up change.SQL into individual statements and executes it. Does NOT fully implement ApplySchema.
func (tmc *fakeTMClient) ApplySchema(ctx context.Context, tablet *topodatapb.Tablet, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
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

func (tmc *fakeTMClient) schemaRequested(uid int) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	key := strconv.Itoa(int(uid))
	n, ok := tmc.getSchemaCounts[key]
	if !ok {
		tmc.getSchemaCounts[key] = 1
	} else {
		tmc.getSchemaCounts[key] = n + 1
	}
}

func (tmc *fakeTMClient) getSchemaRequestCount(uid int) int {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	key := strconv.Itoa(int(uid))
	return tmc.getSchemaCounts[key]
}

func (tmc *fakeTMClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	tmc.schemaRequested(int(tablet.Alias.Uid))
	// Return the schema for the tablet if it exists.
	if schema, ok := tmc.tabletSchemas[int(tablet.Alias.Uid)]; ok {
		return schema, nil
	}
	// Otherwise use the global one.
	return tmc.schema, nil
}

func (tmc *fakeTMClient) SetSchema(schema *tabletmanagerdatapb.SchemaDefinition) {
	tmc.schema = schema
}

func (tmc *fakeTMClient) ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (*querypb.QueryResult, error) {
	// Reuse VReplicationExec
	return tmc.VReplicationExec(ctx, tablet, string(req.Query))
}

func (tmc *fakeTMClient) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	// Reuse VReplicationExec
	return tmc.VReplicationExec(ctx, tablet, string(req.Query))
}

// setVReplicationExecResults allows you to specify VReplicationExec queries
// and their results. You can specify exact strings or strings prefixed with
// a '/', in which case they will be treated as a valid regexp.
func (tmc *fakeTMClient) setVReplicationExecResults(tablet *topodatapb.Tablet, query string, result *sqltypes.Result) {
	queries, ok := tmc.vreQueries[int(tablet.Alias.Uid)]
	if !ok {
		queries = make(map[string]*querypb.QueryResult)
		tmc.vreQueries[int(tablet.Alias.Uid)] = queries
	}
	queries[query] = sqltypes.ResultToProto3(result)
}

func (tmc *fakeTMClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	if result, ok := tmc.vreQueries[int(tablet.Alias.Uid)][query]; ok {
		return result, nil
	}
	for qry, res := range tmc.vreQueries[int(tablet.Alias.Uid)] {
		if strings.HasPrefix(qry, "/") {
			re := regexp.MustCompile(qry)
			if re.MatchString(qry) {
				return res, nil
			}
		}
	}
	return nil, fmt.Errorf("query %q not found for tablet %d", query, tablet.Alias.Uid)
}

func (tmc *fakeTMClient) PrimaryPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	return fmt.Sprintf("%s/%s", gtidFlavor, gtidPosition), nil
}

func (tmc *fakeTMClient) VReplicationWaitForPos(ctx context.Context, tablet *topodatapb.Tablet, id int32, pos string) error {
	return nil
}

func (tmc *fakeTMClient) ExecuteFetchAsAllPrivs(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest) (*querypb.QueryResult, error) {
	return &querypb.QueryResult{
		RowsAffected: 1,
	}, nil
}

func (tmc *fakeTMClient) VDiff(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.VDiffRequest) (*tabletmanagerdatapb.VDiffResponse, error) {
	return &tabletmanagerdatapb.VDiffResponse{
		Id:        1,
		VdiffUuid: req.VdiffUuid,
		Output: &querypb.QueryResult{
			RowsAffected: 1,
		},
	}, nil
}

func (tmc *fakeTMClient) CreateVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.CreateVReplicationWorkflowRequest) (*tabletmanagerdatapb.CreateVReplicationWorkflowResponse, error) {
	return tmc.tablets[int(tablet.Alias.Uid)].tm.CreateVReplicationWorkflow(ctx, req)
}

func (tmc *fakeTMClient) DeleteVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.DeleteVReplicationWorkflowRequest) (response *tabletmanagerdatapb.DeleteVReplicationWorkflowResponse, err error) {
	tmc.mu.Lock()
	defer tmc.mu.Unlock()
	tmc.workflowDeleteCalls++
	return &tabletmanagerdatapb.DeleteVReplicationWorkflowResponse{
		Result: &querypb.QueryResult{
			RowsAffected: 1,
		},
	}, nil
}

func (tmc *fakeTMClient) HasVReplicationWorkflows(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.HasVReplicationWorkflowsRequest) (*tabletmanagerdatapb.HasVReplicationWorkflowsResponse, error) {
	return tmc.tablets[int(tablet.Alias.Uid)].tm.HasVReplicationWorkflows(ctx, req)
}

func (tmc *fakeTMClient) ReadVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
	return tmc.tablets[int(tablet.Alias.Uid)].tm.ReadVReplicationWorkflow(ctx, req)
}

func (tmc *fakeTMClient) ReadVReplicationWorkflows(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ReadVReplicationWorkflowsRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowsResponse, error) {
	return tmc.tablets[int(tablet.Alias.Uid)].tm.ReadVReplicationWorkflows(ctx, req)
}

func (tmc *fakeTMClient) UpdateVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.UpdateVReplicationWorkflowRequest) (*tabletmanagerdatapb.UpdateVReplicationWorkflowResponse, error) {
	return tmc.tablets[int(tablet.Alias.Uid)].tm.UpdateVReplicationWorkflow(ctx, req)
}
