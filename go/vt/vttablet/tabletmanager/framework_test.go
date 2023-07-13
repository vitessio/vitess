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
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclienttest"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type testEnv struct {
	mu         sync.Mutex
	ctx        context.Context
	vrengine   *vreplication.Engine
	vrdbClient *binlogplayer.MockDBClient
	ts         *topo.Server
	cells      []string
	tablets    map[int]*fakeTabletConn
	tmc        *fakeTMClient
	dbName     string
	protoName  string
}

func newTestEnv(t *testing.T) *testEnv {
	tenv := &testEnv{
		ctx:        context.Background(),
		vrdbClient: binlogplayer.NewMockDBClient(t),
		tablets:    make(map[int]*fakeTabletConn),
		tmc:        newFakeTMClient(),
		cells:      []string{"zone1"},
		dbName:     "tmtestdb",
		protoName:  t.Name(),
	}
	tenv.mu.Lock()
	defer tenv.mu.Unlock()
	tenv.ts = memorytopo.NewServer(tenv.cells...)

	tabletconn.RegisterDialer(t.Name(), func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		tenv.mu.Lock()
		defer tenv.mu.Unlock()
		if qs, ok := tenv.tablets[int(tablet.Alias.Uid)]; ok {
			return qs, nil
		}
		return nil, fmt.Errorf("tablet %d not found", tablet.Alias.Uid)
	})
	tabletconntest.SetProtocol(fmt.Sprintf("go.vt.vttablet.tabletmanager.framework_test_%s", t.Name()), tenv.protoName)
	tmclient.RegisterTabletManagerClientFactory(t.Name(), func() tmclient.TabletManagerClient {
		return tenv.tmc
	})
	tmclienttest.SetProtocol(fmt.Sprintf("go.vt.vttablet.tabletmanager.framework_test_%s", t.Name()), tenv.protoName)

	dbClientFactory := func() binlogplayer.DBClient {
		return tenv.vrdbClient
	}
	tenv.vrengine = vreplication.NewTestEngine(tenv.ts, tenv.cells[0], mysqlctl.NewFakeMysqlDaemon(fakesqldb.New(t)), dbClientFactory, dbClientFactory, tenv.dbName, nil)
	tenv.vrdbClient.ExpectRequest(fmt.Sprintf("select * from _vt.vreplication where db_name='%s'", tenv.dbName), &sqltypes.Result{}, nil)
	tenv.vrengine.Open(tenv.ctx)
	require.True(t, tenv.vrengine.IsOpen(), "vreplication engine was not open")

	tenv.tmc.tm = TabletManager{
		VREngine: tenv.vrengine,
		DBConfigs: &dbconfigs.DBConfigs{
			DBName: tenv.dbName,
		},
	}

	return tenv
}

func (tenv *testEnv) close() {
	tenv.mu.Lock()
	defer tenv.mu.Unlock()
	tenv.vrengine.Close()
	tenv.ts.Close()
}

//--------------------------------------
// Tablets

func (tenv *testEnv) addTablet(id int, keyspace, shard string) *fakeTabletConn {
	tenv.mu.Lock()
	defer tenv.mu.Unlock()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: tenv.cells[0],
			Uid:  uint32(id),
		},
		Keyspace: keyspace,
		Shard:    shard,
		KeyRange: &topodatapb.KeyRange{},
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

	tenv.tablets[id] = &fakeTabletConn{tablet: tablet}
	return tenv.tablets[id]
}

func (tenv *testEnv) deleteTablet(tablet *topodatapb.Tablet) {
	tenv.mu.Lock()
	defer tenv.mu.Unlock()
	tenv.ts.DeleteTablet(tenv.ctx, tablet.Alias)
	// This is not automatically removed from shard replication, which results in log spam.
	topo.DeleteTabletReplicationData(tenv.ctx, tenv.ts, tablet)
}

// fakeTabletConn implements the TabletConn interface.
type fakeTabletConn struct {
	queryservice.QueryService
	tablet *topodatapb.Tablet
}

//----------------------------------------------
// fakeTMClient

type fakeTMClient struct {
	tmclient.TabletManagerClient
	tm         TabletManager
	schema     *tabletmanagerdatapb.SchemaDefinition
	vreQueries map[int]map[string]*querypb.QueryResult
}

func newFakeTMClient() *fakeTMClient {
	return &fakeTMClient{
		vreQueries: make(map[int]map[string]*querypb.QueryResult),
		schema:     &tabletmanagerdatapb.SchemaDefinition{},
	}
}

func (tmc *fakeTMClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return tmc.schema, nil
}

func (tmc *fakeTMClient) SetSchema(schema *tabletmanagerdatapb.SchemaDefinition) {
	tmc.schema = schema
}

// ExecuteFetchAsApp is is needed for the materializer's checkTZConversion function.
func (tmc *fakeTMClient) ExecuteFetchAsApp(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsAppRequest) (*querypb.QueryResult, error) {
	return sqltypes.ResultToProto3(
		&sqltypes.Result{
			Fields: []*querypb.Field{{
				Name: "convert_tz",
			}},
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarChar("2023-07-14 09:05:01"),
			}},
		}), nil
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

func (tmc *fakeTMClient) CreateVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.CreateVReplicationWorkflowRequest) (*tabletmanagerdatapb.CreateVReplicationWorkflowResponse, error) {
	return tmc.tm.CreateVReplicationWorkflow(ctx, req)
}

func (tmc *fakeTMClient) ReadVReplicationWorkflow(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.ReadVReplicationWorkflowRequest) (*tabletmanagerdatapb.ReadVReplicationWorkflowResponse, error) {
	return tmc.tm.ReadVReplicationWorkflow(ctx, req)
}
