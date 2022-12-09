/*
Copyright 2022 The Vitess Authors.

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
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclienttest"
	"vitess.io/vitess/go/vt/withddl"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	// vdiffStopPosition is the default stop position for the target vreplication.
	// It can be overridden with the positons argument to newTestVDiffEnv.
	vdiffStopPosition = "MariaDB/5-456-892"
	// vdiffSourceGtid should be the position reported by the source side VStreamResults.
	// It's expected to be higher the vdiffStopPosition.
	vdiffSourceGtid = "MariaDB/5-456-893"
	// vdiffTargetPrimaryPosition is the primary position of the target after
	// vreplication has been synchronized.
	vdiffTargetPrimaryPosition = "MariaDB/6-456-892"
)

type testVDiffEnv struct {
	workflow        string
	vstreamerEngine *vstreamer.Engine
	vreplEngine     *vreplication.Engine
	tenv            *testenv.Env
	tmc             *fakeTMClient

	mu      sync.Mutex
	tablets map[int]*testVDiffTablet
}

var (
	vdiffEnv          = &testVDiffEnv{}
	globalFBC         = &fakeBinlogClient{}
	globalDBQueries   = make(chan string, 1000)
	vdiffdb           = "vdiff_test"
	doNotLogDBQueries = false
)

type LogExpectation struct {
	Type   string
	Detail string
}

func init() {
	tabletconn.RegisterDialer("test", func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		return &fakeTabletConn{
			QueryService: fakes.ErrorQueryService,
			tablet:       tablet,
		}, nil
	})
	tabletconntest.SetProtocol("go.vt.vttablet.tabletmanager.vdiff.framework_test", "test")

	binlogplayer.RegisterClientFactory("test", func() binlogplayer.Client { return globalFBC })

	tmclient.RegisterTabletManagerClientFactory("test", func() tmclient.TabletManagerClient { return vdiffEnv.tmc })
	tmclienttest.SetProtocol("go.vt.vttablet.tabletmanager.vdiff.framework_test", "test")
}

func TestMain(m *testing.M) {
	binlogplayer.SetProtocol("vdiff_test_framework", "test")
	exitCode := func() int {
		var err error
		vdiffEnv.tenv, err = testenv.Init()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer vdiffEnv.tenv.Close()

		vdiffEnv.tmc = newFakeTMClient()

		vdiffEnv.vstreamerEngine = vstreamer.NewEngine(vdiffEnv.tenv.TabletEnv, vdiffEnv.tenv.SrvTopo, vdiffEnv.tenv.SchemaEngine, nil, vdiffEnv.tenv.Cells[0])
		vdiffEnv.vstreamerEngine.InitDBConfig(vdiffEnv.tenv.KeyspaceName, vdiffEnv.tenv.ShardName)
		vdiffEnv.vstreamerEngine.Open()
		defer vdiffEnv.vstreamerEngine.Close()

		ddls := binlogplayer.CreateVReplicationTable()
		ddls = append(ddls, binlogplayer.AlterVReplicationTable...)
		ddls = append(ddls, withDDL.DDLs()...)
		ddls = append(ddls, fmt.Sprintf("create database %s", vdiffdb))

		for _, ddl := range ddls {
			if err := vdiffEnv.tenv.Mysqld.ExecuteSuperQuery(context.Background(), ddl); err != nil {
				fmt.Fprintf(os.Stderr, "%v", err)
			}
		}

		vdiffEnv.vreplEngine = vreplication.NewTestEngine(vdiffEnv.tenv.TopoServ, vdiffEnv.tenv.Cells[0], vdiffEnv.tenv.Mysqld, realDBClientFactory, realDBClientFactory, vdiffdb, nil)
		vdiffEnv.vreplEngine.Open(context.Background())
		defer vdiffEnv.vreplEngine.Close()

		vdiffEnv.tmc.schema = testSchema

		return m.Run()
	}()
	os.Exit(exitCode)
}

func resetBinlogClient() {
	globalFBC = &fakeBinlogClient{}
}

// shortCircuitTestAfterQuery is used to short circuit a test after a specific query is executed.
// This can be used to end a vdiff, by returning an error from the specified query, once the test
// has verified the necessary behavior.
func shortCircuitTestAfterQuery(query string, dbClient *binlogplayer.MockDBClient) {
	dbClient.ExpectRequest(query, singleRowAffected, fmt.Errorf("Short circuiting test"))
	dbClient.ExpectRequest("update _vt.vdiff set state = 'error', last_error = 'Short circuiting test'  where id = 1", singleRowAffected, nil)
	dbClient.ExpectRequest("insert into _vt.vdiff_log(vdiff_id, message) values (1, 'State changed to: error')", singleRowAffected, nil)
	dbClient.ExpectRequest("insert into _vt.vdiff_log(vdiff_id, message) values (1, 'Error: Short circuiting test')", singleRowAffected, nil)
}

//--------------------------------------
// Topos and tablets

func addTablet(id int) *topodatapb.Tablet {
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: vdiffEnv.tenv.Cells[0],
			Uid:  uint32(id),
		},
		Keyspace: vdiffEnv.tenv.KeyspaceName,
		Shard:    vdiffEnv.tenv.ShardName,
		KeyRange: &topodatapb.KeyRange{},
		Type:     topodatapb.TabletType_REPLICA,
		PortMap: map[string]int32{
			"test": int32(id),
		},
	}
	if err := vdiffEnv.tenv.TopoServ.CreateTablet(context.Background(), tablet); err != nil {
		panic(err)
	}
	vdiffEnv.tenv.SchemaEngine.Reload(context.Background())
	return tablet
}

func deleteTablet(tablet *topodatapb.Tablet) {
	vdiffEnv.tenv.TopoServ.DeleteTablet(context.Background(), tablet.Alias)
	// This is not automatically removed from shard replication, which results in log spam.
	topo.DeleteTabletReplicationData(context.Background(), vdiffEnv.tenv.TopoServ, tablet)
	vdiffEnv.tenv.SchemaEngine.Reload(context.Background())
}

// fakeTabletConn implement TabletConn interface. We only care about the
// health check part. The state reported by the tablet will depend
// on the Tag values "serving" and "healthy".
type fakeTabletConn struct {
	queryservice.QueryService
	tablet *topodatapb.Tablet
}

// StreamHealth is part of queryservice.QueryService.
func (ftc *fakeTabletConn) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	return callback(&querypb.StreamHealthResponse{
		Serving: true,
		Target: &querypb.Target{
			Keyspace:   ftc.tablet.Keyspace,
			Shard:      ftc.tablet.Shard,
			TabletType: ftc.tablet.Type,
		},
		RealtimeStats: &querypb.RealtimeStats{},
	})
}

// vstreamHook allows you to do work just before calling VStream.
var vstreamHook func(ctx context.Context)

// VStream directly calls into the pre-initialized engine.
func (ftc *fakeTabletConn) VStream(ctx context.Context, request *binlogdatapb.VStreamRequest, send func([]*binlogdatapb.VEvent) error) error {
	if request.Target.Keyspace != "vttest" {
		<-ctx.Done()
		return io.EOF
	}
	if vstreamHook != nil {
		vstreamHook(ctx)
	}
	return vdiffEnv.vstreamerEngine.Stream(ctx, request.Position, request.TableLastPKs, request.Filter, send)
}

// vstreamRowsHook allows you to do work just before calling VStreamRows.
var vstreamRowsHook func(ctx context.Context)

// vstreamRowsSendHook allows you to do work just before VStreamRows calls send.
var vstreamRowsSendHook func(ctx context.Context)

// VStreamRows directly calls into the pre-initialized engine.
func (ftc *fakeTabletConn) VStreamRows(ctx context.Context, request *binlogdatapb.VStreamRowsRequest, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	if vstreamRowsHook != nil {
		vstreamRowsHook(ctx)
	}
	var row []sqltypes.Value
	if request.Lastpk != nil {
		r := sqltypes.Proto3ToResult(request.Lastpk)
		if len(r.Rows) != 1 {
			return fmt.Errorf("unexpected lastpk input: %v", request.Lastpk)
		}
		row = r.Rows[0]
	}
	return vdiffEnv.vstreamerEngine.StreamRows(ctx, request.Query, row, func(rows *binlogdatapb.VStreamRowsResponse) error {
		if vstreamRowsSendHook != nil {
			vstreamRowsSendHook(ctx)
		}
		return send(rows)
	})
}

//--------------------------------------
// Binlog Client to TabletManager

// fakeBinlogClient satisfies binlogplayer.Client.
// Not to be used concurrently.
type fakeBinlogClient struct {
	lastTablet   *topodatapb.Tablet
	lastPos      string
	lastTables   []string
	lastKeyRange *topodatapb.KeyRange
	lastCharset  *binlogdatapb.Charset
}

func (fbc *fakeBinlogClient) Dial(tablet *topodatapb.Tablet) error {
	fbc.lastTablet = tablet
	return nil
}

func (fbc *fakeBinlogClient) Close() {
}

func (fbc *fakeBinlogClient) StreamTables(ctx context.Context, position string, tables []string, charset *binlogdatapb.Charset) (binlogplayer.BinlogTransactionStream, error) {
	fbc.lastPos = position
	fbc.lastTables = tables
	fbc.lastCharset = charset
	return &btStream{ctx: ctx}, nil
}

func (fbc *fakeBinlogClient) StreamKeyRange(ctx context.Context, position string, keyRange *topodatapb.KeyRange, charset *binlogdatapb.Charset) (binlogplayer.BinlogTransactionStream, error) {
	fbc.lastPos = position
	fbc.lastKeyRange = keyRange
	fbc.lastCharset = charset
	return &btStream{ctx: ctx}, nil
}

// btStream satisfies binlogplayer.BinlogTransactionStream
type btStream struct {
	ctx  context.Context
	sent bool
}

func (bts *btStream) Recv() (*binlogdatapb.BinlogTransaction, error) {
	if !bts.sent {
		bts.sent = true
		return &binlogdatapb.BinlogTransaction{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{
					Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
					Sql:      []byte("insert into t values(1)"),
				},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 72,
				Position:  "MariaDB/0-1-1235",
			},
		}, nil
	}
	<-bts.ctx.Done()
	return nil, bts.ctx.Err()
}

//--------------------------------------
// DBCLient wrapper

func realDBClientFactory() binlogplayer.DBClient {
	return &realDBClient{}
}

type realDBClient struct {
	conn  *mysql.Conn
	nolog bool
}

func (dbc *realDBClient) DBName() string {
	return vdiffdb
}

func (dbc *realDBClient) Connect() error {
	app, err := vdiffEnv.tenv.Dbcfgs.AppWithDB().MysqlParams()
	if err != nil {
		return err
	}
	app.DbName = vdiffdb
	conn, err := mysql.Connect(context.Background(), app)
	if err != nil {
		return err
	}
	dbc.conn = conn
	return nil
}

func (dbc *realDBClient) Begin() error {
	_, err := dbc.ExecuteFetch("begin", 10000)
	return err
}

func (dbc *realDBClient) Commit() error {
	_, err := dbc.ExecuteFetch("commit", 10000)
	return err
}

func (dbc *realDBClient) Rollback() error {
	_, err := dbc.ExecuteFetch("rollback", 10000)
	return err
}

func (dbc *realDBClient) Close() {
	dbc.conn.Close()
}

func (dbc *realDBClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	// Use Clone() because the contents of memory region referenced by
	// string can change when clients (e.g. vcopier) use unsafe string methods.
	query = strings.Clone(query)
	if strings.HasPrefix(query, "use") ||
		query == withddl.QueryToTriggerWithDDL { // this query breaks unit tests since it errors out
		return nil, nil
	}
	qr, err := dbc.conn.ExecuteFetch(query, 10000, true)
	if doNotLogDBQueries {
		return qr, err
	}
	if !strings.HasPrefix(query, "select") && !strings.HasPrefix(query, "set") && !dbc.nolog {
		globalDBQueries <- query
	}
	return qr, err
}

//----------------------------------------------
// fakeTMClient

type fakeTMClient struct {
	tmclient.TabletManagerClient
	schema    *tabletmanagerdatapb.SchemaDefinition
	vrQueries map[int]map[string]*querypb.QueryResult
	waitpos   map[int]string
	vrpos     map[int]string
	pos       map[int]string
}

func newFakeTMClient() *fakeTMClient {
	return &fakeTMClient{
		vrQueries: make(map[int]map[string]*querypb.QueryResult),
		waitpos:   make(map[int]string),
		vrpos:     make(map[int]string),
		pos:       make(map[int]string),
	}
}

func (tmc *fakeTMClient) GetSchema(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	return tmc.schema, nil
}

func (tmc *fakeTMClient) setVRResults(tablet *topodatapb.Tablet, query string, result *sqltypes.Result) {
	queries, ok := tmc.vrQueries[int(tablet.Alias.Uid)]
	if !ok {
		queries = make(map[string]*querypb.QueryResult)
		tmc.vrQueries[int(tablet.Alias.Uid)] = queries
	}
	queries[query] = sqltypes.ResultToProto3(result)
}

func (tmc *fakeTMClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	result, ok := tmc.vrQueries[int(tablet.Alias.Uid)][query]
	if !ok {
		return nil, fmt.Errorf("query %q not found for tablet %d", query, tablet.Alias.Uid)
	}
	return result, nil
}

func (tmc *fakeTMClient) WaitForPosition(ctx context.Context, tablet *topodatapb.Tablet, pos string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if pos != tmc.waitpos[int(tablet.Alias.Uid)] {
		return fmt.Errorf("waitpos %s not reached for tablet %d", pos, tablet.Alias.Uid)
	}
	return nil
}

func (tmc *fakeTMClient) VReplicationWaitForPos(ctx context.Context, tablet *topodatapb.Tablet, id int, pos string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if pos != tmc.vrpos[int(tablet.Alias.Uid)] {
		return fmt.Errorf("vrpos %s not reached for tablet %d", pos, tablet.Alias.Uid)
	}
	return nil
}

func (tmc *fakeTMClient) PrimaryPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	pos, ok := tmc.pos[int(tablet.Alias.Uid)]
	if !ok {
		return "", fmt.Errorf("no primary position for %d", tablet.Alias.Uid)
	}
	return pos, nil
}

//----------------------------------------------
// testVDiffEnv

func newTestVDiffEnv(sourceShards, targetShards []string, query string, positions map[string]string) *testVDiffEnv {
	vdiffEnv := &testVDiffEnv{
		workflow: "vdiffTest",
		tablets:  make(map[int]*testVDiffTablet),
		tmc:      newFakeTMClient(),
	}

	tabletID := 100
	for _, shard := range sourceShards {
		_ = vdiffEnv.addTablet(tabletID, "source", shard, topodatapb.TabletType_PRIMARY)
		_ = vdiffEnv.addTablet(tabletID+1, "source", shard, topodatapb.TabletType_REPLICA)
		vdiffEnv.tmc.waitpos[tabletID+1] = vdiffStopPosition

		tabletID += 10
	}
	tabletID = 200
	for _, shard := range targetShards {
		primary := vdiffEnv.addTablet(tabletID, "target", shard, topodatapb.TabletType_PRIMARY)
		_ = vdiffEnv.addTablet(tabletID+1, "target", shard, topodatapb.TabletType_REPLICA)

		var rows []string
		var posRows []string
		for j, sourceShard := range sourceShards {
			bls := &binlogdatapb.BinlogSource{
				Keyspace: "source",
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
			vdiffEnv.tmc.setVRResults(
				primary.tablet,
				fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos='%s', message='synchronizing for vdiff' where id=%d", vdiffSourceGtid, j+1),
				&sqltypes.Result{},
			)
		}
		// migrater buildMigrationTargets
		vdiffEnv.tmc.setVRResults(
			primary.tablet,
			"select id, source, message, cell, tablet_types, workflow_type, workflow_sub_type from _vt.vreplication where workflow='vdiffTest' and db_name='vt_target'",
			sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type",
				"int64|varchar|varchar|varchar|varchar|int64|int64"),
				rows...,
			),
		)

		// vdiff.stopTargets
		vdiffEnv.tmc.setVRResults(primary.tablet, "update _vt.vreplication set state='Stopped', message='for vdiff' where db_name='vt_target' and workflow='vdiffTest'", &sqltypes.Result{})
		vdiffEnv.tmc.setVRResults(
			primary.tablet,
			"select source, pos from _vt.vreplication where db_name='vt_target' and workflow='vdiffTest'",
			sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"source|pos",
				"varchar|varchar"),
				posRows...,
			),
		)

		// vdiff.syncTargets (continued)
		vdiffEnv.tmc.vrpos[tabletID] = vdiffSourceGtid
		vdiffEnv.tmc.pos[tabletID] = vdiffTargetPrimaryPosition

		// vdiff.startQueryStreams
		vdiffEnv.tmc.waitpos[tabletID+1] = vdiffTargetPrimaryPosition

		// vdiff.restartTargets
		vdiffEnv.tmc.setVRResults(primary.tablet, "update _vt.vreplication set state='Running', message='', stop_pos='' where db_name='vt_target' and workflow='vdiffTest'", &sqltypes.Result{})

		tabletID += 10
	}
	return vdiffEnv
}

func (tvde *testVDiffEnv) close() {
	tvde.mu.Lock()
	defer tvde.mu.Unlock()
	for _, t := range tvde.tablets {
		vdiffEnv.tenv.TopoServ.DeleteTablet(context.Background(), t.tablet.Alias)
	}
	tvde.tablets = nil
}

func (tvde *testVDiffEnv) addTablet(id int, keyspace, shard string, tabletType topodatapb.TabletType) *testVDiffTablet {
	tvde.mu.Lock()
	defer tvde.mu.Unlock()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: vdiffEnv.tenv.Cells[0],
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
	tvde.tablets[id] = newTestVDiffTablet(tablet)
	if err := vdiffEnv.tenv.TopoServ.InitTablet(context.Background(), tablet, false /* allowPrimaryOverride */, true /* createShardAndKeyspace */, false /* allowUpdate */); err != nil {
		panic(err)
	}
	if tabletType == topodatapb.TabletType_PRIMARY {
		_, err := vdiffEnv.tenv.TopoServ.UpdateShardFields(context.Background(), keyspace, shard, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = tablet.Alias
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	return tvde.tablets[id]
}

//----------------------------------------------
// testVDiffTablet

type testVDiffTablet struct {
	queryservice.QueryService
	tablet  *topodatapb.Tablet
	queries map[string][]*binlogdatapb.VStreamResultsResponse
}

func newTestVDiffTablet(tablet *topodatapb.Tablet) *testVDiffTablet {
	return &testVDiffTablet{
		QueryService: fakes.ErrorQueryService,
		tablet:       tablet,
		queries:      make(map[string][]*binlogdatapb.VStreamResultsResponse),
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

func (tvt *testVDiffTablet) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	results, ok := tvt.queries[query]
	if !ok {
		return fmt.Errorf("query %q not in list", query)
	}
	for _, result := range results {
		if err := send(result); err != nil {
			return err
		}
	}
	return nil
}

func (tvt *testVDiffTablet) setResults(query string, gtid string, results []*sqltypes.Result) {
	vrs := []*binlogdatapb.VStreamResultsResponse{{
		Fields: results[0].Fields,
		Gtid:   gtid,
	}}

	for _, result := range results[1:] {
		vr := &binlogdatapb.VStreamResultsResponse{
			Rows: sqltypes.RowsToProto3(result.Rows),
		}
		vrs = append(vrs, vr)
	}
	tvt.queries[query] = vrs
}
