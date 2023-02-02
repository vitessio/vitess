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
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconntest"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclienttest"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	optionsJS         = `{"core_options": {"auto_retry": true}}`
	vdiffTestCols     = "id|vdiff_uuid|workflow|keyspace|shard|db_name|state|options|last_error"
	vdiffTestColTypes = "int64|varchar|varbinary|varbinary|varchar|varbinary|varbinary|json|varbinary"
	// This is also the keyspace name used
	vdiffDBName = "vttest"

	// vdiffSourceGtid should be the position reported by the source side VStreamResults.
	vdiffSourceGtid            = "MySQL56/f69ed286-6909-11ed-8342-0a50724f3211:1-110"
	vdiffTargetPrimaryPosition = vdiffSourceGtid
)

var (
	vreplSource       = fmt.Sprintf(`keyspace:"%s" shard:"0" filter:{rules:{match:"t1" filter:"select * from t1"}}`, vdiffDBName)
	singleRowAffected = &sqltypes.Result{RowsAffected: 1}
	noResults         = &sqltypes.Result{}
	testSchema        = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:              "t1",
				Columns:           []string{"c1", "c2"},
				PrimaryKeyColumns: []string{"c1"},
				Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			}, {
				Name:              "nonpktext",
				Columns:           []string{"c1", "textcol"},
				PrimaryKeyColumns: []string{"c1"},
				Fields:            sqltypes.MakeTestFields("c1|textcol", "int64|varchar"),
			}, {
				Name:              "pktext",
				Columns:           []string{"textcol", "c2"},
				PrimaryKeyColumns: []string{"textcol"},
				Fields:            sqltypes.MakeTestFields("textcol|c2", "varchar|int64"),
			}, {
				Name:              "multipk",
				Columns:           []string{"c1", "c2"},
				PrimaryKeyColumns: []string{"c1", "c2"},
				Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			}, {
				Name:              "aggr",
				Columns:           []string{"c1", "c2", "c3", "c4"},
				PrimaryKeyColumns: []string{"c1"},
				Fields:            sqltypes.MakeTestFields("c1|c2|c3|c4", "int64|int64|int64|int64"),
			}, {
				Name:              "datze",
				Columns:           []string{"id", "dt"},
				PrimaryKeyColumns: []string{"id"},
				Fields:            sqltypes.MakeTestFields("id|dt", "int64|datetime"),
			},
		},
	}
	tableDefMap = map[string]int{
		"t1":        0,
		"nonpktext": 1,
		"pktext":    2,
		"multipk":   3,
		"aggr":      4,
		"datze":     5,
	}
)

type testVDiffEnv struct {
	workflow        string
	se              *schema.Engine
	vse             *vstreamer.Engine
	vre             *vreplication.Engine
	vde             *Engine
	tmc             *fakeTMClient
	opts            *tabletmanagerdatapb.VDiffOptions
	dbClient        *binlogplayer.MockDBClient
	dbClientFactory func() binlogplayer.DBClient
	tmClientFactory func() tmclient.TabletManagerClient

	mu      sync.Mutex
	tablets map[int]*fakeTabletConn
}

var (
	// vdiffenv has to be a global for RegisterDialer to work.
	vdiffenv          *testVDiffEnv
	tstenv            *testenv.Env
	globalFBC         = &fakeBinlogClient{}
	globalDBQueries   = make(chan string, 1000)
	doNotLogDBQueries = false
	once              sync.Once
)

type LogExpectation struct {
	Type   string
	Detail string
}

func init() {
	tabletconn.RegisterDialer("test", func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		vdiffenv.mu.Lock()
		defer vdiffenv.mu.Unlock()
		if qs, ok := vdiffenv.tablets[int(tablet.Alias.Uid)]; ok {
			return qs, nil
		}
		return nil, fmt.Errorf("tablet %d not found", tablet.Alias.Uid)
	})
	// TableDiffer does a default grpc dial just to be sure it can talk to the tablet.
	tabletconn.RegisterDialer("grpc", func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		vdiffenv.mu.Lock()
		defer vdiffenv.mu.Unlock()
		if qs, ok := vdiffenv.tablets[int(tablet.Alias.Uid)]; ok {
			return qs, nil
		}
		return nil, fmt.Errorf("tablet %d not found", tablet.Alias.Uid)
	})
	tabletconntest.SetProtocol("go.vt.vttablet.tabletmanager.vdiff.framework_test", "test")

	binlogplayer.RegisterClientFactory("test", func() binlogplayer.Client { return globalFBC })
	binlogplayer.SetProtocol("vdiff_test_framework", "test")

	tmclient.RegisterTabletManagerClientFactory("test", func() tmclient.TabletManagerClient { return vdiffenv.tmc })
	tmclienttest.SetProtocol("go.vt.vttablet.tabletmanager.vdiff.framework_test", "test")
}

func TestMain(m *testing.M) {
	exitCode := func() int {
		var err error
		tstenv, err = testenv.Init()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer tstenv.Close()

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
	if request.Target.Keyspace != vdiffDBName {
		<-ctx.Done()
		return io.EOF
	}
	if vstreamHook != nil {
		vstreamHook(ctx)
	}
	return vdiffenv.vse.Stream(ctx, request.Position, request.TableLastPKs, request.Filter, send)
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
	return vdiffenv.vse.StreamRows(ctx, request.Query, row, func(rows *binlogdatapb.VStreamRowsResponse) error {
		if vstreamRowsSendHook != nil {
			vstreamRowsSendHook(ctx)
		}
		return send(rows)
	})
}

func (ftc *fakeTabletConn) Close(ctx context.Context) error {
	return nil
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
	return vdiffDBName
}

func (dbc *realDBClient) Connect() error {
	app, err := tstenv.Dbcfgs.AppWithDB().MysqlParams()
	if err != nil {
		return err
	}
	app.DbName = vdiffDBName
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

// setVRResults allows you to specify VReplicationExec queries and their results. You can specify
// exact strings or strings prefixed with a '/', in which case they will be treated as a valid
// regexp.
func (tmc *fakeTMClient) setVRResults(tablet *topodatapb.Tablet, query string, result *sqltypes.Result) {
	queries, ok := tmc.vrQueries[int(tablet.Alias.Uid)]
	if !ok {
		queries = make(map[string]*querypb.QueryResult)
		tmc.vrQueries[int(tablet.Alias.Uid)] = queries
	}
	queries[query] = sqltypes.ResultToProto3(result)
}

func (tmc *fakeTMClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	if result, ok := tmc.vrQueries[int(tablet.Alias.Uid)][query]; ok {
		return result, nil
	}
	for qry, res := range tmc.vrQueries[int(tablet.Alias.Uid)] {
		if strings.HasPrefix(qry, "/") {
			re := regexp.MustCompile(qry)
			if re.MatchString(qry) {
				return res, nil
			}
		}
	}
	return nil, fmt.Errorf("query %q not found for tablet %d", query, tablet.Alias.Uid)
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

// ----------------------------------------------
// testVDiffEnv

func newTestVDiffEnv(t *testing.T) *testVDiffEnv {
	vdiffenv = &testVDiffEnv{
		workflow: "testwf",
		tablets:  make(map[int]*fakeTabletConn),
		tmc:      newFakeTMClient(),
		se:       schema.NewEngineForTests(),
		dbClient: binlogplayer.NewMockDBClient(t),
	}
	vdiffenv.dbClientFactory = func() binlogplayer.DBClient { return vdiffenv.dbClient }
	vdiffenv.tmClientFactory = func() tmclient.TabletManagerClient { return vdiffenv.tmc }

	tstenv.KeyspaceName = vdiffDBName

	vdiffenv.vse = vstreamer.NewEngine(tstenv.TabletEnv, tstenv.SrvTopo, vdiffenv.se, nil, tstenv.Cells[0])
	vdiffenv.vse.InitDBConfig(tstenv.KeyspaceName, tstenv.ShardName)
	vdiffenv.vse.Open()

	once.Do(func() {
		var ddls []string

		// This is needed for the vstreamer engine and the snapshotConn which
		// use the real DB started by vttestserver
		ddls = append(ddls, fmt.Sprintf("create database if not exists %s", vdiffDBName))
		ddls = append(ddls, fmt.Sprintf("create table if not exists %s.t1 (c1 bigint primary key, c2 bigint)", vdiffDBName))

		for _, ddl := range ddls {
			if err := tstenv.Mysqld.ExecuteSuperQuery(context.Background(), ddl); err != nil {
				fmt.Fprintf(os.Stderr, "%v", err)
			}
		}
	})

	vdiffenv.vre = vreplication.NewSimpleTestEngine(tstenv.TopoServ, tstenv.Cells[0], tstenv.Mysqld, realDBClientFactory, realDBClientFactory, vdiffDBName, nil)
	vdiffenv.vre.Open(context.Background())

	vdiffenv.tmc.schema = testSchema
	// We need to add t1, which we use for a full VDiff in TestVDiff, to
	// the schema engine with the PK val.
	st := &schema.Table{
		Name:      sqlparser.NewIdentifierCS(testSchema.TableDefinitions[tableDefMap["t1"]].Name),
		Fields:    testSchema.TableDefinitions[tableDefMap["t1"]].Fields,
		PKColumns: []int{0},
	}
	vdiffenv.se.SetTableForTests(st)

	tabletID := 100
	primary := vdiffenv.addTablet(tabletID, tstenv.KeyspaceName, tstenv.ShardName, topodatapb.TabletType_PRIMARY)

	vdiffenv.vde = NewTestEngine(tstenv.TopoServ, primary.tablet, vdiffDBName, vdiffenv.dbClientFactory, vdiffenv.tmClientFactory)
	require.False(t, vdiffenv.vde.IsOpen())

	vdiffenv.opts = &tabletmanagerdatapb.VDiffOptions{
		CoreOptions: &tabletmanagerdatapb.VDiffCoreOptions{},
		ReportOptions: &tabletmanagerdatapb.VDiffReportOptions{
			Format:     "json",
			OnlyPks:    true,
			DebugQuery: true,
		},
	}

	// vdiff.syncTargets. This actually happens after stopTargets.
	// But this is one statement per stream.
	vdiffenv.tmc.setVRResults(
		primary.tablet,
		"/update _vt.vreplication set state='Running', stop_pos='MySQL56/.*', message='synchronizing for vdiff' where id=1",
		noResults,
	)

	// vdiff.stopTargets
	vdiffenv.tmc.setVRResults(primary.tablet, fmt.Sprintf("update _vt.vreplication set state='Stopped', message='for vdiff' where workflow = '%s' and db_name = '%s'", vdiffenv.workflow, vdiffDBName), singleRowAffected)

	// vdiff.syncTargets (continued)
	vdiffenv.tmc.vrpos[tabletID] = vdiffSourceGtid
	vdiffenv.tmc.pos[tabletID] = vdiffTargetPrimaryPosition

	// vdiff.startQueryStreams
	vdiffenv.tmc.waitpos[tabletID] = vdiffTargetPrimaryPosition

	// vdiff.restartTargets
	vdiffenv.tmc.setVRResults(primary.tablet, fmt.Sprintf("update _vt.vreplication set state='Running', message='', stop_pos='' where db_name='%s' and workflow='%s'", vdiffDBName, vdiffenv.workflow), singleRowAffected)

	vdiffenv.dbClient.ExpectRequest("select * from _vt.vdiff where state in ('started','pending')", noResults, nil)
	vdiffenv.vde.Open(context.Background(), vdiffenv.vre)
	assert.True(t, vdiffenv.vde.IsOpen())
	assert.Equal(t, 0, len(vdiffenv.vde.controllers))

	resetBinlogClient()

	return vdiffenv
}

func (tvde *testVDiffEnv) close() {
	tvde.mu.Lock()
	defer tvde.mu.Unlock()
	for _, t := range tvde.tablets {
		tstenv.TopoServ.DeleteTablet(context.Background(), t.tablet.Alias)
		topo.DeleteTabletReplicationData(context.Background(), tstenv.TopoServ, t.tablet)
		tstenv.SchemaEngine.Reload(context.Background())
	}
	tvde.tablets = nil
	vdiffenv.vse.Close()
	vdiffenv.vre.Close()
	vdiffenv.vde.Close()
	vdiffenv.dbClient.Close()
}

func (tvde *testVDiffEnv) addTablet(id int, keyspace, shard string, tabletType topodatapb.TabletType) *fakeTabletConn {
	tvde.mu.Lock()
	defer tvde.mu.Unlock()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: tstenv.Cells[0],
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
	tvde.tablets[id] = &fakeTabletConn{tablet: tablet}
	if err := tstenv.TopoServ.InitTablet(context.Background(), tablet, false /* allowPrimaryOverride */, true /* createShardAndKeyspace */, false /* allowUpdate */); err != nil {
		panic(err)
	}
	if tabletType == topodatapb.TabletType_PRIMARY {
		_, err := tstenv.TopoServ.UpdateShardFields(context.Background(), keyspace, shard, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = tablet.Alias
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	tstenv.SchemaEngine.Reload(context.Background())
	return tvde.tablets[id]
}
