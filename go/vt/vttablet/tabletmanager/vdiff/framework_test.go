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

var (
	vstreamerEngine   *vstreamer.Engine
	vreplEngine       *vreplication.Engine
	env               *testenv.Env
	tmc               = newFakeTMClient()
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

	tmclient.RegisterTabletManagerClientFactory("test", func() tmclient.TabletManagerClient { return tmc })
	tmclienttest.SetProtocol("go.vt.vttablet.tabletmanager.vdiff.framework_test", "test")
}

func TestMain(m *testing.M) {
	binlogplayer.SetProtocol("vdiff_test_framework", "test")
	exitCode := func() int {
		var err error
		env, err = testenv.Init()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer env.Close()

		vstreamerEngine = vstreamer.NewEngine(env.TabletEnv, env.SrvTopo, env.SchemaEngine, nil, env.Cells[0])
		vstreamerEngine.InitDBConfig(env.KeyspaceName, env.ShardName)
		vstreamerEngine.Open()
		defer vstreamerEngine.Close()

		ddls := binlogplayer.CreateVReplicationTable()
		ddls = append(ddls, binlogplayer.AlterVReplicationTable...)
		ddls = append(ddls, withDDL.DDLs()...)
		ddls = append(ddls, fmt.Sprintf("create database %s", vdiffdb))

		for _, ddl := range ddls {
			if err := env.Mysqld.ExecuteSuperQuery(context.Background(), ddl); err != nil {
				fmt.Fprintf(os.Stderr, "%v", err)
			}
		}

		vreplEngine = vreplication.NewTestEngine(env.TopoServ, env.Cells[0], env.Mysqld, realDBClientFactory, realDBClientFactory, vdiffdb, nil)
		vreplEngine.Open(context.Background())
		defer vreplEngine.Close()

		tmc.schema = testSchema

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
			Cell: env.Cells[0],
			Uid:  uint32(id),
		},
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		KeyRange: &topodatapb.KeyRange{},
		Type:     topodatapb.TabletType_REPLICA,
		PortMap: map[string]int32{
			"test": int32(id),
		},
	}
	if err := env.TopoServ.CreateTablet(context.Background(), tablet); err != nil {
		panic(err)
	}
	env.SchemaEngine.Reload(context.Background())
	return tablet
}

func deleteTablet(tablet *topodatapb.Tablet) {
	env.TopoServ.DeleteTablet(context.Background(), tablet.Alias)
	// This is not automatically removed from shard replication, which results in log spam.
	topo.DeleteTabletReplicationData(context.Background(), env.TopoServ, tablet)
	env.SchemaEngine.Reload(context.Background())
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
	return vstreamerEngine.Stream(ctx, request.Position, request.TableLastPKs, request.Filter, send)
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
	return vstreamerEngine.StreamRows(ctx, request.Query, row, func(rows *binlogdatapb.VStreamRowsResponse) error {
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
	app, err := env.Dbcfgs.AppWithDB().MysqlParams()
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
