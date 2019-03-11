/*
Copyright 2018 The Vitess Authors.

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

package vreplication

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	playerEngine    *Engine
	streamerEngine  *vstreamer.Engine
	env             *testenv.Env
	globalFBC       = &fakeBinlogClient{}
	vrepldb         = "vrepl"
	globalDBQueries = make(chan string, 1000)
)

func init() {
	tabletconn.RegisterDialer("test", func(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
		return &fakeTabletConn{
			QueryService: fakes.ErrorQueryService,
			tablet:       tablet,
		}, nil
	})
	flag.Set("tablet_protocol", "test")

	binlogplayer.RegisterClientFactory("test", func() binlogplayer.Client { return globalFBC })
	flag.Set("binlog_player_protocol", "test")
}

func TestMain(m *testing.M) {
	flag.Parse() // Do not remove this comment, import into google3 depends on it

	exitCode := func() int {
		var err error
		env, err = testenv.Init()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer env.Close()

		// engines cannot be initialized in testenv because it introduces
		// circular dependencies.
		streamerEngine = vstreamer.NewEngine(env.SrvTopo, env.SchemaEngine)
		streamerEngine.InitDBConfig(env.Dbcfgs)
		streamerEngine.Open(env.KeyspaceName, env.Cells[0])
		defer streamerEngine.Close()

		if err := env.Mysqld.ExecuteSuperQuery(context.Background(), fmt.Sprintf("create database %s", vrepldb)); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}

		if err := env.Mysqld.ExecuteSuperQuery(context.Background(), "set @@global.innodb_lock_wait_timeout=1"); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}

		playerEngine = NewEngine(env.TopoServ, env.Cells[0], env.Mysqld, realDBClientFactory, vrepldb)
		if err := playerEngine.Open(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer playerEngine.Close()

		if err := env.Mysqld.ExecuteSuperQueryList(context.Background(), binlogplayer.CreateVReplicationTable()); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}

		if err := env.Mysqld.ExecuteSuperQueryList(context.Background(), CreateCopyState); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func resetBinlogClient() {
	globalFBC = &fakeBinlogClient{}
}

//--------------------------------------
// Topos and tablets

func addTablet(id int, shard string, tabletType topodatapb.TabletType, serving, healthy bool) *topodatapb.Tablet {
	t := newTablet(id, shard, tabletType, serving, healthy)
	if err := env.TopoServ.CreateTablet(context.Background(), t); err != nil {
		panic(err)
	}
	return t
}

func deleteTablet(t *topodatapb.Tablet) {
	env.TopoServ.DeleteTablet(context.Background(), t.Alias)
	// This is not automatically removed from shard replication, which results in log spam.
	topo.DeleteTabletReplicationData(context.Background(), env.TopoServ, t)
}

func newTablet(id int, shard string, tabletType topodatapb.TabletType, serving, healthy bool) *topodatapb.Tablet {
	stag := "not_serving"
	if serving {
		stag = "serving"
	}
	htag := "not_healthy"
	if healthy {
		htag = "healthy"
	}
	_, kr, err := topo.ValidateShardName(shard)
	if err != nil {
		panic(err)
	}
	return &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: env.Cells[0],
			Uid:  uint32(id),
		},
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		KeyRange: kr,
		Type:     tabletType,
		Tags: map[string]string{
			"serving": stag,
			"healthy": htag,
		},
		PortMap: map[string]int32{
			"test": int32(id),
		},
	}
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
	serving := true
	if s, ok := ftc.tablet.Tags["serving"]; ok {
		serving = (s == "serving")
	}
	var herr string
	if s, ok := ftc.tablet.Tags["healthy"]; ok && s != "healthy" {
		herr = "err"
	}
	callback(&querypb.StreamHealthResponse{
		Serving: serving,
		Target: &querypb.Target{
			Keyspace:   ftc.tablet.Keyspace,
			Shard:      ftc.tablet.Shard,
			TabletType: ftc.tablet.Type,
		},
		RealtimeStats: &querypb.RealtimeStats{HealthError: herr},
	})
	return nil
}

// VStream directly calls into the pre-initialized engine.
func (ftc *fakeTabletConn) VStream(ctx context.Context, target *querypb.Target, startPos string, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	return streamerEngine.Stream(ctx, startPos, filter, send)
}

// VStreamRows directly calls into the pre-initialized engine.
func (ftc *fakeTabletConn) VStreamRows(ctx context.Context, target *querypb.Target, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	var row []sqltypes.Value
	if lastpk != nil {
		r := sqltypes.Proto3ToResult(lastpk)
		if len(r.Rows) != 1 {
			return fmt.Errorf("unexpected lastpk input: %v", lastpk)
		}
		row = r.Rows[0]
	}
	return streamerEngine.StreamRows(ctx, query, row, send)
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

func (t *btStream) Recv() (*binlogdatapb.BinlogTransaction, error) {
	if !t.sent {
		t.sent = true
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
	<-t.ctx.Done()
	return nil, t.ctx.Err()
}

func expectFBCRequest(t *testing.T, tablet *topodatapb.Tablet, pos string, tables []string, kr *topodatapb.KeyRange) {
	t.Helper()
	if !proto.Equal(tablet, globalFBC.lastTablet) {
		t.Errorf("Request tablet: %v, want %v", globalFBC.lastTablet, tablet)
	}
	if pos != globalFBC.lastPos {
		t.Errorf("Request pos: %v, want %v", globalFBC.lastPos, pos)
	}
	if !reflect.DeepEqual(tables, globalFBC.lastTables) {
		t.Errorf("Request tables: %v, want %v", globalFBC.lastTables, tables)
	}
	if !proto.Equal(kr, globalFBC.lastKeyRange) {
		t.Errorf("Request KeyRange: %v, want %v", globalFBC.lastKeyRange, kr)
	}
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
	return vrepldb
}

func (dbc *realDBClient) Connect() error {
	app := env.Dbcfgs.AppWithDB()
	app.DbName = vrepldb
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
	dbc.conn = nil
}

func (dbc *realDBClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	if strings.HasPrefix(query, "use") {
		return nil, nil
	}
	qr, err := dbc.conn.ExecuteFetch(query, 10000, true)
	if !strings.HasPrefix(query, "select") && !strings.HasPrefix(query, "set") && !dbc.nolog {
		globalDBQueries <- query
	}
	return qr, err
}

func expectDBClientQueries(t *testing.T, queries []string) {
	t.Helper()
	failed := false
	for i, query := range queries {
		if failed {
			t.Errorf("no query received, expecting %s", query)
			continue
		}
		var got string
		select {
		case got = <-globalDBQueries:
			var match bool
			if query[0] == '/' {
				result, err := regexp.MatchString(query[1:], got)
				if err != nil {
					panic(err)
				}
				match = result
			} else {
				match = (got == query)
			}
			if !match {
				t.Errorf("query:\n%q, does not match query %d:\n%q", got, i, query)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("no query received, expecting %s", query)
			failed = true
		}
	}
	for {
		select {
		case got := <-globalDBQueries:
			t.Errorf("unexpected query: %s", got)
		default:
			return
		}
	}
}

func expectData(t *testing.T, table string, values [][]string) {
	t.Helper()

	var query string
	if len(strings.Split(table, ".")) == 1 {
		query = fmt.Sprintf("select * from %s.%s", vrepldb, table)
	} else {
		query = fmt.Sprintf("select * from %s", table)
	}
	qr, err := env.Mysqld.FetchSuperQuery(context.Background(), query)
	if err != nil {
		t.Error(err)
		return
	}
	if len(values) != len(qr.Rows) {
		t.Fatalf("row counts don't match: %v, want %v", qr.Rows, values)
	}
	for i, row := range values {
		if len(row) != len(qr.Rows[i]) {
			t.Fatalf("Too few columns, result: %v, row: %d, want: %v", qr.Rows[i], i, row)
		}
		for j, val := range row {
			if got := qr.Rows[i][j].ToString(); got != val {
				t.Errorf("Mismatch at (%d, %d): %v, want %s", i, j, qr.Rows[i][j], val)
			}
		}
	}
}
