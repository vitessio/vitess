// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"strings"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/faketmclient"
	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	_ "github.com/youtube/vitess/go/vt/tabletserver/gorpctabletconn"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"
)

// This is a local SqlQuery RPC implementation to support the tests
type DestinationSqlQuery struct {
	t *testing.T
}

func (sq *DestinationSqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	return nil
}

func (sq *DestinationSqlQuery) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(reply interface{}) error) error {

	if hasKeyspace := strings.Contains(query.Sql, "WHERE keyspace_id"); hasKeyspace == true {
		sq.t.Errorf("Sql query on destination should not contain a keyspace_id WHERE clause; query received: %v", query.Sql)
	}

	sq.t.Logf("DestinationSqlQuery: got query: %v", *query)

	// Send the headers
	if err := sendReply(&mproto.QueryResult{
		Fields: []mproto.Field{
			mproto.Field{
				Name: "id",
				Type: mproto.VT_LONGLONG,
			},
			mproto.Field{
				Name: "msg",
				Type: mproto.VT_VARCHAR,
			},
			mproto.Field{
				Name: "keyspace_id",
				Type: mproto.VT_LONGLONG,
			},
		},
	}); err != nil {
		return err
	}

	// Send the values
	ksids := []uint64{0x2000000000000000, 0x6000000000000000}
	for i := 0; i < 100; i++ {
		if err := sendReply(&mproto.QueryResult{
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("Text for %v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", ksids[i%2]))),
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

type SourceSqlQuery struct {
	t *testing.T
}

func (sq *SourceSqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	return nil
}

func (sq *SourceSqlQuery) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(reply interface{}) error) error {

	// we test for a keyspace_id where clause, except for on views.
	if !strings.Contains(query.Sql, "view") {
		if hasKeyspace := strings.Contains(query.Sql, "WHERE keyspace_id < 0x4000000000000000"); hasKeyspace != true {
			sq.t.Errorf("Sql query on source should contain a keyspace_id WHERE clause; query received: %v", query.Sql)
		}
	}

	sq.t.Logf("SourceSqlQuery: got query: %v", *query)

	// Send the headers
	if err := sendReply(&mproto.QueryResult{
		Fields: []mproto.Field{
			mproto.Field{
				Name: "id",
				Type: mproto.VT_LONGLONG,
			},
			mproto.Field{
				Name: "msg",
				Type: mproto.VT_VARCHAR,
			},
			mproto.Field{
				Name: "keyspace_id",
				Type: mproto.VT_LONGLONG,
			},
		},
	}); err != nil {
		return err
	}

	// Send the values
	ksids := []uint64{0x2000000000000000, 0x6000000000000000}
	for i := 0; i < 100; i++ {
		if err := sendReply(&mproto.QueryResult{
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("Text for %v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", ksids[i%2]))),
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

// TODO(aaijazi): Create a test in which source and destination data does not match

func TestSplitDiff(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	// We need to use FakeTabletManagerClient because we don't have a good way to fake the binlog player yet,
	// which is necessary for synchronizing replication.
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, faketmclient.NewFakeTabletManagerClient(), time.Second)
	ctx := context.Background()

	sourceMaster := testlib.NewFakeTablet(t, wr, "cell1", 0,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly1 := testlib.NewFakeTablet(t, wr, "cell1", 1,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "ks", "-80"),
		testlib.TabletParent(sourceMaster.Tablet.Alias))
	sourceRdonly2 := testlib.NewFakeTablet(t, wr, "cell1", 2,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "ks", "-80"),
		testlib.TabletParent(sourceMaster.Tablet.Alias))

	leftMaster := testlib.NewFakeTablet(t, wr, "cell1", 10,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "ks", "-40"))
	leftRdonly1 := testlib.NewFakeTablet(t, wr, "cell1", 11,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "ks", "-40"),
		testlib.TabletParent(leftMaster.Tablet.Alias))
	leftRdonly2 := testlib.NewFakeTablet(t, wr, "cell1", 12,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "ks", "-40"),
		testlib.TabletParent(leftMaster.Tablet.Alias))

	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2, leftMaster, leftRdonly1, leftRdonly2} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	// add the topo and schema data we'll need
	if err := topo.CreateShard(ts, "ks", "80-"); err != nil {
		t.Fatalf("CreateShard(\"-80\") failed: %v", err)
	}
	wr.SetSourceShards(ctx, "ks", "-40", []topo.TabletAlias{sourceRdonly1.Tablet.Alias}, nil)
	if err := wr.SetKeyspaceShardingInfo(ctx, "ks", "keyspace_id", key.KIT_UINT64, 4, false); err != nil {
		t.Fatalf("SetKeyspaceShardingInfo failed: %v", err)
	}
	if err := wr.RebuildKeyspaceGraph(ctx, "ks", nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}

	gwrk := NewSplitDiffWorker(wr, "cell1", "ks", "-40")
	wrk := gwrk.(*SplitDiffWorker)

	for _, rdonly := range []*testlib.FakeTablet{sourceRdonly1, sourceRdonly2, leftRdonly1, leftRdonly2} {
		// In reality, the destinations *shouldn't* have identical data to the source - instead, we should see
		// the data split into left and right. However, if we do that in this test, we would really just be
		// testing our fake SQL logic, since we do the data filtering in SQL.
		// To simplify things, just assume that both sides have identical data.
		rdonly.FakeMysqlDaemon.Schema = &myproto.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*myproto.TableDefinition{
				&myproto.TableDefinition{
					Name:              "table1",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              myproto.TABLE_BASE_TABLE,
				},
				&myproto.TableDefinition{
					Name: "view1",
					Type: myproto.TABLE_VIEW,
				},
			},
		}
	}

	leftRdonly1.RPCServer.RegisterName("SqlQuery", &DestinationSqlQuery{t: t})
	leftRdonly2.RPCServer.RegisterName("SqlQuery", &DestinationSqlQuery{t: t})
	sourceRdonly1.RPCServer.RegisterName("SqlQuery", &SourceSqlQuery{t: t})
	sourceRdonly2.RPCServer.RegisterName("SqlQuery", &SourceSqlQuery{t: t})

	wrk.Run()
	status := wrk.StatusAsText()
	t.Logf("Got status: %v", status)
	if wrk.err != nil || wrk.state != stateSCDone {
		t.Errorf("Worker run failed")
	}
}
