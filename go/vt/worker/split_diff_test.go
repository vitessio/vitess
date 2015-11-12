// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/tabletmanager/faketmclient"
	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// destinationTabletServer is a local QueryService implementation to
// support the tests
type destinationTabletServer struct {
	queryservice.ErrorQueryService
	t             *testing.T
	excludedTable string
}

func (sq *destinationTabletServer) StreamExecute(ctx context.Context, target *querypb.Target, query *proto.Query, sendReply func(reply *sqltypes.Result) error) error {
	if strings.Contains(query.Sql, sq.excludedTable) {
		sq.t.Errorf("Split Diff operation on destination should skip the excluded table: %v query: %v", sq.excludedTable, query.Sql)
	}

	if hasKeyspace := strings.Contains(query.Sql, "WHERE keyspace_id"); hasKeyspace == true {
		sq.t.Errorf("Sql query on destination should not contain a keyspace_id WHERE clause; query received: %v", query.Sql)
	}

	sq.t.Logf("destinationTabletServer: got query: %v", *query)

	// Send the headers
	if err := sendReply(&sqltypes.Result{
		Fields: []*querypb.Field{
			&querypb.Field{
				Name: "id",
				Type: sqltypes.Int64,
			},
			&querypb.Field{
				Name: "msg",
				Type: sqltypes.VarChar,
			},
			&querypb.Field{
				Name: "keyspace_id",
				Type: sqltypes.Int64,
			},
		},
	}); err != nil {
		return err
	}

	// Send the values
	ksids := []uint64{0x2000000000000000, 0x6000000000000000}
	for i := 0; i < 100; i++ {
		if err := sendReply(&sqltypes.Result{
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

// sourceTabletServer is a local QueryService implementation to support the tests
type sourceTabletServer struct {
	queryservice.ErrorQueryService
	t             *testing.T
	excludedTable string
}

func (sq *sourceTabletServer) StreamExecute(ctx context.Context, target *querypb.Target, query *proto.Query, sendReply func(reply *sqltypes.Result) error) error {
	if strings.Contains(query.Sql, sq.excludedTable) {
		sq.t.Errorf("Split Diff operation on source should skip the excluded table: %v query: %v", sq.excludedTable, query.Sql)
	}

	// we test for a keyspace_id where clause, except for on views.
	if !strings.Contains(query.Sql, "view") {
		if hasKeyspace := strings.Contains(query.Sql, "WHERE keyspace_id < 0x4000000000000000"); hasKeyspace != true {
			sq.t.Errorf("Sql query on source should contain a keyspace_id WHERE clause; query received: %v", query.Sql)
		}
	}

	sq.t.Logf("sourceTabletServer: got query: %v", *query)

	// Send the headers
	if err := sendReply(&sqltypes.Result{
		Fields: []*querypb.Field{
			&querypb.Field{
				Name: "id",
				Type: sqltypes.Int64,
			},
			&querypb.Field{
				Name: "msg",
				Type: sqltypes.VarChar,
			},
			&querypb.Field{
				Name: "keyspace_id",
				Type: sqltypes.Int64,
			},
		},
	}); err != nil {
		return err
	}

	// Send the values
	ksids := []uint64{0x2000000000000000, 0x6000000000000000}
	for i := 0; i < 100; i++ {
		if err := sendReply(&sqltypes.Result{
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
	db := fakesqldb.Register()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	ctx := context.Background()
	wi := NewInstance(ctx, ts, "cell1", time.Second)

	if err := ts.CreateKeyspace(context.Background(), "ks", &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	sourceMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 0,
		topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly1 := testlib.NewFakeTablet(t, wi.wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly2 := testlib.NewFakeTablet(t, wi.wr, "cell1", 2,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "ks", "-80"))

	leftMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 10,
		topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(t, "ks", "-40"))
	leftRdonly1 := testlib.NewFakeTablet(t, wi.wr, "cell1", 11,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "ks", "-40"))
	leftRdonly2 := testlib.NewFakeTablet(t, wi.wr, "cell1", 12,
		topodatapb.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "ks", "-40"))

	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2, leftMaster, leftRdonly1, leftRdonly2} {
		ft.StartActionLoop(t, wi.wr)
		defer ft.StopActionLoop(t)
	}

	// add the topo and schema data we'll need
	if err := ts.CreateShard(ctx, "ks", "80-"); err != nil {
		t.Fatalf("CreateShard(\"-80\") failed: %v", err)
	}
	wi.wr.SetSourceShards(ctx, "ks", "-40", []*topodatapb.TabletAlias{sourceRdonly1.Tablet.Alias}, nil)
	if err := wi.wr.SetKeyspaceShardingInfo(ctx, "ks", "keyspace_id", topodatapb.KeyspaceIdType_UINT64, 4, false); err != nil {
		t.Fatalf("SetKeyspaceShardingInfo failed: %v", err)
	}
	if err := wi.wr.RebuildKeyspaceGraph(ctx, "ks", nil, true); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}

	subFlags := flag.NewFlagSet("SplitDiff", flag.ContinueOnError)
	// We need to use FakeTabletManagerClient because we don't
	// have a good way to fake the binlog player yet, which is
	// necessary for synchronizing replication.
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, faketmclient.NewFakeTabletManagerClient())
	excludedTable := "excludedTable1"
	gwrk, err := commandSplitDiff(wi, wr, subFlags, []string{
		"-exclude_tables", excludedTable,
		"ks/-40",
	})
	if err != nil {
		t.Fatalf("commandSplitDiff failed: %v", err)
	}
	wrk := gwrk.(*SplitDiffWorker)

	for _, rdonly := range []*testlib.FakeTablet{sourceRdonly1, sourceRdonly2, leftRdonly1, leftRdonly2} {
		// In reality, the destinations *shouldn't* have identical data to the source - instead, we should see
		// the data split into left and right. However, if we do that in this test, we would really just be
		// testing our fake SQL logic, since we do the data filtering in SQL.
		// To simplify things, just assume that both sides have identical data.
		rdonly.FakeMysqlDaemon.Schema = &tabletmanagerdatapb.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:              "table1",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
				{
					Name:              excludedTable,
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
				{
					Name: "view1",
					Type: tmutils.TableView,
				},
			},
		}
	}

	grpcqueryservice.RegisterForTest(leftRdonly1.RPCServer, &destinationTabletServer{t: t, excludedTable: excludedTable})
	grpcqueryservice.RegisterForTest(leftRdonly2.RPCServer, &destinationTabletServer{t: t, excludedTable: excludedTable})
	grpcqueryservice.RegisterForTest(sourceRdonly1.RPCServer, &sourceTabletServer{t: t, excludedTable: excludedTable})
	grpcqueryservice.RegisterForTest(sourceRdonly2.RPCServer, &sourceTabletServer{t: t, excludedTable: excludedTable})

	err = wrk.Run(ctx)
	status := wrk.StatusAsText()
	t.Logf("Got status: %v", status)
	if err != nil || wrk.State != WorkerStateDone {
		t.Errorf("Worker run failed")
	}
}
