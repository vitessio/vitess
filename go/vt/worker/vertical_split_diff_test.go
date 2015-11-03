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

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/faketmclient"
	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/query"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
)

// verticalDiffTabletServer is a local QueryService implementation to
// support the tests
type verticalDiffTabletServer struct {
	queryservice.ErrorQueryService
	t             *testing.T
	excludedTable string
}

func (sq *verticalDiffTabletServer) StreamExecute(ctx context.Context, target *pb.Target, query *proto.Query, sendReply func(reply *mproto.QueryResult) error) error {
	if strings.Contains(query.Sql, sq.excludedTable) {
		sq.t.Errorf("Vertical Split Diff operation should skip the excluded table: %v query: %v", sq.excludedTable, query.Sql)
	}

	if hasKeyspace := strings.Contains(query.Sql, "WHERE keyspace_id"); hasKeyspace == true {
		sq.t.Errorf("Sql query for VerticalSplitDiff should never contain a keyspace_id WHERE clause; query received: %v", query.Sql)
	}

	sq.t.Logf("verticalDiffTabletServer: got query: %v", *query)

	// Send the headers
	if err := sendReply(&mproto.QueryResult{
		Fields: []mproto.Field{
			mproto.Field{
				Name: "id",
				Type: mproto.VT_LONGLONG,
			},
			mproto.Field{
				Name: "msg",
				Type: mproto.VT_VAR_STRING,
			},
		},
	}); err != nil {
		return err
	}

	// Send the values
	for i := 0; i < 1000; i++ {
		if err := sendReply(&mproto.QueryResult{
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte(fmt.Sprintf("%v", i))),
					sqltypes.MakeString([]byte(fmt.Sprintf("Text for %v", i))),
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

// TODO(aaijazi): Create a test in which source and destination data does not match

func TestVerticalSplitDiff(t *testing.T) {
	db := fakesqldb.Register()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wi := NewInstance(ts, "cell1", time.Second)
	ctx := context.Background()

	sourceMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 0,
		pbt.TabletType_MASTER, db, testlib.TabletKeyspaceShard(t, "source_ks", "0"))
	sourceRdonly1 := testlib.NewFakeTablet(t, wi.wr, "cell1", 1,
		pbt.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "source_ks", "0"))
	sourceRdonly2 := testlib.NewFakeTablet(t, wi.wr, "cell1", 2,
		pbt.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "source_ks", "0"))

	// Create the destination keyspace with the appropriate ServedFromMap
	ki := &pbt.Keyspace{
		ServedFroms: []*pbt.Keyspace_ServedFrom{
			&pbt.Keyspace_ServedFrom{
				TabletType: pbt.TabletType_MASTER,
				Keyspace:   "source_ks",
			},
			&pbt.Keyspace_ServedFrom{
				TabletType: pbt.TabletType_REPLICA,
				Keyspace:   "source_ks",
			},
			&pbt.Keyspace_ServedFrom{
				TabletType: pbt.TabletType_RDONLY,
				Keyspace:   "source_ks",
			},
		},
	}
	wi.wr.TopoServer().CreateKeyspace(ctx, "destination_ks", ki)

	destMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 10,
		pbt.TabletType_MASTER, db, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))
	destRdonly1 := testlib.NewFakeTablet(t, wi.wr, "cell1", 11,
		pbt.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))
	destRdonly2 := testlib.NewFakeTablet(t, wi.wr, "cell1", 12,
		pbt.TabletType_RDONLY, db, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))

	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2, destMaster, destRdonly1, destRdonly2} {
		ft.StartActionLoop(t, wi.wr)
		defer ft.StopActionLoop(t)
	}

	wi.wr.SetSourceShards(ctx, "destination_ks", "0", []*pbt.TabletAlias{sourceRdonly1.Tablet.Alias}, []string{"moving.*", "view1"})

	// add the topo and schema data we'll need
	if err := wi.wr.RebuildKeyspaceGraph(ctx, "source_ks", nil, true); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}
	if err := wi.wr.RebuildKeyspaceGraph(ctx, "destination_ks", nil, true); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}

	// We need to use FakeTabletManagerClient because we don't
	// have a good way to fake the binlog player yet, which is
	// necessary for synchronizing replication.
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, faketmclient.NewFakeTabletManagerClient())
	excludedTable := "excludedTable1"
	subFlags := flag.NewFlagSet("VerticalSplitDiff", flag.ContinueOnError)
	gwrk, err := commandVerticalSplitDiff(wi, wr, subFlags, []string{
		"-exclude_tables", excludedTable,
		"destination_ks/0",
	})
	if err != nil {
		t.Fatalf("commandVerticalSplitDiff failed: %v", err)
	}
	wrk := gwrk.(*VerticalSplitDiffWorker)

	for _, rdonly := range []*testlib.FakeTablet{sourceRdonly1, sourceRdonly2, destRdonly1, destRdonly2} {
		// both source and destination should be identical (for schema and data returned)
		rdonly.FakeMysqlDaemon.Schema = &myproto.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*myproto.TableDefinition{
				&myproto.TableDefinition{
					Name:              "moving1",
					Columns:           []string{"id", "msg"},
					PrimaryKeyColumns: []string{"id"},
					Type:              myproto.TableBaseTable,
				},
				&myproto.TableDefinition{
					Name:              excludedTable,
					Columns:           []string{"id", "msg"},
					PrimaryKeyColumns: []string{"id"},
					Type:              myproto.TableBaseTable,
				},
				&myproto.TableDefinition{
					Name: "view1",
					Type: myproto.TableView,
				},
			},
		}
		grpcqueryservice.RegisterForTest(rdonly.RPCServer, &verticalDiffTabletServer{t: t, excludedTable: excludedTable})
	}

	err = wrk.Run(ctx)
	status := wrk.StatusAsText()
	t.Logf("Got status: %v", status)
	if err != nil || wrk.State != WorkerStateDone {
		t.Errorf("Worker run failed")
	}
}
