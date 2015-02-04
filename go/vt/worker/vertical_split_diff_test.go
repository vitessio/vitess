// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
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

// This is a local VerticalDiffSqlQuery RPC implementation to support the tests
type VerticalDiffSqlQuery struct {
	t *testing.T
}

func (sq *VerticalDiffSqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	return nil
}

func (sq *VerticalDiffSqlQuery) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(reply interface{}) error) error {
	sq.t.Logf("VerticalDiffSqlQuery: got query: %v", *query)

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
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	// We need to use FakeTabletManagerClient because we don't have a good way to fake the binlog player yet,
	// which is necessary for synchronizing replication.
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, faketmclient.NewFakeTabletManagerClient(), time.Second)
	ctx := context.Background()

	sourceMaster := testlib.NewFakeTablet(t, wr, "cell1", 0,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "source_ks", "0"))
	sourceRdonly1 := testlib.NewFakeTablet(t, wr, "cell1", 1,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "source_ks", "0"),
		testlib.TabletParent(sourceMaster.Tablet.Alias))
	sourceRdonly2 := testlib.NewFakeTablet(t, wr, "cell1", 2,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "source_ks", "0"),
		testlib.TabletParent(sourceMaster.Tablet.Alias))

	// Create the destination keyspace with the appropriate ServedFromMap
	ki := &topo.Keyspace{}
	ki.ServedFromMap = map[topo.TabletType]*topo.KeyspaceServedFrom{
		topo.TYPE_MASTER:  &topo.KeyspaceServedFrom{Keyspace: "source_ks"},
		topo.TYPE_REPLICA: &topo.KeyspaceServedFrom{Keyspace: "source_ks"},
		topo.TYPE_RDONLY:  &topo.KeyspaceServedFrom{Keyspace: "source_ks"},
	}
	wr.TopoServer().CreateKeyspace("destination_ks", ki)

	destMaster := testlib.NewFakeTablet(t, wr, "cell1", 10,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))
	destRdonly1 := testlib.NewFakeTablet(t, wr, "cell1", 11,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "destination_ks", "0"),
		testlib.TabletParent(destMaster.Tablet.Alias))
	destRdonly2 := testlib.NewFakeTablet(t, wr, "cell1", 12,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "destination_ks", "0"),
		testlib.TabletParent(destMaster.Tablet.Alias))

	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2, destMaster, destRdonly1, destRdonly2} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	wr.SetSourceShards(ctx, "destination_ks", "0", []topo.TabletAlias{sourceRdonly1.Tablet.Alias}, []string{"moving.*", "view1"})

	// add the topo and schema data we'll need
	if err := wr.RebuildKeyspaceGraph(ctx, "source_ks", nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}
	if err := wr.RebuildKeyspaceGraph(ctx, "destination_ks", nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}

	gwrk := NewVerticalSplitDiffWorker(wr, "cell1", "destination_ks", "0")
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
					Type:              myproto.TABLE_BASE_TABLE,
				},
				&myproto.TableDefinition{
					Name: "view1",
					Type: myproto.TABLE_VIEW,
				},
			},
		}
		rdonly.RPCServer.RegisterName("SqlQuery", &VerticalDiffSqlQuery{t: t})
	}

	wrk.Run()
	status := wrk.StatusAsText()
	t.Logf("Got status: %v", status)
	if wrk.err != nil || wrk.state != stateSCDone {
		t.Errorf("Worker run failed")
	}
}
