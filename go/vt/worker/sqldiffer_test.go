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

// This is a local SqlQuery RPC implementation to support the tests
type SqlDifferSqlQuery struct {
	t *testing.T
}

func (sq *SqlDifferSqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	return nil
}

func (sq *SqlDifferSqlQuery) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(reply interface{}) error) error {
	sq.t.Logf("SqlDifferSqlQuery: got query: %v", *query)

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
// TODO(aaijazi): This test is reallly slow; investigate why.
func TestSqlDiffer(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	// We need to use FakeTabletManagerClient because we don't have a good way to fake the binlog player yet,
	// which is necessary for synchronizing replication.
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, faketmclient.NewFakeTabletManagerClient(), time.Second)
	ctx := context.Background()

	supersetMaster := testlib.NewFakeTablet(t, wr, "cell1", 0,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "source_ks", "0"))
	supersetRdonly1 := testlib.NewFakeTablet(t, wr, "cell1", 1,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "source_ks", "0"),
		testlib.TabletParent(supersetMaster.Tablet.Alias))
	supersetRdonly2 := testlib.NewFakeTablet(t, wr, "cell1", 2,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "source_ks", "0"),
		testlib.TabletParent(supersetMaster.Tablet.Alias))

	subsetMaster := testlib.NewFakeTablet(t, wr, "cell1", 10,
		topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))
	subsetRdonly1 := testlib.NewFakeTablet(t, wr, "cell1", 11,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "destination_ks", "0"),
		testlib.TabletParent(subsetMaster.Tablet.Alias))
	subsetRdonly2 := testlib.NewFakeTablet(t, wr, "cell1", 12,
		topo.TYPE_RDONLY, testlib.TabletKeyspaceShard(t, "destination_ks", "0"),
		testlib.TabletParent(subsetMaster.Tablet.Alias))

	for _, ft := range []*testlib.FakeTablet{supersetMaster, supersetRdonly1, supersetRdonly2, subsetMaster, subsetRdonly1, subsetRdonly2} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	wr.SetSourceShards(ctx, "destination_ks", "0", []topo.TabletAlias{supersetRdonly1.Tablet.Alias}, []string{"moving.*", "view1"})

	// add the topo and schema data we'll need
	if err := wr.RebuildKeyspaceGraph(ctx, "source_ks", nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}
	if err := wr.RebuildKeyspaceGraph(ctx, "destination_ks", nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}

	supersetSourceSpec := SourceSpec{"source_ks", "0", "SELECT *", supersetRdonly1.Tablet.Alias}
	subsetSourceSpec := SourceSpec{"destination_ks", "0", "SELECT *", subsetRdonly1.Tablet.Alias}

	gwrk := NewSQLDiffWorker(wr, "cell1", supersetSourceSpec, subsetSourceSpec)
	wrk := gwrk.(*SQLDiffWorker)

	for _, rdonly := range []*testlib.FakeTablet{supersetRdonly1, supersetRdonly2, subsetRdonly1, subsetRdonly2} {
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
		rdonly.RPCServer.RegisterName("SqlQuery", &SqlDifferSqlQuery{t: t})
	}

	wrk.Run()
	status := wrk.StatusAsText()
	t.Logf("Got status: %v", status)
	if wrk.err != nil || wrk.state != stateSCDone {
		t.Errorf("Worker run failed")
	}
}
