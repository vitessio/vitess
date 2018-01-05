/*
Copyright 2017 Google Inc.

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

package worker

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vttablet/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice/fakes"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// verticalDiffTabletServer is a local QueryService implementation to
// support the tests
type verticalDiffTabletServer struct {
	t *testing.T

	*fakes.StreamHealthQueryService
}

func (sq *verticalDiffTabletServer) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(reply *sqltypes.Result) error) error {
	if !strings.Contains(sql, "moving1") {
		sq.t.Errorf("Vertical Split Diff operation should only operate on the 'moving1' table. query: %v", sql)
	}

	if strings.Contains(sql, "WHERE `keyspace_id`") {
		sq.t.Errorf("Sql query for VerticalSplitDiff should never contain a keyspace_id WHERE clause; query received: %v", sql)
	}

	sq.t.Logf("verticalDiffTabletServer: got query: %v", sql)

	// Send the headers
	if err := callback(&sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: sqltypes.Int64,
			},
			{
				Name: "msg",
				Type: sqltypes.VarChar,
			},
		},
	}); err != nil {
		return err
	}

	// Send the values
	for i := 0; i < 1000; i++ {
		if err := callback(&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{
					sqltypes.NewInt64(int64(i)),
					sqltypes.NewVarBinary(fmt.Sprintf("Text for %v", i)),
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
	ts := memorytopo.NewServer("cell1", "cell2")
	ctx := context.Background()
	wi := NewInstance(ts, "cell1", time.Second)

	sourceMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 0,
		topodatapb.TabletType_MASTER, nil, testlib.TabletKeyspaceShard(t, "source_ks", "0"))
	sourceRdonly1 := testlib.NewFakeTablet(t, wi.wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(t, "source_ks", "0"))
	sourceRdonly2 := testlib.NewFakeTablet(t, wi.wr, "cell1", 2,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(t, "source_ks", "0"))

	// Create the destination keyspace with the appropriate ServedFromMap
	ki := &topodatapb.Keyspace{
		ServedFroms: []*topodatapb.Keyspace_ServedFrom{
			{
				TabletType: topodatapb.TabletType_MASTER,
				Keyspace:   "source_ks",
			},
			{
				TabletType: topodatapb.TabletType_REPLICA,
				Keyspace:   "source_ks",
			},
			{
				TabletType: topodatapb.TabletType_RDONLY,
				Keyspace:   "source_ks",
			},
		},
	}
	wi.wr.TopoServer().CreateKeyspace(ctx, "destination_ks", ki)

	destMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 10,
		topodatapb.TabletType_MASTER, nil, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))
	destRdonly1 := testlib.NewFakeTablet(t, wi.wr, "cell1", 11,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))
	destRdonly2 := testlib.NewFakeTablet(t, wi.wr, "cell1", 12,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(t, "destination_ks", "0"))

	wi.wr.SetSourceShards(ctx, "destination_ks", "0", []*topodatapb.TabletAlias{sourceRdonly1.Tablet.Alias}, []string{"moving.*", "view1"})

	// add the topo and schema data we'll need
	if err := wi.wr.RebuildKeyspaceGraph(ctx, "source_ks", nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}
	if err := wi.wr.RebuildKeyspaceGraph(ctx, "destination_ks", nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}

	for _, rdonly := range []*testlib.FakeTablet{sourceRdonly1, sourceRdonly2, destRdonly1, destRdonly2} {
		// both source and destination have the table definition for 'moving1'.
		// source also has "staying1" while destination has "extra1".
		// (Both additional tables should be ignored by the diff.)
		extraTable := "staying1"
		if rdonly == destRdonly1 || rdonly == destRdonly2 {
			extraTable = "extra1"
		}
		rdonly.FakeMysqlDaemon.Schema = &tabletmanagerdatapb.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:              "moving1",
					Columns:           []string{"id", "msg"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
				{
					Name:              extraTable,
					Columns:           []string{"id", "msg"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
				{
					Name: "view1",
					Type: tmutils.TableView,
				},
			},
		}
		qs := fakes.NewStreamHealthQueryService(rdonly.Target())
		qs.AddDefaultHealthResponse()
		grpcqueryservice.Register(rdonly.RPCServer, &verticalDiffTabletServer{
			t: t,
			StreamHealthQueryService: qs,
		})
	}

	// Start action loop after having registered all RPC services.
	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2, destMaster, destRdonly1, destRdonly2} {
		ft.StartActionLoop(t, wi.wr)
		defer ft.StopActionLoop(t)
	}

	// Run the vtworker command.
	args := []string{"VerticalSplitDiff", "destination_ks/0"}
	// We need to use FakeTabletManagerClient because we don't
	// have a good way to fake the binlog player yet, which is
	// necessary for synchronizing replication.
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, newFakeTMCTopo(ts))
	if err := runCommand(t, wi, wr, args); err != nil {
		t.Fatal(err)
	}
}
