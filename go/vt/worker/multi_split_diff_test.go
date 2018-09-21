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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/wrangler"
	"vitess.io/vitess/go/vt/wrangler/testlib"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// msdDestinationTabletServer is a local QueryService implementation to
// support the tests
type msdDestinationTabletServer struct {
	t *testing.T

	*fakes.StreamHealthQueryService
	excludedTable string
	shardIndex    int
}

func (sq *msdDestinationTabletServer) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(reply *sqltypes.Result) error) error {
	if strings.Contains(sql, sq.excludedTable) {
		sq.t.Errorf("Split Diff operation on destination should skip the excluded table: %v query: %v", sq.excludedTable, sql)
	}

	if hasKeyspace := strings.Contains(sql, "WHERE `keyspace_id`"); hasKeyspace == true {
		sq.t.Errorf("Sql query on destination should not contain a keyspace_id WHERE clause; query received: %v", sql)
	}

	sq.t.Logf("msdDestinationTabletServer: got query: %v", sql)

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
			{
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
		// skip the out-of-range values
		if i%2 == sq.shardIndex {
			continue
		}
		if err := callback(&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{
					sqltypes.NewVarBinary(fmt.Sprintf("%v", i)),
					sqltypes.NewVarBinary(fmt.Sprintf("Text for %v", i)),
					sqltypes.NewVarBinary(fmt.Sprintf("%v", ksids[i%2])),
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

// msdSourceTabletServer is a local QueryService implementation to support the tests
type msdSourceTabletServer struct {
	t *testing.T

	*fakes.StreamHealthQueryService
	excludedTable string
	v3            bool
}

func (sq *msdSourceTabletServer) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(reply *sqltypes.Result) error) error {
	if strings.Contains(sql, sq.excludedTable) {
		sq.t.Errorf("Split Diff operation on source should skip the excluded table: %v query: %v", sq.excludedTable, sql)
	}

	// we test for a keyspace_id where clause, except for v3
	if !sq.v3 {
		if hasKeyspace := strings.Contains(sql, "WHERE `keyspace_id` < 4611686018427387904"); hasKeyspace != true {
			sq.t.Errorf("Sql query on source should contain a keyspace_id WHERE clause; query received: %v", sql)
		}
	}

	sq.t.Logf("msdSourceTabletServer: got query: %v", sql)

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
			{
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
		if !sq.v3 && i%2 == 1 {
			// for v2, filtering is done at SQL layer
			continue
		}
		if err := callback(&sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{
					sqltypes.NewVarBinary(fmt.Sprintf("%v", i)),
					sqltypes.NewVarBinary(fmt.Sprintf("Text for %v", i)),
					sqltypes.NewVarBinary(fmt.Sprintf("%v", ksids[i%2])),
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

// TODO(aaijazi): Create a test in which source and destination data does not match

func testMultiSplitDiff(t *testing.T, v3 bool, destinationTabletType topodatapb.TabletType) {
	*useV3ReshardingMode = v3
	ts := memorytopo.NewServer("cell1", "cell2")
	ctx := context.Background()
	wi := NewInstance(ts, "cell1", time.Second)

	if v3 {
		if err := ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}); err != nil {
			t.Fatalf("CreateKeyspace v3 failed: %v", err)
		}

		vs := &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"table1_index": {
					Type: "numeric",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"table1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{
						{
							Column: "keyspace_id",
							Name:   "table1_index",
						},
					},
				},
			},
		}
		if err := ts.SaveVSchema(ctx, "ks", vs); err != nil {
			t.Fatalf("SaveVSchema v3 failed: %v", err)
		}
	} else {
		if err := ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{
			ShardingColumnName: "keyspace_id",
			ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
		}); err != nil {
			t.Fatalf("CreateKeyspace failed: %v", err)
		}
	}

	sourceMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 0,
		topodatapb.TabletType_MASTER, nil, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly1 := testlib.NewFakeTablet(t, wi.wr, "cell1", 1,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	sourceRdonly2 := testlib.NewFakeTablet(t, wi.wr, "cell1", 2,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(t, "ks", "-80"))

	leftMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 10,
		topodatapb.TabletType_MASTER, nil, testlib.TabletKeyspaceShard(t, "ks", "-40"))
	leftRdonly1 := testlib.NewFakeTablet(t, wi.wr, "cell1", 11,
		destinationTabletType, nil, testlib.TabletKeyspaceShard(t, "ks", "-40"))
	leftRdonly2 := testlib.NewFakeTablet(t, wi.wr, "cell1", 12,
		destinationTabletType, nil, testlib.TabletKeyspaceShard(t, "ks", "-40"))

	rightMaster := testlib.NewFakeTablet(t, wi.wr, "cell1", 20,
		topodatapb.TabletType_MASTER, nil, testlib.TabletKeyspaceShard(t, "ks", "40-80"))
	rightRdonly1 := testlib.NewFakeTablet(t, wi.wr, "cell1", 21,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(t, "ks", "40-80"))
	rightRdonly2 := testlib.NewFakeTablet(t, wi.wr, "cell1", 22,
		topodatapb.TabletType_RDONLY, nil, testlib.TabletKeyspaceShard(t, "ks", "40-80"))

	// add the topo and schema data we'll need
	if err := ts.CreateShard(ctx, "ks", "80-"); err != nil {
		t.Fatalf("CreateShard(\"-80\") failed: %v", err)
	}
	wi.wr.SetSourceShards(ctx, "ks", "-40", []*topodatapb.TabletAlias{sourceRdonly1.Tablet.Alias}, nil)
	if err := wi.wr.SetKeyspaceShardingInfo(ctx, "ks", "keyspace_id", topodatapb.KeyspaceIdType_UINT64, false); err != nil {
		t.Fatalf("SetKeyspaceShardingInfo failed: %v", err)
	}
	wi.wr.SetSourceShards(ctx, "ks", "40-80", []*topodatapb.TabletAlias{sourceRdonly1.Tablet.Alias}, nil)
	if err := wi.wr.SetKeyspaceShardingInfo(ctx, "ks", "keyspace_id", topodatapb.KeyspaceIdType_UINT64, false); err != nil {
		t.Fatalf("SetKeyspaceShardingInfo failed: %v", err)
	}
	if err := wi.wr.RebuildKeyspaceGraph(ctx, "ks", nil); err != nil {
		t.Fatalf("RebuildKeyspaceGraph failed: %v", err)
	}

	excludedTable := "excludedTable1"

	for _, rdonly := range []*testlib.FakeTablet{sourceRdonly1, sourceRdonly2, leftRdonly1, leftRdonly2, rightRdonly1, rightRdonly2} {
		// The destination only has half the data.
		// For v2, we do filtering at the SQL level.
		// For v3, we do it in the client.
		// So in any case, we need real data.
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
			},
		}
	}

	for _, sourceRdonly := range []*testlib.FakeTablet{sourceRdonly1, sourceRdonly2} {
		qs := fakes.NewStreamHealthQueryService(sourceRdonly.Target())
		qs.AddDefaultHealthResponse()
		grpcqueryservice.Register(sourceRdonly.RPCServer, &msdSourceTabletServer{
			t: t,
			StreamHealthQueryService: qs,
			excludedTable:            excludedTable,
			v3:                       v3,
		})
	}

	for _, destRdonly := range []*testlib.FakeTablet{leftRdonly1, leftRdonly2} {
		qs := fakes.NewStreamHealthQueryService(destRdonly.Target())
		qs.AddDefaultHealthResponse()
		grpcqueryservice.Register(destRdonly.RPCServer, &msdDestinationTabletServer{
			t: t,
			StreamHealthQueryService: qs,
			excludedTable:            excludedTable,
			shardIndex:               0,
		})
	}

	for _, destRdonly := range []*testlib.FakeTablet{rightRdonly1, rightRdonly2} {
		qs := fakes.NewStreamHealthQueryService(destRdonly.Target())
		qs.AddDefaultHealthResponse()
		grpcqueryservice.Register(destRdonly.RPCServer, &msdDestinationTabletServer{
			t: t,
			StreamHealthQueryService: qs,
			excludedTable:            excludedTable,
			shardIndex:               1,
		})
	}

	// Start action loop after having registered all RPC services.
	for _, ft := range []*testlib.FakeTablet{sourceMaster, sourceRdonly1, sourceRdonly2, leftMaster, leftRdonly1, leftRdonly2, rightMaster, rightRdonly1, rightRdonly2} {
		ft.StartActionLoop(t, wi.wr)
		defer ft.StopActionLoop(t)
	}

	tabletTypeName, _ := topodatapb.TabletType_name[int32(destinationTabletType)]
	// Run the vtworker command.
	args := []string{
		"MultiSplitDiff",
		"-exclude_tables", excludedTable,
		"-dest_tablet_type", tabletTypeName,
		"ks/-80",
	}
	// We need to use FakeTabletManagerClient because we don't
	// have a good way to fake the binlog player yet, which is
	// necessary for synchronizing replication.
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, newFakeTMCTopo(ts))
	if err := runCommand(t, wi, wr, args); err != nil {
		t.Fatal(err)
	}
}

func TestMultiSplitDiffv2(t *testing.T) {
	testMultiSplitDiff(t, false, topodatapb.TabletType_RDONLY)
}

func TestMultiSplitDiffv3(t *testing.T) {
	testMultiSplitDiff(t, true, topodatapb.TabletType_RDONLY)
}

func TestMultiSplitDiffWithReplica(t *testing.T) {
	testMultiSplitDiff(t, true, topodatapb.TabletType_REPLICA)
}
