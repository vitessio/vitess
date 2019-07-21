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

package testlib

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestMigrateServedFrom(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// create the source keyspace tablets
	sourceMaster := NewFakeTablet(t, wr, "cell1", 10, topodatapb.TabletType_MASTER, nil,
		TabletKeyspaceShard(t, "source", "0"))
	sourceReplica := NewFakeTablet(t, wr, "cell1", 11, topodatapb.TabletType_REPLICA, nil,
		TabletKeyspaceShard(t, "source", "0"))
	sourceRdonly := NewFakeTablet(t, wr, "cell1", 12, topodatapb.TabletType_RDONLY, nil,
		TabletKeyspaceShard(t, "source", "0"))

	// create the destination keyspace, served form source
	// double check it has all entries in map
	if err := vp.Run([]string{"CreateKeyspace", "-served_from", "master:source,replica:source,rdonly:source", "dest"}); err != nil {
		t.Fatalf("CreateKeyspace(dest) failed: %v", err)
	}
	ki, err := ts.GetKeyspace(ctx, "dest")
	if err != nil {
		t.Fatalf("GetKeyspace failed: %v", err)
	}
	if len(ki.ServedFroms) != 3 {
		t.Fatalf("bad initial dest ServedFroms: %+v", ki.ServedFroms)
	}

	// create the destination keyspace tablets
	destMaster := NewFakeTablet(t, wr, "cell1", 20, topodatapb.TabletType_MASTER, nil,
		TabletKeyspaceShard(t, "dest", "0"))
	destReplica := NewFakeTablet(t, wr, "cell1", 21, topodatapb.TabletType_REPLICA, nil,
		TabletKeyspaceShard(t, "dest", "0"))
	destRdonly := NewFakeTablet(t, wr, "cell1", 22, topodatapb.TabletType_RDONLY, nil,
		TabletKeyspaceShard(t, "dest", "0"))

	// sourceRdonly will see the refresh
	sourceRdonly.StartActionLoop(t, wr)
	defer sourceRdonly.StopActionLoop(t)

	// sourceReplica will see the refresh
	sourceReplica.StartActionLoop(t, wr)
	defer sourceReplica.StopActionLoop(t)

	// sourceMaster will see the refresh, and has to respond to it
	// also will be asked about its replication position.
	sourceMaster.FakeMysqlDaemon.CurrentMasterPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 892,
			},
		},
	}
	sourceMaster.StartActionLoop(t, wr)
	defer sourceMaster.StopActionLoop(t)

	// destRdonly will see the refresh
	destRdonly.StartActionLoop(t, wr)
	defer destRdonly.StopActionLoop(t)

	// destReplica will see the refresh
	destReplica.StartActionLoop(t, wr)
	defer destReplica.StopActionLoop(t)

	destMaster.StartActionLoop(t, wr)
	defer destMaster.StopActionLoop(t)

	// Override with a fake VREngine after Agent is initialized in action loop.
	dbClient := binlogplayer.NewMockDBClient(t)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	destMaster.Agent.VREngine = vreplication.NewEngine(ts, "", destMaster.FakeMysqlDaemon, dbClientFactory, dbClient.DBName())
	dbClient.ExpectRequest("select * from _vt.vreplication where db_name='db'", &sqltypes.Result{}, nil)
	if err := destMaster.Agent.VREngine.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	// select pos, state, message from _vt.vreplication
	dbClient.ExpectRequest("select pos, state, message from _vt.vreplication where id=1", &sqltypes.Result{Rows: [][]sqltypes.Value{{
		sqltypes.NewVarBinary("MariaDB/5-456-892"),
		sqltypes.NewVarBinary("Running"),
		sqltypes.NewVarBinary(""),
	}}}, nil)
	expectDeleteVRepl(dbClient)

	// simulate the clone, by fixing the dest shard record
	if err := vp.Run([]string{"SourceShardAdd", "--tables", "gone1,gone2", "dest/0", "1", "source/0"}); err != nil {
		t.Fatalf("SourceShardAdd failed: %v", err)
	}

	// migrate rdonly over in a cell
	if err := vp.Run([]string{"MigrateServedFrom", "--cells", "cell1", "dest/0", "rdonly"}); err != nil {
		t.Fatalf("MigrateServedFrom(rdonly) cell2 failed: %v", err)
	}

	// check it's gone from keyspace
	ki, err = ts.GetKeyspace(ctx, "dest")
	if err != nil {
		t.Fatalf("GetKeyspace failed: %v", err)
	}
	if len(ki.ServedFroms) != 2 || ki.GetServedFrom(topodatapb.TabletType_RDONLY) != nil {
		t.Fatalf("bad initial dest ServedFroms: %v", ki.ServedFroms)
	}

	// check the source shard has the right blacklisted tables
	si, err := ts.GetShard(ctx, "source", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	expected := []*topodatapb.Shard_TabletControl{
		{
			TabletType:        topodatapb.TabletType_RDONLY,
			Cells:             []string{"cell1"},
			BlacklistedTables: []string{"gone1", "gone2"},
		},
	}
	if len(si.TabletControls) != 1 || !reflect.DeepEqual(si.TabletControls, expected) {
		t.Fatalf("rdonly type doesn't have right blacklisted tables. Expected: %v, got: %v", expected, si.TabletControls)
	}

	// migrate rdonly reverse cell
	if err := vp.Run([]string{"MigrateServedFrom", "--cells", "cell1", "--reverse", "dest/0", "rdonly"}); err != nil {
		t.Fatalf("MigrateServedFrom(rdonly) cell2 failed: %v", err)
	}

	// check it's gone from keyspace
	ki, err = ts.GetKeyspace(ctx, "dest")
	if err != nil {
		t.Fatalf("GetKeyspace failed: %v", err)
	}
	if len(ki.ServedFroms) != 3 {
		t.Fatalf("bad initial dest ServedFroms: %v", ki.ServedFroms)
	}

	// check the source shard has the right blacklisted tables
	si, err = ts.GetShard(ctx, "source", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	if len(si.TabletControls) != 0 {
		t.Fatalf("rdonly type doesn't have right blacklisted tables. Expected: nil, got: %v", si.TabletControls)
	}

	// Now migrate rdonly over
	if err := vp.Run([]string{"MigrateServedFrom", "dest/0", "rdonly"}); err != nil {
		t.Fatalf("MigrateServedFrom(rdonly) failed: %v", err)
	}

	// check it's gone from keyspace
	ki, err = ts.GetKeyspace(ctx, "dest")
	if err != nil {
		t.Fatalf("GetKeyspace failed: %v", err)
	}
	if len(ki.ServedFroms) != 2 || ki.GetServedFrom(topodatapb.TabletType_RDONLY) != nil {
		t.Fatalf("bad initial dest ServedFroms: %v", ki.ServedFroms)
	}

	// check the source shard has the right blacklisted tables
	si, err = ts.GetShard(ctx, "source", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	expected = []*topodatapb.Shard_TabletControl{
		{
			TabletType:        topodatapb.TabletType_RDONLY,
			BlacklistedTables: []string{"gone1", "gone2"},
		},
	}
	if len(si.TabletControls) != 1 || !reflect.DeepEqual(si.TabletControls, expected) {
		t.Fatalf("rdonly type doesn't have right blacklisted tables. Expected: %v, got: %v", expected, si.TabletControls)
	}

	// migrate replica over
	if err := vp.Run([]string{"MigrateServedFrom", "dest/0", "replica"}); err != nil {
		t.Fatalf("MigrateServedFrom(replica) failed: %v", err)
	}

	// check it's gone from keyspace
	ki, err = ts.GetKeyspace(ctx, "dest")
	if err != nil {
		t.Fatalf("GetKeyspace failed: %v", err)
	}
	if len(ki.ServedFroms) != 1 || ki.GetServedFrom(topodatapb.TabletType_REPLICA) != nil {
		t.Fatalf("bad initial dest ServedFrom: %+v", ki.ServedFroms)
	}

	// check the source shard has the right blacklisted tables
	si, err = ts.GetShard(ctx, "source", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	if len(si.TabletControls) != 2 || !reflect.DeepEqual(si.TabletControls, []*topodatapb.Shard_TabletControl{
		{
			TabletType:        topodatapb.TabletType_RDONLY,
			BlacklistedTables: []string{"gone1", "gone2"},
		},
		{
			TabletType:        topodatapb.TabletType_REPLICA,
			BlacklistedTables: []string{"gone1", "gone2"},
		},
	}) {
		t.Fatalf("replica type doesn't have right blacklisted tables")
	}

	// migrate master over
	if err := vp.Run([]string{"MigrateServedFrom", "dest/0", "master"}); err != nil {
		t.Fatalf("MigrateServedFrom(master) failed: %v", err)
	}

	// make sure ServedFromMap is empty
	ki, err = ts.GetKeyspace(ctx, "dest")
	if err != nil {
		t.Fatalf("GetKeyspace failed: %v", err)
	}
	if len(ki.ServedFroms) > 0 {
		t.Fatalf("dest keyspace still is ServedFrom: %+v", ki.ServedFroms)
	}

	// check the source shard has the right blacklisted tables
	si, err = ts.GetShard(ctx, "source", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	if len(si.TabletControls) != 3 || !reflect.DeepEqual(si.TabletControls, []*topodatapb.Shard_TabletControl{
		{
			TabletType:        topodatapb.TabletType_RDONLY,
			BlacklistedTables: []string{"gone1", "gone2"},
		},
		{
			TabletType:        topodatapb.TabletType_REPLICA,
			BlacklistedTables: []string{"gone1", "gone2"},
		},
		{
			TabletType:        topodatapb.TabletType_MASTER,
			BlacklistedTables: []string{"gone1", "gone2"},
		},
	}) {
		t.Fatalf("master type doesn't have right blacklisted tables")
	}
}
