// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestMigrateServedFrom(t *testing.T) {
	ctx := context.Background()
	db := fakesqldb.Register()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// create the source keyspace tablets
	sourceMaster := NewFakeTablet(t, wr, "cell1", 10, topodatapb.TabletType_MASTER, db,
		TabletKeyspaceShard(t, "source", "0"))
	sourceReplica := NewFakeTablet(t, wr, "cell1", 11, topodatapb.TabletType_REPLICA, db,
		TabletKeyspaceShard(t, "source", "0"))
	sourceRdonly := NewFakeTablet(t, wr, "cell1", 12, topodatapb.TabletType_RDONLY, db,
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
	destMaster := NewFakeTablet(t, wr, "cell1", 20, topodatapb.TabletType_MASTER, db,
		TabletKeyspaceShard(t, "dest", "0"))
	destReplica := NewFakeTablet(t, wr, "cell1", 21, topodatapb.TabletType_REPLICA, db,
		TabletKeyspaceShard(t, "dest", "0"))
	destRdonly := NewFakeTablet(t, wr, "cell1", 22, topodatapb.TabletType_RDONLY, db,
		TabletKeyspaceShard(t, "dest", "0"))

	// sourceRdonly will see the refresh
	sourceRdonly.StartActionLoop(t, wr)
	defer sourceRdonly.StopActionLoop(t)

	// sourceReplica will see the refresh
	sourceReplica.StartActionLoop(t, wr)
	defer sourceReplica.StopActionLoop(t)

	// sourceMaster will see the refresh, and has to respond to it
	// also will be asked about its replication position.
	sourceMaster.FakeMysqlDaemon.CurrentMasterPosition = replication.Position{
		GTIDSet: replication.MariadbGTID{
			Domain:   5,
			Server:   456,
			Sequence: 892,
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

	// destMaster will see the refresh, and has to respond to it.
	// It will also need to respond to WaitBlpPosition, saying it's already caught up.
	destMaster.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=0": {
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte(replication.EncodePosition(sourceMaster.FakeMysqlDaemon.CurrentMasterPosition))),
					sqltypes.MakeString([]byte("")),
				},
			},
		},
	}
	destMaster.StartActionLoop(t, wr)
	defer destMaster.StopActionLoop(t)

	// simulate the clone, by fixing the dest shard record
	if err := vp.Run([]string{"SourceShardAdd", "--tables", "gone1,gone2", "dest/0", "0", "source/0"}); err != nil {
		t.Fatalf("SourceShardAdd failed: %v", err)
	}

	// migrate rdonly over
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
	si, err := ts.GetShard(ctx, "source", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	if len(si.TabletControls) != 1 || !reflect.DeepEqual(si.TabletControls, []*topodatapb.Shard_TabletControl{
		{
			TabletType:        topodatapb.TabletType_RDONLY,
			BlacklistedTables: []string{"gone1", "gone2"},
		},
	}) {
		t.Fatalf("rdonly type doesn't have right blacklisted tables")
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
