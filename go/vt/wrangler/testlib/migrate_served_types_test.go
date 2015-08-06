// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"
)

func checkShardServedTypes(t *testing.T, ts topo.Server, shard string, expected int) {
	ctx := context.Background()
	si, err := ts.GetShard(ctx, "ks", shard)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	if len(si.ServedTypes) != expected {
		t.Fatalf("shard %v has wrong served types: %#v", shard, si.ServedTypes)
	}
}

func TestMigrateServedTypes(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// create the source shard
	sourceMaster := NewFakeTablet(t, wr, "cell1", 10, topo.TYPE_MASTER,
		TabletKeyspaceShard(t, "ks", "0"))
	sourceReplica := NewFakeTablet(t, wr, "cell1", 11, topo.TYPE_REPLICA,
		TabletKeyspaceShard(t, "ks", "0"))
	sourceRdonly := NewFakeTablet(t, wr, "cell1", 12, topo.TYPE_RDONLY,
		TabletKeyspaceShard(t, "ks", "0"))

	// create the first destination shard
	dest1Master := NewFakeTablet(t, wr, "cell1", 20, topo.TYPE_MASTER,
		TabletKeyspaceShard(t, "ks", "-80"))
	dest1Replica := NewFakeTablet(t, wr, "cell1", 21, topo.TYPE_REPLICA,
		TabletKeyspaceShard(t, "ks", "-80"))
	dest1Rdonly := NewFakeTablet(t, wr, "cell1", 22, topo.TYPE_RDONLY,
		TabletKeyspaceShard(t, "ks", "-80"))

	// create the second destination shard
	dest2Master := NewFakeTablet(t, wr, "cell1", 30, topo.TYPE_MASTER,
		TabletKeyspaceShard(t, "ks", "80-"))
	dest2Replica := NewFakeTablet(t, wr, "cell1", 31, topo.TYPE_REPLICA,
		TabletKeyspaceShard(t, "ks", "80-"))
	dest2Rdonly := NewFakeTablet(t, wr, "cell1", 32, topo.TYPE_RDONLY,
		TabletKeyspaceShard(t, "ks", "80-"))

	// double check the shards have the right served types
	checkShardServedTypes(t, ts, "0", 3)
	checkShardServedTypes(t, ts, "-80", 0)
	checkShardServedTypes(t, ts, "80-", 0)

	// sourceRdonly will see the refresh
	sourceRdonly.StartActionLoop(t, wr)
	defer sourceRdonly.StopActionLoop(t)

	// sourceReplica will see the refresh
	sourceReplica.StartActionLoop(t, wr)
	defer sourceReplica.StopActionLoop(t)

	// sourceMaster will see the refresh, and has to respond to it
	// also will be asked about its replication position.
	sourceMaster.FakeMysqlDaemon.CurrentMasterPosition = myproto.ReplicationPosition{
		GTIDSet: myproto.MariadbGTID{
			Domain:   5,
			Server:   456,
			Sequence: 892,
		},
	}
	sourceMaster.StartActionLoop(t, wr)
	defer sourceMaster.StopActionLoop(t)

	// dest1Rdonly will see the refresh
	dest1Rdonly.StartActionLoop(t, wr)
	defer dest1Rdonly.StopActionLoop(t)

	// dest1Replica will see the refresh
	dest1Replica.StartActionLoop(t, wr)
	defer dest1Replica.StopActionLoop(t)

	// dest1Master will see the refresh, and has to respond to it.
	// It will also need to respond to WaitBlpPosition, saying it's already caught up.
	dest1Master.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*mproto.QueryResult{
		"SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=0": &mproto.QueryResult{
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte(myproto.EncodeReplicationPosition(sourceMaster.FakeMysqlDaemon.CurrentMasterPosition))),
					sqltypes.MakeString([]byte("")),
				},
			},
		},
	}
	dest1Master.StartActionLoop(t, wr)
	defer dest1Master.StopActionLoop(t)

	// dest2Rdonly will see the refresh
	dest2Rdonly.StartActionLoop(t, wr)
	defer dest2Rdonly.StopActionLoop(t)

	// dest2Replica will see the refresh
	dest2Replica.StartActionLoop(t, wr)
	defer dest2Replica.StopActionLoop(t)

	// dest2Master will see the refresh, and has to respond to it.
	// It will also need to respond to WaitBlpPosition, saying it's already caught up.
	dest2Master.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*mproto.QueryResult{
		"SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=0": &mproto.QueryResult{
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte(myproto.EncodeReplicationPosition(sourceMaster.FakeMysqlDaemon.CurrentMasterPosition))),
					sqltypes.MakeString([]byte("")),
				},
			},
		},
	}
	dest2Master.StartActionLoop(t, wr)
	defer dest2Master.StopActionLoop(t)

	// simulate the clone, by fixing the dest shard record
	if err := vp.Run([]string{"SourceShardAdd", "--key_range=-", "ks/-80", "0", "ks/0"}); err != nil {
		t.Fatalf("SourceShardAdd failed: %v", err)
	}
	if err := vp.Run([]string{"SourceShardAdd", "--key_range=-", "ks/80-", "0", "ks/0"}); err != nil {
		t.Fatalf("SourceShardAdd failed: %v", err)
	}

	// migrate rdonly over
	if err := vp.Run([]string{"MigrateServedTypes", "ks/0", "rdonly"}); err != nil {
		t.Fatalf("MigrateServedType(rdonly) failed: %v", err)
	}

	checkShardServedTypes(t, ts, "0", 2)
	checkShardServedTypes(t, ts, "-80", 1)
	checkShardServedTypes(t, ts, "80-", 1)

	// migrate replica over
	if err := vp.Run([]string{"MigrateServedTypes", "ks/0", "replica"}); err != nil {
		t.Fatalf("MigrateServedType(replica) failed: %v", err)
	}

	checkShardServedTypes(t, ts, "0", 1)
	checkShardServedTypes(t, ts, "-80", 2)
	checkShardServedTypes(t, ts, "80-", 2)

	// migrate master over
	if err := vp.Run([]string{"MigrateServedTypes", "ks/0", "master"}); err != nil {
		t.Fatalf("MigrateServedType(master) failed: %v", err)
	}

	checkShardServedTypes(t, ts, "0", 0)
	checkShardServedTypes(t, ts, "-80", 3)
	checkShardServedTypes(t, ts, "80-", 3)
}
