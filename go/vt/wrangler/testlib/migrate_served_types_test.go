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
	"flag"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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

func checkShardSourceShards(t *testing.T, ts topo.Server, shard string, expected int) {
	ctx := context.Background()
	si, err := ts.GetShard(ctx, "ks", shard)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	if len(si.SourceShards) != expected {
		t.Fatalf("shard %v has wrong SourceShards: %#v", shard, si.SourceShards)
	}
}

func TestMigrateServedTypes(t *testing.T) {
	// TODO(b/26388813): Remove the next two lines once vtctl WaitForDrain is integrated in the vtctl MigrateServed* commands.
	flag.Set("wait_for_drain_sleep_rdonly", "0s")
	flag.Set("wait_for_drain_sleep_replica", "0s")

	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// create keyspace
	if err := ts.CreateKeyspace(context.Background(), "ks", &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	// create the source shard
	sourceMaster := NewFakeTablet(t, wr, "cell1", 10, topodatapb.TabletType_MASTER, nil,
		TabletKeyspaceShard(t, "ks", "0"))
	sourceReplica := NewFakeTablet(t, wr, "cell1", 11, topodatapb.TabletType_REPLICA, nil,
		TabletKeyspaceShard(t, "ks", "0"))
	sourceRdonly := NewFakeTablet(t, wr, "cell1", 12, topodatapb.TabletType_RDONLY, nil,
		TabletKeyspaceShard(t, "ks", "0"))

	// create the first destination shard
	dest1Master := NewFakeTablet(t, wr, "cell1", 20, topodatapb.TabletType_MASTER, nil,
		TabletKeyspaceShard(t, "ks", "-80"))
	dest1Replica := NewFakeTablet(t, wr, "cell1", 21, topodatapb.TabletType_REPLICA, nil,
		TabletKeyspaceShard(t, "ks", "-80"))
	dest1Rdonly := NewFakeTablet(t, wr, "cell1", 22, topodatapb.TabletType_RDONLY, nil,
		TabletKeyspaceShard(t, "ks", "-80"))

	// create the second destination shard
	dest2Master := NewFakeTablet(t, wr, "cell1", 30, topodatapb.TabletType_MASTER, nil,
		TabletKeyspaceShard(t, "ks", "80-"))
	dest2Replica := NewFakeTablet(t, wr, "cell1", 31, topodatapb.TabletType_REPLICA, nil,
		TabletKeyspaceShard(t, "ks", "80-"))
	dest2Rdonly := NewFakeTablet(t, wr, "cell1", 32, topodatapb.TabletType_RDONLY, nil,
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
	sourceMaster.FakeMysqlDaemon.CurrentMasterPosition = replication.Position{
		GTIDSet: replication.MariadbGTID{
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
	dest1Master.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=0": {
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte(replication.EncodePosition(sourceMaster.FakeMysqlDaemon.CurrentMasterPosition))),
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
	dest2Master.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=0": {
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte(replication.EncodePosition(sourceMaster.FakeMysqlDaemon.CurrentMasterPosition))),
					sqltypes.MakeString([]byte("")),
				},
			},
		},
	}
	dest2Master.StartActionLoop(t, wr)
	defer dest2Master.StopActionLoop(t)

	// migrate will error if the overlapping shards have no "SourceShard" entry
	// and we cannot decide which shard is the source or the destination.
	if err := vp.Run([]string{"MigrateServedTypes", "ks/0", "rdonly"}); err == nil || !strings.Contains(err.Error(), "' have a 'SourceShards' entry. Did you successfully run vtworker SplitClone before? Or did you already migrate the MASTER type?") {
		t.Fatalf("MigrateServedType(rdonly) should fail if no 'SourceShards' entry is present: %v", err)
	}

	// simulate the clone, by fixing the dest shard record
	checkShardSourceShards(t, ts, "-80", 0)
	checkShardSourceShards(t, ts, "80-", 0)
	if err := vp.Run([]string{"SourceShardAdd", "--key_range=-", "ks/-80", "0", "ks/0"}); err != nil {
		t.Fatalf("SourceShardAdd failed: %v", err)
	}
	if err := vp.Run([]string{"SourceShardAdd", "--key_range=-", "ks/80-", "0", "ks/0"}); err != nil {
		t.Fatalf("SourceShardAdd failed: %v", err)
	}
	checkShardSourceShards(t, ts, "-80", 1)
	checkShardSourceShards(t, ts, "80-", 1)

	// migrate rdonly over
	if err := vp.Run([]string{"MigrateServedTypes", "ks/0", "rdonly"}); err != nil {
		t.Fatalf("MigrateServedType(rdonly) failed: %v", err)
	}

	checkShardServedTypes(t, ts, "0", 2)
	checkShardServedTypes(t, ts, "-80", 1)
	checkShardServedTypes(t, ts, "80-", 1)
	checkShardSourceShards(t, ts, "-80", 1)
	checkShardSourceShards(t, ts, "80-", 1)

	// migrate replica over
	if err := vp.Run([]string{"MigrateServedTypes", "ks/0", "replica"}); err != nil {
		t.Fatalf("MigrateServedType(replica) failed: %v", err)
	}

	checkShardServedTypes(t, ts, "0", 1)
	checkShardServedTypes(t, ts, "-80", 2)
	checkShardServedTypes(t, ts, "80-", 2)
	checkShardSourceShards(t, ts, "-80", 1)
	checkShardSourceShards(t, ts, "80-", 1)

	// migrate master over
	if err := vp.Run([]string{"MigrateServedTypes", "ks/0", "master"}); err != nil {
		t.Fatalf("MigrateServedType(master) failed: %v", err)
	}

	checkShardServedTypes(t, ts, "0", 0)
	checkShardServedTypes(t, ts, "-80", 3)
	checkShardServedTypes(t, ts, "80-", 3)
	checkShardSourceShards(t, ts, "-80", 0)
	checkShardSourceShards(t, ts, "80-", 0)
}
