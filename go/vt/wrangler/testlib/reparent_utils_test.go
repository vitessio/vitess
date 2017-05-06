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
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestShardReplicationStatuses(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// create shard and tablets
	if _, err := ts.GetOrCreateShard(ctx, "test_keyspace", "0"); err != nil {
		t.Fatalf("GetOrCreateShard failed: %v", err)
	}
	master := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_MASTER, nil)
	slave := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// mark the master inside the shard
	if _, err := ts.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.MasterAlias = master.Tablet.Alias
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// master action loop (to initialize host and port)
	master.FakeMysqlDaemon.CurrentMasterPosition = replication.Position{
		GTIDSet: replication.MariadbGTID{
			Domain:   5,
			Server:   456,
			Sequence: 892,
		},
	}
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)

	// slave loop
	slave.FakeMysqlDaemon.CurrentMasterPosition = replication.Position{
		GTIDSet: replication.MariadbGTID{
			Domain:   5,
			Server:   456,
			Sequence: 890,
		},
	}
	slave.FakeMysqlDaemon.CurrentMasterHost = master.Tablet.Hostname
	slave.FakeMysqlDaemon.CurrentMasterPort = int(master.Tablet.PortMap["mysql"])
	slave.StartActionLoop(t, wr)
	defer slave.StopActionLoop(t)

	// run ShardReplicationStatuses
	ti, rs, err := wr.ShardReplicationStatuses(ctx, "test_keyspace", "0")
	if err != nil {
		t.Fatalf("ShardReplicationStatuses failed: %v", err)
	}

	// check result (make master first in the array)
	if len(ti) != 2 || len(rs) != 2 {
		t.Fatalf("ShardReplicationStatuses returned wrong results: %v %v", ti, rs)
	}
	if topoproto.TabletAliasEqual(ti[0].Alias, slave.Tablet.Alias) {
		ti[0], ti[1] = ti[1], ti[0]
		rs[0], rs[1] = rs[1], rs[0]
	}
	if !topoproto.TabletAliasEqual(ti[0].Alias, master.Tablet.Alias) ||
		!topoproto.TabletAliasEqual(ti[1].Alias, slave.Tablet.Alias) ||
		rs[0].MasterHost != "" ||
		rs[1].MasterHost != master.Tablet.Hostname {
		t.Fatalf("ShardReplicationStatuses returend wrong results: %v %v", ti, rs)
	}
}

func TestReparentTablet(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// create shard and tablets
	if _, err := ts.GetOrCreateShard(ctx, "test_keyspace", "0"); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}
	master := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_MASTER, nil)
	slave := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// mark the master inside the shard
	if _, err := ts.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.MasterAlias = master.Tablet.Alias
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// master action loop (to initialize host and port)
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)

	// slave loop
	slave.FakeMysqlDaemon.SetMasterCommandsInput = fmt.Sprintf("%v:%v", master.Tablet.Hostname, master.Tablet.PortMap["mysql"])
	slave.FakeMysqlDaemon.SetMasterCommandsResult = []string{"set master cmd 1"}
	slave.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"set master cmd 1",
	}
	slave.StartActionLoop(t, wr)
	defer slave.StopActionLoop(t)

	// run ReparentTablet
	if err := wr.ReparentTablet(ctx, slave.Tablet.Alias); err != nil {
		t.Fatalf("ReparentTablet failed: %v", err)
	}

	// check what was run
	if err := slave.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("slave.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	checkSemiSyncEnabled(t, false, true, slave)
}
