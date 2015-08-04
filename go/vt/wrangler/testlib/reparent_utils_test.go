// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"fmt"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"
)

func TestShardReplicationStatuses(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)

	// create shard and tablets
	if err := topo.CreateShard(ctx, ts, "test_keyspace", "0"); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}
	master := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_MASTER)
	slave := NewFakeTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA)

	// mark the master inside the shard
	si, err := ts.GetShard(ctx, "test_keyspace", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	si.MasterAlias = topo.TabletAliasToProto(master.Tablet.Alias)
	if err := topo.UpdateShard(ctx, ts, si); err != nil {
		t.Fatalf("UpdateShard failed: %v", err)
	}

	// master action loop (to initialize host and port)
	master.FakeMysqlDaemon.CurrentMasterPosition = myproto.ReplicationPosition{
		GTIDSet: myproto.MariadbGTID{
			Domain:   5,
			Server:   456,
			Sequence: 892,
		},
	}
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)

	// slave loop
	slave.FakeMysqlDaemon.CurrentMasterPosition = myproto.ReplicationPosition{
		GTIDSet: myproto.MariadbGTID{
			Domain:   5,
			Server:   456,
			Sequence: 890,
		},
	}
	slave.FakeMysqlDaemon.CurrentMasterHost = master.Tablet.Hostname
	slave.FakeMysqlDaemon.CurrentMasterPort = master.Tablet.Portmap["mysql"]
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
	if ti[0].Alias == slave.Tablet.Alias {
		ti[0], ti[1] = ti[1], ti[0]
		rs[0], rs[1] = rs[1], rs[0]
	}
	if ti[0].Alias != master.Tablet.Alias ||
		ti[1].Alias != slave.Tablet.Alias ||
		rs[0].MasterHost != "" ||
		rs[1].MasterHost != master.Tablet.Hostname {
		t.Fatalf("ShardReplicationStatuses returend wrong results: %v %v", ti, rs)
	}
}

func TestReparentTablet(t *testing.T) {
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)

	// create shard and tablets
	if err := topo.CreateShard(ctx, ts, "test_keyspace", "0"); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}
	master := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_MASTER)
	slave := NewFakeTablet(t, wr, "cell1", 2, topo.TYPE_REPLICA)

	// mark the master inside the shard
	si, err := ts.GetShard(ctx, "test_keyspace", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	si.MasterAlias = topo.TabletAliasToProto(master.Tablet.Alias)
	if err := topo.UpdateShard(ctx, ts, si); err != nil {
		t.Fatalf("UpdateShard failed: %v", err)
	}

	// master action loop (to initialize host and port)
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)

	// slave loop
	slave.FakeMysqlDaemon.SetMasterCommandsInput = fmt.Sprintf("%v:%v", master.Tablet.Hostname, master.Tablet.Portmap["mysql"])
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
}
