/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestShardReplicationStatuses(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// create shard and tablets
	if _, err := ts.GetOrCreateShard(ctx, "test_keyspace", "0"); err != nil {
		t.Fatalf("GetOrCreateShard failed: %v", err)
	}
	primary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_PRIMARY, nil)
	replica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// mark the primary inside the shard
	if _, err := ts.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = primary.Tablet.Alias
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// primary action loop (to initialize host and port)
	primary.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			5: mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 892,
			},
		},
	}
	primary.StartActionLoop(t, wr)
	defer primary.StopActionLoop(t)

	// replica loop
	replica.FakeMysqlDaemon.CurrentPrimaryPosition = mysql.Position{
		GTIDSet: mysql.MariadbGTIDSet{
			5: mysql.MariadbGTID{
				Domain:   5,
				Server:   456,
				Sequence: 890,
			},
		},
	}
	replica.FakeMysqlDaemon.CurrentSourceHost = primary.Tablet.MysqlHostname
	replica.FakeMysqlDaemon.CurrentSourcePort = int(primary.Tablet.MysqlPort)
	replica.FakeMysqlDaemon.SetReplicationSourceInputs = append(replica.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(primary.Tablet))
	replica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	replica.StartActionLoop(t, wr)
	defer replica.StopActionLoop(t)

	// run ShardReplicationStatuses
	ti, rs, err := reparentutil.ShardReplicationStatuses(ctx, wr.TopoServer(), wr.TabletManagerClient(), "test_keyspace", "0")
	if err != nil {
		t.Fatalf("ShardReplicationStatuses failed: %v", err)
	}

	// check result (make primary first in the array)
	if len(ti) != 2 || len(rs) != 2 {
		t.Fatalf("ShardReplicationStatuses returned wrong results: %v %v", ti, rs)
	}
	if topoproto.TabletAliasEqual(ti[0].Alias, replica.Tablet.Alias) {
		ti[0], ti[1] = ti[1], ti[0]
		rs[0], rs[1] = rs[1], rs[0]
	}
	if !topoproto.TabletAliasEqual(ti[0].Alias, primary.Tablet.Alias) ||
		!topoproto.TabletAliasEqual(ti[1].Alias, replica.Tablet.Alias) ||
		rs[0].SourceHost != "" ||
		rs[1].SourceHost != primary.Tablet.Hostname {
		t.Fatalf("ShardReplicationStatuses returend wrong results: %v %v", ti, rs)
	}
}

func TestReparentTablet(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// create shard and tablets
	if _, err := ts.GetOrCreateShard(ctx, "test_keyspace", "0"); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}
	primary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_PRIMARY, nil)
	replica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
	reparenttestutil.SetKeyspaceDurability(context.Background(), t, ts, "test_keyspace", "semi_sync")

	// mark the primary inside the shard
	if _, err := ts.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = primary.Tablet.Alias
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// primary action loop (to initialize host and port)
	primary.StartActionLoop(t, wr)
	defer primary.StopActionLoop(t)

	// replica loop
	// We have to set the settings as replicating. Otherwise,
	// the replication manager intervenes and tries to fix replication,
	// which ends up making this test unpredictable.
	replica.FakeMysqlDaemon.Replicating = true
	replica.FakeMysqlDaemon.IOThreadRunning = true
	replica.FakeMysqlDaemon.SetReplicationSourceInputs = append(replica.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(primary.Tablet))
	replica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 3 statements come from tablet startup
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
		"STOP SLAVE",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	replica.StartActionLoop(t, wr)
	defer replica.StopActionLoop(t)

	// run ReparentTablet
	if err := wr.ReparentTablet(ctx, replica.Tablet.Alias); err != nil {
		t.Fatalf("ReparentTablet failed: %v", err)
	}

	// check what was run
	if err := replica.FakeMysqlDaemon.CheckSuperQueryList(); err != nil {
		t.Fatalf("replica.FakeMysqlDaemon.CheckSuperQueryList failed: %v", err)
	}
	checkSemiSyncEnabled(t, false, true, replica)
}

// TestSetReplicationSource tests that SetReplicationSource works as intended under various circumstances.
func TestSetReplicationSource(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// create shard and tablets
	_, err := ts.GetOrCreateShard(ctx, "test_keyspace", "0")
	require.NoError(t, err, "CreateShard failed")

	primary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_PRIMARY, nil)
	reparenttestutil.SetKeyspaceDurability(context.Background(), t, ts, "test_keyspace", "semi_sync")

	// mark the primary inside the shard
	_, err = ts.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = primary.Tablet.Alias
		return nil
	})
	require.NoError(t, err, "UpdateShardFields failed")

	// primary action loop (to initialize host and port)
	primary.StartActionLoop(t, wr)
	defer primary.StopActionLoop(t)

	// test when we receive a relay log error while starting replication
	t.Run("Relay log error", func(t *testing.T) {
		replica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)
		// replica loop
		// We have to set the settings as replicating. Otherwise,
		// the replication manager intervenes and tries to fix replication,
		// which ends up making this test unpredictable.
		replica.FakeMysqlDaemon.Replicating = true
		replica.FakeMysqlDaemon.IOThreadRunning = true
		replica.FakeMysqlDaemon.SetReplicationSourceInputs = append(replica.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(primary.Tablet))
		replica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
			// These 3 statements come from tablet startup
			"STOP SLAVE",
			"FAKE SET MASTER",
			"START SLAVE",
			// We stop and reset the replication parameters because of relay log issues.
			"STOP SLAVE",
			"RESET SLAVE",
			"START SLAVE",
		}
		replica.StartActionLoop(t, wr)
		defer replica.StopActionLoop(t)

		// Set the correct error message that indicates we have received a relay log error.
		replica.FakeMysqlDaemon.SetReplicationSourceError = errors.New("ERROR 1201 (HY000): Could not initialize master info structure; more error messages can be found in the MySQL error log")
		// run ReparentTablet
		err = wr.SetReplicationSource(ctx, replica.Tablet)
		require.NoError(t, err, "SetReplicationSource failed")

		// check what was run
		err = replica.FakeMysqlDaemon.CheckSuperQueryList()
		require.NoError(t, err, "CheckSuperQueryList failed")
		checkSemiSyncEnabled(t, false, true, replica)
	})

	// test setting an empty hostname because of primary shutdown
	t.Run("Primary tablet already shutdown", func(t *testing.T) {
		replica := NewFakeTablet(t, wr, "cell1", 3, topodatapb.TabletType_REPLICA, nil)
		// replica loop
		replica.FakeMysqlDaemon.Replicating = true
		replica.FakeMysqlDaemon.IOThreadRunning = true
		replica.FakeMysqlDaemon.SetReplicationSourceInputs = append(replica.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(primary.Tablet))
		replica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
			// These 3 statements come from tablet startup
			"STOP SLAVE",
			"FAKE SET MASTER",
			"START SLAVE",
			// For the SetReplicationSource call, we shouldn't get any queries at all!
		}
		replica.StartActionLoop(t, wr)
		defer replica.StopActionLoop(t)

		// stop the primary
		primary.StopActionLoop(t)
		// update the primary topo record
		wr.TopoServer().UpdateTabletFields(ctx, primary.Tablet.Alias, func(tablet *topodatapb.Tablet) error {
			tablet.MysqlHostname = ""
			return nil
		})

		// run SetReplicationSource
		err = wr.SetReplicationSource(ctx, replica.Tablet)
		require.ErrorContains(t, err, "Shard primary has empty mysql hostname")

		// check what was run
		err = replica.FakeMysqlDaemon.CheckSuperQueryList()
		require.NoError(t, err, "CheckSuperQueryList failed")
		checkSemiSyncEnabled(t, false, true, replica)
	})
}
