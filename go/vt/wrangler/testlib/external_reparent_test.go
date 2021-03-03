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
	"testing"
	"time"

	"vitess.io/vitess/go/vt/discovery"

	"context"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// The tests in this package test the wrangler version of TabletExternallyReparented
// This is the one that is now called by the vtctl command

// TestTabletExternallyReparentedBasic tests the base cases for TER
func TestTabletExternallyReparentedBasic(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create an old master, a new master, two good replicas, one bad replica
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"}, false)
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// On the elected master, we will respond to
	// TabletActionReplicaWasPromoted
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// On the old master, we will only respond to
	// TabletActionReplicaWasRestarted.
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// First test: reparent to the same master, make sure it works
	// as expected.
	if err := vp.Run([]string{"TabletExternallyReparented", topoproto.TabletAliasString(oldMaster.Tablet.Alias)}); err != nil {
		t.Fatalf("TabletExternallyReparented(same master) should have worked: %v", err)
	}

	// check the old master is still master
	tablet, err := ts.GetTablet(ctx, oldMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("old master should be MASTER but is: %v", tablet.Type)
	}

	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START Replica",
	}

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new master) expecting success")
	if err := wr.TabletExternallyReparented(ctx, newMaster.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}

	// check the new master is master
	tablet, err = ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new master should be MASTER but is: %v", tablet.Type)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			t.Fatalf("old master (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMaster.Tablet.Alias), tablet.Type)
		}
		// check the old master was converted to replica
		tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}
}

func TestTabletExternallyReparentedToReplica(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, two good replicas, one bad replica
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	newMaster.FakeMysqlDaemon.ReadOnly = true
	newMaster.FakeMysqlDaemon.Replicating = true

	// Build keyspace graph
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"}, false)
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// On the elected master, we will respond to
	// TabletActionReplicaWasPromoted
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// On the old master, we will only respond to
	// TabletActionReplicaWasRestarted.
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// Second test: reparent to a replica, and pretend the old
	// master is still good to go.
	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START Replica",
	}

	// This tests a bad case: the new designated master is a replica at mysql level,
	// but we should do what we're told anyway.
	if err := wr.TabletExternallyReparented(ctx, newMaster.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) error: %v", err)
	}

	// check that newMaster is master
	tablet, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new master should be MASTER but is: %v", tablet.Type)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			t.Fatalf("old master (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMaster.Tablet.Alias), tablet.Type)
		}
		// check the old master was converted to replica
		tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}
}

// TestTabletExternallyReparentedWithDifferentMysqlPort makes sure
// that if mysql is restarted on the master-elect tablet and has a different
// port, we pick it up correctly.
func TestTabletExternallyReparentedWithDifferentMysqlPort(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, two good replicas, one bad replica
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"}, false)
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// Now we're restarting mysql on a different port, 3301->3303
	// but without updating the Tablet record in topology.

	// On the elected master, we will respond to
	// TabletActionReplicaWasPromoted, so we need a MysqlDaemon
	// that returns no master, and the new port (as returned by mysql)
	newMaster.FakeMysqlDaemon.MysqlPort.Set(3303)
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START Replica",
	}
	// On the old master, we will only respond to
	// TabletActionReplicaWasRestarted and point to the new mysql port
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// On the good replicas, we will respond to
	// TabletActionReplicaWasRestarted and point to the new mysql port
	goodReplica.StartActionLoop(t, wr)
	defer goodReplica.StopActionLoop(t)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new master) expecting success")
	if err := wr.TabletExternallyReparented(ctx, newMaster.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	// check the new master is master
	tablet, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new master should be MASTER but is: %v", tablet.Type)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			t.Fatalf("old master (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMaster.Tablet.Alias), tablet.Type)
		}
		// check the old master was converted to replica
		tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}
}

// TestTabletExternallyReparentedContinueOnUnexpectedMaster makes sure
// that we ignore mysql's master if the flag is set
func TestTabletExternallyReparentedContinueOnUnexpectedMaster(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, two good replicas, one bad replica
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"}, false)
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// On the elected master, we will respond to
	// TabletActionReplicaWasPromoted, so we need a MysqlDaemon
	// that returns no master, and the new port (as returned by mysql)
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START Replica",
	}
	// On the old master, we will only respond to
	// TabletActionReplicaWasRestarted and point to a bad host
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// On the good replica, we will respond to
	// TabletActionReplicaWasRestarted and point to a bad host
	goodReplica.StartActionLoop(t, wr)
	defer goodReplica.StopActionLoop(t)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new master) expecting success")
	if err := wr.TabletExternallyReparented(ctx, newMaster.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	// check the new master is master
	tablet, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new master should be MASTER but is: %v", tablet.Type)
	}
	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			t.Fatalf("old master (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMaster.Tablet.Alias), tablet.Type)
		}
		// check the old master was converted to replica
		tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}
}

func TestTabletExternallyReparentedRerun(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, and a good replica.
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"}, false)
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// On the elected master, we will respond to
	// TabletActionReplicaWasPromoted.
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START Replica",
	}
	// On the old master, we will only respond to
	// TabletActionReplicaWasRestarted.
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	goodReplica.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	// On the good replica, we will respond to
	// TabletActionReplicaWasRestarted.
	goodReplica.StartActionLoop(t, wr)
	defer goodReplica.StopActionLoop(t)

	// The reparent should work as expected here
	if err := wr.TabletExternallyReparented(ctx, newMaster.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}

	// check the new master is master
	tablet, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new master should be MASTER but is: %v", tablet.Type)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			t.Fatalf("old master (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMaster.Tablet.Alias), tablet.Type)
		}
		// check the old master was converted to replica
		tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}

	// run TER again and make sure the master is still correct
	if err := wr.TabletExternallyReparented(ctx, newMaster.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}

	// check the new master is still master
	tablet, err = ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new master should be MASTER but is: %v", tablet.Type)
	}

}

func TestRPCTabletExternallyReparentedDemotesMasterToConfiguredTabletType(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	flag.Set("disable_active_reparents", "true")
	defer flag.Set("disable_active_reparents", "false")

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master and a new master
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_SPARE, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_SPARE, nil)

	oldMaster.StartActionLoop(t, wr)
	newMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)
	defer newMaster.StopActionLoop(t)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"}, false)
	assert.NoError(t, err, "RebuildKeyspaceLocked failed: %v", err)

	// Reparent to new master
	ti, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}

	if err := wr.TabletExternallyReparented(context.Background(), ti.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented failed: %v", err)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err := ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			t.Fatalf("old master (%v) should be spare but is: %v", topoproto.TabletAliasString(oldMaster.Tablet.Alias), tablet.Type)
		}
		// check the old master was converted to replica
		tablet, err := ts.GetTablet(ctx, oldMaster.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_SPARE {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}

	shardInfo, err := ts.GetShard(context.Background(), newMaster.Tablet.Keyspace, newMaster.Tablet.Shard)
	assert.NoError(t, err)

	assert.True(t, topoproto.TabletAliasEqual(newMaster.Tablet.Alias, shardInfo.MasterAlias))
	assert.Equal(t, topodatapb.TabletType_MASTER, newMaster.TM.Tablet().Type)
}
