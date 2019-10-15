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
	"testing"
	"time"

	"golang.org/x/net/context"

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
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create an old master, a new master, two good replicas, one bad replica
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// On the elected master, we will respond to
	// TabletActionSlaveWasPromoted
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// On the old master, we will only respond to
	// TabletActionSlaveWasRestarted.
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
		"START SLAVE",
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
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

loop:
	for {
		select {
		case <-timer.C:
			// we timed out
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			t.Fatalf("old master (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMaster.Tablet.Alias), tablet.Type)
		default:
			// check the old master was converted to replica
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			if tablet.Type != topodatapb.TabletType_REPLICA {
				time.Sleep(100 * time.Millisecond)
			} else {
				break loop
			}
		}
	}
}

func TestTabletExternallyReparentedToSlave(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, two good replicas, one bad replica
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	newMaster.FakeMysqlDaemon.ReadOnly = true
	newMaster.FakeMysqlDaemon.Replicating = true

	// Build keyspace graph
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// On the elected master, we will respond to
	// TabletActionSlaveWasPromoted
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// On the old master, we will only respond to
	// TabletActionSlaveWasRestarted.
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// Second test: reparent to a replica, and pretend the old
	// master is still good to go.
	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}

	// This tests a bad case: the new designated master is a slave at mysql level,
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
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

loop:
	for {
		select {
		case <-timer.C:
			// we timed out
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			t.Fatalf("old master (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMaster.Tablet.Alias), tablet.Type)
		default:
			// check the old master was converted to replica
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			if tablet.Type != topodatapb.TabletType_REPLICA {
				time.Sleep(100 * time.Millisecond)
			} else {
				break loop
			}
		}
	}
}

// TestTabletExternallyReparentedWithDifferentMysqlPort makes sure
// that if mysql is restarted on the master-elect tablet and has a different
// port, we pick it up correctly.
func TestTabletExternallyReparentedWithDifferentMysqlPort(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, two good replicas, one bad replica
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// Now we're restarting mysql on a different port, 3301->3303
	// but without updating the Tablet record in topology.

	// On the elected master, we will respond to
	// TabletActionSlaveWasPromoted, so we need a MysqlDaemon
	// that returns no master, and the new port (as returned by mysql)
	newMaster.FakeMysqlDaemon.MysqlPort = 3303
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	// On the old master, we will only respond to
	// TabletActionSlaveWasRestarted and point to the new mysql port
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// On the good replicas, we will respond to
	// TabletActionSlaveWasRestarted and point to the new mysql port
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
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

loop:
	for {
		select {
		case <-timer.C:
			// we timed out
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			t.Fatalf("old master (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMaster.Tablet.Alias), tablet.Type)
		default:
			// check the old master was converted to replica
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			if tablet.Type != topodatapb.TabletType_REPLICA {
				time.Sleep(100 * time.Millisecond)
			} else {
				break loop
			}
		}
	}
}

// TestTabletExternallyReparentedContinueOnUnexpectedMaster makes sure
// that we ignore mysql's master if the flag is set
func TestTabletExternallyReparentedContinueOnUnexpectedMaster(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, two good replicas, one bad replica
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// On the elected master, we will respond to
	// TabletActionSlaveWasPromoted, so we need a MysqlDaemon
	// that returns no master, and the new port (as returned by mysql)
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	// On the old master, we will only respond to
	// TabletActionSlaveWasRestarted and point to a bad host
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	// On the good replica, we will respond to
	// TabletActionSlaveWasRestarted and point to a bad host
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
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

loop:
	for {
		select {
		case <-timer.C:
			// we timed out
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			t.Fatalf("old master (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMaster.Tablet.Alias), tablet.Type)
		default:
			// check the old master was converted to replica
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			if tablet.Type != topodatapb.TabletType_REPLICA {
				time.Sleep(100 * time.Millisecond)
			} else {
				break loop
			}
		}
	}
}

func TestTabletExternallyReparentedRerun(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, and a good replica.
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// On the elected master, we will respond to
	// TabletActionSlaveWasPromoted.
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	// On the old master, we will only respond to
	// TabletActionSlaveWasRestarted.
	oldMaster.StartActionLoop(t, wr)
	defer oldMaster.StopActionLoop(t)

	goodReplica.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	// On the good replica, we will respond to
	// TabletActionSlaveWasRestarted.
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
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

loop:
	for {
		select {
		case <-timer.C:
			// we timed out
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			t.Fatalf("old master (%v) should be replica but is: %v", topoproto.TabletAliasString(oldMaster.Tablet.Alias), tablet.Type)
		default:
			// check the old master was converted to replica
			tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
			}
			if tablet.Type != topodatapb.TabletType_REPLICA {
				time.Sleep(100 * time.Millisecond)
			} else {
				break loop
			}
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
