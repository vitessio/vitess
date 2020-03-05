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
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestRPCTabletExternallyReparented(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master and a new master
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell2", 1, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1", "cell2"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// On the elected master, we will respond to
	// TabletActionSlaveWasPromoted
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

	// First test: reparent to the same master, make sure it works
	// as expected.
	tmc := tmclient.NewTabletManagerClient()
	_, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), oldMaster.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(same master) should have worked: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparented: same master", waitID)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new master) expecting success")
	ti, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID = makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparented: good case", waitID)
}

func TestRPCTabletExternallyReparentedSlaveMysql(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, two good slaves, one bad slave
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell2", 1, topodatapb.TabletType_REPLICA, nil)
	newMaster.FakeMysqlDaemon.ReadOnly = true
	newMaster.FakeMysqlDaemon.Replicating = true

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1", "cell2"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// On the elected master, we will respond to
	// TabletActionSlaveWasPromoted
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

	// This tests a bad case: the new designated master is a slave,
	// but we should do what we're told anyway.
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(slave) error: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparented: slave designated as master", waitID)

}

// TestRPCTabletExternallyReparentedWithDifferentMysqlPort makes sure
// that if mysql is restarted on the master-elect tablet and has a different
// port, we pick it up correctly.
func TestRPCTabletExternallyReparentedWithDifferentMysqlPort(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, two good slaves, one bad slave
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSlave := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

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

	// On the good slaves, we will respond to
	// TabletActionSlaveWasRestarted and point to the new mysql port
	goodSlave.StartActionLoop(t, wr)
	defer goodSlave.StopActionLoop(t)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new master) expecting success")
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedWithDifferentMysqlPort: good case", waitID)
}

// TestRPCTabletExternallyReparentedContinueOnUnexpectedMaster makes sure
// that we ignore mysql's master if the flag is set
func TestRPCTabletExternallyReparentedContinueOnUnexpectedMaster(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, two good slaves, one bad slave
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSlave := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

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

	// On the good slave, we will respond to
	// TabletActionSlaveWasRestarted and point to a bad host
	goodSlave.StartActionLoop(t, wr)
	defer goodSlave.StopActionLoop(t)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new master) expecting success")
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedContinueOnUnexpectedMaster: good case", waitID)
}

func TestRPCTabletExternallyReparentedFailedOldMaster(t *testing.T) {
	// The 'RefreshState' call on the old master will timeout on
	// this value, so it has to be smaller than the 10s of the
	// wait for the 'finished' state of waitForExternalReparent.
	tabletmanager.SetReparentFlags(2 * time.Second /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, and a good slave.
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSlave := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// Reparent to a replica, and pretend the old master is not responding.

	// On the elected master, we will respond to
	// TabletActionSlaveWasPromoted.
	newMaster.StartActionLoop(t, wr)
	defer newMaster.StopActionLoop(t)

	// On the old master, we will only get a RefreshState call,
	// let's just not respond to it at all, and let it timeout.
	oldMaster.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(newMaster.Tablet)
	oldMaster.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}

	// On the good slave, we will respond to
	// TabletActionSlaveWasRestarted.
	goodSlave.StartActionLoop(t, wr)
	defer goodSlave.StopActionLoop(t)

	// The reparent should work as expected here
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedFailedOldMaster: good case", waitID)

	// check the old master was converted to replica
	tablet, err := ts.GetTablet(ctx, oldMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_REPLICA {
		t.Fatalf("old master should be replica but is: %v", tablet.Type)
	}
}

func TestRPCTabletExternallyReparentedImpostorMaster(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, and a bad slave.
	badSlave := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_MASTER, nil)
	// do this after badSlave so that the shard record has the expected master
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil, ForceInitTablet())
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// check the old master is really master
	tablet, err := ts.GetTablet(ctx, oldMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("old master should be MASTER but is: %v", tablet.Type)
	}

	// check the impostor also claims to be master
	tablet, err = ts.GetTablet(ctx, badSlave.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", badSlave.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("impostor should be MASTER but is: %v", tablet.Type)
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

	// set this to old master because as soon as badSlave starts, it detects that
	// there is a master with a later timestamp and demotes itself
	badSlave.FakeMysqlDaemon.SetMasterInput = topoproto.MysqlAddr(oldMaster.Tablet)
	badSlave.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET MASTER",
		"START SLAVE",
	}
	// On the bad slave, we will respond to
	// TabletActionSlaveWasRestarted.
	badSlave.StartActionLoop(t, wr)
	defer badSlave.StopActionLoop(t)

	// The reparent should work as expected here
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedImpostorMaster: good case", waitID)

	// check the new master is really master
	tablet, err = ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new master should be MASTER but is: %v", tablet.Type)
	}

	// check the old master was converted to replica
	tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_REPLICA {
		t.Fatalf("old master should be replica but is: %v", tablet.Type)
	}

	// check the impostor master was converted to replica
	tablet, err = ts.GetTablet(ctx, badSlave.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", badSlave.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_REPLICA {
		t.Fatalf("bad slave should be replica but is: %v", tablet.Type)
	}
}

func TestRPCTabletExternallyReparentedFailedImpostorMaster(t *testing.T) {
	tabletmanager.SetReparentFlags(2 * time.Second /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, and a bad slave.
	badSlave := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_MASTER, nil)
	// do this after badSlave so that the shard record has the expected master
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil, ForceInitTablet())
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldMaster.Tablet.Keyspace, []string{"cell1"})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// check the old master is really master
	tablet, err := ts.GetTablet(ctx, oldMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("old master should be MASTER but is: %v", tablet.Type)
	}

	// check the impostor also claims to be master
	tablet, err = ts.GetTablet(ctx, badSlave.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", badSlave.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("old master should be MASTER but is: %v", tablet.Type)
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

	// The reparent should work as expected here
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedImpostorMaster: good case", waitID)

	// check the new master is really master
	tablet, err = ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new master should be MASTER but is: %v", tablet.Type)
	}

	// check the old master was converted to replica
	tablet, err = ts.GetTablet(ctx, oldMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_REPLICA {
		t.Fatalf("old master should be replica but is: %v", tablet.Type)
	}

	// check the impostor master was converted to replica
	tablet, err = ts.GetTablet(ctx, badSlave.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", badSlave.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_REPLICA {
		t.Fatalf("bad slave should be replica but is: %v", tablet.Type)
	}
}

func TestRPCTabletExternallyReparentedRerun(t *testing.T) {
	tabletmanager.SetReparentFlags(time.Minute /* finalizeTimeout */)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old master, a new master, and a good slave.
	oldMaster := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	newMaster := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodSlave := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

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

	// On the good slave, we will respond to
	// TabletActionSlaveWasRestarted.
	goodSlave.StartActionLoop(t, wr)
	defer goodSlave.StopActionLoop(t)

	// The reparent should work as expected here
	tmc := tmclient.NewTabletManagerClient()
	ti, err := ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	waitID := makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedFailedOldMaster: good case", waitID)

	// check the old master was converted to replica
	tablet, err := ts.GetTablet(ctx, oldMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_REPLICA {
		t.Fatalf("old master should be replica but is: %v", tablet.Type)
	}

	// run TER again and make sure the master is still correct
	waitID = makeWaitID()
	if err := tmc.TabletExternallyReparented(context.Background(), ti.Tablet, waitID); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	waitForExternalReparent(t, "TestRPCTabletExternallyReparentedFailedOldMaster: good case", waitID)

	// check the new master is still master
	tablet, err = ts.GetTablet(ctx, newMaster.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newMaster.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_MASTER {
		t.Fatalf("new master should be MASTER but is: %v", tablet.Type)
	}

}

var (
	externalReparents      = make(map[string]chan struct{})
	externalReparentsMutex sync.Mutex
)

// makeWaitID generates a unique externalID that can be passed to
// TabletExternallyReparented, and then to waitForExternalReparent.
func makeWaitID() string {
	externalReparentsMutex.Lock()
	id := fmt.Sprintf("wait id %v", len(externalReparents))
	externalReparents[id] = make(chan struct{})
	externalReparentsMutex.Unlock()
	return id
}

func init() {
	event.AddListener(func(ev *events.Reparent) {
		if ev.Status == "finished" {
			externalReparentsMutex.Lock()
			if c, ok := externalReparents[ev.ExternalID]; ok {
				close(c)
			}
			externalReparentsMutex.Unlock()
		}
	})
}

// waitForExternalReparent waits up to a fixed duration for the external
// reparent with the given ID to finish. The ID must have been previously
// generated by makeWaitID().
//
// The TabletExternallyReparented RPC returns as soon as the
// new master is visible in the serving graph. Before checking things like
// replica endpoints and old master status, we should wait for the finalize
// stage, which happens in the background.
func waitForExternalReparent(t *testing.T, name, externalID string) {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	externalReparentsMutex.Lock()
	c := externalReparents[externalID]
	externalReparentsMutex.Unlock()

	select {
	case <-c:
		return
	case <-timer.C:
		t.Fatalf("deadline exceeded waiting for finalized external reparent %q for test %v", externalID, name)
	}
}
