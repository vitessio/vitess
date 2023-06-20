/*
Copyright 2018 The Vitess Authors.

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
	"flag"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/discovery"

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

	// Create an old primary, a new primary, two good replicas, one bad replica
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, oldPrimary.Tablet.Keyspace, []string{"cell1"}, false)
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// On the elected primary, we will respond to
	// TabletActionReplicaWasPromoted
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	// On the old primary, we will only respond to
	// TabletActionReplicaWasRestarted.
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)

	// First test: reparent to the same primary, make sure it works
	// as expected.
	if err := vp.Run([]string{"TabletExternallyReparented", topoproto.TabletAliasString(oldPrimary.Tablet.Alias)}); err != nil {
		t.Fatalf("TabletExternallyReparented(same primary) should have worked: %v", err)
	}

	// check the old primary is still primary
	tablet, err := ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_PRIMARY {
		t.Fatalf("old primary should be PRIMARY but is: %v", tablet.Type)
	}

	oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START Replica",
	}

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new primary) expecting success")
	if err := wr.TabletExternallyReparented(ctx, newPrimary.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}

	// check the new primary is primary
	tablet, err = ts.GetTablet(ctx, newPrimary.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newPrimary.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_PRIMARY {
		t.Fatalf("new primary should be PRIMARY but is: %v", tablet.Type)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
			}
			t.Fatalf("old primary (%v) should be replica but is: %v", topoproto.TabletAliasString(oldPrimary.Tablet.Alias), tablet.Type)
		}
		// check the old primary was converted to replica
		tablet, err = ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
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

	// Create an old primary, a new primary, two good replicas, one bad replica
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	newPrimary.FakeMysqlDaemon.ReadOnly = true
	newPrimary.FakeMysqlDaemon.Replicating = true

	// Build keyspace graph
	err := topotools.RebuildKeyspace(ctx, logutil.NewConsoleLogger(), ts, oldPrimary.Tablet.Keyspace, []string{"cell1"}, false)
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	// On the elected primary, we will respond to
	// TabletActionReplicaWasPromoted
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	// On the old primary, we will only respond to
	// TabletActionReplicaWasRestarted.
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)

	// Second test: reparent to a replica, and pretend the old
	// primary is still good to go.
	oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START Replica",
	}

	// This tests a bad case: the new designated primary is a replica at mysql level,
	// but we should do what we're told anyway.
	if err := wr.TabletExternallyReparented(ctx, newPrimary.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) error: %v", err)
	}

	// check that newPrimary is primary
	tablet, err := ts.GetTablet(ctx, newPrimary.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newPrimary.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_PRIMARY {
		t.Fatalf("new primary should be PRIMARY but is: %v", tablet.Type)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
			}
			t.Fatalf("old primary (%v) should be replica but is: %v", topoproto.TabletAliasString(oldPrimary.Tablet.Alias), tablet.Type)
		}
		// check the old primary was converted to replica
		tablet, err = ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}
}

// TestTabletExternallyReparentedWithDifferentMysqlPort makes sure
// that if mysql is restarted on the primary-elect tablet and has a different
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

	// Create an old primary, a new primary, two good replicas, one bad replica
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldPrimary.Tablet.Keyspace, []string{"cell1"}, false)
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// Now we're restarting mysql on a different port, 3301->3303
	// but without updating the Tablet record in topology.

	// On the elected primary, we will respond to
	// TabletActionReplicaWasPromoted, so we need a MysqlDaemon
	// that returns no primary, and the new port (as returned by mysql)
	newPrimary.FakeMysqlDaemon.MysqlPort.Set(3303)
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START Replica",
	}
	// On the old primary, we will only respond to
	// TabletActionReplicaWasRestarted and point to the new mysql port
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)

	// On the good replicas, we will respond to
	// TabletActionReplicaWasRestarted and point to the new mysql port
	goodReplica.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 4 statements come from tablet startup
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica.StartActionLoop(t, wr)
	defer goodReplica.StopActionLoop(t)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new primary) expecting success")
	if err := wr.TabletExternallyReparented(ctx, newPrimary.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	// check the new primary is primary
	tablet, err := ts.GetTablet(ctx, newPrimary.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newPrimary.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_PRIMARY {
		t.Fatalf("new primary should be PRIMARY but is: %v", tablet.Type)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
			}
			t.Fatalf("old primary (%v) should be replica but is: %v", topoproto.TabletAliasString(oldPrimary.Tablet.Alias), tablet.Type)
		}
		// check the old primary was converted to replica
		tablet, err = ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}
}

// TestTabletExternallyReparentedContinueOnUnexpectedPrimary makes sure
// that we ignore mysql's primary if the flag is set
func TestTabletExternallyReparentedContinueOnUnexpectedPrimary(t *testing.T) {
	delay := discovery.GetTabletPickerRetryDelay()
	defer func() {
		discovery.SetTabletPickerRetryDelay(delay)
	}()
	discovery.SetTabletPickerRetryDelay(5 * time.Millisecond)

	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	// Create an old primary, a new primary, two good replicas, one bad replica
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldPrimary.Tablet.Keyspace, []string{"cell1"}, false)
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// On the elected primary, we will respond to
	// TabletActionReplicaWasPromoted, so we need a MysqlDaemon
	// that returns no primary, and the new port (as returned by mysql)
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START Replica",
	}
	// On the old primary, we will only respond to
	// TabletActionReplicaWasRestarted and point to a bad host
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)

	// On the good replica, we will respond to
	// TabletActionReplicaWasRestarted and point to a bad host
	goodReplica.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(oldPrimary.Tablet))
	goodReplica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 4 statements come from tablet startup
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica.StartActionLoop(t, wr)
	defer goodReplica.StopActionLoop(t)

	// This tests the good case, where everything works as planned
	t.Logf("TabletExternallyReparented(new primary) expecting success")
	if err := wr.TabletExternallyReparented(ctx, newPrimary.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}
	// check the new primary is primary
	tablet, err := ts.GetTablet(ctx, newPrimary.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newPrimary.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_PRIMARY {
		t.Fatalf("new primary should be PRIMARY but is: %v", tablet.Type)
	}
	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
			}
			t.Fatalf("old primary (%v) should be replica but is: %v", topoproto.TabletAliasString(oldPrimary.Tablet.Alias), tablet.Type)
		}
		// check the old primary was converted to replica
		tablet, err = ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
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

	// Create an old primary, a new primary, and a good replica.
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_PRIMARY, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	goodReplica := NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldPrimary.Tablet.Keyspace, []string{"cell1"}, false)
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}
	// On the elected primary, we will respond to
	// TabletActionReplicaWasPromoted.
	newPrimary.StartActionLoop(t, wr)
	defer newPrimary.StopActionLoop(t)

	oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs = append(oldPrimary.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet))
	oldPrimary.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START Replica",
	}
	// On the old primary, we will only respond to
	// TabletActionReplicaWasRestarted.
	oldPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)

	goodReplica.FakeMysqlDaemon.SetReplicationSourceInputs = append(goodReplica.FakeMysqlDaemon.SetReplicationSourceInputs, topoproto.MysqlAddr(newPrimary.Tablet), topoproto.MysqlAddr(oldPrimary.Tablet))
	// On the good replica, we will respond to
	// TabletActionReplicaWasRestarted.
	goodReplica.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		// These 4 statements come from tablet startup
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	goodReplica.StartActionLoop(t, wr)
	defer goodReplica.StopActionLoop(t)

	// The reparent should work as expected here
	if err := wr.TabletExternallyReparented(ctx, newPrimary.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}

	// check the new primary is primary
	tablet, err := ts.GetTablet(ctx, newPrimary.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newPrimary.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_PRIMARY {
		t.Fatalf("new primary should be PRIMARY but is: %v", tablet.Type)
	}

	// We have to wait for shard sync to do its magic in the background
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			tablet, err = ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
			}
			t.Fatalf("old primary (%v) should be replica but is: %v", topoproto.TabletAliasString(oldPrimary.Tablet.Alias), tablet.Type)
		}
		// check the old primary was converted to replica
		tablet, err = ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_REPLICA {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}

	// run TER again and make sure the primary is still correct
	if err := wr.TabletExternallyReparented(ctx, newPrimary.Tablet.Alias); err != nil {
		t.Fatalf("TabletExternallyReparented(replica) failed: %v", err)
	}

	// check the new primary is still primary
	tablet, err = ts.GetTablet(ctx, newPrimary.Tablet.Alias)
	if err != nil {
		t.Fatalf("GetTablet(%v) failed: %v", newPrimary.Tablet.Alias, err)
	}
	if tablet.Type != topodatapb.TabletType_PRIMARY {
		t.Fatalf("new primary should be PRIMARY but is: %v", tablet.Type)
	}

}

func TestRPCTabletExternallyReparentedDemotesPrimaryToConfiguredTabletType(t *testing.T) {
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

	// Create an old primary and a new primary
	oldPrimary := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_SPARE, nil)
	newPrimary := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_SPARE, nil)

	oldPrimary.StartActionLoop(t, wr)
	newPrimary.StartActionLoop(t, wr)
	defer oldPrimary.StopActionLoop(t)
	defer newPrimary.StopActionLoop(t)

	// Build keyspace graph
	err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, oldPrimary.Tablet.Keyspace, []string{"cell1"}, false)
	assert.NoError(t, err, "RebuildKeyspaceLocked failed: %v", err)

	// Reparent to new primary
	ti, err := ts.GetTablet(ctx, newPrimary.Tablet.Alias)
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
			tablet, err := ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
			if err != nil {
				t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
			}
			t.Fatalf("old primary (%v) should be spare but is: %v", topoproto.TabletAliasString(oldPrimary.Tablet.Alias), tablet.Type)
		}
		// check the old primary was converted to replica
		tablet, err := ts.GetTablet(ctx, oldPrimary.Tablet.Alias)
		if err != nil {
			t.Fatalf("GetTablet(%v) failed: %v", oldPrimary.Tablet.Alias, err)
		}
		if tablet.Type == topodatapb.TabletType_SPARE {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to check again */)
		}
	}

	shardInfo, err := ts.GetShard(context.Background(), newPrimary.Tablet.Keyspace, newPrimary.Tablet.Shard)
	assert.NoError(t, err)

	assert.True(t, topoproto.TabletAliasEqual(newPrimary.Tablet.Alias, shardInfo.PrimaryAlias))
	assert.Equal(t, topodatapb.TabletType_PRIMARY, newPrimary.TM.Tablet().Type)
}
