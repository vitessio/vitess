/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package discovery

import (
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func checkOpCounts(t *testing.T, tw *TopologyWatcher, prevCounts, deltas map[string]int64) map[string]int64 {
	t.Helper()
	newCounts := topologyWatcherOperations.Counts()
	for key, prevVal := range prevCounts {
		delta, ok := deltas[key]
		if !ok {
			delta = 0
		}
		newVal, ok := newCounts[key]
		if !ok {
			newVal = 0
		}

		if newVal != prevVal+delta {
			t.Errorf("expected %v to increase by %v, got %v -> %v", key, delta, prevVal, newVal)
		}
	}
	return newCounts
}

func checkChecksum(t *testing.T, tw *TopologyWatcher, want uint32) {
	t.Helper()
	got := tw.TopoChecksum()
	if want != got {
		t.Errorf("want checksum %v got %v", want, got)
	}
}

func TestCellTabletsWatcher(t *testing.T) {
	checkWatcher(t, true, true)
}

func TestCellTabletsWatcherNoRefreshKnown(t *testing.T) {
	checkWatcher(t, true, false)
}

func TestShardReplicationWatcher(t *testing.T) {
	checkWatcher(t, false, true)
}

func checkWatcher(t *testing.T, cellTablets, refreshKnownTablets bool) {
	ts := memorytopo.NewServer("aa")
	fhc := NewFakeHealthCheck()
	logger := logutil.NewMemoryLogger()
	topologyWatcherOperations.ZeroAll()
	counts := topologyWatcherOperations.Counts()
	var tw *TopologyWatcher
	if cellTablets {
		tw = NewCellTabletsWatcher(context.Background(), ts, fhc, "aa", 10*time.Minute, refreshKnownTablets, 5)
	} else {
		tw = NewShardReplicationWatcher(context.Background(), ts, fhc, "aa", "keyspace", "shard", 10*time.Minute, 5)
	}

	// Wait for the initial topology load to finish. Otherwise we
	// have a background loadTablets() that's running, and it can
	// interact with our tests in weird ways.
	if err := tw.WaitForInitialTopology(); err != nil {
		t.Fatalf("initial WaitForInitialTopology failed")
	}
	counts = checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1})
	checkChecksum(t, tw, 0)

	// Add a tablet to the topology.
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "aa",
			Uid:  0,
		},
		Hostname: "host1",
		PortMap: map[string]int32{
			"vt": 123,
		},
		Keyspace: "keyspace",
		Shard:    "shard",
	}
	if err := ts.CreateTablet(context.Background(), tablet); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}
	tw.loadTablets()
	counts = checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1, "GetTablet": 1, "AddTablet": 1})
	checkChecksum(t, tw, 1261153186)

	// Check the tablet is returned by GetAllTablets().
	allTablets := fhc.GetAllTablets()
	key := TabletToMapKey(tablet)
	if _, ok := allTablets[key]; !ok || len(allTablets) != 1 || !proto.Equal(allTablets[key], tablet) {
		t.Errorf("fhc.GetAllTablets() = %+v; want %+v", allTablets, tablet)
	}

	// Add a second tablet to the topology.
	tablet2 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "aa",
			Uid:  2,
		},
		Hostname: "host2",
		PortMap: map[string]int32{
			"vt": 789,
		},
		Keyspace: "keyspace",
		Shard:    "shard",
	}
	if err := ts.CreateTablet(context.Background(), tablet2); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}
	tw.loadTablets()

	// If refreshKnownTablets is disabled, only the new tablet is read
	// from the topo
	if refreshKnownTablets {
		counts = checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1, "GetTablet": 2, "AddTablet": 1})
	} else {
		counts = checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1, "GetTablet": 1, "AddTablet": 1})
	}
	checkChecksum(t, tw, 832404892)

	// Check the new tablet is returned by GetAllTablets().
	allTablets = fhc.GetAllTablets()
	key = TabletToMapKey(tablet2)
	if _, ok := allTablets[key]; !ok || len(allTablets) != 2 || !proto.Equal(allTablets[key], tablet2) {
		t.Errorf("fhc.GetAllTablets() = %+v; want %+v", allTablets, tablet2)
	}

	// Load the tablets again to show that when refreshKnownTablets is disabled,
	// only the list is read from the topo and the checksum doesn't change
	tw.loadTablets()
	if refreshKnownTablets {
		counts = checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1, "GetTablet": 2})
	} else {
		counts = checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1})
	}
	checkChecksum(t, tw, 832404892)

	// same tablet, different port, should update (previous
	// one should go away, new one be added)
	//
	// if refreshKnownTablets is disabled, this case is *not*
	// detected and the tablet remains in the topo using the
	// old key
	origTablet := proto.Clone(tablet).(*topodatapb.Tablet)
	origKey := TabletToMapKey(tablet)
	tablet.PortMap["vt"] = 456
	if _, err := ts.UpdateTabletFields(context.Background(), tablet.Alias, func(t *topodatapb.Tablet) error {
		t.PortMap["vt"] = 456
		return nil
	}); err != nil {
		t.Fatalf("UpdateTabletFields failed: %v", err)
	}
	tw.loadTablets()
	allTablets = fhc.GetAllTablets()
	key = TabletToMapKey(tablet)

	if refreshKnownTablets {
		counts = checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1, "GetTablet": 2, "ReplaceTablet": 1})

		if _, ok := allTablets[key]; !ok || len(allTablets) != 2 || !proto.Equal(allTablets[key], tablet) {
			t.Errorf("fhc.GetAllTablets() = %+v; want %+v", allTablets, tablet)
		}
		if _, ok := allTablets[origKey]; ok {
			t.Errorf("fhc.GetAllTablets() = %+v; don't want %v", allTablets, origKey)
		}
		checkChecksum(t, tw, 698548794)
	} else {
		counts = checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1})

		if _, ok := allTablets[origKey]; !ok || len(allTablets) != 2 || !proto.Equal(allTablets[origKey], origTablet) {
			t.Errorf("fhc.GetAllTablets() = %+v; want %+v", allTablets, origTablet)
		}
		if _, ok := allTablets[key]; ok {
			t.Errorf("fhc.GetAllTablets() = %+v; don't want %v", allTablets, key)
		}
		checkChecksum(t, tw, 832404892)
	}

	// Remove the second tablet and re-add with a new uid. This should
	// trigger a ReplaceTablet in loadTablets because the uid does not
	// match.
	//
	// This case *is* detected even if refreshKnownTablets is false
	// because the delete tablet / create tablet sequence causes the
	// list of tablets to change and therefore the change is detected.
	if err := ts.DeleteTablet(context.Background(), tablet2.Alias); err != nil {
		t.Fatalf("DeleteTablet failed: %v", err)
	}
	tablet2.Alias.Uid = 3
	if err := ts.CreateTablet(context.Background(), tablet2); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}
	if err := topo.FixShardReplication(context.Background(), ts, logger, "aa", "keyspace", "shard"); err != nil {
		t.Fatalf("FixShardReplication failed: %v", err)
	}
	tw.loadTablets()
	allTablets = fhc.GetAllTablets()

	if refreshKnownTablets {
		counts = checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1, "GetTablet": 2, "ReplaceTablet": 1})
		checkChecksum(t, tw, 4097170367)
	} else {
		counts = checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1, "GetTablet": 1, "ReplaceTablet": 1})
		checkChecksum(t, tw, 3960185881)
	}
	key = TabletToMapKey(tablet2)
	if _, ok := allTablets[key]; !ok || len(allTablets) != 2 || !proto.Equal(allTablets[key], tablet2) {
		t.Errorf("fhc.GetAllTablets() = %+v; want %v => %+v", allTablets, key, tablet2)
	}

	// Remove the tablet and check that it is detected as being gone.
	if err := ts.DeleteTablet(context.Background(), tablet.Alias); err != nil {
		t.Fatalf("DeleteTablet failed: %v", err)
	}
	if err := topo.FixShardReplication(context.Background(), ts, logger, "aa", "keyspace", "shard"); err != nil {
		t.Fatalf("FixShardReplication failed: %v", err)
	}
	tw.loadTablets()
	if refreshKnownTablets {
		counts = checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1, "GetTablet": 1, "RemoveTablet": 1})
	} else {
		counts = checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1, "RemoveTablet": 1})
	}
	checkChecksum(t, tw, 1725545897)

	allTablets = fhc.GetAllTablets()
	key = TabletToMapKey(tablet)
	if _, ok := allTablets[key]; ok || len(allTablets) != 1 {
		t.Errorf("fhc.GetAllTablets() = %+v; don't want %v", allTablets, key)
	}
	key = TabletToMapKey(tablet2)
	if _, ok := allTablets[key]; !ok || len(allTablets) != 1 || !proto.Equal(allTablets[key], tablet2) {
		t.Errorf("fhc.GetAllTablets() = %+v; want %+v", allTablets, tablet2)
	}

	// Remove the other and check that it is detected as being gone.
	if err := ts.DeleteTablet(context.Background(), tablet2.Alias); err != nil {
		t.Fatalf("DeleteTablet failed: %v", err)
	}
	if err := topo.FixShardReplication(context.Background(), ts, logger, "aa", "keyspace", "shard"); err != nil {
		t.Fatalf("FixShardReplication failed: %v", err)
	}
	tw.loadTablets()
	checkOpCounts(t, tw, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0, "RemoveTablet": 1})
	checkChecksum(t, tw, 0)

	allTablets = fhc.GetAllTablets()
	key = TabletToMapKey(tablet)
	if _, ok := allTablets[key]; ok || len(allTablets) != 0 {
		t.Errorf("fhc.GetAllTablets() = %+v; don't want %v", allTablets, key)
	}
	key = TabletToMapKey(tablet2)
	if _, ok := allTablets[key]; ok || len(allTablets) != 0 {
		t.Errorf("fhc.GetAllTablets() = %+v; don't want %v", allTablets, key)
	}

	tw.Stop()
}

func TestFilterByShard(t *testing.T) {
	testcases := []struct {
		filters  []string
		keyspace string
		shard    string
		included bool
	}{
		// un-sharded keyspaces
		{
			filters:  []string{"ks1|0"},
			keyspace: "ks1",
			shard:    "0",
			included: true,
		},
		{
			filters:  []string{"ks1|0"},
			keyspace: "ks2",
			shard:    "0",
			included: false,
		},
		// custom sharding, different shard
		{
			filters:  []string{"ks1|0"},
			keyspace: "ks1",
			shard:    "1",
			included: false,
		},
		// keyrange based sharding
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "0",
			included: false,
		},
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "-40",
			included: true,
		},
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "-80",
			included: true,
		},
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "80-",
			included: false,
		},
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "c0-",
			included: false,
		},
	}

	for _, tc := range testcases {
		fbs, err := NewFilterByShard(nil, tc.filters)
		if err != nil {
			t.Errorf("cannot create FilterByShard for filters %v: %v", tc.filters, err)
		}

		tablet := &topodatapb.Tablet{
			Keyspace: tc.keyspace,
			Shard:    tc.shard,
		}

		got := fbs.isIncluded(tablet)
		if got != tc.included {
			t.Errorf("isIncluded(%v,%v) for filters %v returned %v but expected %v", tc.keyspace, tc.shard, tc.filters, got, tc.included)
		}
	}
}
