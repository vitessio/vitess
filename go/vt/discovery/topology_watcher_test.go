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

package discovery

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func checkOpCounts(t *testing.T, prevCounts, deltas map[string]int64) map[string]int64 {
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

		assert.Equal(t, newVal, prevVal+delta, "expected %v to increase by %v, got %v -> %v", key, delta, prevVal, newVal)
	}
	return newCounts
}

func checkChecksum(t *testing.T, tw *TopologyWatcher, want uint32) {
	t.Helper()
	assert.Equal(t, want, tw.TopoChecksum())
}

func TestStartAndCloseTopoWatcher(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	ts := memorytopo.NewServer(ctx, "aa")
	defer ts.Close()
	fhc := NewFakeHealthCheck(nil)
	defer fhc.Close()
	topologyWatcherOperations.ZeroAll()
	tw := NewTopologyWatcher(context.Background(), ts, fhc, nil, "aa", 100*time.Microsecond, true, 5)

	done := make(chan bool, 3)
	result := make(chan bool, 1)
	go func() {
		// We wait for the done channel three times since we execute three
		// topo-watcher actions (Start, Stop and Wait), once we have read
		// from the done channel three times we know we have completed all
		// the actions, the test is then successful.
		// Each action has a one-second timeout after which the test will be
		// marked as failed.
		for i := 0; i < 3; i++ {
			select {
			case <-time.After(1 * time.Second):
				close(result)
				return
			case <-done:
				break
			}
		}
		result <- true
	}()

	tw.Start()
	done <- true

	// This sleep gives enough time to the topo-watcher to do 10 iterations
	// The topo-watcher's refresh interval is set to 100 microseconds.
	time.Sleep(1 * time.Millisecond)

	tw.Stop()
	done <- true

	tw.wg.Wait()
	done <- true

	_, ok := <-result
	require.True(t, ok, "timed out")

}

func TestCellTabletsWatcher(t *testing.T) {
	checkWatcher(t, true)
}

func TestCellTabletsWatcherNoRefreshKnown(t *testing.T) {
	checkWatcher(t, false)
}

func checkWatcher(t *testing.T, refreshKnownTablets bool) {
	ctx := utils.LeakCheckContext(t)

	ts := memorytopo.NewServer(ctx, "aa")
	defer ts.Close()
	fhc := NewFakeHealthCheck(nil)
	defer fhc.Close()
	logger := logutil.NewMemoryLogger()
	topologyWatcherOperations.ZeroAll()
	counts := topologyWatcherOperations.Counts()
	tw := NewTopologyWatcher(context.Background(), ts, fhc, nil, "aa", 10*time.Minute, refreshKnownTablets, 5)

	counts = checkOpCounts(t, counts, map[string]int64{})
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
	require.NoError(t, ts.CreateTablet(context.Background(), tablet), "CreateTablet failed for %v", tablet.Alias)

	tw.loadTablets()
	counts = checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0, "AddTablet": 1})
	checkChecksum(t, tw, 3238442862)

	// Check the tablet is returned by GetAllTablets().
	allTablets := fhc.GetAllTablets()
	key := TabletToMapKey(tablet)
	assert.Len(t, allTablets, 1)
	assert.Contains(t, allTablets, key)
	assert.True(t, proto.Equal(tablet, allTablets[key]))

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
	require.NoError(t, ts.CreateTablet(context.Background(), tablet2), "CreateTablet failed for %v", tablet2.Alias)
	tw.loadTablets()

	counts = checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0, "AddTablet": 1})
	checkChecksum(t, tw, 2762153755)

	// Check the new tablet is returned by GetAllTablets().
	allTablets = fhc.GetAllTablets()
	key = TabletToMapKey(tablet2)
	assert.Len(t, allTablets, 2)
	assert.Contains(t, allTablets, key)
	assert.True(t, proto.Equal(tablet2, allTablets[key]))

	// same tablet, different port, should update (previous
	// one should go away, new one be added)
	//
	// if refreshKnownTablets is disabled, this case is *not*
	// detected and the tablet remains in the healthcheck using the
	// old key
	origTablet := tablet.CloneVT()
	origKey := TabletToMapKey(tablet)
	tablet.PortMap["vt"] = 456
	_, err := ts.UpdateTabletFields(context.Background(), tablet.Alias, func(t *topodatapb.Tablet) error {
		t.PortMap["vt"] = 456
		return nil
	})
	require.Nil(t, err, "UpdateTabletFields failed")

	tw.loadTablets()
	allTablets = fhc.GetAllTablets()
	key = TabletToMapKey(tablet)

	if refreshKnownTablets {
		counts = checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0, "ReplaceTablet": 1})
		assert.Len(t, allTablets, 2)
		assert.Contains(t, allTablets, key)
		assert.True(t, proto.Equal(tablet, allTablets[key]))
		assert.NotContains(t, allTablets, origKey)
		checkChecksum(t, tw, 2762153755)
	} else {
		counts = checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0, "ReplaceTablet": 0})
		assert.Len(t, allTablets, 2)
		assert.Contains(t, allTablets, origKey)
		assert.True(t, proto.Equal(origTablet, allTablets[origKey]))
		assert.NotContains(t, allTablets, key)
		checkChecksum(t, tw, 2762153755)
	}

	// Both tablets restart on different hosts.
	// tablet2 happens to land on the host:port that tablet 1 used to be on.
	// This can only be tested when we refresh known tablets.
	if refreshKnownTablets {
		origTablet := tablet.CloneVT()
		origTablet2 := tablet2.CloneVT()
		_, err := ts.UpdateTabletFields(context.Background(), tablet2.Alias, func(t *topodatapb.Tablet) error {
			t.Hostname = tablet.Hostname
			t.PortMap = tablet.PortMap
			tablet2 = t
			return nil
		})
		require.Nil(t, err, "UpdateTabletFields failed")
		_, err = ts.UpdateTabletFields(context.Background(), tablet.Alias, func(t *topodatapb.Tablet) error {
			t.Hostname = "host3"
			tablet = t
			return nil
		})
		require.Nil(t, err, "UpdateTabletFields failed")
		tw.loadTablets()
		counts = checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0, "ReplaceTablet": 2})
		allTablets = fhc.GetAllTablets()
		key2 := TabletToMapKey(tablet2)
		assert.Contains(t, allTablets, key2, "tablet was lost because it's reusing an address recently used by another tablet: %v", key2)

		// Change tablets back to avoid altering later tests.
		_, err = ts.UpdateTabletFields(context.Background(), tablet2.Alias, func(t *topodatapb.Tablet) error {
			t.Hostname = origTablet2.Hostname
			t.PortMap = origTablet2.PortMap
			tablet2 = t
			return nil
		})
		require.Nil(t, err, "UpdateTabletFields failed")

		_, err = ts.UpdateTabletFields(context.Background(), tablet.Alias, func(t *topodatapb.Tablet) error {
			t.Hostname = origTablet.Hostname
			tablet = t
			return nil
		})
		require.Nil(t, err, "UpdateTabletFields failed")

		tw.loadTablets()
		counts = checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0, "ReplaceTablet": 2})
	}

	// Remove the tablet and check that it is detected as being gone.
	require.NoError(t, ts.DeleteTablet(context.Background(), tablet.Alias))

	_, err = topo.FixShardReplication(context.Background(), ts, logger, "aa", "keyspace", "shard")
	require.Nil(t, err, "FixShardReplication failed")
	tw.loadTablets()
	counts = checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0, "RemoveTablet": 1})
	checkChecksum(t, tw, 789108290)

	allTablets = fhc.GetAllTablets()
	assert.Len(t, allTablets, 1)
	key = TabletToMapKey(tablet)
	assert.NotContains(t, allTablets, key)

	key = TabletToMapKey(tablet2)
	assert.Contains(t, allTablets, key)
	assert.True(t, proto.Equal(tablet2, allTablets[key]))

	// Remove the other and check that it is detected as being gone.
	require.NoError(t, ts.DeleteTablet(context.Background(), tablet2.Alias))
	_, err = topo.FixShardReplication(context.Background(), ts, logger, "aa", "keyspace", "shard")
	require.Nil(t, err, "FixShardReplication failed")
	tw.loadTablets()
	checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0, "RemoveTablet": 1})
	checkChecksum(t, tw, 0)

	allTablets = fhc.GetAllTablets()
	assert.Len(t, allTablets, 0)
	key = TabletToMapKey(tablet)
	assert.NotContains(t, allTablets, key)
	key = TabletToMapKey(tablet2)
	assert.NotContains(t, allTablets, key)

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
		fbs, err := NewFilterByShard(tc.filters)
		require.Nil(t, err, "cannot create FilterByShard for filters %v", tc.filters)

		tablet := &topodatapb.Tablet{
			Keyspace: tc.keyspace,
			Shard:    tc.shard,
		}
		require.Equal(t, tc.included, fbs.IsIncluded(tablet))
	}
}

var (
	testFilterByKeyspace = []struct {
		keyspace string
		expected bool
	}{
		{"ks1", true},
		{"ks2", true},
		{"ks3", false},
		{"ks4", true},
		{"ks5", true},
		{"ks6", false},
		{"ks7", false},
	}
	testKeyspacesToWatch = []string{"ks1", "ks2", "ks4", "ks5"}
	testCell             = "testCell"
	testShard            = "testShard"
	testHostName         = "testHostName"
)

func TestFilterByKeyspace(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	hc := NewFakeHealthCheck(nil)
	f := NewFilterByKeyspace(testKeyspacesToWatch)
	ts := memorytopo.NewServer(ctx, testCell)
	defer ts.Close()
	tw := NewTopologyWatcher(context.Background(), ts, hc, f, testCell, 10*time.Minute, true, 5)

	for _, test := range testFilterByKeyspace {
		// Add a new tablet to the topology.
		port := rand.Int32N(1000)
		tablet := &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: testCell,
				Uid:  rand.Uint32(),
			},
			Hostname: testHostName,
			PortMap: map[string]int32{
				"vt": port,
			},
			Keyspace: test.keyspace,
			Shard:    testShard,
		}

		assert.Equal(t, test.expected, f.IsIncluded(tablet))

		// Make this fatal because there is no point continuing if CreateTablet fails
		require.NoError(t, ts.CreateTablet(context.Background(), tablet))

		tw.loadTablets()
		key := TabletToMapKey(tablet)
		allTablets := hc.GetAllTablets()

		if test.expected {
			assert.Contains(t, allTablets, key)
		} else {
			assert.NotContains(t, allTablets, key)
		}
		assert.Equal(t, test.expected, proto.Equal(tablet, allTablets[key]))

		// Replace the tablet we added above
		tabletReplacement := &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: testCell,
				Uid:  rand.Uint32(),
			},
			Hostname: testHostName,
			PortMap: map[string]int32{
				"vt": port,
			},
			Keyspace: test.keyspace,
			Shard:    testShard,
		}
		assert.Equal(t, test.expected, f.IsIncluded(tabletReplacement))
		require.NoError(t, ts.CreateTablet(context.Background(), tabletReplacement))

		tw.loadTablets()
		key = TabletToMapKey(tabletReplacement)
		allTablets = hc.GetAllTablets()

		if test.expected {
			assert.Contains(t, allTablets, key)
		} else {
			assert.NotContains(t, allTablets, key)
		}
		assert.Equal(t, test.expected, proto.Equal(tabletReplacement, allTablets[key]))

		// Delete the tablet
		require.NoError(t, ts.DeleteTablet(context.Background(), tabletReplacement.Alias))
	}
}

// TestFilterByKeyspaceSkipsIgnoredTablets confirms a bug fix for the case when a TopologyWatcher
// has a FilterByKeyspace TabletFilter configured along with refreshKnownTablets turned off. We want
// to ensure that the TopologyWatcher:
//   - does not continuously call GetTablets for tablets that do not satisfy the filter
//   - does not add or remove these filtered out tablets from its healthcheck
func TestFilterByKeyspaceSkipsIgnoredTablets(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	ts := memorytopo.NewServer(ctx, "aa")
	defer ts.Close()
	fhc := NewFakeHealthCheck(nil)
	defer fhc.Close()
	topologyWatcherOperations.ZeroAll()
	counts := topologyWatcherOperations.Counts()
	f := NewFilterByKeyspace(testKeyspacesToWatch)
	tw := NewTopologyWatcher(context.Background(), ts, fhc, f, "aa", 10*time.Minute, false /*refreshKnownTablets*/, 5)

	counts = checkOpCounts(t, counts, map[string]int64{})
	checkChecksum(t, tw, 0)

	// Add a tablet from a tracked keyspace to the topology.
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "aa",
			Uid:  0,
		},
		Hostname: "host1",
		PortMap: map[string]int32{
			"vt": 123,
		},
		Keyspace: "ks1",
		Shard:    "shard",
	}
	require.NoError(t, ts.CreateTablet(context.Background(), tablet))

	tw.loadTablets()
	counts = checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0, "AddTablet": 1})
	checkChecksum(t, tw, 3238442862)

	// Check tablet is reported by HealthCheck
	allTablets := fhc.GetAllTablets()
	key := TabletToMapKey(tablet)
	assert.Contains(t, allTablets, key)
	assert.True(t, proto.Equal(tablet, allTablets[key]))

	// Add a second tablet to the topology that should get filtered out by the keyspace filter
	tablet2 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "aa",
			Uid:  2,
		},
		Hostname: "host2",
		PortMap: map[string]int32{
			"vt": 789,
		},
		Keyspace: "ks3",
		Shard:    "shard",
	}
	require.NoError(t, ts.CreateTablet(context.Background(), tablet2))

	tw.loadTablets()
	counts = checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0})
	checkChecksum(t, tw, 2762153755)

	// Check the new tablet is NOT reported by HealthCheck.
	allTablets = fhc.GetAllTablets()
	assert.Len(t, allTablets, 1)
	key = TabletToMapKey(tablet2)
	assert.NotContains(t, allTablets, key)

	// Load the tablets again to show that when refreshKnownTablets is disabled,
	// only the list is read from the topo and the checksum doesn't change
	tw.loadTablets()
	counts = checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0})
	checkChecksum(t, tw, 2762153755)

	// With refreshKnownTablets set to false, changes to the port map for the same tablet alias
	// should not be reflected in the HealtCheck state
	_, err := ts.UpdateTabletFields(context.Background(), tablet.Alias, func(t *topodatapb.Tablet) error {
		t.PortMap["vt"] = 456
		return nil
	})
	require.NoError(t, err)

	tw.loadTablets()
	counts = checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0})
	checkChecksum(t, tw, 2762153755)

	allTablets = fhc.GetAllTablets()
	assert.Len(t, allTablets, 1)
	origKey := TabletToMapKey(tablet)
	tabletWithNewPort := tablet.CloneVT()
	tabletWithNewPort.PortMap["vt"] = 456
	keyWithNewPort := TabletToMapKey(tabletWithNewPort)
	assert.Contains(t, allTablets, origKey)
	assert.NotContains(t, allTablets, keyWithNewPort)

	// Remove the tracked tablet from the topo and check that it is detected as being gone.
	require.NoError(t, ts.DeleteTablet(context.Background(), tablet.Alias))

	tw.loadTablets()
	counts = checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0, "RemoveTablet": 1})
	checkChecksum(t, tw, 789108290)
	assert.Empty(t, fhc.GetAllTablets())

	// Remove ignored tablet and check that we didn't try to remove it from the health check
	require.NoError(t, ts.DeleteTablet(context.Background(), tablet2.Alias))

	tw.loadTablets()
	checkOpCounts(t, counts, map[string]int64{"ListTablets": 1, "GetTablet": 0})
	checkChecksum(t, tw, 0)
	assert.Empty(t, fhc.GetAllTablets())

	tw.Stop()
}
