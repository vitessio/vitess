/*
Copyright 2025 The Vitess Authors.

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

package balancer

import (
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

func createTestTabletForHashRing(cell string, uid uint32) *discovery.TabletHealth {
	tablet := topo.NewTablet(uid, cell, strconv.FormatUint(uint64(uid), 10))
	tablet.PortMap["vt"] = 1
	tablet.PortMap["grpc"] = 2
	tablet.Keyspace = "test_keyspace"
	tablet.Shard = "0"

	return &discovery.TabletHealth{
		Tablet:               tablet,
		Target:               &querypb.Target{Keyspace: "test_keyspace", Shard: "0", TabletType: topodatapb.TabletType_REPLICA},
		Serving:              true,
		Stats:                nil,
		PrimaryTermStartTime: 0,
	}
}

func TestNewHashRing(t *testing.T) {
	ring := newHashRing()

	require.NotNil(t, ring)
	require.Equal(t, defaultVirtualNodes, ring.numVirtualNodes)
	require.NotNil(t, ring.nodeMap)
	require.Empty(t, ring.nodes)
	require.Empty(t, ring.nodeMap)
}

func TestHashRingAdd(t *testing.T) {
	ring := newHashRing()
	tablet1 := createTestTabletForHashRing("cell1", 100)
	tablet2 := createTestTabletForHashRing("cell2", 200)

	ring.add(tablet1)

	require.Len(t, ring.nodes, defaultVirtualNodes)
	require.Len(t, ring.nodeMap, defaultVirtualNodes)

	for _, hash := range ring.nodes {
		require.Equal(t, tablet1, ring.nodeMap[hash])
	}

	ring.add(tablet2)

	require.Len(t, ring.nodes, 2*defaultVirtualNodes)
	require.Len(t, ring.nodeMap, 2*defaultVirtualNodes)

	tablet1Count := 0
	tablet2Count := 0
	for _, tablet := range ring.nodeMap {
		switch tablet {
		case tablet1:
			tablet1Count++
		case tablet2:
			tablet2Count++
		}
	}
	require.Equal(t, defaultVirtualNodes, tablet1Count)
	require.Equal(t, defaultVirtualNodes, tablet2Count)
}

func TestHashRingAddDuplicate(t *testing.T) {
	ring := newHashRing()
	tablet := createTestTabletForHashRing("cell1", 100)

	ring.add(tablet)
	originalLen := len(ring.nodes)

	ring.add(tablet)

	require.Len(t, ring.nodes, originalLen)
	require.Len(t, ring.nodeMap, originalLen)
}

func TestHashRingRemove(t *testing.T) {
	ring := newHashRing()
	tablet1 := createTestTabletForHashRing("cell1", 100)
	tablet2 := createTestTabletForHashRing("cell2", 200)

	ring.add(tablet1)
	ring.add(tablet2)

	require.Len(t, ring.nodes, 2*defaultVirtualNodes)

	ring.remove(tablet1)

	require.Len(t, ring.nodes, defaultVirtualNodes)
	require.Len(t, ring.nodeMap, defaultVirtualNodes)

	for _, tablet := range ring.nodeMap {
		require.Equal(t, tablet2, tablet)
	}
}

func TestHashRingRemoveNonExistent(t *testing.T) {
	ring := newHashRing()
	tablet1 := createTestTabletForHashRing("cell1", 100)
	tablet2 := createTestTabletForHashRing("cell2", 200)

	ring.add(tablet1)
	originalLen := len(ring.nodes)

	ring.remove(tablet2)

	require.Len(t, ring.nodes, originalLen)
	require.Len(t, ring.nodeMap, originalLen)
}

func TestHashRingGet(t *testing.T) {
	ring := newHashRing()
	tablet1 := createTestTabletForHashRing("cell1", 100)
	tablet2 := createTestTabletForHashRing("cell2", 200)

	result := ring.get("test_key")
	require.Nil(t, result)

	ring.add(tablet1)
	ring.sort()

	result = ring.get("test_key")
	require.NotNil(t, result)
	require.Equal(t, tablet1, result)

	ring.add(tablet2)
	ring.sort()

	result = ring.get("test_key")
	require.NotNil(t, result)

	// Empirically know that "test_key" hashes closest to tablet2
	require.Equal(t, tablet2, result)
}

func TestHashRingSort(t *testing.T) {
	ring := newHashRing()
	tablet1 := createTestTabletForHashRing("cell1", 100)
	tablet2 := createTestTabletForHashRing("cell2", 200)

	ring.add(tablet1)
	ring.add(tablet2)

	originalNodes := make([]uint64, len(ring.nodes))
	copy(originalNodes, ring.nodes)

	ring.sort()

	require.True(t, slices.IsSorted(ring.nodes))
	require.Equal(t, len(originalNodes), len(ring.nodes))

	for _, node := range originalNodes {
		require.Contains(t, ring.nodes, node)
	}
}

func TestBuildHash(t *testing.T) {
	tablet := createTestTabletForHashRing("cell1", 100)

	hash1 := buildHash(tablet, 0)
	hash2 := buildHash(tablet, 1)
	hash3 := buildHash(tablet, 0)

	require.NotEqual(t, hash1, hash2)
	require.Equal(t, hash1, hash3)

	tablet2 := createTestTabletForHashRing("cell2", 200)
	hash4 := buildHash(tablet2, 0)

	require.NotEqual(t, hash1, hash4)
}

func TestHashRingAddRemoveSequence(t *testing.T) {
	ring := newHashRing()
	tablet1 := createTestTabletForHashRing("cell1", 100)
	tablet2 := createTestTabletForHashRing("cell2", 200)
	tablet3 := createTestTabletForHashRing("cell3", 300)

	ring.add(tablet1)
	ring.add(tablet2)
	ring.add(tablet3)
	ring.sort()

	key := "test_sequence"

	initialTablet := ring.get(key)
	require.NotNil(t, initialTablet)

	ring.remove(tablet2)
	ring.sort()

	afterRemovalTablet := ring.get(key)
	require.NotNil(t, afterRemovalTablet)

	ring.add(tablet2)
	ring.sort()

	afterReaddTablet := ring.get(key)
	require.NotNil(t, afterReaddTablet)

	require.Equal(t, initialTablet, afterReaddTablet)
}

func TestHashRingWrapAround(t *testing.T) {
	ring := newHashRing()
	tablet1 := createTestTabletForHashRing("cell1", 100)
	tablet2 := createTestTabletForHashRing("cell2", 200)

	// Create a synthetic scenario where we know the hash will be larger
	ring.nodes = []uint64{1000, 2000, 3000} // Small values
	ring.nodeMap = make(map[uint64]*discovery.TabletHealth)
	ring.nodeMap[1000] = tablet1
	ring.nodeMap[2000] = tablet2
	ring.nodeMap[3000] = tablet1

	// Any large hash should wrap around to the first node
	result := ring.get("this_should_wrap_around_with_large_hash")
	require.NotNil(t, result)
	require.Contains(t, []*discovery.TabletHealth{tablet1, tablet2}, result)
}

func TestHashRingRemoveAllTablets(t *testing.T) {
	ring := newHashRing()
	tablets := make([]*discovery.TabletHealth, 3)

	for i := range 3 {
		tablets[i] = createTestTabletForHashRing(fmt.Sprintf("cell%d", i), uint32(100+i))
		ring.add(tablets[i])
	}

	ring.sort()

	for _, tablet := range tablets {
		ring.remove(tablet)
	}

	require.Empty(t, ring.nodes)
	require.Empty(t, ring.nodeMap)
	require.Nil(t, ring.get("any_key"))
}

func TestHashRingMultipleAddSameTablet(t *testing.T) {
	ring := newHashRing()
	tablet := createTestTabletForHashRing("cell1", 100)

	// Add the same tablet multiple times
	for range 5 {
		ring.add(tablet)
	}

	// Should still only have defaultVirtualNodes entries
	require.Len(t, ring.nodes, defaultVirtualNodes)
	require.Len(t, ring.nodeMap, defaultVirtualNodes)
}

func TestHashRingGetAfterRemove(t *testing.T) {
	ring := newHashRing()
	tablet1 := createTestTabletForHashRing("cell1", 100)
	tablet2 := createTestTabletForHashRing("cell2", 200)
	tablet3 := createTestTabletForHashRing("cell3", 300)

	ring.add(tablet1)
	ring.add(tablet2)
	ring.add(tablet3)
	ring.sort()

	// Empirically know that this hashes closest to tablet3
	got := ring.get("key")
	require.Equal(t, tablet3, got)

	// Remove tablet3
	ring.remove(tablet3)

	got = ring.get("key")
	require.NotEqual(t, tablet3, got)
}

func TestHashRingConcurrentGetOperations(t *testing.T) {
	ring := newHashRing()
	tablets := make([]*discovery.TabletHealth, 5)

	for i := range 5 {
		tablets[i] = createTestTabletForHashRing(fmt.Sprintf("cell%d", i), uint32(100+i))
		ring.add(tablets[i])
	}
	ring.sort()

	var wg sync.WaitGroup
	numGoroutines := 1000
	wg.Add(numGoroutines)
	for i := range numGoroutines {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent_key_%d", i)
			tablet := ring.get(key)
			require.NotNil(t, tablet)
		}(i)
	}

	wg.Wait()
}
