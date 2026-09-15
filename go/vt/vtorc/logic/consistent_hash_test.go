/*
Copyright 2020 The Vitess Authors.

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

package logic

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIsInRingSegment_NoPartitioningBelowThreshold verifies that ring sizes of
// 3 or fewer always return true regardless of index, matching the full-fleet
// default. At these sizes every instance is simultaneously primary and neighbor
// for every shard so there is no load-reduction benefit.
func TestIsInRingSegment_NoPartitioningBelowThreshold(t *testing.T) {
	for _, ringSize := range []int{0, 1, 2, 3} {
		for ringIndex := 0; ringIndex < max(ringSize, 1); ringIndex++ {
			assert.True(t, isInRingSegment("ks", "0", ringIndex, ringSize),
				"ringSize=%d ringIndex=%d should always return true", ringSize, ringIndex)
		}
	}
}

func TestIsInRingSegment_Deterministic(t *testing.T) {
	for i := 0; i < 10; i++ {
		first := isInRingSegment("mykeyspace", "0", 2, 5)
		assert.Equal(t, first, isInRingSegment("mykeyspace", "0", 2, 5),
			"repeated calls must return the same result")
	}
}

// TestIsInRingSegment_PartitioningActivatesAtFour verifies that ring-size=4 is
// the minimum at which actual partitioning occurs (each instance watches 3 of
// 4 segments, not all of them).
func TestIsInRingSegment_PartitioningActivatesAtFour(t *testing.T) {
	const ringSize = 4
	// Pick a keyspace whose primary owner is instance 0 so we can assert that
	// instance 0 does NOT watch it. We probe until we find one.
	var unownedKS string
	for i := 0; i < 1000; i++ {
		ks := fmt.Sprintf("keyspace%d", i)
		// count how many instances watch this shard
		watchers := 0
		for idx := 0; idx < ringSize; idx++ {
			if isInRingSegment(ks, "0", idx, ringSize) {
				watchers++
			}
		}
		if watchers == 3 {
			// find the one that doesn't watch
			for idx := 0; idx < ringSize; idx++ {
				if !isInRingSegment(ks, "0", idx, ringSize) {
					unownedKS = ks
					break
				}
			}
			break
		}
	}
	assert.NotEmpty(t, unownedKS, "expected to find at least one shard not watched by all instances at ring-size=4")
}

// TestIsInRingSegment_ExactlyThreeWatchersPerShard verifies that for ringSize >= 4
// (the minimum effective ring size), every shard is watched by exactly 3 instances
// (primary + 2 neighbors).
func TestIsInRingSegment_ExactlyThreeWatchersPerShard(t *testing.T) {
	for _, ringSize := range []int{4, 5, 7, 10} {
		keyspaces := make([]string, 50)
		for i := range keyspaces {
			keyspaces[i] = fmt.Sprintf("keyspace%d", i)
		}

		for _, ks := range keyspaces {
			watchers := 0
			for idx := 0; idx < ringSize; idx++ {
				if isInRingSegment(ks, "0", idx, ringSize) {
					watchers++
				}
			}
			assert.Equal(t, 3, watchers,
				"ringSize=%d: shard %s/0 must have exactly 3 watchers, got %d", ringSize, ks, watchers)
		}
	}
}

// TestIsInRingSegment_NoShardIsOrphaned verifies that every shard has at least
// one watcher regardless of ring size, including effective ring sizes.
func TestIsInRingSegment_NoShardIsOrphaned(t *testing.T) {
	for _, ringSize := range []int{1, 4, 5, 10} {
		for i := 0; i < 100; i++ {
			ks := fmt.Sprintf("keyspace%d", i)
			watched := false
			for idx := 0; idx < ringSize; idx++ {
				if isInRingSegment(ks, "0", idx, ringSize) {
					watched = true
					break
				}
			}
			assert.True(t, watched,
				"ringSize=%d: shard %s/0 must be watched by at least one instance", ringSize, ks)
		}
	}
}

// TestComputePrimary_BucketAssignmentOverridesHash verifies that when
// bucketAssignments is set, computePrimary uses the file mapping instead of
// direct hash modulo.
func TestComputePrimary_BucketAssignmentOverridesHash(t *testing.T) {
	const ringSize = 5
	// Build a trivial assignment that routes everything to partition 3.
	orig := bucketAssignments
	defer func() { bucketAssignments = orig }()

	bucketAssignments = make([]int, 256)
	for i := range bucketAssignments {
		bucketAssignments[i] = 3
	}

	for i := 0; i < 50; i++ {
		ks := fmt.Sprintf("keyspace%d", i)
		p := computePrimary(ks, "0", ringSize)
		assert.Equal(t, 3, p, "bucket assignment should route %s/0 to partition 3", ks)
	}
}

// TestComputePrimary_NilBucketsFallsBackToHash verifies that when no bucket
// assignment is loaded, computePrimary uses direct hash modulo.
func TestComputePrimary_NilBucketsFallsBackToHash(t *testing.T) {
	const ringSize = 5
	orig := bucketAssignments
	defer func() { bucketAssignments = orig }()
	bucketAssignments = nil

	// Same key must always map to same partition and stay in [0, ringSize).
	for i := 0; i < 100; i++ {
		ks := fmt.Sprintf("keyspace%d", i)
		p := computePrimary(ks, "0", ringSize)
		assert.GreaterOrEqual(t, p, 0, "partition must be non-negative")
		assert.Less(t, p, ringSize, "partition must be < ringSize")
		assert.Equal(t, p, computePrimary(ks, "0", ringSize), "computePrimary must be deterministic")
	}
}

// TestIsInRingSegment_BucketAssignmentRespectedByWatchDecision verifies that
// isInRingSegment correctly uses the bucket assignment when computing neighbors.
func TestIsInRingSegment_BucketAssignmentRespectedByWatchDecision(t *testing.T) {
	const ringSize = 5
	orig := bucketAssignments
	defer func() { bucketAssignments = orig }()

	// Route everything to partition 2. Neighbors are 1 and 3; partition 0 and 4 never watch.
	bucketAssignments = make([]int, 256)
	for i := range bucketAssignments {
		bucketAssignments[i] = 2
	}

	for i := 0; i < 20; i++ {
		ks := fmt.Sprintf("keyspace%d", i)
		assert.True(t, isInRingSegment(ks, "0", 1, ringSize), "left neighbor (p1) must watch")
		assert.True(t, isInRingSegment(ks, "0", 2, ringSize), "primary owner (p2) must watch")
		assert.True(t, isInRingSegment(ks, "0", 3, ringSize), "right neighbor (p3) must watch")
		assert.False(t, isInRingSegment(ks, "0", 0, ringSize), "p0 must not watch")
		assert.False(t, isInRingSegment(ks, "0", 4, ringSize), "p4 must not watch")
	}
}

// TestIsInRingSegment_ReasonableDistribution verifies that load is spread
// across instances with no instance handling more than 2x the average.
func TestIsInRingSegment_ReasonableDistribution(t *testing.T) {
	const (
		ringSize     = 5
		numKeyspaces = 500
	)
	counts := make([]int, ringSize)
	for i := 0; i < numKeyspaces; i++ {
		ks := fmt.Sprintf("keyspace%d", i)
		for idx := 0; idx < ringSize; idx++ {
			if isInRingSegment(ks, "0", idx, ringSize) {
				counts[idx]++
			}
		}
	}
	// Each instance is primary for ~1/N of shards, so it watches ~3/N of the total.
	// With 500 keyspaces and 5 replicas, each should watch ~300 (3/5 * 500).
	expected := numKeyspaces * 3 / ringSize
	for idx, count := range counts {
		assert.InDelta(t, expected, count, float64(expected)*0.3,
			"instance %d watches %d shards; expected ~%d ±30%%", idx, count, expected)
	}
}
