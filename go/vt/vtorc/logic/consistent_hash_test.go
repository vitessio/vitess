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

package logic

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// watchersFor returns the set of ring indices that watch the given
// keyspace/shard for a ring of ringSize with watchersPerShard watchers.
func watchersFor(keyspace, shard string, ringSize, watchersPerShard int) map[int]bool {
	watchers := make(map[int]bool)
	for idx := 0; idx < ringSize; idx++ {
		if isInRingSegment(keyspace, shard, idx, ringSize, watchersPerShard) {
			watchers[idx] = true
		}
	}
	return watchers
}

func TestRingWeight_Deterministic(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.Equal(t, ringWeight(3, "ks/0"), ringWeight(3, "ks/0"),
			"same (index, key) must produce the same weight")
	}
}

func TestRingWeight_VariesWithIndexAndKey(t *testing.T) {
	assert.NotEqual(t, ringWeight(0, "ks/0"), ringWeight(1, "ks/0"),
		"different indices should (almost always) differ")
	assert.NotEqual(t, ringWeight(0, "ks/0"), ringWeight(0, "ks/1"),
		"different keys should (almost always) differ")
}

func TestHigherRank(t *testing.T) {
	// Higher weight wins regardless of index.
	assert.True(t, higherRank(10, 5, 9, 0), "greater weight outranks")
	assert.False(t, higherRank(9, 0, 10, 5), "lesser weight does not outrank")
	// Equal weights break toward the lower index.
	assert.True(t, higherRank(7, 1, 7, 2), "tie: lower index outranks higher index")
	assert.False(t, higherRank(7, 2, 7, 1), "tie: higher index does not outrank lower index")
	assert.False(t, higherRank(7, 2, 7, 2), "identical candidate does not outrank itself")
}

// TestIsInRingSegment_NoOpWhenRingSizeAtOrBelowWatchers verifies that when the
// ring is no larger than the watcher count, every instance watches everything.
func TestIsInRingSegment_NoOpWhenRingSizeAtOrBelowWatchers(t *testing.T) {
	for _, watchersPerShard := range []int{1, 3} {
		for ringSize := 1; ringSize <= watchersPerShard; ringSize++ {
			for idx := 0; idx < ringSize; idx++ {
				assert.True(t, isInRingSegment("ks", "0", idx, ringSize, watchersPerShard),
					"ringSize=%d watchers=%d idx=%d must watch everything", ringSize, watchersPerShard, idx)
			}
		}
	}
}

func TestIsInRingSegment_Deterministic(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.Equal(t,
			isInRingSegment("mykeyspace", "0", 2, 8, 3),
			isInRingSegment("mykeyspace", "0", 2, 8, 3),
			"repeated calls must return the same result")
	}
}

// TestIsInRingSegment_ExactlyKWatchersPerShard verifies that once the ring is
// larger than the watcher count, every shard is watched by exactly
// watchersPerShard instances.
func TestIsInRingSegment_ExactlyKWatchersPerShard(t *testing.T) {
	for _, watchersPerShard := range []int{1, 2, 3, 4} {
		for _, ringSize := range []int{watchersPerShard + 1, 8, 12} {
			for i := 0; i < 100; i++ {
				ks := fmt.Sprintf("keyspace%d", i)
				watchers := watchersFor(ks, "0", ringSize, watchersPerShard)
				assert.Len(t, watchers, watchersPerShard,
					"ringSize=%d watchers=%d: shard %s/0 must have exactly %d watchers",
					ringSize, watchersPerShard, ks, watchersPerShard)
			}
		}
	}
}

// TestIsInRingSegment_NoShardOrphaned verifies every shard keeps at least one
// watcher across a range of ring sizes.
func TestIsInRingSegment_NoShardOrphaned(t *testing.T) {
	const watchersPerShard = 3
	for _, ringSize := range []int{1, 4, 5, 10, 25} {
		for i := 0; i < 200; i++ {
			ks := fmt.Sprintf("keyspace%d", i)
			assert.NotEmpty(t, watchersFor(ks, "0", ringSize, watchersPerShard),
				"ringSize=%d: shard %s/0 must have at least one watcher", ringSize, ks)
		}
	}
}

// TestIsInRingSegment_ResizePreservesCoverage is the core property: growing the
// ring from N to N+1 keeps at least watchersPerShard-1 of a shard's watchers,
// so a rolling resize never drops coverage below that. This is what rendezvous
// hashing buys over modulo, where nearly every shard would remap at once.
func TestIsInRingSegment_ResizePreservesCoverage(t *testing.T) {
	const watchersPerShard = 3
	for ringSize := watchersPerShard; ringSize < 40; ringSize++ {
		for i := 0; i < 200; i++ {
			ks := fmt.Sprintf("keyspace%d", i)
			before := watchersFor(ks, "0", ringSize, watchersPerShard)
			after := watchersFor(ks, "0", ringSize+1, watchersPerShard)

			shared := 0
			for idx := range before {
				if after[idx] {
					shared++
				}
			}
			assert.GreaterOrEqual(t, shared, watchersPerShard-1,
				"resize %d->%d for %s/0 must retain >= %d watchers (before=%v after=%v)",
				ringSize, ringSize+1, ks, watchersPerShard-1, before, after)
		}
	}
}

// TestIsInRingSegment_ReasonableDistribution verifies load is spread across
// instances without any instance carrying far more than its share.
func TestIsInRingSegment_ReasonableDistribution(t *testing.T) {
	const (
		ringSize         = 8
		watchersPerShard = 3
		numKeyspaces     = 1000
	)
	counts := make([]int, ringSize)
	for i := 0; i < numKeyspaces; i++ {
		ks := fmt.Sprintf("keyspace%d", i)
		for idx := range watchersFor(ks, "0", ringSize, watchersPerShard) {
			counts[idx]++
		}
	}
	// Each instance watches ~watchersPerShard/ringSize of the fleet.
	expected := numKeyspaces * watchersPerShard / ringSize
	for idx, count := range counts {
		assert.InDelta(t, expected, count, float64(expected)*0.3,
			"instance %d watches %d shards; expected ~%d +/-30%%", idx, count, expected)
	}
}

func TestValidateRingConfig(t *testing.T) {
	tests := []struct {
		name             string
		ringSize         int
		ringIndex        int
		watchersPerShard int
		wantErr          string
	}{
		{name: "valid disabled", ringSize: 1, ringIndex: 0, watchersPerShard: 3},
		{name: "valid partitioned", ringSize: 8, ringIndex: 7, watchersPerShard: 3},
		{name: "ring size zero", ringSize: 0, ringIndex: 0, watchersPerShard: 3, wantErr: "--vtorc-ring-size must be >= 1"},
		{name: "index negative", ringSize: 4, ringIndex: -1, watchersPerShard: 3, wantErr: "out of range"},
		{name: "index too large", ringSize: 4, ringIndex: 4, watchersPerShard: 3, wantErr: "out of range"},
		{name: "watchers zero", ringSize: 4, ringIndex: 0, watchersPerShard: 0, wantErr: "--vtorc-ring-watchers-per-shard must be >= 1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRingConfig(tc.ringSize, tc.ringIndex, tc.watchersPerShard)
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestLogRingConfig exercises each branch of logRingConfig for coverage; it
// asserts the calls do not panic across the disabled, no-op, and partitioned
// configurations.
func TestLogRingConfig(t *testing.T) {
	origSize, origIndex, origWatchers := ringSize, ringIndex, ringWatchersPerShard
	t.Cleanup(func() {
		ringSize, ringIndex, ringWatchersPerShard = origSize, origIndex, origWatchers
	})

	cases := [][3]int{
		{1, 0, 3}, // disabled
		{3, 0, 3}, // no-op (ring-size <= watchers)
		{8, 1, 3}, // partitioned
	}
	for _, c := range cases {
		ringSize, ringIndex, ringWatchersPerShard = c[0], c[1], c[2]
		assert.NotPanics(t, logRingConfig)
	}
}
