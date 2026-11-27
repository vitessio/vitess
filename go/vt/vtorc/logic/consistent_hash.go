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
	"strconv"

	"github.com/cespare/xxhash/v2"
)

// ringWeight returns the rendezvous-hashing (HRW) weight of a ring index for a
// keyspace/shard key. The weight depends only on the (index, key) pair and not
// on the rest of the ring, which is what keeps ownership stable as the ring
// resizes: adding or removing a different index never changes this value.
//
// xxhash is used (rather than a weaker hash such as fnv) because HRW relies on
// the per-index weights for a given key being effectively independent; xxhash's
// avalanche gives that when only the index prefix changes. This mirrors the
// rendezvous hashing already used by vtgate's balancer (tabletWeight).
func ringWeight(ringIndex int, keyspaceShard string) uint64 {
	h := xxhash.New()
	_, _ = h.WriteString(strconv.Itoa(ringIndex))
	_, _ = h.WriteString("#")
	_, _ = h.WriteString(keyspaceShard)
	return h.Sum64()
}

// higherRank reports whether candidate a outranks candidate b under HRW: the
// higher weight wins, and equal weights (a vanishingly rare 64-bit collision)
// break toward the lower index. The tie-break is deterministic so every VTOrc
// instance computes the same ordering and agrees on the watcher set.
func higherRank(weightA uint64, indexA int, weightB uint64, indexB int) bool {
	if weightA != weightB {
		return weightA > weightB
	}
	return indexA < indexB
}

// isInRingSegment reports whether the VTOrc instance with ringIndex is one of
// the watchersPerShard highest-ranked instances for the keyspace/shard under
// rendezvous hashing, i.e. whether this instance should watch that shard.
//
// Because HRW weights are independent of the candidate set, changing the ring
// by a single instance moves a shard's top-k set by at most one member, so the
// old and new watcher sets share at least k-1 instances; a resize of m shares
// at least max(0, k-m) and can become disjoint once m >= k.
// TestIsInRingSegment_ResizePreservesCoverage asserts this bound.
//
// Turning that set overlap into gap-free live coverage during a resize must
// account for the restart each instance needs to adopt new ring flags: it is
// briefly offline while restarting, so it does not watch during that window.
// With watchersPerShard >= 2, staging the rollout preserves coverage for any
// resize: bring the instances that will watch a shard under the new size
// online before the old watchers stop. To grow, start all added indices on the
// new size before restarting the incumbents onto it; to shrink, restart every
// surviving instance onto the new size before removing any old index. In
// arbitrary restart order (any one instance offline at a time) at least one
// watcher survives only when the old and new watcher sets share >= 2
// instances, i.e. a change of at most k-2 instances per rollout.
//
// When ringSize <= watchersPerShard every instance is always within the top-k,
// so all instances watch every shard — matching the full-fleet default.
// Partitioning only reduces per-instance load once ringSize > watchersPerShard.
func isInRingSegment(keyspace, shard string, ringIndex, ringSize, watchersPerShard int) bool {
	if ringSize <= watchersPerShard {
		return true
	}
	key := keyspace + "/" + shard
	self := ringWeight(ringIndex, key)
	// This instance is a watcher iff fewer than watchersPerShard other instances
	// outrank it for this key. Stop early once enough higher-ranked instances
	// are found.
	higher := 0
	for i := range ringSize {
		if i == ringIndex {
			continue
		}
		if higherRank(ringWeight(i, key), i, self, ringIndex) {
			higher++
			if higher >= watchersPerShard {
				return false
			}
		}
	}
	return true
}
