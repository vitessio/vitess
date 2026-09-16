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
	"hash/fnv"
	"sync/atomic"
)

// bucketAssignments maps virtual bucket index → ring partition index.
// When set, computePrimary uses it instead of direct hash modulo. Loaded from
// --vtorc-ring-assignments-file at startup; a nil pointer means pure hash mode.
//
// It is published through an atomic.Pointer because loadRingAssignmentsFile runs
// inside the discovery goroutine while the /debug/vars handler may concurrently
// read it via the VtorcRingBuckets gauge.
var bucketAssignments atomic.Pointer[[]int]

// computePrimary returns the primary ring partition index for the given
// keyspace/shard. When a bucket assignment file is loaded, the key is hashed
// into a virtual bucket and the file's mapping determines the partition.
// Otherwise, the partition is derived directly from the hash modulo ringSize.
func computePrimary(keyspace, shard string, ringSize int) int {
	h := fnv.New32a()
	h.Write([]byte(keyspace + "/" + shard))
	// int is 64-bit on the platforms VTOrc runs on, so int(h.Sum32()) is the
	// non-negative hash and the modulus never narrows ringSize to uint32.
	if ba := bucketAssignments.Load(); ba != nil {
		bucket := int(h.Sum32()) % len(*ba)
		return (*ba)[bucket]
	}
	return int(h.Sum32()) % ringSize
}

// isInRingSegment reports whether the VTOrc instance with the given ringIndex
// should watch the keyspace/shard pair based on consistent hash ring assignment.
//
// Each shard has exactly one primary owner determined by computePrimary. The two
// ring-adjacent instances (left and right neighbors) also watch the segment for
// HA, giving three-way coverage per shard at all times.
//
// Ring sizes of 3 or fewer always produce full overlap (every instance is
// simultaneously primary and neighbor for every shard), so partitioning only
// takes effect at ringSize >= 4. For ringSize <= 3 this function always
// returns true, matching the full-fleet default behavior.
func isInRingSegment(keyspace, shard string, ringIndex, ringSize int) bool {
	if ringSize <= 3 {
		return true
	}
	primary := computePrimary(keyspace, shard, ringSize)
	left := (primary + 1) % ringSize
	right := (primary - 1 + ringSize) % ringSize
	return ringIndex == primary || ringIndex == left || ringIndex == right
}
