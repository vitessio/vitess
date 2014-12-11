// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"fmt"
	"sort"

	"github.com/henryanand/vitess/go/vt/key"
	"github.com/henryanand/vitess/go/vt/topo"
)

// OverlappingShards contains sets of shards that overlap which each-other.
// With this library, there is no guarantee of which set will be left or right.
type OverlappingShards struct {
	Left  []*topo.ShardInfo
	Right []*topo.ShardInfo
}

// ContainsShard returns true if either Left or Right lists contain
// the provided Shard.
func (os *OverlappingShards) ContainsShard(shardName string) bool {
	for _, l := range os.Left {
		if l.ShardName() == shardName {
			return true
		}
	}
	for _, r := range os.Right {
		if r.ShardName() == shardName {
			return true
		}
	}
	return false
}

// OverlappingShardsForShard returns the OverlappingShards object
// from the list that has he provided shard, or nil
func OverlappingShardsForShard(os []*OverlappingShards, shardName string) *OverlappingShards {
	for _, o := range os {
		if o.ContainsShard(shardName) {
			return o
		}
	}
	return nil
}

// FindOverlappingShards will return an array of OverlappingShards
// for the provided keyspace.
// We do not support more than two overlapping shards (for instance,
// having 40-80, 40-60 and 40-50 in the same keyspace is not supported and
// will return an error).
// If shards don't perfectly overlap, they are not returned.
func FindOverlappingShards(ts topo.Server, keyspace string) ([]*OverlappingShards, error) {
	shardMap, err := topo.FindAllShardsInKeyspace(ts, keyspace)
	if err != nil {
		return nil, err
	}

	return findOverlappingShards(shardMap)
}

// findOverlappingShards does the work for FindOverlappingShards but
// can be called on test data too.
func findOverlappingShards(shardMap map[string]*topo.ShardInfo) ([]*OverlappingShards, error) {

	var result []*OverlappingShards

	for len(shardMap) > 0 {
		var left []*topo.ShardInfo
		var right []*topo.ShardInfo

		// get the first value from the map, seed our left array with it
		var name string
		var si *topo.ShardInfo
		for name, si = range shardMap {
			break
		}
		left = append(left, si)
		delete(shardMap, name)

		// keep adding entries until we have no more to add
		for {
			foundOne := false

			// try left to right
			si := findIntersectingShard(shardMap, left)
			if si != nil {
				if intersect(si, right) {
					return nil, fmt.Errorf("Shard %v interesect with more than one shard, this is not supported", si.ShardName)
				}
				foundOne = true
				right = append(right, si)
			}

			// try right to left
			si = findIntersectingShard(shardMap, right)
			if si != nil {
				if intersect(si, left) {
					return nil, fmt.Errorf("Shard %v interesect with more than one shard, this is not supported", si.ShardName)
				}
				foundOne = true
				left = append(left, si)
			}

			// we haven't found anything new, we're done
			if !foundOne {
				break
			}
		}

		// save what we found if it's good
		if len(right) > 0 {
			// sort both lists
			sort.Sort(shardInfoList(left))
			sort.Sort(shardInfoList(right))

			// we should not have holes on either side
			hasHoles := false
			for i := 0; i < len(left)-1; i++ {
				if left[i].KeyRange.End != left[i+1].KeyRange.Start {
					hasHoles = true
				}
			}
			for i := 0; i < len(right)-1; i++ {
				if right[i].KeyRange.End != right[i+1].KeyRange.Start {
					hasHoles = true
				}
			}
			if hasHoles {
				continue
			}

			// the two sides should match
			if left[0].KeyRange.Start != right[0].KeyRange.Start {
				continue
			}
			if left[len(left)-1].KeyRange.End != right[len(right)-1].KeyRange.End {
				continue
			}

			// all good, we have a valid overlap
			result = append(result, &OverlappingShards{
				Left:  left,
				Right: right,
			})
		}
	}
	return result, nil
}

// findIntersectingShard will go through the map and take the first
// entry in there that intersect with the source array, remove it from
// the map, and return it
func findIntersectingShard(shardMap map[string]*topo.ShardInfo, sourceArray []*topo.ShardInfo) *topo.ShardInfo {
	for name, si := range shardMap {
		for _, sourceShardInfo := range sourceArray {
			if key.KeyRangesIntersect(si.KeyRange, sourceShardInfo.KeyRange) {
				delete(shardMap, name)
				return si
			}
		}
	}
	return nil
}

// intersect returns true if the provided shard intersect with any shard
// in the destination array
func intersect(si *topo.ShardInfo, allShards []*topo.ShardInfo) bool {
	for _, shard := range allShards {
		if key.KeyRangesIntersect(si.KeyRange, shard.KeyRange) {
			return true
		}
	}
	return false
}

// shardInfoList is a helper type to sort ShardInfo array by keyrange
type shardInfoList []*topo.ShardInfo

// Len is part of sort.Interface
func (sil shardInfoList) Len() int {
	return len(sil)
}

// Less is part of sort.Interface
func (sil shardInfoList) Less(i, j int) bool {
	return sil[i].KeyRange.Start < sil[j].KeyRange.Start
}

// Swap is part of sort.Interface
func (sil shardInfoList) Swap(i, j int) {
	sil[i], sil[j] = sil[j], sil[i]
}
