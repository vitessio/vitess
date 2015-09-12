// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"bytes"
	"sort"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// ShardReferenceArray is used for sorting ShardReference arrays
type ShardReferenceArray []*pb.ShardReference

// Len implements sort.Interface
func (sra ShardReferenceArray) Len() int { return len(sra) }

// Len implements sort.Interface
func (sra ShardReferenceArray) Less(i, j int) bool {
	return bytes.Compare(sra[i].KeyRange.Start, sra[j].KeyRange.Start) < 0
}

// Len implements sort.Interface
func (sra ShardReferenceArray) Swap(i, j int) {
	sra[i], sra[j] = sra[j], sra[i]
}

// Sort will sort the list according to KeyRange.Start
func (sra ShardReferenceArray) Sort() { sort.Sort(sra) }

// KeyspacePartitionHasShard returns true if this KeyspacePartition
// has the shard with the given name in it.
func KeyspacePartitionHasShard(kp *pb.SrvKeyspace_KeyspacePartition, name string) bool {
	for _, shardReference := range kp.ShardReferences {
		if shardReference.Name == name {
			return true
		}
	}
	return false
}
