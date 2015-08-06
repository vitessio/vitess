// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"sort"

	"github.com/youtube/vitess/go/vt/key"
)

// ShardReference is the structure used by SrvKeyspace to point to a Shard
type ShardReference struct {
	// Copied / inferred from Shard
	Name     string
	KeyRange key.KeyRange
}

//go:generate bsongen -file $GOFILE -type ShardReference -o shard_reference_bson.go

// ShardReferenceArray is used for sorting ShardReference arrays
type ShardReferenceArray []ShardReference

// Len implements sort.Interface
func (sra ShardReferenceArray) Len() int { return len(sra) }

// Len implements sort.Interface
func (sra ShardReferenceArray) Less(i, j int) bool {
	return sra[i].KeyRange.Start < sra[j].KeyRange.Start
}

// Len implements sort.Interface
func (sra ShardReferenceArray) Swap(i, j int) {
	sra[i], sra[j] = sra[j], sra[i]
}

// Sort will sort the list according to KeyRange.Start
func (sra ShardReferenceArray) Sort() { sort.Sort(sra) }

// KeyspacePartition represents a continuous set of shards to
// serve an entire data set.
type KeyspacePartition struct {
	// List of non-overlapping continuous shard references sorted by range.
	ShardReferences []ShardReference
}

//go:generate bsongen -file $GOFILE -type KeyspacePartition -o keyspace_partition_bson.go

// HasShard returns true if this KeyspacePartition has the shard with
// the given name in it.
func (kp *KeyspacePartition) HasShard(name string) bool {
	for _, shardReference := range kp.ShardReferences {
		if shardReference.Name == name {
			return true
		}
	}
	return false
}

// SrvKeyspace is a distilled serving copy of keyspace detail stored
// in the local cell for fast access. Derived from the global
// keyspace, shards and local details.
// By design, it should not contain details about the Shards themselves,
// but just which shards to use for serving.
// FIXME(alainjobart) KeyspacePartition has SrvShard, to be replaced by
// ShardReference.
// In zk, it is in /zk/<cell>/vt/ns/<keyspace>
type SrvKeyspace struct {
	// Shards to use per type, only contains complete partitions.
	Partitions map[TabletType]*KeyspacePartition

	// Copied from Keyspace
	ShardingColumnName string
	ShardingColumnType key.KeyspaceIdType
	ServedFrom         map[TabletType]string
	SplitShardCount    int32

	// For atomic updates
	version int64
}

//go:generate bsongen -file $GOFILE -type SrvKeyspace -o srvkeyspace_bson.go

// NewSrvKeyspace returns an empty SrvKeyspace with the given version.
func NewSrvKeyspace(version int64) *SrvKeyspace {
	return &SrvKeyspace{
		version: version,
	}
}
