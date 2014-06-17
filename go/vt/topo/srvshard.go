// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"fmt"
	"sort"

	"github.com/youtube/vitess/go/vt/key"
)

// This is the shard name for when the keyrange covers the entire space
// for unsharded database.
const SHARD_ZERO = "0"

// SrvShard contains a roll-up of the shard in the local namespace.
// In zk, it is under /zk/<cell>/vt/ns/<keyspace>/<shard>
type SrvShard struct {
	// Copied / infered from Shard
	Name        string
	KeyRange    key.KeyRange
	ServedTypes []TabletType

	// TabletTypes represents the list of types we have serving tablets
	// for, in this cell only.
	TabletTypes []TabletType

	// For atomic updates
	version int64
}

// SrvShardArray is used for sorting SrvShard arrays
type SrvShardArray []SrvShard

// Len implements sort.Interface
func (sa SrvShardArray) Len() int { return len(sa) }

// Len implements sort.Interface
func (sa SrvShardArray) Less(i, j int) bool {
	return sa[i].KeyRange.Start < sa[j].KeyRange.Start
}

// Len implements sort.Interface
func (sa SrvShardArray) Swap(i, j int) {
	sa[i], sa[j] = sa[j], sa[i]
}

func (sa SrvShardArray) Sort() { sort.Sort(sa) }

// NewSrvShard returns an empty SrvShard with the given version.
func NewSrvShard(version int64) *SrvShard {
	return &SrvShard{
		version: version,
	}
}

// ShardName returns the name of a shard.
func (ss *SrvShard) ShardName() string {
	if ss.Name != "" {
		return ss.Name
	}
	if !ss.KeyRange.IsPartial() {
		return SHARD_ZERO
	}
	return fmt.Sprintf("%v-%v", string(ss.KeyRange.Start.Hex()), string(ss.KeyRange.End.Hex()))
}

// KeyspacePartition represents a continuous set of shards to
// serve an entire data set.
type KeyspacePartition struct {
	// List of non-overlapping continuous shards sorted by range.
	Shards []SrvShard
}

// HasShard returns true if this KeyspacePartition has the shard with
// the given name in it.
func (kp *KeyspacePartition) HasShard(name string) bool {
	for _, srvShard := range kp.Shards {
		if srvShard.ShardName() == name {
			return true
		}
	}
	return false
}

// A distilled serving copy of keyspace detail stored in the local
// cell for fast access. Derived from the global keyspace, shards and
// local details.
// In zk, it is in /zk/<cell>/vt/ns/<keyspace>
type SrvKeyspace struct {
	// Shards to use per type, only contains complete partitions.
	Partitions map[TabletType]*KeyspacePartition

	// This list will be deprecated as soon as Partitions is used.
	// List of non-overlapping shards sorted by range.
	Shards []SrvShard

	// List of available tablet types for this keyspace in this cell.
	// May not have a server for every shard, but we have some.
	TabletTypes []TabletType

	// Copied from Keyspace
	ShardingColumnName string
	ShardingColumnType key.KeyspaceIdType
	ServedFrom         map[TabletType]string

	// For atomic updates
	version int64
}

// NewSrvKeyspace returns an empty SrvKeyspace with the given version.
func NewSrvKeyspace(version int64) *SrvKeyspace {
	return &SrvKeyspace{
		version: version,
	}
}
