// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"sort"

	"github.com/youtube/vitess/go/vt/key"
)

// SrvShard contains a roll-up of the shard in the local namespace.
// In zk, it is under /zk/local/vt/ns/<keyspace>/<shard>
type SrvShard struct {
	// Copied from Shard
	KeyRange    key.KeyRange
	ServedTypes []TabletType

	// This is really keyed by TabletType, but the json marshaller doesn't like
	// that since it requires all keys to be "string" - not "string-ish".
	AddrsByType map[string]VtnsAddrs

	// True if the master cannot process writes
	ReadOnly bool
	version  int64
}

type SrvShardArray []SrvShard

func (sa SrvShardArray) Len() int { return len(sa) }

func (sa SrvShardArray) Less(i, j int) bool {
	return sa[i].KeyRange.Start < sa[j].KeyRange.Start
}

func (sa SrvShardArray) Swap(i, j int) {
	sa[i], sa[j] = sa[j], sa[i]
}

func (sa SrvShardArray) Sort() { sort.Sort(sa) }

func NewSrvShard(version int64) *SrvShard {
	return &SrvShard{
		version: version,
	}
}

// KeyspacePartition represents a continuous set of shards to
// serve an entire data set.
type KeyspacePartition struct {
	// List of non-overlapping continuous shards sorted by range.
	Shards []SrvShard
}

// A distilled serving copy of keyspace detail stored in the local
// cell for fast access. Derived from the global keyspace, shards and
// local details.
// In zk, it is in /zk/local/vt/ns/<keyspace>
type SrvKeyspace struct {
	// Shards to use per type, only contains complete partitions.
	Partitions map[TabletType]*KeyspacePartition

	// This list will be deprecated as soon as Partitions is used.
	// List of non-overlapping shards sorted by range.
	Shards []SrvShard

	// List of available tablet types for this keyspace in this cell.
	// May not have a server for every shard, but we have some.
	TabletTypes []TabletType

	version int64
}

func NewSrvKeyspace(version int64) *SrvKeyspace {
	return &SrvKeyspace{
		version: version,
	}
}
