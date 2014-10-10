// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"github.com/youtube/vitess/go/vt/key"
)

// This file contains keyspace utility functions

// Keyspace is the data structure that has data about the Keyspaces in
// the topology. Most fields are optional.
type Keyspace struct {
	// name of the column used for sharding
	// empty if the keyspace is not sharded
	ShardingColumnName string

	// type of the column used for sharding
	// KIT_UNSET if the keyspace is not sharded
	ShardingColumnType key.KeyspaceIdType

	// ServedFrom will redirect the appropriate traffic to
	// another keyspace
	ServedFrom map[TabletType]string

	// Number of shards to use for batch job / mapreduce jobs
	// that need to split a given keyspace into multiple shards.
	// The value N used should be big enough that all possible shards
	// cover 1/Nth of the entire space or more.
	// It is usually the number of shards in the system. If a keyspace
	// is being resharded from M to P shards, it should be max(M, P).
	// That way we can guarantee a query that is targetted to 1/N of the
	// keyspace will land on just one shard.
	SplitShardCount int32
}

// KeyspaceInfo is a meta struct that contains metadata to give the
// data more context and convenience. This is the main way we interact
// with a keyspace.
type KeyspaceInfo struct {
	keyspace string
	version  int64
	*Keyspace
}

// KeyspaceName returns the keyspace name
func (ki *KeyspaceInfo) KeyspaceName() string {
	return ki.keyspace
}

// Version returns the keyspace version from last time it was read or updated.
func (ki *KeyspaceInfo) Version() int64 {
	return ki.version
}

// NewKeyspaceInfo returns a KeyspaceInfo basing on keyspace with the
// keyspace. This function should be only used by Server
// implementations.
func NewKeyspaceInfo(keyspace string, value *Keyspace, version int64) *KeyspaceInfo {
	return &KeyspaceInfo{
		keyspace: keyspace,
		version:  version,
		Keyspace: value,
	}
}

// UpdateKeyspace updates the keyspace data, with the right version
func UpdateKeyspace(ts Server, ki *KeyspaceInfo) error {
	var version int64 = -1
	if ki.version != 0 {
		version = ki.version
	}

	newVersion, err := ts.UpdateKeyspace(ki, version)
	if err == nil {
		ki.version = newVersion
	}
	return err
}

// FindAllShardsInKeyspace reads and returns all the existing shards in
// a keyspace. It doesn't take any lock.
func FindAllShardsInKeyspace(ts Server, keyspace string) (map[string]*ShardInfo, error) {
	shards, err := ts.GetShardNames(keyspace)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*ShardInfo, len(shards))
	for _, shard := range shards {
		si, err := ts.GetShard(keyspace, shard)
		if err != nil {
			return nil, err
		}
		result[shard] = si
	}
	return result, nil
}
