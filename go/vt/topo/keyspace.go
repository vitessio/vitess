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
}

// KeyspaceInfo is a meta struct that contains metadata to give the
// data more context and convenience. This is the main way we interact
// with a keyspace.
type KeyspaceInfo struct {
	keyspace string
	*Keyspace
}

// KeyspaceName returns the keyspace name
func (ki *KeyspaceInfo) KeyspaceName() string {
	return ki.keyspace
}

// NewKeyspaceInfo returns a KeyspaceInfo basing on keyspace with the
// keyspace. This function should be only used by Server
// implementations.
func NewKeyspaceInfo(keyspace string, value *Keyspace) *KeyspaceInfo {
	return &KeyspaceInfo{
		keyspace: keyspace,
		Keyspace: value,
	}
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
