// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

// This file contains keyspace utility functions

// ShardingColumnType represents the type of the sharding key.
type ShardingColumnType string

const (
	// unset - no sharding for this keyspace
	SCT_UNSET = ShardingColumnType("")

	// uint64 - a uint64 value is used for sharding key
	// this is represented as 'unsigned bigint' in mysql
	SCT_UINT64 = ShardingColumnType("uint64")

	// bytes - a string of bytes is used for sharding key
	// this is represented as 'varbinary' in mysql
	SCT_BYTES = ShardingColumnType("bytes")
)

var AllShardingColumnTypes = []ShardingColumnType{
	SCT_UNSET,
	SCT_UINT64,
	SCT_BYTES,
}

// IsShardingColumnTypeInList returns true if the given type is in the list.
// Use it with AllShardingColumnTypes for instance.
func IsShardingColumnTypeInList(columnType ShardingColumnType, types []ShardingColumnType) bool {
	for _, t := range types {
		if columnType == t {
			return true
		}
	}
	return false
}

type Keyspace struct {
	// name of the column used for sharding
	// empty if the keyspace is not sharded
	ShardingColumnName string

	// type of the column used for sharding
	// SCT_UNSET if the keyspace is not sharded
	ShardingColumnType ShardingColumnType
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
// keyspace / keyspace. This function should be only used by Server
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
