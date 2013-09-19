// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

// ReplicationLink describes a MySQL replication relationship
type ReplicationLink struct {
	TabletAlias TabletAlias
	Parent      TabletAlias
}

// ShardReplication describes the MySQL replication relationships
// whithin a cell.
type ShardReplication struct {
	// Note there can be only one ReplicationLink in this array
	// for a given Slave (each Slave can only have one parent)
	ReplicationLinks []ReplicationLink
}

// ShardReplicationInfo is the companion structure for ShardReplication.
type ShardReplicationInfo struct {
	*ShardReplication
	cell     string
	keyspace string
	shard    string
}

// For topo.Server implementations to create the structure
func NewShardReplicationInfo(sr *ShardReplication, cell, keyspace, shard string) *ShardReplicationInfo {
	return &ShardReplicationInfo{
		ShardReplication: sr,
		cell:             cell,
		keyspace:         keyspace,
		shard:            shard,
	}
}

// Accessor methods

func (sri *ShardReplicationInfo) Cell() string {
	return sri.cell
}

func (sri *ShardReplicationInfo) Keyspace() string {
	return sri.keyspace
}

func (sri *ShardReplicationInfo) Shard() string {
	return sri.shard
}
