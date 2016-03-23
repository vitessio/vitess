// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"errors"

	"github.com/youtube/vitess/go/sqltypes"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
)

// This file defines the interface and implementations of sharding keys.

// shardingKey defines the sharding key interface that vtworkers need for sharding operations.
type shardingKey interface {
	// columnName returns the column name that will contain the sharding key on the given keyspace.tablename.
	// Note that the column will not necessarily contain the sharding key value directly;
	// it may need to be transformed, using the Value method, to retrieve the sharding key value.
	// This assumes that the sharding key can always be computed from a single column.
	columnName(keyspace, tablename string) string
	// keyType returns the sharding key type for the given keyspace.tablename
	keyType(keyspace, tablename string) topodatapb.KeyspaceIdType
	// value transforms the sharding key column's stored value to the final sharding key value.
	// This is necessary because the final value may be computed from the stored value.
	value(colValue sqltypes.Value) (sqltypes.Value, error)
}

// v2ShardingKey is the sharding key that is used by VTGate V2 deployments.
// In V2, they sharding key column name and type is the same for all tables,
// and the sharding key value is the same as the stored value.
type v2ShardingKey struct {
	keyspaceInfo *topo.KeyspaceInfo
}

// newShardingKey returns a new sharding key for the keyspace.
func newShardingKey(keyspaceInfo *topo.KeyspaceInfo) (shardingKey, error) {
	sk, err := newV2ShardingKey(keyspaceInfo)
	if err != nil {
		// TODO(aaijazi): if we failed to get a v2 sharding key, it means that the sharding info isn't
		// set for this keyspace, and we should try to return a v3 sharding key instead.
		return nil, err
	}
	// This keyspace uses a V2 sharding key.
	return sk, nil
}

// newV2ShardingKey returns a shardingKey for a keyspace that has a set sharding column name and type.
func newV2ShardingKey(keyspaceInfo *topo.KeyspaceInfo) (shardingKey, error) {
	if keyspaceInfo.ShardingColumnName == "" {
		return nil, errors.New("ShardingColumnName needs to be set for a v2 sharding key")
	}
	if keyspaceInfo.ShardingColumnType == topodatapb.KeyspaceIdType_UNSET {
		return nil, errors.New("ShardingColumnType needs to be set for a v2 sharding key")
	}
	return &v2ShardingKey{keyspaceInfo}, nil
}

// columnName implements the shardingKey interface.
func (sk *v2ShardingKey) columnName(keyspace, tablename string) string {
	return sk.keyspaceInfo.ShardingColumnName
}

// keyType implements the shardingKey interface.
func (sk *v2ShardingKey) keyType(keyspace, tablename string) topodatapb.KeyspaceIdType {
	return sk.keyspaceInfo.ShardingColumnType
}

// value implements the shardingKey interface.
func (*v2ShardingKey) value(colValue sqltypes.Value) (sqltypes.Value, error) {
	return colValue, nil
}

// TODO(aaijazi): implement a V3 shardingKey, which takes advantage of a table's primary ColVindex
