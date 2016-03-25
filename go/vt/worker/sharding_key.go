// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
)

// This file defines the interface and implementations of sharding key resolvers.

// shardingKeyResolver defines the interface that needs to be satisifed to get a sharding
// key from a database row.
type shardingKeyResolver interface {
	// shardingKey takes a table row, and returns the sharding key as bytes.
	// It will return an error if no sharding key can be found.
	shardingKey(tableName string, row []sqltypes.Value) ([]byte, error)
}

// v2Resolver is the sharding key resolver that is used by VTGate V2 deployments.
// In V2, the sharding key column name and type is the same for all tables,
// and the sharding key value is stored in the database directly.
type v2Resolver struct {
	keyspaceInfo               *topo.KeyspaceInfo
	tableToShardingColumnIndex map[string]int
}

// newV2Resolver returns a shardingKeyResolver for a v2 keyspace.
// V2 keyspaces have a preset sharding column name and type.
func newV2Resolver(keyspaceInfo *topo.KeyspaceInfo, tableDefs []*tabletmanagerdatapb.TableDefinition) (shardingKeyResolver, error) {
	if keyspaceInfo.ShardingColumnName == "" {
		return nil, errors.New("ShardingColumnName needs to be set for a v2 sharding key")
	}
	if keyspaceInfo.ShardingColumnType == topodatapb.KeyspaceIdType_UNSET {
		return nil, errors.New("ShardingColumnType needs to be set for a v2 sharding key")
	}
	// Find the sharding key column index for each table.
	tableToShardingColumnIndex := make(map[string]int)
	for _, td := range tableDefs {
		if td.Type == tmutils.TableBaseTable {
			foundColumn := false
			for i, name := range td.Columns {
				if name == keyspaceInfo.ShardingColumnName {
					foundColumn = true
					tableToShardingColumnIndex[td.Name] = i
					break
				}
			}
			if !foundColumn {
				return nil, fmt.Errorf("table %v doesn't have a column named '%v'", td.Name, keyspaceInfo.ShardingColumnName)
			}
		}
	}

	return &v2Resolver{keyspaceInfo, tableToShardingColumnIndex}, nil
}

// shardingKey implements the shardingKeyResolver interface.
func (r *v2Resolver) shardingKey(tableName string, row []sqltypes.Value) ([]byte, error) {
	keyIndex, ok := r.tableToShardingColumnIndex[tableName]
	if !ok {
		return nil, fmt.Errorf("unable to get sharding key from table %v", tableName)
	}
	v := row[keyIndex]
	switch r.keyspaceInfo.ShardingColumnType {
	case topodatapb.KeyspaceIdType_BYTES:
		return v.Raw(), nil
	case topodatapb.KeyspaceIdType_UINT64:
		i, err := v.ParseUint64()
		if err != nil {
			return nil, fmt.Errorf("Non numerical value: %v", err)
		}
		return key.Uint64Key(i).Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported ShardingColumnType: %v", r.keyspaceInfo.ShardingColumnType)
	}
}

// TODO(sougou): implement a V3 shardingKeyResolver, which takes advantage of a table's primary ColVindex.
