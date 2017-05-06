/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package worker

import (
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file defines the interface and implementations of sharding key resolvers.

// keyspaceIDResolver defines the interface that needs to be satisifed to get a
// keyspace ID from a database row.
type keyspaceIDResolver interface {
	// keyspaceID takes a table row, and returns the keyspace id as bytes.
	// It will return an error if no sharding key can be found.
	keyspaceID(row []sqltypes.Value) ([]byte, error)
}

// v2Resolver is the keyspace id resolver that is used by VTGate V2 deployments.
// In V2, the sharding key column name and type is the same for all tables,
// and the keyspace ID is stored in the sharding key database column.
type v2Resolver struct {
	keyspaceInfo        *topo.KeyspaceInfo
	shardingColumnIndex int
}

// newV2Resolver returns a keyspaceIDResolver for a v2 table.
// V2 keyspaces have a preset sharding column name and type.
func newV2Resolver(keyspaceInfo *topo.KeyspaceInfo, td *tabletmanagerdatapb.TableDefinition) (keyspaceIDResolver, error) {
	if keyspaceInfo.ShardingColumnName == "" {
		return nil, errors.New("ShardingColumnName needs to be set for a v2 sharding key")
	}
	if keyspaceInfo.ShardingColumnType == topodatapb.KeyspaceIdType_UNSET {
		return nil, errors.New("ShardingColumnType needs to be set for a v2 sharding key")
	}
	if td.Type != tmutils.TableBaseTable {
		return nil, fmt.Errorf("a keyspaceID resolver can only be created for a base table, got %v", td.Type)
	}

	// Find the sharding key column index.
	columnIndex, ok := tmutils.TableDefinitionGetColumn(td, keyspaceInfo.ShardingColumnName)
	if !ok {
		return nil, fmt.Errorf("table %v doesn't have a column named '%v'", td.Name, keyspaceInfo.ShardingColumnName)
	}

	return &v2Resolver{keyspaceInfo, columnIndex}, nil
}

// keyspaceID implements the keyspaceIDResolver interface.
func (r *v2Resolver) keyspaceID(row []sqltypes.Value) ([]byte, error) {
	v := row[r.shardingColumnIndex]
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

// v3Resolver is the keyspace id resolver that is used by VTGate V3 deployments.
// In V3, we use the VSchema to find a Unique VIndex of cost 0 or 1 for each
// table.
type v3Resolver struct {
	shardingColumnIndex int
	vindex              vindexes.Unique
}

// newV3ResolverFromTableDefinition returns a keyspaceIDResolver for a v3 table.
func newV3ResolverFromTableDefinition(keyspaceSchema *vindexes.KeyspaceSchema, td *tabletmanagerdatapb.TableDefinition) (keyspaceIDResolver, error) {
	if td.Type != tmutils.TableBaseTable {
		return nil, fmt.Errorf("a keyspaceID resolver can only be created for a base table, got %v", td.Type)
	}
	tableSchema, ok := keyspaceSchema.Tables[td.Name]
	if !ok {
		return nil, fmt.Errorf("no vschema definition for table %v", td.Name)
	}
	// the primary vindex is most likely the sharding key, and has to
	// be unique.
	if len(tableSchema.ColumnVindexes) == 0 {
		return nil, fmt.Errorf("no vindex definition for table %v", td.Name)
	}
	colVindex := tableSchema.ColumnVindexes[0]
	if colVindex.Vindex.Cost() > 1 {
		return nil, fmt.Errorf("primary vindex cost is too high for table %v", td.Name)
	}
	unique, ok := colVindex.Vindex.(vindexes.Unique)
	if !ok {
		return nil, fmt.Errorf("primary vindex is not unique for table %v", td.Name)
	}

	// Find the sharding key column index.
	columnIndex, ok := tmutils.TableDefinitionGetColumn(td, colVindex.Column.String())
	if !ok {
		return nil, fmt.Errorf("table %v has a Vindex on unknown column %v", td.Name, colVindex.Column)
	}

	return &v3Resolver{
		shardingColumnIndex: columnIndex,
		vindex:              unique,
	}, nil
}

// newV3ResolverFromColumnList returns a keyspaceIDResolver for a v3 table.
func newV3ResolverFromColumnList(keyspaceSchema *vindexes.KeyspaceSchema, name string, columns []string) (keyspaceIDResolver, error) {
	tableSchema, ok := keyspaceSchema.Tables[name]
	if !ok {
		return nil, fmt.Errorf("no vschema definition for table %v", name)
	}
	// the primary vindex is most likely the sharding key, and has to
	// be unique.
	if len(tableSchema.ColumnVindexes) == 0 {
		return nil, fmt.Errorf("no vindex definition for table %v", name)
	}
	colVindex := tableSchema.ColumnVindexes[0]
	if colVindex.Vindex.Cost() > 1 {
		return nil, fmt.Errorf("primary vindex cost is too high for table %v", name)
	}
	unique, ok := colVindex.Vindex.(vindexes.Unique)
	if !ok {
		return nil, fmt.Errorf("primary vindex is not unique for table %v", name)
	}

	// Find the sharding key column index.
	columnIndex := -1
	for i, n := range columns {
		if colVindex.Column.EqualString(n) {
			columnIndex = i
			break
		}
	}
	if columnIndex == -1 {
		return nil, fmt.Errorf("table %v has a Vindex on unknown column %v", name, colVindex.Column)
	}

	return &v3Resolver{
		shardingColumnIndex: columnIndex,
		vindex:              unique,
	}, nil
}

// keyspaceID implements the keyspaceIDResolver interface.
func (r *v3Resolver) keyspaceID(row []sqltypes.Value) ([]byte, error) {
	v := row[r.shardingColumnIndex]
	ids := []interface{}{v}
	ksids, err := r.vindex.Map(nil, ids)
	if err != nil {
		return nil, err
	}
	if len(ksids) != 1 {
		return nil, fmt.Errorf("maping row to keyspace id returned an invalid array of keyspace ids: %v", ksids)
	}
	return ksids[0], nil
}
