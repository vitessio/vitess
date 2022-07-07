/*
Copyright 2019 The Vitess Authors.

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
	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// This file defines the interface and implementations of sharding key resolvers.

// keyspaceIDResolver defines the interface that needs to be satisfied to get a
// keyspace ID from a database row.
type keyspaceIDResolver interface {
	// keyspaceID takes a table row, and returns the keyspace id as bytes.
	// It will return an error if no sharding key can be found.
	keyspaceID(row []sqltypes.Value) ([]byte, error)
}

// v3Resolver is the keyspace id resolver that is used by VTGate V3 deployments.
// In V3, we use the VSchema to find a Unique VIndex of cost 0 or 1 for each
// table.
type v3Resolver struct {
	shardingColumnIndex int
	vindex              vindexes.SingleColumn
}

// newV3ResolverFromTableDefinition returns a keyspaceIDResolver for a v3 table.
func newV3ResolverFromTableDefinition(keyspaceSchema *vindexes.KeyspaceSchema, td *tabletmanagerdatapb.TableDefinition) (keyspaceIDResolver, error) {
	if td.Type != tmutils.TableBaseTable {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "a keyspaceID resolver can only be created for a base table, got %v", td.Type)
	}
	tableSchema, ok := keyspaceSchema.Tables[td.Name]
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no vschema definition for table %v", td.Name)
	}
	// use the lowest cost unique vindex as the sharding key
	colVindex, err := vindexes.FindVindexForSharding(td.Name, tableSchema.ColumnVindexes)
	if err != nil {
		return nil, err
	}

	// Find the sharding key column index.
	columnIndex, ok := tmutils.TableDefinitionGetColumn(td, colVindex.Columns[0].String())
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "table %v has a Vindex on unknown column %v", td.Name, colVindex.Columns[0])
	}

	return &v3Resolver{
		shardingColumnIndex: columnIndex,
		// Only SingleColumn vindexes are returned by FindVindexForSharding.
		vindex: colVindex.Vindex.(vindexes.SingleColumn),
	}, nil
}

// newV3ResolverFromColumnList returns a keyspaceIDResolver for a v3 table.
func newV3ResolverFromColumnList(keyspaceSchema *vindexes.KeyspaceSchema, name string, columns []string) (keyspaceIDResolver, error) {
	tableSchema, ok := keyspaceSchema.Tables[name]
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no vschema definition for table %v", name)
	}
	// use the lowest cost unique vindex as the sharding key
	colVindex, err := vindexes.FindVindexForSharding(name, tableSchema.ColumnVindexes)
	if err != nil {
		return nil, err
	}

	// Find the sharding key column index.
	columnIndex := -1
	for i, n := range columns {
		if colVindex.Columns[0].EqualString(n) {
			columnIndex = i
			break
		}
	}
	if columnIndex == -1 {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "table %v has a Vindex on unknown column %v", name, colVindex.Columns[0])
	}

	return &v3Resolver{
		shardingColumnIndex: columnIndex,
		// Only SingleColumn vindexes are returned by FindVindexForSharding.
		vindex: colVindex.Vindex.(vindexes.SingleColumn),
	}, nil
}

// keyspaceID implements the keyspaceIDResolver interface.
func (r *v3Resolver) keyspaceID(row []sqltypes.Value) ([]byte, error) {
	v := row[r.shardingColumnIndex]
	destinations, err := r.vindex.Map(context.TODO(), nil, []sqltypes.Value{v})
	if err != nil {
		return nil, err
	}
	if len(destinations) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "mapping row to keyspace id returned an invalid array of keyspace ids: %v", key.DestinationsString(destinations))
	}
	ksid, ok := destinations[0].(key.DestinationKeyspaceID)
	if !ok || len(ksid) == 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "could not map %v to a keyspace id, got destination %v", v, destinations[0])
	}
	return ksid, nil
}
