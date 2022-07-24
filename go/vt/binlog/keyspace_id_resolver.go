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

package binlog

import (
	"fmt"
	"strings"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

// keyspaceIDResolver is constructed for a tableMap entry in RBR.  It
// is used for each row, and passed in the value used for figuring out
// the keyspace id.
type keyspaceIDResolver interface {
	// keyspaceID takes a table row, and returns the keyspace id as bytes.
	// It will return an error if no sharding key can be found.
	// The bitmap describes which columns are present in the row.
	keyspaceID(value sqltypes.Value) ([]byte, error)
}

// keyspaceIDResolverFactory creates a keyspaceIDResolver for a table
// given its schema. It returns the index of the field to used to compute
// the keyspaceID, and a function that given a value for that
// field, returns the keyspace id.
type keyspaceIDResolverFactory func(*schema.Table) (int, keyspaceIDResolver, error)

// newKeyspaceIDResolverFactory creates a new
// keyspaceIDResolverFactory for the provided keyspace and cell.
func newKeyspaceIDResolverFactory(ctx context.Context, ts *topo.Server, keyspace string, cell string) (keyspaceIDResolverFactory, error) {
	return newKeyspaceIDResolverFactoryV3(ctx, ts, keyspace, cell)
}

// newKeyspaceIDResolverFactoryV3 finds the SrvVSchema in the cell,
// gets the keyspace part, and uses it to find the column name.
func newKeyspaceIDResolverFactoryV3(ctx context.Context, ts *topo.Server, keyspace string, cell string) (keyspaceIDResolverFactory, error) {
	srvVSchema, err := ts.GetSrvVSchema(ctx, cell)
	if err != nil {
		return nil, err
	}
	kschema, ok := srvVSchema.Keyspaces[keyspace]
	if !ok {
		return nil, fmt.Errorf("SrvVSchema has no entry for keyspace %v", keyspace)
	}
	keyspaceSchema, err := vindexes.BuildKeyspaceSchema(kschema, keyspace)
	if err != nil {
		return nil, fmt.Errorf("cannot build vschema for keyspace %v: %v", keyspace, err)
	}
	return func(table *schema.Table) (int, keyspaceIDResolver, error) {
		// Find the v3 schema.
		tableSchema, ok := keyspaceSchema.Tables[table.Name.String()]
		if !ok {
			return -1, nil, fmt.Errorf("no vschema definition for table %v", table.Name)
		}

		// use the lowest cost unique vindex as the sharding key
		colVindex, err := vindexes.FindVindexForSharding(table.Name.String(), tableSchema.ColumnVindexes)
		if err != nil {
			return -1, nil, err
		}

		// TODO @rafael - when rewriting the mapping function, this will need to change.
		// for now it's safe to assume the sharding key will be always on index 0.
		shardingColumnName := colVindex.Columns[0].String()
		for i, col := range table.Fields {
			if strings.EqualFold(col.Name, shardingColumnName) {
				// We found the column.
				return i, &keyspaceIDResolverFactoryV3{
					// Only SingleColumn vindexes are returned by FindVindexForSharding.
					vindex: colVindex.Vindex.(vindexes.SingleColumn),
				}, nil
			}
		}
		// The column was not found.
		return -1, nil, fmt.Errorf("cannot find column %v in table %v", shardingColumnName, table.Name)
	}, nil
}

// keyspaceIDResolverFactoryV3 uses the Vindex to compute the value.
type keyspaceIDResolverFactoryV3 struct {
	vindex vindexes.SingleColumn
}

func (r *keyspaceIDResolverFactoryV3) keyspaceID(v sqltypes.Value) ([]byte, error) {
	destinations, err := r.vindex.Map(context.TODO(), nil, []sqltypes.Value{v})
	if err != nil {
		return nil, err
	}
	if len(destinations) != 1 {
		return nil, fmt.Errorf("mapping row to keyspace id returned an invalid array of destinations: %v", key.DestinationsString(destinations))
	}
	ksid, ok := destinations[0].(key.DestinationKeyspaceID)
	if !ok || len(ksid) == 0 {
		return nil, fmt.Errorf("could not map %v to a keyspace id, got destination %v", v, destinations[0])
	}
	return ksid, nil
}
