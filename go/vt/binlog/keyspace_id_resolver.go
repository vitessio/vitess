/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package binlog

import (
	"flag"
	"fmt"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var useV3ReshardingMode = flag.Bool("binlog_use_v3_resharding_mode", true, "True iff the binlog streamer should use V3-style sharding, which doesn't require a preset sharding key column.")

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
	if *useV3ReshardingMode {
		return newKeyspaceIDResolverFactoryV3(ctx, ts, keyspace, cell)
	}

	return newKeyspaceIDResolverFactoryV2(ctx, ts, keyspace)
}

// newKeyspaceIDResolverFactoryV2 finds the ShardingColumnName / Type
// from the keyspace, and uses it to find the column name.
func newKeyspaceIDResolverFactoryV2(ctx context.Context, ts *topo.Server, keyspace string) (keyspaceIDResolverFactory, error) {
	ki, err := ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return nil, err
	}
	if ki.ShardingColumnName == "" {
		return nil, fmt.Errorf("ShardingColumnName needs to be set for a v2 sharding key for keyspace %v", keyspace)
	}
	switch ki.ShardingColumnType {
	case topodatapb.KeyspaceIdType_UNSET:
		return nil, fmt.Errorf("ShardingColumnType needs to be set for a v2 sharding key for keyspace %v", keyspace)
	case topodatapb.KeyspaceIdType_BYTES, topodatapb.KeyspaceIdType_UINT64:
		// Supported values, we're good.
	default:
		return nil, fmt.Errorf("unknown ShardingColumnType %v for v2 sharding key for keyspace %v", ki.ShardingColumnType, keyspace)
	}
	return func(table *schema.Table) (int, keyspaceIDResolver, error) {
		for i, col := range table.Columns {
			if col.Name.EqualString(ki.ShardingColumnName) {
				// We found the column.
				return i, &keyspaceIDResolverFactoryV2{
					shardingColumnType: ki.ShardingColumnType,
				}, nil
			}
		}
		// The column was not found.
		return -1, nil, fmt.Errorf("cannot find column %v in table %v", ki.ShardingColumnName, table.Name)
	}, nil
}

// keyspaceIDResolverFactoryV2 uses the KeyspaceInfo of the Keyspace
// to find the sharding column name.
type keyspaceIDResolverFactoryV2 struct {
	shardingColumnType topodatapb.KeyspaceIdType
}

func (r *keyspaceIDResolverFactoryV2) keyspaceID(v sqltypes.Value) ([]byte, error) {
	switch r.shardingColumnType {
	case topodatapb.KeyspaceIdType_BYTES:
		return v.ToBytes(), nil
	case topodatapb.KeyspaceIdType_UINT64:
		i, err := sqltypes.ToUint64(v)
		if err != nil {
			return nil, fmt.Errorf("Non numerical value: %v", err)
		}
		return key.Uint64Key(i).Bytes(), nil
	default:
		panic("unreachable")
	}
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

		// The primary vindex is most likely the sharding key,
		// and has to be unique.
		if len(tableSchema.ColumnVindexes) == 0 {
			return -1, nil, fmt.Errorf("no vindex definition for table %v", table.Name)
		}
		colVindex := tableSchema.ColumnVindexes[0]
		if colVindex.Vindex.Cost() > 1 {
			return -1, nil, fmt.Errorf("primary vindex cost is too high for table %v", table.Name)
		}
		unique, ok := colVindex.Vindex.(vindexes.Unique)
		if !ok {
			return -1, nil, fmt.Errorf("primary vindex is not unique for table %v", table.Name)
		}

		// TODO @rafael - when rewriting the mapping function, this will need to change.
		// for now it's safe to assume the sharding key will be always on index 0.
		shardingColumnName := colVindex.Columns[0].String()
		for i, col := range table.Columns {
			if col.Name.EqualString(shardingColumnName) {
				// We found the column.
				return i, &keyspaceIDResolverFactoryV3{
					vindex: unique,
				}, nil
			}
		}
		// The column was not found.
		return -1, nil, fmt.Errorf("cannot find column %v in table %v", shardingColumnName, table.Name)
	}, nil
}

// keyspaceIDResolverFactoryV3 uses the Vindex to compute the value.
type keyspaceIDResolverFactoryV3 struct {
	vindex vindexes.Unique
}

func (r *keyspaceIDResolverFactoryV3) keyspaceID(v sqltypes.Value) ([]byte, error) {
	ids := []sqltypes.Value{v}
	ksids, err := r.vindex.Map(nil, ids)
	if err != nil {
		return nil, err
	}
	if len(ksids) != 1 {
		return nil, fmt.Errorf("mapping row to keyspace id returned an invalid array of keyspace ids: %v", ksids)
	}
	if ksids[0] == nil {
		return nil, fmt.Errorf("could not map %v to a keyspace id", v)
	}
	return ksids[0], nil
}
