/*
Copyright 2018 The Vitess Authors

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

// Package helpers contains a few utility classes to handle topo.Server
// objects, and transitions from one topo implementation to another.
package helpers

import (
	"fmt"
	"reflect"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/topo"
)

// CompareKeyspaces will compare the keyspaces in the destination topo.
func CompareKeyspaces(ctx context.Context, fromTS, toTS *topo.Server) error {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		return fmt.Errorf("GetKeyspace(%v): %v", keyspaces, err)
	}

	for _, keyspace := range keyspaces {

		fromKs, err := fromTS.GetKeyspace(ctx, keyspace)
		if err != nil {
			return fmt.Errorf("GetKeyspace(%v): %v", keyspace, err)
		}

		toKs, err := toTS.GetKeyspace(ctx, keyspace)
		if err != nil {
			return fmt.Errorf("GetKeyspace(%v): %v", keyspace, err)
		}

		if !reflect.DeepEqual(fromKs.Keyspace, toKs.Keyspace) {
			return fmt.Errorf("Keyspace: %v does not match between from and to topology", keyspace)
		}

		fromVs, err := fromTS.GetVSchema(ctx, keyspace)
		switch {
		case err == nil:
			// Nothing to do.
		case topo.IsErrType(err, topo.NoNode):
			// Nothing to do.
		default:
			return fmt.Errorf("GetVSchema(%v): %v", keyspace, err)
		}

		toVs, err := toTS.GetVSchema(ctx, keyspace)
		switch {
		case err == nil:
			// Nothing to do.
		case topo.IsErrType(err, topo.NoNode):
			// Nothing to do.
		default:
			return fmt.Errorf("GetVSchema(%v): %v", keyspace, err)
		}

		if !reflect.DeepEqual(fromVs, toVs) {
			return fmt.Errorf("Vschema for keyspace: %v does not match between from and to topology", keyspace)
		}
	}
	return nil
}

// CompareShards will compare the shards in the destination topo.
func CompareShards(ctx context.Context, fromTS, toTS *topo.Server) error {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		return fmt.Errorf("fromTS.GetKeyspaces: %v", err)
	}

	for _, keyspace := range keyspaces {
		shards, err := fromTS.GetShardNames(ctx, keyspace)
		if err != nil {
			return fmt.Errorf("GetShardNames(%v): %v", keyspace, err)
		}

		for _, shard := range shards {
			fromSi, err := fromTS.GetShard(ctx, keyspace, shard)
			if err != nil {
				return fmt.Errorf("GetShard(%v, %v): %v", keyspace, shard, err)
			}
			toSi, err := toTS.GetShard(ctx, keyspace, shard)
			if err != nil {
				return fmt.Errorf("GetShard(%v, %v): %v", keyspace, shard, err)
			}

			if !reflect.DeepEqual(fromSi.Shard, toSi.Shard) {
				return fmt.Errorf("Shard %v for keyspace: %v does not match between from and to topology", shard, keyspace)
			}
		}
	}
	return nil
}

// CompareTablets will compare the tablets in the destination topo.
func CompareTablets(ctx context.Context, fromTS, toTS *topo.Server) error {
	cells, err := fromTS.GetKnownCells(ctx)
	if err != nil {
		return fmt.Errorf("fromTS.GetKnownCells: %v", err)
	}

	for _, cell := range cells {
		tabletAliases, err := fromTS.GetTabletsByCell(ctx, cell)
		if err != nil {
			return fmt.Errorf("GetTabletsByCell(%v): %v", cell, err)
		}
		for _, tabletAlias := range tabletAliases {

			// read the source tablet
			fromTi, err := fromTS.GetTablet(ctx, tabletAlias)
			if err != nil {
				return fmt.Errorf("GetTablet(%v): %v", tabletAlias, err)
			}
			toTi, err := toTS.GetTablet(ctx, tabletAlias)
			if err != nil {
				return fmt.Errorf("GetTablet(%v): %v", tabletAlias, err)
			}
			if !reflect.DeepEqual(fromTi.Tablet, toTi.Tablet) {
				return fmt.Errorf("Tablet %v:  does not match between from and to topology", tabletAlias)
			}
		}
	}
	return nil
}

// CompareShardReplications will compare the ShardReplication objects in
// the destination topo.
func CompareShardReplications(ctx context.Context, fromTS, toTS *topo.Server) error {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		return fmt.Errorf("fromTS.GetKeyspaces: %v", err)
	}
	cells, err := fromTS.GetCellInfoNames(ctx)
	if err != nil {
		return fmt.Errorf("GetCellInfoNames(): %v", err)
	}

	for _, keyspace := range keyspaces {
		shards, err := fromTS.GetShardNames(ctx, keyspace)
		if err != nil {
			return fmt.Errorf("GetShardNames(%v): %v", keyspace, err)
		}

		for _, shard := range shards {
			for _, cell := range cells {
				fromSRi, err := fromTS.GetShardReplication(ctx, cell, keyspace, shard)
				if err != nil {
					return fmt.Errorf("GetShardReplication(%v, %v, %v): %v", cell, keyspace, shard, err)
				}
				toSRi, err := toTS.GetShardReplication(ctx, cell, keyspace, shard)
				if err != nil {
					return fmt.Errorf("GetShardReplication(%v, %v, %v): %v", cell, keyspace, shard, err)
				}
				if !reflect.DeepEqual(fromSRi.ShardReplication, toSRi.ShardReplication) {
					return fmt.Errorf(
						"Shard Replication in cell %v, keyspace %v, shard %v:  does not match between from and to topology",
						cell,
						keyspace,
						shard)
				}
			}
		}
	}
	return nil
}
