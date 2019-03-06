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
	"reflect"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
)

// CompareKeyspaces will compare the keyspaces in the destination topo.
func CompareKeyspaces(ctx context.Context, fromTS, toTS *topo.Server) error {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		return vterrors.Wrapf(err, "GetKeyspace(%v)", keyspaces)
	}

	for _, keyspace := range keyspaces {

		fromKs, err := fromTS.GetKeyspace(ctx, keyspace)
		if err != nil {
			return vterrors.Wrapf(err, "GetKeyspace(%v)", keyspace)
		}

		toKs, err := toTS.GetKeyspace(ctx, keyspace)
		if err != nil {
			return vterrors.Wrapf(err, "GetKeyspace(%v)", keyspace)
		}

		if !reflect.DeepEqual(fromKs.Keyspace, toKs.Keyspace) {
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "Keyspace: %v does not match between from and to topology", keyspace)
		}

		fromVs, err := fromTS.GetVSchema(ctx, keyspace)
		switch {
		case err == nil:
			// Nothing to do.
		case topo.IsErrType(err, topo.NoNode):
			// Nothing to do.
		default:
			return vterrors.Wrapf(err, "GetVSchema(%v)", keyspace)
		}

		toVs, err := toTS.GetVSchema(ctx, keyspace)
		switch {
		case err == nil:
			// Nothing to do.
		case topo.IsErrType(err, topo.NoNode):
			// Nothing to do.
		default:
			return vterrors.Wrapf(err, "GetVSchema(%v)", keyspace)
		}

		if !reflect.DeepEqual(fromVs, toVs) {
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "Vschema for keyspace: %v does not match between from and to topology", keyspace)
		}
	}
	return nil
}

// CompareShards will compare the shards in the destination topo.
func CompareShards(ctx context.Context, fromTS, toTS *topo.Server) error {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		return vterrors.Wrapf(err, "fromTS.GetKeyspaces")
	}

	for _, keyspace := range keyspaces {
		shards, err := fromTS.GetShardNames(ctx, keyspace)
		if err != nil {
			return vterrors.Wrapf(err, "GetShardNames(%v)", keyspace)
		}

		for _, shard := range shards {
			fromSi, err := fromTS.GetShard(ctx, keyspace, shard)
			if err != nil {
				return vterrors.Wrapf(err, "GetShard(%v, %v)", keyspace, shard)
			}
			toSi, err := toTS.GetShard(ctx, keyspace, shard)
			if err != nil {
				return vterrors.Wrapf(err, "GetShard(%v, %v)", keyspace, shard)
			}

			if !reflect.DeepEqual(fromSi.Shard, toSi.Shard) {
				return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "Shard %v for keyspace: %v does not match between from and to topology", shard, keyspace)
			}
		}
	}
	return nil
}

// CompareTablets will compare the tablets in the destination topo.
func CompareTablets(ctx context.Context, fromTS, toTS *topo.Server) error {
	cells, err := fromTS.GetKnownCells(ctx)
	if err != nil {
		return vterrors.Wrapf(err, "fromTS.GetKnownCells")
	}

	for _, cell := range cells {
		tabletAliases, err := fromTS.GetTabletsByCell(ctx, cell)
		if err != nil {
			return vterrors.Wrapf(err, "GetTabletsByCell(%v)", cell)
		}
		for _, tabletAlias := range tabletAliases {

			// read the source tablet
			fromTi, err := fromTS.GetTablet(ctx, tabletAlias)
			if err != nil {
				return vterrors.Wrapf(err, "GetTablet(%v)", tabletAlias)
			}
			toTi, err := toTS.GetTablet(ctx, tabletAlias)
			if err != nil {
				return vterrors.Wrapf(err, "GetTablet(%v)", tabletAlias)
			}
			if !reflect.DeepEqual(fromTi.Tablet, toTi.Tablet) {
				return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "Tablet %v:  does not match between from and to topology", tabletAlias)
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
		return vterrors.Wrapf(err, "fromTS.GetKeyspaces")
	}

	for _, keyspace := range keyspaces {
		shards, err := fromTS.GetShardNames(ctx, keyspace)
		if err != nil {
			return vterrors.Wrapf(err, "GetShardNames(%v)", keyspace)
		}

		for _, shard := range shards {

			// read the source shard to get the cells
			si, err := fromTS.GetShard(ctx, keyspace, shard)
			if err != nil {
				return vterrors.Wrapf(err, "GetShard(%v, %v)", keyspace, shard)
			}

			for _, cell := range si.Shard.Cells {
				fromSRi, err := fromTS.GetShardReplication(ctx, cell, keyspace, shard)
				if err != nil {
					return vterrors.Wrapf(err, "GetShardReplication(%v, %v, %v)", cell, keyspace, shard)
				}
				toSRi, err := toTS.GetShardReplication(ctx, cell, keyspace, shard)
				if err != nil {
					return vterrors.Wrapf(err, "GetShardReplication(%v, %v, %v)", cell, keyspace, shard)
				}
				if !reflect.DeepEqual(fromSRi.ShardReplication, toSRi.ShardReplication) {
					return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION,
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
