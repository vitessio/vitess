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

// Package helpers contains a few utility classes to handle topo.Server
// objects, and transitions from one topo implementation to another.
package helpers

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// CopyKeyspaces will create the keyspaces in the destination topo.
func CopyKeyspaces(ctx context.Context, fromTS, toTS *topo.Server, parser *sqlparser.Parser) error {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		return fmt.Errorf("GetKeyspaces: %w", err)
	}

	for _, keyspace := range keyspaces {

		ki, err := fromTS.GetKeyspace(ctx, keyspace)
		if err != nil {
			return fmt.Errorf("GetKeyspace(%v): %w", keyspace, err)
		}

		if err := toTS.CreateKeyspace(ctx, keyspace, ki.Keyspace); err != nil {
			if topo.IsErrType(err, topo.NodeExists) {
				log.Warningf("keyspace %v already exists", keyspace)
			} else {
				log.Errorf("CreateKeyspace(%v): %v", keyspace, err)
			}
		}

		vs, err := fromTS.GetVSchema(ctx, keyspace)
		switch {
		case err == nil:
			_, err = vindexes.BuildKeyspace(vs, parser)
			if err != nil {
				log.Errorf("BuildKeyspace(%v): %v", keyspace, err)
				break
			}
			if err := toTS.SaveVSchema(ctx, keyspace, vs); err != nil {
				log.Errorf("SaveVSchema(%v): %v", keyspace, err)
			}
		case topo.IsErrType(err, topo.NoNode):
			// Nothing to do.
		default:
			log.Errorf("GetVSchema(%v): %v", keyspace, err)
		}
	}

	return nil
}

// CopyShards will create the shards in the destination topo.
func CopyShards(ctx context.Context, fromTS, toTS *topo.Server) error {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		return fmt.Errorf("fromTS.GetKeyspaces: %w", err)
	}

	for _, keyspace := range keyspaces {
		shards, err := fromTS.GetShardNames(ctx, keyspace)
		if err != nil {
			return fmt.Errorf("GetShardNames(%v): %w", keyspace, err)
		}

		for _, shard := range shards {

			si, err := fromTS.GetShard(ctx, keyspace, shard)
			if err != nil {
				return fmt.Errorf("GetShard(%v, %v): %w", keyspace, shard, err)
			}

			if err := toTS.CreateShard(ctx, keyspace, shard); err != nil {
				if topo.IsErrType(err, topo.NodeExists) {
					log.Warningf("shard %v/%v already exists", keyspace, shard)
				} else {
					return fmt.Errorf("CreateShard(%v, %v): %w", keyspace, shard, err)
				}
			}
			if _, err := toTS.UpdateShardFields(ctx, keyspace, shard, func(toSI *topo.ShardInfo) error {
				toSI.Shard = si.Shard.CloneVT()
				return nil
			}); err != nil {
				return fmt.Errorf("UpdateShardFields(%v, %v): %w", keyspace, shard, err)
			}
		}
	}

	return nil
}

// CopyTablets will create the tablets in the destination topo.
func CopyTablets(ctx context.Context, fromTS, toTS *topo.Server) error {
	cells, err := fromTS.GetKnownCells(ctx)
	if err != nil {
		return fmt.Errorf("fromTS.GetKnownCells: %w", err)
	}

	for _, cell := range cells {
		tabletAliases, err := fromTS.GetTabletAliasesByCell(ctx, cell)
		if err != nil {
			return fmt.Errorf("GetTabletsByCell(%v): %w", cell, err)
		} else {
			for _, tabletAlias := range tabletAliases {

				// read the source tablet
				ti, err := fromTS.GetTablet(ctx, tabletAlias)
				if err != nil {
					return fmt.Errorf("GetTablet(%v): %w", tabletAlias, err)
				}

				// try to create the destination
				err = toTS.CreateTablet(ctx, ti.Tablet)
				if topo.IsErrType(err, topo.NodeExists) {
					// update the destination tablet
					log.Warningf("tablet %v already exists, updating it", tabletAlias)
					_, err = toTS.UpdateTabletFields(ctx, tabletAlias, func(t *topodatapb.Tablet) error {
						proto.Merge(t, ti.Tablet)
						return nil
					})
				}
				if err != nil {
					return fmt.Errorf("CreateTablet(%v): %w", tabletAlias, err)
				}
			}
		}
	}

	return nil
}

// CopyShardReplications will create the ShardReplication objects in
// the destination topo.
func CopyShardReplications(ctx context.Context, fromTS, toTS *topo.Server) error {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		return fmt.Errorf("fromTS.GetKeyspaces: %w", err)
	}

	cells, err := fromTS.GetCellInfoNames(ctx)
	if err != nil {
		return fmt.Errorf("GetCellInfoNames(): %w", err)
	}

	for _, keyspace := range keyspaces {
		shards, err := fromTS.GetShardNames(ctx, keyspace)
		if err != nil {
			return fmt.Errorf("GetShardNames(%v): %w", keyspace, err)
		}

		for _, shard := range shards {
			for _, cell := range cells {
				sri, err := fromTS.GetShardReplication(ctx, cell, keyspace, shard)
				if err != nil {
					return fmt.Errorf("GetShardReplication(%v, %v, %v): %w", cell, keyspace, shard, err)
				}

				sriNodes := map[string]struct{}{}
				for _, node := range sri.Nodes {
					sriNodes[topoproto.TabletAliasString(node.TabletAlias)] = struct{}{}
				}

				if err := toTS.UpdateShardReplicationFields(ctx, cell, keyspace, shard, func(oldSR *topodatapb.ShardReplication) error {
					var nodes []*topodatapb.ShardReplication_Node
					for _, oldNode := range oldSR.Nodes {
						if _, ok := sriNodes[topoproto.TabletAliasString(oldNode.TabletAlias)]; ok {
							continue
						}

						nodes = append(nodes, oldNode)
					}

					nodes = append(nodes, sri.ShardReplication.Nodes...)
					// Even though ShardReplication currently only has the .Nodes field,
					// keeping the proto.Merge call here prevents this copy from
					// unintentionally breaking if we add new fields.
					proto.Merge(oldSR, sri.ShardReplication)
					oldSR.Nodes = nodes
					return nil
				}); err != nil {
					log.Warningf("UpdateShardReplicationFields(%v, %v, %v): %v", cell, keyspace, shard, err)
				}
			}
		}
	}

	return nil
}

// CopyRoutingRules will create the routing rules in the destination topo.
func CopyRoutingRules(ctx context.Context, fromTS, toTS *topo.Server) error {
	rr, err := fromTS.GetRoutingRules(ctx)
	if err != nil {
		return fmt.Errorf("GetRoutingRules: %w", err)
	}
	if err := toTS.SaveRoutingRules(ctx, rr); err != nil {
		log.Errorf("SaveRoutingRules(%v): %v", rr, err)
	}

	return nil
}
