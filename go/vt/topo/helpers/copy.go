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

// Package helpers contains a few utility classes to handle topo.Server
// objects, and transitions from one topo implementation to another.
package helpers

import (
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// CopyKeyspaces will create the keyspaces in the destination topo.
func CopyKeyspaces(ctx context.Context, fromTS, toTS *topo.Server) {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		log.Fatalf("GetKeyspaces: %v", err)
	}

	for _, keyspace := range keyspaces {

		ki, err := fromTS.GetKeyspace(ctx, keyspace)
		if err != nil {
			log.Fatalf("GetKeyspace(%v): %v", keyspace, err)
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
			if err := toTS.SaveVSchema(ctx, keyspace, vs); err != nil {
				log.Errorf("SaveVSchema(%v): %v", keyspace, err)
			}
		case topo.IsErrType(err, topo.NoNode):
			// Nothing to do.
		default:
			log.Errorf("GetVSchema(%v): %v", keyspace, err)
		}
	}
}

// CopyShards will create the shards in the destination topo.
func CopyShards(ctx context.Context, fromTS, toTS *topo.Server) {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		log.Fatalf("fromTS.GetKeyspaces: %v", err)
	}

	for _, keyspace := range keyspaces {
		shards, err := fromTS.GetShardNames(ctx, keyspace)
		if err != nil {
			log.Fatalf("GetShardNames(%v): %v", keyspace, err)
			return
		}

		for _, shard := range shards {

			si, err := fromTS.GetShard(ctx, keyspace, shard)
			if err != nil {
				log.Fatalf("GetShard(%v, %v): %v", keyspace, shard, err)
			}

			if err := toTS.CreateShard(ctx, keyspace, shard); err != nil {
				if topo.IsErrType(err, topo.NodeExists) {
					log.Warningf("shard %v/%v already exists", keyspace, shard)
				} else {
					log.Fatalf("CreateShard(%v, %v): %v", keyspace, shard, err)
				}
			}
			if _, err := toTS.UpdateShardFields(ctx, keyspace, shard, func(toSI *topo.ShardInfo) error {
				*toSI.Shard = *si.Shard
				return nil
			}); err != nil {
				log.Fatalf("UpdateShardFields(%v, %v): %v", keyspace, shard, err)
			}
		}
	}
}

// CopyTablets will create the tablets in the destination topo.
func CopyTablets(ctx context.Context, fromTS, toTS *topo.Server) {
	cells, err := fromTS.GetKnownCells(ctx)
	if err != nil {
		log.Fatalf("fromTS.GetKnownCells: %v", err)
	}

	for _, cell := range cells {
		tabletAliases, err := fromTS.GetTabletsByCell(ctx, cell)
		if err != nil {
			log.Fatalf("GetTabletsByCell(%v): %v", cell, err)
		} else {
			for _, tabletAlias := range tabletAliases {

				// read the source tablet
				ti, err := fromTS.GetTablet(ctx, tabletAlias)
				if err != nil {
					log.Fatalf("GetTablet(%v): %v", tabletAlias, err)
				}

				// try to create the destination
				err = toTS.CreateTablet(ctx, ti.Tablet)
				if topo.IsErrType(err, topo.NodeExists) {
					// update the destination tablet
					log.Warningf("tablet %v already exists, updating it", tabletAlias)
					_, err = toTS.UpdateTabletFields(ctx, tabletAlias, func(t *topodatapb.Tablet) error {
						*t = *ti.Tablet
						return nil
					})
				}
				if err != nil {
					log.Fatalf("CreateTablet(%v): %v", tabletAlias, err)
				}
			}
		}
	}
}

// CopyShardReplications will create the ShardReplication objects in
// the destination topo.
func CopyShardReplications(ctx context.Context, fromTS, toTS *topo.Server) {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		log.Fatalf("fromTS.GetKeyspaces: %v", err)
	}

	cells, err := fromTS.GetCellInfoNames(ctx)
	if err != nil {
		log.Fatalf("GetCellInfoNames(): %v", err)
	}

	for _, keyspace := range keyspaces {
		shards, err := fromTS.GetShardNames(ctx, keyspace)
		if err != nil {
			log.Fatalf("GetShardNames(%v): %v", keyspace, err)
		}

		for _, shard := range shards {
			for _, cell := range cells {
				sri, err := fromTS.GetShardReplication(ctx, cell, keyspace, shard)
				if err != nil {
					log.Fatalf("GetShardReplication(%v, %v, %v): %v", cell, keyspace, shard, err)
					continue
				}

				if err := toTS.UpdateShardReplicationFields(ctx, cell, keyspace, shard, func(oldSR *topodatapb.ShardReplication) error {
					*oldSR = *sri.ShardReplication
					return nil
				}); err != nil {
					log.Warningf("UpdateShardReplicationFields(%v, %v, %v): %v", cell, keyspace, shard, err)
				}
			}
		}
	}
}
