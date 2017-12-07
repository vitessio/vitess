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
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CopyKeyspaces will create the keyspaces in the destination topo.
func CopyKeyspaces(ctx context.Context, fromTS, toTS *topo.Server) {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		log.Fatalf("GetKeyspaces: %v", err)
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()

			ki, err := fromTS.GetKeyspace(ctx, keyspace)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetKeyspace(%v): %v", keyspace, err))
				return
			}

			if err := toTS.CreateKeyspace(ctx, keyspace, ki.Keyspace); err != nil {
				if err == topo.ErrNodeExists {
					log.Warningf("keyspace %v already exists", keyspace)
				} else {
					rec.RecordError(fmt.Errorf("CreateKeyspace(%v): %v", keyspace, err))
				}
			}

			vs, err := fromTS.GetVSchema(ctx, keyspace)
			switch err {
			case nil:
				if err := toTS.SaveVSchema(ctx, keyspace, vs); err != nil {
					rec.RecordError(fmt.Errorf("SaveVSchema(%v): %v", keyspace, err))
				}
			case topo.ErrNoNode:
				// Nothing to do.
			default:
				rec.RecordError(fmt.Errorf("GetVSchema(%v): %v", keyspace, err))
				return
			}
		}(keyspace)
	}
	wg.Wait()
	if rec.HasErrors() {
		log.Fatalf("copyKeyspaces failed: %v", rec.Error())
	}
}

// CopyShards will create the shards in the destination topo.
func CopyShards(ctx context.Context, fromTS, toTS *topo.Server) {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		log.Fatalf("fromTS.GetKeyspaces: %v", err)
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shards, err := fromTS.GetShardNames(ctx, keyspace)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetShardNames(%v): %v", keyspace, err))
				return
			}

			for _, shard := range shards {
				wg.Add(1)
				go func(keyspace, shard string) {
					defer wg.Done()

					si, err := fromTS.GetShard(ctx, keyspace, shard)
					if err != nil {
						rec.RecordError(fmt.Errorf("GetShard(%v, %v): %v", keyspace, shard, err))
						return
					}

					if err := toTS.CreateShard(ctx, keyspace, shard); err != nil {
						if err == topo.ErrNodeExists {
							log.Warningf("shard %v/%v already exists", keyspace, shard)
						} else {
							rec.RecordError(fmt.Errorf("CreateShard(%v, %v): %v", keyspace, shard, err))
							return
						}
					}
					if _, err := toTS.UpdateShardFields(ctx, keyspace, shard, func(toSI *topo.ShardInfo) error {
						*toSI.Shard = *si.Shard
						return nil
					}); err != nil {
						rec.RecordError(fmt.Errorf("UpdateShardFields(%v, %v): %v", keyspace, shard, err))
						return
					}
				}(keyspace, shard)
			}
		}(keyspace)
	}
	wg.Wait()
	if rec.HasErrors() {
		log.Fatalf("copyShards failed: %v", rec.Error())
	}
}

// CopyTablets will create the tablets in the destination topo.
func CopyTablets(ctx context.Context, fromTS, toTS *topo.Server) {
	cells, err := fromTS.GetKnownCells(ctx)
	if err != nil {
		log.Fatalf("fromTS.GetKnownCells: %v", err)
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			tabletAliases, err := fromTS.GetTabletsByCell(ctx, cell)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetTabletsByCell(%v): %v", cell, err))
			} else {
				for _, tabletAlias := range tabletAliases {
					wg.Add(1)
					go func(tabletAlias *topodatapb.TabletAlias) {
						defer wg.Done()

						// read the source tablet
						ti, err := fromTS.GetTablet(ctx, tabletAlias)
						if err != nil {
							rec.RecordError(fmt.Errorf("GetTablet(%v): %v", tabletAlias, err))
							return
						}

						// try to create the destination
						err = toTS.CreateTablet(ctx, ti.Tablet)
						if err == topo.ErrNodeExists {
							// update the destination tablet
							log.Warningf("tablet %v already exists, updating it", tabletAlias)
							_, err = toTS.UpdateTabletFields(ctx, tabletAlias, func(t *topodatapb.Tablet) error {
								*t = *ti.Tablet
								return nil
							})
						}
						if err != nil {
							rec.RecordError(fmt.Errorf("CreateTablet(%v): %v", tabletAlias, err))
							return
						}
					}(tabletAlias)
				}
			}
		}(cell)
	}
	wg.Wait()
	if rec.HasErrors() {
		log.Fatalf("copyTablets failed: %v", rec.Error())
	}
}

// CopyShardReplications will create the ShardReplication objects in
// the destination topo.
func CopyShardReplications(ctx context.Context, fromTS, toTS *topo.Server) {
	keyspaces, err := fromTS.GetKeyspaces(ctx)
	if err != nil {
		log.Fatalf("fromTS.GetKeyspaces: %v", err)
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shards, err := fromTS.GetShardNames(ctx, keyspace)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetShardNames(%v): %v", keyspace, err))
				return
			}

			for _, shard := range shards {
				wg.Add(1)
				go func(keyspace, shard string) {
					defer wg.Done()

					// read the source shard to get the cells
					si, err := fromTS.GetShard(ctx, keyspace, shard)
					if err != nil {
						rec.RecordError(fmt.Errorf("GetShard(%v, %v): %v", keyspace, shard, err))
						return
					}

					for _, cell := range si.Shard.Cells {
						sri, err := fromTS.GetShardReplication(ctx, cell, keyspace, shard)
						if err != nil {
							rec.RecordError(fmt.Errorf("GetShardReplication(%v, %v, %v): %v", cell, keyspace, shard, err))
							continue
						}

						if err := toTS.UpdateShardReplicationFields(ctx, cell, keyspace, shard, func(oldSR *topodatapb.ShardReplication) error {
							*oldSR = *sri.ShardReplication
							return nil
						}); err != nil {
							rec.RecordError(fmt.Errorf("UpdateShardReplicationFields(%v, %v, %v): %v", cell, keyspace, shard, err))
						}
					}
				}(keyspace, shard)
			}
		}(keyspace)
	}
	wg.Wait()
	if rec.HasErrors() {
		log.Fatalf("copyShards failed: %v", rec.Error())
	}
}
