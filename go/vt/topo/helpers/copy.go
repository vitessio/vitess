// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// helpers package contains a few utility classes to handle topo.Server
// objects, and transitions from one topo implementation to another.
package helpers

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
)

// CopyKeyspaces will create the keyspaces in the destination topo
func CopyKeyspaces(fromTS, toTS topo.Server) {
	keyspaces, err := fromTS.GetKeyspaces()
	if err != nil {
		log.Fatalf("GetKeyspaces: %v", err)
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()

			k, err := fromTS.GetKeyspace(keyspace)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetKeyspace(%v): %v", keyspace, err))
				return
			}

			if err := toTS.CreateKeyspace(keyspace, k.Keyspace); err != nil {
				if err == topo.ErrNodeExists {
					log.Warningf("keyspace %v already exists", keyspace)
				} else {
					rec.RecordError(fmt.Errorf("CreateKeyspace(%v): %v", keyspace, err))
				}
			}
		}(keyspace)
	}
	wg.Wait()
	if rec.HasErrors() {
		log.Fatalf("copyKeyspaces failed: %v", rec.Error())
	}
}

// CopyShards will create the shards in the destination topo
func CopyShards(fromTS, toTS topo.Server, deleteKeyspaceShards bool) {
	keyspaces, err := fromTS.GetKeyspaces()
	if err != nil {
		log.Fatalf("fromTS.GetKeyspaces: %v", err)
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shards, err := fromTS.GetShardNames(keyspace)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetShardNames(%v): %v", keyspace, err))
				return
			}

			if deleteKeyspaceShards {
				if err := toTS.DeleteKeyspaceShards(keyspace); err != nil {
					rec.RecordError(fmt.Errorf("DeleteKeyspaceShards(%v): %v", keyspace, err))
					return
				}
			}

			for _, shard := range shards {
				wg.Add(1)
				go func(keyspace, shard string) {
					defer wg.Done()
					if err := topo.CreateShard(toTS, keyspace, shard); err != nil {
						if err == topo.ErrNodeExists {
							log.Warningf("shard %v/%v already exists", keyspace, shard)
						} else {
							rec.RecordError(fmt.Errorf("CreateShard(%v, %v): %v", keyspace, shard, err))
							return
						}
					}

					si, err := fromTS.GetShard(keyspace, shard)
					if err != nil {
						rec.RecordError(fmt.Errorf("GetShard(%v, %v): %v", keyspace, shard, err))
						return
					}

					toSi, err := toTS.GetShard(keyspace, shard)
					if err != nil {
						rec.RecordError(fmt.Errorf("toTS.GetShard(%v, %v): %v", keyspace, shard, err))
						return
					}

					if _, err := toTS.UpdateShard(si, toSi.Version()); err != nil {
						rec.RecordError(fmt.Errorf("UpdateShard(%v, %v): %v", keyspace, shard, err))
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

// CopyTablets will create the tablets in the destination topo
func CopyTablets(fromTS, toTS topo.Server) {
	cells, err := fromTS.GetKnownCells()
	if err != nil {
		log.Fatalf("fromTS.GetKnownCells: %v", err)
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			tabletAliases, err := fromTS.GetTabletsByCell(cell)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetTabletsByCell(%v): %v", cell, err))
			} else {
				for _, tabletAlias := range tabletAliases {
					wg.Add(1)
					go func(tabletAlias topo.TabletAlias) {
						defer wg.Done()

						// read the source tablet
						ti, err := fromTS.GetTablet(tabletAlias)
						if err != nil {
							rec.RecordError(fmt.Errorf("GetTablet(%v): %v", tabletAlias, err))
							return
						}

						// try to create the destination
						err = toTS.CreateTablet(ti.Tablet)
						if err == topo.ErrNodeExists {
							// update the destination tablet
							log.Warningf("tablet %v already exists, updating it", tabletAlias)
							err = toTS.UpdateTabletFields(ti.Alias, func(t *topo.Tablet) error {
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
// the destination topo
func CopyShardReplications(fromTS, toTS topo.Server) {
	keyspaces, err := fromTS.GetKeyspaces()
	if err != nil {
		log.Fatalf("fromTS.GetKeyspaces: %v", err)
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shards, err := fromTS.GetShardNames(keyspace)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetShardNames(%v): %v", keyspace, err))
				return
			}

			for _, shard := range shards {
				wg.Add(1)
				go func(keyspace, shard string) {
					defer wg.Done()

					// read the source shard to get the cells
					si, err := fromTS.GetShard(keyspace, shard)
					if err != nil {
						rec.RecordError(fmt.Errorf("GetShard(%v, %v): %v", keyspace, shard, err))
						return
					}

					for _, cell := range si.Cells {
						sri, err := fromTS.GetShardReplication(cell, keyspace, shard)
						if err != nil {
							rec.RecordError(fmt.Errorf("GetShardReplication(%v, %v, %v): %v", cell, keyspace, shard, err))
							continue
						}

						if err := toTS.UpdateShardReplicationFields(cell, keyspace, shard, func(oldSR *topo.ShardReplication) error {
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
