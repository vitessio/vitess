// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// topotools package contains a few utility classes to handle topo.Server
// objects, and transitions.
package topotools

import (
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
)

// CopyKeyspaces will create the keyspaces in the destination topo
func CopyKeyspaces(fromTS, toTS topo.Server) {
	keyspaces, err := fromTS.GetKeyspaces()
	if err != nil {
		log.Fatalf("fromTS.GetKeyspaces failed: %v", err)
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			if err := toTS.CreateKeyspace(keyspace); err != nil {
				if err == topo.ErrNodeExists {
					log.Warningf("keyspace %v already exists", keyspace)
				} else {
					rec.RecordError(err)
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
		log.Fatalf("fromTS.GetKeyspaces failed: %v", err)
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shards, err := fromTS.GetShardNames(keyspace)
			if err != nil {
				rec.RecordError(err)
				return
			}

			if deleteKeyspaceShards {
				if err := toTS.DeleteKeyspaceShards(keyspace); err != nil {
					rec.RecordError(err)
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
							rec.RecordError(err)
							return
						}
					}

					si, err := fromTS.GetShard(keyspace, shard)
					if err != nil {
						rec.RecordError(err)
						return
					}

					if err := toTS.UpdateShard(si); err != nil {
						rec.RecordError(err)
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
		log.Fatalf("fromTS.GetKnownCells failed: %v", err)
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			tabletAliases, err := fromTS.GetTabletsByCell(cell)
			if err != nil {
				rec.RecordError(err)
			} else {
				for _, tabletAlias := range tabletAliases {
					wg.Add(1)
					go func(tabletAlias topo.TabletAlias) {
						defer wg.Done()

						// read the source tablet
						ti, err := fromTS.GetTablet(tabletAlias)
						if err != nil {
							rec.RecordError(err)
							return
						}

						// try to create the destination
						err = toTS.CreateTablet(ti.Tablet)
						if err == topo.ErrNodeExists {
							// update the destination tablet
							log.Warningf("tablet %v already exists, updating it", tabletAlias)
							err = toTS.UpdateTabletFields(ti.Alias(), func(t *topo.Tablet) error {
								*t = *ti.Tablet
								return nil
							})
						}
						if err != nil {
							rec.RecordError(err)
							return
						}

						// create the replication paths
						// for masters only here
						if ti.Type == topo.TYPE_MASTER {
							if err = toTS.CreateReplicationPath(ti.Keyspace, ti.Shard, ti.Alias().String()); err != nil && err != topo.ErrNodeExists {
								rec.RecordError(err)
							}
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
		log.Fatalf("fromTS.GetKeyspaces failed: %v", err)
	}

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shards, err := fromTS.GetShardNames(keyspace)
			if err != nil {
				rec.RecordError(err)
				return
			}

			for _, shard := range shards {
				wg.Add(1)
				go func(keyspace, shard string) {
					defer wg.Done()

					// read the source shard to get the cells
					si, err := fromTS.GetShard(keyspace, shard)
					if err != nil {
						rec.RecordError(err)
						return
					}

					for _, cell := range si.Cells {
						sri, err := fromTS.GetShardReplication(cell, keyspace, shard)
						if err != nil {
							rec.RecordError(err)
							continue
						}

						err = toTS.CreateShardReplication(cell, keyspace, shard, sri.ShardReplication)
						switch err {
						case nil:
							// good
						case topo.ErrNodeExists:
							if err := toTS.UpdateShardReplicationFields(cell, keyspace, shard, func(oldSR *topo.ShardReplication) error {
								*oldSR = *sri.ShardReplication
								return nil
							}); err != nil {
								rec.RecordError(err)
							}
						default:
							rec.RecordError(err)
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
