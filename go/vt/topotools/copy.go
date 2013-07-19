// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"sync"

	"github.com/youtube/vitess/go/relog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
)

// CopyKeyspaces will create the keyspaces in the destination topo
func CopyKeyspaces(fromTS, toTS topo.Server) {
	keyspaces, err := fromTS.GetKeyspaces()
	if err != nil {
		relog.Fatal("fromTS.GetKeyspaces failed: %v", err)
	}

	wg := sync.WaitGroup{}
	er := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			if err := toTS.CreateKeyspace(keyspace); err != nil {
				if err == topo.ErrNodeExists {
					relog.Warning("Keyspace %v already exists", keyspace)
				} else {
					er.RecordError(err)
				}
			}
		}(keyspace)
	}
	wg.Wait()
	if er.HasErrors() {
		relog.Fatal("copyKeyspaces failed: %v", err)
	}
}

// CopyShards will create the keyspaces in the destination topo
func CopyShards(fromTS, toTS topo.Server, deleteKeyspaceShards bool) {
	keyspaces, err := fromTS.GetKeyspaces()
	if err != nil {
		relog.Fatal("fromTS.GetKeyspaces failed: %v", err)
	}

	wg := sync.WaitGroup{}
	er := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shards, err := fromTS.GetShardNames(keyspace)
			if err != nil {
				er.RecordError(err)
				return
			}

			if deleteKeyspaceShards {
				if err := toTS.DeleteKeyspaceShards(keyspace); err != nil {
					er.RecordError(err)
					return
				}
			}

			for _, shard := range shards {
				wg.Add(1)
				go func(keyspace, shard string) {
					defer wg.Done()
					if err := toTS.CreateShard(keyspace, shard); err != nil {
						if err == topo.ErrNodeExists {
							relog.Warning("Shard %v/%v already exists", keyspace, shard)
						} else {
							er.RecordError(err)
						}
					}
				}(keyspace, shard)
			}
		}(keyspace)
	}
	wg.Wait()
	if er.HasErrors() {
		relog.Fatal("copyShards failed: %v", err)
	}
}

// CopyTablets will create the tablets in the destination topo
func CopyTablets(fromTS, toTS topo.Server) {
	cells, err := fromTS.GetKnownCells()
	if err != nil {
		relog.Fatal("fromTS.GetKnownCells failed: %v", err)
	}

	wg := sync.WaitGroup{}
	er := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			tabletAliases, err := fromTS.GetTabletsByCell(cell)
			if err != nil {
				er.RecordError(err)
			} else {
				for _, tabletAlias := range tabletAliases {
					wg.Add(1)
					go func(tabletAlias topo.TabletAlias) {
						defer wg.Done()

						// read the source tablet
						ti, err := fromTS.GetTablet(tabletAlias)
						if err != nil {
							er.RecordError(err)
							return
						}

						// try to create the destination
						err = toTS.CreateTablet(ti.Tablet)
						if err == topo.ErrNodeExists {
							// update the destination tablet
							_, err = toTS.UpdateTablet(ti, -1)

						}
						if err != nil {
							er.RecordError(err)
							return
						}

						// create the replication paths
						// for masters only here
						if ti.Type == topo.TYPE_MASTER {
							if err = toTS.CreateReplicationPath(ti.Keyspace, ti.Shard, ti.Alias().String()); err != nil && err != topo.ErrNodeExists {
								er.RecordError(err)
							}
						}
					}(tabletAlias)
				}
			}
		}(cell)
	}
	wg.Wait()
	if er.HasErrors() {
		relog.Fatal("copyTablets failed: %v", er.Error())
	}
}
