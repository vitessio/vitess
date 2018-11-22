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
	"sync"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// CompareKeyspaces will create the keyspaces in the destination topo.
func CompareKeyspaces(ctx context.Context, fromTS, toTS *topo.Server) {
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

			fromKs, err := fromTS.GetKeyspace(ctx, keyspace)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetKeyspace(%v): %v", keyspace, err))
				return
			}

			toKs, err := toTS.GetKeyspace(ctx, keyspace)
			if err != nil {
				rec.RecordError(fmt.Errorf("GetKeyspace(%v): %v", keyspace, err))
				return
			}

			if !reflect.DeepEqual(fromKs.Keyspace, toKs.Keyspace) {
				rec.RecordError(fmt.Errorf("Keyspace: %v does not match between from and to topology", keyspace))
				return
			}

			fromVs, err := fromTS.GetVSchema(ctx, keyspace)
			switch {
			case err == nil:
				// Nothing to do.
			case topo.IsErrType(err, topo.NoNode):
				// Nothing to do.
			default:
				rec.RecordError(fmt.Errorf("GetVSchema(%v): %v", keyspace, err))
				return
			}

			toVs, err := toTS.GetVSchema(ctx, keyspace)
			switch {
			case err == nil:
				// Nothing to do.
			case topo.IsErrType(err, topo.NoNode):
				// Nothing to do.
			default:
				rec.RecordError(fmt.Errorf("GetVSchema(%v): %v", keyspace, err))
				return
			}

			if !reflect.DeepEqual(fromVs, toVs) {
				rec.RecordError(fmt.Errorf("Vschema for keyspace: %v does not match between from and to topology", keyspace))
				return
			}
		}(keyspace)
	}
	wg.Wait()
	if rec.HasErrors() {
		log.Fatalf("compareKeyspaces failed: %v", rec.Error())
	}
	fmt.Println("Keyspaces are matching between from and to topologies")
}

// CompareShards will create the shards in the destination topo.
func CompareShards(ctx context.Context, fromTS, toTS *topo.Server) {
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

					fromSi, err := fromTS.GetShard(ctx, keyspace, shard)
					if err != nil {
						rec.RecordError(fmt.Errorf("GetShard(%v, %v): %v", keyspace, shard, err))
						return
					}
					toSi, err := toTS.GetShard(ctx, keyspace, shard)
					if err != nil {
						rec.RecordError(fmt.Errorf("GetShard(%v, %v): %v", keyspace, shard, err))
						return
					}

					if !reflect.DeepEqual(fromSi.Shard, toSi.Shard) {
						rec.RecordError(fmt.Errorf("Shard %v for keyspace: %v does not match between from and to topology", shard, keyspace))
						return
					}
				}(keyspace, shard)
			}
		}(keyspace)
	}
	wg.Wait()
	if rec.HasErrors() {
		log.Fatalf("compareShards failed: %v", rec.Error())
	}
	fmt.Println("Shards are matching between from and to topologies")
}

// CompareTablets will create the tablets in the destination topo.
func CompareTablets(ctx context.Context, fromTS, toTS *topo.Server) {
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
						fromTi, err := fromTS.GetTablet(ctx, tabletAlias)
						if err != nil {
							rec.RecordError(fmt.Errorf("GetTablet(%v): %v", tabletAlias, err))
							return
						}
						toTi, err := toTS.GetTablet(ctx, tabletAlias)
						if err != nil {
							rec.RecordError(fmt.Errorf("GetTablet(%v): %v", tabletAlias, err))
							return
						}
						if !reflect.DeepEqual(fromTi.Tablet, toTi.Tablet) {
							rec.RecordError(fmt.Errorf("Tablet %v:  does not match between from and to topology", tabletAlias))
							return
						}
					}(tabletAlias)
				}
			}
		}(cell)
	}
	wg.Wait()
	if rec.HasErrors() {
		log.Fatalf("compareTablets failed: %v", rec.Error())
	}
	fmt.Println("Tablets are matching between from and to topologies")
}

// CompareShardReplications will create the ShardReplication objects in
// the destination topo.
func CompareShardReplications(ctx context.Context, fromTS, toTS *topo.Server) {
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
						fromSRi, err := fromTS.GetShardReplication(ctx, cell, keyspace, shard)
						if err != nil {
							rec.RecordError(fmt.Errorf("GetShardReplication(%v, %v, %v): %v", cell, keyspace, shard, err))
							continue
						}
						toSRi, err := toTS.GetShardReplication(ctx, cell, keyspace, shard)
						if err != nil {
							rec.RecordError(fmt.Errorf("GetShardReplication(%v, %v, %v): %v", cell, keyspace, shard, err))
							continue
						}
						if !reflect.DeepEqual(fromSRi.ShardReplication, toSRi.ShardReplication) {
							rec.RecordError(
								fmt.Errorf(
									"Shard Replication in cell %v, keyspace %v, shard %v:  does not match between from and to topology",
									cell,
									keyspace,
									shard),
							)
							return
						}
					}
				}(keyspace, shard)
			}
		}(keyspace)
	}
	wg.Wait()
	if rec.HasErrors() {
		log.Fatalf("compareShardReplication failed: %v", rec.Error())
	}
	fmt.Println("Shard replications are matching between from and to topologies")
}
