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

package srvtopo

import (
	"sync"

	"context"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// FindAllTargets goes through all serving shards in the topology
// for the provided tablet types. It returns one Target object per
// keyspace / shard / matching TabletType.
func FindAllTargets(ctx context.Context, ts Server, cell string, tabletTypes []topodatapb.TabletType) ([]*querypb.Target, error) {
	ksNames, err := ts.GetSrvKeyspaceNames(ctx, cell, true)
	if err != nil {
		return nil, err
	}

	var targets []*querypb.Target
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errRecorder concurrency.AllErrorRecorder
	for _, ksName := range ksNames {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()

			// Get SrvKeyspace for cell/keyspace.
			ks, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
			if err != nil {
				if topo.IsErrType(err, topo.NoNode) {
					// Possibly a race condition, or leftover
					// crud in the topology service. Just log it.
					log.Warningf("GetSrvKeyspace(%v, %v) returned ErrNoNode, skipping that SrvKeyspace", cell, keyspace)
				} else {
					// More serious error, abort.
					errRecorder.RecordError(err)
				}
				return
			}

			// Get all shard names that are used for serving.
			for _, ksPartition := range ks.Partitions {
				// Check we're waiting for tablets of that type.
				waitForIt := false
				for _, tt := range tabletTypes {
					if tt == ksPartition.ServedType {
						waitForIt = true
					}
				}
				if !waitForIt {
					continue
				}

				// Add all the shards. Note we can't have
				// duplicates, as there is only one entry per
				// TabletType in the Partitions list.
				mu.Lock()
				for _, shard := range ksPartition.ShardReferences {
					targets = append(targets, &querypb.Target{
						Cell:       cell,
						Keyspace:   keyspace,
						Shard:      shard.Name,
						TabletType: ksPartition.ServedType,
					})
				}
				mu.Unlock()
			}
		}(ksName)
	}
	wg.Wait()
	if errRecorder.HasErrors() {
		return nil, errRecorder.Error()
	}

	return targets, nil
}
