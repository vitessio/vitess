/*
Copyright 2021 The Vitess Authors.

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

package topotools

import (
	"context"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// RefreshTabletsByShard calls RefreshState on all the tablets in a given shard.
//
// It only returns errors from looking up the tablet map from the topology;
// errors returned from any RefreshState RPCs are logged and then ignored. Also,
// any tablets without a .Hostname set in the topology are skipped.
func RefreshTabletsByShard(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, si *topo.ShardInfo, cells []string, logger logutil.Logger) error {
	logger.Infof("RefreshTabletsByShard called on shard %v/%v", si.Keyspace(), si.ShardName())

	tabletMap, err := ts.GetTabletMapForShardByCell(ctx, si.Keyspace(), si.ShardName(), cells)
	switch {
	case err == nil:
		// keep going
	case topo.IsErrType(err, topo.PartialResult):
		logger.Warningf("RefreshTabletsByShard: got partial result for shard %v/%v, may not refresh all tablets everywhere", si.Keyspace(), si.ShardName())
	default:
		return err
	}

	// Any errors from this point onward are ignored.
	var wg sync.WaitGroup
	for _, ti := range tabletMap {
		if ti.Hostname == "" {
			// The tablet is not running, we don't have the host
			// name to connect to, so we just skip this tablet.
			logger.Infof("Tablet %v has no hostname, skipping its RefreshState", ti.AliasString())
			continue
		}

		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			defer wg.Done()
			logger.Infof("Calling RefreshState on tablet %v", ti.AliasString())

			ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()

			if err := tmc.RefreshState(ctx, ti.Tablet); err != nil {
				logger.Warningf("RefreshTabletsByShard: failed to refresh %v: %v", ti.AliasString(), err)
			}
		}(ti)
	}

	wg.Wait()
	return nil
}

// UpdateShardRecords updates the shard records based on 'from' or 'to'
// direction.
func UpdateShardRecords(
	ctx context.Context,
	ts *topo.Server,
	tmc tmclient.TabletManagerClient,
	keyspace string,
	shards []*topo.ShardInfo,
	cells []string,
	servedType topodatapb.TabletType,
	isFrom bool,
	clearSourceShards bool,
	logger logutil.Logger,
) error {
	disableQueryService := isFrom
	if err := ts.UpdateDisableQueryService(ctx, keyspace, shards, servedType, cells, disableQueryService); err != nil {
		return err
	}

	for i, si := range shards {
		updatedShard, err := ts.UpdateShardFields(ctx, si.Keyspace(), si.ShardName(), func(si *topo.ShardInfo) error {
			if clearSourceShards {
				si.SourceShards = nil
			}

			return nil
		})

		if err != nil {
			return err
		}

		shards[i] = updatedShard

		// For 'to' shards, refresh to make them serve. The 'from' shards will
		// be refreshed after traffic has migrated.
		if !isFrom {
			if err := RefreshTabletsByShard(ctx, ts, tmc, si, cells, logger); err != nil {
				logger.Warningf("RefreshTabletsByShard(%v/%v, cells=%v) failed with %v; continuing ...", si.Keyspace(), si.ShardName(), cells, err)
			}
		}
	}

	return nil
}
