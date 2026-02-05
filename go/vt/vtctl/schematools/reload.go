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

package schematools

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// ReloadShard reloads the schema for all replica tablets in a shard, after
// they reach a given replication position (empty pos means immediate).
//
// In general, we don't always expect all replicas to be ready to reload,
// and the periodic schema reload makes them self-healing anyway.
// So we do this on a best-effort basis, and log warnings for any tablets
// that fail to reload within the context deadline.
func ReloadShard(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, logger logutil.Logger, keyspace, shard, replicationPos string, concurrency *semaphore.Weighted, includePrimary bool) (isPartial bool, ok bool) {
	tablets, err := ts.GetTabletMapForShard(ctx, keyspace, shard)
	switch {
	case topo.IsErrType(err, topo.PartialResult):
		// We got a partial result. Do what we can, but warn
		// that some may be missed.
		logger.Warningf("ReloadSchemaShard(%v/%v) got a partial tablet list. Some tablets may not have schema reloaded (use vtctl ReloadSchema to fix individual tablets)", keyspace, shard)
		isPartial = true
	case err == nil:
		// Good case, keep going too.
		isPartial = false
	default:
		// This is best-effort, so just log it and move on.
		logger.Warningf("ReloadSchemaShard(%v/%v) failed to load tablet list, will not reload schema (use vtctl ReloadSchemaShard to try again): %v", keyspace, shard, err)
		return false, false
	}

	var wg sync.WaitGroup
	for _, ti := range tablets {
		if !includePrimary && ti.Type == topodatapb.TabletType_PRIMARY {
			// We don't need to reload on the primary
			// because we assume ExecuteFetchAsDba()
			// already did that.
			continue
		}

		wg.Add(1)
		go func(tablet *topodatapb.Tablet) {
			defer wg.Done()

			if concurrency != nil {
				if err := concurrency.Acquire(ctx, 1); err != nil {
					// We timed out waiting for the semaphore. This is best-effort, so just log it and move on.
					logger.Warningf(
						"Failed to reload schema on replica tablet %v in %v/%v (use vtctl ReloadSchema to try again): timed out waiting for concurrency: %v",
						topoproto.TabletAliasString(tablet.Alias), keyspace, shard, err,
					)
					return
				}
				defer concurrency.Release(1)
			}

			pos := replicationPos
			// Primary is always up-to-date. So, don't wait for position.
			if tablet.Type == topodatapb.TabletType_PRIMARY {
				pos = ""
			}

			if err := tmc.ReloadSchema(ctx, tablet, pos); err != nil {
				logger.Warningf(
					"Failed to reload schema on replica tablet %v in %v/%v (use vtctl ReloadSchema to try again): %v",
					topoproto.TabletAliasString(tablet.Alias), keyspace, shard, err,
				)
			}
		}(ti.Tablet)
	}
	wg.Wait()

	return isPartial, true
}
