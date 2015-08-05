// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// RebuildShard updates the SrvShard objects and underlying serving graph.
//
// Re-read from TopologyServer to make sure we are using the side
// effects of all actions.
//
// This function will start each cell over from the beginning on ErrBadVersion,
// so it doesn't need a lock on the shard.
func RebuildShard(ctx context.Context, log logutil.Logger, ts topo.Server, keyspace, shard string, cells []string, lockTimeout time.Duration) (*topo.ShardInfo, error) {
	log.Infof("RebuildShard %v/%v", keyspace, shard)

	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("topotools.RebuildShard")
	defer span.Finish()
	ctx = trace.NewContext(ctx, span)

	// read the existing shard info. It has to exist.
	shardInfo, err := ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return nil, err
	}

	// rebuild all cells in parallel
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range shardInfo.Cells {
		// skip this cell if we shouldn't rebuild it
		if !topo.InCellList(cell, cells) {
			continue
		}

		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			rec.RecordError(rebuildCellSrvShard(ctx, log, ts, shardInfo, cell))
		}(cell)
	}
	wg.Wait()

	return shardInfo, rec.Error()
}

// rebuildCellSrvShard computes and writes the serving graph data to a
// single cell
func rebuildCellSrvShard(ctx context.Context, log logutil.Logger, ts topo.Server, si *topo.ShardInfo, cell string) (err error) {
	log.Infof("rebuildCellSrvShard %v/%v in cell %v", si.Keyspace(), si.ShardName(), cell)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read existing EndPoints node versions, so we know if any
		// changes sneak in after we read the tablets.
		versions, err := getEndPointsVersions(ctx, ts, cell, si.Keyspace(), si.ShardName())

		// Get all tablets in this cell/shard.
		tablets, err := topo.GetTabletMapForShardByCell(ctx, ts, si.Keyspace(), si.ShardName(), []string{cell})
		if err != nil {
			if err != topo.ErrPartialResult {
				return err
			}
			log.Warningf("Got ErrPartialResult from topo.GetTabletMapForShardByCell(%v), some tablets may not be added properly to serving graph", cell)
		}

		// Build up the serving graph from scratch.
		serving := make(map[topo.TabletType]*pb.EndPoints)
		for _, tablet := range tablets {
			if !tablet.IsInReplicationGraph() {
				// only valid case is a scrapped master in the
				// catastrophic reparent case
				log.Warningf("Tablet %v should not be in the replication graph, please investigate (it is being ignored in the rebuild)", tablet.Alias)
				continue
			}

			// Only add serving types.
			if !tablet.IsInServingGraph() {
				continue
			}

			// Check the Keyspace and Shard for the tablet are right.
			if tablet.Keyspace != si.Keyspace() || tablet.Shard != si.ShardName() {
				return fmt.Errorf("CRITICAL: tablet %v is in replication graph for shard %v/%v but belongs to shard %v:%v", tablet.Alias, si.Keyspace(), si.ShardName(), tablet.Keyspace, tablet.Shard)
			}

			// Add the tablet to the list.
			endpoints, ok := serving[tablet.Type]
			if !ok {
				endpoints = topo.NewEndPoints()
				serving[tablet.Type] = endpoints
			}
			entry, err := tablet.EndPoint()
			if err != nil {
				log.Warningf("EndPointForTablet failed for tablet %v: %v", tablet.Alias, err)
				continue
			}
			endpoints.Entries = append(endpoints.Entries, entry)
		}

		wg := sync.WaitGroup{}
		fatalErrs := concurrency.AllErrorRecorder{}
		retryErrs := concurrency.AllErrorRecorder{}

		// Write nodes that should exist.
		for tabletType, endpoints := range serving {
			wg.Add(1)
			go func(tabletType topo.TabletType, endpoints *pb.EndPoints) {
				defer wg.Done()

				log.Infof("saving serving graph for cell %v shard %v/%v tabletType %v", cell, si.Keyspace(), si.ShardName(), tabletType)

				version, ok := versions[tabletType]
				if !ok {
					// This type didn't exist when we first checked.
					// Try to create, but only if it still doesn't exist.
					if err := ts.CreateEndPoints(ctx, cell, si.Keyspace(), si.ShardName(), tabletType, endpoints); err != nil {
						log.Warningf("CreateEndPoints(%v, %v, %v) failed during rebuild: %v", cell, si, tabletType, err)
						switch err {
						case topo.ErrNodeExists:
							retryErrs.RecordError(err)
						default:
							fatalErrs.RecordError(err)
						}
					}
					return
				}

				// Update only if the version matches.
				if err := ts.UpdateEndPoints(ctx, cell, si.Keyspace(), si.ShardName(), tabletType, endpoints, version); err != nil {
					log.Warningf("UpdateEndPoints(%v, %v, %v) failed during rebuild: %v", cell, si, tabletType, err)
					switch err {
					case topo.ErrBadVersion, topo.ErrNoNode:
						retryErrs.RecordError(err)
					default:
						fatalErrs.RecordError(err)
					}
				}
			}(tabletType, endpoints)
		}

		// Delete nodes that shouldn't exist.
		for tabletType, version := range versions {
			if _, ok := serving[tabletType]; !ok {
				wg.Add(1)
				go func(tabletType topo.TabletType, version int64) {
					defer wg.Done()
					log.Infof("removing stale db type from serving graph: %v", tabletType)
					if err := ts.DeleteEndPoints(ctx, cell, si.Keyspace(), si.ShardName(), tabletType, version); err != nil && err != topo.ErrNoNode {
						log.Warningf("DeleteEndPoints(%v, %v, %v) failed during rebuild: %v", cell, si, tabletType, err)
						switch err {
						case topo.ErrNoNode:
							// Someone else deleted it, which is fine.
						case topo.ErrBadVersion:
							retryErrs.RecordError(err)
						default:
							fatalErrs.RecordError(err)
						}
					}
				}(tabletType, version)
			}
		}

		// Update srvShard object
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Infof("updating shard serving graph in cell %v for %v/%v", cell, si.Keyspace(), si.ShardName())
			if err := UpdateSrvShard(ctx, ts, cell, si); err != nil {
				fatalErrs.RecordError(err)
				log.Warningf("writing serving data in cell %v for %v/%v failed: %v", cell, si.Keyspace(), si.ShardName(), err)
			}
		}()

		wg.Wait()

		// If there are any fatal errors, give up.
		if fatalErrs.HasErrors() {
			return fatalErrs.Error()
		}
		// If there are any retry errors, try again.
		if retryErrs.HasErrors() {
			continue
		}
		// Otherwise, success!
		return nil
	}
}

func getEndPointsVersions(ctx context.Context, ts topo.Server, cell, keyspace, shard string) (map[topo.TabletType]int64, error) {
	// Get all existing tablet types.
	tabletTypes, err := ts.GetSrvTabletTypesPerShard(ctx, cell, keyspace, shard)
	if err != nil {
		if err == topo.ErrNoNode {
			// This just means there aren't any EndPoints lists yet.
			return nil, nil
		}
		return nil, err
	}

	// Get node versions.
	wg := sync.WaitGroup{}
	errs := concurrency.AllErrorRecorder{}
	versions := make(map[topo.TabletType]int64)
	mu := sync.Mutex{}

	for _, tabletType := range tabletTypes {
		wg.Add(1)
		go func(tabletType topo.TabletType) {
			defer wg.Done()

			_, version, err := ts.GetEndPoints(ctx, cell, keyspace, shard, tabletType)
			if err != nil && err != topo.ErrNoNode {
				errs.RecordError(err)
				return
			}

			mu.Lock()
			versions[tabletType] = version
			mu.Unlock()
		}(tabletType)
	}

	wg.Wait()
	return versions, errs.Error()
}

func updateEndpoint(ctx context.Context, ts topo.Server, cell, keyspace, shard string, tabletType topo.TabletType, endpoint *pb.EndPoint) error {
	return retryUpdateEndpoints(ctx, ts, cell, keyspace, shard, tabletType, true, /* create */
		func(endpoints *pb.EndPoints) bool {
			// Look for an existing entry to update.
			for i := range endpoints.Entries {
				if endpoints.Entries[i].Uid == endpoint.Uid {
					if topo.EndPointEquality(endpoints.Entries[i], endpoint) {
						// The entry already exists and is the same.
						return false
					}
					// Update an existing entry.
					endpoints.Entries[i] = endpoint
					return true
				}
			}
			// The entry doesn't exist, so add it.
			endpoints.Entries = append(endpoints.Entries, endpoint)
			return true
		})
}

func removeEndpoint(ctx context.Context, ts topo.Server, cell, keyspace, shard string, tabletType topo.TabletType, tabletUID uint32) error {
	err := retryUpdateEndpoints(ctx, ts, cell, keyspace, shard, tabletType, false, /* create */
		func(endpoints *pb.EndPoints) bool {
			// Make a new list, excluding the given UID.
			entries := make([]*pb.EndPoint, 0, len(endpoints.Entries))
			for _, ep := range endpoints.Entries {
				if ep.Uid != tabletUID {
					entries = append(entries, ep)
				}
			}
			if len(entries) == len(endpoints.Entries) {
				// Nothing was removed. Don't bother updating.
				return false
			}
			// Do the update.
			endpoints.Entries = entries
			return true
		})

	if err == topo.ErrNoNode {
		// Our goal is to remove one endpoint. If the list is empty, we're fine.
		err = nil
	}
	return err
}

func retryUpdateEndpoints(ctx context.Context, ts topo.Server, cell, keyspace, shard string, tabletType topo.TabletType, create bool, updateFunc func(*pb.EndPoints) bool) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Get or create EndPoints list.
		endpoints, version, err := ts.GetEndPoints(ctx, cell, keyspace, shard, tabletType)
		if err == topo.ErrNoNode && create {
			// Create instead of updating.
			endpoints = &pb.EndPoints{}
			if !updateFunc(endpoints) {
				// Nothing changed.
				return nil
			}
			err = ts.CreateEndPoints(ctx, cell, keyspace, shard, tabletType, endpoints)
			if err == topo.ErrNodeExists {
				// Someone else beat us to it. Try again.
				continue
			}
			return err
		}
		if err != nil {
			return err
		}

		// We got an existing EndPoints list. Try to update.
		if !updateFunc(endpoints) {
			// Nothing changed.
			return nil
		}

		// If there's nothing left, we should delete the list entirely.
		if len(endpoints.Entries) == 0 {
			err = ts.DeleteEndPoints(ctx, cell, keyspace, shard, tabletType, version)
			switch err {
			case topo.ErrNoNode:
				// Someone beat us to it, which is fine.
				return nil
			case topo.ErrBadVersion:
				// Someone else updated the list. Try again.
				continue
			}
			return err
		}

		err = ts.UpdateEndPoints(ctx, cell, keyspace, shard, tabletType, endpoints, version)
		if err == topo.ErrBadVersion || (err == topo.ErrNoNode && create) {
			// Someone else updated or deleted the list in the meantime. Try again.
			continue
		}
		return err
	}
}

// UpdateTabletEndpoints fixes up any entries in the serving graph that relate
// to a given tablet.
func UpdateTabletEndpoints(ctx context.Context, ts topo.Server, tablet *topo.Tablet) (err error) {
	srvTypes, err := ts.GetSrvTabletTypesPerShard(ctx, tablet.Alias.Cell, tablet.Keyspace, tablet.Shard)
	if err != nil {
		if err != topo.ErrNoNode {
			return err
		}
		// It's fine if there are no existing types.
		srvTypes = nil
	}

	wg := sync.WaitGroup{}
	errs := concurrency.AllErrorRecorder{}

	// Update the list that the tablet is supposed to be in (if any).
	if tablet.IsInServingGraph() {
		endpoint, err := tablet.EndPoint()
		if err != nil {
			return err
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			errs.RecordError(
				updateEndpoint(ctx, ts, tablet.Alias.Cell, tablet.Keyspace, tablet.Shard,
					tablet.Type, endpoint))
		}()
	}

	// Remove it from any other lists it isn't supposed to be in.
	for _, srvType := range srvTypes {
		if srvType != tablet.Type {
			wg.Add(1)
			go func(tabletType topo.TabletType) {
				defer wg.Done()
				errs.RecordError(
					removeEndpoint(ctx, ts, tablet.Alias.Cell, tablet.Keyspace, tablet.Shard,
						tabletType, tablet.Alias.Uid))
			}(srvType)
		}
	}

	wg.Wait()
	return errs.Error()
}

// UpdateSrvShard creates the SrvShard object based on the global ShardInfo,
// and writes it to the given cell.
func UpdateSrvShard(ctx context.Context, ts topo.Server, cell string, si *topo.ShardInfo) error {
	srvShard := &pb.SrvShard{
		Name:     si.ShardName(),
		KeyRange: si.KeyRange,
	}
	if si.MasterAlias != nil {
		srvShard.MasterCell = si.MasterAlias.Cell
	}
	return ts.UpdateSrvShard(ctx, cell, si.Keyspace(), si.ShardName(), srvShard)
}

// UpdateAllSrvShards calls UpdateSrvShard for all cells concurrently.
func UpdateAllSrvShards(ctx context.Context, ts topo.Server, si *topo.ShardInfo) error {
	wg := sync.WaitGroup{}
	errs := concurrency.AllErrorRecorder{}

	for _, cell := range si.Cells {
		wg.Add(1)
		go func(cell string) {
			errs.RecordError(UpdateSrvShard(ctx, ts, cell, si))
			wg.Done()
		}(cell)
	}
	wg.Wait()
	return errs.Error()
}
