// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/topotools/events"
	"golang.org/x/net/context"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// keyspace related methods for Wrangler

func (wr *Wrangler) lockKeyspace(ctx context.Context, keyspace string, actionNode *actionnode.ActionNode) (lockPath string, err error) {
	return actionNode.LockKeyspace(ctx, wr.ts, keyspace)
}

func (wr *Wrangler) unlockKeyspace(ctx context.Context, keyspace string, actionNode *actionnode.ActionNode, lockPath string, actionError error) error {
	return actionNode.UnlockKeyspace(ctx, wr.ts, keyspace, lockPath, actionError)
}

// SetKeyspaceShardingInfo locks a keyspace and sets its ShardingColumnName
// and ShardingColumnType
func (wr *Wrangler) SetKeyspaceShardingInfo(ctx context.Context, keyspace, shardingColumnName string, shardingColumnType topodatapb.KeyspaceIdType, splitShardCount int32, force bool) error {
	actionNode := actionnode.SetKeyspaceShardingInfo()
	lockPath, err := wr.lockKeyspace(ctx, keyspace, actionNode)
	if err != nil {
		return err
	}

	err = wr.setKeyspaceShardingInfo(ctx, keyspace, shardingColumnName, shardingColumnType, splitShardCount, force)
	return wr.unlockKeyspace(ctx, keyspace, actionNode, lockPath, err)

}

func (wr *Wrangler) setKeyspaceShardingInfo(ctx context.Context, keyspace, shardingColumnName string, shardingColumnType topodatapb.KeyspaceIdType, splitShardCount int32, force bool) error {
	ki, err := wr.ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}

	if ki.ShardingColumnName != "" && ki.ShardingColumnName != shardingColumnName {
		if force {
			wr.Logger().Warningf("Forcing keyspace ShardingColumnName change from %v to %v", ki.ShardingColumnName, shardingColumnName)
		} else {
			return fmt.Errorf("Cannot change ShardingColumnName from %v to %v (use -force to override)", ki.ShardingColumnName, shardingColumnName)
		}
	}

	if ki.ShardingColumnType != topodatapb.KeyspaceIdType_UNSET && ki.ShardingColumnType != shardingColumnType {
		if force {
			wr.Logger().Warningf("Forcing keyspace ShardingColumnType change from %v to %v", ki.ShardingColumnType, shardingColumnType)
		} else {
			return fmt.Errorf("Cannot change ShardingColumnType from %v to %v (use -force to override)", ki.ShardingColumnType, shardingColumnType)
		}
	}

	ki.ShardingColumnName = shardingColumnName
	ki.ShardingColumnType = shardingColumnType
	ki.SplitShardCount = splitShardCount
	return wr.ts.UpdateKeyspace(ctx, ki)
}

// MigrateServedTypes is used during horizontal splits to migrate a
// served type from a list of shards to another.
func (wr *Wrangler) MigrateServedTypes(ctx context.Context, keyspace, shard string, cells []string, servedType topodatapb.TabletType, reverse, skipReFreshState bool, filteredReplicationWaitTime time.Duration) error {
	if servedType == topodatapb.TabletType_MASTER {
		// we cannot migrate a master back, since when master migration
		// is done, the source shards are dead
		if reverse {
			return fmt.Errorf("Cannot migrate master back to %v/%v", keyspace, shard)
		}
		// we cannot skip refresh state for a master
		if skipReFreshState {
			return fmt.Errorf("Cannot skip refresh state for master migration on %v/%v", keyspace, shard)
		}
	}

	// find overlapping shards in this keyspace
	wr.Logger().Infof("Finding the overlapping shards in keyspace %v", keyspace)
	osList, err := topotools.FindOverlappingShards(ctx, wr.ts, keyspace)
	if err != nil {
		return fmt.Errorf("FindOverlappingShards failed: %v", err)
	}

	// find our shard in there
	os := topotools.OverlappingShardsForShard(osList, shard)
	if os == nil {
		return fmt.Errorf("Shard %v is not involved in any overlapping shards", shard)
	}

	// find which list is which: the sources have no source
	// shards, the destination have source shards. We check the
	// first entry in the lists, then just check they're
	// consistent
	var sourceShards []*topo.ShardInfo
	var destinationShards []*topo.ShardInfo
	if len(os.Left[0].SourceShards) == 0 {
		sourceShards = os.Left
		destinationShards = os.Right
	} else {
		sourceShards = os.Right
		destinationShards = os.Left
	}

	// Verify the sources has the type we're migrating (or not if reverse)
	for _, si := range sourceShards {
		if err := si.CheckServedTypesMigration(servedType, cells, !reverse); err != nil {
			return err
		}
	}

	// Verify the destinations do not have the type we're
	// migrating (or do if reverse)
	for _, si := range destinationShards {
		if err := si.CheckServedTypesMigration(servedType, cells, reverse); err != nil {
			return err
		}
	}

	// lock the shards: sources, then destinations
	// (note they're all ordered by shard name)
	actionNode := actionnode.MigrateServedTypes(servedType)
	sourceLockPath := make([]string, len(sourceShards))
	for i, si := range sourceShards {
		sourceLockPath[i], err = wr.lockShard(ctx, si.Keyspace(), si.ShardName(), actionNode)
		if err != nil {
			wr.Logger().Errorf("Failed to lock source shard %v/%v, may need to unlock other shards manually", si.Keyspace(), si.ShardName())
			return err
		}
	}
	destinationLockPath := make([]string, len(destinationShards))
	for i, si := range destinationShards {
		destinationLockPath[i], err = wr.lockShard(ctx, si.Keyspace(), si.ShardName(), actionNode)
		if err != nil {
			wr.Logger().Errorf("Failed to lock destination shard %v/%v, may need to unlock other shards manually", si.Keyspace(), si.ShardName())
			return err
		}
	}

	// record the action error and all unlock errors
	rec := concurrency.AllErrorRecorder{}

	// execute the migration
	rec.RecordError(wr.migrateServedTypes(ctx, keyspace, sourceShards, destinationShards, cells, servedType, reverse, filteredReplicationWaitTime))

	// unlock the shards, we're done
	for i := len(destinationShards) - 1; i >= 0; i-- {
		rec.RecordError(wr.unlockShard(ctx, destinationShards[i].Keyspace(), destinationShards[i].ShardName(), actionNode, destinationLockPath[i], nil))
	}
	for i := len(sourceShards) - 1; i >= 0; i-- {
		rec.RecordError(wr.unlockShard(ctx, sourceShards[i].Keyspace(), sourceShards[i].ShardName(), actionNode, sourceLockPath[i], nil))
	}

	// rebuild the keyspace serving graph if there was no error
	if !rec.HasErrors() {
		rec.RecordError(wr.RebuildKeyspaceGraph(ctx, keyspace, cells, false))
	}

	// Send a refresh to the tablets we just disabled, iff:
	// - we're not migrating a master
	// - we don't have any errors
	// - we're not told to skip the refresh
	if servedType != topodatapb.TabletType_MASTER && !rec.HasErrors() && !skipReFreshState {
		var refreshShards []*topo.ShardInfo
		if reverse {
			// For a backwards migration, we just disabled query service on the destination shards
			refreshShards = destinationShards
		} else {
			// For a forwards migration, we just disabled query service on the source shards
			refreshShards = sourceShards
		}
		for _, si := range refreshShards {
			rec.RecordError(wr.RefreshTablesByShard(ctx, si, servedType, cells))
		}
	}

	return rec.Error()
}

func (wr *Wrangler) getMastersPosition(ctx context.Context, shards []*topo.ShardInfo) (map[*topo.ShardInfo]string, error) {
	mu := sync.Mutex{}
	result := make(map[*topo.ShardInfo]string)

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range shards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
			wr.Logger().Infof("Gathering master position for %v", topoproto.TabletAliasString(si.MasterAlias))
			ti, err := wr.ts.GetTablet(ctx, si.MasterAlias)
			if err != nil {
				rec.RecordError(err)
				return
			}

			pos, err := wr.tmc.MasterPosition(ctx, ti)
			if err != nil {
				rec.RecordError(err)
				return
			}

			wr.Logger().Infof("Got master position for %v", topoproto.TabletAliasString(si.MasterAlias))
			mu.Lock()
			result[si] = pos
			mu.Unlock()
		}(si)
	}
	wg.Wait()
	return result, rec.Error()
}

func (wr *Wrangler) waitForFilteredReplication(ctx context.Context, sourcePositions map[*topo.ShardInfo]string, destinationShards []*topo.ShardInfo, waitTime time.Duration) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range destinationShards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
			for _, sourceShard := range si.SourceShards {
				// we're waiting on this guy
				blpPosition := &tabletmanagerdatapb.BlpPosition{
					Uid: sourceShard.Uid,
				}

				// find the position it should be at
				for s, pos := range sourcePositions {
					if s.Keyspace() == sourceShard.Keyspace && s.ShardName() == sourceShard.Shard {
						blpPosition.Position = pos
					}
				}

				// and wait for it
				wr.Logger().Infof("Waiting for %v to catch up", topoproto.TabletAliasString(si.MasterAlias))
				tablet, err := wr.ts.GetTablet(ctx, si.MasterAlias)
				if err != nil {
					rec.RecordError(err)
					return
				}

				if err := wr.tmc.WaitBlpPosition(ctx, tablet, blpPosition, waitTime); err != nil {
					rec.RecordError(err)
				} else {
					wr.Logger().Infof("%v caught up", topoproto.TabletAliasString(si.MasterAlias))
				}
			}
		}(si)
	}
	wg.Wait()
	return rec.Error()
}

// refreshMasters will just RPC-ping all the masters with RefreshState
func (wr *Wrangler) refreshMasters(ctx context.Context, shards []*topo.ShardInfo) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range shards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
			wr.Logger().Infof("RefreshState master %v", topoproto.TabletAliasString(si.MasterAlias))
			ti, err := wr.ts.GetTablet(ctx, si.MasterAlias)
			if err != nil {
				rec.RecordError(err)
				return
			}

			if err := wr.tmc.RefreshState(ctx, ti); err != nil {
				rec.RecordError(err)
			} else {
				wr.Logger().Infof("%v responded", topoproto.TabletAliasString(si.MasterAlias))
			}
		}(si)
	}
	wg.Wait()
	return rec.Error()
}

// migrateServedTypes operates with all concerned shards locked.
func (wr *Wrangler) migrateServedTypes(ctx context.Context, keyspace string, sourceShards, destinationShards []*topo.ShardInfo, cells []string, servedType topodatapb.TabletType, reverse bool, filteredReplicationWaitTime time.Duration) (err error) {

	// re-read all the shards so we are up to date
	wr.Logger().Infof("Re-reading all shards")
	for i, si := range sourceShards {
		if sourceShards[i], err = wr.ts.GetShard(ctx, si.Keyspace(), si.ShardName()); err != nil {
			return err
		}
	}
	for i, si := range destinationShards {
		if destinationShards[i], err = wr.ts.GetShard(ctx, si.Keyspace(), si.ShardName()); err != nil {
			return err
		}
	}

	ev := &events.MigrateServedTypes{
		KeyspaceName:      keyspace,
		SourceShards:      sourceShards,
		DestinationShards: destinationShards,
		ServedType:        servedType,
		Reverse:           reverse,
	}
	event.DispatchUpdate(ev, "start")
	defer func() {
		if err != nil {
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()

	// For master type migration, need to:
	// - switch the source shards to read-only by disabling query service
	// - gather all replication points
	// - wait for filtered replication to catch up before we continue
	// - disable filtered replication after the fact
	if servedType == topodatapb.TabletType_MASTER {
		event.DispatchUpdate(ev, "disabling query service on all source masters")
		for _, si := range sourceShards {
			if err := si.UpdateDisableQueryService(topodatapb.TabletType_MASTER, nil, true); err != nil {
				return err
			}
			if err := wr.ts.UpdateShard(ctx, si); err != nil {
				return err
			}
		}
		if err := wr.refreshMasters(ctx, sourceShards); err != nil {
			return err
		}

		event.DispatchUpdate(ev, "getting positions of source masters")
		masterPositions, err := wr.getMastersPosition(ctx, sourceShards)
		if err != nil {
			return err
		}

		event.DispatchUpdate(ev, "waiting for destination masters to catch up")
		if err := wr.waitForFilteredReplication(ctx, masterPositions, destinationShards, filteredReplicationWaitTime); err != nil {
			return err
		}

		for _, si := range destinationShards {
			si.SourceShards = nil
		}
	}

	// Check and update all shard records, in memory only.
	// We remember if we need to refresh the state of the source tablets
	// so their query service is enabled again, for reverse migration.
	needToRefreshSourceTablets := false
	for _, si := range sourceShards {
		if err := si.UpdateServedTypesMap(servedType, cells, !reverse); err != nil {
			return err
		}
		if tc := si.GetTabletControl(servedType); reverse && tc != nil && tc.DisableQueryService {
			// this is a backward migration, where the
			// source tablets were disabled previously, so
			// we need to refresh them
			if err := si.UpdateDisableQueryService(servedType, cells, false); err != nil {
				return err
			}
			needToRefreshSourceTablets = true
		}
		if !reverse && servedType != topodatapb.TabletType_MASTER {
			// this is a forward migration, we need to disable
			// query service on the source shards.
			// (this was already done for masters earlier)
			if err := si.UpdateDisableQueryService(servedType, cells, true); err != nil {
				return err
			}
		}
	}
	// We remember if we need to refresh the state of the destination tablets
	// so their query service will be enabled.
	needToRefreshDestinationTablets := false
	for _, si := range destinationShards {
		if err := si.UpdateServedTypesMap(servedType, cells, reverse); err != nil {
			return err
		}
		if tc := si.GetTabletControl(servedType); !reverse && tc != nil && tc.DisableQueryService {
			// This is a forwards migration, and the destination query service was already in a disabled state.
			// We need to enable and force a refresh, otherwise it's possible that both the source and destination
			// will have query service disabled at the same time, and queries would have nowhere to go.
			if err := si.UpdateDisableQueryService(servedType, cells, false); err != nil {
				return err
			}
			needToRefreshDestinationTablets = true
		}
		if reverse && servedType != topodatapb.TabletType_MASTER {
			// this is a backwards migration, we need to disable
			// query service on the destination shards.
			// (we're not allowed to reverse a master migration)
			if err := si.UpdateDisableQueryService(servedType, cells, true); err != nil {
				return err
			}
		}
	}

	// All is good, we can save the shards now
	event.DispatchUpdate(ev, "updating source shards")
	for _, si := range sourceShards {
		if err := wr.ts.UpdateShard(ctx, si); err != nil {
			return err
		}
	}
	if needToRefreshSourceTablets {
		event.DispatchUpdate(ev, "refreshing source shard tablets so they restart their query service")
		for _, si := range sourceShards {
			wr.RefreshTablesByShard(ctx, si, servedType, cells)
		}
	}
	event.DispatchUpdate(ev, "updating destination shards")
	for _, si := range destinationShards {
		if err := wr.ts.UpdateShard(ctx, si); err != nil {
			return err
		}
	}
	if needToRefreshDestinationTablets {
		event.DispatchUpdate(ev, "refreshing destination shard tablets so they restart their query service")
		for _, si := range destinationShards {
			wr.RefreshTablesByShard(ctx, si, servedType, cells)
		}
	}

	// And tell the new shards masters they can now be read-write.
	// Invoking a remote action will also make the tablet stop filtered
	// replication.
	if servedType == topodatapb.TabletType_MASTER {
		event.DispatchUpdate(ev, "setting destination masters read-write")
		if err := wr.refreshMasters(ctx, destinationShards); err != nil {
			return err
		}
	}

	event.DispatchUpdate(ev, "finished")
	return nil
}

// WaitForDrain blocks until the selected tablets (cells/keyspace/shard/tablet_type)
// have reported a QPS rate of 0.0.
// NOTE: This is just an observation of one point in time and no guarantee that
// the tablet was actually drained. At later times, a QPS rate > 0.0 could still
// be observed.
func (wr *Wrangler) WaitForDrain(ctx context.Context, cells []string, keyspace, shard string, servedType topodatapb.TabletType,
	retryDelay, healthCheckTopologyRefresh, healthcheckRetryDelay, healthCheckTimeout time.Duration) error {
	if len(cells) == 0 {
		// Retrieve list of cells for the shard from the topology.
		shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return fmt.Errorf("failed to retrieve list of all cells. GetShard() failed: %v", err)
		}
		cells = shardInfo.Cells
	}

	// Check all cells in parallel.
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			rec.RecordError(wr.waitForDrainInCell(ctx, cell, keyspace, shard, servedType,
				retryDelay, healthCheckTopologyRefresh, healthcheckRetryDelay, healthCheckTimeout))
		}(cell)
	}
	wg.Wait()

	return rec.Error()
}

func (wr *Wrangler) waitForDrainInCell(ctx context.Context, cell, keyspace, shard string, servedType topodatapb.TabletType,
	retryDelay, healthCheckTopologyRefresh, healthcheckRetryDelay, healthCheckTimeout time.Duration) error {
	hc := discovery.NewHealthCheck(healthCheckTimeout /* connectTimeout */, healthcheckRetryDelay, healthCheckTimeout, cell)
	defer hc.Close()
	watcher := discovery.NewShardReplicationWatcher(wr.TopoServer(), hc, cell, keyspace, shard, healthCheckTopologyRefresh, 5 /* topoReadConcurrency */)
	defer watcher.Stop()

	if err := discovery.WaitForEndPoints(ctx, hc, cell, keyspace, shard, []topodatapb.TabletType{servedType}); err != nil {
		return fmt.Errorf("%v: error waiting for initial %v endpoints for %v/%v: %v", cell, servedType, keyspace, shard, err)
	}

	wr.Logger().Infof("%v: Waiting for %.1f seconds to make sure that the discovery module retrieves healthcheck information from all tablets.",
		cell, healthCheckTimeout.Seconds())
	// Wait at least for -vtctl_healthcheck_timeout to elapse to make sure that we
	// see all healthy tablets. Otherwise, we might miss some tablets.
	// It's safe to wait not longer for this because we would only miss slow
	// tablets and vtgate would not serve from such tablets anyway.
	time.Sleep(healthCheckTimeout)

	// Now check the QPS rate of all tablets until the timeout expires.
	startTime := time.Now()
	for {
		healthyTabletsCount := 0
		// map key: tablet uid
		drainedHealthyTablets := make(map[uint32]*discovery.EndPointStats)
		notDrainedHealtyTablets := make(map[uint32]*discovery.EndPointStats)

		addrs := hc.GetEndPointStatsFromTarget(keyspace, shard, servedType)
		healthyTabletsCount = 0
		for _, addr := range addrs {
			// TODO(mberlin): Move this health check logic into a common function
			// because other code uses it as well e.g. go/vt/worker/topo_utils.go.
			if addr.Stats == nil || addr.Stats.HealthError != "" || addr.Stats.SecondsBehindMaster > 30 {
				// not healthy
				continue
			}

			healthyTabletsCount++
			if addr.Stats.Qps == 0.0 {
				drainedHealthyTablets[addr.EndPoint.Uid] = addr
			} else {
				notDrainedHealtyTablets[addr.EndPoint.Uid] = addr
			}
		}

		if len(drainedHealthyTablets) == healthyTabletsCount {
			wr.Logger().Infof("%v: All %d healthy tablets were drained after %.1f seconds (not counting %.1f seconds for the initial wait).",
				cell, healthyTabletsCount, time.Now().Sub(startTime).Seconds(), healthCheckTimeout.Seconds())
			break
		}

		// Continue waiting, sleep in between.
		deadlineString := ""
		if d, ok := ctx.Deadline(); ok {
			deadlineString = fmt.Sprintf(" up to %.1f more seconds", d.Sub(time.Now()).Seconds())
		}
		wr.Logger().Infof("%v: Waiting%v for all healthy tablets to be drained (%d/%d done).",
			cell, deadlineString, len(drainedHealthyTablets), healthyTabletsCount)

		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()

			var l []string
			for _, eps := range notDrainedHealtyTablets {
				l = append(l, formatEndpointStats(eps))
			}
			return fmt.Errorf("%v: WaitForDrain failed for %v tablets in %v/%v. Only %d/%d tablets were drained. err: %v List of tablets which were not drained: %v",
				cell, servedType, keyspace, shard, len(drainedHealthyTablets), healthyTabletsCount, ctx.Err(), strings.Join(l, ";"))
		case <-timer.C:
		}
	}

	return nil
}

func formatEndpointStats(eps *discovery.EndPointStats) string {
	webURL := "unknown http port"
	if webPort, ok := eps.EndPoint.PortMap["vt"]; ok {
		webURL = fmt.Sprintf("http://%v:%d/", eps.EndPoint.Host, webPort)
	}
	alias := &topodatapb.TabletAlias{
		Cell: eps.Cell,
		Uid:  eps.EndPoint.Uid,
	}
	return fmt.Sprintf("%v: %v stats: %v", topoproto.TabletAliasString(alias), webURL, eps.Stats)
}

// MigrateServedFrom is used during vertical splits to migrate a
// served type from a keyspace to another.
func (wr *Wrangler) MigrateServedFrom(ctx context.Context, keyspace, shard string, servedType topodatapb.TabletType, cells []string, reverse bool, filteredReplicationWaitTime time.Duration) error {
	// read the destination keyspace, check it
	ki, err := wr.ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}
	if len(ki.ServedFroms) == 0 {
		return fmt.Errorf("Destination keyspace %v is not a vertical split target", keyspace)
	}

	// read the destination shard, check it
	si, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	if len(si.SourceShards) != 1 || len(si.SourceShards[0].Tables) == 0 {
		return fmt.Errorf("Destination shard %v/%v is not a vertical split target", keyspace, shard)
	}

	// check the migration is valid before locking (will also be checked
	// after locking to be sure)
	if err := ki.CheckServedFromMigration(servedType, cells, si.SourceShards[0].Keyspace, !reverse); err != nil {
		return err
	}

	// lock the keyspace and shards
	actionNode := actionnode.MigrateServedFrom(servedType)
	keyspaceLockPath, err := wr.lockKeyspace(ctx, keyspace, actionNode)
	if err != nil {
		wr.Logger().Errorf("Failed to lock destination keyspace %v", keyspace)
		return err
	}
	destinationShardLockPath, err := wr.lockShard(ctx, keyspace, shard, actionNode)
	if err != nil {
		wr.Logger().Errorf("Failed to lock destination shard %v/%v", keyspace, shard)
		wr.unlockKeyspace(ctx, keyspace, actionNode, keyspaceLockPath, nil)
		return err
	}
	sourceKeyspace := si.SourceShards[0].Keyspace
	sourceShard := si.SourceShards[0].Shard
	sourceShardLockPath, err := wr.lockShard(ctx, sourceKeyspace, sourceShard, actionNode)
	if err != nil {
		wr.Logger().Errorf("Failed to lock source shard %v/%v", sourceKeyspace, sourceShard)
		wr.unlockShard(ctx, keyspace, shard, actionNode, destinationShardLockPath, nil)
		wr.unlockKeyspace(ctx, keyspace, actionNode, keyspaceLockPath, nil)
		return err
	}

	// record the action error and all unlock errors
	rec := concurrency.AllErrorRecorder{}

	// execute the migration
	rec.RecordError(wr.migrateServedFrom(ctx, ki, si, servedType, cells, reverse, filteredReplicationWaitTime))

	rec.RecordError(wr.unlockShard(ctx, sourceKeyspace, sourceShard, actionNode, sourceShardLockPath, nil))
	rec.RecordError(wr.unlockShard(ctx, keyspace, shard, actionNode, destinationShardLockPath, nil))
	rec.RecordError(wr.unlockKeyspace(ctx, keyspace, actionNode, keyspaceLockPath, nil))

	// rebuild the keyspace serving graph if there was no error
	if rec.Error() == nil {
		rec.RecordError(wr.RebuildKeyspaceGraph(ctx, keyspace, cells, false))
	}

	return rec.Error()
}

func (wr *Wrangler) migrateServedFrom(ctx context.Context, ki *topo.KeyspaceInfo, destinationShard *topo.ShardInfo, servedType topodatapb.TabletType, cells []string, reverse bool, filteredReplicationWaitTime time.Duration) (err error) {

	// re-read and update keyspace info record
	ki, err = wr.ts.GetKeyspace(ctx, ki.KeyspaceName())
	if err != nil {
		return err
	}
	if reverse {
		ki.UpdateServedFromMap(servedType, cells, destinationShard.SourceShards[0].Keyspace, false, nil)
	} else {
		ki.UpdateServedFromMap(servedType, cells, destinationShard.SourceShards[0].Keyspace, true, destinationShard.Cells)
	}

	// re-read and check the destination shard
	destinationShard, err = wr.ts.GetShard(ctx, destinationShard.Keyspace(), destinationShard.ShardName())
	if err != nil {
		return err
	}
	if len(destinationShard.SourceShards) != 1 {
		return fmt.Errorf("Destination shard %v/%v is not a vertical split target", destinationShard.Keyspace(), destinationShard.ShardName())
	}
	tables := destinationShard.SourceShards[0].Tables

	// read the source shard, we'll need its master, and we'll need to
	// update the blacklisted tables.
	var sourceShard *topo.ShardInfo
	sourceShard, err = wr.ts.GetShard(ctx, destinationShard.SourceShards[0].Keyspace, destinationShard.SourceShards[0].Shard)
	if err != nil {
		return err
	}

	ev := &events.MigrateServedFrom{
		KeyspaceName:     ki.KeyspaceName(),
		SourceShard:      *sourceShard,
		DestinationShard: *destinationShard,
		ServedType:       servedType,
		Reverse:          reverse,
	}
	event.DispatchUpdate(ev, "start")
	defer func() {
		if err != nil {
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()

	if servedType == topodatapb.TabletType_MASTER {
		err = wr.masterMigrateServedFrom(ctx, ki, sourceShard, destinationShard, tables, ev, filteredReplicationWaitTime)
	} else {
		err = wr.replicaMigrateServedFrom(ctx, ki, sourceShard, destinationShard, servedType, cells, reverse, tables, ev)
	}
	event.DispatchUpdate(ev, "finished")
	return
}

// replicaMigrateServedFrom handles the slave (replica, rdonly) migration.
func (wr *Wrangler) replicaMigrateServedFrom(ctx context.Context, ki *topo.KeyspaceInfo, sourceShard *topo.ShardInfo, destinationShard *topo.ShardInfo, servedType topodatapb.TabletType, cells []string, reverse bool, tables []string, ev *events.MigrateServedFrom) error {
	// Save the destination keyspace (its ServedFrom has been changed)
	event.DispatchUpdate(ev, "updating keyspace")
	if err := wr.ts.UpdateKeyspace(ctx, ki); err != nil {
		return err
	}

	// Save the source shard (its blacklisted tables field has changed)
	event.DispatchUpdate(ev, "updating source shard")
	if err := sourceShard.UpdateSourceBlacklistedTables(servedType, cells, reverse, tables); err != nil {
		return fmt.Errorf("UpdateSourceBlacklistedTables(%v/%v) failed: %v", sourceShard.Keyspace(), sourceShard.ShardName(), err)
	}
	if err := wr.ts.UpdateShard(ctx, sourceShard); err != nil {
		return fmt.Errorf("UpdateShard(%v/%v) failed: %v", sourceShard.Keyspace(), sourceShard.ShardName(), err)
	}

	// Now refresh the source servers so they reload their
	// blacklisted table list
	event.DispatchUpdate(ev, "refreshing sources tablets state so they update their blacklisted tables")
	if err := wr.RefreshTablesByShard(ctx, sourceShard, servedType, cells); err != nil {
		return err
	}

	return nil
}

// masterMigrateServedFrom handles the master migration. The ordering is
// a bit different than for rdonly / replica to guarantee a smooth transition.
//
// The order is as follows:
// - Add BlacklistedTables on the source shard map for master
// - Refresh the source master, so it stops writing on the tables
// - Get the source master position, wait until destination master reaches it
// - Clear SourceShard on the destination Shard
// - Refresh the destination master, so its stops its filtered
//   replication and starts accepting writes
func (wr *Wrangler) masterMigrateServedFrom(ctx context.Context, ki *topo.KeyspaceInfo, sourceShard *topo.ShardInfo, destinationShard *topo.ShardInfo, tables []string, ev *events.MigrateServedFrom, filteredReplicationWaitTime time.Duration) error {
	// Read the data we need
	sourceMasterTabletInfo, err := wr.ts.GetTablet(ctx, sourceShard.MasterAlias)
	if err != nil {
		return err
	}
	destinationMasterTabletInfo, err := wr.ts.GetTablet(ctx, destinationShard.MasterAlias)
	if err != nil {
		return err
	}

	// Update source shard (more blacklisted tables)
	event.DispatchUpdate(ev, "updating source shard")
	if err := sourceShard.UpdateSourceBlacklistedTables(topodatapb.TabletType_MASTER, nil, false, tables); err != nil {
		return fmt.Errorf("UpdateSourceBlacklistedTables(%v/%v) failed: %v", sourceShard.Keyspace(), sourceShard.ShardName(), err)
	}
	if err := wr.ts.UpdateShard(ctx, sourceShard); err != nil {
		return fmt.Errorf("UpdateShard(%v/%v) failed: %v", sourceShard.Keyspace(), sourceShard.ShardName(), err)
	}

	// Now refresh the blacklisted table list on the source master
	event.DispatchUpdate(ev, "refreshing source master so it updates its blacklisted tables")
	if err := wr.tmc.RefreshState(ctx, sourceMasterTabletInfo); err != nil {
		return err
	}

	// get the position
	event.DispatchUpdate(ev, "getting master position")
	masterPosition, err := wr.tmc.MasterPosition(ctx, sourceMasterTabletInfo)
	if err != nil {
		return err
	}

	// wait for it
	event.DispatchUpdate(ev, "waiting for destination master to catch up to source master")
	if err := wr.tmc.WaitBlpPosition(ctx, destinationMasterTabletInfo, &tabletmanagerdatapb.BlpPosition{
		Uid:      0,
		Position: masterPosition,
	}, filteredReplicationWaitTime); err != nil {
		return err
	}

	// Update the destination keyspace (its ServedFrom has changed)
	event.DispatchUpdate(ev, "updating keyspace")
	if err = wr.ts.UpdateKeyspace(ctx, ki); err != nil {
		return err
	}

	// Update the destination shard (no more source shard)
	event.DispatchUpdate(ev, "updating destination shard")
	destinationShard.SourceShards = nil
	if err := wr.ts.UpdateShard(ctx, destinationShard); err != nil {
		return err
	}

	// Tell the new shards masters they can now be read-write.
	// Invoking a remote action will also make the tablet stop filtered
	// replication.
	event.DispatchUpdate(ev, "setting destination shard masters read-write")
	if err := wr.refreshMasters(ctx, []*topo.ShardInfo{destinationShard}); err != nil {
		return err
	}

	return nil
}

// SetKeyspaceServedFrom locks a keyspace and changes its ServerFromMap
func (wr *Wrangler) SetKeyspaceServedFrom(ctx context.Context, keyspace string, servedType topodatapb.TabletType, cells []string, sourceKeyspace string, remove bool) error {
	actionNode := actionnode.SetKeyspaceServedFrom()
	lockPath, err := wr.lockKeyspace(ctx, keyspace, actionNode)
	if err != nil {
		return err
	}

	err = wr.setKeyspaceServedFrom(ctx, keyspace, servedType, cells, sourceKeyspace, remove)
	return wr.unlockKeyspace(ctx, keyspace, actionNode, lockPath, err)
}

func (wr *Wrangler) setKeyspaceServedFrom(ctx context.Context, keyspace string, servedType topodatapb.TabletType, cells []string, sourceKeyspace string, remove bool) error {
	ki, err := wr.ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}
	if err := ki.UpdateServedFromMap(servedType, cells, sourceKeyspace, remove, nil); err != nil {
		return err
	}
	return wr.ts.UpdateKeyspace(ctx, ki)
}

// RefreshTablesByShard calls RefreshState on all the tables of a
// given type in a shard. It would work for the master, but the
// discovery wouldn't be very efficient.
func (wr *Wrangler) RefreshTablesByShard(ctx context.Context, si *topo.ShardInfo, tabletType topodatapb.TabletType, cells []string) error {
	wr.Logger().Infof("RefreshTablesByShard called on shard %v/%v", si.Keyspace(), si.ShardName())
	tabletMap, err := wr.ts.GetTabletMapForShardByCell(ctx, si.Keyspace(), si.ShardName(), cells)
	switch err {
	case nil:
		// keep going
	case topo.ErrPartialResult:
		wr.Logger().Warningf("RefreshTablesByShard: got partial result for shard %v/%v, may not refresh all tablets everywhere", si.Keyspace(), si.ShardName())
	default:
		return err
	}

	// ignore errors in this phase
	wg := sync.WaitGroup{}
	for _, ti := range tabletMap {
		if ti.Type != tabletType {
			continue
		}

		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			wr.Logger().Infof("Calling RefreshState on tablet %v", ti.AliasString())
			// Setting an upper bound timeout to fail faster in case of an error.
			// Using 60 seconds because RefreshState should not take more than 30 seconds.
			// (RefreshState will restart the tablet's QueryService and most time will be spent on the shutdown, i.e. waiting up to 30 seconds on transactions (see Config.TransactionTimeout)).
			ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			if err := wr.tmc.RefreshState(ctx, ti); err != nil {
				wr.Logger().Warningf("RefreshTablesByShard: failed to refresh %v: %v", ti.AliasString(), err)
			}
			cancel()
			wg.Done()
		}(ti)
	}
	wg.Wait()

	return nil
}

// DeleteKeyspace will do all the necessary changes in the topology server
// to entirely remove a keyspace.
func (wr *Wrangler) DeleteKeyspace(ctx context.Context, keyspace string, recursive bool) error {
	shards, err := wr.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return err
	}
	if recursive {
		wr.Logger().Infof("Deleting all shards (and their tablets) in keyspace %v", keyspace)
		for _, shard := range shards {
			wr.Logger().Infof("Recursively deleting shard %v/%v", keyspace, shard)
			if err := wr.DeleteShard(ctx, keyspace, shard, recursive); err != nil && err != topo.ErrNoNode {
				// Unlike the errors below in non-recursive steps, we don't want to
				// continue if a DeleteShard fails. If we continue and delete the
				// keyspace, the tablet records will be orphaned, since we'll
				// no longer know how to list out the shard they belong to.
				//
				// If the problem is temporary, or resolved externally, re-running
				// DeleteKeyspace will skip over shards that were already deleted.
				return fmt.Errorf("can't delete shard %v/%v: %v", keyspace, shard, err)
			}
		}
	} else if len(shards) > 0 {
		return fmt.Errorf("keyspace %v still has %v shards; use -recursive or remove them manually", keyspace, len(shards))
	}

	// Delete the cell-local keyspace entries.
	cells, err := wr.ts.GetKnownCells(ctx)
	if err != nil {
		return err
	}
	for _, cell := range cells {
		if err := wr.ts.DeleteKeyspaceReplication(ctx, cell, keyspace); err != nil && err != topo.ErrNoNode {
			wr.Logger().Warningf("Cannot delete KeyspaceReplication in cell %v for %v: %v", cell, keyspace, err)
		}

		if err := wr.ts.DeleteSrvKeyspace(ctx, cell, keyspace); err != nil && err != topo.ErrNoNode {
			wr.Logger().Warningf("Cannot delete SrvKeyspace in cell %v for %v: %v", cell, keyspace, err)
		}
	}

	return wr.ts.DeleteKeyspace(ctx, keyspace)
}

// RemoveKeyspaceCell will remove a cell from the Cells list in all shards of a keyspace.
//
// It is essentially a shortcut for calling RemoveShardCell on every shard,
// reducing the potential for operator error when there are many shards.
func (wr *Wrangler) RemoveKeyspaceCell(ctx context.Context, keyspace, cell string, force, recursive bool) error {
	shards, err := wr.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return err
	}
	for _, shard := range shards {
		wr.Logger().Infof("Removing cell %v from shard %v/%v", cell, keyspace, shard)
		if err := wr.RemoveShardCell(ctx, keyspace, shard, cell, force, recursive); err != nil {
			return fmt.Errorf("can't remove cell %v from shard %v/%v: %v", cell, keyspace, shard, err)
		}
	}
	return nil
}
