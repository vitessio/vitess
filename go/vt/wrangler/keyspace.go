// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	"code.google.com/p/go.net/context"
	"github.com/youtube/vitess/go/event"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/topotools/events"
)

// keyspace related methods for Wrangler

func (wr *Wrangler) lockKeyspace(keyspace string, actionNode *actionnode.ActionNode) (lockPath string, err error) {
	return actionNode.LockKeyspace(context.TODO(), wr.ts, keyspace, wr.lockTimeout, interrupted)
}

func (wr *Wrangler) unlockKeyspace(keyspace string, actionNode *actionnode.ActionNode, lockPath string, actionError error) error {
	return actionNode.UnlockKeyspace(wr.ts, keyspace, lockPath, actionError)
}

// SetKeyspaceShardingInfo locks a keyspace and sets its ShardingColumnName
// and ShardingColumnType
func (wr *Wrangler) SetKeyspaceShardingInfo(keyspace, shardingColumnName string, shardingColumnType key.KeyspaceIdType, splitShardCount int32, force bool) error {
	actionNode := actionnode.SetKeyspaceShardingInfo()
	lockPath, err := wr.lockKeyspace(keyspace, actionNode)
	if err != nil {
		return err
	}

	err = wr.setKeyspaceShardingInfo(keyspace, shardingColumnName, shardingColumnType, splitShardCount, force)
	return wr.unlockKeyspace(keyspace, actionNode, lockPath, err)

}

func (wr *Wrangler) setKeyspaceShardingInfo(keyspace, shardingColumnName string, shardingColumnType key.KeyspaceIdType, splitShardCount int32, force bool) error {
	ki, err := wr.ts.GetKeyspace(keyspace)
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

	if ki.ShardingColumnType != key.KIT_UNSET && ki.ShardingColumnType != shardingColumnType {
		if force {
			wr.Logger().Warningf("Forcing keyspace ShardingColumnType change from %v to %v", ki.ShardingColumnType, shardingColumnType)
		} else {
			return fmt.Errorf("Cannot change ShardingColumnType from %v to %v (use -force to override)", ki.ShardingColumnType, shardingColumnType)
		}
	}

	ki.ShardingColumnName = shardingColumnName
	ki.ShardingColumnType = shardingColumnType
	ki.SplitShardCount = splitShardCount
	return topo.UpdateKeyspace(wr.ts, ki)
}

// MigrateServedTypes is used during horizontal splits to migrate a
// served type from a list of shards to another.
func (wr *Wrangler) MigrateServedTypes(keyspace, shard string, cells []string, servedType topo.TabletType, reverse, skipReFreshState bool) error {
	if servedType == topo.TYPE_MASTER {
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
	osList, err := topotools.FindOverlappingShards(wr.ts, keyspace)
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
		sourceLockPath[i], err = wr.lockShard(si.Keyspace(), si.ShardName(), actionNode)
		if err != nil {
			wr.Logger().Errorf("Failed to lock source shard %v/%v, may need to unlock other shards manually", si.Keyspace(), si.ShardName())
			return err
		}
	}
	destinationLockPath := make([]string, len(destinationShards))
	for i, si := range destinationShards {
		destinationLockPath[i], err = wr.lockShard(si.Keyspace(), si.ShardName(), actionNode)
		if err != nil {
			wr.Logger().Errorf("Failed to lock destination shard %v/%v, may need to unlock other shards manually", si.Keyspace(), si.ShardName())
			return err
		}
	}

	// record the action error and all unlock errors
	rec := concurrency.AllErrorRecorder{}

	// execute the migration
	rec.RecordError(wr.migrateServedTypes(keyspace, sourceShards, destinationShards, cells, servedType, reverse))

	// unlock the shards, we're done
	for i := len(destinationShards) - 1; i >= 0; i-- {
		rec.RecordError(wr.unlockShard(destinationShards[i].Keyspace(), destinationShards[i].ShardName(), actionNode, destinationLockPath[i], nil))
	}
	for i := len(sourceShards) - 1; i >= 0; i-- {
		rec.RecordError(wr.unlockShard(sourceShards[i].Keyspace(), sourceShards[i].ShardName(), actionNode, sourceLockPath[i], nil))
	}

	// rebuild the keyspace serving graph if there was no error
	if !rec.HasErrors() {
		rec.RecordError(wr.RebuildKeyspaceGraph(keyspace, nil))
	}

	// Send a refresh to the source tablets we just disabled, iff:
	// - we're not migrating a master
	// - it is not a reverse migration
	// - we dont' have any errors
	// - we're not told to skip the refresh
	if servedType != topo.TYPE_MASTER && !reverse && !rec.HasErrors() && !skipReFreshState {
		for _, si := range sourceShards {
			rec.RecordError(wr.RefreshTablesByShard(si, servedType, cells))
		}
	}

	return rec.Error()
}

func removeType(tabletType topo.TabletType, types []topo.TabletType) ([]topo.TabletType, bool) {
	result := make([]topo.TabletType, 0, len(types)-1)
	found := false
	for _, t := range types {
		if t == tabletType {
			found = true
		} else {
			result = append(result, t)
		}
	}
	return result, found
}

func (wr *Wrangler) getMastersPosition(shards []*topo.ShardInfo) (map[*topo.ShardInfo]myproto.ReplicationPosition, error) {
	mu := sync.Mutex{}
	result := make(map[*topo.ShardInfo]myproto.ReplicationPosition)

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range shards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
			wr.Logger().Infof("Gathering master position for %v", si.MasterAlias)
			ti, err := wr.ts.GetTablet(si.MasterAlias)
			if err != nil {
				rec.RecordError(err)
				return
			}

			pos, err := wr.tmc.MasterPosition(context.TODO(), ti, wr.ActionTimeout())
			if err != nil {
				rec.RecordError(err)
				return
			}

			wr.Logger().Infof("Got master position for %v", si.MasterAlias)
			mu.Lock()
			result[si] = pos
			mu.Unlock()
		}(si)
	}
	wg.Wait()
	return result, rec.Error()
}

func (wr *Wrangler) waitForFilteredReplication(sourcePositions map[*topo.ShardInfo]myproto.ReplicationPosition, destinationShards []*topo.ShardInfo) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range destinationShards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
			for _, sourceShard := range si.SourceShards {
				// we're waiting on this guy
				blpPosition := blproto.BlpPosition{
					Uid: sourceShard.Uid,
				}

				// find the position it should be at
				for s, pos := range sourcePositions {
					if s.Keyspace() == sourceShard.Keyspace && s.ShardName() == sourceShard.Shard {
						blpPosition.Position = pos
					}
				}

				// and wait for it
				wr.Logger().Infof("Waiting for %v to catch up", si.MasterAlias)
				tablet, err := wr.ts.GetTablet(si.MasterAlias)
				if err != nil {
					rec.RecordError(err)
					return
				}

				if err := wr.tmc.WaitBlpPosition(context.TODO(), tablet, blpPosition, wr.ActionTimeout()); err != nil {
					rec.RecordError(err)
				} else {
					wr.Logger().Infof("%v caught up", si.MasterAlias)
				}
			}
		}(si)
	}
	wg.Wait()
	return rec.Error()
}

// refreshMasters will just RPC-ping all the masters with RefreshState
func (wr *Wrangler) refreshMasters(shards []*topo.ShardInfo) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range shards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
			wr.Logger().Infof("RefreshState master %v", si.MasterAlias)
			ti, err := wr.ts.GetTablet(si.MasterAlias)
			if err != nil {
				rec.RecordError(err)
				return
			}

			if err := wr.tmc.RefreshState(context.TODO(), ti, wr.ActionTimeout()); err != nil {
				rec.RecordError(err)
			} else {
				wr.Logger().Infof("%v responded", si.MasterAlias)
			}
		}(si)
	}
	wg.Wait()
	return rec.Error()
}

// migrateServedTypes operates with all concerned shards locked.
func (wr *Wrangler) migrateServedTypes(keyspace string, sourceShards, destinationShards []*topo.ShardInfo, cells []string, servedType topo.TabletType, reverse bool) (err error) {

	// re-read all the shards so we are up to date
	wr.Logger().Infof("Re-reading all shards")
	for i, si := range sourceShards {
		if sourceShards[i], err = wr.ts.GetShard(si.Keyspace(), si.ShardName()); err != nil {
			return err
		}
	}
	for i, si := range destinationShards {
		if destinationShards[i], err = wr.ts.GetShard(si.Keyspace(), si.ShardName()); err != nil {
			return err
		}
	}

	ev := &events.MigrateServedTypes{
		Keyspace:          *topo.NewKeyspaceInfo(keyspace, nil, -1),
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
	if servedType == topo.TYPE_MASTER {
		event.DispatchUpdate(ev, "disabling query service on all source masters")
		for _, si := range sourceShards {
			if err := si.UpdateDisableQueryService(topo.TYPE_MASTER, nil, true); err != nil {
				return err
			}
			if err := topo.UpdateShard(context.TODO(), wr.ts, si); err != nil {
				return err
			}
		}
		if err := wr.refreshMasters(sourceShards); err != nil {
			return err
		}

		event.DispatchUpdate(ev, "getting positions of source masters")
		masterPositions, err := wr.getMastersPosition(sourceShards)
		if err != nil {
			return err
		}

		event.DispatchUpdate(ev, "waiting for destination masters to catch up")
		if err := wr.waitForFilteredReplication(masterPositions, destinationShards); err != nil {
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
		if tc, ok := si.TabletControlMap[servedType]; reverse && ok && tc.DisableQueryService {
			// this is a backward migration, where the
			// source tablets were disabled previously, so
			// we need to refresh them
			if err := si.UpdateDisableQueryService(servedType, cells, false); err != nil {
				return err
			}
			needToRefreshSourceTablets = true
		}
		if !reverse && servedType != topo.TYPE_MASTER {
			// this is a forward migration, we need to disable
			// query service on the source shards.
			// (this was already done for masters earlier)
			if err := si.UpdateDisableQueryService(servedType, cells, true); err != nil {
				return err
			}
		}
	}
	for _, si := range destinationShards {
		if err := si.UpdateServedTypesMap(servedType, cells, reverse); err != nil {
			return err
		}
	}

	// All is good, we can save the shards now
	event.DispatchUpdate(ev, "updating source shards")
	for _, si := range sourceShards {
		if err := topo.UpdateShard(context.TODO(), wr.ts, si); err != nil {
			return err
		}
	}
	if needToRefreshSourceTablets {
		event.DispatchUpdate(ev, "refreshing source shard tablets so they restart their query service")
		for _, si := range sourceShards {
			wr.RefreshTablesByShard(si, servedType, cells)
		}
	}
	event.DispatchUpdate(ev, "updating destination shards")
	for _, si := range destinationShards {
		if err := topo.UpdateShard(context.TODO(), wr.ts, si); err != nil {
			return err
		}
	}

	// And tell the new shards masters they can now be read-write.
	// Invoking a remote action will also make the tablet stop filtered
	// replication.
	if servedType == topo.TYPE_MASTER {
		event.DispatchUpdate(ev, "setting destination masters read-write")
		if err := wr.refreshMasters(destinationShards); err != nil {
			return err
		}
	}

	event.DispatchUpdate(ev, "finished")
	return nil
}

// MigrateServedFrom is used during vertical splits to migrate a
// served type from a keyspace to another.
func (wr *Wrangler) MigrateServedFrom(keyspace, shard string, servedType topo.TabletType, cells []string, reverse bool) error {
	// read the destination keyspace, check it
	ki, err := wr.ts.GetKeyspace(keyspace)
	if err != nil {
		return err
	}
	if len(ki.ServedFromMap) == 0 {
		return fmt.Errorf("Destination keyspace %v is not a vertical split target", keyspace)
	}

	// read the destination shard, check it
	si, err := wr.ts.GetShard(keyspace, shard)
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
	keyspaceLockPath, err := wr.lockKeyspace(keyspace, actionNode)
	if err != nil {
		wr.Logger().Errorf("Failed to lock destination keyspace %v", keyspace)
		return err
	}
	destinationShardLockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		wr.Logger().Errorf("Failed to lock destination shard %v/%v", keyspace, shard)
		wr.unlockKeyspace(keyspace, actionNode, keyspaceLockPath, nil)
		return err
	}
	sourceKeyspace := si.SourceShards[0].Keyspace
	sourceShard := si.SourceShards[0].Shard
	sourceShardLockPath, err := wr.lockShard(sourceKeyspace, sourceShard, actionNode)
	if err != nil {
		wr.Logger().Errorf("Failed to lock source shard %v/%v", sourceKeyspace, sourceShard)
		wr.unlockShard(keyspace, shard, actionNode, destinationShardLockPath, nil)
		wr.unlockKeyspace(keyspace, actionNode, keyspaceLockPath, nil)
		return err
	}

	// record the action error and all unlock errors
	rec := concurrency.AllErrorRecorder{}

	// execute the migration
	rec.RecordError(wr.migrateServedFrom(ki, si, servedType, cells, reverse))

	rec.RecordError(wr.unlockShard(sourceKeyspace, sourceShard, actionNode, sourceShardLockPath, nil))
	rec.RecordError(wr.unlockShard(keyspace, shard, actionNode, destinationShardLockPath, nil))
	rec.RecordError(wr.unlockKeyspace(keyspace, actionNode, keyspaceLockPath, nil))

	// rebuild the keyspace serving graph if there was no error
	if rec.Error() == nil {
		rec.RecordError(wr.RebuildKeyspaceGraph(keyspace, cells))
	}

	return rec.Error()
}

func (wr *Wrangler) migrateServedFrom(ki *topo.KeyspaceInfo, destinationShard *topo.ShardInfo, servedType topo.TabletType, cells []string, reverse bool) (err error) {

	// re-read and update keyspace info record
	ki, err = wr.ts.GetKeyspace(ki.KeyspaceName())
	if err != nil {
		return err
	}
	if reverse {
		ki.UpdateServedFromMap(servedType, cells, destinationShard.SourceShards[0].Keyspace, false, nil)
	} else {
		ki.UpdateServedFromMap(servedType, cells, destinationShard.SourceShards[0].Keyspace, true, destinationShard.Cells)
	}

	// re-read and check the destination shard
	destinationShard, err = wr.ts.GetShard(destinationShard.Keyspace(), destinationShard.ShardName())
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
	sourceShard, err = wr.ts.GetShard(destinationShard.SourceShards[0].Keyspace, destinationShard.SourceShards[0].Shard)
	if err != nil {
		return err
	}

	ev := &events.MigrateServedFrom{
		Keyspace:         *ki,
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

	if servedType == topo.TYPE_MASTER {
		err = wr.masterMigrateServedFrom(ki, sourceShard, destinationShard, tables, ev)
	} else {
		err = wr.replicaMigrateServedFrom(ki, sourceShard, destinationShard, servedType, cells, reverse, tables, ev)
	}
	event.DispatchUpdate(ev, "finished")
	return
}

// replicaMigrateServedFrom handles the slave (replica, rdonly) migration.
func (wr *Wrangler) replicaMigrateServedFrom(ki *topo.KeyspaceInfo, sourceShard *topo.ShardInfo, destinationShard *topo.ShardInfo, servedType topo.TabletType, cells []string, reverse bool, tables []string, ev *events.MigrateServedFrom) error {
	// Save the destination keyspace (its ServedFrom has been changed)
	event.DispatchUpdate(ev, "updating keyspace")
	if err := topo.UpdateKeyspace(wr.ts, ki); err != nil {
		return err
	}

	// Save the source shard (its blacklisted tables field has changed)
	event.DispatchUpdate(ev, "updating source shard")
	if err := sourceShard.UpdateSourceBlacklistedTables(servedType, cells, reverse, tables); err != nil {
		return fmt.Errorf("UpdateSourceBlacklistedTables(%v/%v) failed: %v", sourceShard.Keyspace(), sourceShard.ShardName(), err)
	}
	if err := topo.UpdateShard(context.TODO(), wr.ts, sourceShard); err != nil {
		return fmt.Errorf("UpdateShard(%v/%v) failed: %v", sourceShard.Keyspace(), sourceShard.ShardName(), err)
	}

	// Now refresh the source servers so they reload their
	// blacklisted table list
	event.DispatchUpdate(ev, "refreshing sources tablets state so they update their blacklisted tables")
	if err := wr.RefreshTablesByShard(sourceShard, servedType, cells); err != nil {
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
func (wr *Wrangler) masterMigrateServedFrom(ki *topo.KeyspaceInfo, sourceShard *topo.ShardInfo, destinationShard *topo.ShardInfo, tables []string, ev *events.MigrateServedFrom) error {
	// Read the data we need
	sourceMasterTabletInfo, err := wr.ts.GetTablet(sourceShard.MasterAlias)
	if err != nil {
		return err
	}
	destinationMasterTabletInfo, err := wr.ts.GetTablet(destinationShard.MasterAlias)
	if err != nil {
		return err
	}

	// Update source shard (more blacklisted tables)
	event.DispatchUpdate(ev, "updating source shard")
	if err := sourceShard.UpdateSourceBlacklistedTables(topo.TYPE_MASTER, nil, false, tables); err != nil {
		return fmt.Errorf("UpdateSourceBlacklistedTables(%v/%v) failed: %v", sourceShard.Keyspace(), sourceShard.ShardName(), err)
	}
	if err := topo.UpdateShard(context.TODO(), wr.ts, sourceShard); err != nil {
		return fmt.Errorf("UpdateShard(%v/%v) failed: %v", sourceShard.Keyspace(), sourceShard.ShardName(), err)
	}

	// Now refresh the blacklisted table list on the source master
	event.DispatchUpdate(ev, "refreshing source master so it updates its blacklisted tables")
	if err := wr.tmc.RefreshState(context.TODO(), sourceMasterTabletInfo, wr.ActionTimeout()); err != nil {
		return err
	}

	// get the position
	event.DispatchUpdate(ev, "getting master position")
	masterPosition, err := wr.tmc.MasterPosition(context.TODO(), sourceMasterTabletInfo, wr.ActionTimeout())
	if err != nil {
		return err
	}

	// wait for it
	event.DispatchUpdate(ev, "waiting for destination master to catch up to source master")
	if err := wr.tmc.WaitBlpPosition(context.TODO(), destinationMasterTabletInfo, blproto.BlpPosition{
		Uid:      0,
		Position: masterPosition,
	}, wr.ActionTimeout()); err != nil {
		return err
	}

	// Update the destination keyspace (its ServedFrom has changed)
	event.DispatchUpdate(ev, "updating keyspace")
	if err = topo.UpdateKeyspace(wr.ts, ki); err != nil {
		return err
	}

	// Update the destination shard (no more source shard)
	event.DispatchUpdate(ev, "updating destination shard")
	destinationShard.SourceShards = nil
	if err := topo.UpdateShard(context.TODO(), wr.ts, destinationShard); err != nil {
		return err
	}

	// Tell the new shards masters they can now be read-write.
	// Invoking a remote action will also make the tablet stop filtered
	// replication.
	event.DispatchUpdate(ev, "setting destination shard masters read-write")
	if err := wr.refreshMasters([]*topo.ShardInfo{destinationShard}); err != nil {
		return err
	}

	return nil
}

// SetKeyspaceServedFrom locks a keyspace and changes its ServerFromMap
func (wr *Wrangler) SetKeyspaceServedFrom(keyspace string, servedType topo.TabletType, cells []string, sourceKeyspace string, remove bool) error {
	actionNode := actionnode.SetKeyspaceServedFrom()
	lockPath, err := wr.lockKeyspace(keyspace, actionNode)
	if err != nil {
		return err
	}

	err = wr.setKeyspaceServedFrom(keyspace, servedType, cells, sourceKeyspace, remove)
	return wr.unlockKeyspace(keyspace, actionNode, lockPath, err)
}

func (wr *Wrangler) setKeyspaceServedFrom(keyspace string, servedType topo.TabletType, cells []string, sourceKeyspace string, remove bool) error {
	ki, err := wr.ts.GetKeyspace(keyspace)
	if err != nil {
		return err
	}
	if err := ki.UpdateServedFromMap(servedType, cells, sourceKeyspace, remove, nil); err != nil {
		return err
	}
	return topo.UpdateKeyspace(wr.ts, ki)
}

// RefreshTablesByShard calls RefreshState on all the tables of a
// given type in a shard. It would work for the master, but the
// discovery wouldn't be very efficient.
func (wr *Wrangler) RefreshTablesByShard(si *topo.ShardInfo, tabletType topo.TabletType, cells []string) error {
	tabletMap, err := topo.GetTabletMapForShardByCell(wr.ts, si.Keyspace(), si.ShardName(), cells)
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
			if err := wr.tmc.RefreshState(context.TODO(), ti, wr.ActionTimeout()); err != nil {
				wr.Logger().Warningf("RefreshTablesByShard: failed to refresh %v: %v", ti.Alias, err)
			}
			wg.Done()
		}(ti)
	}
	wg.Wait()

	return nil
}
