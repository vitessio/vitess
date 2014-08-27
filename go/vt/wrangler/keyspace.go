// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools/events"
)

// keyspace related methods for Wrangler

func (wr *Wrangler) lockKeyspace(keyspace string, actionNode *actionnode.ActionNode) (lockPath string, err error) {
	return actionNode.LockKeyspace(wr.ts, keyspace, wr.lockTimeout, interrupted)
}

func (wr *Wrangler) unlockKeyspace(keyspace string, actionNode *actionnode.ActionNode, lockPath string, actionError error) error {
	return actionNode.UnlockKeyspace(wr.ts, keyspace, lockPath, actionError)
}

func (wr *Wrangler) SetKeyspaceShardingInfo(keyspace, shardingColumnName string, shardingColumnType key.KeyspaceIdType, force bool) error {
	actionNode := actionnode.SetKeyspaceShardingInfo()
	lockPath, err := wr.lockKeyspace(keyspace, actionNode)
	if err != nil {
		return err
	}

	err = wr.setKeyspaceShardingInfo(keyspace, shardingColumnName, shardingColumnType, force)
	return wr.unlockKeyspace(keyspace, actionNode, lockPath, err)

}

func (wr *Wrangler) setKeyspaceShardingInfo(keyspace, shardingColumnName string, shardingColumnType key.KeyspaceIdType, force bool) error {
	ki, err := wr.ts.GetKeyspace(keyspace)
	if err != nil {
		// Temporary change: we try to keep going even if node
		// doesn't exist
		if err != topo.ErrNoNode {
			return err
		}
		ki = topo.NewKeyspaceInfo(keyspace, &topo.Keyspace{})
	}

	if ki.ShardingColumnName != "" && ki.ShardingColumnName != shardingColumnName {
		if force {
			log.Warningf("Forcing keyspace ShardingColumnName change from %v to %v", ki.ShardingColumnName, shardingColumnName)
		} else {
			return fmt.Errorf("Cannot change ShardingColumnName from %v to %v (use -force to override)", ki.ShardingColumnName, shardingColumnName)
		}
	}

	if ki.ShardingColumnType != key.KIT_UNSET && ki.ShardingColumnType != shardingColumnType {
		if force {
			log.Warningf("Forcing keyspace ShardingColumnType change from %v to %v", ki.ShardingColumnType, shardingColumnType)
		} else {
			return fmt.Errorf("Cannot change ShardingColumnType from %v to %v (use -force to override)", ki.ShardingColumnType, shardingColumnType)
		}
	}

	ki.ShardingColumnName = shardingColumnName
	ki.ShardingColumnType = shardingColumnType
	return wr.ts.UpdateKeyspace(ki)
}

func (wr *Wrangler) MigrateServedTypes(keyspace, shard string, servedType topo.TabletType, reverse, skipRebuild bool) error {
	if servedType == topo.TYPE_MASTER {
		// we cannot migrate a master back, since when master migration
		// is done, the source shards are dead
		if reverse {
			return fmt.Errorf("Cannot migrate master back to %v/%v", keyspace, shard)
		}
		// we cannot skip rebuild for a master
		if skipRebuild {
			return fmt.Errorf("Cannot skip rebuild for master migration on %v/%v", keyspace, shard)
		}
	}

	// first figure out the destination shards
	// TODO(alainjobart) for now we only look in the same keyspace.
	// We might want to look elsewhere eventually too, maybe through
	// an extra command line parameter?
	shardNames, err := wr.ts.GetShardNames(keyspace)
	if err != nil {
		return nil
	}
	destinationShards := make([]*topo.ShardInfo, 0, 0)
	for _, shardName := range shardNames {
		si, err := wr.ts.GetShard(keyspace, shardName)
		if err != nil {
			return err
		}

		for _, sourceShard := range si.SourceShards {
			if sourceShard.Keyspace == keyspace && sourceShard.Shard == shard {
				// this shard is replicating from the source shard we specified
				log.Infof("Found %v/%v as a destination shard", si.Keyspace(), si.ShardName())
				destinationShards = append(destinationShards, si)
				break
			}
		}
	}
	if len(destinationShards) == 0 {
		return fmt.Errorf("Cannot find any destination shard replicating from %v/%v", keyspace, shard)
	}

	// TODO(alainjobart) for a reverse split, we also need to find
	// more sources. For now, a single source is all we need, but we
	// still use a list of sources to not have to change the code later.
	sourceShards := make([]*topo.ShardInfo, 0, 0)

	// Verify the source has the type we're migrating
	si, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}
	foundType := topo.IsTypeInList(servedType, si.ServedTypes)
	if reverse {
		if foundType {
			return fmt.Errorf("Source shard %v/%v is already serving type %v", keyspace, shard, servedType)
		}
	} else {
		if !foundType {
			return fmt.Errorf("Source shard %v/%v is not serving type %v", keyspace, shard, servedType)
		}
	}
	if servedType == topo.TYPE_MASTER && len(si.ServedTypes) > 1 {
		return fmt.Errorf("Cannot migrate master out of %v/%v until everything else is migrated out", keyspace, shard)
	}
	sourceShards = append(sourceShards, si)

	// lock the shards: sources, then destinations
	// (note they're all ordered by shard name)
	actionNode := actionnode.MigrateServedTypes(servedType)
	sourceLockPath := make([]string, len(sourceShards))
	for i, si := range sourceShards {
		sourceLockPath[i], err = wr.lockShard(si.Keyspace(), si.ShardName(), actionNode)
		if err != nil {
			log.Errorf("Failed to lock source shard %v/%v, may need to unlock other shards manually", si.Keyspace(), si.ShardName())
			return err
		}
	}
	destinationLockPath := make([]string, len(destinationShards))
	for i, si := range destinationShards {
		destinationLockPath[i], err = wr.lockShard(si.Keyspace(), si.ShardName(), actionNode)
		if err != nil {
			log.Errorf("Failed to lock destination shard %v/%v, may need to unlock other shards manually", si.Keyspace(), si.ShardName())
			return err
		}
	}

	// record the action error and all unlock errors
	rec := concurrency.AllErrorRecorder{}

	// execute the migration
	shardCache := make(map[string]*topo.ShardInfo)
	rec.RecordError(wr.migrateServedTypes(keyspace, sourceShards, destinationShards, servedType, reverse, shardCache))

	// unlock the shards, we're done
	for i := len(destinationShards) - 1; i >= 0; i-- {
		rec.RecordError(wr.unlockShard(destinationShards[i].Keyspace(), destinationShards[i].ShardName(), actionNode, destinationLockPath[i], nil))
	}
	for i := len(sourceShards) - 1; i >= 0; i-- {
		rec.RecordError(wr.unlockShard(sourceShards[i].Keyspace(), sourceShards[i].ShardName(), actionNode, sourceLockPath[i], nil))
	}

	// rebuild the keyspace serving graph if there was no error
	if rec.Error() == nil {
		if skipRebuild {
			log.Infof("Skipping keyspace rebuild, please run it at earliest convenience")
		} else {
			rec.RecordError(wr.RebuildKeyspaceGraph(keyspace, nil, shardCache))
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

func (wr *Wrangler) makeMastersReadOnly(shards []*topo.ShardInfo) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range shards {
		if si.MasterAlias.IsZero() {
			rec.RecordError(fmt.Errorf("Shard %v/%v has no master?", si.Keyspace(), si.ShardName()))
			continue
		}

		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()

			log.Infof("Making master %v read-only", si.MasterAlias)
			actionPath, err := wr.ai.SetReadOnly(si.MasterAlias)
			if err != nil {
				rec.RecordError(err)
				return
			}
			rec.RecordError(wr.WaitForCompletion(actionPath))
			log.Infof("Master %v is now read-only", si.MasterAlias)
		}(si)
	}
	wg.Wait()
	return rec.Error()
}

func (wr *Wrangler) getMastersPosition(shards []*topo.ShardInfo) (map[*topo.ShardInfo]*myproto.ReplicationPosition, error) {
	mu := sync.Mutex{}
	result := make(map[*topo.ShardInfo]*myproto.ReplicationPosition)

	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range shards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
			log.Infof("Gathering master position for %v", si.MasterAlias)
			ti, err := wr.ts.GetTablet(si.MasterAlias)
			if err != nil {
				rec.RecordError(err)
				return
			}

			pos, err := wr.ai.MasterPosition(ti, wr.ActionTimeout())
			if err != nil {
				rec.RecordError(err)
				return
			}

			log.Infof("Got master position for %v", si.MasterAlias)
			mu.Lock()
			result[si] = pos
			mu.Unlock()
		}(si)
	}
	wg.Wait()
	return result, rec.Error()
}

func (wr *Wrangler) waitForFilteredReplication(sourcePositions map[*topo.ShardInfo]*myproto.ReplicationPosition, destinationShards []*topo.ShardInfo) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range destinationShards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			for _, sourceShard := range si.SourceShards {
				// we're waiting on this guy
				blpPosition := blproto.BlpPosition{
					Uid: sourceShard.Uid,
				}

				// find the position it should be at
				for s, rp := range sourcePositions {
					if s.Keyspace() == sourceShard.Keyspace && s.ShardName() == sourceShard.Shard {
						blpPosition.GTIDField = rp.MasterLogGTIDField
					}
				}

				log.Infof("Waiting for %v to catch up", si.MasterAlias)
				if err := wr.ai.WaitBlpPosition(si.MasterAlias, blpPosition, wr.ActionTimeout()); err != nil {
					rec.RecordError(err)
				} else {
					log.Infof("%v caught up", si.MasterAlias)
				}
				wg.Done()
			}
		}(si)
	}
	wg.Wait()
	return rec.Error()
}

// FIXME(alainjobart) no action to become read-write now, just use Ping,
// that forces the shard reload and will stop replication.
func (wr *Wrangler) makeMastersReadWrite(shards []*topo.ShardInfo) error {
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, si := range shards {
		wg.Add(1)
		go func(si *topo.ShardInfo) {
			defer wg.Done()
			log.Infof("Pinging master %v", si.MasterAlias)

			actionPath, err := wr.ai.Ping(si.MasterAlias)
			if err != nil {
				rec.RecordError(err)
				return
			}

			if err := wr.WaitForCompletion(actionPath); err != nil {
				rec.RecordError(err)
			} else {
				log.Infof("%v responded", si.MasterAlias)
			}

		}(si)
	}
	wg.Wait()
	return rec.Error()
}

// migrateServedTypes operates with all concerned shards locked.
func (wr *Wrangler) migrateServedTypes(keyspace string, sourceShards, destinationShards []*topo.ShardInfo, servedType topo.TabletType, reverse bool, shardCache map[string]*topo.ShardInfo) (err error) {

	// re-read all the shards so we are up to date
	for i, si := range sourceShards {
		if sourceShards[i], err = wr.ts.GetShard(si.Keyspace(), si.ShardName()); err != nil {
			return err
		}
		shardCache[si.ShardName()] = sourceShards[i]
	}
	for i, si := range destinationShards {
		if destinationShards[i], err = wr.ts.GetShard(si.Keyspace(), si.ShardName()); err != nil {
			return err
		}
		shardCache[si.ShardName()] = destinationShards[i]
	}

	ev := &events.MigrateServedTypes{
		Keyspace:          *topo.NewKeyspaceInfo(keyspace, nil),
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

	// check and update all shard records, in memory only
	for _, si := range sourceShards {
		if reverse {
			// need to add to source
			if topo.IsTypeInList(servedType, si.ServedTypes) {
				return fmt.Errorf("Source shard %v/%v is already serving type %v", si.Keyspace(), si.ShardName(), servedType)
			}
			si.ServedTypes = append(si.ServedTypes, servedType)
		} else {
			// need to remove from source
			var found bool
			if si.ServedTypes, found = removeType(servedType, si.ServedTypes); !found {
				return fmt.Errorf("Source shard %v/%v is not serving type %v", si.Keyspace(), si.ShardName(), servedType)
			}
		}
	}
	for _, si := range destinationShards {
		if reverse {
			// need to remove from destination
			var found bool
			if si.ServedTypes, found = removeType(servedType, si.ServedTypes); !found {
				return fmt.Errorf("Destination shard %v/%v is not serving type %v", si.Keyspace(), si.ShardName(), servedType)
			}
		} else {
			// need to add to destination
			if topo.IsTypeInList(servedType, si.ServedTypes) {
				return fmt.Errorf("Destination shard %v/%v is already serving type %v", si.Keyspace(), si.ShardName(), servedType)
			}
			si.ServedTypes = append(si.ServedTypes, servedType)
		}
	}

	// For master type migration, need to:
	// - switch the source shards to read-only
	// - gather all replication points
	// - wait for filtered replication to catch up before we continue
	// - disable filtered replication after the fact
	if servedType == topo.TYPE_MASTER {
		event.DispatchUpdate(ev, "setting all source masters read-only")
		err := wr.makeMastersReadOnly(sourceShards)
		if err != nil {
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

	// All is good, we can save the shards now
	event.DispatchUpdate(ev, "updating source shards")
	for _, si := range sourceShards {
		if err := wr.ts.UpdateShard(si); err != nil {
			return err
		}
		shardCache[si.ShardName()] = si
	}
	event.DispatchUpdate(ev, "updating destination shards")
	for _, si := range destinationShards {
		if err := wr.ts.UpdateShard(si); err != nil {
			return err
		}
		shardCache[si.ShardName()] = si
	}

	// And tell the new shards masters they can now be read-write.
	// Invoking a remote action will also make the tablet stop filtered
	// replication.
	if servedType == topo.TYPE_MASTER {
		event.DispatchUpdate(ev, "setting destination masters read-write")
		if err := wr.makeMastersReadWrite(destinationShards); err != nil {
			return err
		}
	}

	event.DispatchUpdate(ev, "finished")
	return nil
}

func (wr *Wrangler) MigrateServedFrom(keyspace, shard string, servedType topo.TabletType, reverse, skipRebuild bool) error {
	if servedType == topo.TYPE_MASTER {
		// we cannot migrate a master back
		if reverse {
			return fmt.Errorf("Cannot migrate master back to %v/%v", keyspace, shard)
		}
		// we cannot skip rebuild for a master
		if skipRebuild {
			return fmt.Errorf("Cannot skip rebuild for master migration on %v/%v", keyspace, shard)
		}
	}

	// read the destination keyspace, check it
	ki, err := wr.ts.GetKeyspace(keyspace)
	if err != nil {
		return err
	}
	if len(ki.ServedFrom) == 0 {
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

	// check the migration is valid
	foundType := false
	for tt, _ := range ki.ServedFrom {
		if tt == servedType {
			foundType = true
		}
	}
	if foundType == reverse {
		return fmt.Errorf("Supplied type cannot be migrated")
	}
	if servedType == topo.TYPE_MASTER && len(ki.ServedFrom) > 1 {
		return fmt.Errorf("Cannot migrate master into %v/%v until everything else is migrated", keyspace, shard)
	}

	// lock the keyspace and shard
	actionNode := actionnode.MigrateServedFrom(servedType)
	keyspaceLockPath, err := wr.lockKeyspace(keyspace, actionNode)
	if err != nil {
		log.Errorf("Failed to lock destination keyspace %v", keyspace)
		return err
	}
	shardLockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		log.Errorf("Failed to lock destination shard %v/%v", keyspace, shard)
		wr.unlockKeyspace(keyspace, actionNode, keyspaceLockPath, nil)
		return err
	}

	// record the action error and all unlock errors
	rec := concurrency.AllErrorRecorder{}

	// execute the migration
	rec.RecordError(wr.migrateServedFrom(ki, si, servedType, reverse))

	rec.RecordError(wr.unlockShard(keyspace, shard, actionNode, shardLockPath, nil))
	rec.RecordError(wr.unlockKeyspace(keyspace, actionNode, keyspaceLockPath, nil))

	// rebuild the keyspace serving graph if there was no error
	if rec.Error() == nil {
		if skipRebuild {
			log.Infof("Skipping keyspace rebuild, please run it at earliest convenience")
		} else {
			rec.RecordError(wr.RebuildKeyspaceGraph(keyspace, nil, nil))
		}
	}

	return rec.Error()
}

func (wr *Wrangler) migrateServedFrom(ki *topo.KeyspaceInfo, si *topo.ShardInfo, servedType topo.TabletType, reverse bool) (err error) {

	// re-read and update keyspace info record
	ki, err = wr.ts.GetKeyspace(ki.KeyspaceName())
	if err != nil {
		return err
	}
	if reverse {
		if _, ok := ki.ServedFrom[servedType]; ok {
			return fmt.Errorf("Destination Keyspace %s is not serving type %v", ki.KeyspaceName(), servedType)
		}
		ki.ServedFrom[servedType] = si.SourceShards[0].Keyspace
	} else {
		if _, ok := ki.ServedFrom[servedType]; !ok {
			return fmt.Errorf("Destination Keyspace %s is already serving type %v", ki.KeyspaceName(), servedType)
		}
		delete(ki.ServedFrom, servedType)
	}

	// re-read and check the destination shard
	si, err = wr.ts.GetShard(si.Keyspace(), si.ShardName())
	if err != nil {
		return err
	}
	if len(si.SourceShards) != 1 {
		return fmt.Errorf("Destination shard %v/%v is not a vertical split target", si.Keyspace(), si.ShardName())
	}
	tables := si.SourceShards[0].Tables

	// read the source shard, we'll need its master
	sourceShard, err := wr.ts.GetShard(si.SourceShards[0].Keyspace, si.SourceShards[0].Shard)
	if err != nil {
		return err
	}

	ev := &events.MigrateServedFrom{
		Keyspace:         *ki,
		SourceShard:      *sourceShard,
		DestinationShard: *si,
		ServedType:       servedType,
		Reverse:          reverse,
	}
	event.DispatchUpdate(ev, "start")
	defer func() {
		if err != nil {
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()

	// For master type migration, need to:
	// - switch the source shard to read-only
	// - gather the replication point
	// - wait for filtered replication to catch up before we continue
	// - disable filtered replication after the fact
	var sourceMasterTabletInfo *topo.TabletInfo
	if servedType == topo.TYPE_MASTER {
		// set master to read-only
		event.DispatchUpdate(ev, "setting source shard master to read-only")
		actionPath, err := wr.ai.SetReadOnly(sourceShard.MasterAlias)
		if err != nil {
			return err
		}
		if err := wr.WaitForCompletion(actionPath); err != nil {
			return err
		}

		// get the position
		event.DispatchUpdate(ev, "getting master position")
		sourceMasterTabletInfo, err = wr.ts.GetTablet(sourceShard.MasterAlias)
		if err != nil {
			return err
		}
		masterPosition, err := wr.ai.MasterPosition(sourceMasterTabletInfo, wr.ActionTimeout())
		if err != nil {
			return err
		}

		// wait for it
		event.DispatchUpdate(ev, "waiting for destination master to catch up to source master")
		if err := wr.ai.WaitBlpPosition(si.MasterAlias, blproto.BlpPosition{
			Uid:       0,
			GTIDField: masterPosition.MasterLogGTIDField,
		}, wr.ActionTimeout()); err != nil {
			return err
		}

		// and clear the shard record
		si.SourceShards = nil
	}

	// All is good, we can save the keyspace and shard (if needed) now
	event.DispatchUpdate(ev, "updating keyspace")
	if err = wr.ts.UpdateKeyspace(ki); err != nil {
		return err
	}
	event.DispatchUpdate(ev, "updating destination shard")
	if servedType == topo.TYPE_MASTER {
		if err := wr.ts.UpdateShard(si); err != nil {
			return err
		}
	}

	// Tell the new shards masters they can now be read-write.
	// Invoking a remote action will also make the tablet stop filtered
	// replication.
	event.DispatchUpdate(ev, "setting destination shard masters read-write")
	if servedType == topo.TYPE_MASTER {
		if err := wr.makeMastersReadWrite([]*topo.ShardInfo{si}); err != nil {
			return err
		}
	}

	// Now blacklist the table list on the right servers
	event.DispatchUpdate(ev, "setting blacklisted tables on source shard")
	if servedType == topo.TYPE_MASTER {
		if err := wr.ai.SetBlacklistedTables(sourceMasterTabletInfo, tables, wr.ActionTimeout()); err != nil {
			return err
		}
	} else {
		// We use the list of tables that are replicating
		// for the blacklist. In case of a reverse move, we clear the
		// blacklist.
		if reverse {
			tables = nil
		}
		if err := wr.SetBlacklistedTablesByShard(sourceShard.Keyspace(), sourceShard.ShardName(), servedType, tables); err != nil {
			return err
		}
	}

	event.DispatchUpdate(ev, "finished")
	return nil
}

// SetBlacklistedTablesByShard sets the blacklisted table list of all
// tablets of a given type in a shard. It would work for the master,
// but it wouldn't be very efficient.
func (wr *Wrangler) SetBlacklistedTablesByShard(keyspace, shard string, tabletType topo.TabletType, tables []string) error {
	tabletMap, err := topo.GetTabletMapForShard(wr.ts, keyspace, shard)
	switch err {
	case nil:
		// keep going
	case topo.ErrPartialResult:
		log.Warningf("SetBlacklistedTablesByShard: got partial result, may not blacklist everything everywhere")
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
			if err := wr.ai.SetBlacklistedTables(ti, tables, wr.ActionTimeout()); err != nil {
				log.Warningf("SetBlacklistedTablesByShard: failed to set tables for %v: %v", ti.Alias, err)
			}
			wg.Done()
		}(ti)
	}
	wg.Wait()

	return nil
}
