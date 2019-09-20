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

package wrangler

import (
	"bytes"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	// DefaultFilteredReplicationWaitTime is the default value for argument filteredReplicationWaitTime.
	DefaultFilteredReplicationWaitTime = 30 * time.Second
)

// TODO(b/26388813): Remove these flags once vtctl WaitForDrain is integrated in the vtctl MigrateServed* commands.
var (
	waitForDrainSleepRdonly  = flag.Duration("wait_for_drain_sleep_rdonly", 5*time.Second, "time to wait before shutting the query service on old RDONLY tablets during MigrateServedTypes")
	waitForDrainSleepReplica = flag.Duration("wait_for_drain_sleep_replica", 15*time.Second, "time to wait before shutting the query service on old REPLICA tablets during MigrateServedTypes")
)

// keyspace related methods for Wrangler

// SetKeyspaceShardingInfo locks a keyspace and sets its ShardingColumnName
// and ShardingColumnType
func (wr *Wrangler) SetKeyspaceShardingInfo(ctx context.Context, keyspace, shardingColumnName string, shardingColumnType topodatapb.KeyspaceIdType, force bool) (err error) {
	// Lock the keyspace
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, keyspace, "SetKeyspaceShardingInfo")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// and change it
	ki, err := wr.ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}

	if ki.ShardingColumnName != "" && ki.ShardingColumnName != shardingColumnName {
		if force {
			wr.Logger().Warningf("Forcing keyspace ShardingColumnName change from %v to %v", ki.ShardingColumnName, shardingColumnName)
		} else {
			return fmt.Errorf("cannot change ShardingColumnName from %v to %v (use -force to override)", ki.ShardingColumnName, shardingColumnName)
		}
	}

	if ki.ShardingColumnType != topodatapb.KeyspaceIdType_UNSET && ki.ShardingColumnType != shardingColumnType {
		if force {
			wr.Logger().Warningf("Forcing keyspace ShardingColumnType change from %v to %v", ki.ShardingColumnType, shardingColumnType)
		} else {
			return fmt.Errorf("cannot change ShardingColumnType from %v to %v (use -force to override)", ki.ShardingColumnType, shardingColumnType)
		}
	}

	ki.ShardingColumnName = shardingColumnName
	ki.ShardingColumnType = shardingColumnType
	return wr.ts.UpdateKeyspace(ctx, ki)
}

// SplitClone initiates a SplitClone workflow.
func (wr *Wrangler) SplitClone(ctx context.Context, keyspace string, from, to []string) error {
	var fromShards, toShards []*topo.ShardInfo
	for _, shard := range from {
		si, err := wr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return vterrors.Wrapf(err, "GetShard(%s) failed", shard)
		}
		fromShards = append(fromShards, si)
	}
	for _, shard := range to {
		si, err := wr.ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return vterrors.Wrapf(err, "GetShard(%s) failed", shard)
		}
		toShards = append(toShards, si)
	}
	// TODO(sougou): validate from and to shards.

	for _, dest := range toShards {
		master, err := wr.ts.GetTablet(ctx, dest.MasterAlias)
		if err != nil {
			return vterrors.Wrapf(err, "GetTablet(%v) failed", dest.MasterAlias)
		}
		var ids []uint64
		for _, source := range fromShards {
			filter := &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{
					Match:  "/.*",
					Filter: key.KeyRangeString(dest.KeyRange),
				}},
			}
			bls := &binlogdatapb.BinlogSource{
				Keyspace: keyspace,
				Shard:    source.ShardName(),
				Filter:   filter,
			}
			cmd := binlogplayer.CreateVReplicationState("VSplitClone", bls, "", binlogplayer.BlpStopped, master.DbName())
			qr, err := wr.TabletManagerClient().VReplicationExec(ctx, master.Tablet, cmd)
			if err != nil {
				return vterrors.Wrapf(err, "VReplicationExec(%v, %s) failed", dest.MasterAlias, cmd)
			}
			if err := wr.SourceShardAdd(ctx, keyspace, dest.ShardName(), uint32(qr.InsertId), keyspace, source.ShardName(), source.Shard.KeyRange, nil); err != nil {
				return vterrors.Wrapf(err, "SourceShardAdd(%s, %s) failed", dest.ShardName(), source.ShardName())
			}
			ids = append(ids, qr.InsertId)
		}
		// Start vreplication only if all metadata was successfully created.
		for _, id := range ids {
			cmd := fmt.Sprintf("update _vt.vreplication set state='%s' where id=%d", binlogplayer.VReplicationInit, id)
			if _, err = wr.TabletManagerClient().VReplicationExec(ctx, master.Tablet, cmd); err != nil {
				return vterrors.Wrapf(err, "VReplicationExec(%v, %s) failed", dest.MasterAlias, cmd)
			}
		}
	}
	return wr.refreshMasters(ctx, toShards)
}

// VerticalSplitClone initiates a VerticalSplitClone workflow.
func (wr *Wrangler) VerticalSplitClone(ctx context.Context, fromKeyspace, toKeyspace string, tables []string) error {
	source, err := wr.ts.GetOnlyShard(ctx, fromKeyspace)
	if err != nil {
		return vterrors.Wrapf(err, "GetOnlyShard(%s) failed", fromKeyspace)
	}
	dest, err := wr.ts.GetOnlyShard(ctx, toKeyspace)
	if err != nil {
		return vterrors.Wrapf(err, "GetOnlyShard(%s) failed", toKeyspace)
	}
	// TODO(sougou): validate from and to shards.

	master, err := wr.ts.GetTablet(ctx, dest.MasterAlias)
	if err != nil {
		return vterrors.Wrapf(err, "GetTablet(%v) failed", dest.MasterAlias)
	}
	filter := &binlogdatapb.Filter{}
	for _, table := range tables {
		filter.Rules = append(filter.Rules, &binlogdatapb.Rule{
			Match: table,
		})
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: fromKeyspace,
		Shard:    source.ShardName(),
		Filter:   filter,
	}
	cmd := binlogplayer.CreateVReplicationState("VSplitClone", bls, "", binlogplayer.BlpStopped, master.DbName())
	qr, err := wr.TabletManagerClient().VReplicationExec(ctx, master.Tablet, cmd)
	if err != nil {
		return vterrors.Wrapf(err, "VReplicationExec(%v, %s) failed", dest.MasterAlias, cmd)
	}
	if err := wr.SourceShardAdd(ctx, toKeyspace, dest.ShardName(), uint32(qr.InsertId), fromKeyspace, source.ShardName(), nil, tables); err != nil {
		return vterrors.Wrapf(err, "SourceShardAdd(%s, %s) failed", dest.ShardName(), source.ShardName())
	}
	// Start vreplication only if metadata was successfully created.
	cmd = fmt.Sprintf("update _vt.vreplication set state='%s' where id=%d", binlogplayer.VReplicationInit, qr.InsertId)
	if _, err = wr.TabletManagerClient().VReplicationExec(ctx, master.Tablet, cmd); err != nil {
		return vterrors.Wrapf(err, "VReplicationExec(%v, %s) failed", dest.MasterAlias, cmd)
	}
	return wr.refreshMasters(ctx, []*topo.ShardInfo{dest})
}

// ShowResharding shows all resharding related metadata for the keyspace/shard.
func (wr *Wrangler) ShowResharding(ctx context.Context, keyspace, shard string) (err error) {
	ki, err := wr.ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}
	if len(ki.ServedFroms) == 0 {
		return wr.showHorizontalResharding(ctx, keyspace, shard)
	}
	return wr.showVerticalResharding(ctx, keyspace, shard)
}

func (wr *Wrangler) showHorizontalResharding(ctx context.Context, keyspace, shard string) error {
	osList, err := topotools.FindOverlappingShards(ctx, wr.ts, keyspace)
	if err != nil {
		return fmt.Errorf("FindOverlappingShards failed: %v", err)
	}
	os := topotools.OverlappingShardsForShard(osList, shard)
	if os == nil {
		wr.Logger().Printf("No resharding in progress\n")
		return nil
	}

	sourceShards, destinationShards, err := wr.findSourceDest(ctx, os)
	if err != nil {
		return err
	}
	wr.Logger().Printf("Horizontal Resharding for %v:\n", keyspace)
	wr.Logger().Printf("  Sources:\n")
	if err := wr.printShards(ctx, sourceShards); err != nil {
		return err
	}
	wr.Logger().Printf("  Destinations:\n")
	return wr.printShards(ctx, destinationShards)
}

func (wr *Wrangler) printShards(ctx context.Context, si []*topo.ShardInfo) error {
	for _, si := range si {
		wr.Logger().Printf("    Shard: %v\n", si.ShardName())
		if len(si.SourceShards) != 0 {
			wr.Logger().Printf("      Source Shards: %v\n", si.SourceShards)
		}
		ti, err := wr.ts.GetTablet(ctx, si.MasterAlias)
		if err != nil {
			return err
		}
		qr, err := wr.tmc.VReplicationExec(ctx, ti.Tablet, fmt.Sprintf("select * from _vt.vreplication where db_name=%v", encodeString(ti.DbName())))
		if err != nil {
			return err
		}
		res := sqltypes.Proto3ToResult(qr)
		if len(res.Rows) != 0 {
			wr.Logger().Printf("      VReplication:\n")
			for _, row := range res.Rows {
				wr.Logger().Printf("        %v\n", row)
			}
		}
		wr.Logger().Printf("      Is Master Serving: %v\n", si.IsMasterServing)
		if len(si.TabletControls) != 0 {
			wr.Logger().Printf("      Tablet Controls: %v\n", si.TabletControls)
		}
	}
	return nil
}

// CancelResharding cancels any resharding in progress on the specified keyspace/shard.
// This works for horizontal as well as vertical resharding.
func (wr *Wrangler) CancelResharding(ctx context.Context, keyspace, shard string) (err error) {
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, keyspace, "CancelResharding")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	ki, err := wr.ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}
	if len(ki.ServedFroms) == 0 {
		return wr.cancelHorizontalResharding(ctx, keyspace, shard)
	}
	return wr.cancelVerticalResharding(ctx, keyspace, shard)
}

func (wr *Wrangler) cancelHorizontalResharding(ctx context.Context, keyspace, shard string) error {
	wr.Logger().Infof("Finding the overlapping shards in keyspace %v", keyspace)
	osList, err := topotools.FindOverlappingShards(ctx, wr.ts, keyspace)
	if err != nil {
		return fmt.Errorf("FindOverlappingShards failed: %v", err)
	}

	// find our shard in there
	os := topotools.OverlappingShardsForShard(osList, shard)
	if os == nil {
		return fmt.Errorf("shard %v is not involved in any overlapping shards", shard)
	}

	_, destinationShards, err := wr.findSourceDest(ctx, os)
	if err != nil {
		return err
	}

	// get srvKeyspaces in all cells to check if they are already serving this shard
	srvKeyspaces, err := wr.ts.GetSrvKeyspaceAllCells(ctx, keyspace)
	if err != nil {
		return err
	}

	for _, si := range destinationShards {
		for _, srvKeyspace := range srvKeyspaces {
			if topo.ShardIsServing(srvKeyspace, si.Shard) {
				return fmt.Errorf("some served types have migrated for %v/%v, please undo them before canceling", keyspace, shard)
			}
		}
	}
	for i, si := range destinationShards {
		ti, err := wr.ts.GetTablet(ctx, si.MasterAlias)
		if err != nil {
			return err
		}
		for _, sourceShard := range si.SourceShards {
			if _, err := wr.tmc.VReplicationExec(ctx, ti.Tablet, binlogplayer.DeleteVReplication(sourceShard.Uid)); err != nil {
				return err
			}
		}
		updatedShard, err := wr.ts.UpdateShardFields(ctx, si.Keyspace(), si.ShardName(), func(si *topo.ShardInfo) error {
			si.TabletControls = nil
			si.SourceShards = nil
			return nil
		})
		if err != nil {
			return err
		}

		destinationShards[i] = updatedShard

		if err := wr.RefreshTabletsByShard(ctx, si, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

// MigrateServedTypes is used during horizontal splits to migrate a
// served type from a list of shards to another.
func (wr *Wrangler) MigrateServedTypes(ctx context.Context, keyspace, shard string, cells []string, servedType topodatapb.TabletType, reverse, skipReFreshState bool, filteredReplicationWaitTime time.Duration, reverseReplication bool) (err error) {
	// check input parameters
	if servedType == topodatapb.TabletType_MASTER {
		// we cannot migrate a master back, since when master migration
		// is done, the source shards are dead
		if reverse {
			return fmt.Errorf("cannot migrate master back to %v/%v", keyspace, shard)
		}
		// we cannot skip refresh state for a master
		if skipReFreshState {
			return fmt.Errorf("cannot skip refresh state for master migration on %v/%v", keyspace, shard)
		}
		if cells != nil {
			return fmt.Errorf("cannot specify cells for master migration on %v/%v", keyspace, shard)
		}
	}

	// lock the keyspace
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, keyspace, fmt.Sprintf("MigrateServedTypes(%v)", servedType))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// find overlapping shards in this keyspace
	wr.Logger().Infof("Finding the overlapping shards in keyspace %v", keyspace)
	osList, err := topotools.FindOverlappingShards(ctx, wr.ts, keyspace)
	if err != nil {
		return fmt.Errorf("FindOverlappingShards failed: %v", err)
	}

	// find our shard in there
	os := topotools.OverlappingShardsForShard(osList, shard)
	if os == nil {
		return fmt.Errorf("shard %v is not involved in any overlapping shards", shard)
	}

	sourceShards, destinationShards, err := wr.findSourceDest(ctx, os)
	if err != nil {
		return err
	}

	// execute the migration
	if servedType == topodatapb.TabletType_MASTER {
		if err = wr.masterMigrateServedType(ctx, keyspace, sourceShards, destinationShards, filteredReplicationWaitTime, reverseReplication); err != nil {
			return err
		}
	} else {
		if err = wr.replicaMigrateServedType(ctx, keyspace, sourceShards, destinationShards, cells, servedType, reverse); err != nil {
			return err
		}
	}

	// Master migrate performs its own refresh.
	// Otherwise, honor skipRefreshState if requested.
	if servedType == topodatapb.TabletType_MASTER || skipReFreshState {
		return nil
	}

	// refresh
	// TODO(b/26388813): Integrate vtctl WaitForDrain here instead of just sleeping.
	// Anything that's not a replica will use the RDONLY sleep time.
	waitForDrainSleep := *waitForDrainSleepRdonly
	if servedType == topodatapb.TabletType_REPLICA {
		waitForDrainSleep = *waitForDrainSleepReplica
	}
	wr.Logger().Infof("WaitForDrain: Sleeping for %.0f seconds before shutting down query service on old tablets...", waitForDrainSleep.Seconds())
	time.Sleep(waitForDrainSleep)
	wr.Logger().Infof("WaitForDrain: Sleeping finished. Shutting down queryservice on old tablets now.")

	rec := concurrency.AllErrorRecorder{}
	refreshShards := sourceShards
	if reverse {
		// For a backwards migration, we should refresh (disable) destination shards instead.
		refreshShards = destinationShards
	}
	for _, si := range refreshShards {
		rec.RecordError(wr.RefreshTabletsByShard(ctx, si, []topodatapb.TabletType{servedType}, cells))
	}
	return rec.Error()
}

// findSourceDest derives the source and destination from the overlapping shards.
// Whichever side has SourceShards is a destination.
func (wr *Wrangler) findSourceDest(ctx context.Context, os *topotools.OverlappingShards) (sourceShards, destinationShards []*topo.ShardInfo, err error) {
	// It's possible that both source and destination have source shards because of reversible replication.
	// If so, the Frozen flag in the tablet control record dictates the direction.
	// So, check that first.
	for _, left := range os.Left {
		tc := left.GetTabletControl(topodatapb.TabletType_MASTER)
		if tc == nil {
			continue
		}
		if tc.Frozen {
			return os.Left, os.Right, nil
		}
	}
	for _, right := range os.Right {
		tc := right.GetTabletControl(topodatapb.TabletType_MASTER)
		if tc == nil {
			continue
		}
		if tc.Frozen {
			return os.Right, os.Left, nil
		}
	}
	for _, left := range os.Left {
		if len(left.SourceShards) != 0 {
			return os.Right, os.Left, nil
		}
	}
	for _, right := range os.Right {
		if len(right.SourceShards) != 0 {
			return os.Left, os.Right, nil
		}
	}
	return nil, nil, fmt.Errorf("neither Shard '%v' nor Shard '%v' have a 'SourceShards' entry. Did you successfully run vtworker SplitClone before? Or did you already migrate the MASTER type?", os.Left[0].ShardName(), os.Right[0].ShardName())
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

			pos, err := wr.tmc.MasterPosition(ctx, ti.Tablet)
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
			ctx, cancel := context.WithTimeout(ctx, waitTime)
			defer cancel()

			var pos string
			for _, sourceShard := range si.SourceShards {
				// find the position it should be at
				for s, sp := range sourcePositions {
					if s.Keyspace() == sourceShard.Keyspace && s.ShardName() == sourceShard.Shard {
						pos = sp
						break
					}
				}

				// and wait for it
				wr.Logger().Infof("Waiting for %v to catch up", topoproto.TabletAliasString(si.MasterAlias))
				ti, err := wr.ts.GetTablet(ctx, si.MasterAlias)
				if err != nil {
					rec.RecordError(err)
					return
				}

				if err := wr.tmc.VReplicationWaitForPos(ctx, ti.Tablet, int(sourceShard.Uid), pos); err != nil {
					if strings.Contains(err.Error(), "not found") {
						wr.Logger().Infof("%v stream %d was not found. Skipping wait.", topoproto.TabletAliasString(si.MasterAlias), sourceShard.Uid)
					} else {
						rec.RecordError(err)
					}
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

			if err := wr.tmc.RefreshState(ctx, ti.Tablet); err != nil {
				rec.RecordError(err)
			} else {
				wr.Logger().Infof("%v responded", topoproto.TabletAliasString(si.MasterAlias))
			}
		}(si)
	}
	wg.Wait()
	return rec.Error()
}

// replicaMigrateServedType operates with the keyspace locked
func (wr *Wrangler) replicaMigrateServedType(ctx context.Context, keyspace string, sourceShards, destinationShards []*topo.ShardInfo, cells []string, servedType topodatapb.TabletType, reverse bool) (err error) {
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

	fromShards, toShards := sourceShards, destinationShards
	if reverse {
		fromShards, toShards = toShards, fromShards
	}

	// Check and update all source shard records.
	// Enable query service if needed
	event.DispatchUpdate(ev, "updating shards to migrate from")
	if err = wr.updateShardRecords(ctx, keyspace, fromShards, cells, servedType, true /* isFrom */, false /* clearSourceShards */); err != nil {
		return err
	}

	// Do the same for destination shards
	event.DispatchUpdate(ev, "updating shards to migrate to")
	if err = wr.updateShardRecords(ctx, keyspace, toShards, cells, servedType, false, false); err != nil {
		return err
	}

	// Now update serving keyspace

	if err = wr.ts.MigrateServedType(ctx, keyspace, toShards, fromShards, servedType, cells); err != nil {
		return err
	}

	event.DispatchUpdate(ev, "finished")
	return nil
}

// masterMigrateServedType operates with the keyspace locked
func (wr *Wrangler) masterMigrateServedType(ctx context.Context, keyspace string, sourceShards, destinationShards []*topo.ShardInfo, filteredReplicationWaitTime time.Duration, reverseReplication bool) (err error) {
	// Ensure other served types have migrated.
	srvKeyspaces, err := wr.ts.GetSrvKeyspaceAllCells(ctx, keyspace)
	if err != nil {
		return err
	}

	si := sourceShards[0]
	for _, srvKeyspace := range srvKeyspaces {
		var shardServedTypes []string
		for _, partition := range srvKeyspace.GetPartitions() {
			if partition.GetServedType() != topodatapb.TabletType_MASTER {
				for _, shardReference := range partition.GetShardReferences() {
					if key.KeyRangeEqual(shardReference.GetKeyRange(), si.GetKeyRange()) {
						shardServedTypes = append(shardServedTypes, partition.GetServedType().String())
					}
				}
			}
		}
		if len(shardServedTypes) > 0 {
			return fmt.Errorf("cannot migrate MASTER away from %v/%v until everything else is migrated. Make sure that the following types are migrated first: %v", si.Keyspace(), si.ShardName(), strings.Join(shardServedTypes, ", "))
		}
	}

	ev := &events.MigrateServedTypes{
		KeyspaceName:      keyspace,
		SourceShards:      sourceShards,
		DestinationShards: destinationShards,
		ServedType:        topodatapb.TabletType_MASTER,
	}
	event.DispatchUpdate(ev, "start")
	defer func() {
		if err != nil {
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()

	// Phase 1
	// - check topology service can successfully refresh both source and target master
	// - switch the source shards to read-only by disabling query service
	// - gather all replication points
	// - wait for filtered replication to catch up
	// - mark source shards as frozen
	event.DispatchUpdate(ev, "disabling query service on all source masters")
	// making sure the refreshMaster on both source and target are working before turning off query service on source
	if err := wr.refreshMasters(ctx, sourceShards); err != nil {
		wr.cancelMasterMigrateServedTypes(ctx, keyspace, sourceShards)
		return err
	}
	if err := wr.refreshMasters(ctx, destinationShards); err != nil {
		wr.cancelMasterMigrateServedTypes(ctx, keyspace, sourceShards)
		return err
	}

	if err := wr.updateShardRecords(ctx, keyspace, sourceShards, nil, topodatapb.TabletType_MASTER, true, false); err != nil {
		wr.cancelMasterMigrateServedTypes(ctx, keyspace, sourceShards)
		return err
	}
	if err := wr.refreshMasters(ctx, sourceShards); err != nil {
		wr.cancelMasterMigrateServedTypes(ctx, keyspace, sourceShards)
		return err
	}

	event.DispatchUpdate(ev, "getting positions of source masters")
	masterPositions, err := wr.getMastersPosition(ctx, sourceShards)
	if err != nil {
		wr.cancelMasterMigrateServedTypes(ctx, keyspace, sourceShards)
		return err
	}

	event.DispatchUpdate(ev, "waiting for destination masters to catch up")
	if err := wr.waitForFilteredReplication(ctx, masterPositions, destinationShards, filteredReplicationWaitTime); err != nil {
		wr.cancelMasterMigrateServedTypes(ctx, keyspace, sourceShards)
		return err
	}

	// We've reached the point of no return. Freeze the tablet control records in the source masters.
	if err := wr.updateFrozenFlag(ctx, sourceShards, true); err != nil {
		wr.cancelMasterMigrateServedTypes(ctx, keyspace, sourceShards)
		return err
	}

	// Phase 2
	// Always setup reverse replication. We'll start it later if reverseReplication was specified.
	// This will allow someone to reverse the replication later if they change their mind.
	if err := wr.setupReverseReplication(ctx, sourceShards, destinationShards); err != nil {
		// It's safe to unfreeze if reverse replication setup fails.
		wr.cancelMasterMigrateServedTypes(ctx, keyspace, sourceShards)
		unfreezeErr := wr.updateFrozenFlag(ctx, sourceShards, false)
		if unfreezeErr != nil {
			wr.Logger().Errorf("Problem recovering for failed reverse replication: %v", unfreezeErr)
		}

		return err
	}

	// Destination shards need different handling than what updateShardRecords does.
	event.DispatchUpdate(ev, "updating destination shards")

	// Enable query service
	err = wr.ts.UpdateDisableQueryService(ctx, keyspace, destinationShards, topodatapb.TabletType_MASTER, nil, false)
	if err != nil {
		return err
	}

	for i, si := range destinationShards {
		ti, err := wr.ts.GetTablet(ctx, si.MasterAlias)
		if err != nil {
			return err
		}
		// Stop VReplication streams.
		for _, sourceShard := range si.SourceShards {
			if _, err := wr.tmc.VReplicationExec(ctx, ti.Tablet, binlogplayer.DeleteVReplication(sourceShard.Uid)); err != nil {
				return err
			}
		}
		// Similar to updateShardRecords, but we also remove SourceShards.
		destinationShards[i], err = wr.ts.UpdateShardFields(ctx, si.Keyspace(), si.ShardName(), func(si *topo.ShardInfo) error {
			si.SourceShards = nil
			si.IsMasterServing = true
			return nil
		})
		if err != nil {
			return err
		}
	}

	event.DispatchUpdate(ev, "setting destination masters read-write")
	if err := wr.refreshMasters(ctx, destinationShards); err != nil {
		return err
	}

	// Update srvKeyspace now
	if err = wr.ts.MigrateServedType(ctx, keyspace, destinationShards, sourceShards, topodatapb.TabletType_MASTER, nil); err != nil {
		return err
	}

	// Make sure that from now on source shards have IsMasterServing set to false
	for _, si := range sourceShards {
		_, err := wr.ts.UpdateShardFields(ctx, si.Keyspace(), si.ShardName(), func(si *topo.ShardInfo) error {
			si.IsMasterServing = false
			return nil
		})
		if err != nil {
			return err
		}
	}

	if reverseReplication {
		if err := wr.startReverseReplication(ctx, sourceShards); err != nil {
			return err
		}
		// We also have to remove the frozen flag as final step.
		if err := wr.updateFrozenFlag(ctx, sourceShards, false); err != nil {
			return err
		}
	}

	event.DispatchUpdate(ev, "finished")
	return nil
}

func (wr *Wrangler) cancelMasterMigrateServedTypes(ctx context.Context, keyspace string, sourceShards []*topo.ShardInfo) {
	wr.Logger().Infof("source shards cancelMasterMigrateServedTypes: %v", sourceShards)
	if err := wr.updateShardRecords(ctx, keyspace, sourceShards, nil, topodatapb.TabletType_MASTER, false, true); err != nil {
		wr.Logger().Errorf2(err, "failed to re-enable source masters")
		return
	}
	if err := wr.refreshMasters(ctx, sourceShards); err != nil {
		wr.Logger().Errorf2(err, "failed to refresh source masters")
	}
}

func (wr *Wrangler) setupReverseReplication(ctx context.Context, sourceShards, destinationShards []*topo.ShardInfo) error {
	// Retrieve master positions of all destinations.
	masterPositions := make([]string, len(destinationShards))
	for i, dest := range destinationShards {
		ti, err := wr.ts.GetTablet(ctx, dest.MasterAlias)
		if err != nil {
			return err
		}

		wr.Logger().Infof("Gathering master position for %v", topoproto.TabletAliasString(dest.MasterAlias))
		masterPositions[i], err = wr.tmc.MasterPosition(ctx, ti.Tablet)
		if err != nil {
			return err
		}
	}

	// Create reverse replication for each source.
	for i, sourceShard := range sourceShards {
		ti, err := wr.ts.GetTablet(ctx, sourceShard.MasterAlias)
		if err != nil {
			return err
		}
		dbName := ti.DbName()
		if len(sourceShard.SourceShards) != 0 {
			continue
		}
		// Handle the case where the source is "unsharded".
		kr := sourceShard.KeyRange
		if kr == nil {
			kr = &topodatapb.KeyRange{}
		}
		// Create replications streams first using the retrieved master positions.
		uids := make([]uint32, len(destinationShards))
		for j, dest := range destinationShards {
			bls := &binlogdatapb.BinlogSource{
				Keyspace: dest.Keyspace(),
				Shard:    dest.ShardName(),
				KeyRange: kr,
			}
			qr, err := wr.VReplicationExec(ctx, sourceShard.MasterAlias, binlogplayer.CreateVReplicationState("ReversedResharding", bls, masterPositions[j], binlogplayer.BlpStopped, dbName))
			if err != nil {
				return err
			}
			uids[j] = uint32(qr.InsertId)
			wr.Logger().Infof("Created reverse replication for tablet %v/%v: %v, db: %v, pos: %v, uid: %v", sourceShard.Keyspace(), sourceShard.ShardName(), bls, dbName, masterPositions[j], uids[j])
		}
		// Source shards have to be atomically added to ensure idempotence.
		// If this fails, there's no harm because the unstarted vreplication streams will just be abandoned.
		sourceShards[i], err = wr.ts.UpdateShardFields(ctx, sourceShard.Keyspace(), sourceShard.ShardName(), func(si *topo.ShardInfo) error {
			for j, dest := range destinationShards {
				si.SourceShards = append(si.SourceShards, &topodatapb.Shard_SourceShard{
					Uid:      uids[j],
					Keyspace: dest.Keyspace(),
					Shard:    dest.ShardName(),
					KeyRange: dest.KeyRange,
				})
			}
			return nil
		})
		if err != nil {
			wr.Logger().Errorf("Unstarted vreplication streams for %v/%v need to be deleted: %v", sourceShard.Keyspace(), sourceShard.ShardName(), uids)
			return fmt.Errorf("failed to setup reverse replication: %v, unstarted vreplication streams for %v/%v need to be deleted: %v", err, sourceShard.Keyspace(), sourceShard.ShardName(), uids)
		}
	}
	return nil
}

func (wr *Wrangler) startReverseReplication(ctx context.Context, sourceShards []*topo.ShardInfo) error {
	for _, sourceShard := range sourceShards {
		for _, dest := range sourceShard.SourceShards {
			wr.Logger().Infof("Starting reverse replication for tablet %v/%v, uid: %v", sourceShard.Keyspace(), sourceShard.ShardName(), dest.Uid)
			_, err := wr.VReplicationExec(ctx, sourceShard.MasterAlias, binlogplayer.StartVReplication(dest.Uid))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// updateShardRecords updates the shard records based on 'from' or 'to' direction.
func (wr *Wrangler) updateShardRecords(ctx context.Context, keyspace string, shards []*topo.ShardInfo, cells []string, servedType topodatapb.TabletType, isFrom bool, clearSourceShards bool) (err error) {
	err = wr.ts.UpdateDisableQueryService(ctx, keyspace, shards, servedType, cells, isFrom /* disable */)
	if err != nil {
		return err
	}

	for i, si := range shards {
		updatedShard, err := wr.ts.UpdateShardFields(ctx, si.Keyspace(), si.ShardName(), func(si *topo.ShardInfo) error {
			if clearSourceShards {
				si.SourceShards = nil
			}
			return nil
		})

		if err != nil {
			return err
		}

		shards[i] = updatedShard

		// For 'to' shards, refresh to make them serve.
		// The 'from' shards will be refreshed after traffic has migrated.
		if !isFrom {
			wr.RefreshTabletsByShard(ctx, si, []topodatapb.TabletType{servedType}, cells)
		}
	}
	return nil
}

// updateFrozenFlag sets or unsets the Frozen flag for master migration. This is performed
// for all master tablet control records.
func (wr *Wrangler) updateFrozenFlag(ctx context.Context, shards []*topo.ShardInfo, value bool) (err error) {
	for i, si := range shards {
		updatedShard, err := wr.ts.UpdateShardFields(ctx, si.Keyspace(), si.ShardName(), func(si *topo.ShardInfo) error {
			tc := si.GetTabletControl(topodatapb.TabletType_MASTER)
			if tc != nil {
				tc.Frozen = value
				return nil
			}
			// This shard does not have a tablet control record, adding one to set frozen flag
			tc = &topodatapb.Shard_TabletControl{
				TabletType: topodatapb.TabletType_MASTER,
				Frozen:     value,
			}
			si.TabletControls = append(si.TabletControls, tc)
			return nil
		})
		if err != nil {
			return err
		}

		shards[i] = updatedShard
	}
	return nil
}

// WaitForDrain blocks until the selected tablets (cells/keyspace/shard/tablet_type)
// have reported a QPS rate of 0.0.
// NOTE: This is just an observation of one point in time and no guarantee that
// the tablet was actually drained. At later times, a QPS rate > 0.0 could still
// be observed.
func (wr *Wrangler) WaitForDrain(ctx context.Context, cells []string, keyspace, shard string, servedType topodatapb.TabletType,
	retryDelay, healthCheckTopologyRefresh, healthcheckRetryDelay, healthCheckTimeout, initialWait time.Duration) error {
	var err error
	if len(cells) == 0 {
		// Retrieve list of cells for the shard from the topology.
		cells, err = wr.ts.GetCellInfoNames(ctx)
		if err != nil {
			return fmt.Errorf("failed to retrieve list of all cells. GetCellInfoNames() failed: %v", err)
		}
	}

	// Check all cells in parallel.
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			rec.RecordError(wr.waitForDrainInCell(ctx, cell, keyspace, shard, servedType,
				retryDelay, healthCheckTopologyRefresh, healthcheckRetryDelay, healthCheckTimeout, initialWait))
		}(cell)
	}
	wg.Wait()

	return rec.Error()
}

func (wr *Wrangler) waitForDrainInCell(ctx context.Context, cell, keyspace, shard string, servedType topodatapb.TabletType,
	retryDelay, healthCheckTopologyRefresh, healthcheckRetryDelay, healthCheckTimeout, initialWait time.Duration) error {

	// Create the healthheck module, with a cache.
	hc := discovery.NewHealthCheck(healthcheckRetryDelay, healthCheckTimeout)
	defer hc.Close()
	tsc := discovery.NewTabletStatsCache(hc, wr.TopoServer(), cell)

	// Create a tablet watcher.
	watcher := discovery.NewShardReplicationWatcher(ctx, wr.TopoServer(), hc, cell, keyspace, shard, healthCheckTopologyRefresh, discovery.DefaultTopoReadConcurrency)
	defer watcher.Stop()

	// Wait for at least one tablet.
	if err := tsc.WaitForTablets(ctx, cell, keyspace, shard, servedType); err != nil {
		return fmt.Errorf("%v: error waiting for initial %v tablets for %v/%v: %v", cell, servedType, keyspace, shard, err)
	}

	wr.Logger().Infof("%v: Waiting for %.1f seconds to make sure that the discovery module retrieves healthcheck information from all tablets.",
		cell, initialWait.Seconds())
	// Wait at least for -initial_wait to elapse to make sure that we
	// see all healthy tablets. Otherwise, we might miss some tablets.
	// Note the default value for the parameter is set to the same
	// default as healthcheck timeout, and it's safe to wait not
	// longer for this because we would only miss slow tablets and
	// vtgate would not serve from such tablets anyway.
	time.Sleep(initialWait)

	// Now check the QPS rate of all tablets until the timeout expires.
	startTime := time.Now()
	for {
		// map key: tablet uid
		drainedHealthyTablets := make(map[uint32]*discovery.TabletStats)
		notDrainedHealtyTablets := make(map[uint32]*discovery.TabletStats)

		healthyTablets := tsc.GetHealthyTabletStats(keyspace, shard, servedType)
		for _, ts := range healthyTablets {
			if ts.Stats.Qps == 0.0 {
				drainedHealthyTablets[ts.Tablet.Alias.Uid] = &ts
			} else {
				notDrainedHealtyTablets[ts.Tablet.Alias.Uid] = &ts
			}
		}

		if len(drainedHealthyTablets) == len(healthyTablets) {
			wr.Logger().Infof("%v: All %d healthy tablets were drained after %.1f seconds (not counting %.1f seconds for the initial wait).",
				cell, len(healthyTablets), time.Since(startTime).Seconds(), healthCheckTimeout.Seconds())
			break
		}

		// Continue waiting, sleep in between.
		deadlineString := ""
		if d, ok := ctx.Deadline(); ok {
			deadlineString = fmt.Sprintf(" up to %.1f more seconds", time.Until(d).Seconds())
		}
		wr.Logger().Infof("%v: Waiting%v for all healthy tablets to be drained (%d/%d done).",
			cell, deadlineString, len(drainedHealthyTablets), len(healthyTablets))

		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()

			var l []string
			for _, ts := range notDrainedHealtyTablets {
				l = append(l, formatTabletStats(ts))
			}
			return fmt.Errorf("%v: WaitForDrain failed for %v tablets in %v/%v. Only %d/%d tablets were drained. err: %v List of tablets which were not drained: %v",
				cell, servedType, keyspace, shard, len(drainedHealthyTablets), len(healthyTablets), ctx.Err(), strings.Join(l, ";"))
		case <-timer.C:
		}
	}

	return nil
}

func formatTabletStats(ts *discovery.TabletStats) string {
	webURL := "unknown http port"
	if webPort, ok := ts.Tablet.PortMap["vt"]; ok {
		webURL = fmt.Sprintf("http://%v:%d/", ts.Tablet.Hostname, webPort)
	}
	return fmt.Sprintf("%v: %v stats: %v", topoproto.TabletAliasString(ts.Tablet.Alias), webURL, ts.Stats)
}

func (wr *Wrangler) showVerticalResharding(ctx context.Context, keyspace, shard string) error {
	ki, err := wr.ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}
	destinationShard, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	if len(destinationShard.SourceShards) != 1 || len(destinationShard.SourceShards[0].Tables) == 0 {
		wr.Logger().Printf("No resharding in progress\n")
		return nil
	}
	sourceShard, err := wr.ts.GetShard(ctx, destinationShard.SourceShards[0].Keyspace, destinationShard.SourceShards[0].Shard)
	if err != nil {
		return err
	}
	wr.Logger().Printf("Vertical Resharding:\n")
	wr.Logger().Printf("  Served From: %v\n", ki.ServedFroms)
	wr.Logger().Printf("  Source:\n")
	if err := wr.printShards(ctx, []*topo.ShardInfo{sourceShard}); err != nil {
		return err
	}
	wr.Logger().Printf("  Destination:\n")
	return wr.printShards(ctx, []*topo.ShardInfo{destinationShard})
}

func (wr *Wrangler) cancelVerticalResharding(ctx context.Context, keyspace, shard string) error {
	wr.Logger().Infof("Cancel vertical resharding in keyspace %v", keyspace)
	destinationShard, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	if len(destinationShard.SourceShards) != 1 || len(destinationShard.SourceShards[0].Tables) == 0 {
		return fmt.Errorf("destination shard %v/%v is not a vertical split target", keyspace, shard)
	}
	sourceShard, err := wr.ts.GetShard(ctx, destinationShard.SourceShards[0].Keyspace, destinationShard.SourceShards[0].Shard)
	if err != nil {
		return err
	}
	if len(sourceShard.TabletControls) != 0 {
		return fmt.Errorf("some served types have migrated for %v/%v, please undo them before canceling", keyspace, shard)
	}
	destinationMasterTabletInfo, err := wr.ts.GetTablet(ctx, destinationShard.MasterAlias)
	if err != nil {
		return err
	}
	if _, err := wr.tmc.VReplicationExec(ctx, destinationMasterTabletInfo.Tablet, binlogplayer.DeleteVReplication(destinationShard.SourceShards[0].Uid)); err != nil {
		return err
	}
	if _, err = wr.ts.UpdateShardFields(ctx, destinationShard.Keyspace(), destinationShard.ShardName(), func(si *topo.ShardInfo) error {
		si.SourceShards = nil
		return nil
	}); err != nil {
		return err
	}
	// set destination master back to serving
	return wr.refreshMasters(ctx, []*topo.ShardInfo{destinationShard})
}

// MigrateServedFrom is used during vertical splits to migrate a
// served type from a keyspace to another.
func (wr *Wrangler) MigrateServedFrom(ctx context.Context, keyspace, shard string, servedType topodatapb.TabletType, cells []string, reverse bool, filteredReplicationWaitTime time.Duration) (err error) {
	// read the destination keyspace, check it
	ki, err := wr.ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}
	if len(ki.ServedFroms) == 0 {
		return fmt.Errorf("destination keyspace %v is not a vertical split target", keyspace)
	}

	// read the destination shard, check it
	si, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	if len(si.SourceShards) != 1 || len(si.SourceShards[0].Tables) == 0 {
		return fmt.Errorf("destination shard %v/%v is not a vertical split target", keyspace, shard)
	}

	// check the migration is valid before locking (will also be checked
	// after locking to be sure)
	sourceKeyspace := si.SourceShards[0].Keyspace
	if err := ki.CheckServedFromMigration(servedType, cells, sourceKeyspace, !reverse); err != nil {
		return err
	}

	// lock the keyspaces, source first.
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, sourceKeyspace, fmt.Sprintf("MigrateServedFrom(%v)", servedType))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)
	ctx, unlock, lockErr = wr.ts.LockKeyspace(ctx, keyspace, fmt.Sprintf("MigrateServedFrom(%v)", servedType))
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// execute the migration
	err = wr.migrateServedFromLocked(ctx, ki, si, servedType, cells, reverse, filteredReplicationWaitTime)

	// rebuild the keyspace serving graph if there was no error
	if err == nil {
		err = topotools.RebuildKeyspaceLocked(ctx, wr.logger, wr.ts, keyspace, cells)
	}

	return err
}

func (wr *Wrangler) migrateServedFromLocked(ctx context.Context, ki *topo.KeyspaceInfo, destinationShard *topo.ShardInfo, servedType topodatapb.TabletType, cells []string, reverse bool, filteredReplicationWaitTime time.Duration) (err error) {

	// re-read and update keyspace info record
	ki, err = wr.ts.GetKeyspace(ctx, ki.KeyspaceName())
	if err != nil {
		return err
	}
	if reverse {
		ki.UpdateServedFromMap(servedType, cells, destinationShard.SourceShards[0].Keyspace, false, nil)
	} else {
		destinationShardcells, err := wr.ts.GetShardServingCells(ctx, destinationShard)
		if err != nil {
			return err
		}
		ki.UpdateServedFromMap(servedType, cells, destinationShard.SourceShards[0].Keyspace, true, destinationShardcells)
	}

	// re-read and check the destination shard
	destinationShard, err = wr.ts.GetShard(ctx, destinationShard.Keyspace(), destinationShard.ShardName())
	if err != nil {
		return err
	}
	if len(destinationShard.SourceShards) != 1 {
		return fmt.Errorf("destination shard %v/%v is not a vertical split target", destinationShard.Keyspace(), destinationShard.ShardName())
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
	if _, err := wr.ts.UpdateShardFields(ctx, sourceShard.Keyspace(), sourceShard.ShardName(), func(si *topo.ShardInfo) error {
		return si.UpdateSourceBlacklistedTables(ctx, servedType, cells, reverse, tables)
	}); err != nil {
		return err
	}

	// Now refresh the source servers so they reload their
	// blacklisted table list
	event.DispatchUpdate(ev, "refreshing sources tablets state so they update their blacklisted tables")
	return wr.RefreshTabletsByShard(ctx, sourceShard, []topodatapb.TabletType{servedType}, cells)
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
	ctx, cancel := context.WithTimeout(ctx, filteredReplicationWaitTime)
	defer cancel()
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
	if _, err := wr.ts.UpdateShardFields(ctx, sourceShard.Keyspace(), sourceShard.ShardName(), func(si *topo.ShardInfo) error {
		return si.UpdateSourceBlacklistedTables(ctx, topodatapb.TabletType_MASTER, nil, false, tables)
	}); err != nil {
		return err
	}

	// Now refresh the blacklisted table list on the source master
	event.DispatchUpdate(ev, "refreshing source master so it updates its blacklisted tables")
	if err := wr.tmc.RefreshState(ctx, sourceMasterTabletInfo.Tablet); err != nil {
		return err
	}

	// get the position
	event.DispatchUpdate(ev, "getting master position")
	masterPosition, err := wr.tmc.MasterPosition(ctx, sourceMasterTabletInfo.Tablet)
	if err != nil {
		return err
	}

	// wait for it
	event.DispatchUpdate(ev, "waiting for destination master to catch up to source master")
	uid := destinationShard.SourceShards[0].Uid
	if err := wr.tmc.VReplicationWaitForPos(ctx, destinationMasterTabletInfo.Tablet, int(uid), masterPosition); err != nil {
		return err
	}

	// Stop the VReplication stream.
	event.DispatchUpdate(ev, "stopping vreplication")
	if _, err := wr.tmc.VReplicationExec(ctx, destinationMasterTabletInfo.Tablet, binlogplayer.DeleteVReplication(uid)); err != nil {
		return err
	}

	// Update the destination keyspace (its ServedFrom has changed)
	event.DispatchUpdate(ev, "updating keyspace")
	if err = wr.ts.UpdateKeyspace(ctx, ki); err != nil {
		return err
	}

	// Update the destination shard (no more source shard)
	event.DispatchUpdate(ev, "updating destination shard")
	destinationShard, err = wr.ts.UpdateShardFields(ctx, destinationShard.Keyspace(), destinationShard.ShardName(), func(si *topo.ShardInfo) error {
		if len(si.SourceShards) != 1 {
			return fmt.Errorf("unexpected concurrent access for destination shard %v/%v SourceShards array", si.Keyspace(), si.ShardName())
		}
		si.SourceShards = nil
		return nil
	})
	if err != nil {
		return err
	}

	// Tell the new shards masters they can now be read-write.
	// Invoking a remote action will also make the tablet stop filtered
	// replication.
	event.DispatchUpdate(ev, "setting destination shard masters read-write")
	return wr.refreshMasters(ctx, []*topo.ShardInfo{destinationShard})
}

// SetKeyspaceServedFrom locks a keyspace and changes its ServerFromMap
func (wr *Wrangler) SetKeyspaceServedFrom(ctx context.Context, keyspace string, servedType topodatapb.TabletType, cells []string, sourceKeyspace string, remove bool) (err error) {
	// Lock the keyspace
	ctx, unlock, lockErr := wr.ts.LockKeyspace(ctx, keyspace, "SetKeyspaceServedFrom")
	if lockErr != nil {
		return lockErr
	}
	defer unlock(&err)

	// and update it
	ki, err := wr.ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return err
	}
	if err := ki.UpdateServedFromMap(servedType, cells, sourceKeyspace, remove, nil); err != nil {
		return err
	}
	return wr.ts.UpdateKeyspace(ctx, ki)
}

// RefreshTabletsByShard calls RefreshState on all the tables of a
// given type in a shard. It would work for the master, but the
// discovery wouldn't be very efficient.
func (wr *Wrangler) RefreshTabletsByShard(ctx context.Context, si *topo.ShardInfo, tabletTypes []topodatapb.TabletType, cells []string) error {
	wr.Logger().Infof("RefreshTabletsByShard called on shard %v/%v", si.Keyspace(), si.ShardName())
	tabletMap, err := wr.ts.GetTabletMapForShardByCell(ctx, si.Keyspace(), si.ShardName(), cells)
	switch {
	case err == nil:
		// keep going
	case topo.IsErrType(err, topo.PartialResult):
		wr.Logger().Warningf("RefreshTabletsByShard: got partial result for shard %v/%v, may not refresh all tablets everywhere", si.Keyspace(), si.ShardName())
	default:
		return err
	}

	// ignore errors in this phase
	wg := sync.WaitGroup{}
	for _, ti := range tabletMap {
		if tabletTypes != nil && !topoproto.IsTypeInList(ti.Type, tabletTypes) {
			continue
		}
		if ti.Hostname == "" {
			// The tablet is not running, we don't have the host
			// name to connect to, so we just skip this tablet.
			wr.Logger().Infof("Tablet %v has no hostname, skipping its RefreshState", ti.AliasString())
			continue
		}

		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			wr.Logger().Infof("Calling RefreshState on tablet %v", ti.AliasString())
			// Setting an upper bound timeout to fail faster in case of an error.
			// Using 60 seconds because RefreshState should not take more than 30 seconds.
			// (RefreshState will restart the tablet's QueryService and most time will be spent on the shutdown, i.e. waiting up to 30 seconds on transactions (see Config.TransactionTimeout)).
			ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			if err := wr.tmc.RefreshState(ctx, ti.Tablet); err != nil {
				wr.Logger().Warningf("RefreshTabletsByShard: failed to refresh %v: %v", ti.AliasString(), err)
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
			if err := wr.DeleteShard(ctx, keyspace, shard, true /* recursive */, true /* evenIfServing */); err != nil && !topo.IsErrType(err, topo.NoNode) {
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
		if err := wr.ts.DeleteKeyspaceReplication(ctx, cell, keyspace); err != nil && !topo.IsErrType(err, topo.NoNode) {
			wr.Logger().Warningf("Cannot delete KeyspaceReplication in cell %v for %v: %v", cell, keyspace, err)
		}

		if err := wr.ts.DeleteSrvKeyspace(ctx, cell, keyspace); err != nil && !topo.IsErrType(err, topo.NoNode) {
			wr.Logger().Warningf("Cannot delete SrvKeyspace in cell %v for %v: %v", cell, keyspace, err)
		}
	}

	return wr.ts.DeleteKeyspace(ctx, keyspace)
}

// RemoveKeyspaceCell will remove a cell from the Cells list in all
// shards of a keyspace (by calling RemoveShardCell on every
// shard). It will also remove the SrvKeyspace for that keyspace/cell.
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

	// Now remove the SrvKeyspace object.
	wr.Logger().Infof("Removing cell %v keyspace %v SrvKeyspace object", cell, keyspace)
	return wr.ts.DeleteSrvKeyspace(ctx, cell, keyspace)
}

func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}
