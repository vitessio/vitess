// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"html/template"
	"sync"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sync2"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

// SplitDiffWorker executes a diff between a destination shard and its
// source shards in a shard split case.
type SplitDiffWorker struct {
	StatusWorker

	wr            *wrangler.Wrangler
	cell          string
	keyspace      string
	shard         string
	excludeTables []string
	cleaner       *wrangler.Cleaner

	// all subsequent fields are protected by the mutex

	// populated during WorkerStateInit, read-only after that
	keyspaceInfo *topo.KeyspaceInfo
	shardInfo    *topo.ShardInfo

	// populated during WorkerStateFindTargets, read-only after that
	sourceAliases    []topo.TabletAlias
	destinationAlias topo.TabletAlias

	// populated during WorkerStateDiff
	sourceSchemaDefinitions     []*myproto.SchemaDefinition
	destinationSchemaDefinition *myproto.SchemaDefinition
}

// NewSplitDiffWorker returns a new SplitDiffWorker object.
func NewSplitDiffWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, excludeTables []string) Worker {
	return &SplitDiffWorker{
		StatusWorker:  NewStatusWorker(),
		wr:            wr,
		cell:          cell,
		keyspace:      keyspace,
		shard:         shard,
		excludeTables: excludeTables,
		cleaner:       &wrangler.Cleaner{},
	}
}

// StatusAsHTML is part of the Worker interface
func (sdw *SplitDiffWorker) StatusAsHTML() template.HTML {
	sdw.Mu.Lock()
	defer sdw.Mu.Unlock()
	result := "<b>Working on:</b> " + sdw.keyspace + "/" + sdw.shard + "</br>\n"
	result += "<b>State:</b> " + sdw.State.String() + "</br>\n"
	switch sdw.State {
	case WorkerStateDiff:
		result += "<b>Running...</b></br>\n"
	case WorkerStateDone:
		result += "<b>Success.</b></br>\n"
	}

	return template.HTML(result)
}

// StatusAsText is part of the Worker interface
func (sdw *SplitDiffWorker) StatusAsText() string {
	sdw.Mu.Lock()
	defer sdw.Mu.Unlock()
	result := "Working on: " + sdw.keyspace + "/" + sdw.shard + "\n"
	result += "State: " + sdw.State.String() + "\n"
	switch sdw.State {
	case WorkerStateDiff:
		result += "Running...\n"
	case WorkerStateDone:
		result += "Success.\n"
	}
	return result
}

// Run is mostly a wrapper to run the cleanup at the end.
func (sdw *SplitDiffWorker) Run(ctx context.Context) error {
	resetVars()
	err := sdw.run(ctx)

	sdw.SetState(WorkerStateCleanUp)
	cerr := sdw.cleaner.CleanUp(sdw.wr)
	if cerr != nil {
		if err != nil {
			sdw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}
	if err != nil {
		sdw.SetState(WorkerStateError)
		return err
	}
	sdw.SetState(WorkerStateDone)
	return nil
}

func (sdw *SplitDiffWorker) run(ctx context.Context) error {
	// first state: read what we need to do
	if err := sdw.init(ctx); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// second state: find targets
	if err := sdw.findTargets(ctx); err != nil {
		return fmt.Errorf("findTargets() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// third phase: synchronize replication
	if err := sdw.synchronizeReplication(ctx); err != nil {
		return fmt.Errorf("synchronizeReplication() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// fourth phase: diff
	if err := sdw.diff(ctx); err != nil {
		return fmt.Errorf("diff() failed: %v", err)
	}

	return nil
}

// init phase:
// - read the shard info, make sure it has sources
func (sdw *SplitDiffWorker) init(ctx context.Context) error {
	sdw.SetState(WorkerStateInit)

	var err error
	sdw.keyspaceInfo, err = sdw.wr.TopoServer().GetKeyspace(ctx, sdw.keyspace)
	if err != nil {
		return fmt.Errorf("cannot read keyspace %v: %v", sdw.keyspace, err)
	}
	sdw.shardInfo, err = sdw.wr.TopoServer().GetShard(ctx, sdw.keyspace, sdw.shard)
	if err != nil {
		return fmt.Errorf("cannot read shard %v/%v: %v", sdw.keyspace, sdw.shard, err)
	}

	if len(sdw.shardInfo.SourceShards) == 0 {
		return fmt.Errorf("shard %v/%v has no source shard", sdw.keyspace, sdw.shard)
	}
	if topo.TabletAliasIsZero(sdw.shardInfo.MasterAlias) {
		return fmt.Errorf("shard %v/%v has no master", sdw.keyspace, sdw.shard)
	}

	return nil
}

// findTargets phase:
// - find one rdonly per source shard
// - find one rdonly in destination shard
// - mark them all as 'worker' pointing back to us
func (sdw *SplitDiffWorker) findTargets(ctx context.Context) error {
	sdw.SetState(WorkerStateFindTargets)

	// find an appropriate endpoint in destination shard
	var err error
	sdw.destinationAlias, err = FindWorkerTablet(ctx, sdw.wr, sdw.cleaner, sdw.cell, sdw.keyspace, sdw.shard)
	if err != nil {
		return fmt.Errorf("FindWorkerTablet() failed for %v/%v/%v: %v", sdw.cell, sdw.keyspace, sdw.shard, err)
	}

	// find an appropriate endpoint in the source shards
	sdw.sourceAliases = make([]topo.TabletAlias, len(sdw.shardInfo.SourceShards))
	for i, ss := range sdw.shardInfo.SourceShards {
		sdw.sourceAliases[i], err = FindWorkerTablet(ctx, sdw.wr, sdw.cleaner, sdw.cell, sdw.keyspace, ss.Shard)
		if err != nil {
			return fmt.Errorf("FindWorkerTablet() failed for %v/%v/%v: %v", sdw.cell, sdw.keyspace, ss.Shard, err)
		}
	}

	return nil
}

// synchronizeReplication phase:
// 1 - ask the master of the destination shard to pause filtered replication,
//   and return the source binlog positions
//   (add a cleanup task to restart filtered replication on master)
// 2 - stop all the source tablets at a binlog position higher than the
//   destination master. Get that new list of positions.
//   (add a cleanup task to restart binlog replication on them, and change
//    the existing ChangeSlaveType cleanup action to 'spare' type)
// 3 - ask the master of the destination shard to resume filtered replication
//   up to the new list of positions, and return its binlog position.
// 4 - wait until the destination tablet is equal or passed that master
//   binlog position, and stop its replication.
//   (add a cleanup task to restart binlog replication on it, and change
//    the existing ChangeSlaveType cleanup action to 'spare' type)
// 5 - restart filtered replication on destination master.
//   (remove the cleanup task that does the same)
// At this point, all source and destination tablets are stopped at the same point.

func (sdw *SplitDiffWorker) synchronizeReplication(ctx context.Context) error {
	sdw.SetState(WorkerStateSyncReplication)

	masterInfo, err := sdw.wr.TopoServer().GetTablet(ctx, topo.ProtoToTabletAlias(sdw.shardInfo.MasterAlias))
	if err != nil {
		return fmt.Errorf("synchronizeReplication: cannot get Tablet record for master %v: %v", sdw.shardInfo.MasterAlias, err)
	}

	// 1 - stop the master binlog replication, get its current position
	sdw.wr.Logger().Infof("Stopping master binlog replication on %v", sdw.shardInfo.MasterAlias)
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	blpPositionList, err := sdw.wr.TabletManagerClient().StopBlp(shortCtx, masterInfo)
	cancel()
	if err != nil {
		return fmt.Errorf("StopBlp for %v failed: %v", sdw.shardInfo.MasterAlias, err)
	}
	wrangler.RecordStartBlpAction(sdw.cleaner, masterInfo)

	// 2 - stop all the source tablets at a binlog position
	//     higher than the destination master
	stopPositionList := blproto.BlpPositionList{
		Entries: make([]blproto.BlpPosition, len(sdw.shardInfo.SourceShards)),
	}
	for i, ss := range sdw.shardInfo.SourceShards {
		// find where we should be stopping
		blpPos, err := blpPositionList.FindBlpPositionById(ss.Uid)
		if err != nil {
			return fmt.Errorf("no binlog position on the master for Uid %v", ss.Uid)
		}

		// read the tablet
		sourceTablet, err := sdw.wr.TopoServer().GetTablet(ctx, sdw.sourceAliases[i])
		if err != nil {
			return err
		}

		// stop replication
		sdw.wr.Logger().Infof("Stopping slave[%v] %v at a minimum of %v", i, sdw.sourceAliases[i], blpPos.Position)
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		stoppedAt, err := sdw.wr.TabletManagerClient().StopSlaveMinimum(shortCtx, sourceTablet, blpPos.Position, *remoteActionsTimeout)
		cancel()
		if err != nil {
			return fmt.Errorf("cannot stop slave %v at right binlog position %v: %v", sdw.sourceAliases[i], blpPos.Position, err)
		}
		stopPositionList.Entries[i].Uid = ss.Uid
		stopPositionList.Entries[i].Position = stoppedAt

		// change the cleaner actions from ChangeSlaveType(rdonly)
		// to StartSlave() + ChangeSlaveType(spare)
		wrangler.RecordStartSlaveAction(sdw.cleaner, sourceTablet)
		action, err := wrangler.FindChangeSlaveTypeActionByTarget(sdw.cleaner, sdw.sourceAliases[i])
		if err != nil {
			return fmt.Errorf("cannot find ChangeSlaveType action for %v: %v", sdw.sourceAliases[i], err)
		}
		action.TabletType = topo.TYPE_SPARE
	}

	// 3 - ask the master of the destination shard to resume filtered
	//     replication up to the new list of positions
	sdw.wr.Logger().Infof("Restarting master %v until it catches up to %v", sdw.shardInfo.MasterAlias, stopPositionList)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	masterPos, err := sdw.wr.TabletManagerClient().RunBlpUntil(shortCtx, masterInfo, &stopPositionList, *remoteActionsTimeout)
	cancel()
	if err != nil {
		return fmt.Errorf("RunBlpUntil for %v until %v failed: %v", sdw.shardInfo.MasterAlias, stopPositionList, err)
	}

	// 4 - wait until the destination tablet is equal or passed
	//     that master binlog position, and stop its replication.
	sdw.wr.Logger().Infof("Waiting for destination tablet %v to catch up to %v", sdw.destinationAlias, masterPos)
	destinationTablet, err := sdw.wr.TopoServer().GetTablet(ctx, sdw.destinationAlias)
	if err != nil {
		return err
	}
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	_, err = sdw.wr.TabletManagerClient().StopSlaveMinimum(shortCtx, destinationTablet, masterPos, *remoteActionsTimeout)
	cancel()
	if err != nil {
		return fmt.Errorf("StopSlaveMinimum for %v at %v failed: %v", sdw.destinationAlias, masterPos, err)
	}
	wrangler.RecordStartSlaveAction(sdw.cleaner, destinationTablet)
	action, err := wrangler.FindChangeSlaveTypeActionByTarget(sdw.cleaner, sdw.destinationAlias)
	if err != nil {
		return fmt.Errorf("cannot find ChangeSlaveType action for %v: %v", sdw.destinationAlias, err)
	}
	action.TabletType = topo.TYPE_SPARE

	// 5 - restart filtered replication on destination master
	sdw.wr.Logger().Infof("Restarting filtered replication on master %v", sdw.shardInfo.MasterAlias)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	err = sdw.wr.TabletManagerClient().StartBlp(shortCtx, masterInfo)
	if err := sdw.cleaner.RemoveActionByName(wrangler.StartBlpActionName, sdw.shardInfo.MasterAlias.String()); err != nil {
		sdw.wr.Logger().Warningf("Cannot find cleaning action %v/%v: %v", wrangler.StartBlpActionName, sdw.shardInfo.MasterAlias.String(), err)
	}
	cancel()
	if err != nil {
		return fmt.Errorf("StartBlp failed for %v: %v", sdw.shardInfo.MasterAlias, err)
	}

	return nil
}

// diff phase: will log messages regarding the diff.
// - get the schema on all tablets
// - if some table schema mismatches, record them (use existing schema diff tools).
// - for each table in destination, run a diff pipeline.

func (sdw *SplitDiffWorker) diff(ctx context.Context) error {
	sdw.SetState(WorkerStateDiff)

	sdw.wr.Logger().Infof("Gathering schema information...")
	sdw.sourceSchemaDefinitions = make([]*myproto.SchemaDefinition, len(sdw.sourceAliases))
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	wg.Add(1)
	go func() {
		var err error
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		sdw.destinationSchemaDefinition, err = sdw.wr.GetSchema(
			shortCtx, sdw.destinationAlias, nil /* tables */, sdw.excludeTables, false /* includeViews */)
		cancel()
		rec.RecordError(err)
		sdw.wr.Logger().Infof("Got schema from destination %v", sdw.destinationAlias)
		wg.Done()
	}()
	for i, sourceAlias := range sdw.sourceAliases {
		wg.Add(1)
		go func(i int, sourceAlias topo.TabletAlias) {
			var err error
			shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
			sdw.sourceSchemaDefinitions[i], err = sdw.wr.GetSchema(
				shortCtx, sourceAlias, nil /* tables */, sdw.excludeTables, false /* includeViews */)
			cancel()
			rec.RecordError(err)
			sdw.wr.Logger().Infof("Got schema from source[%v] %v", i, sourceAlias)
			wg.Done()
		}(i, sourceAlias)
	}
	wg.Wait()
	if rec.HasErrors() {
		return rec.Error()
	}

	// TODO(alainjobart) Checking against each source may be
	// overkill, if all sources have the same schema?
	sdw.wr.Logger().Infof("Diffing the schema...")
	rec = concurrency.AllErrorRecorder{}
	for i, sourceSchemaDefinition := range sdw.sourceSchemaDefinitions {
		sourceName := fmt.Sprintf("source[%v]", i)
		myproto.DiffSchema("destination", sdw.destinationSchemaDefinition, sourceName, sourceSchemaDefinition, &rec)
	}
	if rec.HasErrors() {
		sdw.wr.Logger().Warningf("Different schemas: %v", rec.Error().Error())
	} else {
		sdw.wr.Logger().Infof("Schema match, good.")
	}

	// run the diffs, 8 at a time
	sdw.wr.Logger().Infof("Running the diffs...")
	sem := sync2.NewSemaphore(8, 0)
	for _, tableDefinition := range sdw.destinationSchemaDefinition.TableDefinitions {
		wg.Add(1)
		go func(tableDefinition *myproto.TableDefinition) {
			defer wg.Done()
			sem.Acquire()
			defer sem.Release()

			sdw.wr.Logger().Infof("Starting the diff on table %v", tableDefinition.Name)
			if len(sdw.sourceAliases) != 1 {
				err := fmt.Errorf("Don't support more than one source for table yet: %v", tableDefinition.Name)
				rec.RecordError(err)
				sdw.wr.Logger().Errorf(err.Error())
				return
			}

			overlap, err := key.KeyRangesOverlap3(sdw.shardInfo.KeyRange, sdw.shardInfo.SourceShards[0].KeyRange)
			if err != nil {
				newErr := fmt.Errorf("Source shard doesn't overlap with destination????: %v", err)
				rec.RecordError(newErr)
				sdw.wr.Logger().Errorf(newErr.Error())
				return
			}
			sourceQueryResultReader, err := TableScanByKeyRange(ctx, sdw.wr.Logger(), sdw.wr.TopoServer(), sdw.sourceAliases[0], tableDefinition, overlap, key.ProtoToKeyspaceIdType(sdw.keyspaceInfo.ShardingColumnType))
			if err != nil {
				newErr := fmt.Errorf("TableScanByKeyRange(source) failed: %v", err)
				rec.RecordError(newErr)
				sdw.wr.Logger().Errorf(newErr.Error())
				return
			}
			defer sourceQueryResultReader.Close()

			destinationQueryResultReader, err := TableScanByKeyRange(ctx, sdw.wr.Logger(), sdw.wr.TopoServer(), sdw.destinationAlias, tableDefinition, nil, key.ProtoToKeyspaceIdType(sdw.keyspaceInfo.ShardingColumnType))
			if err != nil {
				newErr := fmt.Errorf("TableScanByKeyRange(destination) failed: %v", err)
				rec.RecordError(newErr)
				sdw.wr.Logger().Errorf(newErr.Error())
				return
			}
			defer destinationQueryResultReader.Close()

			differ, err := NewRowDiffer(sourceQueryResultReader, destinationQueryResultReader, tableDefinition)
			if err != nil {
				newErr := fmt.Errorf("NewRowDiffer() failed: %v", err)
				rec.RecordError(newErr)
				sdw.wr.Logger().Errorf(newErr.Error())
				return
			}

			report, err := differ.Go(sdw.wr.Logger())
			if err != nil {
				newErr := fmt.Errorf("Differ.Go failed: %v", err.Error())
				rec.RecordError(newErr)
				sdw.wr.Logger().Errorf(newErr.Error())
			} else {
				if report.HasDifferences() {
					err := fmt.Errorf("Table %v has differences: %v", tableDefinition.Name, report.String())
					rec.RecordError(err)
					sdw.wr.Logger().Warningf(err.Error())
				} else {
					sdw.wr.Logger().Infof("Table %v checks out (%v rows processed, %v qps)", tableDefinition.Name, report.processedRows, report.processingQPS)
				}
			}
		}(tableDefinition)
	}
	wg.Wait()

	return rec.Error()
}
