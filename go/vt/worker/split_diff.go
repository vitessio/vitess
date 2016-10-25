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
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/wrangler"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// SplitDiffWorker executes a diff between a destination shard and its
// source shards in a shard split case.
type SplitDiffWorker struct {
	StatusWorker

	wr                      *wrangler.Wrangler
	cell                    string
	keyspace                string
	shard                   string
	sourceUID               uint32
	excludeTables           []string
	minHealthyRdonlyTablets int
	cleaner                 *wrangler.Cleaner

	// populated during WorkerStateInit, read-only after that
	keyspaceInfo *topo.KeyspaceInfo
	shardInfo    *topo.ShardInfo

	// populated during WorkerStateFindTargets, read-only after that
	sourceAlias      *topodatapb.TabletAlias
	destinationAlias *topodatapb.TabletAlias

	// populated during WorkerStateDiff
	sourceSchemaDefinition      *tabletmanagerdatapb.SchemaDefinition
	destinationSchemaDefinition *tabletmanagerdatapb.SchemaDefinition
}

// NewSplitDiffWorker returns a new SplitDiffWorker object.
func NewSplitDiffWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, sourceUID uint32, excludeTables []string, minHealthyRdonlyTablets int) Worker {
	return &SplitDiffWorker{
		StatusWorker:            NewStatusWorker(),
		wr:                      wr,
		cell:                    cell,
		keyspace:                keyspace,
		shard:                   shard,
		sourceUID:               sourceUID,
		excludeTables:           excludeTables,
		minHealthyRdonlyTablets: minHealthyRdonlyTablets,
		cleaner:                 &wrangler.Cleaner{},
	}
}

// StatusAsHTML is part of the Worker interface
func (sdw *SplitDiffWorker) StatusAsHTML() template.HTML {
	state := sdw.State()

	result := "<b>Working on:</b> " + sdw.keyspace + "/" + sdw.shard + "</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateDiff:
		result += "<b>Running...</b></br>\n"
	case WorkerStateDone:
		result += "<b>Success.</b></br>\n"
	}

	return template.HTML(result)
}

// StatusAsText is part of the Worker interface
func (sdw *SplitDiffWorker) StatusAsText() string {
	state := sdw.State()

	result := "Working on: " + sdw.keyspace + "/" + sdw.shard + "\n"
	result += "State: " + state.String() + "\n"
	switch state {
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
	if err := checkDone(ctx); err != nil {
		return err
	}

	return nil
}

// init phase:
// - read the shard info, make sure it has sources
func (sdw *SplitDiffWorker) init(ctx context.Context) error {
	sdw.SetState(WorkerStateInit)

	var err error
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	sdw.keyspaceInfo, err = sdw.wr.TopoServer().GetKeyspace(shortCtx, sdw.keyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read keyspace %v: %v", sdw.keyspace, err)
	}
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	sdw.shardInfo, err = sdw.wr.TopoServer().GetShard(shortCtx, sdw.keyspace, sdw.shard)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read shard %v/%v: %v", sdw.keyspace, sdw.shard, err)
	}

	if len(sdw.shardInfo.SourceShards) == 0 {
		return fmt.Errorf("shard %v/%v has no source shard", sdw.keyspace, sdw.shard)
	}
	foundSourceShard := false
	for _, ss := range sdw.shardInfo.SourceShards {
		if ss.Uid == sdw.sourceUID {
			foundSourceShard = true
		}
	}
	if !foundSourceShard {
		return fmt.Errorf("shard %v/%v has no source shard with UID %v", sdw.keyspace, sdw.shard, sdw.sourceUID)
	}

	if !sdw.shardInfo.HasMaster() {
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

	// find an appropriate tablet in destination shard
	var err error
	sdw.destinationAlias, err = FindWorkerTablet(ctx, sdw.wr, sdw.cleaner, nil /* tsc */, sdw.cell, sdw.keyspace, sdw.shard, sdw.minHealthyRdonlyTablets)
	if err != nil {
		return fmt.Errorf("FindWorkerTablet() failed for %v/%v/%v: %v", sdw.cell, sdw.keyspace, sdw.shard, err)
	}

	// find an appropriate tablet in the source shard
	for _, ss := range sdw.shardInfo.SourceShards {
		if ss.Uid == sdw.sourceUID {
			sdw.sourceAlias, err = FindWorkerTablet(ctx, sdw.wr, sdw.cleaner, nil /* tsc */, sdw.cell, sdw.keyspace, ss.Shard, sdw.minHealthyRdonlyTablets)
			if err != nil {
				return fmt.Errorf("FindWorkerTablet() failed for %v/%v/%v: %v", sdw.cell, sdw.keyspace, ss.Shard, err)
			}
		}
	}

	return nil
}

// synchronizeReplication phase:
// 1 - ask the master of the destination shard to pause filtered replication,
//   and return the source binlog positions
//   (add a cleanup task to restart filtered replication on master)
// 2 - stop the source tablet at a binlog position higher than the
//   destination master. Get that new list of positions.
//   (add a cleanup task to restart binlog replication on the source tablet, and
//    change the existing ChangeSlaveType cleanup action to 'spare' type)
// 3 - ask the master of the destination shard to resume filtered replication
//   up to the new list of positions, and return its binlog position.
// 4 - wait until the destination tablet is equal or passed that master
//   binlog position, and stop its replication.
//   (add a cleanup task to restart binlog replication on it, and change
//    the existing ChangeSlaveType cleanup action to 'spare' type)
// 5 - restart filtered replication on the destination master.
//   (remove the cleanup task that does the same)
// At this point, the source and the destination tablet are stopped at the same
// point.

func (sdw *SplitDiffWorker) synchronizeReplication(ctx context.Context) error {
	sdw.SetState(WorkerStateSyncReplication)

	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	masterInfo, err := sdw.wr.TopoServer().GetTablet(shortCtx, sdw.shardInfo.MasterAlias)
	cancel()
	if err != nil {
		return fmt.Errorf("synchronizeReplication: cannot get Tablet record for master %v: %v", sdw.shardInfo.MasterAlias, err)
	}

	// 1 - stop the master binlog replication, get its current position
	sdw.wr.Logger().Infof("Stopping master binlog replication on %v", sdw.shardInfo.MasterAlias)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	blpPositionList, err := sdw.wr.TabletManagerClient().StopBlp(shortCtx, masterInfo.Tablet)
	cancel()
	if err != nil {
		return fmt.Errorf("StopBlp for %v failed: %v", sdw.shardInfo.MasterAlias, err)
	}
	wrangler.RecordStartBlpAction(sdw.cleaner, masterInfo.Tablet)

	// 2 - stop the source tablet at a binlog position
	//     higher than the destination master

	// find where we should be stopping
	blpPos := tmutils.FindBlpPositionByID(blpPositionList, sdw.sourceUID)
	if blpPos == nil {
		return fmt.Errorf("no binlog position on the master for Uid %v", sdw.sourceUID)
	}

	// read the tablet
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	sourceTablet, err := sdw.wr.TopoServer().GetTablet(shortCtx, sdw.sourceAlias)
	cancel()
	if err != nil {
		return err
	}

	// stop replication
	sdw.wr.Logger().Infof("Stopping slave %v at a minimum of %v", sdw.sourceAlias, blpPos.Position)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	stoppedAt, err := sdw.wr.TabletManagerClient().StopSlaveMinimum(shortCtx, sourceTablet.Tablet, blpPos.Position, *remoteActionsTimeout)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot stop slave %v at right binlog position %v: %v", sdw.sourceAlias, blpPos.Position, err)
	}
	stopPositionList := []*tabletmanagerdatapb.BlpPosition{
		{
			Uid:      sdw.sourceUID,
			Position: stoppedAt,
		},
	}

	// change the cleaner actions from ChangeSlaveType(rdonly)
	// to StartSlave() + ChangeSlaveType(spare)
	wrangler.RecordStartSlaveAction(sdw.cleaner, sourceTablet.Tablet)

	// 3 - ask the master of the destination shard to resume filtered
	//     replication up to the new list of positions
	sdw.wr.Logger().Infof("Restarting master %v until it catches up to %v", sdw.shardInfo.MasterAlias, stopPositionList)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	masterPos, err := sdw.wr.TabletManagerClient().RunBlpUntil(shortCtx, masterInfo.Tablet, stopPositionList, *remoteActionsTimeout)
	cancel()
	if err != nil {
		return fmt.Errorf("RunBlpUntil for %v until %v failed: %v", sdw.shardInfo.MasterAlias, stopPositionList, err)
	}

	// 4 - wait until the destination tablet is equal or passed
	//     that master binlog position, and stop its replication.
	sdw.wr.Logger().Infof("Waiting for destination tablet %v to catch up to %v", sdw.destinationAlias, masterPos)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	destinationTablet, err := sdw.wr.TopoServer().GetTablet(shortCtx, sdw.destinationAlias)
	cancel()
	if err != nil {
		return err
	}
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	_, err = sdw.wr.TabletManagerClient().StopSlaveMinimum(shortCtx, destinationTablet.Tablet, masterPos, *remoteActionsTimeout)
	cancel()
	if err != nil {
		return fmt.Errorf("StopSlaveMinimum for %v at %v failed: %v", sdw.destinationAlias, masterPos, err)
	}
	wrangler.RecordStartSlaveAction(sdw.cleaner, destinationTablet.Tablet)

	// 5 - restart filtered replication on destination master
	sdw.wr.Logger().Infof("Restarting filtered replication on master %v", sdw.shardInfo.MasterAlias)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	err = sdw.wr.TabletManagerClient().StartBlp(shortCtx, masterInfo.Tablet)
	if err := sdw.cleaner.RemoveActionByName(wrangler.StartBlpActionName, topoproto.TabletAliasString(sdw.shardInfo.MasterAlias)); err != nil {
		sdw.wr.Logger().Warningf("Cannot find cleaning action %v/%v: %v", wrangler.StartBlpActionName, topoproto.TabletAliasString(sdw.shardInfo.MasterAlias), err)
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
	wg := sync.WaitGroup{}
	rec := &concurrency.AllErrorRecorder{}
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
	wg.Add(1)
	go func() {
		var err error
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		sdw.sourceSchemaDefinition, err = sdw.wr.GetSchema(
			shortCtx, sdw.sourceAlias, nil /* tables */, sdw.excludeTables, false /* includeViews */)
		cancel()
		rec.RecordError(err)
		sdw.wr.Logger().Infof("Got schema from source %v", sdw.sourceAlias)
		wg.Done()
	}()

	wg.Wait()
	if rec.HasErrors() {
		return rec.Error()
	}

	sdw.wr.Logger().Infof("Diffing the schema...")
	rec = &concurrency.AllErrorRecorder{}
	tmutils.DiffSchema("destination", sdw.destinationSchemaDefinition, "source", sdw.sourceSchemaDefinition, rec)
	if rec.HasErrors() {
		sdw.wr.Logger().Warningf("Different schemas: %v", rec.Error().Error())
	} else {
		sdw.wr.Logger().Infof("Schema match, good.")
	}

	// read the vschema if needed
	var keyspaceSchema *vindexes.KeyspaceSchema
	if *useV3ReshardingMode {
		kschema, err := sdw.wr.TopoServer().GetVSchema(ctx, sdw.keyspace)
		if err != nil {
			return fmt.Errorf("cannot load VSchema for keyspace %v: %v", sdw.keyspace, err)
		}
		if kschema == nil {
			return fmt.Errorf("no VSchema for keyspace %v", sdw.keyspace)
		}

		keyspaceSchema, err = vindexes.BuildKeyspaceSchema(kschema, sdw.keyspace)
		if err != nil {
			return fmt.Errorf("cannot build vschema for keyspace %v: %v", sdw.keyspace, err)
		}
	}

	// Compute the overlap keyrange. Later, we'll compare it with
	// source or destination keyrange. If it matches either,
	// we'll just ask for all the data. If the overlap is a subset,
	// we'll filter.
	overlap, err := key.KeyRangesOverlap(sdw.shardInfo.KeyRange, sdw.shardInfo.SourceShards[sdw.sourceUID].KeyRange)
	if err != nil {
		return fmt.Errorf("Source shard doesn't overlap with destination: %v", err)
	}

	// run the diffs, 8 at a time
	sdw.wr.Logger().Infof("Running the diffs...")
	// TODO(mberlin): Parameterize the hard coded value 8.
	sem := sync2.NewSemaphore(8, 0)
	for _, tableDefinition := range sdw.destinationSchemaDefinition.TableDefinitions {
		wg.Add(1)
		go func(tableDefinition *tabletmanagerdatapb.TableDefinition) {
			defer wg.Done()
			sem.Acquire()
			defer sem.Release()

			sdw.wr.Logger().Infof("Starting the diff on table %v", tableDefinition.Name)

			// On the source, see if we need a full scan
			// or a filtered scan.
			var sourceQueryResultReader *QueryResultReader
			if key.KeyRangeEqual(overlap, sdw.shardInfo.SourceShards[sdw.sourceUID].KeyRange) {
				sourceQueryResultReader, err = TableScan(ctx, sdw.wr.Logger(), sdw.wr.TopoServer(), sdw.sourceAlias, tableDefinition)
			} else {
				sourceQueryResultReader, err = TableScanByKeyRange(ctx, sdw.wr.Logger(), sdw.wr.TopoServer(), sdw.sourceAlias, tableDefinition, overlap, keyspaceSchema, sdw.keyspaceInfo.ShardingColumnName, sdw.keyspaceInfo.ShardingColumnType)
			}
			if err != nil {
				newErr := fmt.Errorf("TableScan(ByKeyRange?)(source) failed: %v", err)
				rec.RecordError(newErr)
				sdw.wr.Logger().Errorf("%v", newErr)
				return
			}
			defer sourceQueryResultReader.Close(ctx)

			// On the destination, see if we need a full scan
			// or a filtered scan.
			var destinationQueryResultReader *QueryResultReader
			if key.KeyRangeEqual(overlap, sdw.shardInfo.KeyRange) {
				destinationQueryResultReader, err = TableScan(ctx, sdw.wr.Logger(), sdw.wr.TopoServer(), sdw.destinationAlias, tableDefinition)
			} else {
				destinationQueryResultReader, err = TableScanByKeyRange(ctx, sdw.wr.Logger(), sdw.wr.TopoServer(), sdw.destinationAlias, tableDefinition, overlap, keyspaceSchema, sdw.keyspaceInfo.ShardingColumnName, sdw.keyspaceInfo.ShardingColumnType)
			}
			if err != nil {
				newErr := fmt.Errorf("TableScan(ByKeyRange?)(destination) failed: %v", err)
				rec.RecordError(newErr)
				sdw.wr.Logger().Errorf("%v", newErr)
				return
			}
			defer destinationQueryResultReader.Close(ctx)

			// Create the row differ.
			differ, err := NewRowDiffer(sourceQueryResultReader, destinationQueryResultReader, tableDefinition)
			if err != nil {
				newErr := fmt.Errorf("NewRowDiffer() failed: %v", err)
				rec.RecordError(newErr)
				sdw.wr.Logger().Errorf("%v", newErr)
				return
			}

			// And run the diff.
			report, err := differ.Go(sdw.wr.Logger())
			if err != nil {
				newErr := fmt.Errorf("Differ.Go failed: %v", err.Error())
				rec.RecordError(newErr)
				sdw.wr.Logger().Errorf("%v", newErr)
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
