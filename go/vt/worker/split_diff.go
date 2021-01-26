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

package worker

import (
	"html/template"
	"sort"
	"sync"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/wrangler"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
	sourceShard             *topodatapb.Shard_SourceShard
	excludeTables           []string
	minHealthyRdonlyTablets int
	destinationTabletType   topodatapb.TabletType
	parallelDiffsCount      int
	skipVerify              bool
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
func NewSplitDiffWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, sourceUID uint32, excludeTables []string, minHealthyRdonlyTablets, parallelDiffsCount int, tabletType topodatapb.TabletType, skipVerify bool) Worker {
	return &SplitDiffWorker{
		StatusWorker:            NewStatusWorker(),
		wr:                      wr,
		cell:                    cell,
		keyspace:                keyspace,
		shard:                   shard,
		sourceUID:               sourceUID,
		excludeTables:           excludeTables,
		minHealthyRdonlyTablets: minHealthyRdonlyTablets,
		destinationTabletType:   tabletType,
		parallelDiffsCount:      parallelDiffsCount,
		skipVerify:              skipVerify,
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
	case WorkerStateDiffWillFail:
		result += "<b>Running - have already found differences...</b></br>\n"
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
	case WorkerStateDiffWillFail:
		result += "Running - have already found differences...\n"
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
			sdw.wr.Logger().Errorf2(cerr, "CleanUp failed in addition to job error")
		} else {
			err = cerr
		}
	}
	if err != nil {
		sdw.wr.Logger().Errorf2(err, "Run() error")
		sdw.SetState(WorkerStateError)
		return err
	}
	sdw.SetState(WorkerStateDone)
	return nil
}

func (sdw *SplitDiffWorker) run(ctx context.Context) error {
	// first state: read what we need to do
	if err := sdw.init(ctx); err != nil {
		return vterrors.Wrap(err, "init() failed")
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// second state: find targets
	if err := sdw.findTargets(ctx); err != nil {
		return vterrors.Wrap(err, "findTargets() failed")
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// third phase: synchronize replication
	if err := sdw.synchronizeReplication(ctx); err != nil {
		return vterrors.Wrap(err, "synchronizeReplication() failed")
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// fourth phase: diff
	if err := sdw.diff(ctx); err != nil {
		return vterrors.Wrap(err, "diff() failed")
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
		return vterrors.Wrapf(err, "cannot read keyspace %v", sdw.keyspace)
	}
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	sdw.shardInfo, err = sdw.wr.TopoServer().GetShard(shortCtx, sdw.keyspace, sdw.shard)
	cancel()
	if err != nil {
		return vterrors.Wrapf(err, "cannot read shard %v/%v", sdw.keyspace, sdw.shard)
	}

	if len(sdw.shardInfo.SourceShards) == 0 {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "shard %v/%v has no source shard", sdw.keyspace, sdw.shard)
	}
	if sdw.sourceUID == 0 {
		if len(sdw.shardInfo.SourceShards) == 1 {
			sdw.sourceShard = sdw.shardInfo.SourceShards[0]
		} else {
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "shard %v/%v has more than one source, please specify a source UID", sdw.keyspace, sdw.shard)
		}
	} else {
		for _, ss := range sdw.shardInfo.SourceShards {
			if ss.Uid == sdw.sourceUID {
				sdw.sourceShard = ss
				break
			}
		}
	}
	if sdw.sourceShard == nil {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "shard %v/%v has no source shard with UID %v", sdw.keyspace, sdw.shard, sdw.sourceUID)
	}

	if !sdw.shardInfo.HasMaster() {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "shard %v/%v has no master", sdw.keyspace, sdw.shard)
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
	sdw.destinationAlias, err = FindWorkerTablet(
		ctx,
		sdw.wr,
		sdw.cleaner,
		nil, /* tsc */
		sdw.cell,
		sdw.keyspace,
		sdw.shard,
		1, /* minHealthyTablets */
		sdw.destinationTabletType,
	)
	if err != nil {
		return vterrors.Wrapf(err, "FindWorkerTablet() failed for %v/%v/%v", sdw.cell, sdw.keyspace, sdw.shard)
	}

	// find an appropriate tablet in the source shard
	// During an horizontal shard split, multiple workers will race to get
	// a RDONLY tablet in the source shard. When this happen, concurrent calls
	// to FindWorkerTablet could attempt to set to DRAIN state the same tablet. Only
	// one of these calls to FindWorkerTablet will succeed and the rest will fail.
	// The following, makes sures we keep trying to find a worker tablet when this error occur.
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	for {
		select {
		case <-shortCtx.Done():
			cancel()
			return vterrors.Errorf(vtrpc.Code_ABORTED, "could not find healthy table for %v/%v%v: after: %v, aborting", sdw.cell, sdw.keyspace, sdw.sourceShard.Shard, *remoteActionsTimeout)
		default:
			sdw.sourceAlias, err = FindWorkerTablet(ctx, sdw.wr, sdw.cleaner, nil /* tsc */, sdw.cell, sdw.keyspace, sdw.sourceShard.Shard, sdw.minHealthyRdonlyTablets, topodatapb.TabletType_RDONLY)
			if err != nil {
				sdw.wr.Logger().Infof("FindWorkerTablet() failed for %v/%v/%v: %v retrying...", sdw.cell, sdw.keyspace, sdw.sourceShard.Shard, err)
				continue
			}
			cancel()
			return nil
		}
	}
}

// synchronizeReplication phase:
// 1 - ask the master of the destination shard to pause filtered replication,
//   and return the source binlog positions
//   (add a cleanup task to restart filtered replication on master)
// 2 - stop the source tablet at a binlog position higher than the
//   destination master. Get that new list of positions.
//   (add a cleanup task to restart binlog replication on the source tablet, and
//    change the existing ChangeTabletType cleanup action to 'spare' type)
// 3 - ask the master of the destination shard to resume filtered replication
//   up to the new list of positions, and return its binlog position.
// 4 - wait until the destination tablet is equal or passed that master
//   binlog position, and stop its replication.
//   (add a cleanup task to restart binlog replication on it, and change
//    the existing ChangeTabletType cleanup action to 'spare' type)
// 5 - restart filtered replication on the destination master.
//   (remove the cleanup task that does the same)
// At this point, the source and the destination tablet are stopped at the same
// point.

func (sdw *SplitDiffWorker) synchronizeReplication(ctx context.Context) error {
	sdw.SetState(WorkerStateSyncReplication)

	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	masterInfo, err := sdw.wr.TopoServer().GetTablet(shortCtx, sdw.shardInfo.MasterAlias)
	if err != nil {
		return vterrors.Wrapf(err, "synchronizeReplication: cannot get Tablet record for master %v", sdw.shardInfo.MasterAlias)
	}

	// 1 - stop the master binlog replication, get its current position
	sdw.wr.Logger().Infof("Stopping master binlog replication on %v", sdw.shardInfo.MasterAlias)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	_, err = sdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StopVReplication(sdw.sourceShard.Uid, "for split diff"))
	if err != nil {
		return vterrors.Wrapf(err, "VReplicationExec(stop) for %v failed", sdw.shardInfo.MasterAlias)
	}
	wrangler.RecordVReplicationAction(sdw.cleaner, masterInfo.Tablet, binlogplayer.StartVReplication(sdw.sourceShard.Uid))
	p3qr, err := sdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.ReadVReplicationPos(sdw.sourceShard.Uid))
	if err != nil {
		return vterrors.Wrapf(err, "ReadVReplicationPos for %v failed", sdw.shardInfo.MasterAlias)
	}
	qr := sqltypes.Proto3ToResult(p3qr)
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected result while reading position: %v", qr)
	}
	vreplicationPos := qr.Rows[0][0].ToString()

	// 2 - stop replication
	sdw.wr.Logger().Infof("Stopping replica %v at a minimum of %v", sdw.sourceAlias, vreplicationPos)
	// read the tablet
	sourceTablet, err := sdw.wr.TopoServer().GetTablet(shortCtx, sdw.sourceAlias)
	if err != nil {
		return err
	}

	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	mysqlPos, err := sdw.wr.TabletManagerClient().StopReplicationMinimum(shortCtx, sourceTablet.Tablet, vreplicationPos, *remoteActionsTimeout)
	if err != nil {
		return vterrors.Wrapf(err, "cannot stop replica %v at right binlog position %v", sdw.sourceAlias, vreplicationPos)
	}

	// change the cleaner actions from ChangeTabletType(rdonly)
	// to StartReplication() + ChangeTabletType(spare)
	wrangler.RecordStartReplicationAction(sdw.cleaner, sourceTablet.Tablet)

	// 3 - ask the master of the destination shard to resume filtered
	//     replication up to the new list of positions
	sdw.wr.Logger().Infof("Restarting master %v until it catches up to %v", sdw.shardInfo.MasterAlias, mysqlPos)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	_, err = sdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StartVReplicationUntil(sdw.sourceShard.Uid, mysqlPos))
	if err != nil {
		return vterrors.Wrapf(err, "VReplication(start until) for %v until %v failed", sdw.shardInfo.MasterAlias, mysqlPos)
	}
	if err := sdw.wr.TabletManagerClient().VReplicationWaitForPos(shortCtx, masterInfo.Tablet, int(sdw.sourceShard.Uid), mysqlPos); err != nil {
		return vterrors.Wrapf(err, "VReplicationWaitForPos for %v until %v failed", sdw.shardInfo.MasterAlias, mysqlPos)
	}
	masterPos, err := sdw.wr.TabletManagerClient().MasterPosition(shortCtx, masterInfo.Tablet)
	if err != nil {
		return vterrors.Wrapf(err, "MasterPosition for %v failed", sdw.shardInfo.MasterAlias)
	}

	// 4 - wait until the destination tablet is equal or passed
	//     that master binlog position, and stop its replication.
	sdw.wr.Logger().Infof("Waiting for destination tablet %v to catch up to %v", sdw.destinationAlias, masterPos)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	destinationTablet, err := sdw.wr.TopoServer().GetTablet(shortCtx, sdw.destinationAlias)
	if err != nil {
		return err
	}
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	if _, err = sdw.wr.TabletManagerClient().StopReplicationMinimum(shortCtx, destinationTablet.Tablet, masterPos, *remoteActionsTimeout); err != nil {
		return vterrors.Wrapf(err, "StopReplicationMinimum for %v at %v failed", sdw.destinationAlias, masterPos)
	}
	wrangler.RecordStartReplicationAction(sdw.cleaner, destinationTablet.Tablet)

	// 5 - restart filtered replication on destination master
	sdw.wr.Logger().Infof("Restarting filtered replication on master %v", sdw.shardInfo.MasterAlias)
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	defer cancel()
	if _, err = sdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StartVReplication(sdw.sourceShard.Uid)); err != nil {
		return vterrors.Wrapf(err, "VReplicationExec(start) failed for %v", sdw.shardInfo.MasterAlias)
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
		if err != nil {
			sdw.markAsWillFail(rec, err)
		}
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
		if err != nil {
			sdw.markAsWillFail(rec, err)
		}
		sdw.wr.Logger().Infof("Got schema from source %v", sdw.sourceAlias)
		wg.Done()
	}()

	wg.Wait()
	if rec.HasErrors() {
		return rec.Error()
	}

	// In splitClone state:
	// if source destination shard table has column like:
	// `object_id` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT
	// Then after copy and exec it on destination the table column will turn to like this:
	// `object_id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT
	// In that case,(mysql's behavior) when source has too many tables contains columns as `object_id`.
	// there will be too much differ schema fail error out put on vtworkerclient side says:
	// remainder of the error is truncated because gRPC has a size limit on errors
	// This will obscure the real problem.
	// so add this flag assumed people already know the schema does not match and make the process going on
	if !sdw.skipVerify {
		sdw.wr.Logger().Infof("Diffing the schema...")
		rec = &concurrency.AllErrorRecorder{}
		tmutils.DiffSchema("destination", sdw.destinationSchemaDefinition, "source", sdw.sourceSchemaDefinition, rec)
		if !rec.HasErrors() {
			sdw.wr.Logger().Infof("Schema match, good.")
		} else {
			sdw.wr.Logger().Warningf("Different schemas: %v", rec.Error().Error())
			return rec.Error()
		}
	}

	// read the vschema if needed
	var keyspaceSchema *vindexes.KeyspaceSchema
	if *useV3ReshardingMode {
		kschema, err := sdw.wr.TopoServer().GetVSchema(ctx, sdw.keyspace)
		if err != nil {
			return vterrors.Wrapf(err, "cannot load VSchema for keyspace %v", sdw.keyspace)
		}
		if kschema == nil {
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no VSchema for keyspace %v", sdw.keyspace)
		}

		keyspaceSchema, err = vindexes.BuildKeyspaceSchema(kschema, sdw.keyspace)
		if err != nil {
			return vterrors.Wrapf(err, "cannot build vschema for keyspace %v", sdw.keyspace)
		}
	}

	// Compute the overlap keyrange. Later, we'll compare it with
	// source or destination keyrange. If it matches either,
	// we'll just ask for all the data. If the overlap is a subset,
	// we'll filter.
	overlap, err := key.KeyRangesOverlap(sdw.shardInfo.KeyRange, sdw.sourceShard.KeyRange)
	if err != nil {
		return vterrors.Wrap(err, "Source shard doesn't overlap with destination")
	}

	// run the diffs, 8 at a time
	sdw.wr.Logger().Infof("Running the diffs...")
	sem := sync2.NewSemaphore(sdw.parallelDiffsCount, 0)
	tableDefinitions := sdw.destinationSchemaDefinition.TableDefinitions

	// sort tables by size
	// if there are large deltas between table sizes then it's more efficient to start working on the large tables first
	sort.Slice(tableDefinitions, func(i, j int) bool { return tableDefinitions[i].DataLength > tableDefinitions[j].DataLength })

	// use a channel to make sure tables are diffed in order
	tableChan := make(chan *tabletmanagerdatapb.TableDefinition, len(tableDefinitions))
	for _, tableDefinition := range tableDefinitions {
		tableChan <- tableDefinition
	}

	// start as many goroutines as there are tables to diff
	for range tableDefinitions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// use the semaphore to limit the number of tables that are diffed in parallel
			sem.Acquire()
			defer sem.Release()

			// grab the table to process out of the channel
			tableDefinition := <-tableChan

			sdw.wr.Logger().Infof("Starting the diff on table %v", tableDefinition.Name)

			// On the source, see if we need a full scan
			// or a filtered scan.
			var sourceQueryResultReader *QueryResultReader
			if key.KeyRangeEqual(overlap, sdw.sourceShard.KeyRange) {
				sourceQueryResultReader, err = TableScan(ctx, sdw.wr.Logger(), sdw.wr.TopoServer(), sdw.sourceAlias, tableDefinition)
			} else {
				sourceQueryResultReader, err = TableScanByKeyRange(ctx, sdw.wr.Logger(), sdw.wr.TopoServer(), sdw.sourceAlias, tableDefinition, overlap, keyspaceSchema, sdw.keyspaceInfo.ShardingColumnName, sdw.keyspaceInfo.ShardingColumnType)
			}
			if err != nil {
				newErr := vterrors.Wrap(err, "TableScan(ByKeyRange?)(source) failed")
				sdw.markAsWillFail(rec, newErr)
				sdw.wr.Logger().Error(newErr)
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
				newErr := vterrors.Wrap(err, "TableScan(ByKeyRange?)(destination) failed")
				sdw.markAsWillFail(rec, newErr)
				sdw.wr.Logger().Error(newErr)
				return
			}
			defer destinationQueryResultReader.Close(ctx)

			// Create the row differ.
			differ, err := NewRowDiffer(sourceQueryResultReader, destinationQueryResultReader, tableDefinition)
			if err != nil {
				newErr := vterrors.Wrap(err, "NewRowDiffer() failed")
				sdw.markAsWillFail(rec, newErr)
				sdw.wr.Logger().Error(newErr)
				return
			}

			// And run the diff.
			report, err := differ.Go(sdw.wr.Logger())
			if err != nil {
				newErr := vterrors.Wrapf(err, "Differ.Go failed")
				sdw.markAsWillFail(rec, newErr)
				sdw.wr.Logger().Error(newErr)
			} else {
				if report.HasDifferences() {
					err := vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "table %v has differences: %v", tableDefinition.Name, report.String())
					sdw.markAsWillFail(rec, err)
					sdw.wr.Logger().Warningf(err.Error())
				} else {
					sdw.wr.Logger().Infof("Table %v checks out (%v rows processed, %v qps)", tableDefinition.Name, report.processedRows, report.processingQPS)
				}
			}
		}()
	}

	// grab the table to process out of the channel
	wg.Wait()

	return rec.Error()
}

// markAsWillFail records the error and changes the state of the worker to reflect this
func (sdw *SplitDiffWorker) markAsWillFail(er concurrency.ErrorRecorder, err error) {
	er.RecordError(err)
	sdw.SetState(WorkerStateDiffWillFail)
}
