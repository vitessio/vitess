/*
Copyright 2017 Google Inc.

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
	"fmt"
	"html/template"
	"sync"

	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/wrangler"

	"sort"

	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// MultiSplitDiffWorker executes a diff between a destination shard and its
// source shards in a shard split case.
type MultiSplitDiffWorker struct {
	StatusWorker

	wr                                *wrangler.Wrangler
	cell                              string
	keyspace                          string
	shard                             string
	excludeTables                     []string
	minHealthyRdonlyTablets           int
	parallelDiffsCount                int
	waitForFixedTimeRatherThanGtidSet bool
	cleaner                           *wrangler.Cleaner

	// populated during WorkerStateInit, read-only after that
	keyspaceInfo      *topo.KeyspaceInfo
	shardInfo         *topo.ShardInfo
	sourceUID         uint32
	destinationShards []*topo.ShardInfo

	// populated during WorkerStateFindTargets, read-only after that
	sourceAlias        *topodatapb.TabletAlias
	destinationAliases []*topodatapb.TabletAlias // matches order of destinationShards
}

// NewMultiSplitDiffWorker returns a new MultiSplitDiffWorker object.
func NewMultiSplitDiffWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, excludeTables []string, minHealthyRdonlyTablets, parallelDiffsCount int, waitForFixedTimeRatherThanGtidSet bool) Worker {
	return &MultiSplitDiffWorker{
		waitForFixedTimeRatherThanGtidSet: waitForFixedTimeRatherThanGtidSet,
		StatusWorker:                      NewStatusWorker(),
		wr:                                wr,
		cell:                              cell,
		keyspace:                          keyspace,
		shard:                             shard,
		excludeTables:                     excludeTables,
		minHealthyRdonlyTablets:           minHealthyRdonlyTablets,
		parallelDiffsCount:                parallelDiffsCount,
		cleaner:                           &wrangler.Cleaner{},
	}
}

// StatusAsHTML is part of the Worker interface
func (msdw *MultiSplitDiffWorker) StatusAsHTML() template.HTML {
	state := msdw.State()

	result := "<b>Working on:</b> " + msdw.keyspace + "/" + msdw.shard + "</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateDiff:
		result += "<b>Running...</b></br>\n"
	case WorkerStateDiffWillFail:
		result += "Running - have already found differences...\n"
	case WorkerStateDone:
		result += "<b>Success.</b></br>\n"
	}

	return template.HTML(result)
}

// StatusAsText is part of the Worker interface
func (msdw *MultiSplitDiffWorker) StatusAsText() string {
	state := msdw.State()

	result := "Working on: " + msdw.keyspace + "/" + msdw.shard + "\n"
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
func (msdw *MultiSplitDiffWorker) Run(ctx context.Context) error {
	resetVars()
	err := msdw.run(ctx)

	msdw.SetState(WorkerStateCleanUp)
	cerr := msdw.cleaner.CleanUp(msdw.wr)
	if cerr != nil {
		if err != nil {
			msdw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}
	if err != nil {
		msdw.wr.Logger().Errorf("Run() error: %v", err)
		msdw.SetState(WorkerStateError)
		return err
	}
	msdw.SetState(WorkerStateDone)
	return nil
}

func (msdw *MultiSplitDiffWorker) run(ctx context.Context) error {
	// first state: read what we need to do
	if err := msdw.init(ctx); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// second state: find targets
	if err := msdw.findTargets(ctx); err != nil {
		return fmt.Errorf("findTargets() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// third phase: synchronize replication
	if err := msdw.synchronizeReplication(ctx); err != nil {
		return fmt.Errorf("synchronizeReplication() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// fourth phase: diff
	if err := msdw.diff(ctx); err != nil {
		return fmt.Errorf("diff() failed: %v", err)
	}

	return checkDone(ctx)
}

// init phase:
// - read the shard info, make sure it has sources
func (msdw *MultiSplitDiffWorker) init(ctx context.Context) error {
	msdw.SetState(WorkerStateInit)

	var err error
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	msdw.keyspaceInfo, err = msdw.wr.TopoServer().GetKeyspace(shortCtx, msdw.keyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read keyspace %v: %v", msdw.keyspace, err)
	}
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	msdw.shardInfo, err = msdw.wr.TopoServer().GetShard(shortCtx, msdw.keyspace, msdw.shard)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read shard %v/%v: %v", msdw.keyspace, msdw.shard, err)
	}

	if !msdw.shardInfo.HasMaster() {
		return fmt.Errorf("shard %v/%v has no master", msdw.keyspace, msdw.shard)
	}

	destinationShards, err := msdw.findDestinationShards(ctx)
	if err != nil {
		return fmt.Errorf("findDestinationShards() failed for %v/%v/%v: %v", msdw.cell, msdw.keyspace, msdw.shard, err)
	}
	msdw.destinationShards = destinationShards

	return nil
}

// findDestinationShards finds all the shards that have filtered replication from the source shard
func (msdw *MultiSplitDiffWorker) findDestinationShards(ctx context.Context) ([]*topo.ShardInfo, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	keyspaces, err := msdw.wr.TopoServer().GetKeyspaces(shortCtx)
	cancel()
	if err != nil {
		return nil, vterrors.Wrap(err, "failed to get list of keyspaces")
	}

	var resultArray []*topo.ShardInfo

	for _, keyspace := range keyspaces {
		shardInfo, err := msdw.findShardsInKeyspace(ctx, keyspace)
		if err != nil {
			return nil, err
		}
		resultArray = append(resultArray, shardInfo...)
	}

	if len(resultArray) == 0 {
		return nil, fmt.Errorf("there are no destination shards")
	}
	return resultArray, nil
}

func (msdw *MultiSplitDiffWorker) findShardsInKeyspace(ctx context.Context, keyspace string) ([]*topo.ShardInfo, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	shards, err := msdw.wr.TopoServer().GetShardNames(shortCtx, keyspace)
	cancel()
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to get list of shards for keyspace '%v'", keyspace)
	}

	var resultArray []*topo.ShardInfo
	first := true

	for _, shard := range shards {
		shardInfo, uid, err := msdw.getShardInfo(ctx, keyspace, shard)
		if err != nil {
			return nil, err
		}
		// There might not be any source shards here
		if shardInfo != nil {
			if first {
				msdw.sourceUID = uid
				first = false
			} else if msdw.sourceUID != uid {
				return nil, fmt.Errorf("found a source ID that was different, aborting. %v vs %v", msdw.sourceUID, uid)
			}

			resultArray = append(resultArray, shardInfo)
		}
	}

	return resultArray, nil
}

func (msdw *MultiSplitDiffWorker) getShardInfo(ctx context.Context, keyspace string, shard string) (*topo.ShardInfo, uint32, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	si, err := msdw.wr.TopoServer().GetShard(shortCtx, keyspace, shard)
	cancel()
	if err != nil {
		return nil, 0, vterrors.Wrap(err, "failed to get shard info from toposerver")
	}

	for _, sourceShard := range si.SourceShards {
		if len(sourceShard.Tables) == 0 && sourceShard.Keyspace == msdw.keyspace && sourceShard.Shard == msdw.shard {
			// Prevents the same shard from showing up multiple times
			return si, sourceShard.Uid, nil
		}
	}

	return nil, 0, nil
}

// findTargets phase:
// - find one rdonly in source shard
// - find one rdonly per destination shard
// - mark them all as 'worker' pointing back to us
func (msdw *MultiSplitDiffWorker) findTargets(ctx context.Context) error {
	msdw.SetState(WorkerStateFindTargets)

	var err error

	// find an appropriate tablet in the source shard
	msdw.sourceAlias, err = FindWorkerTablet(
		ctx,
		msdw.wr,
		msdw.cleaner,
		nil, /* tsc */
		msdw.cell,
		msdw.keyspace,
		msdw.shard,
		1, /* minHealthyTablets */
		topodatapb.TabletType_RDONLY)
	if err != nil {
		return fmt.Errorf("FindWorkerTablet() failed for %v/%v/%v: %v", msdw.cell, msdw.keyspace, msdw.shard, err)
	}

	// find an appropriate tablet in each destination shard
	msdw.destinationAliases = make([]*topodatapb.TabletAlias, len(msdw.destinationShards))
	for i, destinationShard := range msdw.destinationShards {
		keyspace := destinationShard.Keyspace()
		shard := destinationShard.ShardName()
		destinationAlias, err := FindWorkerTablet(
			ctx,
			msdw.wr,
			msdw.cleaner,
			nil, /* tsc */
			msdw.cell,
			keyspace,
			shard,
			msdw.minHealthyRdonlyTablets,
			topodatapb.TabletType_RDONLY)
		if err != nil {
			return fmt.Errorf("FindWorkerTablet() failed for %v/%v/%v: %v", msdw.cell, keyspace, shard, err)
		}
		msdw.destinationAliases[i] = destinationAlias
	}
	if err != nil {
		return fmt.Errorf("FindWorkerTablet() failed for %v/%v/%v: %v", msdw.cell, msdw.keyspace, msdw.shard, err)
	}

	return nil
}

// ask the master of the destination shard to pause filtered replication,
// and return the source binlog positions
// (add a cleanup task to restart filtered replication on master)
func (msdw *MultiSplitDiffWorker) stopReplicationOnAllDestinationMasters(ctx context.Context, masterInfos []*topo.TabletInfo) ([]string, error) {
	destVreplicationPos := make([]string, len(msdw.destinationShards))

	for i, shardInfo := range msdw.destinationShards {
		masterInfo := masterInfos[i]

		msdw.wr.Logger().Infof("Stopping master binlog replication on %v", shardInfo.MasterAlias)
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		_, err := msdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StopVReplication(msdw.sourceUID, "for split diff"))
		cancel()
		if err != nil {
			return nil, fmt.Errorf("VReplicationExec(stop) for %v failed: %v", shardInfo.MasterAlias, err)
		}
		wrangler.RecordVReplicationAction(msdw.cleaner, masterInfo.Tablet, binlogplayer.StartVReplication(msdw.sourceUID))
		shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
		p3qr, err := msdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.ReadVReplicationPos(msdw.sourceUID))
		cancel()
		if err != nil {
			return nil, fmt.Errorf("VReplicationExec(stop) for %v failed: %v", msdw.shardInfo.MasterAlias, err)
		}
		qr := sqltypes.Proto3ToResult(p3qr)
		if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
			return nil, fmt.Errorf("unexpected result while reading position: %v", qr)
		}
		destVreplicationPos[i] = qr.Rows[0][0].ToString()
		if err != nil {
			return nil, fmt.Errorf("StopBlp for %v failed: %v", msdw.shardInfo.MasterAlias, err)
		}
	}
	return destVreplicationPos, nil
}

func (msdw *MultiSplitDiffWorker) getTabletInfoForShard(ctx context.Context, shardInfo *topo.ShardInfo) (*topo.TabletInfo, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	masterInfo, err := msdw.wr.TopoServer().GetTablet(shortCtx, shardInfo.MasterAlias)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("synchronizeReplication: cannot get Tablet record for master %v: %v", msdw.shardInfo.MasterAlias, err)
	}
	return masterInfo, nil
}

//  stop the source tablet at a binlog position higher than the
//  destination masters. Return the reached position
//  (add a cleanup task to restart binlog replication on the source tablet, and
//   change the existing ChangeSlaveType cleanup action to 'spare' type)
func (msdw *MultiSplitDiffWorker) stopReplicationOnSourceRdOnlyTabletAt(ctx context.Context, destVreplicationPos []string) (string, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	sourceTablet, err := msdw.wr.TopoServer().GetTablet(shortCtx, msdw.sourceAlias)
	cancel()
	if err != nil {
		return "", err
	}

	var mysqlPos string // will be the last GTID that we stopped at
	for _, vreplicationPos := range destVreplicationPos {
		// We need to stop the source RDONLY tablet at a position which includes ALL of the positions of the destination
		// shards. We do this by starting replication and then stopping at a minimum of each blp position separately.
		// TODO this is not terribly efficient but it's possible to implement without changing the existing RPC,
		// if we make StopSlaveMinimum take multiple blp positions then this will be a lot more efficient because you just
		// check for each position using WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS and then stop replication.

		msdw.wr.Logger().Infof("Stopping slave %v at a minimum of %v", msdw.sourceAlias, vreplicationPos)
		// read the tablet
		sourceTablet, err := msdw.wr.TopoServer().GetTablet(shortCtx, msdw.sourceAlias)
		if err != nil {
			return "", err
		}
		shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
		msdw.wr.TabletManagerClient().StartSlave(shortCtx, sourceTablet.Tablet)
		cancel()
		if err != nil {
			return "", err
		}

		shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
		mysqlPos, err = msdw.wr.TabletManagerClient().StopSlaveMinimum(shortCtx, sourceTablet.Tablet, vreplicationPos, *remoteActionsTimeout)
		cancel()
		if err != nil {
			return "", fmt.Errorf("cannot stop slave %v at right binlog position %v: %v", msdw.sourceAlias, vreplicationPos, err)
		}
	}
	// change the cleaner actions from ChangeSlaveType(rdonly)
	// to StartSlave() + ChangeSlaveType(spare)
	wrangler.RecordStartSlaveAction(msdw.cleaner, sourceTablet.Tablet)

	return mysqlPos, nil
}

// ask the master of the destination shard to resume filtered replication
// up to the new list of positions, and return its binlog position.
func (msdw *MultiSplitDiffWorker) resumeReplicationOnDestinationMasterUntil(ctx context.Context, shardInfo *topo.ShardInfo, mysqlPos string, masterInfo *topo.TabletInfo) (string, error) {
	msdw.wr.Logger().Infof("Restarting master %v until it catches up to %v", shardInfo.MasterAlias, mysqlPos)
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	_, err := msdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StartVReplicationUntil(msdw.sourceUID, mysqlPos))
	cancel()
	if err != nil {
		return "", fmt.Errorf("VReplication(start until) for %v until %v failed: %v", shardInfo.MasterAlias, mysqlPos, err)
	}
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	if err := msdw.wr.TabletManagerClient().VReplicationWaitForPos(shortCtx, masterInfo.Tablet, int(msdw.sourceUID), mysqlPos); err != nil {
		cancel()
		return "", fmt.Errorf("VReplicationWaitForPos for %v until %v failed: %v", shardInfo.MasterAlias, mysqlPos, err)
	}
	cancel()

	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	masterPos, err := msdw.wr.TabletManagerClient().MasterPosition(shortCtx, masterInfo.Tablet)
	cancel()
	if err != nil {
		return "", fmt.Errorf("MasterPosition for %v failed: %v", msdw.shardInfo.MasterAlias, err)
	}
	return masterPos, nil
}

// wait until the destination tablet is equal or passed that master
// binlog position, and stop its replication.
// (add a cleanup task to restart binlog replication on it, and change
//  the existing ChangeSlaveType cleanup action to 'spare' type)
func (msdw *MultiSplitDiffWorker) stopReplicationOnDestinationRdOnlys(ctx context.Context, destinationAlias *topodatapb.TabletAlias, masterPos string) error {
	if msdw.waitForFixedTimeRatherThanGtidSet {
		msdw.wr.Logger().Infof("Workaround for broken GTID set in destination RDONLY. Just waiting for 1 minute for %v and assuming replication has caught up. (should be at %v)", destinationAlias, masterPos)
	} else {
		msdw.wr.Logger().Infof("Waiting for destination tablet %v to catch up to %v", destinationAlias, masterPos)
	}
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	destinationTablet, err := msdw.wr.TopoServer().GetTablet(shortCtx, destinationAlias)
	cancel()
	if err != nil {
		return err
	}

	if msdw.waitForFixedTimeRatherThanGtidSet {
		time.Sleep(1 * time.Minute)
	}

	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	if msdw.waitForFixedTimeRatherThanGtidSet {
		err = msdw.wr.TabletManagerClient().StopSlave(shortCtx, destinationTablet.Tablet)
	} else {
		_, err = msdw.wr.TabletManagerClient().StopSlaveMinimum(shortCtx, destinationTablet.Tablet, masterPos, *remoteActionsTimeout)
	}
	cancel()
	if err != nil {
		return fmt.Errorf("StopSlaveMinimum for %v at %v failed: %v", destinationAlias, masterPos, err)
	}
	wrangler.RecordStartSlaveAction(msdw.cleaner, destinationTablet.Tablet)
	return nil
}

// restart filtered replication on the destination master.
// (remove the cleanup task that does the same)
func (msdw *MultiSplitDiffWorker) restartReplicationOn(ctx context.Context, shardInfo *topo.ShardInfo, masterInfo *topo.TabletInfo) error {
	msdw.wr.Logger().Infof("Restarting filtered replication on master %v", shardInfo.MasterAlias)
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	_, err := msdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StartVReplication(msdw.sourceUID))
	if err != nil {
		return fmt.Errorf("VReplicationExec(start) failed for %v: %v", shardInfo.MasterAlias, err)
	}
	cancel()
	return nil
}

// synchronizeReplication phase:
// At this point, the source and the destination tablet are stopped at the same
// point.

func (msdw *MultiSplitDiffWorker) synchronizeReplication(ctx context.Context) error {
	msdw.SetState(WorkerStateSyncReplication)
	var err error

	masterInfos := make([]*topo.TabletInfo, len(msdw.destinationAliases))
	for i, shardInfo := range msdw.destinationShards {
		masterInfos[i], err = msdw.getTabletInfoForShard(ctx, shardInfo)
		if err != nil {
			return err
		}
	}

	destVreplicationPos, err := msdw.stopReplicationOnAllDestinationMasters(ctx, masterInfos)
	if err != nil {
		return err
	}

	mysqlPos, err := msdw.stopReplicationOnSourceRdOnlyTabletAt(ctx, destVreplicationPos)
	if err != nil {
		return err
	}

	for i, shardInfo := range msdw.destinationShards {
		masterInfo := masterInfos[i]
		destinationAlias := msdw.destinationAliases[i]

		masterPos, err := msdw.resumeReplicationOnDestinationMasterUntil(ctx, shardInfo, mysqlPos, masterInfo)
		if err != nil {
			return err
		}

		err = msdw.stopReplicationOnDestinationRdOnlys(ctx, destinationAlias, masterPos)
		if err != nil {
			return err
		}

		err = msdw.restartReplicationOn(ctx, shardInfo, masterInfo)
		if err != nil {
			return err
		}
	}

	return nil
}

func (msdw *MultiSplitDiffWorker) diffSingleTable(ctx context.Context, wg *sync.WaitGroup, tableDefinition *tabletmanagerdatapb.TableDefinition, keyspaceSchema *vindexes.KeyspaceSchema) error {
	msdw.wr.Logger().Infof("Starting the diff on table %v", tableDefinition.Name)

	sourceQueryResultReader, err := TableScan(ctx, msdw.wr.Logger(), msdw.wr.TopoServer(), msdw.sourceAlias, tableDefinition)
	if err != nil {
		return fmt.Errorf("TableScan(source) failed: %v", err)
	}
	defer sourceQueryResultReader.Close(ctx)

	destinationQueryResultReaders := make([]ResultReader, len(msdw.destinationAliases))
	for i, destinationAlias := range msdw.destinationAliases {
		destinationQueryResultReader, err := TableScan(ctx, msdw.wr.Logger(), msdw.wr.TopoServer(), destinationAlias, tableDefinition)
		if err != nil {
			return fmt.Errorf("TableScan(destination) failed: %v", err)
		}

		// For the first result scanner, let's check the PKs are of types that we can work with
		if i == 0 {
			err = CheckValidTypesForResultMerger(destinationQueryResultReader.fields, len(tableDefinition.PrimaryKeyColumns))
			if err != nil {
				return fmt.Errorf("invalid types for multi split diff. use the regular split diff instead %v", err.Error())
			}
		}

		// We are knowingly using defer inside the for loop.
		// All these readers need to be active until the diff is done
		//noinspection GoDeferInLoop
		defer destinationQueryResultReader.Close(ctx)
		destinationQueryResultReaders[i] = destinationQueryResultReader
	}
	mergedResultReader, err := NewResultMerger(destinationQueryResultReaders, len(tableDefinition.PrimaryKeyColumns))
	if err != nil {
		return fmt.Errorf("NewResultMerger failed: %v", err)
	}

	// Create the row differ.
	differ, err := NewRowDiffer(sourceQueryResultReader, mergedResultReader, tableDefinition)
	if err != nil {
		return fmt.Errorf("NewRowDiffer() failed: %v", err)
	}

	// And run the diff.
	report, err := differ.Go(msdw.wr.Logger())
	if err != nil {
		return fmt.Errorf("Differ.Go failed: %v", err.Error())
	}

	if report.HasDifferences() {
		return fmt.Errorf("table %v has differences: %v", tableDefinition.Name, report.String())
	}

	msdw.wr.Logger().Infof("Table %v checks out (%v rows processed, %v qps)", tableDefinition.Name, report.processedRows, report.processingQPS)

	return nil
}

func (msdw *MultiSplitDiffWorker) tableDiffingConsumer(ctx context.Context, wg *sync.WaitGroup, tableChan chan *tabletmanagerdatapb.TableDefinition, rec *concurrency.AllErrorRecorder, keyspaceSchema *vindexes.KeyspaceSchema) {
	defer wg.Done()

	for tableDefinition := range tableChan {
		err := msdw.diffSingleTable(ctx, wg, tableDefinition, keyspaceSchema)
		if err != nil {
			msdw.markAsWillFail(rec, err)
			msdw.wr.Logger().Errorf("%v", err)
		}
	}
}

func (msdw *MultiSplitDiffWorker) gatherSchemaInfo(ctx context.Context) ([]*tabletmanagerdatapb.SchemaDefinition, *tabletmanagerdatapb.SchemaDefinition, error) {
	msdw.wr.Logger().Infof("Gathering schema information...")
	wg := sync.WaitGroup{}
	rec := &concurrency.AllErrorRecorder{}

	// this array will have concurrent writes to it, but no two goroutines will write to the same slot in the array
	destinationSchemaDefinitions := make([]*tabletmanagerdatapb.SchemaDefinition, len(msdw.destinationAliases))
	var sourceSchemaDefinition *tabletmanagerdatapb.SchemaDefinition
	for i, destinationAlias := range msdw.destinationAliases {
		wg.Add(1)
		go func(i int, destinationAlias *topodatapb.TabletAlias) {
			var err error
			shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
			destinationSchemaDefinition, err := msdw.wr.GetSchema(
				shortCtx, destinationAlias, nil /* tables */, msdw.excludeTables, false /* includeViews */)
			cancel()
			msdw.markAsWillFail(rec, err)
			destinationSchemaDefinitions[i] = destinationSchemaDefinition
			msdw.wr.Logger().Infof("Got schema from destination %v", destinationAlias)
			wg.Done()
		}(i, destinationAlias)
	}
	wg.Add(1)
	go func() {
		var err error
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		sourceSchemaDefinition, err = msdw.wr.GetSchema(
			shortCtx, msdw.sourceAlias, nil /* tables */, msdw.excludeTables, false /* includeViews */)
		cancel()
		msdw.markAsWillFail(rec, err)
		msdw.wr.Logger().Infof("Got schema from source %v", msdw.sourceAlias)
		wg.Done()
	}()

	wg.Wait()
	if rec.HasErrors() {
		return nil, nil, rec.Error()
	}

	return destinationSchemaDefinitions, sourceSchemaDefinition, nil
}

func (msdw *MultiSplitDiffWorker) diffSchemaInformation(ctx context.Context, destinationSchemaDefinitions []*tabletmanagerdatapb.SchemaDefinition, sourceSchemaDefinition *tabletmanagerdatapb.SchemaDefinition) {
	msdw.wr.Logger().Infof("Diffing the schema...")
	rec := &concurrency.AllErrorRecorder{}
	sourceShardName := fmt.Sprintf("%v/%v", msdw.shardInfo.Keyspace(), msdw.shardInfo.ShardName())
	for i, destinationSchemaDefinition := range destinationSchemaDefinitions {
		destinationShard := msdw.destinationShards[i]
		destinationShardName := fmt.Sprintf("%v/%v", destinationShard.Keyspace(), destinationShard.ShardName())
		tmutils.DiffSchema(destinationShardName, destinationSchemaDefinition, sourceShardName, sourceSchemaDefinition, rec)
	}
	if rec.HasErrors() {
		msdw.wr.Logger().Warningf("Different schemas: %v", rec.Error().Error())
	} else {
		msdw.wr.Logger().Infof("Schema match, good.")
	}
}

func (msdw *MultiSplitDiffWorker) loadVSchema(ctx context.Context) (*vindexes.KeyspaceSchema, error) {
	shortCtx, cancel := context.WithCancel(ctx)
	kschema, err := msdw.wr.TopoServer().GetVSchema(shortCtx, msdw.keyspace)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("cannot load VSchema for keyspace %v: %v", msdw.keyspace, err)
	}
	if kschema == nil {
		return nil, fmt.Errorf("no VSchema for keyspace %v", msdw.keyspace)
	}

	keyspaceSchema, err := vindexes.BuildKeyspaceSchema(kschema, msdw.keyspace)
	if err != nil {
		return nil, fmt.Errorf("cannot build vschema for keyspace %v: %v", msdw.keyspace, err)
	}
	return keyspaceSchema, nil
}

// diff phase: will log messages regarding the diff.
// - get the schema on all tablets
// - if some table schema mismatches, record them (use existing schema diff tools).
// - for each table in destination, run a diff pipeline.

func (msdw *MultiSplitDiffWorker) diff(ctx context.Context) error {
	msdw.SetState(WorkerStateDiff)

	destinationSchemaDefinitions, sourceSchemaDefinition, err := msdw.gatherSchemaInfo(ctx)
	if err != nil {
		return err
	}
	msdw.diffSchemaInformation(ctx, destinationSchemaDefinitions, sourceSchemaDefinition)

	// read the vschema if needed
	var keyspaceSchema *vindexes.KeyspaceSchema
	if *useV3ReshardingMode {
		keyspaceSchema, err = msdw.loadVSchema(ctx)
		if err != nil {
			return err
		}
	}

	msdw.wr.Logger().Infof("Running the diffs...")
	tableDefinitions := sourceSchemaDefinition.TableDefinitions
	rec := &concurrency.AllErrorRecorder{}

	// sort tables by size
	// if there are large deltas between table sizes then it's more efficient to start working on the large tables first
	sort.Slice(tableDefinitions, func(i, j int) bool { return tableDefinitions[i].DataLength > tableDefinitions[j].DataLength })
	tableChan := make(chan *tabletmanagerdatapb.TableDefinition, len(tableDefinitions))
	for _, tableDefinition := range tableDefinitions {
		tableChan <- tableDefinition
	}
	close(tableChan)

	consumers := sync.WaitGroup{}
	// start as many goroutines we want parallel diffs running
	for i := 0; i < msdw.parallelDiffsCount; i++ {
		consumers.Add(1)
		go msdw.tableDiffingConsumer(ctx, &consumers, tableChan, rec, keyspaceSchema)
	}

	// wait for all consumers to wrap up their work
	consumers.Wait()

	return rec.Error()
}

// markAsWillFail records the error and changes the state of the worker to reflect this
func (msdw *MultiSplitDiffWorker) markAsWillFail(er concurrency.ErrorRecorder, err error) {
	er.RecordError(err)
	msdw.SetState(WorkerStateDiffWillFail)
}
