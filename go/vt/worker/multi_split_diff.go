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
	"sort"
	"sync"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/wrangler"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// Scanners encapsulates a source and a destination. We create one of these per paralell runner.
type Scanners struct {
	// this is how we get data from the source shard
	sourceScanner TableScanner

	// this is how we get data from the destination shards - we'll have one scanner per shard we are reading from
	destinationScanner []TableScanner
}

// MultiSplitDiffWorker executes a diff between a destination shard and its
// source shards in a shard split case.
type MultiSplitDiffWorker struct {
	StatusWorker

	wr                                *wrangler.Wrangler
	cell                              string
	keyspace                          string
	shard                             string
	excludeTables                     []string
	minHealthyTablets                 int
	parallelDiffsCount                int
	waitForFixedTimeRatherThanGtidSet bool
	cleaner                           *wrangler.Cleaner
	useConsistentSnapshot             bool
	tabletType                        topodatapb.TabletType

	// populated during WorkerStateInit, read-only after that
	keyspaceInfo      *topo.KeyspaceInfo
	shardInfo         *topo.ShardInfo
	sourceUID         uint32
	destinationShards []*topo.ShardInfo

	// populated during WorkerStateFindTargets, read-only after that
	sourceAlias        *topodatapb.TabletAlias
	destinationAliases []*topodatapb.TabletAlias // matches order of destinationShards
	scanners           []Scanners
}

// NewMultiSplitDiffWorker returns a new MultiSplitDiffWorker object.
func NewMultiSplitDiffWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, excludeTables []string, minHealthyTablets, parallelDiffsCount int, waitForFixedTimeRatherThanGtidSet bool, useConsistentSnapshot bool, tabletType topodatapb.TabletType) Worker {
	return &MultiSplitDiffWorker{
		StatusWorker:                      NewStatusWorker(),
		wr:                                wr,
		cell:                              cell,
		keyspace:                          keyspace,
		shard:                             shard,
		excludeTables:                     excludeTables,
		minHealthyTablets:                 minHealthyTablets,
		parallelDiffsCount:                parallelDiffsCount,
		cleaner:                           &wrangler.Cleaner{},
		useConsistentSnapshot:             useConsistentSnapshot,
		waitForFixedTimeRatherThanGtidSet: waitForFixedTimeRatherThanGtidSet,
		tabletType:                        tabletType,
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
		return vterrors.Wrap(err, "init() failed")
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// second state: find targets
	if err := msdw.findTargets(ctx); err != nil {
		return vterrors.Wrap(err, "findTargets() failed")
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// third phase: synchronize replication
	if err := msdw.synchronizeSrcAndDestTxState(ctx); err != nil {
		return vterrors.Wrap(err, "synchronizeSrcAndDestTxState() failed")
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// fourth phase: diff
	if err := msdw.diff(ctx); err != nil {
		return vterrors.Wrap(err, "diff() failed")
	}

	return checkDone(ctx)
}

// init phase:
// - read the shard info, make sure it has sources
func (msdw *MultiSplitDiffWorker) init(ctx context.Context) error {
	msdw.SetState(WorkerStateInit)

	if msdw.useConsistentSnapshot {
		msdw.wr.Logger().Infof("splitting using consistent snapshot")
	} else {
		msdw.wr.Logger().Infof("splitting using STOP SLAVE")
	}

	var err error
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	msdw.keyspaceInfo, err = msdw.wr.TopoServer().GetKeyspace(shortCtx, msdw.keyspace)
	cancel()
	if err != nil {
		return vterrors.Wrapf(err, "cannot read keyspace %v", msdw.keyspace)
	}
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	msdw.shardInfo, err = msdw.wr.TopoServer().GetShard(shortCtx, msdw.keyspace, msdw.shard)
	cancel()
	if err != nil {
		return vterrors.Wrapf(err, "cannot read shard %v/%v", msdw.keyspace, msdw.shard)
	}

	if !msdw.shardInfo.HasMaster() {
		return vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "shard %v/%v has no master", msdw.keyspace, msdw.shard)
	}

	destinationShards, err := msdw.findDestinationShards(ctx)
	if err != nil {
		return vterrors.Wrapf(err, "findDestinationShards() failed for %v/%v/%v", msdw.cell, msdw.keyspace, msdw.shard)
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
		return nil, vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "there are no destination shards")
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
				return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "found a source ID that was different, aborting. %v vs %v", msdw.sourceUID, uid)
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

	var finderFunc func(keyspace string, shard string) (*topodatapb.TabletAlias, error)
	if msdw.tabletType == topodatapb.TabletType_RDONLY {
		finderFunc = func(keyspace string, shard string) (*topodatapb.TabletAlias, error) {
			return FindWorkerTablet(ctx, msdw.wr, msdw.cleaner, nil /*tsc*/, msdw.cell, keyspace, shard, 1, topodatapb.TabletType_RDONLY)
		}
	} else {
		finderFunc = func(keyspace string, shard string) (*topodatapb.TabletAlias, error) {
			return FindHealthyTablet(ctx, msdw.wr, nil /*tsc*/, msdw.cell, keyspace, shard, 1, msdw.tabletType)
		}
	}

	msdw.sourceAlias, err = finderFunc(msdw.keyspace, msdw.shard)
	if err != nil {
		return vterrors.Wrapf(err, "finding source failed for %v/%v/%v", msdw.cell, msdw.keyspace, msdw.shard)
	}

	msdw.destinationAliases = make([]*topodatapb.TabletAlias, len(msdw.destinationShards))
	for i, destinationShard := range msdw.destinationShards {
		keyspace := destinationShard.Keyspace()
		shard := destinationShard.ShardName()
		destinationAlias, err := finderFunc(keyspace, shard)
		if err != nil {
			return vterrors.Wrapf(err, "finding destination failed for %v/%v/%v", msdw.cell, keyspace, shard)
		}
		msdw.destinationAliases[i] = destinationAlias
	}

	return nil
}

// ask the master of the destination shard to pause filtered replication,
// and return the source binlog positions
// (add a cleanup task to restart filtered replication on master)
func (msdw *MultiSplitDiffWorker) stopVreplicationOnAll(ctx context.Context, tabletInfo []*topo.TabletInfo) ([]string, error) {
	destVreplicationPos := make([]string, len(msdw.destinationShards))

	for i, shardInfo := range msdw.destinationShards {
		tablet := tabletInfo[i].Tablet

		msdw.wr.Logger().Infof("stopping master binlog replication on %v", shardInfo.MasterAlias)
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		_, err := msdw.wr.TabletManagerClient().VReplicationExec(shortCtx, tablet, binlogplayer.StopVReplication(msdw.sourceUID, "for split diff"))
		cancel()
		if err != nil {
			return nil, vterrors.Wrapf(err, "VReplicationExec(stop) for %v failed", shardInfo.MasterAlias)
		}
		wrangler.RecordVReplicationAction(msdw.cleaner, tablet, binlogplayer.StartVReplication(msdw.sourceUID))
		shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
		p3qr, err := msdw.wr.TabletManagerClient().VReplicationExec(shortCtx, tablet, binlogplayer.ReadVReplicationPos(msdw.sourceUID))
		cancel()
		if err != nil {
			return nil, vterrors.Wrapf(err, "VReplicationExec(stop) for %v failed", msdw.shardInfo.MasterAlias)
		}
		qr := sqltypes.Proto3ToResult(p3qr)
		if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
			return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "unexpected result while reading position: %v", qr)
		}
		destVreplicationPos[i] = qr.Rows[0][0].ToString()
		if err != nil {
			return nil, vterrors.Wrapf(err, "StopBlp for %v failed", msdw.shardInfo.MasterAlias)
		}
	}
	return destVreplicationPos, nil
}

func (msdw *MultiSplitDiffWorker) getMasterTabletInfoForShard(ctx context.Context, shardInfo *topo.ShardInfo) (*topo.TabletInfo, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	masterInfo, err := msdw.wr.TopoServer().GetTablet(shortCtx, shardInfo.MasterAlias)
	cancel()
	if err != nil {
		return nil, vterrors.Wrapf(err, "synchronizeSrcAndDestTxState: cannot get Tablet record for master %v", msdw.shardInfo.MasterAlias)
	}
	return masterInfo, nil
}

//  stop the source tablet at a binlog position higher than the
//  destination masters. Return the reached position
//  (add a cleanup task to restart binlog replication on the source tablet, and
//   change the existing ChangeSlaveType cleanup action to 'spare' type)
func (msdw *MultiSplitDiffWorker) stopReplicationOnSourceTabletAt(ctx context.Context, destVreplicationPos []string) (string, error) {
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

		msdw.wr.Logger().Infof("stopping slave %v at a minimum of %v", msdw.sourceAlias, vreplicationPos)

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
			return "", vterrors.Wrapf(err, "cannot stop slave %v at right binlog position %v", msdw.sourceAlias, vreplicationPos)
		}
	}
	// change the cleaner actions from ChangeSlaveType(rdonly)
	// to StartSlave() + ChangeSlaveType(spare)
	wrangler.RecordStartSlaveAction(msdw.cleaner, sourceTablet.Tablet)

	return mysqlPos, nil
}

// ask the master of the destination shard to resume filtered replication
// up to the specified source position, and return the destination position.
func (msdw *MultiSplitDiffWorker) stopVreplicationAt(ctx context.Context, shardInfo *topo.ShardInfo, sourcePosition string, masterInfo *topo.TabletInfo) (string, error) {
	msdw.wr.Logger().Infof("Restarting master %v until it catches up to %v", shardInfo.MasterAlias, sourcePosition)
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	_, err := msdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StartVReplicationUntil(msdw.sourceUID, sourcePosition))
	cancel()
	if err != nil {
		return "", vterrors.Wrapf(err, "VReplication(start until) for %v until %v failed", shardInfo.MasterAlias, sourcePosition)
	}

	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	err = msdw.wr.TabletManagerClient().VReplicationWaitForPos(shortCtx, masterInfo.Tablet, int(msdw.sourceUID), sourcePosition)
	cancel()
	if err != nil {
		return "", vterrors.Wrapf(err, "VReplicationWaitForPos for %v until %v failed", shardInfo.MasterAlias, sourcePosition)
	}

	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	masterPos, err := msdw.wr.TabletManagerClient().MasterPosition(shortCtx, masterInfo.Tablet)
	cancel()
	if err != nil {
		return "", vterrors.Wrapf(err, "MasterPosition for %v failed", msdw.shardInfo.MasterAlias)
	}
	return masterPos, nil
}

// wait until the destination tablet is equal or passed that master
// binlog position, and stop its replication.
// (add a cleanup task to restart binlog replication on it, and change
//  the existing ChangeSlaveType cleanup action to 'spare' type)
func (msdw *MultiSplitDiffWorker) stopReplicationAt(ctx context.Context, destinationAlias *topodatapb.TabletAlias, masterPos string) error {
	if msdw.waitForFixedTimeRatherThanGtidSet {
		msdw.wr.Logger().Infof("workaround for broken GTID set in destination RDONLY. Just waiting for 1 minute for %v and assuming replication has caught up. (should be at %v)", destinationAlias, masterPos)
	} else {
		msdw.wr.Logger().Infof("waiting for destination tablet %v to catch up to %v", destinationAlias, masterPos)
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
		return vterrors.Wrapf(err, "StopSlaveMinimum for %v at %v failed", destinationAlias, masterPos)
	}
	wrangler.RecordStartSlaveAction(msdw.cleaner, destinationTablet.Tablet)
	return nil
}

// restart filtered replication on the destination master.
// (remove the cleanup task that does the same)
func (msdw *MultiSplitDiffWorker) startVreplication(ctx context.Context, shardInfo *topo.ShardInfo, masterInfo *topo.TabletInfo) error {
	msdw.wr.Logger().Infof("restarting filtered replication on master %v", shardInfo.MasterAlias)
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	_, err := msdw.wr.TabletManagerClient().VReplicationExec(shortCtx, masterInfo.Tablet, binlogplayer.StartVReplication(msdw.sourceUID))
	if err != nil {
		return vterrors.Wrapf(err, "VReplicationExec(start) failed for %v", shardInfo.MasterAlias)
	}
	cancel()
	return nil
}

func (msdw *MultiSplitDiffWorker) createNonTransactionalTableScanners(ctx context.Context, queryService queryservice.QueryService, source *topo.TabletInfo) ([]TableScanner, error) {
	// If we are not using consistent snapshot, we'll use the NonTransactionalTableScanner,
	// which does not have any instance state and so can be used by all connections
	scanners := make([]TableScanner, msdw.parallelDiffsCount)
	scanner := NonTransactionalTableScanner{
		queryService: queryService,
		cleaner:      msdw.cleaner,
		wr:           msdw.wr,
		tabletAlias:  source.Alias,
	}

	for i := 0; i < msdw.parallelDiffsCount; i++ {
		scanners[i] = scanner
	}

	return scanners, nil
}

func (msdw *MultiSplitDiffWorker) useTransactionScanners(ctx context.Context, source *topo.TabletInfo) (string, []TableScanner, error) {
	connections, pos, err := CreateConsistentTableScanners(ctx, source, msdw.wr, msdw.cleaner, msdw.parallelDiffsCount)
	if err != nil {
		return "", nil, vterrors.Wrapf(err, "failed to create %d transactional connections", msdw.parallelDiffsCount)
	}
	return pos, connections, nil
}
func (msdw *MultiSplitDiffWorker) useNonTransactionalScanners(ctx context.Context, source *topo.TabletInfo, destVreplicationPos []string) (string, []TableScanner, error) {
	pos, err := msdw.stopReplicationOnSourceTabletAt(ctx, destVreplicationPos)
	if err != nil {
		return "", nil, err
	}

	queryService, err := tabletconn.GetDialer()(source.Tablet, true)
	if err != nil {
		return "", nil, vterrors.Wrapf(err, "failed to instantiate query service for %v", source.Tablet)
	}
	sourceScanners, err := msdw.createNonTransactionalTableScanners(ctx, queryService, source)
	if err != nil {
		return "", nil, err
	}

	return pos, sourceScanners, nil
}

// synchronizeSrcAndDestTxState phase:
// After this point, the source and the destination tablet are stopped at the same point.
func (msdw *MultiSplitDiffWorker) synchronizeSrcAndDestTxState(ctx context.Context) error {
	msdw.SetState(WorkerStateSyncReplication)
	var err error

	// 1. Find all the tablets we will need to work with
	masterInfos := make([]*topo.TabletInfo, len(msdw.destinationAliases))
	for i, shardInfo := range msdw.destinationShards {
		masterInfos[i], err = msdw.getMasterTabletInfoForShard(ctx, shardInfo)
		if err != nil {
			return err
		}
	}

	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	source, _ := msdw.wr.TopoServer().GetTablet(shortCtx, msdw.sourceAlias)
	cancel()

	var sourcePosition string

	// 2. Stop replication on destination
	destVreplicationPos, err := msdw.stopVreplicationOnAll(ctx, masterInfos)
	if err != nil {
		return err
	}

	// 3. Pause updates on the source and create consistent snapshot connections
	var scanners []TableScanner
	if msdw.useConsistentSnapshot {
		sourcePosition, scanners, err = msdw.useTransactionScanners(ctx, source)
	} else {
		sourcePosition, scanners, err = msdw.useNonTransactionalScanners(ctx, source, destVreplicationPos)
	}
	if err != nil {
		return err
	}

	msdw.scanners = make([]Scanners, msdw.parallelDiffsCount)
	for i := 0; i < msdw.parallelDiffsCount; i++ {
		// We'll create one of these Scanners struct per thread we want to run in parallel
		scanner := scanners[i]
		i2 := Scanners{
			sourceScanner:      scanner,
			destinationScanner: make([]TableScanner, len(msdw.destinationShards)),
		}
		msdw.scanners[i] = i2
	}

	// 4. Make sure all replicas have caught up with the master
	for i, shardInfo := range msdw.destinationShards {
		masterInfo := masterInfos[i]
		destinationAlias := msdw.destinationAliases[i]

		destinationPosition, err := msdw.stopVreplicationAt(ctx, shardInfo, sourcePosition, masterInfo)
		if err != nil {
			return err
		}

		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		destTabletInfo, err := msdw.wr.TopoServer().GetTablet(shortCtx, destinationAlias)
		cancel()
		if err != nil {
			return vterrors.Wrapf(err, "waitForDestinationTabletToReach: cannot get Tablet record for master %v", msdw.shardInfo.MasterAlias)
		}

		queryService, _ := tabletconn.GetDialer()(source.Tablet, true)
		var destScanners []TableScanner

		if msdw.useConsistentSnapshot {
			// loop to wait for the destinationAlias tablet in shardInfo to have reached destinationPosition
			err = msdw.waitForDestinationTabletToReach(ctx, destTabletInfo.Tablet, destinationPosition)
			if err != nil {
				return err
			}

			destScanners, _, err = CreateConsistentTableScanners(ctx, destTabletInfo, msdw.wr, msdw.cleaner, msdw.parallelDiffsCount)
			if err != nil {
				return err
			}
		} else {
			err = msdw.stopReplicationAt(ctx, destinationAlias, destinationPosition)
			if err != nil {
				return vterrors.Wrapf(err, "failed to stop replication on %v at position %v", destinationAlias, destinationPosition)
			}
			destScanners, err = msdw.createNonTransactionalTableScanners(ctx, queryService, destTabletInfo)
			if err != nil {
				return vterrors.Wrapf(err, "failed to stop create table scanners for %v using %v", destTabletInfo, queryService)
			}
		}

		// 5. Spread out destination scanners between the goroutines
		for j := 0; j < msdw.parallelDiffsCount; j++ {
			msdw.scanners[j].destinationScanner[i] = destScanners[j]
		}

		err = msdw.startVreplication(ctx, shardInfo, masterInfo)
		if err != nil {
			return vterrors.Wrapf(err, "failed to restart vreplication for shard %v on tablet %v", shardInfo, masterInfo)
		}

	}
	return nil
}

func (msdw *MultiSplitDiffWorker) waitForDestinationTabletToReach(ctx context.Context, tablet *topodatapb.Tablet, mysqlPos string) error {
	for i := 0; i < 20; i++ {
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		pos, err := msdw.wr.TabletManagerClient().MasterPosition(shortCtx, tablet)
		cancel()
		if err != nil {
			return vterrors.Wrapf(err, "get MasterPosition for %v failed", tablet)
		}

		if pos == mysqlPos {
			return nil
		}
		time.Sleep(time.Second)
	}
	return vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "failed to reach transaction position after multiple attempts. it is safe to try again")
}

func (msdw *MultiSplitDiffWorker) diffSingleTable(ctx context.Context, wg *sync.WaitGroup, tableDefinition *tabletmanagerdatapb.TableDefinition, keyspaceSchema *vindexes.KeyspaceSchema, sourceScanner TableScanner, destinationScanners []TableScanner) error {
	msdw.wr.Logger().Infof("Starting the diff on table %v", tableDefinition.Name)

	if len(destinationScanners) != len(msdw.destinationAliases) {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "did not receive the expected amount of destination connections")
	}

	sourceQueryResultReader, err := sourceScanner.ScanTable(ctx, tableDefinition)
	if err != nil {
		return err
	}
	defer sourceQueryResultReader.Close(ctx)

	destinationQueryResultReaders := make([]ResultReader, len(msdw.destinationAliases))
	for i := range msdw.destinationAliases {
		scanner := destinationScanners[i]
		destinationQueryResultReader, err := scanner.ScanTable(ctx, tableDefinition)
		if err != nil {
			return vterrors.Wrapf(err, "TableScan(destination) on %v failed", tableDefinition.String())
		}

		// For the first result scanner, let's check the PKs are of types that we can work with
		if i == 0 {
			err = CheckValidTypesForResultMerger(destinationQueryResultReader.fields, len(tableDefinition.PrimaryKeyColumns))
			if err != nil {
				return vterrors.Wrapf(err, "invalid types for multi split diff. use the regular split diff instead")
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
		return err
	}

	// Create the row differ.
	differ, err := NewRowDiffer(sourceQueryResultReader, mergedResultReader, tableDefinition)
	if err != nil {
		return err
	}

	// And run the diff.
	report, err := differ.Go(msdw.wr.Logger())
	if err != nil {
		return err
	}

	if report.HasDifferences() {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "table %v has differences: %v", tableDefinition.Name, report.String())
	}

	msdw.wr.Logger().Infof("table %v checks out (%v rows processed, %v qps)", tableDefinition.Name, report.processedRows, report.processingQPS)

	return nil
}

func (msdw *MultiSplitDiffWorker) tableDiffingConsumer(ctx context.Context, wg *sync.WaitGroup, tableChan chan *tabletmanagerdatapb.TableDefinition, rec *concurrency.AllErrorRecorder, keyspaceSchema *vindexes.KeyspaceSchema, sourceScanner TableScanner, destinationScanners []TableScanner) {
	if destinationScanners == nil || sourceScanner == nil {
		err := vterrors.Errorf(vtrpc.Code_INTERNAL, "should not be nil %v %v", destinationScanners, sourceScanner)
		msdw.markAsWillFail(rec, err)
		msdw.wr.Logger().Errorf("%v", err)
		return
	}
	defer wg.Done()

	for tableDefinition := range tableChan {
		err := msdw.diffSingleTable(ctx, wg, tableDefinition, keyspaceSchema, sourceScanner, destinationScanners)
		if err != nil {
			msdw.markAsWillFail(rec, err)
			msdw.wr.Logger().Errorf("%v", err)
		}
	}
}

func (msdw *MultiSplitDiffWorker) gatherSchemaInfo(ctx context.Context) ([]*tabletmanagerdatapb.SchemaDefinition, *tabletmanagerdatapb.SchemaDefinition, error) {
	msdw.wr.Logger().Infof("gathering schema information...")
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
			if err != nil {
				msdw.markAsWillFail(rec, err)
			}
			destinationSchemaDefinitions[i] = destinationSchemaDefinition
			msdw.wr.Logger().Infof("got schema from destination %v", destinationAlias)
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
		if err != nil {
			msdw.markAsWillFail(rec, err)
		}
		msdw.wr.Logger().Infof("got schema from source %v", msdw.sourceAlias)
		wg.Done()
	}()

	wg.Wait()
	if rec.HasErrors() {
		return nil, nil, rec.Error()
	}

	return destinationSchemaDefinitions, sourceSchemaDefinition, nil
}

func (msdw *MultiSplitDiffWorker) diffSchemaInformation(ctx context.Context, destinationSchemaDefinitions []*tabletmanagerdatapb.SchemaDefinition, sourceSchemaDefinition *tabletmanagerdatapb.SchemaDefinition) error {
	msdw.wr.Logger().Infof("diffing the schema...")
	rec := &concurrency.AllErrorRecorder{}
	sourceShardName := fmt.Sprintf("%v/%v", msdw.shardInfo.Keyspace(), msdw.shardInfo.ShardName())
	for i, destinationSchemaDefinition := range destinationSchemaDefinitions {
		destinationShard := msdw.destinationShards[i]
		destinationShardName := fmt.Sprintf("%v/%v", destinationShard.Keyspace(), destinationShard.ShardName())
		tmutils.DiffSchema(destinationShardName, destinationSchemaDefinition, sourceShardName, sourceSchemaDefinition, rec)
	}
	if rec.HasErrors() {
		msdw.wr.Logger().Warningf("different schemas: %v", rec.Error().Error())
		return rec.Error()
	}

	msdw.wr.Logger().Infof("schema match, good.")
	return nil
}

func (msdw *MultiSplitDiffWorker) loadVSchema(ctx context.Context) (*vindexes.KeyspaceSchema, error) {
	shortCtx, cancel := context.WithCancel(ctx)
	kschema, err := msdw.wr.TopoServer().GetVSchema(shortCtx, msdw.keyspace)
	cancel()
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot load VSchema for keyspace %v", msdw.keyspace)
	}
	if kschema == nil {
		return nil, vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "no VSchema for keyspace %v", msdw.keyspace)
	}

	keyspaceSchema, err := vindexes.BuildKeyspaceSchema(kschema, msdw.keyspace)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot build vschema for keyspace %v", msdw.keyspace)
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
	err = msdw.diffSchemaInformation(ctx, destinationSchemaDefinitions, sourceSchemaDefinition)
	if err != nil {
		return err
	}

	// read the vschema if needed
	var keyspaceSchema *vindexes.KeyspaceSchema
	if *useV3ReshardingMode {
		keyspaceSchema, err = msdw.loadVSchema(ctx)
		if err != nil {
			return err
		}
	}

	msdw.wr.Logger().Infof("running the diffs...")
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
		scanners := msdw.scanners[i]
		if scanners.sourceScanner == nil || scanners.destinationScanner == nil {
			return vterrors.Errorf(vtrpc.Code_INTERNAL, "should not be nil [%v] OR [%v]", scanners.sourceScanner, scanners.destinationScanner)
		}

		consumers.Add(1)
		go msdw.tableDiffingConsumer(ctx, &consumers, tableChan, rec, keyspaceSchema, scanners.sourceScanner, scanners.destinationScanner)
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
