// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"html/template"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sync2"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

const (
	// all the states for the worker
	stateSDNotSarted = "not started"
	stateSDDone      = "done"
	stateSDError     = "error"

	stateSDInit                   = "initializing"
	stateSDFindTargets            = "finding target instances"
	stateSDSynchronizeReplication = "synchronizing replication"
	stateSDDiff                   = "running the diff"
	stateSDCleanUp                = "cleaning up"
)

// SplitDiffWorker executes a diff between a destination shard and its
// source shards in a shard split case.
type SplitDiffWorker struct {
	wr        *wrangler.Wrangler
	cell      string
	keyspace  string
	shard     string
	cleaner   *wrangler.Cleaner
	ctx       context.Context
	ctxCancel context.CancelFunc

	// all subsequent fields are protected by the mutex
	mu    sync.Mutex
	state string

	// populated if state == stateSDError
	err error

	// populated during stateSDInit, read-only after that
	keyspaceInfo *topo.KeyspaceInfo
	shardInfo    *topo.ShardInfo

	// populated during stateSDFindTargets, read-only after that
	sourceAliases    []topo.TabletAlias
	destinationAlias topo.TabletAlias

	// populated during stateSDDiff
	sourceSchemaDefinitions     []*myproto.SchemaDefinition
	destinationSchemaDefinition *myproto.SchemaDefinition
}

// NewSplitDiffWorker returns a new SplitDiffWorker object.
func NewSplitDiffWorker(wr *wrangler.Wrangler, cell, keyspace, shard string) Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &SplitDiffWorker{
		wr:        wr,
		cell:      cell,
		keyspace:  keyspace,
		shard:     shard,
		cleaner:   &wrangler.Cleaner{},
		ctx:       ctx,
		ctxCancel: cancel,

		state: stateSDNotSarted,
	}
}

func (sdw *SplitDiffWorker) setState(state string) {
	sdw.mu.Lock()
	sdw.state = state
	sdw.mu.Unlock()
}

func (sdw *SplitDiffWorker) recordError(err error) {
	sdw.mu.Lock()
	sdw.state = stateSDError
	sdw.err = err
	sdw.mu.Unlock()
}

// StatusAsHTML is part of the Worker interface
func (sdw *SplitDiffWorker) StatusAsHTML() template.HTML {
	sdw.mu.Lock()
	defer sdw.mu.Unlock()
	result := "<b>Working on:</b> " + sdw.keyspace + "/" + sdw.shard + "</br>\n"
	result += "<b>State:</b> " + sdw.state + "</br>\n"
	switch sdw.state {
	case stateSDError:
		result += "<b>Error</b>: " + sdw.err.Error() + "</br>\n"
	case stateSDDiff:
		result += "<b>Running...</b></br>\n"
	case stateSDDone:
		result += "<b>Success.</b></br>\n"
	}

	return template.HTML(result)
}

// StatusAsText is part of the Worker interface
func (sdw *SplitDiffWorker) StatusAsText() string {
	sdw.mu.Lock()
	defer sdw.mu.Unlock()
	result := "Working on: " + sdw.keyspace + "/" + sdw.shard + "\n"
	result += "State: " + sdw.state + "\n"
	switch sdw.state {
	case stateSDError:
		result += "Error: " + sdw.err.Error() + "\n"
	case stateSDDiff:
		result += "Running...\n"
	case stateSDDone:
		result += "Success.\n"
	}
	return result
}

// Cancel is part of the Worker interface
func (sdw *SplitDiffWorker) Cancel() {
	sdw.ctxCancel()
}

func (sdw *SplitDiffWorker) checkInterrupted() bool {
	select {
	case <-sdw.ctx.Done():
		if sdw.ctx.Err() == context.DeadlineExceeded {
			return false
		}
		sdw.recordError(topo.ErrInterrupted)
		return true
	default:
	}
	return false
}

// Run is mostly a wrapper to run the cleanup at the end.
func (sdw *SplitDiffWorker) Run() {
	err := sdw.run()

	sdw.setState(stateSDCleanUp)
	cerr := sdw.cleaner.CleanUp(sdw.wr)
	if cerr != nil {
		if err != nil {
			sdw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}
	if err != nil {
		sdw.recordError(err)
		return
	}
	sdw.setState(stateSDDone)
}

func (sdw *SplitDiffWorker) Error() error {
	return sdw.err
}

func (sdw *SplitDiffWorker) run() error {
	// first state: read what we need to do
	if err := sdw.init(); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if sdw.checkInterrupted() {
		return topo.ErrInterrupted
	}

	// second state: find targets
	if err := sdw.findTargets(); err != nil {
		return fmt.Errorf("findTargets() failed: %v", err)
	}
	if sdw.checkInterrupted() {
		return topo.ErrInterrupted
	}

	// third phase: synchronize replication
	if err := sdw.synchronizeReplication(); err != nil {
		if sdw.checkInterrupted() {
			return topo.ErrInterrupted
		}
		return fmt.Errorf("synchronizeReplication() failed: %v", err)
	}
	if sdw.checkInterrupted() {
		return topo.ErrInterrupted
	}

	// fourth phase: diff
	if err := sdw.diff(); err != nil {
		if sdw.checkInterrupted() {
			return topo.ErrInterrupted
		}
		return fmt.Errorf("diff() failed: %v", err)
	}

	return nil
}

// init phase:
// - read the shard info, make sure it has sources
func (sdw *SplitDiffWorker) init() error {
	sdw.setState(stateSDInit)

	var err error
	sdw.keyspaceInfo, err = sdw.wr.TopoServer().GetKeyspace(sdw.keyspace)
	if err != nil {
		return fmt.Errorf("cannot read keyspace %v: %v", sdw.keyspace, err)
	}
	sdw.shardInfo, err = sdw.wr.TopoServer().GetShard(sdw.keyspace, sdw.shard)
	if err != nil {
		return fmt.Errorf("cannot read shard %v/%v: %v", sdw.keyspace, sdw.shard, err)
	}

	if len(sdw.shardInfo.SourceShards) == 0 {
		return fmt.Errorf("shard %v/%v has no source shard", sdw.keyspace, sdw.shard)
	}
	if sdw.shardInfo.MasterAlias.IsZero() {
		return fmt.Errorf("shard %v/%v has no master", sdw.keyspace, sdw.shard)
	}

	return nil
}

// findTargets phase:
// - find one rdonly per source shard
// - find one rdonly in destination shard
// - mark them all as 'checker' pointing back to us
func (sdw *SplitDiffWorker) findTargets() error {
	sdw.setState(stateSDFindTargets)

	// find an appropriate endpoint in destination shard
	var err error
	sdw.destinationAlias, err = findChecker(sdw.ctx, sdw.wr, sdw.cleaner, sdw.cell, sdw.keyspace, sdw.shard)
	if err != nil {
		return fmt.Errorf("cannot find checker for %v/%v/%v: %v", sdw.cell, sdw.keyspace, sdw.shard, err)
	}

	// find an appropriate endpoint in the source shards
	sdw.sourceAliases = make([]topo.TabletAlias, len(sdw.shardInfo.SourceShards))
	for i, ss := range sdw.shardInfo.SourceShards {
		sdw.sourceAliases[i], err = findChecker(sdw.ctx, sdw.wr, sdw.cleaner, sdw.cell, sdw.keyspace, ss.Shard)
		if err != nil {
			return fmt.Errorf("cannot find checker for %v/%v/%v: %v", sdw.cell, sdw.keyspace, ss.Shard, err)
		}
	}

	return nil
}

// synchronizeReplication phase:
// 1 - ask the master of the destination shard to pause filtered replication,
//   and return the source binlog positions
//   (add a cleanup task to restart filtered replication on master)
// 2 - stop all the source 'checker' at a binlog position higher than the
//   destination master. Get that new list of positions.
//   (add a cleanup task to restart binlog replication on them, and change
//    the existing ChangeSlaveType cleanup action to 'spare' type)
// 3 - ask the master of the destination shard to resume filtered replication
//   up to the new list of positions, and return its binlog position.
// 4 - wait until the destination checker is equal or passed that master binlog
//   position, and stop its replication.
//   (add a cleanup task to restart binlog replication on it, and change
//    the existing ChangeSlaveType cleanup action to 'spare' type)
// 5 - restart filtered replication on destination master.
//   (remove the cleanup task that does the same)
// At this point, all checker instances are stopped at the same point.

func (sdw *SplitDiffWorker) synchronizeReplication() error {
	sdw.setState(stateSDSynchronizeReplication)

	masterInfo, err := sdw.wr.TopoServer().GetTablet(sdw.shardInfo.MasterAlias)
	if err != nil {
		return fmt.Errorf("synchronizeReplication: cannot get Tablet record for master %v: %v", sdw.shardInfo.MasterAlias, err)
	}

	// 1 - stop the master binlog replication, get its current position
	sdw.wr.Logger().Infof("Stopping master binlog replication on %v", sdw.shardInfo.MasterAlias)
	ctx, cancel := context.WithTimeout(sdw.ctx, 60*time.Second)
	blpPositionList, err := sdw.wr.TabletManagerClient().StopBlp(ctx, masterInfo)
	cancel()
	if err != nil {
		return fmt.Errorf("StopBlp for %v failed: %v", sdw.shardInfo.MasterAlias, err)
	}
	wrangler.RecordStartBlpAction(sdw.cleaner, masterInfo)

	// 2 - stop all the source 'checker' at a binlog position
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
		sourceTablet, err := sdw.wr.TopoServer().GetTablet(sdw.sourceAliases[i])
		if err != nil {
			return err
		}

		// stop replication
		sdw.wr.Logger().Infof("Stopping slave[%v] %v at a minimum of %v", i, sdw.sourceAliases[i], blpPos.Position)
		ctx, cancel := context.WithTimeout(sdw.ctx, 60*time.Second)
		stoppedAt, err := sdw.wr.TabletManagerClient().StopSlaveMinimum(ctx, sourceTablet, blpPos.Position, 30*time.Second)
		cancel()
		if err != nil {
			return fmt.Errorf("cannot stop slave %v at right binlog position %v: %v", sdw.sourceAliases[i], blpPos.Position, err)
		}
		stopPositionList.Entries[i].Uid = ss.Uid
		stopPositionList.Entries[i].Position = stoppedAt.Position

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
	ctx, cancel = context.WithTimeout(sdw.ctx, 60*time.Second)
	masterPos, err := sdw.wr.TabletManagerClient().RunBlpUntil(ctx, masterInfo, &stopPositionList, 30*time.Second)
	cancel()
	if err != nil {
		return fmt.Errorf("RunBlpUntil for %v until %v failed: %v", sdw.shardInfo.MasterAlias, stopPositionList, err)
	}

	// 4 - wait until the destination checker is equal or passed
	//     that master binlog position, and stop its replication.
	sdw.wr.Logger().Infof("Waiting for destination checker %v to catch up to %v", sdw.destinationAlias, masterPos)
	destinationTablet, err := sdw.wr.TopoServer().GetTablet(sdw.destinationAlias)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(sdw.ctx, 60*time.Second)
	_, err = sdw.wr.TabletManagerClient().StopSlaveMinimum(ctx, destinationTablet, masterPos, 30*time.Second)
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
	ctx, cancel = context.WithTimeout(sdw.ctx, 60*time.Second)
	err = sdw.wr.TabletManagerClient().StartBlp(ctx, masterInfo)
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
// - get the schema on all checkers
// - if some table schema mismatches, record them (use existing schema diff tools).
// - for each table in destination, run a diff pipeline.

func (sdw *SplitDiffWorker) diff() error {
	sdw.setState(stateSDDiff)

	sdw.wr.Logger().Infof("Gathering schema information...")
	sdw.sourceSchemaDefinitions = make([]*myproto.SchemaDefinition, len(sdw.sourceAliases))
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	wg.Add(1)
	go func() {
		var err error
		ctx, cancel := context.WithTimeout(sdw.ctx, 60*time.Second)
		sdw.destinationSchemaDefinition, err = sdw.wr.GetSchema(ctx, sdw.destinationAlias, nil, nil, false)
		cancel()
		rec.RecordError(err)
		sdw.wr.Logger().Infof("Got schema from destination %v", sdw.destinationAlias)
		wg.Done()
	}()
	for i, sourceAlias := range sdw.sourceAliases {
		wg.Add(1)
		go func(i int, sourceAlias topo.TabletAlias) {
			var err error
			ctx, cancel := context.WithTimeout(sdw.ctx, 60*time.Second)
			sdw.sourceSchemaDefinitions[i], err = sdw.wr.GetSchema(ctx, sourceAlias, nil, nil, false)
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
				sdw.wr.Logger().Errorf("Don't support more than one source for table yet: %v", tableDefinition.Name)
				return
			}

			overlap, err := key.KeyRangesOverlap(sdw.shardInfo.KeyRange, sdw.shardInfo.SourceShards[0].KeyRange)
			if err != nil {
				sdw.wr.Logger().Errorf("Source shard doesn't overlap with destination????: %v", err)
				return
			}
			sourceQueryResultReader, err := TableScanByKeyRange(sdw.ctx, sdw.wr.Logger(), sdw.wr.TopoServer(), sdw.sourceAliases[0], tableDefinition, overlap, sdw.keyspaceInfo.ShardingColumnType)
			if err != nil {
				sdw.wr.Logger().Errorf("TableScanByKeyRange(source) failed: %v", err)
				return
			}
			defer sourceQueryResultReader.Close()

			destinationQueryResultReader, err := TableScanByKeyRange(sdw.ctx, sdw.wr.Logger(), sdw.wr.TopoServer(), sdw.destinationAlias, tableDefinition, key.KeyRange{}, sdw.keyspaceInfo.ShardingColumnType)
			if err != nil {
				sdw.wr.Logger().Errorf("TableScanByKeyRange(destination) failed: %v", err)
				return
			}
			defer destinationQueryResultReader.Close()

			differ, err := NewRowDiffer(sourceQueryResultReader, destinationQueryResultReader, tableDefinition)
			if err != nil {
				sdw.wr.Logger().Errorf("NewRowDiffer() failed: %v", err)
				return
			}

			report, err := differ.Go(sdw.wr.Logger())
			if err != nil {
				sdw.wr.Logger().Errorf("Differ.Go failed: %v", err.Error())
			} else {
				if report.HasDifferences() {
					sdw.wr.Logger().Warningf("Table %v has differences: %v", tableDefinition.Name, report.String())
				} else {
					sdw.wr.Logger().Infof("Table %v checks out (%v rows processed, %v qps)", tableDefinition.Name, report.processedRows, report.processingQPS)
				}
			}
		}(tableDefinition)
	}
	wg.Wait()

	return nil
}
