// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	tm "github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

const (
	// all the states for the worker
	stateNotSarted = "not started"
	stateDone      = "done"
	stateError     = "error"

	stateInit                   = "initializing"
	stateFindTargets            = "finding target instances"
	stateSynchronizeReplication = "synchronizing replication"
	stateDiff                   = "running the diff"
	stateCleanUp                = "cleaning up"
)

// SplitDiffWorker executes a diff between a destination shard and its
// source shards in a shard split case.
type SplitDiffWorker struct {
	wr       *wrangler.Wrangler
	cell     string
	keyspace string
	shard    string
	cleaner  *wrangler.Cleaner

	// all subsequent fields are protected by the mutex
	mu    sync.Mutex
	state string

	// populated if state == stateError
	err error

	// populated during stateInit, read-only after that
	shardInfo *topo.ShardInfo

	// populated during stateFindTargets, read-only after that
	sourceAliases    []topo.TabletAlias
	destinationAlias topo.TabletAlias

	// populated during stateDiff
	diffLogs                    []string
	sourceSchemaDefinitions     []*mysqlctl.SchemaDefinition
	destinationSchemaDefinition *mysqlctl.SchemaDefinition
}

// NewSplitDiff returns a new SplitDiffWorker object.
func NewSplitDiffWorker(wr *wrangler.Wrangler, cell, keyspace, shard string) Worker {
	return &SplitDiffWorker{
		wr:       wr,
		cell:     cell,
		keyspace: keyspace,
		shard:    shard,
		cleaner:  &wrangler.Cleaner{},

		state: stateNotSarted,
	}
}

func (sdw *SplitDiffWorker) setState(state string) {
	sdw.mu.Lock()
	sdw.state = state
	sdw.mu.Unlock()
}

func (sdw *SplitDiffWorker) recordError(err error) {
	sdw.mu.Lock()
	sdw.state = stateError
	sdw.err = err
	sdw.mu.Unlock()
}

func (sdw *SplitDiffWorker) StatusAsHTML() string {
	sdw.mu.Lock()
	defer sdw.mu.Unlock()
	result := "<b>Working on:</b> " + sdw.keyspace + "/" + sdw.shard + "</br>\n"
	result += "<b>State:</b> " + sdw.state + "</br>\n"
	switch sdw.state {
	case stateError:
		result += "<b>Error</b>: " + sdw.err.Error() + "</br>\n"
	case stateDone:
		result += "<b>Success</b>:</br>\n"
		result += strings.Join(sdw.diffLogs, "</br>\n")
	}

	return result
}

func (sdw *SplitDiffWorker) StatusAsText() string {
	sdw.mu.Lock()
	defer sdw.mu.Unlock()
	result := "Working on: " + sdw.keyspace + "/" + sdw.shard + "\n"
	result += "State: " + sdw.state + "\n"
	switch sdw.state {
	case stateError:
		result += "Error: " + sdw.err.Error() + "\n"
	case stateDone:
		result += "Success:\n"
		result += strings.Join(sdw.diffLogs, "\n")
	}
	return result
}

func (sdw *SplitDiffWorker) CheckInterrupted() bool {
	select {
	case <-interrupted:
		sdw.recordError(topo.ErrInterrupted)
		return true
	default:
	}
	return false
}

// Run is mostly a wrapper to run the cleanup at the end.
func (sdw *SplitDiffWorker) Run() {
	err := sdw.run()

	sdw.setState(stateCleanUp)
	cerr := sdw.cleaner.CleanUp(sdw.wr)
	if cerr != nil {
		if err != nil {
			log.Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}
	if err != nil {
		sdw.recordError(err)
		return
	}
	sdw.setState(stateDone)
}

func (sdw *SplitDiffWorker) run() error {
	// first state: read what we need to do
	if err := sdw.init(); err != nil {
		return err
	}
	if sdw.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// second state: find targets
	if err := sdw.findTargets(); err != nil {
		return err
	}
	if sdw.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// third phase: synchronize replication
	if err := sdw.synchronizeReplication(); err != nil {
		return err
	}
	if sdw.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// fourth phase: diff
	if err := sdw.diff(); err != nil {
		return err
	}

	return nil
}

// init phase:
// - read the shard info, make sure it has sources
func (sdw *SplitDiffWorker) init() error {
	sdw.setState(stateInit)

	var err error
	sdw.shardInfo, err = sdw.wr.TopoServer().GetShard(sdw.keyspace, sdw.shard)
	if err != nil {
		return fmt.Errorf("Cannot read shard %v/%v: %v", sdw.keyspace, sdw.shard, err)
	}

	if len(sdw.shardInfo.SourceShards) == 0 {
		return fmt.Errorf("Shard %v/%v has no source shard", sdw.keyspace, sdw.shard)
	}
	if sdw.shardInfo.MasterAlias.IsZero() {
		return fmt.Errorf("Shard %v/%v has no master")
	}

	return nil
}

// findTargets phase:
// - find one rdonly per source shard
// - find one rdonly in destination shard
// - mark them all as 'checker' pointing back to us

func (sdw *SplitDiffWorker) findTarget(shard string) (topo.TabletAlias, error) {
	endPoints, err := sdw.wr.TopoServer().GetEndPoints(sdw.cell, sdw.keyspace, shard, topo.TYPE_RDONLY)
	if err != nil {
		return topo.TabletAlias{}, fmt.Errorf("GetEndPoints(%v,%v,%v,rdonly) failed: %v", sdw.cell, sdw.keyspace, shard, err)
	}
	if len(endPoints.Entries) == 0 {
		return topo.TabletAlias{}, fmt.Errorf("No endpoint to chose from in (%v,%v/%v)", sdw.cell, sdw.keyspace, shard)
	}

	tabletAlias := topo.TabletAlias{
		Cell: sdw.cell,
		Uid:  endPoints.Entries[0].Uid,
	}
	log.Infof("Changing tablet %v to 'checker'", tabletAlias)
	if err := sdw.wr.ChangeType(tabletAlias, topo.TYPE_CHECKER, false /*force*/); err != nil {
		return topo.TabletAlias{}, err
	}
	ourUrl := servenv.ListeningUrl.String()
	log.Infof("Adding tag[worker]=%v to tablet %v", ourUrl, tabletAlias)
	if err := sdw.wr.TopoServer().UpdateTabletFields(tabletAlias, func(tablet *topo.Tablet) error {
		if tablet.Tags == nil {
			tablet.Tags = make(map[string]string)
		}
		tablet.Tags["worker"] = ourUrl
		return nil
	}); err != nil {
		return topo.TabletAlias{}, err
	}
	wrangler.RecordTabletTagAction(sdw.cleaner, tabletAlias, "worker", "")

	// Record a clean-up action to take the tablet back to rdonly.
	// We will alter this one later on and let the tablet go back to
	// 'spare' if we have stopped replication for too long on it.
	wrangler.RecordChangeSlaveTypeAction(sdw.cleaner, tabletAlias, topo.TYPE_RDONLY)
	return tabletAlias, nil
}

func (sdw *SplitDiffWorker) findTargets() error {
	sdw.setState(stateFindTargets)

	// find an appropriate endpoint in destination shard
	var err error
	sdw.destinationAlias, err = sdw.findTarget(sdw.shard)
	if err != nil {
		return err
	}

	// find an appropriate endpoint in the source shards
	sdw.sourceAliases = make([]topo.TabletAlias, len(sdw.shardInfo.SourceShards))
	for i, ss := range sdw.shardInfo.SourceShards {
		sdw.sourceAliases[i], err = sdw.findTarget(ss.Shard)
		if err != nil {
			return err
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
	sdw.setState(stateSynchronizeReplication)

	// 1 - stop the master binlog replication, get its current position
	log.Infof("Stopping master binlog replication on %v", sdw.shardInfo.MasterAlias)
	blpPositionList, err := sdw.wr.ActionInitiator().StopBlp(sdw.shardInfo.MasterAlias, 30*time.Second)
	if err != nil {
		return err
	}
	wrangler.RecordStartBlpAction(sdw.cleaner, sdw.shardInfo.MasterAlias, 30*time.Second)

	// 2 - stop all the source 'checker' at a binlog position
	//     higher than the destination master
	stopPositionList := tm.BlpPositionList{
		Entries: make([]mysqlctl.BlpPosition, len(sdw.shardInfo.SourceShards)),
	}
	for i, ss := range sdw.shardInfo.SourceShards {
		// find where we should be stopping
		pos, err := blpPositionList.FindBlpPositionById(ss.Uid)
		if err != nil {
			return fmt.Errorf("No binlog position on the master for Uid %v", ss.Uid)
		}

		// stop replication
		log.Infof("Stopping slave[%v] %v at a minimum of %v", i, sdw.sourceAliases[i], pos.GroupId)
		stoppedAt, err := sdw.wr.ActionInitiator().StopSlaveMinimum(sdw.sourceAliases[i], pos.GroupId, 30*time.Second)
		if err != nil {
			return fmt.Errorf("Cannot stop slave %v at right binlog position %v: %v", sdw.sourceAliases[i], pos.GroupId, err)
		}
		stopPositionList.Entries[i].Uid = ss.Uid
		stopPositionList.Entries[i].GroupId = stoppedAt.MasterLogGroupId

		// change the cleaner actions from ChangeSlaveType(rdonly)
		// to StartSlave() + ChangeSlaveType(spare)
		wrangler.RecordStartSlaveAction(sdw.cleaner, sdw.sourceAliases[i], 30*time.Second)
		action, err := wrangler.FindChangeSlaveTypeActionByTarget(sdw.cleaner, sdw.sourceAliases[i])
		if err != nil {
			return fmt.Errorf("cannot find ChangeSlaveType action for %v: %v", sdw.sourceAliases[i], err)
		}
		action.TabletType = topo.TYPE_SPARE
	}

	// 3 - ask the master of the destination shard to resume filtered
	//     replication up to the new list of positions
	log.Infof("Restarting master %v until it catches up to %v", sdw.shardInfo.MasterAlias, stopPositionList)
	masterPos, err := sdw.wr.ActionInitiator().RunBlpUntil(sdw.shardInfo.MasterAlias, &stopPositionList, 30*time.Second)
	if err != nil {
		return err
	}

	// 4 - wait until the destination checker is equal or passed
	//     that master binlog position, and stop its replication.
	log.Infof("Waiting for destination checker %v to catch up to %v", sdw.destinationAlias, masterPos.MasterLogGroupId)
	_, err = sdw.wr.ActionInitiator().StopSlaveMinimum(sdw.destinationAlias, masterPos.MasterLogGroupId, 30*time.Second)
	if err != nil {
		return err
	}
	wrangler.RecordStartSlaveAction(sdw.cleaner, sdw.destinationAlias, 30*time.Second)
	action, err := wrangler.FindChangeSlaveTypeActionByTarget(sdw.cleaner, sdw.destinationAlias)
	if err != nil {
		return fmt.Errorf("cannot find ChangeSlaveType action for %v: %v", sdw.destinationAlias, err)
	}
	action.TabletType = topo.TYPE_SPARE

	// 5 - restart filtered replication on destination master
	log.Infof("Restarting filtered replication on master %v", sdw.shardInfo.MasterAlias)
	err = sdw.wr.ActionInitiator().StartBlp(sdw.shardInfo.MasterAlias, 30*time.Second)
	if err := sdw.cleaner.RemoveActionByName(wrangler.StartBlpActionName, sdw.shardInfo.MasterAlias.String()); err != nil {
		log.Warningf("Cannot find cleaning action %v/%v: %v", wrangler.StartBlpActionName, sdw.shardInfo.MasterAlias.String(), err)
	}
	if err != nil {
		return err
	}

	return nil
}

// diff phase: will create a list of messages regarding the diff.
// - get the schema on all checkers
// - if some table schema mismatches, record them (use existing schema diff tools).
// - for each table in destination, run a diff pipeline.

func (sdw *SplitDiffWorker) diffLog(msg string) {
	sdw.mu.Lock()
	sdw.diffLogs = append(sdw.diffLogs, msg)
	sdw.mu.Unlock()
	log.Infof("diffLog: %v", msg)
}

func (sdw *SplitDiffWorker) diff() error {
	sdw.setState(stateDiff)

	sdw.diffLog("Gathering schema information...")
	sdw.sourceSchemaDefinitions = make([]*mysqlctl.SchemaDefinition, len(sdw.sourceAliases))
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	wg.Add(1)
	go func() {
		var err error
		sdw.destinationSchemaDefinition, err = sdw.wr.GetSchema(sdw.destinationAlias, nil, false)
		rec.RecordError(err)
		sdw.diffLog(fmt.Sprintf("Got schema from destination %v", sdw.destinationAlias))
		wg.Done()
	}()
	for i, sourceAlias := range sdw.sourceAliases {
		wg.Add(1)
		go func(i int, sourceAlias topo.TabletAlias) {
			var err error
			sdw.sourceSchemaDefinitions[i], err = sdw.wr.GetSchema(sourceAlias, nil, false)
			rec.RecordError(err)
			sdw.diffLog(fmt.Sprintf("Got schema from source[%v] %v", i, sourceAlias))
			wg.Done()
		}(i, sourceAlias)
	}
	wg.Wait()
	if rec.HasErrors() {
		return rec.Error()
	}

	// TODO(alainjobart) Checking against each source may be
	// overkill, if all sources have the same schema?
	sdw.diffLog("Diffing the schema...")
	rec = concurrency.AllErrorRecorder{}
	for i, sourceSchemaDefinition := range sdw.sourceSchemaDefinitions {
		sourceName := fmt.Sprintf("source[%v]", i)
		mysqlctl.DiffSchema("destination", sdw.destinationSchemaDefinition, sourceName, sourceSchemaDefinition, &rec)
	}
	if rec.HasErrors() {
		sdw.diffLog("Different schemas: " + rec.Error().Error())
	} else {
		sdw.diffLog("Schema match, good.")
	}

	// run the diffs, 8 at a time
	sdw.diffLog("Running the diffs...")
	sem := sync2.NewSemaphore(8, 0)
	for _, tableDefinition := range sdw.destinationSchemaDefinition.TableDefinitions {
		wg.Add(1)
		go func(tableDefinition mysqlctl.TableDefinition) {
			defer wg.Done()
			sem.Acquire()
			defer sem.Release()

			log.Infof("Starting the diff on table %v", tableDefinition.Name)
			if len(sdw.sourceAliases) != 1 {
				sdw.diffLog("Don't support more than one source for table yet: " + tableDefinition.Name)
				return
			}

			overlap, err := key.KeyRangesOverlap(sdw.shardInfo.KeyRange, sdw.shardInfo.SourceShards[0].KeyRange)
			if err != nil {
				sdw.diffLog("Source shard doesn't overlap with destination????: " + err.Error())
				return
			}
			sourceQueryResultReader, err := FullTableScan(sdw.wr.TopoServer(), sdw.sourceAliases[0], &tableDefinition, overlap)
			if err != nil {
				sdw.diffLog("FullTableScan(source) failed: " + err.Error())
				return
			}
			defer sourceQueryResultReader.Close()

			destinationQueryResultReader, err := FullTableScan(sdw.wr.TopoServer(), sdw.destinationAlias, &tableDefinition, key.KeyRange{})
			if err != nil {
				sdw.diffLog("FullTableScan(destination) failed: " + err.Error())
				return
			}
			defer destinationQueryResultReader.Close()

			differ, err := NewRowDiffer(sourceQueryResultReader, destinationQueryResultReader, &tableDefinition)
			if err != nil {
				sdw.diffLog("NewRowDiffer() failed: " + err.Error())
				return
			}

			report, err := differ.Go()
			if err != nil {
				sdw.diffLog("Differ.Go failed: " + err.Error())
			} else {
				if report.HasDifferences() {
					sdw.diffLog(fmt.Sprintf("Table %v has differences: %v", tableDefinition.Name, report))
				} else {
					sdw.diffLog(fmt.Sprintf("Table %v checks out (%v rows processed)", tableDefinition.Name, report.processedRows))
				}
			}
		}(tableDefinition)
	}
	wg.Wait()

	return nil
}
