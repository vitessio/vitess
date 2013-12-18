// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/mysqlctl"
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
	var err error
	if err = sdw.run(); err != nil {
		sdw.recordError(err)
	}

	sdw.setState(stateCleanUp)
	cerr := sdw.cleaner.CleanUp(sdw.wr)
	if cerr != nil {
		if err != nil {
			log.Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			sdw.recordError(cerr)
			err = cerr
		}
	}
	if err == nil {
		sdw.setState(stateDone)
	}
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
// TODO(alainjobart) add a tag pointing back to us to the checker instances

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

func findBlpPositionById(id uint32, list *tm.BlpPositionList) (*mysqlctl.BlpPosition, error) {
	for _, pos := range list.Entries {
		if pos.Uid == id {
			return &pos, nil
		}
	}
	return nil, topo.ErrNoNode
}

func (sdw *SplitDiffWorker) synchronizeReplication() error {
	sdw.setState(stateSynchronizeReplication)

	// 1 - stop the master binlog replication, get its current position
	blpPositionList, err := sdw.wr.ActionInitiator().StopBlp(sdw.shardInfo.MasterAlias, 30*time.Second)
	if err != nil {
		return err
	}
	wrangler.RecordStartBlpAction(sdw.cleaner, sdw.shardInfo.MasterAlias, 30*time.Second)

	// 2 - stop all the source 'checker' at a binlog position
	//     higher than the destination master
	sourcePositions := make([]*mysqlctl.ReplicationPosition, len(sdw.shardInfo.SourceShards))
	for i, ss := range sdw.shardInfo.SourceShards {
		// find where we should be stopping
		pos, err := findBlpPositionById(ss.Uid, blpPositionList)
		if err != nil {
			return fmt.Errorf("No binlog position on the master for Uid %v", ss.Uid)
		}

		// stop replication
		sourcePositions[i], err = sdw.wr.ActionInitiator().StopSlaveMinimum(sdw.sourceAliases[i], pos.GroupId, 30*time.Second)
		if err != nil {
			return fmt.Errorf("Cannot stop slave %v at right binlog position %v: %v", sdw.sourceAliases[i], pos.GroupId, err)
		}

		// change the cleaner actions from ChangeSlaveType(rdonly)
		// to StartSlave() + ChangeSlaveType(spare)
		wrangler.RecordStartSlaveAction(sdw.cleaner, sdw.sourceAliases[i], 30*time.Second)
		action, err := wrangler.FindChangeSlaveTypeActionByTarget(sdw.cleaner, sdw.sourceAliases[i])
		if err != nil {
			return fmt.Errorf("cannot find ChangeSlaveType action for %v: %v", sdw.sourceAliases[i], err)
		}
		action.TabletType = topo.TYPE_SPARE
	}

	// 5 - restart filtered replication on destination master
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

	// TODO(alainjobart) Checking against each source may be overkill, if all
	// sources have the same schema?
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

	return nil
}
