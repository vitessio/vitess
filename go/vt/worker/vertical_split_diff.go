// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"html/template"
	"regexp"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sync2"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/concurrency"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

const (
	// all the states for the worker
	stateVSDNotSarted = "not started"
	stateVSDDone      = "done"
	stateVSDError     = "error"

	stateVSDInit                   = "initializing"
	stateVSDFindTargets            = "finding target instances"
	stateVSDSynchronizeReplication = "synchronizing replication"
	stateVSDDiff                   = "running the diff"
	stateVSDCleanUp                = "cleaning up"
)

// VerticalSplitDiffWorker executes a diff between a destination shard and its
// source shards in a shard split case.
type VerticalSplitDiffWorker struct {
	wr       *wrangler.Wrangler
	cell     string
	keyspace string
	shard    string
	cleaner  *wrangler.Cleaner

	// all subsequent fields are protected by the mutex
	mu    sync.Mutex
	state string

	// populated if state == stateVSDError
	err error

	// populated during stateVSDInit, read-only after that
	keyspaceInfo *topo.KeyspaceInfo
	shardInfo    *topo.ShardInfo

	// populated during stateVSDFindTargets, read-only after that
	sourceAlias      topo.TabletAlias
	destinationAlias topo.TabletAlias

	// populated during stateVSDDiff
	sourceSchemaDefinition      *myproto.SchemaDefinition
	destinationSchemaDefinition *myproto.SchemaDefinition
}

// NewVerticalSplitDiffWorker returns a new VerticalSplitDiffWorker object.
func NewVerticalSplitDiffWorker(wr *wrangler.Wrangler, cell, keyspace, shard string) Worker {
	return &VerticalSplitDiffWorker{
		wr:       wr,
		cell:     cell,
		keyspace: keyspace,
		shard:    shard,
		cleaner:  &wrangler.Cleaner{},

		state: stateVSDNotSarted,
	}
}

func (vsdw *VerticalSplitDiffWorker) setState(state string) {
	vsdw.mu.Lock()
	vsdw.state = state
	vsdw.mu.Unlock()
}

func (vsdw *VerticalSplitDiffWorker) recordError(err error) {
	vsdw.mu.Lock()
	vsdw.state = stateVSDError
	vsdw.err = err
	vsdw.mu.Unlock()
}

// StatusAsHTML is part of the Worker interface.
func (vsdw *VerticalSplitDiffWorker) StatusAsHTML() template.HTML {
	vsdw.mu.Lock()
	defer vsdw.mu.Unlock()
	result := "<b>Working on:</b> " + vsdw.keyspace + "/" + vsdw.shard + "</br>\n"
	result += "<b>State:</b> " + vsdw.state + "</br>\n"
	switch vsdw.state {
	case stateVSDError:
		result += "<b>Error</b>: " + vsdw.err.Error() + "</br>\n"
	case stateVSDDiff:
		result += "<b>Running</b>:</br>\n"
	case stateVSDDone:
		result += "<b>Success</b>:</br>\n"
	}

	return template.HTML(result)
}

// StatusAsText is part of the Worker interface.
func (vsdw *VerticalSplitDiffWorker) StatusAsText() string {
	vsdw.mu.Lock()
	defer vsdw.mu.Unlock()
	result := "Working on: " + vsdw.keyspace + "/" + vsdw.shard + "\n"
	result += "State: " + vsdw.state + "\n"
	switch vsdw.state {
	case stateVSDError:
		result += "Error: " + vsdw.err.Error() + "\n"
	case stateVSDDiff:
		result += "Running...\n"
	case stateVSDDone:
		result += "Success.\n"
	}
	return result
}

// CheckInterrupted is part of the Worker interface.
func (vsdw *VerticalSplitDiffWorker) CheckInterrupted() bool {
	select {
	case <-interrupted:
		vsdw.recordError(topo.ErrInterrupted)
		return true
	default:
	}
	return false
}

// Run is mostly a wrapper to run the cleanup at the end.
func (vsdw *VerticalSplitDiffWorker) Run() {
	err := vsdw.run()

	vsdw.setState(stateVSDCleanUp)
	cerr := vsdw.cleaner.CleanUp(vsdw.wr)
	if cerr != nil {
		if err != nil {
			vsdw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}
	if err != nil {
		vsdw.recordError(err)
		return
	}
	vsdw.setState(stateVSDDone)
}

func (vsdw *VerticalSplitDiffWorker) Error() error {
	return vsdw.err
}

func (vsdw *VerticalSplitDiffWorker) run() error {
	// first state: read what we need to do
	if err := vsdw.init(); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if vsdw.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// second state: find targets
	if err := vsdw.findTargets(); err != nil {
		return fmt.Errorf("findTargets() failed: %v", err)
	}
	if vsdw.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// third phase: synchronize replication
	if err := vsdw.synchronizeReplication(); err != nil {
		return fmt.Errorf("synchronizeReplication() failed: %v", err)
	}
	if vsdw.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// fourth phase: diff
	if err := vsdw.diff(); err != nil {
		return fmt.Errorf("diff() failed: %v", err)
	}

	return nil
}

// init phase:
// - read the shard info, make sure it has sources
func (vsdw *VerticalSplitDiffWorker) init() error {
	vsdw.setState(stateVSDInit)

	var err error

	// read the keyspace and validate it
	vsdw.keyspaceInfo, err = vsdw.wr.TopoServer().GetKeyspace(vsdw.keyspace)
	if err != nil {
		return fmt.Errorf("cannot read keyspace %v: %v", vsdw.keyspace, err)
	}
	if len(vsdw.keyspaceInfo.ServedFromMap) == 0 {
		return fmt.Errorf("keyspace %v has no KeyspaceServedFrom", vsdw.keyspace)
	}

	// read the shardinfo and validate it
	vsdw.shardInfo, err = vsdw.wr.TopoServer().GetShard(vsdw.keyspace, vsdw.shard)
	if err != nil {
		return fmt.Errorf("cannot read shard %v/%v: %v", vsdw.keyspace, vsdw.shard, err)
	}
	if len(vsdw.shardInfo.SourceShards) != 1 {
		return fmt.Errorf("shard %v/%v has bad number of source shards", vsdw.keyspace, vsdw.shard)
	}
	if len(vsdw.shardInfo.SourceShards[0].Tables) == 0 {
		return fmt.Errorf("shard %v/%v has no tables in source shard[0]", vsdw.keyspace, vsdw.shard)
	}
	if vsdw.shardInfo.MasterAlias.IsZero() {
		return fmt.Errorf("shard %v/%v has no master", vsdw.keyspace, vsdw.shard)
	}

	return nil
}

// findTargets phase:
// - find one rdonly per source shard
// - find one rdonly in destination shard
// - mark them all as 'checker' pointing back to us
func (vsdw *VerticalSplitDiffWorker) findTargets() error {
	vsdw.setState(stateVSDFindTargets)

	// find an appropriate endpoint in destination shard
	var err error
	vsdw.destinationAlias, err = findChecker(vsdw.wr, vsdw.cleaner, vsdw.cell, vsdw.keyspace, vsdw.shard)
	if err != nil {
		return fmt.Errorf("cannot find checker for %v/%v/%v: %v", vsdw.cell, vsdw.keyspace, vsdw.shard, err)
	}

	// find an appropriate endpoint in the source shard
	vsdw.sourceAlias, err = findChecker(vsdw.wr, vsdw.cleaner, vsdw.cell, vsdw.shardInfo.SourceShards[0].Keyspace, vsdw.shardInfo.SourceShards[0].Shard)
	if err != nil {
		return fmt.Errorf("cannot find checker for %v/%v/%v: %v", vsdw.cell, vsdw.shardInfo.SourceShards[0].Keyspace, vsdw.shardInfo.SourceShards[0].Shard, err)
	}

	return nil
}

// synchronizeReplication phase:
// 1 - ask the master of the destination shard to pause filtered replication,
//   and return the source binlog positions
//   (add a cleanup task to restart filtered replication on master)
// 2 - stop the source 'checker' at a binlog position higher than the
//   destination master. Get that new position.
//   (add a cleanup task to restart binlog replication on it, and change
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

func (vsdw *VerticalSplitDiffWorker) synchronizeReplication() error {
	vsdw.setState(stateVSDSynchronizeReplication)

	masterInfo, err := vsdw.wr.TopoServer().GetTablet(vsdw.shardInfo.MasterAlias)
	if err != nil {
		return fmt.Errorf("synchronizeReplication: cannot get Tablet record for master %v: %v", vsdw.shardInfo.MasterAlias, err)
	}

	// 1 - stop the master binlog replication, get its current position
	vsdw.wr.Logger().Infof("Stopping master binlog replication on %v", vsdw.shardInfo.MasterAlias)
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	blpPositionList, err := vsdw.wr.TabletManagerClient().StopBlp(ctx, masterInfo)
	cancel()
	if err != nil {
		return fmt.Errorf("StopBlp on master %v failed: %v", vsdw.shardInfo.MasterAlias, err)
	}
	wrangler.RecordStartBlpAction(vsdw.cleaner, masterInfo, 30*time.Second)

	// 2 - stop the source 'checker' at a binlog position
	//     higher than the destination master
	stopPositionList := blproto.BlpPositionList{
		Entries: make([]blproto.BlpPosition, 1),
	}
	ss := vsdw.shardInfo.SourceShards[0]
	// find where we should be stopping
	pos, err := blpPositionList.FindBlpPositionById(ss.Uid)
	if err != nil {
		return fmt.Errorf("no binlog position on the master for Uid %v", ss.Uid)
	}

	// stop replication
	vsdw.wr.Logger().Infof("Stopping slave %v at a minimum of %v", vsdw.sourceAlias, pos.Position)
	sourceTablet, err := vsdw.wr.TopoServer().GetTablet(vsdw.sourceAlias)
	if err != nil {
		return err
	}
	stoppedAt, err := vsdw.wr.TabletManagerClient().StopSlaveMinimum(context.TODO(), sourceTablet, pos.Position, 30*time.Second)
	if err != nil {
		return fmt.Errorf("cannot stop slave %v at right binlog position %v: %v", vsdw.sourceAlias, pos.Position, err)
	}
	stopPositionList.Entries[0].Uid = ss.Uid
	stopPositionList.Entries[0].Position = stoppedAt.Position

	// change the cleaner actions from ChangeSlaveType(rdonly)
	// to StartSlave() + ChangeSlaveType(spare)
	wrangler.RecordStartSlaveAction(vsdw.cleaner, sourceTablet, 30*time.Second)
	action, err := wrangler.FindChangeSlaveTypeActionByTarget(vsdw.cleaner, vsdw.sourceAlias)
	if err != nil {
		return fmt.Errorf("cannot find ChangeSlaveType action for %v: %v", vsdw.sourceAlias, err)
	}
	action.TabletType = topo.TYPE_SPARE

	// 3 - ask the master of the destination shard to resume filtered
	//     replication up to the new list of positions
	vsdw.wr.Logger().Infof("Restarting master %v until it catches up to %v", vsdw.shardInfo.MasterAlias, stopPositionList)
	masterPos, err := vsdw.wr.TabletManagerClient().RunBlpUntil(context.TODO(), masterInfo, &stopPositionList, 30*time.Second)
	if err != nil {
		return fmt.Errorf("RunBlpUntil on %v until %v failed: %v", vsdw.shardInfo.MasterAlias, stopPositionList, err)
	}

	// 4 - wait until the destination checker is equal or passed
	//     that master binlog position, and stop its replication.
	vsdw.wr.Logger().Infof("Waiting for destination checker %v to catch up to %v", vsdw.destinationAlias, masterPos)
	destinationTablet, err := vsdw.wr.TopoServer().GetTablet(vsdw.destinationAlias)
	if err != nil {
		return err
	}
	_, err = vsdw.wr.TabletManagerClient().StopSlaveMinimum(context.TODO(), destinationTablet, masterPos, 30*time.Second)
	if err != nil {
		return fmt.Errorf("StopSlaveMinimum on %v at %v failed: %v", vsdw.destinationAlias, masterPos, err)
	}
	wrangler.RecordStartSlaveAction(vsdw.cleaner, destinationTablet, 30*time.Second)
	action, err = wrangler.FindChangeSlaveTypeActionByTarget(vsdw.cleaner, vsdw.destinationAlias)
	if err != nil {
		return fmt.Errorf("cannot find ChangeSlaveType action for %v: %v", vsdw.destinationAlias, err)
	}
	action.TabletType = topo.TYPE_SPARE

	// 5 - restart filtered replication on destination master
	vsdw.wr.Logger().Infof("Restarting filtered replication on master %v", vsdw.shardInfo.MasterAlias)
	ctx, cancel = context.WithTimeout(context.TODO(), 30*time.Second)
	err = vsdw.wr.TabletManagerClient().StartBlp(ctx, masterInfo)
	if err := vsdw.cleaner.RemoveActionByName(wrangler.StartBlpActionName, vsdw.shardInfo.MasterAlias.String()); err != nil {
		vsdw.wr.Logger().Warningf("Cannot find cleaning action %v/%v: %v", wrangler.StartBlpActionName, vsdw.shardInfo.MasterAlias.String(), err)
	}
	cancel()
	if err != nil {
		return fmt.Errorf("StartBlp on %v failed: %v", vsdw.shardInfo.MasterAlias, err)
	}

	return nil
}

// diff phase: will create a list of messages regarding the diff.
// - get the schema on all checkers
// - if some table schema mismatches, record them (use existing schema diff tools).
// - for each table in destination, run a diff pipeline.

func (vsdw *VerticalSplitDiffWorker) diff() error {
	vsdw.setState(stateVSDDiff)

	vsdw.wr.Logger().Infof("Gathering schema information...")
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	wg.Add(1)
	go func() {
		var err error
		vsdw.destinationSchemaDefinition, err = vsdw.wr.GetSchema(vsdw.wr.Context(), vsdw.destinationAlias, nil, nil, false)
		rec.RecordError(err)
		vsdw.wr.Logger().Infof("Got schema from destination %v", vsdw.destinationAlias)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		var err error
		vsdw.sourceSchemaDefinition, err = vsdw.wr.GetSchema(vsdw.wr.Context(), vsdw.sourceAlias, nil, nil, false)
		rec.RecordError(err)
		vsdw.wr.Logger().Infof("Got schema from source %v", vsdw.sourceAlias)
		wg.Done()
	}()
	wg.Wait()
	if rec.HasErrors() {
		return rec.Error()
	}

	// Build a list of regexp to exclude tables from source schema
	tableRegexps := make([]*regexp.Regexp, len(vsdw.shardInfo.SourceShards[0].Tables))
	for i, table := range vsdw.shardInfo.SourceShards[0].Tables {
		var err error
		tableRegexps[i], err = regexp.Compile(table)
		if err != nil {
			return fmt.Errorf("cannot compile regexp %v for table: %v", table, err)
		}
	}

	// Remove the tables we don't need from the source schema
	newSourceTableDefinitions := make([]*myproto.TableDefinition, 0, len(vsdw.destinationSchemaDefinition.TableDefinitions))
	for _, tableDefinition := range vsdw.sourceSchemaDefinition.TableDefinitions {
		found := false
		for _, tableRegexp := range tableRegexps {
			if tableRegexp.MatchString(tableDefinition.Name) {
				found = true
				break
			}
		}
		if !found {
			vsdw.wr.Logger().Infof("Removing table %v from source schema", tableDefinition.Name)
			continue
		}
		newSourceTableDefinitions = append(newSourceTableDefinitions, tableDefinition)
	}
	vsdw.sourceSchemaDefinition.TableDefinitions = newSourceTableDefinitions

	// Check the schema
	vsdw.wr.Logger().Infof("Diffing the schema...")
	rec = concurrency.AllErrorRecorder{}
	myproto.DiffSchema("destination", vsdw.destinationSchemaDefinition, "source", vsdw.sourceSchemaDefinition, &rec)
	if rec.HasErrors() {
		vsdw.wr.Logger().Warningf("Different schemas: %v", rec.Error())
	} else {
		vsdw.wr.Logger().Infof("Schema match, good.")
	}

	// run the diffs, 8 at a time
	vsdw.wr.Logger().Infof("Running the diffs...")
	sem := sync2.NewSemaphore(8, 0)
	for _, tableDefinition := range vsdw.destinationSchemaDefinition.TableDefinitions {
		wg.Add(1)
		go func(tableDefinition *myproto.TableDefinition) {
			defer wg.Done()
			sem.Acquire()
			defer sem.Release()

			vsdw.wr.Logger().Infof("Starting the diff on table %v", tableDefinition.Name)
			sourceQueryResultReader, err := TableScan(vsdw.wr.Logger(), vsdw.wr.TopoServer(), vsdw.sourceAlias, tableDefinition)
			if err != nil {
				vsdw.wr.Logger().Errorf("TableScan(source) failed: %v", err)
				return
			}
			defer sourceQueryResultReader.Close()

			destinationQueryResultReader, err := TableScan(vsdw.wr.Logger(), vsdw.wr.TopoServer(), vsdw.destinationAlias, tableDefinition)
			if err != nil {
				vsdw.wr.Logger().Errorf("TableScan(destination) failed: %v", err)
				return
			}
			defer destinationQueryResultReader.Close()

			differ, err := NewRowDiffer(sourceQueryResultReader, destinationQueryResultReader, tableDefinition)
			if err != nil {
				vsdw.wr.Logger().Errorf("NewRowDiffer() failed: %v", err)
				return
			}

			report, err := differ.Go(vsdw.wr.Logger())
			if err != nil {
				vsdw.wr.Logger().Errorf("Differ.Go failed: %v", err)
			} else {
				if report.HasDifferences() {
					vsdw.wr.Logger().Errorf("Table %v has differences: %v", tableDefinition.Name, report.String())
				} else {
					vsdw.wr.Logger().Infof("Table %v checks out (%v rows processed, %v qps)", tableDefinition.Name, report.processedRows, report.processingQPS)
				}
			}
		}(tableDefinition)
	}
	wg.Wait()

	return nil
}
