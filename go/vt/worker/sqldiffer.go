// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"html/template"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

// This file contains the code to run a sanity check in a system with
// a lookup database: any row in the actual database needs to have a
// conuterpart in the lookup database.

type sqlDiffWorkerState string

const (
	// all the states for the worker
	SQLDiffNotSarted              sqlDiffWorkerState = "not started"
	SQLDiffDone                   sqlDiffWorkerState = "done"
	SQLDiffError                  sqlDiffWorkerState = "error"
	SQLDiffFindTargets            sqlDiffWorkerState = "finding target instances"
	SQLDiffSynchronizeReplication sqlDiffWorkerState = "synchronizing replication"
	SQLDiffRunning                sqlDiffWorkerState = "running the diff"
	SQLDiffCleanUp                sqlDiffWorkerState = "cleaning up"
)

func (state sqlDiffWorkerState) String() string {
	return string(state)
}

// SourceSpec specifies a SQL query in some keyspace and shard.
type SourceSpec struct {
	Keyspace string
	Shard    string
	SQL      string

	alias topo.TabletAlias
}

// SQLDiffWorker runs a sanity check in in a system with a lookup
// database: any row in the subset spec needs to have a conuterpart in
// the superset spec.
type SQLDiffWorker struct {
	wr      *wrangler.Wrangler
	cell    string
	shard   string
	cleaner *wrangler.Cleaner

	// alias in the following 2 fields is during
	// SQLDifferFindTargets, read-only after that.
	superset SourceSpec
	subset   SourceSpec

	// all subsequent fields are protected by the mutex
	mu    sync.Mutex
	state sqlDiffWorkerState

	// populated if state == SQLDiffError
	err error
}

// NewSQLDiffWorker returns a new SQLDiffWorker object.
func NewSQLDiffWorker(wr *wrangler.Wrangler, cell string, superset, subset SourceSpec) Worker {
	return &SQLDiffWorker{
		wr:       wr,
		cell:     cell,
		superset: superset,
		subset:   subset,
		cleaner:  new(wrangler.Cleaner),
		state:    SQLDiffNotSarted,
	}
}

func (worker *SQLDiffWorker) setState(state sqlDiffWorkerState) {
	worker.mu.Lock()
	worker.state = state
	worker.mu.Unlock()
}

func (worker *SQLDiffWorker) recordError(err error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	worker.state = SQLDiffError
	worker.err = err
}

func (worker *SQLDiffWorker) StatusAsHTML() template.HTML {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	result := "<b>Working on:</b> " + worker.subset.Keyspace + "/" + worker.subset.Shard + "</br>\n"
	result += "<b>State:</b> " + worker.state.String() + "</br>\n"
	switch worker.state {
	case SQLDiffError:
		result += "<b>Error</b>: " + worker.err.Error() + "</br>\n"
	case SQLDiffRunning:
		result += "<b>Running...</b></br>\n"
	case SQLDiffDone:
		result += "<b>Success.</b></br>\n"
	}

	return template.HTML(result)
}

func (worker *SQLDiffWorker) StatusAsText() string {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	result := "Working on: " + worker.subset.Keyspace + "/" + worker.subset.Shard + "\n"
	result += "State: " + worker.state.String() + "\n"
	switch worker.state {
	case SQLDiffError:
		result += "Error: " + worker.err.Error() + "\n"
	case SQLDiffRunning:
		result += "Running...\n"
	case SQLDiffDone:
		result += "Success.\n"
	}
	return result
}

func (worker *SQLDiffWorker) CheckInterrupted() bool {
	select {
	case <-interrupted:
		worker.recordError(topo.ErrInterrupted)
		return true
	default:
	}
	return false
}

// Run is mostly a wrapper to run the cleanup at the end.
func (worker *SQLDiffWorker) Run() {
	err := worker.run()

	worker.setState(SQLDiffCleanUp)
	cerr := worker.cleaner.CleanUp(worker.wr)
	if cerr != nil {
		if err != nil {
			worker.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}
	if err != nil {
		worker.recordError(err)
		return
	}
	worker.setState(SQLDiffDone)
}

func (worker *SQLDiffWorker) Error() error {
	return worker.err
}

func (worker *SQLDiffWorker) run() error {
	// first state: find targets
	if err := worker.findTargets(); err != nil {
		return err
	}
	if worker.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// second phase: synchronize replication
	if err := worker.synchronizeReplication(); err != nil {
		return err
	}
	if worker.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// third phase: diff
	if err := worker.diff(); err != nil {
		return err
	}

	return nil
}

// findTargets phase:
// - find one rdonly in superset
// - find one rdonly in subset
// - mark them all as 'checker' pointing back to us
func (worker *SQLDiffWorker) findTargets() error {
	worker.setState(SQLDiffFindTargets)

	// find an appropriate endpoint in superset
	var err error
	worker.superset.alias, err = findChecker(worker.wr, worker.cleaner, worker.cell, worker.superset.Keyspace, worker.superset.Shard)
	if err != nil {
		return err
	}

	// find an appropriate endpoint in subset
	worker.subset.alias, err = findChecker(worker.wr, worker.cleaner, worker.cell, worker.subset.Keyspace, worker.subset.Shard)
	if err != nil {
		return err
	}

	return nil
}

// synchronizeReplication phase:
// 1 - ask the subset slave to stop replication
// 2 - sleep for 5 seconds
// 3 - ask the superset slave to stop replication
// Note this is not 100% correct, but good enough for now
func (worker *SQLDiffWorker) synchronizeReplication() error {
	worker.setState(SQLDiffSynchronizeReplication)

	// stop replication on subset slave
	worker.wr.Logger().Infof("Stopping replication on subset slave %v", worker.subset.alias)
	subsetTablet, err := worker.wr.TopoServer().GetTablet(worker.subset.alias)
	if err != nil {
		return err
	}
	if err := worker.wr.ActionInitiator().StopSlave(subsetTablet, 30*time.Second); err != nil {
		return fmt.Errorf("Cannot stop slave %v: %v", worker.subset.alias, err)
	}
	if worker.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// change the cleaner actions from ChangeSlaveType(rdonly)
	// to StartSlave() + ChangeSlaveType(spare)
	wrangler.RecordStartSlaveAction(worker.cleaner, worker.subset.alias, 30*time.Second)
	action, err := wrangler.FindChangeSlaveTypeActionByTarget(worker.cleaner, worker.subset.alias)
	if err != nil {
		return fmt.Errorf("cannot find ChangeSlaveType action for %v: %v", worker.subset.alias, err)
	}
	action.TabletType = topo.TYPE_SPARE

	// sleep for a few seconds
	time.Sleep(5 * time.Second)
	if worker.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// stop replication on superset slave
	worker.wr.Logger().Infof("Stopping replication on superset slave %v", worker.superset.alias)
	supersetTablet, err := worker.wr.TopoServer().GetTablet(worker.superset.alias)
	if err != nil {
		return err
	}
	if err := worker.wr.ActionInitiator().StopSlave(supersetTablet, 30*time.Second); err != nil {
		return fmt.Errorf("Cannot stop slave %v: %v", worker.superset.alias, err)
	}

	// change the cleaner actions from ChangeSlaveType(rdonly)
	// to StartSlave() + ChangeSlaveType(spare)
	wrangler.RecordStartSlaveAction(worker.cleaner, worker.superset.alias, 30*time.Second)
	action, err = wrangler.FindChangeSlaveTypeActionByTarget(worker.cleaner, worker.superset.alias)
	if err != nil {
		return fmt.Errorf("cannot find ChangeSlaveType action for %v: %v", worker.superset.alias, err)
	}
	action.TabletType = topo.TYPE_SPARE

	return nil
}

// diff phase: will create a list of messages regarding the diff.
// - get the schema on all checkers
// - if some table schema mismatches, record them (use existing schema diff tools).
// - for each table in destination, run a diff pipeline.

func (worker *SQLDiffWorker) diff() error {
	worker.setState(SQLDiffRunning)

	// run the diff
	worker.wr.Logger().Infof("Running the diffs...")

	supersetQueryResultReader, err := NewQueryResultReaderForTablet(worker.wr.TopoServer(), worker.superset.alias, worker.superset.SQL)
	if err != nil {
		worker.wr.Logger().Errorf("NewQueryResultReaderForTablet(superset) failed: %v", err)
		return err
	}
	defer supersetQueryResultReader.Close()

	subsetQueryResultReader, err := NewQueryResultReaderForTablet(worker.wr.TopoServer(), worker.subset.alias, worker.subset.SQL)
	if err != nil {
		worker.wr.Logger().Errorf("NewQueryResultReaderForTablet(subset) failed: %v", err)
		return err
	}
	defer subsetQueryResultReader.Close()

	differ, err := NewRowSubsetDiffer(supersetQueryResultReader, subsetQueryResultReader, 1)
	if err != nil {
		worker.wr.Logger().Errorf("NewRowSubsetDiffer() failed: %v", err)
		return err
	}

	report, err := differ.Go(worker.wr.Logger())
	switch {
	case err != nil:
		worker.wr.Logger().Errorf("Differ.Go failed: %v", err)
	case report.HasDifferences():
		worker.wr.Logger().Infof("Found differences: %v", report.String())
	default:
		worker.wr.Logger().Infof("No difference found (%v rows processed, %v qps)", report.processedRows, report.processingQPS)
	}

	return nil
}
