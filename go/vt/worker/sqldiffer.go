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

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

// This file contains the code to run a sanity check in a system with
// a lookup database: any row in the actual database needs to have a
// conuterpart in the lookup database.

type sqlDiffWorkerState string

const (
	// all the states for the worker
	sqlDiffNotSarted              sqlDiffWorkerState = "not started"
	sqlDiffDone                   sqlDiffWorkerState = "done"
	sqlDiffError                  sqlDiffWorkerState = "error"
	sqlDiffFindTargets            sqlDiffWorkerState = "finding target instances"
	sqlDiffSynchronizeReplication sqlDiffWorkerState = "synchronizing replication"
	sqlDiffRunning                sqlDiffWorkerState = "running the diff"
	sqlDiffCleanUp                sqlDiffWorkerState = "cleaning up"
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

		state: sqlDiffNotSarted,
	}
}

func (worker *SQLDiffWorker) setState(state sqlDiffWorkerState) {
	worker.mu.Lock()
	worker.state = state
	statsState.Set(string(state))
	worker.mu.Unlock()
}

func (worker *SQLDiffWorker) recordError(err error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	worker.state = sqlDiffError
	statsState.Set(string(sqlDiffError))
	worker.err = err
}

// StatusAsHTML is part of the Worker interface
func (worker *SQLDiffWorker) StatusAsHTML() template.HTML {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	result := "<b>Working on:</b> " + worker.subset.Keyspace + "/" + worker.subset.Shard + "</br>\n"
	result += "<b>State:</b> " + worker.state.String() + "</br>\n"
	switch worker.state {
	case sqlDiffError:
		result += "<b>Error</b>: " + worker.err.Error() + "</br>\n"
	case sqlDiffRunning:
		result += "<b>Running...</b></br>\n"
	case sqlDiffDone:
		result += "<b>Success.</b></br>\n"
	}

	return template.HTML(result)
}

// StatusAsText is part of the Worker interface
func (worker *SQLDiffWorker) StatusAsText() string {
	worker.mu.Lock()
	defer worker.mu.Unlock()

	result := "Working on: " + worker.subset.Keyspace + "/" + worker.subset.Shard + "\n"
	result += "State: " + worker.state.String() + "\n"
	switch worker.state {
	case sqlDiffError:
		result += "Error: " + worker.err.Error() + "\n"
	case sqlDiffRunning:
		result += "Running...\n"
	case sqlDiffDone:
		result += "Success.\n"
	}
	return result
}

func (worker *SQLDiffWorker) checkInterrupted(ctx context.Context) error {
	select {
	case <-ctx.Done():
		err := ctx.Err()
		worker.recordError(err)
		return err
	default:
	}
	return nil
}

// Run is mostly a wrapper to run the cleanup at the end.
func (worker *SQLDiffWorker) Run(ctx context.Context) error {
	resetVars()
	err := worker.run(ctx)

	worker.setState(sqlDiffCleanUp)
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
		return err
	}
	worker.setState(sqlDiffDone)
	return nil
}

func (worker *SQLDiffWorker) run(ctx context.Context) error {
	// first state: find targets
	if err := worker.findTargets(ctx); err != nil {
		return err
	}
	if err := worker.checkInterrupted(ctx); err != nil {
		return err
	}

	// second phase: synchronize replication
	if err := worker.synchronizeReplication(ctx); err != nil {
		return err
	}
	if err := worker.checkInterrupted(ctx); err != nil {
		return err
	}

	// third phase: diff
	if err := worker.diff(ctx); err != nil {
		return err
	}

	return nil
}

// findTargets phase:
// - find one rdonly in superset
// - find one rdonly in subset
// - mark them all as 'checker' pointing back to us
func (worker *SQLDiffWorker) findTargets(ctx context.Context) error {
	worker.setState(sqlDiffFindTargets)

	// find an appropriate endpoint in superset
	var err error
	worker.superset.alias, err = findChecker(ctx, worker.wr, worker.cleaner, worker.cell, worker.superset.Keyspace, worker.superset.Shard)
	if err != nil {
		return err
	}

	// find an appropriate endpoint in subset
	worker.subset.alias, err = findChecker(ctx, worker.wr, worker.cleaner, worker.cell, worker.subset.Keyspace, worker.subset.Shard)
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
func (worker *SQLDiffWorker) synchronizeReplication(ctx context.Context) error {
	worker.setState(sqlDiffSynchronizeReplication)

	// stop replication on subset slave
	worker.wr.Logger().Infof("Stopping replication on subset slave %v", worker.subset.alias)
	subsetTablet, err := worker.wr.TopoServer().GetTablet(worker.subset.alias)
	if err != nil {
		return err
	}
	shortCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	err = worker.wr.TabletManagerClient().StopSlave(shortCtx, subsetTablet)
	cancel()
	if err != nil {
		return fmt.Errorf("Cannot stop slave %v: %v", worker.subset.alias, err)
	}
	if err := worker.checkInterrupted(ctx); err != nil {
		return err
	}

	// change the cleaner actions from ChangeSlaveType(rdonly)
	// to StartSlave() + ChangeSlaveType(spare)
	wrangler.RecordStartSlaveAction(worker.cleaner, subsetTablet)
	action, err := wrangler.FindChangeSlaveTypeActionByTarget(worker.cleaner, worker.subset.alias)
	if err != nil {
		return fmt.Errorf("cannot find ChangeSlaveType action for %v: %v", worker.subset.alias, err)
	}
	action.TabletType = topo.TYPE_SPARE

	// sleep for a few seconds
	time.Sleep(5 * time.Second)
	if err := worker.checkInterrupted(ctx); err != nil {
		return err
	}

	// stop replication on superset slave
	worker.wr.Logger().Infof("Stopping replication on superset slave %v", worker.superset.alias)
	supersetTablet, err := worker.wr.TopoServer().GetTablet(worker.superset.alias)
	if err != nil {
		return err
	}
	shortCtx, cancel = context.WithTimeout(ctx, 60*time.Second)
	err = worker.wr.TabletManagerClient().StopSlave(shortCtx, supersetTablet)
	cancel()
	if err != nil {
		return fmt.Errorf("Cannot stop slave %v: %v", worker.superset.alias, err)
	}

	// change the cleaner actions from ChangeSlaveType(rdonly)
	// to StartSlave() + ChangeSlaveType(spare)
	wrangler.RecordStartSlaveAction(worker.cleaner, supersetTablet)
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

func (worker *SQLDiffWorker) diff(ctx context.Context) error {
	worker.setState(sqlDiffRunning)

	// run the diff
	worker.wr.Logger().Infof("Running the diffs...")

	supersetQueryResultReader, err := NewQueryResultReaderForTablet(ctx, worker.wr.TopoServer(), worker.superset.alias, worker.superset.SQL)
	if err != nil {
		worker.wr.Logger().Errorf("NewQueryResultReaderForTablet(superset) failed: %v", err)
		return err
	}
	defer supersetQueryResultReader.Close()

	subsetQueryResultReader, err := NewQueryResultReaderForTablet(ctx, worker.wr.TopoServer(), worker.subset.alias, worker.subset.SQL)
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
