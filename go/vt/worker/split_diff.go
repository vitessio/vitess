// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"sync"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

const (
	// all the states for the worker
	stateNotSarted = "not started"
	stateDone      = "done"
	stateError     = "error"

	stateInit        = "initializing"
	stateFindTargets = "finding target instances"
)

// SplitDiffWorker executes a diff between a destination shard and its
// source shards in a shard split case.
type SplitDiffWorker struct {
	wr       *wrangler.Wrangler
	cell     string
	keyspace string
	shard    string

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
}

// NewSplitDiff returns a new SplitDiffWorker object.
func NewSplitDiffWorker(wr *wrangler.Wrangler, cell, keyspace, shard string) Worker {
	return &SplitDiffWorker{
		wr:       wr,
		cell:     cell,
		keyspace: keyspace,
		shard:    shard,

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

func (sdw *SplitDiffWorker) Run() {
	// first state: read what we need to do
	if err := sdw.init(); err != nil {
		sdw.recordError(err)
		return
	}
	if sdw.CheckInterrupted() {
		return
	}

	// second state: find targets
	if err := sdw.findTargets(); err != nil {
		sdw.recordError(err)
		return
	}
	if sdw.CheckInterrupted() {
		return
	}

	sdw.setState(stateDone)
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

	return nil
}

// findTargets phase:
// - find one rdonly per source shard
// - find one rdonly in destination shard
// - mark them all as 'checker' pointing back to us

func (sdw *SplitDiffWorker) findTarget(shard string) (topo.TabletAlias, error) {
	endPoints, err := sdw.wr.TopoServer().GetEndPoints(sdw.cell, sdw.keyspace, sdw.shard, topo.TYPE_RDONLY)
	if err != nil {
		return topo.TabletAlias{}, fmt.Errorf("GetEndPoints(%v,%v,%v,rdonly) failed: %v", sdw.cell, sdw.keyspace, sdw.shard, err)
	}
	if len(endPoints.Entries) == 0 {
		return topo.TabletAlias{}, fmt.Errorf("No endpoint to chose from in (%v,%v/%v)", sdw.cell, sdw.keyspace, sdw.shard)
	}

	tabletAlias := topo.TabletAlias{
		Cell: sdw.cell,
		Uid:  endPoints.Entries[0].Uid,
	}
	if err := sdw.wr.ChangeType(tabletAlias, topo.TYPE_CHECKER, false /*force*/); err != nil {
		return topo.TabletAlias{}, err
	}
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
