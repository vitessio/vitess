// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
'worker' package contains the framework, utility methods and core
functions for long running actions. 'vtworker' binary will use these.
*/
package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

const (
	// all the states for the worker
	stateNotSarted = "not started"
	stateInit      = "initializing"
	stateRunning   = "running"
	stateDone      = "done"
	stateError     = "error"
)

// SplitDiffWorker executes a diff between a destination shard and its
// source shards in a shard split case.
type SplitDiffWorker struct {
	wr       *wrangler.Wrangler
	keyspace string
	shard    string

	// protected by the mutex
	mu    sync.Mutex
	state string

	// populated if state == stateError
	err error

	// populated during stateInit
	shardInfo *topo.ShardInfo
}

// NewSplitDiff returns a new SplitDiffWorker object.
func NewSplitDiffWorker(wr *wrangler.Wrangler, keyspace, shard string) Worker {
	return &SplitDiffWorker{
		wr:       wr,
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
	result := "<b>Working on:</b> " + sdw.keyspace + "/" + sdw.shard + "<br></br>\n"
	result += "<b>State:</b> " + sdw.state + "<br></br>\n"
	switch sdw.state {
	case stateError:
		result += "<b>Error</b>: " + sdw.err.Error() + "<br></br>\n"
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
	if err := sdw.Init(); err != nil {
		sdw.recordError(err)
		return
	}

	if sdw.CheckInterrupted() {
		return
	}

	// second state: dummy sleep
	sdw.setState(stateRunning)
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		if sdw.CheckInterrupted() {
			return
		}
	}

	sdw.setState(stateDone)
}

func (sdw *SplitDiffWorker) Init() error {
	sdw.setState(stateInit)

	shardInfo, err := sdw.wr.TopoServer().GetShard(sdw.keyspace, sdw.shard)
	if err != nil {
		return fmt.Errorf("Cannot read shard %v/%v: %v", sdw.keyspace, sdw.shard, err)
	}

	if len(shardInfo.SourceShards) == 0 {
		return fmt.Errorf("Shard %v/%v has no source shard", sdw.keyspace, sdw.shard)
	}

	sdw.mu.Lock()
	sdw.shardInfo = shardInfo
	sdw.mu.Unlock()

	return nil
}
