// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"html/template"
	"sync"
)

// StatusWorkerState is the type for a StatusWorker's status
type StatusWorkerState string

// All possible status strings (if your implementation needs more,
// just add them)

const (
	WorkerStateNotSarted       StatusWorkerState = "not started"
	WorkerStateDone            StatusWorkerState = "done"
	WorkerStateError           StatusWorkerState = "error"
	WorkerStateInit            StatusWorkerState = "initializing"
	WorkerStateFindTargets     StatusWorkerState = "finding target instances"
	WorkerStateSyncReplication StatusWorkerState = "synchronizing replication"
	WorkerStateCopy            StatusWorkerState = "copying the data"
	WorkerStateDiff            StatusWorkerState = "running the diff"
	WorkerStateCleanUp         StatusWorkerState = "cleaning up"
)

func (state StatusWorkerState) String() string {
	return string(state)
}

// StatusWorker is the base type for a worker which keeps a status.
// The status is protected by a mutex. Any other internal variable
// can also be protected by that mutex.
// StatusWorker also provides default implementations for StatusAsHTML
// and StatusAsText to make it easier on workers if they don't need to
// export more.
type StatusWorker struct {
	// Mu is protecting the state variable, and can also be used
	// by implementations to protect their own variables.
	Mu *sync.Mutex

	// State contains the worker's current state, and should only
	// be accessed under Mu.
	State StatusWorkerState
}

// NewStatusWorker returns a StatusWorker in state WorkerStateNotSarted
func NewStatusWorker() StatusWorker {
	return StatusWorker{
		Mu:    &sync.Mutex{},
		State: WorkerStateNotSarted,
	}
}

// SetState is a convenience function for workers
func (worker *StatusWorker) SetState(state StatusWorkerState) {
	worker.Mu.Lock()
	worker.State = state
	statsState.Set(string(state))
	worker.Mu.Unlock()
}

// StatusAsHTML is part of the Worker interface
func (worker *StatusWorker) StatusAsHTML() template.HTML {
	worker.Mu.Lock()
	defer worker.Mu.Unlock()
	return template.HTML("<b>State:</b> " + worker.State.String() + "</br>\n")
}

// StatusAsText is part of the Worker interface
func (worker *StatusWorker) StatusAsText() string {
	worker.Mu.Lock()
	defer worker.Mu.Unlock()
	return "State: " + worker.State.String() + "\n"
}
