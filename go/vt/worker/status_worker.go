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
	"html/template"
	"sync"
)

// StatusWorkerState is the type for a StatusWorker's status
type StatusWorkerState string

// All possible status strings (if your implementation needs more,
// just add them).

const (
	// WorkerStateNotStarted is the initial state.
	WorkerStateNotStarted StatusWorkerState = "not started"
	// WorkerStateDone is set when the worker successfully finished.
	WorkerStateDone StatusWorkerState = "done"
	// WorkerStateError is set when the worker failed.
	WorkerStateError StatusWorkerState = "error"
	// WorkerStateInit is set when the worker does initialize its state.
	WorkerStateInit StatusWorkerState = "initializing"
	// WorkerStateFindTargets is set when the worker searches healthy RDONLY tablets.
	WorkerStateFindTargets StatusWorkerState = "finding target instances"
	// WorkerStateSyncReplication is set when the worker ensures that source and
	// destination tablets are at the same GTID during the diff.
	WorkerStateSyncReplication StatusWorkerState = "synchronizing replication"

	// WorkerStateBackfilling is set when the worker copies the data.
	WorkerStateBackfilling StatusWorkerState = "backfilling the data"

	// WorkerStateCloneOnline is set when the worker copies the data in the online phase.
	WorkerStateCloneOnline StatusWorkerState = "cloning the data (online)"
	// WorkerStateCloneOffline is set when the worker copies the data in the offline phase.
	WorkerStateCloneOffline StatusWorkerState = "cloning the data (offline)"

	// WorkerStateDiff is set when the worker compares the data.
	WorkerStateDiff StatusWorkerState = "running the diff"

	// WorkerStateDiffWillFail is set when the worker is still comparing the data, but we have already found discrepancies.
	WorkerStateDiffWillFail StatusWorkerState = "running the diff, already found differences"

	// WorkerStateDebugRunning is set when an internal command (e.g. Block or Ping) is currently running.
	WorkerStateDebugRunning StatusWorkerState = "running an internal debug command"

	// WorkerStateCleanUp is set when the worker reverses the initialization e.g.
	// the type of a taken out RDONLY tablet is changed back from "worker" to "spare".
	WorkerStateCleanUp StatusWorkerState = "cleaning up"
)

func (state StatusWorkerState) String() string {
	return string(state)
}

// StatusWorker is the base type for a worker which keeps a status.
// The status is protected by a mutex.
// StatusWorker also provides default implementations for StatusAsHTML
// and StatusAsText to make it easier on workers if they don't need to
// export more.
type StatusWorker struct {
	mu *sync.Mutex
	// state contains the worker's current state. Guarded by mu.
	state StatusWorkerState
}

// NewStatusWorker returns a StatusWorker in state WorkerStateNotStarted.
func NewStatusWorker() StatusWorker {
	return StatusWorker{
		mu:    &sync.Mutex{},
		state: WorkerStateNotStarted,
	}
}

// SetState is a convenience function for workers.
func (w *StatusWorker) SetState(state StatusWorkerState) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.state = state
	statsState.Set(string(state))
}

// State is part of the Worker interface.
func (w *StatusWorker) State() StatusWorkerState {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.state
}

// StatusAsHTML is part of the Worker interface.
func (w *StatusWorker) StatusAsHTML() template.HTML {
	w.mu.Lock()
	defer w.mu.Unlock()

	return template.HTML("<b>State:</b> " + w.state.String() + "</br>\n")
}

// StatusAsText is part of the Worker interface.
func (w *StatusWorker) StatusAsText() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return "State: " + w.state.String() + "\n"
}
