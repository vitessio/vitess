/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workflow

import (
	"encoding/json"
	"flag"
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

const (
	sleepFactoryName = "sleep"
	pauseAction      = "Pause"
	resumeAction     = "Resume"
)

func init() {
	Register(sleepFactoryName, &SleepWorkflowFactory{})
}

// SleepWorkflowData is the data structure serialized as JSON in Workflow.Data.
type SleepWorkflowData struct {
	// Duration is how long we need to sleep total.
	Duration int

	// Slept is how long we've already slept.
	Slept int

	// Paused is true if we should not be making any progress.
	Paused bool
}

// SleepWorkflow implements the Workflow interface.  It is a test
// Workflow that only sleeps for a provided interval.  It is meant to
// test all the plumbing and corner cases of the workflow library.
type SleepWorkflow struct {
	// mu protects the data access.
	// We need it as both Run and Action can be called at the same time.
	mu sync.Mutex

	// data is the current state.
	data *SleepWorkflowData

	// manager is the current Manager.
	manager *Manager

	// wi is the topo.WorkflowInfo
	wi *topo.WorkflowInfo

	// node is the UI node.
	node *Node

	// logger is the logger we export UI logs from.
	logger *logutil.MemoryLogger
}

// Run is part of the workflow.Workflow interface.
// It updates the UI every second, and checkpoints every 5 seconds.
func (sw *SleepWorkflow) Run(ctx context.Context, manager *Manager, wi *topo.WorkflowInfo) error {
	// Save all values, create a UI Node.
	sw.mu.Lock()
	sw.manager = manager
	sw.wi = wi

	sw.node.Listener = sw
	sw.node.Display = NodeDisplayDeterminate
	sw.node.Actions = []*Action{
		{
			Name:  pauseAction,
			State: ActionStateEnabled,
			Style: ActionStyleNormal,
		},
		{
			Name:  resumeAction,
			State: ActionStateDisabled,
			Style: ActionStyleNormal,
		},
	}
	sw.uiUpdateLocked()
	sw.node.BroadcastChanges(false /* updateChildren */)
	sw.mu.Unlock()

	for {
		sw.mu.Lock()
		data := *sw.data
		sw.mu.Unlock()

		if data.Slept == data.Duration {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			if !data.Paused {
				sw.mu.Lock()
				sw.data.Slept++
				// UI update every second.
				sw.uiUpdateLocked()
				sw.node.BroadcastChanges(false /* updateChildren */)
				// Checkpoint every 5 seconds.
				if sw.data.Slept%5 == 0 {
					if err := sw.checkpointLocked(ctx); err != nil {
						sw.mu.Unlock()
						return err
					}
				}
				sw.mu.Unlock()
			}
		}
	}

	return nil
}

// Action is part of the workflow.ActionListener interface.
func (sw *SleepWorkflow) Action(ctx context.Context, path, name string) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Change our state.
	log.Infof("SleepWorkflow.Action(%v) called.", name)
	paused := sw.data.Paused
	switch name {
	case pauseAction:
		sw.data.Paused = true
		sw.logger.Infof("Paused")
	case resumeAction:
		sw.data.Paused = false
		sw.logger.Infof("Resumed")
	default:
		sw.logger.Errorf("Unknown action %v called", name)
		return fmt.Errorf("unknown action %v", name)
	}
	if paused == sw.data.Paused {
		// Nothing changed.
		return nil
	}

	// UI update and Checkpoint.
	sw.uiUpdateLocked()
	sw.node.BroadcastChanges(false /* updateChildren */)
	return sw.checkpointLocked(ctx)
}

// uiUpdateLocked updates the computed parts of the Node, based on the
// current state.  Needs to be called with the lock / inside
// sw.node.Modify.
func (sw *SleepWorkflow) uiUpdateLocked() {
	sw.node.Progress = 100 * sw.data.Slept / sw.data.Duration
	sw.node.ProgressMessage = fmt.Sprintf("%v/%v", sw.data.Slept, sw.data.Duration)
	sw.node.Log = sw.logger.String()
	if sw.data.Paused {
		sw.node.Actions[0].State = ActionStateDisabled
		sw.node.Actions[1].State = ActionStateEnabled
		sw.node.ProgressMessage += " (paused)"
	} else {
		sw.node.Actions[0].State = ActionStateEnabled
		sw.node.Actions[1].State = ActionStateDisabled
	}
}

// checkpointLocked saves a checkpoint in topo server.
// Needs to be called with the lock.
func (sw *SleepWorkflow) checkpointLocked(ctx context.Context) error {
	var err error
	sw.wi.Data, err = json.Marshal(sw.data)
	if err != nil {
		return err
	}
	err = sw.manager.TopoServer().SaveWorkflow(ctx, sw.wi)
	if err != nil {
		sw.logger.Errorf("SaveWorkflow failed: %v", err)
	} else {
		sw.logger.Infof("SaveWorkflow successful")
	}
	return err
}

// SleepWorkflowFactory is the factory to register the Sleep workflows.
type SleepWorkflowFactory struct{}

// Init is part of the workflow.Factory interface.
func (f *SleepWorkflowFactory) Init(_ *Manager, w *workflowpb.Workflow, args []string) error {
	// Parse the flags.
	subFlags := flag.NewFlagSet(sleepFactoryName, flag.ContinueOnError)
	duration := subFlags.Int("duration", 30, "How long to sleep")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if *duration <= 0 {
		return fmt.Errorf("duration cannot be a negative number")
	}

	// Build the Workflow object.
	w.Name = fmt.Sprintf("Sleep(%v seconds)", *duration)
	data := &SleepWorkflowData{
		Duration: *duration,
	}
	var err error
	w.Data, err = json.Marshal(data)
	if err != nil {
		return err
	}
	return nil
}

// Instantiate is part of the workflow.Factory interface.
func (f *SleepWorkflowFactory) Instantiate(_ *Manager, w *workflowpb.Workflow, rootNode *Node) (Workflow, error) {
	rootNode.Message = "This workflow is a test workflow that just sleeps for the provided amount of time."

	data := &SleepWorkflowData{}
	if err := json.Unmarshal(w.Data, data); err != nil {
		return nil, err
	}
	return &SleepWorkflow{
		data:   data,
		node:   rootNode,
		logger: logutil.NewMemoryLogger(),
	}, nil
}

// Compile time interface check.
var _ Factory = (*SleepWorkflowFactory)(nil)
var _ Workflow = (*SleepWorkflow)(nil)
