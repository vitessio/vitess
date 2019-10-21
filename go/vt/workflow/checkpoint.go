/*
Copyright 2019 The Vitess Authors.

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

package workflow

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/vt/topo"

	workflowpb "vitess.io/vitess/go/vt/proto/workflow"
)

// CheckpointWriter saves the checkpoint data into topology server.
type CheckpointWriter struct {
	topoServer *topo.Server

	// checkpointMu is used for protecting data access during checkpointing.
	mu         sync.Mutex
	checkpoint *workflowpb.WorkflowCheckpoint
	wi         *topo.WorkflowInfo
}

// NewCheckpointWriter creates a CheckpointWriter.
func NewCheckpointWriter(ts *topo.Server, checkpoint *workflowpb.WorkflowCheckpoint, wi *topo.WorkflowInfo) *CheckpointWriter {
	return &CheckpointWriter{
		topoServer: ts,
		checkpoint: checkpoint,
		wi:         wi,
	}
}

// UpdateTask updates the task status in the checkpointing copy and
// saves the full checkpoint to the topology server.
func (c *CheckpointWriter) UpdateTask(taskID string, status workflowpb.TaskState, err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	errorMessage := ""
	if err != nil {
		errorMessage = err.Error()
	}

	t := c.checkpoint.Tasks[taskID]
	t.State = status
	t.Error = errorMessage
	return c.saveLocked()
}

func (c *CheckpointWriter) saveLocked() error {
	var err error
	c.wi.Data, err = proto.Marshal(c.checkpoint)
	if err != nil {
		return err
	}
	return c.topoServer.SaveWorkflow(context.TODO(), c.wi)
}
