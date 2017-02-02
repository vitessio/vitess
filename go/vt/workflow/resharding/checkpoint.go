package resharding

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/youtube/vitess/go/vt/topo"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

// CheckpointWriter save the checkpoint data into topology server.
type CheckpointWriter struct {
	topoServer topo.Server

	// checkpointMu is used for protecting data access during checkpointing.
	checkpointMu sync.Mutex
	checkpoint   *workflowpb.WorkflowCheckpoint
	wi           *topo.WorkflowInfo
}

// NewCheckpointWriter creates a CheckpointWriter.
func NewCheckpointWriter(ts topo.Server, checkpoint *workflowpb.WorkflowCheckpoint, wi *topo.WorkflowInfo) *CheckpointWriter {
	return &CheckpointWriter{
		topoServer: ts,
		checkpoint: checkpoint,
		wi:         wi,
	}
}

// UpdateTask updates the status and checkpointing the update.
func (c *CheckpointWriter) UpdateTask(taskID string, status workflowpb.TaskState) error {
	c.checkpointMu.Lock()
	defer c.checkpointMu.Unlock()

	c.checkpoint.Tasks[taskID].State = status
	return c.Save()
}

// Save packets the checkpoint and sends it to the topology server.
func (c *CheckpointWriter) Save() error {
	var err error
	c.wi.Data, err = json.Marshal(c.checkpoint)
	if err != nil {
		return err
	}
	return c.topoServer.SaveWorkflow(context.TODO(), c.wi)
}
