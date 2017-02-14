package resharding

import (
	"context"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/topo"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

// CheckpointWriter saves the checkpoint data into topology server.
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

// UpdateTask updates the status of task in the checkpoint.
func (c *CheckpointWriter) UpdateTask(taskID string, status workflowpb.TaskState, err string) error {
	// Writing the checkpoint is protected to avoid the situation that the
	// task value is partially updated when saving the checkpoint.
	c.checkpointMu.Lock()
	defer c.checkpointMu.Unlock()

	c.checkpoint.Tasks[taskID].State = status
	c.checkpoint.Tasks[taskID].Error = err
	return c.Save()
}

// Save packets the checkpoint and sends it to the topology server.
func (c *CheckpointWriter) Save() error {
	c.checkpointMu.Lock()
	defer c.checkpointMu.Unlock()
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
