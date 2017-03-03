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
	mu         sync.Mutex
	checkpoint *workflowpb.WorkflowCheckpoint
	wi         *topo.WorkflowInfo
}

// NewCheckpointWriter creates a CheckpointWriter.
func NewCheckpointWriter(ts topo.Server, checkpoint *workflowpb.WorkflowCheckpoint, wi *topo.WorkflowInfo) *CheckpointWriter {
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
