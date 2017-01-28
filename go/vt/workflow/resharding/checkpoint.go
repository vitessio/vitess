package resharding

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/youtube/vitess/go/vt/topo"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

// Checkpoint defines the interface for checkpointer.
type Checkpoint interface {
	CheckpointFunc(*workflowpb.WorkflowCheckpoint) error
}

// CheckpointTopology checkpoints the data into topology server.
type CheckpointTopology struct {
	topoServer *topo.Server
}

// CheckpointFunc implements Checkpoint.CheckpointFunc
func (c *CheckpointTopology) CheckpointFunc(s *workflowpb.WorkflowCheckpoint) error {
	var err error
	var data []byte
	data, err = json.Marshal(s)
	if err != nil {
		return err
	}
	wi := new(topo.WorkflowInfo)
	wi.Data = data
	return c.topoServer.SaveWorkflow(context.TODO(), wi)
}

// CheckpointFile checkpoints the data into local files. This is used for debugging.
type CheckpointFile struct {
	FilePath string
	counter  int
}

// CheckpointFunc implements Checkpoint.CheckpointFunc
func (c *CheckpointFile) CheckpointFunc(s *workflowpb.WorkflowCheckpoint) error {
	file, err := os.Create(fmt.Sprintf("%v_%v", c.FilePath, c.counter))
	c.counter++

	if err != nil {
		return err
	}
	defer file.Close()
	fmt.Fprintln(file, fmt.Sprintf("code version: %v", s.CodeVersion))
	for _, task := range s.Tasks {
		fmt.Fprintln(file, fmt.Sprintf("task: state: %v\n      attributes: %v\n", task.State, task.Attributes))
	}
	return nil
}
