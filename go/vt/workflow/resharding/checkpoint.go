package resharding

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/youtube/vitess/go/vt/topo"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

// Checkpoint checkpoints the data into topology server.
type Checkpoint struct {
	topoServer topo.Server
	// checkpointMu is used for protecting data access during checkpointing.
	checkpointMu sync.Mutex
	wcp          *workflowpb.WorkflowCheckpoint
	wi           *topo.WorkflowInfo
}

// Update update the status and checkpointing the update.
func (c *Checkpoint) Update(taskID string, status workflowpb.TaskState) error {
	c.checkpointMu.Lock()
	defer c.checkpointMu.Unlock()
	c.wcp.Tasks[taskID].State = status
	return c.Store()
}

// Store packets the checkpoint and sends it to the topology server.
func (c *Checkpoint) Store() error {
	var err error
	var data []byte
	data, err = json.Marshal(c.wcp)
	if err != nil {
		return err
	}
	c.wi.Data = data
	return c.topoServer.SaveWorkflow(context.TODO(), c.wi)
}

// CheckpointFile checkpoints the data into local files. This is used for debugging.
//type CheckpointFile struct {
//	FilePath string
//	counter  int
//}
//
//// CheckpointFunc implements Checkpoint.CheckpointFunc
//func (c *CheckpointFile) Checkpoint(s *workflowpb.WorkflowCheckpoint) error {
//	file, err := os.Create(fmt.Sprintf("%v_%v", c.FilePath, c.counter))
//	c.counter++
//
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//	fmt.Fprintln(file, fmt.Sprintf("code version: %v", s.CodeVersion))
//	for _, task := range s.Tasks {
//		fmt.Fprintln(file, fmt.Sprintf("task: state: %v\n      attributes: %v\n", task.State, task.Attributes))
//	}
//	return nil
//}
