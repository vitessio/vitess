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
	"errors"
	"flag"
	"fmt"
	"strconv"
	"sync"

	"golang.org/x/net/context"

	"github.com/gogo/protobuf/proto"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"

	workflowpb "vitess.io/vitess/go/vt/proto/workflow"
)

const (
	testWorkflowFactoryName = "test_workflow"

	phaseSimple PhaseType = "simple"

	errMessage = "fake error for testing retry"
)

func createTestTaskID(phase PhaseType, count int) string {
	return fmt.Sprintf("%s/%v", phase, count)
}

func init() {
	Register(testWorkflowFactoryName, &TestWorkflowFactory{})
}

// TestWorkflowFactory is the factory to create a test workflow.
type TestWorkflowFactory struct{}

// Init is part of the workflow.Factory interface.
func (*TestWorkflowFactory) Init(_ *Manager, w *workflowpb.Workflow, args []string) error {
	subFlags := flag.NewFlagSet(testWorkflowFactoryName, flag.ContinueOnError)
	retryFlag := subFlags.Bool("retry", false, "The retry flag should be true if the retry action should be tested")
	count := subFlags.Int("count", 0, "The number of simple tasks")
	enableApprovals := subFlags.Bool("enable_approvals", false, "If true, executions of tasks require user's approvals on the UI.")
	sequential := subFlags.Bool("sequential", false, "If true, executions of tasks are sequential")
	if err := subFlags.Parse(args); err != nil {
		return err
	}

	// Initialize the checkpoint.
	taskMap := make(map[string]*workflowpb.Task)
	for i := 0; i < *count; i++ {
		taskID := createTestTaskID(phaseSimple, i)
		taskMap[taskID] = &workflowpb.Task{
			Id:         taskID,
			State:      workflowpb.TaskState_TaskNotStarted,
			Attributes: map[string]string{"number": fmt.Sprintf("%v", i)},
		}
	}
	checkpoint := &workflowpb.WorkflowCheckpoint{
		CodeVersion: 0,
		Tasks:       taskMap,
		Settings:    map[string]string{"count": fmt.Sprintf("%v", *count), "retry": fmt.Sprintf("%v", *retryFlag), "enable_approvals": fmt.Sprintf("%v", *enableApprovals), "sequential": fmt.Sprintf("%v", *sequential)},
	}
	var err error
	w.Data, err = proto.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return nil
}

// Instantiate is part the workflow.Factory interface.
func (*TestWorkflowFactory) Instantiate(m *Manager, w *workflowpb.Workflow, rootNode *Node) (Workflow, error) {
	checkpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(w.Data, checkpoint); err != nil {
		return nil, err
	}
	// Get the retry flags for all tasks from the checkpoint.
	retry, err := strconv.ParseBool(checkpoint.Settings["retry"])
	if err != nil {
		log.Errorf("converting retry in checkpoint.Settings to bool fails: %v", checkpoint.Settings["retry"])
		return nil, err
	}
	retryFlags := make(map[string]bool)
	for _, task := range checkpoint.Tasks {
		retryFlags[task.Id] = retry
	}

	// Get the user control flags from the checkpoint.
	enableApprovals, err := strconv.ParseBool(checkpoint.Settings["enable_approvals"])
	if err != nil {
		log.Errorf("converting enable_approvals in checkpoint.Settings to bool fails: %v", checkpoint.Settings["user_control"])
		return nil, err
	}

	sequential, err := strconv.ParseBool(checkpoint.Settings["sequential"])
	if err != nil {
		log.Errorf("converting sequential in checkpoint.Settings to bool fails: %v", checkpoint.Settings["user_control"])
		return nil, err
	}

	tw := &TestWorkflow{
		topoServer:      m.TopoServer(),
		manager:         m,
		checkpoint:      checkpoint,
		rootUINode:      rootNode,
		logger:          logutil.NewMemoryLogger(),
		retryFlags:      retryFlags,
		enableApprovals: enableApprovals,
		sequential:      sequential,
	}

	count, err := strconv.Atoi(checkpoint.Settings["count"])
	if err != nil {
		log.Errorf("converting count in checkpoint.Settings to int fails: %v", checkpoint.Settings["count"])
		return nil, err
	}

	phaseNode := &Node{
		Name:     string(phaseSimple),
		PathName: string(phaseSimple),
	}
	tw.rootUINode.Children = append(tw.rootUINode.Children, phaseNode)

	for i := 0; i < count; i++ {
		taskName := fmt.Sprintf("%v", i)
		taskUINode := &Node{
			Name:     taskName,
			PathName: taskName,
		}
		phaseNode.Children = append(phaseNode.Children, taskUINode)
	}
	return tw, nil
}

// TestWorkflow is used to unit test the ParallelRunner object. It is a
// simplified workflow of one phase. To test the ParallelRunner's retry
// behavior, we can let the tasks explicitly fail initially and succeed
// after a retry.
type TestWorkflow struct {
	ctx        context.Context
	manager    *Manager
	topoServer *topo.Server
	wi         *topo.WorkflowInfo
	logger     *logutil.MemoryLogger

	retryMu sync.Mutex
	// retryFlags stores the retry flag for all tasks.
	retryFlags map[string]bool

	rootUINode *Node

	checkpoint       *workflowpb.WorkflowCheckpoint
	checkpointWriter *CheckpointWriter

	enableApprovals bool
	sequential      bool
}

// Run implements the workflow.Workflow interface.
func (tw *TestWorkflow) Run(ctx context.Context, manager *Manager, wi *topo.WorkflowInfo) error {
	tw.ctx = ctx
	tw.wi = wi
	tw.checkpointWriter = NewCheckpointWriter(tw.topoServer, tw.checkpoint, tw.wi)

	tw.rootUINode.Display = NodeDisplayDeterminate
	tw.rootUINode.BroadcastChanges(true /* updateChildren */)

	simpleTasks := tw.getTasks(phaseSimple)
	concurrencyLevel := Parallel
	if tw.sequential {
		concurrencyLevel = Sequential
	}
	simpleRunner := NewParallelRunner(tw.ctx, tw.rootUINode, tw.checkpointWriter, simpleTasks, tw.runSimple, concurrencyLevel, tw.enableApprovals)
	return simpleRunner.Run()
}

func (tw *TestWorkflow) getTasks(phaseName PhaseType) []*workflowpb.Task {
	count, err := strconv.Atoi(tw.checkpoint.Settings["count"])
	if err != nil {
		log.Info("converting count in checkpoint.Settings to int failed: %v", tw.checkpoint.Settings["count"])
		return nil
	}
	var tasks []*workflowpb.Task
	for i := 0; i < count; i++ {
		taskID := createTestTaskID(phaseName, i)
		tasks = append(tasks, tw.checkpoint.Tasks[taskID])
	}
	return tasks
}

func (tw *TestWorkflow) runSimple(ctx context.Context, t *workflowpb.Task) error {
	log.Info("The number passed to me is %v", t.Attributes["number"])

	tw.retryMu.Lock()
	defer tw.retryMu.Unlock()
	if tw.retryFlags[t.Id] {
		log.Info("I will fail at this time since retry flag is true.")
		tw.retryFlags[t.Id] = false
		return errors.New(errMessage)
	}
	return nil
}
