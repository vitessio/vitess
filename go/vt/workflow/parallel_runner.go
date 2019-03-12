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
	"fmt"
	"path"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"

	workflowpb "vitess.io/vitess/go/vt/proto/workflow"
)

type level int

const (
	// Sequential means that the tasks will run sequentially.
	Sequential level = iota
	//Parallel means that the tasks will run in parallel.
	Parallel
)

const (
	actionNameRetry                     = "Retry"
	actionNameApproveFirstTask          = "Approve first shard"
	actionNameApproveFirstTaskDone      = "First shard approved"
	actionNameApproveRemainingTasks     = "Approve remaining shards"
	actionNameApproveRemainingTasksDone = "Remaining shards approved"
)

const taskFinishedMessage = "task finished"

// PhaseType is used to store the phase name in a workflow.
type PhaseType string

// ParallelRunner is used to control executing tasks concurrently.
// Each phase has its own ParallelRunner object.
type ParallelRunner struct {
	ctx              context.Context
	uiLogger         *logutil.MemoryLogger
	rootUINode       *Node
	phaseUINode      *Node
	checkpointWriter *CheckpointWriter
	// tasks stores selected tasks for the phase with expected execution order.
	tasks            []*workflowpb.Task
	concurrencyLevel level
	executeFunc      func(context.Context, *workflowpb.Task) error
	enableApprovals  bool

	// mu is used to protect the access to retryActionRegistry, channels for task
	// approvals and serialize UI node changes.
	mu sync.Mutex
	// retryActionRegistry stores the data for retry actions.
	// Each task can retrieve the channel for synchronizing retrying
	// through its UI node path.
	retryActionRegistry    map[string]chan struct{}
	firstTaskApproved      chan struct{}
	remainingTasksApproved chan struct{}
}

// NewParallelRunner returns a new ParallelRunner.
func NewParallelRunner(ctx context.Context, rootUINode *Node, cp *CheckpointWriter, tasks []*workflowpb.Task, executeFunc func(context.Context, *workflowpb.Task) error, concurrencyLevel level, enableApprovals bool) *ParallelRunner {
	if len(tasks) < 1 {
		log.Fatal("BUG: No tasks passed into ParallelRunner")
	}

	phaseID := path.Dir(tasks[0].Id)
	phaseUINode, err := rootUINode.GetChildByPath(phaseID)
	if err != nil {
		log.Fatalf("BUG: nodepath %v not found", phaseID)
	}

	p := &ParallelRunner{
		ctx:                 ctx,
		uiLogger:            logutil.NewMemoryLogger(),
		rootUINode:          rootUINode,
		phaseUINode:         phaseUINode,
		checkpointWriter:    cp,
		tasks:               tasks,
		executeFunc:         executeFunc,
		concurrencyLevel:    concurrencyLevel,
		retryActionRegistry: make(map[string]chan struct{}),
		enableApprovals:     enableApprovals,
	}

	if p.enableApprovals {
		p.initApprovalActions()
	}

	return p
}

// Run is the entry point for controlling task executions.
func (p *ParallelRunner) Run() error {
	// default value is 0. The task will not run in this case.
	var parallelNum int
	switch p.concurrencyLevel {
	case Sequential:
		parallelNum = 1
	case Parallel:
		parallelNum = len(p.tasks)
	default:
		log.Fatalf("BUG: Invalid concurrency level: %v", p.concurrencyLevel)
	}
	// sem is a channel used to control the level of concurrency.
	sem := make(chan bool, parallelNum)
	wg := sync.WaitGroup{}
	for i, task := range p.tasks {
		if isTaskSucceeded(task) {
			continue
		}

		sem <- true
		if p.enableApprovals && !isTaskRunning(task) {
			p.waitForApproval(i)
		}
		select {
		case <-p.ctx.Done():
			// Break this run and return early. Do not try to execute any subsequent tasks.
			log.Infof("Workflow is cancelled, remaining tasks will be aborted")
			return nil
		default:
			wg.Add(1)
			go func(t *workflowpb.Task) {
				defer wg.Done()
				p.setUIMessage(fmt.Sprintf("Launch task: %v.", t.Id))
				defer func() { <-sem }()
				p.executeTask(t)
			}(task)
		}
	}
	wg.Wait()

	if p.enableApprovals {
		p.clearPhaseActions()
	}
	// TODO(yipeiw): collect error message from tasks.Error instead,
	// s.t. if the task is retried, we can update the error
	return nil
}

func (p *ParallelRunner) executeTask(t *workflowpb.Task) {
	taskID := t.Id
	for {
		// Update the task status to running in the checkpoint.
		if updateErr := p.checkpointWriter.UpdateTask(taskID, workflowpb.TaskState_TaskRunning, nil); updateErr != nil {
			// Only logging the error rather then passing it to ErrorRecorder.
			// Errors in ErrorRecorder will lead to the stop of a workflow. We
			// don't want to stop the workflow if only checkpointing fails.
			log.Errorf("%v", updateErr)
		}
		err := p.executeFunc(p.ctx, t)
		// Update the task status to done in the checkpoint.
		if updateErr := p.checkpointWriter.UpdateTask(taskID, workflowpb.TaskState_TaskDone, err); updateErr != nil {
			log.Errorf("%v", updateErr)
		}

		// The function returns if the task is executed successfully.
		if err == nil {
			p.setFinishUIMessage(t.Id)
			p.setUIMessage(fmt.Sprintf("Task %v has finished.", t.Id))
			return
		}
		// When task fails, first check whether the context is canceled.
		// If so, return right away. If not, enable the retry action.
		select {
		case <-p.ctx.Done():
			return
		default:
		}
		retryChannel := p.addRetryAction(taskID)

		// Block the task execution until the retry action is triggered
		// or the context is canceled.
		select {
		case <-retryChannel:
			continue
		case <-p.ctx.Done():
			return
		}
	}
}

// Action handles retrying, approval of the first task and approval of the
// remaining tasks actions. It implements the interface ActionListener.
func (p *ParallelRunner) Action(ctx context.Context, path, name string) error {
	switch name {
	case actionNameRetry:
		// Extract the path relative to the root node.
		parts := strings.Split(path, "/")
		taskID := strings.Join(parts[2:], "/")
		return p.triggerRetry(taskID)
	case actionNameApproveFirstTask:
		p.mu.Lock()
		defer p.mu.Unlock()

		if p.firstTaskApproved != nil {
			close(p.firstTaskApproved)
			p.firstTaskApproved = nil
			return nil
		}
		return fmt.Errorf("ignored the approval action %v because no pending approval found: it might be already approved before", actionNameApproveFirstTask)
	case actionNameApproveRemainingTasks:
		p.mu.Lock()
		defer p.mu.Unlock()

		if p.remainingTasksApproved != nil {
			close(p.remainingTasksApproved)
			p.remainingTasksApproved = nil
			return nil
		}
		return fmt.Errorf("ignored the approval action %v because no pending approval found: it might be already approved before", actionNameApproveRemainingTasks)
	default:
		return fmt.Errorf("unknown action: %v", name)
	}
}

func (p *ParallelRunner) triggerRetry(taskID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Unregister the retry channel.
	retryChannel, ok := p.retryActionRegistry[taskID]
	if !ok {
		return fmt.Errorf("unregistered action for node: %v", taskID)
	}
	delete(p.retryActionRegistry, taskID)

	// Disable the retry action and synchronize for retrying the job.
	node, err := p.rootUINode.GetChildByPath(taskID)
	if err != nil {
		log.Fatalf("BUG: node on child path %v not found", taskID)
	}
	if len(node.Actions) == 0 {
		log.Fatal("BUG: node actions should not be empty")
	}
	node.Actions = []*Action{}
	node.BroadcastChanges(false /* updateChildren */)
	close(retryChannel)
	return nil
}

func (p *ParallelRunner) addRetryAction(taskID string) chan struct{} {
	node, err := p.rootUINode.GetChildByPath(taskID)
	if err != nil {
		log.Fatalf("BUG: node on child path %v not found", taskID)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Register the channel for synchronizing retrying job.
	if _, ok := p.retryActionRegistry[taskID]; ok {
		log.Fatalf("BUG: duplicate retry action for node: %v", taskID)
	}
	retryChannel := make(chan struct{})
	p.retryActionRegistry[taskID] = retryChannel

	// Enable retry action on the node.
	retryAction := &Action{
		Name:  actionNameRetry,
		State: ActionStateEnabled,
		Style: ActionStyleWaiting,
	}
	node.Actions = []*Action{retryAction}
	node.Listener = p
	node.BroadcastChanges(false /* updateChildren */)
	return retryChannel
}

func (p *ParallelRunner) initApprovalActions() {
	// If all tasks have succeeded, no action is added.
	allDone := true
	for _, task := range p.tasks {
		if !isTaskSucceeded(task) {
			allDone = false
			break
		}
	}
	if allDone {
		return
	}

	actionFirstApproval := &Action{
		Name:  actionNameApproveFirstTask,
		State: ActionStateDisabled,
		Style: ActionStyleTriggered,
	}
	if isTaskSucceeded(p.tasks[0]) || isTaskRunning(p.tasks[0]) {
		// Reset the action name if the first task is running or has succeeded.
		actionFirstApproval.Name = actionNameApproveFirstTaskDone
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.phaseUINode.Actions = []*Action{actionFirstApproval}
	// Add the approval action for the remaining tasks,
	// if there are more than one tasks.
	if len(p.tasks) > 1 {
		actionRemainingTasksApproval := &Action{
			Name:  actionNameApproveRemainingTasks,
			State: ActionStateDisabled,
			Style: ActionStyleTriggered,
		}
		if isTaskSucceeded(p.tasks[1]) || isTaskRunning(p.tasks[1]) {
			// Reset the action name if the second task is running or has succeeded.
			actionRemainingTasksApproval.Name = actionNameApproveRemainingTasksDone
		}
		p.phaseUINode.Actions = append(p.phaseUINode.Actions, actionRemainingTasksApproval)
	}
	p.phaseUINode.Listener = p
	p.phaseUINode.BroadcastChanges(false /* updateChildren */)
}

func isTaskSucceeded(task *workflowpb.Task) bool {
	if task.State == workflowpb.TaskState_TaskDone && task.Error == "" {
		return true
	}
	return false
}

func isTaskRunning(task *workflowpb.Task) bool {
	return task.State == workflowpb.TaskState_TaskRunning
}

func (p *ParallelRunner) waitForApproval(taskIndex int) {
	if taskIndex == 0 {
		p.mu.Lock()
		p.firstTaskApproved = make(chan struct{})
		firstTaskApproved := p.firstTaskApproved
		p.updateApprovalActionLocked(0, actionNameApproveFirstTask, ActionStateEnabled, ActionStyleWaiting)
		p.mu.Unlock()

		p.setUIMessage(fmt.Sprintf("approve first task enabled: %v", taskIndex))

		select {
		case <-firstTaskApproved:
			p.mu.Lock()
			defer p.mu.Unlock()
			p.updateApprovalActionLocked(0, actionNameApproveFirstTaskDone, ActionStateDisabled, ActionStyleTriggered)
		case <-p.ctx.Done():
			return
		}
	} else if taskIndex == 1 {
		p.mu.Lock()
		p.remainingTasksApproved = make(chan struct{})

		remainingTasksApproved := p.remainingTasksApproved
		p.updateApprovalActionLocked(1, actionNameApproveRemainingTasks, ActionStateEnabled, ActionStyleWaiting)
		p.mu.Unlock()

		p.setUIMessage(fmt.Sprintf("approve remaining task enabled: %v", taskIndex))

		select {
		case <-remainingTasksApproved:
			p.mu.Lock()
			defer p.mu.Unlock()
			p.updateApprovalActionLocked(1, actionNameApproveRemainingTasksDone, ActionStateDisabled, ActionStyleTriggered)
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *ParallelRunner) updateApprovalActionLocked(index int, name string, state ActionState, style ActionStyle) {
	action := p.phaseUINode.Actions[index]
	action.Name = name
	action.State = state
	action.Style = style
	p.phaseUINode.BroadcastChanges(false /* updateChildren */)
}

func (p *ParallelRunner) clearPhaseActions() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.phaseUINode.Actions) != 0 {
		p.phaseUINode.Actions = []*Action{}
		p.phaseUINode.BroadcastChanges(false /* updateChildren */)
	}
}

func (p *ParallelRunner) setFinishUIMessage(taskID string) {
	taskNode, err := p.rootUINode.GetChildByPath(taskID)
	if err != nil {
		log.Fatalf("BUG: nodepath %v not found", taskID)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	taskNode.Message = taskFinishedMessage
	taskNode.BroadcastChanges(false /* updateChildren */)
}

func (p *ParallelRunner) setUIMessage(message string) {
	p.uiLogger.Infof(message)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.phaseUINode.Log = p.uiLogger.String()
	p.phaseUINode.Message = message
	p.phaseUINode.BroadcastChanges(false /* updateChildren */)
}

// VerifyAllTasksDone checks that all tasks are done in a workflow. This should only be used for test purposes.
func VerifyAllTasksDone(ctx context.Context, ts *topo.Server, uuid string) error {
	return verifyAllTasksState(ctx, ts, uuid, workflowpb.TaskState_TaskDone)
}

//verifyAllTasksState verifies that all tasks are in taskState. Only for tests purposes.
func verifyAllTasksState(ctx context.Context, ts *topo.Server, uuid string, taskState workflowpb.TaskState) error {
	checkpoint, err := checkpoint(ctx, ts, uuid)
	if err != nil {
		return err
	}

	for _, task := range checkpoint.Tasks {
		if task.State != taskState || task.Error != "" {
			return fmt.Errorf("task: %v should succeed: task status: %v, %v", task.Id, task.State, task.Attributes)
		}
	}
	return nil
}

// verifyTask verifies that a task is in taskState. Only for test purposes.
func verifyTask(ctx context.Context, ts *topo.Server, uuid, taskID string, taskState workflowpb.TaskState, taskError string) error {
	checkpoint, err := checkpoint(ctx, ts, uuid)
	if err != nil {
		return err
	}
	task := checkpoint.Tasks[taskID]

	if task.State != taskState || task.Error != taskError {
		return fmt.Errorf("task status: %v, %v fails to match expected status: %v, %v", task.State, task.Error, taskState, taskError)
	}
	return nil
}

// checkpoint gets Worfklow from topo server. Only for test purposes.
func checkpoint(ctx context.Context, ts *topo.Server, uuid string) (*workflowpb.WorkflowCheckpoint, error) {
	wi, err := ts.GetWorkflow(ctx, uuid)
	if err != nil {
		return nil, fmt.Errorf("fail to get workflow for: %v", uuid)
	}
	checkpoint := &workflowpb.WorkflowCheckpoint{}
	if err := proto.Unmarshal(wi.Data, checkpoint); err != nil {
		return nil, fmt.Errorf("fails to get checkpoint for the workflow: %v", err)
	}
	return checkpoint, nil
}
