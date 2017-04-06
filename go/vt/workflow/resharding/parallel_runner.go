package resharding

import (
	"fmt"
	"path"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/workflow"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
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

// ParallelRunner is used to control executing tasks concurrently.
// Each phase has its own ParallelRunner object.
type ParallelRunner struct {
	ctx              context.Context
	uiLogger         *logutil.MemoryLogger
	rootUINode       *workflow.Node
	phaseUINode      *workflow.Node
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
func NewParallelRunner(ctx context.Context, rootUINode *workflow.Node, cp *CheckpointWriter, tasks []*workflowpb.Task, executeFunc func(context.Context, *workflowpb.Task) error, concurrencyLevel level, enableApprovals bool) *ParallelRunner {
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

// Run is the entry point for controling task executions.
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
		wg.Add(1)
		go func(t *workflowpb.Task) {
			defer wg.Done()
			p.setUIMessage(fmt.Sprintf("Launch task: %v.", t.Id))
			defer func() { <-sem }()
			p.executeTask(t)
		}(task)
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
		return fmt.Errorf("Unknown action: %v", name)
	}
}

func (p *ParallelRunner) triggerRetry(taskID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Unregister the retry channel.
	retryChannel, ok := p.retryActionRegistry[taskID]
	if !ok {
		return fmt.Errorf("Unregistered action for node: %v", taskID)
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
	node.Actions = []*workflow.Action{}
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
	retryAction := &workflow.Action{
		Name:  actionNameRetry,
		State: workflow.ActionStateEnabled,
		Style: workflow.ActionStyleWaiting,
	}
	node.Actions = []*workflow.Action{retryAction}
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

	actionFirstApproval := &workflow.Action{
		Name:  actionNameApproveFirstTask,
		State: workflow.ActionStateDisabled,
		Style: workflow.ActionStyleTriggered,
	}
	if isTaskSucceeded(p.tasks[0]) || isTaskRunning(p.tasks[0]) {
		// Reset the action name if the first task is running or has succeeded.
		actionFirstApproval.Name = actionNameApproveFirstTaskDone
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.phaseUINode.Actions = []*workflow.Action{actionFirstApproval}
	// Add the approval action for the remaining tasks,
	// if there are more than one tasks.
	if len(p.tasks) > 1 {
		actionRemainingTasksApproval := &workflow.Action{
			Name:  actionNameApproveRemainingTasks,
			State: workflow.ActionStateDisabled,
			Style: workflow.ActionStyleTriggered,
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
	if task.State == workflowpb.TaskState_TaskRunning {
		return true
	}
	return false
}

func (p *ParallelRunner) waitForApproval(taskIndex int) {
	if taskIndex == 0 {
		p.mu.Lock()
		p.firstTaskApproved = make(chan struct{})
		firstTaskApproved := p.firstTaskApproved
		p.updateApprovalActionLocked(0, actionNameApproveFirstTask, workflow.ActionStateEnabled, workflow.ActionStyleWaiting)
		p.mu.Unlock()

		p.setUIMessage(fmt.Sprintf("approve first task enabled: %v", taskIndex))

		select {
		case <-firstTaskApproved:
			p.mu.Lock()
			defer p.mu.Unlock()
			p.updateApprovalActionLocked(0, actionNameApproveFirstTaskDone, workflow.ActionStateDisabled, workflow.ActionStyleTriggered)
		case <-p.ctx.Done():
			return
		}
	} else if taskIndex == 1 {
		p.mu.Lock()
		p.remainingTasksApproved = make(chan struct{})

		remainingTasksApproved := p.remainingTasksApproved
		p.updateApprovalActionLocked(1, actionNameApproveRemainingTasks, workflow.ActionStateEnabled, workflow.ActionStyleWaiting)
		p.mu.Unlock()

		p.setUIMessage(fmt.Sprintf("approve remaining task enabled: %v", taskIndex))

		select {
		case <-remainingTasksApproved:
			p.mu.Lock()
			defer p.mu.Unlock()
			p.updateApprovalActionLocked(1, actionNameApproveRemainingTasksDone, workflow.ActionStateDisabled, workflow.ActionStyleTriggered)
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *ParallelRunner) updateApprovalActionLocked(index int, name string, state workflow.ActionState, style workflow.ActionStyle) {
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
		p.phaseUINode.Actions = []*workflow.Action{}
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
