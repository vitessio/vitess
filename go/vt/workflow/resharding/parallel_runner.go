package resharding

import (
	"fmt"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

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

// ParallelRunner is used to control executing tasks concurrently.
// Each phase has its own ParallelRunner object.
type ParallelRunner struct {
	ctx              context.Context
	rootUINode       *workflow.Node
	checkpointWriter *CheckpointWriter
	// tasks stores selected tasks for the phase with expected execution order.
	tasks            []*workflowpb.Task
	concurrencyLevel level
	executeFunc      func(context.Context, *workflowpb.Task) error

	// mu is used to protect the access to retryActionRegistry and
	// serialize UI node changes.
	mu sync.Mutex
	// retryActionRegistry stores the data for retry actions.
	// Each task can retrieve the channel for synchronizing retrying
	// through its UI node path.
	retryActionRegistry map[string]chan struct{}

	// reportTaskStatus gives the worklflow debug option to output the task
	// status through UI.
	// TODO(yipeiw): We will remove this option and make it always report task
	// status, once we can unit test resharding workflow through manager
	// (we ignore creating UI nodes when manually creating the workflow now).
	reportTaskStatus bool
}

// NewParallelRunner returns a new ParallelRunner.
func NewParallelRunner(ctx context.Context, rootUINode *workflow.Node, cp *CheckpointWriter, tasks []*workflowpb.Task, executeFunc func(context.Context, *workflowpb.Task) error, concurrencyLevel level) *ParallelRunner {
	return &ParallelRunner{
		ctx:                 ctx,
		rootUINode:          rootUINode,
		checkpointWriter:    cp,
		tasks:               tasks,
		executeFunc:         executeFunc,
		concurrencyLevel:    concurrencyLevel,
		retryActionRegistry: make(map[string]chan struct{}),
		reportTaskStatus:    false,
	}
}

// Run is the entry point for controling task executions.
func (p *ParallelRunner) Run() error {
	var parallelNum int // default value is 0. The task will not run in this case.
	switch p.concurrencyLevel {
	case Sequential:
		parallelNum = 1
	case Parallel:
		parallelNum = len(p.tasks)
	default:
		panic(fmt.Sprintf("BUG: Invalid concurrency level: %v", p.concurrencyLevel))
	}

	// sem is a channel used to control the level of concurrency.
	sem := make(chan bool, parallelNum)
	for _, task := range p.tasks {
		if task.State == workflowpb.TaskState_TaskDone && task.Error == "" {
			continue
		}

		sem <- true
		go func(t *workflowpb.Task) {
			defer func() { <-sem }()
			defer p.setFinishUIMessage(t.Id)

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
					log.Infof("task %v has finished.", taskID)
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
		}(task)
	}

	// Wait until all running jobs are done.
	for i := 0; i < parallelNum; i++ {
		sem <- true
	}
	// TODO: collect error message from tasks.Error instead, s.t. if the task is retried, we can update the error
	return nil
}

// Action handles the retry action. It implements the interface ActionListener.
func (p *ParallelRunner) Action(ctx context.Context, path, name string) error {
	switch name {
	case "Retry":
		// Extract the path relative to the root node.
		parts := strings.Split(path, "/")
		taskID := strings.Join(parts[2:], "/")
		return p.triggerRetry(taskID)
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
		panic(fmt.Sprintf("BUG: node on child path %v not found", taskID))
	}
	if len(node.Actions) == 0 {
		panic(fmt.Sprintf("BUG: node actions should not be empty"))
	}
	node.Actions = []*workflow.Action{}
	node.BroadcastChanges(false /* updateChildren */)
	close(retryChannel)
	return nil
}

func (p *ParallelRunner) addRetryAction(taskID string) chan struct{} {
	node, err := p.rootUINode.GetChildByPath(taskID)
	if err != nil {
		panic(fmt.Sprintf("BUG: node on child path %v not found", taskID))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Register the channel for synchronizing retrying job.
	if _, ok := p.retryActionRegistry[taskID]; ok {
		panic(fmt.Sprintf("BUG: duplicate retry action for node: %v", taskID))
	}
	retryChannel := make(chan struct{})
	p.retryActionRegistry[taskID] = retryChannel

	// Enable retry action on the node.
	retryAction := &workflow.Action{
		Name:  "Retry",
		State: workflow.ActionStateEnabled,
		Style: workflow.ActionStyleWaiting,
	}
	node.Actions = []*workflow.Action{retryAction}
	node.Listener = p
	node.BroadcastChanges(false /* updateChildren */)
	return retryChannel
}

func (p *ParallelRunner) setFinishUIMessage(taskID string) {
	if p.reportTaskStatus {
		taskNode, err := p.rootUINode.GetChildByPath(taskID)
		if err != nil {
			panic(fmt.Errorf("nodepath %v not found", taskID))
		}

		p.mu.Lock()
		defer p.mu.Unlock()
		taskNode.Message = fmt.Sprintf("task %v finished", taskID)
		taskNode.BroadcastChanges(false /* updateChildren */)
	}
}
