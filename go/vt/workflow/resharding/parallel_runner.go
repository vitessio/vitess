package resharding

import (
	"fmt"
	"path"
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/workflow"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

type level int

const (
	// SEQUENTIAL means that the tasks will run sequentially.
	SEQUENTIAL level = iota
	//PARALLEL means that the tasks will run in parallel.
	PARALLEL
)

// ParallelRunner is used to control executing tasks concurrently.
// Each phase has its own ParallelRunner object.
type ParallelRunner struct {
	ctx              context.Context
	nodeManager      *workflow.NodeManager
	phaseUINode      *workflow.Node
	checkpointWriter *CheckpointWriter
	// tasks stores selected tasks for the phase with expected execution order.
	tasks            []*workflowpb.Task
	concurrencyLevel level
	executeFunc      func(context.Context, *workflowpb.Task) error
	// mu is used to protect the retryActionRegistery.
	mu sync.Mutex
	// retryAtionRegistry stores the data for retry actions.
	// Each task can retrieve its RetryController through its UI node path.
	retryActionRegistry map[string]*RetryController
	// reportTaskStatus gives the worklfow debug option to output the task status through UI.
	reportTaskStatus bool
	// taskFinished stores the channels for synchroizing the finish of tasks.
	taskFinished map[string]chan struct{}
}

func NewParallelRunner(ctx context.Context, nodeManager *workflow.NodeManager, phaseUINode *workflow.Node, cp *CheckpointWriter, tasks []*workflowpb.Task, executeFunc func(context.Context, *workflowpb.Task) error, concurrencyLevel level) *ParallelRunner {
	return &ParallelRunner{
		ctx:                 ctx,
		nodeManager:         nodeManager,
		phaseUINode:         phaseUINode,
		checkpointWriter:    cp,
		tasks:               tasks,
		executeFunc:         executeFunc,
		concurrencyLevel:    concurrencyLevel,
		retryActionRegistry: make(map[string]*RetryController),
		reportTaskStatus:    false,
		taskFinished:        make(map[string]chan struct{}),
	}
}

// Run is the entry point for controling task executions.
func (p *ParallelRunner) Run() error {
	var parallelNum int // default value is 0. The task will not run in this case.
	switch p.concurrencyLevel {
	case SEQUENTIAL:
		parallelNum = 1
	case PARALLEL:
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
		p.taskFinished[task.Id] = make(chan struct{})
		go func(t *workflowpb.Task) {
			defer func() { <-sem }()
			defer close(p.taskFinished[t.Id])

			taskID := t.Id
			for {
				err := p.executeFunc(p.ctx, t)
				// Update the task status in the checkpoint.
				if updateErr := p.checkpointWriter.UpdateTask(taskID, workflowpb.TaskState_TaskDone, err); updateErr != nil {
					// Only logging the error rather then passing it to ErrorRecorder.
					// Errors in ErrorRecorder will lead to the stop of a workflow. We
					// don't want to stop the workflow if only checkpointing fails.
					log.Errorf("%v", updateErr)
				}

				// The function returns if the task is executed successfully.
				if err == nil {
					return
				}
				// When task fails, first check whether the context is cancelled.
				// If so, return right away. If not, enable the retry action.
				select {
				case <-p.ctx.Done():
					return
				default:
				}

				fmt.Printf("enabling retry action for task: %v", taskID)

				retryChannel, registerID := p.addRetryAction(taskID)

				// Block the task execution until the retry action is triggered
				// or the context is canceled.
				select {
				case <-retryChannel:
					continue
				case <-p.ctx.Done():
					p.unregisterRetryController(registerID)
					return
				}
			}
		}(task)

		// Update task finish information on the UI.
		if p.reportTaskStatus {
			go p.setFinishUIMessage(task.Id)
		}
	}

	// Wait until all running jobs are done.
	for i := 0; i < parallelNum; i++ {
		sem <- true
	}
	// TODO: collect error message from tasks.Error instead, s.t. if the task is retried, we can update the error
	return nil
}

// Action handles the retry action. It implements the interface ActionListener.
func (p *ParallelRunner) Action(ctx context.Context, pathName, name string) error {
	switch name {
	case "Retry":
		return p.triggerRetry(pathName)
	default:
		return fmt.Errorf("Unknown action: %v", name)
	}
}

func (p *ParallelRunner) addRetryAction(taskID string) (chan struct{}, string) {
	taskNodePath := path.Join(p.phaseUINode.Path, taskID)
	node, err := p.nodeManager.GetNodeByPath(taskNodePath)
	if err != nil {
		panic(fmt.Errorf("nodepath %v not found", taskNodePath))
	}

	retryController := CreateRetryController(node, p /* actionListener */)
	p.registerRetryController(node.Path, retryController)
	node.BroadcastChanges(false /* updateChildren */)
	return retryController.retryChannel, node.PathName
}

func (p *ParallelRunner) triggerRetry(nodePath string) error {
	p.mu.Lock()
	c, ok := p.retryActionRegistry[nodePath]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("Unknown node path for the action: %v", nodePath)
	}
	p.mu.Unlock()

	p.unregisterRetryController(nodePath)
	c.triggerRetry()
	return nil
}

func (p *ParallelRunner) registerRetryController(nodePath string, c *RetryController) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.retryActionRegistry[nodePath]; ok {
		panic(fmt.Errorf("duplicate retry action on node: %v", nodePath))
	}
	p.retryActionRegistry[nodePath] = c
}

func (p *ParallelRunner) unregisterRetryController(nodePath string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.retryActionRegistry[nodePath]; !ok {
		log.Warningf("retry action on node: %v doesn't exist, cannot unregister it", nodePath)
	} else {
		delete(p.retryActionRegistry, nodePath)
	}
}

func (p *ParallelRunner) setFinishUIMessage(taskID string) {
	done, ok := p.taskFinished[taskID]
	if !ok {
		panic(fmt.Errorf("the finish channl for task %v not found", taskID))
	}

	taskNodePath := path.Join(p.phaseUINode.Path, taskID)
	taskNode, err := p.nodeManager.GetNodeByPath(taskNodePath)
	if err != nil {
		panic(fmt.Errorf("nodepath %v not found", taskNodePath))
	}

	select {
	case <-done:
		taskNode.Message = fmt.Sprintf("task %v finished", taskID)
		taskNode.BroadcastChanges(false /* updateChildren */)
	case <-p.ctx.Done():
		return
	}
}
