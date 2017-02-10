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
	SEQUENTIAL level = 1 + iota
	//PARALLEL means that the tasks will run in parallel.
	PARALLEL
)

// ParallelRunner is used to control executing tasks concurrently.
// Each phase has its own ParallelRunner object.
type ParallelRunner struct {
	ctx context.Context

	checkpointWriter *CheckpointWriter
	taskUINodes      map[string]*workflow.Node
	tasks            []*workflowpb.Task

	// mu is used to protect the retryActionRegistery.
	mu sync.Mutex
	// retryAtionRegistry stores the data for all actions. Each task can retrieve its control object through task ID.
	retryActionRegistery map[string]*RetryController
}

func NewParallelRunner(ctx context.Context, cp *CheckpointWriter, taskUINodes map[string]*workflow.Node, tasks []*workflowpb.Task) *ParallelRunner {
	p := &ParallelRunner{
		ctx:              ctx,
		checkpointWriter: cp,
		taskUINodes:      taskUINodes,
		tasks:            tasks,
	}
	p.retryActionRegistery = make(map[string]*RetryController)
	return p
}

// Run is the entry point for controling task executions.
// tasks should be a copy of tasks with the expected execution order, the status of task should be
// both updated in this copy and the original one (checkpointer.UpdateTask does this). This is used
// to avoid data racing situation.
func (p *ParallelRunner) Run(executeFunc func(map[string]string) error, concurrencyLevel level) error {
	var parallelNum int // default value is 0. The task will not run in this case.
	switch concurrencyLevel {
	case SEQUENTIAL:
		parallelNum = 1
	case PARALLEL:
		parallelNum = len(p.tasks)
	default:
		panic(fmt.Sprintf("BUG: Invalid concurrency level: %v", concurrencyLevel))
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

			taskID := t.Id
			for {
				err := executeFunc(t.Attributes)
				t.State = workflowpb.TaskState_TaskDone
				if err != nil {
					t.Error = err.Error()
				}

				if updateErr := p.checkpointWriter.UpdateTask(taskID, workflowpb.TaskState_TaskDone, t.Error); updateErr != nil {
					// Only logging the error rather then propograting it through ErrorRecorder. Error message in
					// ErrorRecorder will lead to the stop of the workflow, which is unexpected if only checkpointing fails.
					// If the checkpointing fails during initialization, we should stop the workflow.
					log.Errorf("%v", updateErr)
				}

				if err == nil {
					t.Error = ""
					return
				}

				// If task fails, the retry action is enabled.
				n, ok := p.taskUINodes[taskID]
				if !ok {
					log.Errorf("UI node not found for task %v", taskID)
					return
				}

				retryAction := &workflow.Action{
					Name:  "Retry",
					State: workflow.ActionStateEnabled,
					Style: workflow.ActionStyleWaiting,
				}
				n.Actions = []*workflow.Action{retryAction}
				n.Listener = p

				p.mu.Lock()
				p.retryActionRegistery[taskID] = &RetryController{
					node:         n,
					retryChannel: make(chan struct{}),
				}
				p.mu.Unlock()
				n.BroadcastChanges(false /* updateChildren */)

				// Block the task execution until the retry action is triggered or the job is canceled.
				select {
				case <-p.retryActionRegistery[taskID].retryChannel:
					continue
				case <-p.ctx.Done():
					p.retryActionRegistery = nil
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
func (p *ParallelRunner) Action(ctx context.Context, pathName, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	actionID := getTaskID(pathName)
	c, ok := p.retryActionRegistery[actionID]
	if !ok {
		return fmt.Errorf("Unknown node path for the action: %v", pathName)
	}

	switch name {
	case "Retry":
		c.closeRetryChannel()
		delete(p.retryActionRegistery, actionID)
	default:
		return fmt.Errorf("Unknown action: %v", name)
	}
	return nil
}

func getTaskID(nodePath string) string {
	return path.Base(nodePath)
}
