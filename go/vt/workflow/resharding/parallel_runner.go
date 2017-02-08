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
	// actionRegistry stores the data for all actions. Each task can retrieve its control object through task ID.
	actionRegistery map[string]*Control
}

// Control stores the data for controlling through an action.
type Control struct {
	node *workflow.Node
	// mu is used to protect access to retryChannel.
	mu sync.Mutex
	// retryChannel is used to trigger the retrying of task when pressing the button.
	retryChannel chan struct{}
}

// Run is the entry point for controling task executions.
// tasks should be a copy of tasks with the expected execution order, the status of task should be
// both updated in this copy and the original one (checkpointer.UpdateTask does this). This is used
// to avoid data racing situation.
func (p *ParallelRunner) Run(taskUINodes map[string]*workflow.Node, tasks []*workflowpb.Task, executeFunc func(map[string]string) error, cp *CheckpointWriter, concurrencyLevel level) error {
	var parallelNum int // default value is 0. The task will not run in this case.
	switch concurrencyLevel {
	case SEQUENTIAL:
		parallelNum = 1
	case PARALLEL:
		parallelNum = len(tasks)
	default:
		panic(fmt.Sprintf("BUG: Invalid concurrency level: %v", concurrencyLevel))
	}

	p.actionRegistery = make(map[string]*Control)

	// sem is a channel used to control the level of concurrency.
	sem := make(chan bool, parallelNum)
	for _, task := range tasks {
		if task.State == workflowpb.TaskState_TaskDone && task.Error == "" {
			continue
		}

		sem <- true
		go func(t *workflowpb.Task) {
			defer func() { <-sem }()

			taskId := t.Id
			for {
				err := executeFunc(t.Attributes)
				t.State = workflowpb.TaskState_TaskDone
				if err != nil {
					t.Error = err.Error()
				}

				if updateErr := cp.UpdateTask(taskId, workflowpb.TaskState_TaskDone, t.Error); updateErr != nil {
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
				n, ok := taskUINodes[taskId]
				if !ok {
					log.Errorf("UI node not found for task %v", taskId)
					return
				}

				retryAction := &workflow.Action{
					Name:  "Retry",
					State: workflow.ActionStateEnabled,
					Style: workflow.ActionStyleWaiting,
				}
				n.Actions = []*workflow.Action{retryAction}
				n.Listener = p
				p.actionRegistery[taskId] = &Control{
					node:         n,
					retryChannel: make(chan struct{}),
				}
				n.BroadcastChanges(false /* updateChildren */)

				// Block the task execution until the retry action is triggered or the job is canceled.
				select {
				case <-p.actionRegistery[taskId].retryChannel:
					fmt.Println("retry action is pressed")
					continue
				case <-p.ctx.Done():
					p.closeRetryChannel(p.actionRegistery[taskId])
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

func (p *ParallelRunner) Action(ctx context.Context, pathName, name string) error {
	// path needs to be taskId.
	log.Errorf("action function is called pathName:%v, %v, name: %v\n", pathName, getTaskId(pathName), name)

	c, ok := p.actionRegistery[getTaskId(pathName)]
	if !ok {
		return fmt.Errorf("Unknown node path for the action: %v", pathName)
	}

	switch name {
	case "Retry":
		p.closeRetryChannel(c)
		c.node.BroadcastChanges(false /* updateChildren */)
	default:
		return fmt.Errorf("Unknown action: %v", name)
	}
	return nil
}

func getTaskId(nodePath string) string {
	return path.Base(nodePath)
}

// closeRetryChannel closes the retryChannel and empties the Actions list in the rootUINode
// to indicate that the channel is closed and Retry action is not waited for anymore.
func (p *ParallelRunner) closeRetryChannel(c *Control) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.node.Actions) != 0 {
		c.node.Actions = []*workflow.Action{}
		close(c.retryChannel)
	}
}

// for testing action, new nodemanager, than call action()
