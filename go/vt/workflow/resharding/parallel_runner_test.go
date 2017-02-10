package resharding

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/workflow"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

const (
	printName = "Sleep"
)

func TestParallelRunner(t *testing.T) {
	cp, err := startWorkflow(5)
	if err != nil {
		t.Errorf("%s: Fails in creating workflow", err)
	}
	ctx := context.Background()
	tasks := GetOrderedPrintTasks(cp.checkpoint)

	p := NewParallelRunner(ctx, cp, make(map[string]*workflow.Node), tasks)
	executeLog := func(attr map[string]string) error {
		t.Logf("The number passed to me is %v \n", attr["number"])
		return nil
	}
	if err := p.Run(executeLog, PARALLEL); err != nil {
		t.Errorf("%s: Parallel Runner should not fail", err)
	}

	// Check whether all tasks are in finished status.
	for _, task := range cp.checkpoint.Tasks {
		if task.State != workflowpb.TaskState_TaskDone || task.Error != "" {
			t.Fatalf("Task info: %v, %v, %v: Parallel Runner task not succeed", task.Id, task.State, task.Attributes)
		}
	}
}

func TestParallelRunnerRetryAction(t *testing.T) {
	cp, err := startWorkflow(5)
	if err != nil {
		t.Errorf("%s: Fails in creating workflow", err)
	}

	ctx := context.Background()

	// Create UI nodes. Each task has a node. These task nodes are the children of a root node.
	notifications := make(chan []byte, 10)
	nodeManager := workflow.NewNodeManager()
	_, index, err := nodeManager.GetAndWatchFullTree(notifications)
	if err != nil {
		t.Errorf("GetAndWatchTree Failed: %v", err)
	}
	defer nodeManager.CloseWatcher(index)

	rootNode := &workflow.Node{
		PathName: "test_root",
		Name:     "root",
	}
	if err := nodeManager.AddRootNode(rootNode); err != nil {
		t.Errorf("adding root node failed: %v", err)
	}
	result, ok := <-notifications

	taskNodeMap := make(map[string]*workflow.Node)
	for _, task := range cp.checkpoint.Tasks {
		taskNode := &workflow.Node{
			PathName: task.Id,
			Name:     "task_" + task.Id,
		}
		taskNodeMap[task.Id] = taskNode
		rootNode.Children = append(rootNode.Children, taskNode)
	}

	rootNode.BroadcastChanges(true /*updateChildren*/)

	result, ok = <-notifications
	if !ok ||
		strings.Contains(string(result), `"children":[]`) ||
		!strings.Contains(string(result), `"name":"task_Sleep_0"`) ||
		!strings.Contains(string(result), `"name":"task_Sleep_1"`) ||
		!strings.Contains(string(result), `"name":"task_Sleep_2"`) ||
		!strings.Contains(string(result), `"name":"task_Sleep_3"`) ||
		!strings.Contains(string(result), `"name":"task_Sleep_4"`) {
		t.Errorf("unexpected behavior in adding children nodes: %v, %v", ok, string(result))
	}

	// Set up ParallelRunner.
	tasks := GetOrderedPrintTasks(cp.checkpoint)
	p := NewParallelRunner(ctx, cp, taskNodeMap, tasks)

	// Set retry flag to be false. The targeting task will fail under this condition.
	retryFlag := false
	errMessage := "fake error for testing retry"
	executeLog := func(attr map[string]string) error {
		t.Logf("The number passed to me is %v \n", attr["number"])
		n, err := strconv.Atoi(attr["number"])
		if err != nil {
			t.Logf("Converting number string to int fails: %v \n", attr["number"])
			return err
		}
		if !retryFlag {
			if n == 3 {
				t.Logf("I will fail at this time since retry flag is false.")
				return errors.New(errMessage)
			}
		}
		return nil
	}

	go func() {
		// This goroutine is used to mornitor the UI change.
		// When the retry action is enabled, it will trigger it using nodemanager.
		for {
			select {
			case mornitor := <-notifications:
				if strings.Contains(string(mornitor), "Retry") {
					// Check if Retry action is enabled for the expected task.
					taskName := logTaskName(3)
					nodeTarget := taskNodeMap[taskName]
					taskTarget := cp.checkpoint.Tasks[taskName]
					if taskTarget.State != workflowpb.TaskState_TaskDone ||
						taskTarget.Error != errMessage ||
						len(nodeTarget.Actions) != 1 {
						t.Fatalf("Retry action is not enabled as expectedL %v, %v, %v", &nodeTarget, taskTarget.State, taskTarget.Error)
					}

					// Reset the retryFlag to make the task succeed when retrying.
					retryFlag = true

					t.Logf("Triggering retry action.")
					if err := nodeManager.Action(ctx, &workflow.ActionParameters{
						Path: "/test_root/" + logTaskName(3),
						Name: "Retry",
					}); err != nil {
						t.Errorf("unexpected action error: %v", err)
					}

					if len(nodeTarget.Actions) != 0 {
						t.Fatalf("the node actions should be empty after triggering retry: %v", nodeTarget.Actions)
					}
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Call ParallelRunner.Run through a goroutine. In this way, the failure of task will not block the main function.
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		err := p.Run(executeLog, PARALLEL)
		if err != nil {
			t.Logf("ParallelRunner.Run fails: %v", err)
		}
	}()

	waitGroup.Wait()
	// Check that all tasks are finished successfully.
	for _, task := range cp.checkpoint.Tasks {
		if task.State != workflowpb.TaskState_TaskDone || task.Error != "" {
			t.Fatalf("Task info: %v, %v, %v: Parallel Runner task not succeed", task.Id, task.State, task.Attributes)
		}
	}
}

func startWorkflow(taskNum int) (*CheckpointWriter, error) {
	initCheckpoint := InitPrintTasks(taskNum)

	w := &workflowpb.Workflow{
		Uuid:        "testparallelrunner",
		FactoryName: "simple_print",
		State:       workflowpb.WorkflowState_NotStarted,
	}
	ts := memorytopo.NewServer("cell")
	wi, err := ts.CreateWorkflow(context.TODO(), w)
	if err != nil {
		return nil, err
	}

	cp := NewCheckpointWriter(ts, initCheckpoint, wi)
	cp.Save()
	return cp, nil
}

func logTaskName(num int) string {
	return fmt.Sprintf("%v_%v", printName, num)
}

func InitPrintTasks(numTasks int) *workflowpb.WorkflowCheckpoint {
	tasks := make(map[string]*workflowpb.Task)
	var infoList []string
	for i := 0; i < numTasks; i++ {
		numStr := fmt.Sprintf("%v", i)
		t := &workflowpb.Task{
			Id:         logTaskName(i),
			State:      workflowpb.TaskState_TaskNotStarted,
			Attributes: map[string]string{"number": numStr},
		}
		tasks[t.Id] = t
		infoList = append(infoList, numStr)
	}
	return &workflowpb.WorkflowCheckpoint{
		CodeVersion: 0,
		Tasks:       tasks,
		Settings:    map[string]string{"numbers": strings.Join(infoList, ",")},
	}
}

func GetOrderedPrintTasks(checkpoint *workflowpb.WorkflowCheckpoint) []*workflowpb.Task {
	var tasks []*workflowpb.Task
	for _, n := range strings.Split(checkpoint.Settings["numbers"], ",") {
		num, err := strconv.Atoi(n)
		if err != nil {
			return nil
		}
		taskID := logTaskName(num)
		tasks = append(tasks, checkpoint.Tasks[taskID])
	}
	return tasks
}
