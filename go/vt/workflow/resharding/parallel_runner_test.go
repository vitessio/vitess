package resharding

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

const (
	printName = "Sleep"
)

func TestParallelRunner(t *testing.T) {
	w := &workflowpb.Workflow{
		Uuid:        "testparallelrunner",
		FactoryName: "simple_print",
		State:       workflowpb.WorkflowState_NotStarted,
	}

	ts := memorytopo.NewServer("cell")
	wi, err := ts.CreateWorkflow(context.TODO(), w)
	if err != nil {
		t.Errorf("%s: Parallel Runner fails in creating workflow", err)
	}

	taskNum := 5
	initCheckpoint := InitPrintTasks(taskNum)

	cp := NewCheckpointWriter(ts, initCheckpoint, wi)
	cp.Save()

	tasks := GetOrderedPrintTasks(initCheckpoint)
	executeLog := func(attr map[string]string) error {
		t.Logf("The number passed to me is %v \n", attr["number"])
		return nil
	}

	p := &ParallelRunner{}
	if err := p.Run(tasks, executeLog, cp, PARALLEL); err != nil {
		t.Errorf("%s: Parallel Runner should not fail", err)
	}

	// Check whether all tasks are in finished status.
	for _, task := range cp.checkpoint.Tasks {
		if task.State != workflowpb.TaskState_TaskDone {
			t.Fatalf("Task info: %v, %v, %v: Parallel Runner task not finished", task.Id, task.State, task.Attributes)
		}
	}
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
		CodeVersion: codeVersion,
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
