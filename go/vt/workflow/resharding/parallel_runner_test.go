package resharding

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

const (
	printName = "Sleep"

	codeVersion = 1
)

func executePrint(attr map[string]string) error {
	fmt.Printf("The number passed to me is %v \n", attr["Number"])
	return nil
}

func taskNameOfPrint(num string) string {
	return fmt.Sprintf("%v_%v", printName, num)
}

func InitPrintTasks(numTasks int) *workflowpb.WorkflowCheckpoint {
	tasks := make(map[string]*workflowpb.Task)
	var infoList []string
	for i := 0; i < numTasks; i++ {
		num := fmt.Sprintf("%v", i)
		t := &workflowpb.Task{
			TaskId:     taskNameOfPrint(num),
			State:      workflowpb.TaskState_TaskNotStarted,
			Attributes: map[string]string{"Number": num},
		}
		tasks[t.TaskId] = t
		infoList = append(infoList, num)
	}
	return &workflowpb.WorkflowCheckpoint{
		CodeVersion: codeVersion,
		Tasks:       tasks,
		Settings:    map[string]string{"numbers": strings.Join(infoList, ",")},
	}
}

func GetOrderedPrintTasks(wcp *workflowpb.WorkflowCheckpoint) []*workflowpb.Task {
	var tasks []*workflowpb.Task
	for _, n := range strings.Split(wcp.Settings["numbers"], ",") {
		taskID := taskNameOfPrint(n)
		tasks = append(tasks, wcp.Tasks[taskID])
	}
	return tasks
}

func TestParallelRunner(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	w := &workflowpb.Workflow{
		Uuid:        "testparallelrunner",
		FactoryName: "simple_print",
		State:       workflowpb.WorkflowState_NotStarted,
	}
	var err error
	var wi *topo.WorkflowInfo
	wi, err = ts.CreateWorkflow(context.TODO(), w)
	if err != nil {
		t.Errorf("%s: Parallel Runner fails in creating workflow", err)
	}

	taskNum := 5
	initCheckpoint := InitPrintTasks(taskNum)

	cp := &Checkpoint{
		topoServer: ts,
		wcp:        initCheckpoint,
		wi:         wi,
	}
	cp.Store()

	var p *ParallelRunner
	tasks := GetOrderedPrintTasks(initCheckpoint)
	if err := p.Run(tasks, executePrint, cp, PARALLEL); err != nil {
		t.Errorf("%s: Parallel Runner should not fail", err)
	}

	//Check whether all tasks are in finished status.
	for _, task := range cp.wcp.Tasks {
		if task.State != workflowpb.TaskState_TaskDone {
			t.Errorf("Task info: %v, %v, %v: Parallel Runner task not finished", task.TaskId, task.State, task.Attributes)
		}
	}
}
