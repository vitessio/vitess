package resharding

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/workflow/resharding"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

const (
	sleepStepName = "Sleep"
)

func executeSleep(attr *workflowpb.TaskAttribute) error {
	sleepDuration, err := strconv.Atoi(attr.Data["Duration"])
	if err != nil {
		return err
	}
	time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
	fmt.Printf("I've slept %v millisecs\n", sleepDuration)
	return nil
}

func InitSleepTasks(numTasks int) *workflowpb.WorkflowCheckpoint {
	tasks := make(map[string]*workflowpb.Task)
	var orderList []string
	for i := 0; i < numTasks; i++ {
		duration := fmt.Sprintf("%v", i*100)
		t := new(workflowpb.Task)
		t.State = workflowpb.TaskState_TaskNotStated
		t.Attributes = &workflowpb.TaskAttribute{
			Data: map[string]string{"Duration": duration},
		}
		taskName := fmt.Sprintf("%v_%v", sleepStepName, duration)
		tasks[taskName] = t
		orderList = append(orderList, taskName)
	}
	return &workflowpb.WorkflowCheckpoint{
		CodeVersion: 1,
		Tasks:       tasks,
		Settings:    map[string]string{sleepStepName: strings.Join(orderList, ",")},
	}
}

func TestParallelRunner(t *testing.T) {
	// FIXME(yipeiw): Simply dump the checkpoints data to tmp file and check manually at this verison.
	taskNum := 5
	p := resharding.ParallelRunner{
		Checkpoints: InitSleepTasks(taskNum),
	}

	now := time.Now()
	secs := now.Unix()
	cp := &resharding.CheckpointFile{
		FilePath: fmt.Sprintf("/tmp/parallel_test_%v", secs),
	}
	cp.CheckpointFunc(p.Checkpoints)

	if err := p.Run(sleepStepName, executeSleep, cp, taskNum); err != nil {
		t.Errorf("%s: Parallel Runner should not fail", err)
	}
}
