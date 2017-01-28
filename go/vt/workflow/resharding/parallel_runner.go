package resharding

import (
	"strings"
	"sync"

	"github.com/youtube/vitess/go/vt/concurrency"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

// ParallelRunner is used to control execute tasks concurrently.
type ParallelRunner struct {
	Checkpoints *workflowpb.WorkflowCheckpoint
	// checkpointMutex is used for protecting data access during checkpointing.
	checkpointMutex sync.Mutex
}

// Run is entry point for controling task executions.
func (p ParallelRunner) Run(stepName string,
	executeFunc func(*workflowpb.TaskAttribute) error,
	checkpointer Checkpoint,
	concurrencyLevel int) error {
	// TODO(yipeiw): wrap with logic to support retry, pause, restart

	// sem is a channel used to control the level of concurrency.
	var ec concurrency.AllErrorRecorder
	sem := make(chan bool, concurrencyLevel)
	for _, task := range p.extractTasks(p.Checkpoints.Tasks, p.Checkpoints.Settings, stepName) {
		sem <- true
		go func(t *workflowpb.Task) {
			defer func() { <-sem }()
			status := workflowpb.TaskState_TaskDone
			if err := executeFunc(t.Attributes); err != nil {
				status = workflowpb.TaskState_TaskFailed
				ec.RecordError(err)
			}

			p.checkpointMutex.Lock()
			t.State = status
			if err := checkpointer.CheckpointFunc(p.Checkpoints); err != nil {
				ec.RecordError(err)
			}
			p.checkpointMutex.Unlock()
		}(task)
	}
	// Wait until all running jobs are done.
	for i := 0; i < cap(sem); i++ {
		sem <- true
	}
	return ec.Error()
}

func (p ParallelRunner) extractTasks(tasks map[string]*workflowpb.Task, settings map[string]string, stepName string) []*workflowpb.Task {
	var executeTasks []*workflowpb.Task
	for _, taskID := range strings.Split(settings[stepName], ",") {
		t := tasks[taskID]
		if t.State != workflowpb.TaskState_TaskDone {
			executeTasks = append(executeTasks, t)
		}
	}
	return executeTasks
}
