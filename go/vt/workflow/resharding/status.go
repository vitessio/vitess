package resharding

import (
	"fmt"

	statepb "github.com/youtube/vitess/go/vt/proto/workflowstate"
)

// GetTaskParam generates the parameters for unfinished tasks.
func GetTaskParam(tasks *statepb.TaskContainer) []*statepb.TaskParam {
	var params []*statepb.TaskParam
	for _, v := range tasks.Tasks {
		if v.State != statepb.TaskState_Done && v.State != statepb.TaskState_Running {
			params = append(params, v.Param)
		}
	}
	return params
}

// PrintTasks prints the tasks and the dynamically generated parameters for a specific step.
func PrintTasks(step string, tasks *statepb.TaskContainer, params []*statepb.TaskParam) {
	fmt.Printf("\n%s printing the tasks information:", step)
	for _, v := range tasks.Tasks {
		fmt.Println("task param: ", v.Param)
		fmt.Println("task status: ", v.State)
	}

	if params != nil {
		fmt.Println("printing the task parameters passed to execution")
		for _, p := range params {
			fmt.Println("execution task param: ", p)
		}
	}
}

func initParamPerDestinationShard(subWorkflows []*PerShardHorizontalResharding) *statepb.TaskContainer {
	tasks := make(map[string]*statepb.Task)
	for _, sw := range subWorkflows {
		for _, d := range sw.DestinationShards {
			param := &statepb.TaskParam{
				Keyspace:          sw.Keyspace,
				DestinationShards: []string{d},
				SourceShards:      []string{sw.SourceShard},
				Vtworker:          sw.Vtworker,
			}
			t := &statepb.Task{
				Param: param,
				State: statepb.TaskState_Created,
			}
			tasks[param.String()] = t
		}
	}
	return &statepb.TaskContainer{Tasks: tasks}
}

func initParamPerSourceShard(subWorkflows []*PerShardHorizontalResharding) *statepb.TaskContainer {
	tasks := make(map[string]*statepb.Task)
	for _, sw := range subWorkflows {
		param := &statepb.TaskParam{
			Keyspace:          sw.Keyspace,
			DestinationShards: sw.DestinationShards,
			SourceShards:      []string{sw.SourceShard},
			Vtworker:          sw.Vtworker,
		}
		t := &statepb.Task{
			Param: param,
			State: statepb.TaskState_Created,
		}
		tasks[param.String()] = t
	}
	return &statepb.TaskContainer{Tasks: tasks}
}

// TaskHelper includes methods to generate task parameters based on the execution status while updating the status at the same time.
// Each step should implements this interface.
type TaskHelper interface {
	InitTasks(perhw []*PerShardHorizontalResharding) *statepb.TaskContainer
}

// CopySchemaTaskHelper implements TaskHelper interface.
type CopySchemaTaskHelper struct{}

// InitTasks implements TaskHelper.InitTasks (per destination shard manner).
func (c CopySchemaTaskHelper) InitTasks(subWorkflows []*PerShardHorizontalResharding) *statepb.TaskContainer {
	return initParamPerDestinationShard(subWorkflows)
}

// SplitCloneTaskHelper implements TaskHelper interface.
type SplitCloneTaskHelper struct{}

// InitTasks implements TaskHelper.InitTasks.
func (c SplitCloneTaskHelper) InitTasks(subWorkflows []*PerShardHorizontalResharding) *statepb.TaskContainer {
	return initParamPerSourceShard(subWorkflows)
}

// WaitFilteredReplicationTaskHelper implements TaskHelper interface.
type WaitFilteredReplicationTaskHelper struct{}

// InitTasks implements TaskHelper.InitTasks.
func (c WaitFilteredReplicationTaskHelper) InitTasks(subWorkflows []*PerShardHorizontalResharding) *statepb.TaskContainer {
	return initParamPerDestinationShard(subWorkflows)
}

// SplitDiffTaskHelper implements TaskHelper interface.
type SplitDiffTaskHelper struct{}

// InitTasks implements TaskHelper.InitTasks.
func (c SplitDiffTaskHelper) InitTasks(subWorkflows []*PerShardHorizontalResharding) *statepb.TaskContainer {
	return initParamPerSourceShard(subWorkflows)
}

// MigrateTaskHelper implements TaskHelper interface.
type MigrateTaskHelper struct{}

// InitTasks implements TaskHelper.InitTasks.
func (c MigrateTaskHelper) InitTasks(subWorkflows []*PerShardHorizontalResharding) *statepb.TaskContainer {
	return initParamPerSourceShard(subWorkflows)
}
