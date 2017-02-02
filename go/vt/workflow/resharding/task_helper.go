package resharding

import (
	"fmt"
	"strings"

	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

func getTaskID(phase, shardType, shardName string) string {
	return fmt.Sprintf("%s_%s/%s", phase, shardType, shardName)
}

// GenerateTasks generates a copy of tasks for a specific step. The task status is not checked in this function.
func (hw *HorizontalReshardingWorkflow) GenerateTasks(checkpoint *workflowpb.WorkflowCheckpoint, stepName string) []*workflowpb.Task {
	var tasks []*workflowpb.Task
	switch stepName {
	case CopySchemaName, WaitFilteredReplicationName, SplitDiffName:
		// TODO: clean the logics and combine it into one function.
		for _, d := range strings.Split(checkpoint.Settings["destination_shards"], ",") {
			taskID := getTaskID(stepName, "dest", d)
			tasks = append(tasks, checkpoint.Tasks[taskID])
		}
	case SplitCloneName, MigrateName:
		for _, s := range strings.Split(checkpoint.Settings["source_shards"], ",") {
			taskID := getTaskID(stepName, "source", s)
			tasks = append(tasks, checkpoint.Tasks[taskID])
		}
	}
	return tasks
}

// InitTasks initialized the tasks for the workflow and return a checkpoint to store the information.
func (hw *HorizontalReshardingWorkflow) InitTasks() *workflowpb.WorkflowCheckpoint {
	taskMap := make(map[string]*workflowpb.Task)
	var sourceShards, destinationShards []string

	for _, perSrc := range hw.data {
		s := perSrc.sourceShard
		worker := perSrc.vtworker
		sourceShards = append(sourceShards, s)
		for _, d := range perSrc.destinationShards {
			destinationShards = append(destinationShards, d)
			updatePerDestinationTask(s, d, worker, CopySchemaName, taskMap)
			updatePerDestinationTask(s, d, worker, WaitFilteredReplicationName, taskMap)
			updatePerDestinationTask(s, d, worker, SplitDiffName, taskMap)
		}
		updatePerSourceTask(s, worker, SplitCloneName, taskMap)
		updatePerSourceTask(s, worker, MigrateName, taskMap)
	}

	return &workflowpb.WorkflowCheckpoint{
		CodeVersion: codeVersion,
		Tasks:       taskMap,
		Settings: map[string]string{
			"source_shards":      strings.Join(sourceShards, ","),
			"destination_shards": strings.Join(destinationShards, ","),
		},
	}
}

func updatePerDestinationTask(sourceShard, destinationShard, worker, name string, taskMap map[string]*workflowpb.Task) {
	taskID := getTaskID(name, "dest", destinationShard)
	taskMap[taskID] = &workflowpb.Task{
		Id:    taskID,
		State: workflowpb.TaskState_TaskNotStarted,
		Attributes: map[string]string{
			"source_shard":      sourceShard,
			"destination_shard": destinationShard,
			"vtworker":          worker,
		},
	}
}

func updatePerSourceTask(sourceShard, vtworker, name string, taskMap map[string]*workflowpb.Task) {
	taskID := getTaskID(name, "source", sourceShard)
	taskMap[taskID] = &workflowpb.Task{
		Id:    taskID,
		State: workflowpb.TaskState_TaskNotStarted,
		Attributes: map[string]string{
			"source_shard": sourceShard,
			"vtworker":     vtworker,
		},
	}
}
