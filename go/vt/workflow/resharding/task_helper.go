package resharding

import (
	"fmt"
	"strings"

	"github.com/youtube/vitess/go/vt/automation"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

func createTaskID(phase, shardType, shardName string) string {
	return fmt.Sprintf("%s_%s_%s", phase, shardType, shardName)
}

// GenerateTasks generates a copy of tasks for a specific step. The task status is not checked in this function.
func (hw *HorizontalReshardingWorkflow) GenerateTasks(checkpoint *workflowpb.WorkflowCheckpoint, stepName string) []*workflowpb.Task {
	var tasks []*workflowpb.Task
	switch stepName {
	case CopySchemaName, WaitFilteredReplicationName, SplitDiffName:
		for _, d := range strings.Split(checkpoint.Settings["destination_shards"], ",") {
			taskID := createTaskID(stepName, "dest", d)
			tasks = append(tasks, checkpoint.Tasks[taskID])
		}
	case SplitCloneName, MigrateName:
		for _, s := range strings.Split(checkpoint.Settings["source_shards"], ",") {
			taskID := createTaskID(stepName, "source", s)
			tasks = append(tasks, checkpoint.Tasks[taskID])
		}
	}
	return tasks
}

// runCopySchemaPerShard runs CopySchema for a destination shard.
// There should be #destshards parameters, while each param includes 1 sourceshard and 1 destshard.
func (hw *HorizontalReshardingWorkflow) runCopySchema(attr map[string]string) error {
	s := attr["source_shard"]
	d := attr["destination_shard"]
	keyspace := attr["keyspace"]
	err := hw.wr.CopySchemaShardFromShard(hw.ctx, nil /* tableArray*/, nil /* excludeTableArray */, true, /*includeViews*/
		keyspace, s, keyspace, d, wrangler.DefaultWaitSlaveTimeout)
	if err != nil {
		hw.logger.Infof("Horizontal Resharding: error in CopySchemaShardFromShard from %s to %s: %v.", s, d, err)
	}
	hw.logger.Infof("Horizontal Resharding: CopySchemaShardFromShard from %s to %s is finished.", s, d)
	return err
}

// runSplitClonePerShard runs SplitClone for a source shard.
// There should be #sourceshards parameters, while each param includes 1 sourceshard and its destshards. The destShards are useless here.
func (hw *HorizontalReshardingWorkflow) runSplitClone(attr map[string]string) error {
	s := attr["source_shard"]
	worker := attr["vtworker"]
	keyspace := attr["keyspace"]

	sourceKeyspaceShard := topoproto.KeyspaceShardString(keyspace, s)
	// Reset the vtworker to avoid error if vtworker command has been called elsewhere.
	// This is because vtworker class doesn't cleanup the environment after execution.
	automation.ExecuteVtworker(hw.ctx, worker, []string{"Reset"})
	// The flag min_healthy_rdonly_tablets is set to 1 (default value is 2).
	// Therefore, we can reuse the normal end to end test setting, which has only 1 rdonly tablet.
	// TODO(yipeiw): Add min_healthy_rdonly_tablets as an input argument in UI.
	args := []string{"SplitClone", "--min_healthy_rdonly_tablets=1", sourceKeyspaceShard}
	if _, err := automation.ExecuteVtworker(hw.ctx, worker, args); err != nil {
		hw.logger.Infof("Horizontal resharding: error in SplitClone in keyspace %s: %v.", keyspace, err)
		return err
	}
	hw.logger.Infof("Horizontal resharding: SplitClone is finished.")

	return nil
}

// runWaitFilteredReplication runs WaitForFilteredReplication for a destination shard.
// There should be #destshards parameters, while each param includes 1 sourceshard and 1 destshard.
func (hw *HorizontalReshardingWorkflow) runWaitFilteredReplication(attr map[string]string) error {
	d := attr["destination_shard"]
	keyspace := attr["keyspace"]

	if err := hw.wr.WaitForFilteredReplication(hw.ctx, keyspace, d, wrangler.DefaultWaitForFilteredReplicationMaxDelay); err != nil {
		hw.logger.Infof("Horizontal Resharding: error in WaitForFilteredReplication: %v.", err)
		return err
	}
	hw.logger.Infof("Horizontal Resharding:WaitForFilteredReplication is finished on " + d)
	return nil
}

// runSplitDiffPerShard runs SplitDiff for a source shard.
// There should be #sourceshards parameters, while each param includes 1 sourceshard and its destshards.
func (hw *HorizontalReshardingWorkflow) runSplitDiff(attr map[string]string) error {
	d := attr["destination_shard"]
	worker := attr["vtworker"]
	keyspace := attr["keyspace"]

	automation.ExecuteVtworker(hw.ctx, worker, []string{"Reset"})
	args := []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", topoproto.KeyspaceShardString(keyspace, d)}
	_, err := automation.ExecuteVtworker(hw.ctx, worker, args)
	if err != nil {
		return err
	}

	hw.logger.Infof("Horizontal resharding: SplitDiff is finished.")
	return nil
}

// runMigratePerShard runs the migration sequentially among all source shards.
// There should be 1 parameter, which includes all source shards to be migrated.
func (hw *HorizontalReshardingWorkflow) runMigrate(attr map[string]string) error {
	s := attr["source_shard"]
	keyspace := attr["keyspace"]

	sourceKeyspaceShard := topoproto.KeyspaceShardString(keyspace, s)
	servedTypeParams := []topodatapb.TabletType{topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_MASTER}
	for _, servedType := range servedTypeParams {
		err := hw.wr.MigrateServedTypes(hw.ctx, keyspace, s, nil /* cells */, servedType, false /* reverse */, false /* skipReFreshState */, wrangler.DefaultFilteredReplicationWaitTime)
		if err != nil {
			hw.logger.Infof("Horizontal Resharding: error in MigrateServedTypes on servedType %s: %v.", servedType, err)
			return err
		}
		hw.logger.Infof("Horizontal Resharding: MigrateServedTypes is finished on tablet %s serve type %s.", sourceKeyspaceShard, servedType)
	}
	return nil
}

func updatePerDestinationTask(keyspace, sourceShard, destinationShard, worker, name string, taskMap map[string]*workflowpb.Task) {
	taskID := createTaskID(name, "dest", destinationShard)
	taskMap[taskID] = &workflowpb.Task{
		Id:    taskID,
		State: workflowpb.TaskState_TaskNotStarted,
		Attributes: map[string]string{
			"source_shard":      sourceShard,
			"destination_shard": destinationShard,
			"vtworker":          worker,
			"keyspace":          keyspace,
		},
	}
}

func updatePerSourceTask(keyspace, sourceShard, vtworker, name string, taskMap map[string]*workflowpb.Task) {
	taskID := createTaskID(name, "source", sourceShard)
	taskMap[taskID] = &workflowpb.Task{
		Id:    taskID,
		State: workflowpb.TaskState_TaskNotStarted,
		Attributes: map[string]string{
			"source_shard": sourceShard,
			"vtworker":     vtworker,
			"keyspace":     keyspace,
		},
	}
}
