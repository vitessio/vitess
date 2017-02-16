package resharding

import (
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/automation"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

func createTaskID(phase PhaseType, shardName string) string {
	return fmt.Sprintf("%s_%s", phase, shardName)
}

// GetTasks returns selected tasks for a phase from the checkpoint
// with expected execution order.
func (hw *HorizontalReshardingWorkflow) GetTasks(checkpoint *workflowpb.WorkflowCheckpoint, phase PhaseType) []*workflowpb.Task {
	var shards []string
	switch phase {
	case phaseCopySchema, phaseWaitForFilteredReplication, phaseDiff:
		shards = strings.Split(checkpoint.Settings["destination_shards"], ",")
	case phaseClone, phaseMigrateRdonly, phaseMigrateReplica, phaseMigrateMaster:
		shards = strings.Split(checkpoint.Settings["source_shards"], ",")
	}

	var tasks []*workflowpb.Task
	for _, s := range shards {
		taskID := createTaskID(phase, s)
		tasks = append(tasks, checkpoint.Tasks[taskID])
	}
	return tasks
}

func (hw *HorizontalReshardingWorkflow) runCopySchema(ctx context.Context, t *workflowpb.Task) error {
	s := t.Attributes["source_shard"]
	d := t.Attributes["destination_shard"]
	keyspace := t.Attributes["keyspace"]
	err := hw.wr.CopySchemaShardFromShard(ctx, nil /* tableArray*/, nil /* excludeTableArray */, true, /*includeViews*/
		keyspace, s, keyspace, d, wrangler.DefaultWaitSlaveTimeout)
	if err != nil {
		hw.logger.Infof("Horizontal Resharding: error in CopySchemaShard from %s to %s: %v.", s, d, err)
	}
	hw.logger.Infof("Horizontal Resharding: CopySchemaShard from %s to %s is finished.", s, d)
	return err
}

func (hw *HorizontalReshardingWorkflow) runSplitClone(ctx context.Context, t *workflowpb.Task) error {
	s := t.Attributes["source_shard"]
	worker := t.Attributes["vtworker"]
	keyspace := t.Attributes["keyspace"]

	sourceKeyspaceShard := topoproto.KeyspaceShardString(keyspace, s)
	// Reset the vtworker to avoid error if vtworker command has been called elsewhere.
	// This is because vtworker class doesn't cleanup the environment after execution.
	automation.ExecuteVtworker(ctx, worker, []string{"Reset"})
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

func (hw *HorizontalReshardingWorkflow) runWaitForFilteredReplication(ctx context.Context, t *workflowpb.Task) error {
	d := t.Attributes["destination_shard"]
	keyspace := t.Attributes["keyspace"]

	if err := hw.wr.WaitForFilteredReplication(ctx, keyspace, d, wrangler.DefaultWaitForFilteredReplicationMaxDelay); err != nil {
		hw.logger.Infof("Horizontal Resharding: error in WaitForFilteredReplication: %v.", err)
		return err
	}
	hw.logger.Infof("Horizontal Resharding:WaitForFilteredReplication is finished on " + d)
	return nil
}

func (hw *HorizontalReshardingWorkflow) runSplitDiff(ctx context.Context, t *workflowpb.Task) error {
	d := t.Attributes["destination_shard"]
	worker := t.Attributes["vtworker"]
	keyspace := t.Attributes["keyspace"]

	automation.ExecuteVtworker(hw.ctx, worker, []string{"Reset"})
	args := []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", topoproto.KeyspaceShardString(keyspace, d)}
	_, err := automation.ExecuteVtworker(ctx, worker, args)
	if err != nil {
		return err
	}

	hw.logger.Infof("Horizontal resharding: SplitDiff is finished.")
	return nil
}

func (hw *HorizontalReshardingWorkflow) runMigrate(ctx context.Context, t *workflowpb.Task) error {
	s := t.Attributes["source_shard"]
	keyspace := t.Attributes["keyspace"]
	servedTypeStr := t.Attributes["served_type"]

	servedType, err := topoproto.ParseTabletType(servedTypeStr)
	if err != nil {
		return fmt.Errorf("unknown tablet type: %v", servedTypeStr)
	}

	if servedType != topodatapb.TabletType_RDONLY &&
		servedType != topodatapb.TabletType_REPLICA &&
		servedType != topodatapb.TabletType_MASTER {
		return fmt.Errorf("wrong served type to be migrated: %v", servedTypeStr)
	}

	sourceKeyspaceShard := topoproto.KeyspaceShardString(keyspace, s)
	err = hw.wr.MigrateServedTypes(ctx, keyspace, s, nil /* cells */, servedType, false /* reverse */, false /* skipReFreshState */, wrangler.DefaultFilteredReplicationWaitTime)
	if err != nil {
		hw.logger.Infof("Horizontal Resharding: error in MigrateServedTypes on servedType %s: %v.", servedType, err)
		return err
	}
	hw.logger.Infof("Horizontal Resharding: MigrateServedTypes is finished on tablet %s serve type %s.", sourceKeyspaceShard, servedType)

	return nil
}
