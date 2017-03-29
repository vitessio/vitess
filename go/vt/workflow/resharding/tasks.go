package resharding

import (
	"fmt"
	"log"
	"strings"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/automation"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	workflowpb "github.com/youtube/vitess/go/vt/proto/workflow"
)

func createTaskID(phase PhaseType, shardName string) string {
	return fmt.Sprintf("%s/%s", phase, shardName)
}

// GetTasks returns selected tasks for a phase from the checkpoint
// with expected execution order.
func (hw *HorizontalReshardingWorkflow) GetTasks(phase PhaseType) []*workflowpb.Task {
	var shards []string
	switch phase {
	case phaseCopySchema, phaseWaitForFilteredReplication, phaseDiff:
		shards = strings.Split(hw.checkpoint.Settings["destination_shards"], ",")
	case phaseClone, phaseMigrateRdonly, phaseMigrateReplica, phaseMigrateMaster:
		shards = strings.Split(hw.checkpoint.Settings["source_shards"], ",")
	default:
		log.Fatalf("BUG: unknown phase type: %v", phase)
	}

	var tasks []*workflowpb.Task
	for _, s := range shards {
		taskID := createTaskID(phase, s)
		tasks = append(tasks, hw.checkpoint.Tasks[taskID])
	}
	return tasks
}

func (hw *HorizontalReshardingWorkflow) runCopySchema(ctx context.Context, t *workflowpb.Task) error {
	keyspace := t.Attributes["keyspace"]
	sourceShard := t.Attributes["source_shard"]
	destShard := t.Attributes["destination_shard"]
	return hw.wr.CopySchemaShardFromShard(ctx, nil /* tableArray*/, nil /* excludeTableArray */, true, /*includeViews*/
		keyspace, sourceShard, keyspace, destShard, wrangler.DefaultWaitSlaveTimeout)
}

func (hw *HorizontalReshardingWorkflow) runSplitClone(ctx context.Context, t *workflowpb.Task) error {
	keyspace := t.Attributes["keyspace"]
	sourceShard := t.Attributes["source_shard"]
	worker := t.Attributes["vtworker"]

	sourceKeyspaceShard := topoproto.KeyspaceShardString(keyspace, sourceShard)
	// Reset the vtworker to avoid error if vtworker command has been called elsewhere.
	// This is because vtworker class doesn't cleanup the environment after execution.
	if _, err := automation.ExecuteVtworker(ctx, worker, []string{"Reset"}); err != nil {
		return err
	}
	// The flag min_healthy_rdonly_tablets is set to 1 (default value is 2).
	// Therefore, we can reuse the normal end to end test setting, which has only 1 rdonly tablet.
	// TODO(yipeiw): Add min_healthy_rdonly_tablets as an input argument in UI.
	args := []string{"SplitClone", "--min_healthy_rdonly_tablets=1", sourceKeyspaceShard}
	_, err := automation.ExecuteVtworker(hw.ctx, worker, args)
	return err
}

func (hw *HorizontalReshardingWorkflow) runWaitForFilteredReplication(ctx context.Context, t *workflowpb.Task) error {
	keyspace := t.Attributes["keyspace"]
	destShard := t.Attributes["destination_shard"]
	return hw.wr.WaitForFilteredReplication(ctx, keyspace, destShard, wrangler.DefaultWaitForFilteredReplicationMaxDelay)
}

func (hw *HorizontalReshardingWorkflow) runSplitDiff(ctx context.Context, t *workflowpb.Task) error {
	keyspace := t.Attributes["keyspace"]
	destShard := t.Attributes["destination_shard"]
	worker := t.Attributes["vtworker"]

	if _, err := automation.ExecuteVtworker(hw.ctx, worker, []string{"Reset"}); err != nil {
		return err
	}
	args := []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", topoproto.KeyspaceShardString(keyspace, destShard)}
	_, err := automation.ExecuteVtworker(ctx, worker, args)
	return err
}

func (hw *HorizontalReshardingWorkflow) runMigrate(ctx context.Context, t *workflowpb.Task) error {
	keyspace := t.Attributes["keyspace"]
	sourceShard := t.Attributes["source_shard"]
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

	return hw.wr.MigrateServedTypes(ctx, keyspace, sourceShard, nil /* cells */, servedType, false /* reverse */, false /* skipReFreshState */, wrangler.DefaultFilteredReplicationWaitTime)
}
